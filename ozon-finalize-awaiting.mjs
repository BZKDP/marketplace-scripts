import fs from "node:fs";
import path from "node:path";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.trim();
  if (!line || line.startsWith("#")) continue;
  const separator = line.indexOf("=");
  if (separator < 1) continue;
  const key = line.slice(0, separator).trim();
  let value = line.slice(separator + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[key] ??= value;
}

const targets = [
  {
    id: "ozon-sync-awaiting-deliver-irina",
    storeId: "irina",
    storeName: "ИП Ирина",
    warehouseId: "1020005026543450",
    warehouseName: "Мск",
  },
  {
    id: "ozon-sync-awaiting-deliver-evgeny",
    storeId: "evgeny",
    storeName: "ИП Евгений",
    warehouseId: "1020005010664520",
    warehouseName: "Москва",
  },
];

const { Pool } = pg;
const db = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
  max: 1,
});

const client = await db.connect();
let backupDir = "";
try {
  await client.query("begin");
  const ids = targets.map((target) => target.id);
  const beforeAssemblies = await client.query("select * from ozon_assemblies where id = any($1::text[]) for update", [ids]);
  const beforeOrders = await client.query(
    `select assembly_id, posting_number, store_id, store_name, warehouse_id, warehouse_name,
            assigned_user_id, collected_at, sent_to_delivering_at
       from ozon_assembly_orders
      where assembly_id = any($1::text[])
      order by assembly_id, posting_number`,
    [ids]
  );
  const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  backupDir = path.join("/opt/grasklad-sborka/backups", `${stamp}-ozon-awaiting-sync`);
  fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(backupDir, "before.json"), JSON.stringify({ assemblies: beforeAssemblies.rows, orders: beforeOrders.rows }, null, 2), { mode: 0o600 });

  for (const target of targets) {
    const assemblyUpdate = await client.query(
      `update ozon_assemblies
          set store_id = $2,
              store_name = $3,
              warehouse_id = $4,
              warehouse_name = $5,
              location_code = 'moscow',
              sklad_warehouse_id = 38,
              sklad_warehouse_name = 'Москва 2',
              updated_at = now()
        where id = $1`,
      [target.id, target.storeId, target.storeName, target.warehouseId, target.warehouseName]
    );
    if (assemblyUpdate.rowCount !== 1) throw new Error(`Сборка ${target.id} не найдена`);
    await client.query(
      `update ozon_assembly_orders
          set store_id = $2,
              store_name = $3,
              warehouse_id = $4,
              warehouse_name = $5,
              updated_at = now()
        where assembly_id = $1`,
      [target.id, target.storeId, target.storeName, target.warehouseId, target.warehouseName]
    );
  }
  await client.query("commit");

  for (let attempt = 0; attempt < 30; attempt += 1) {
    const pending = await client.query(
      `select count(*)::int as count
         from ozon_assemblies
        where id = any($1::text[])
          and coalesce(labels_status, '') in ('loading', 'pending')`,
      [ids]
    );
    if (Number(pending.rows[0]?.count || 0) === 0) break;
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  const result = await client.query(
    `select a.id, a.name, a.store_id, a.store_name, a.access_mode,
            a.location_code, a.warehouse_id, a.warehouse_name,
            a.sklad_warehouse_id, a.sklad_warehouse_name,
            a.labels_status, a.labels_total, a.labels_loaded, a.labels_error,
            count(distinct o.posting_number)::int as orders,
            coalesce(sum(i.quantity), 0)::int as units,
            count(distinct o.posting_number) filter (where o.assigned_user_id is not null)::int as assigned,
            count(distinct o.posting_number) filter (where o.collected_at is not null)::int as collected,
            count(distinct o.posting_number) filter (where not exists (
              select 1 from ozon_assembly_order_items missing_i
               where missing_i.assembly_id = o.assembly_id and missing_i.posting_number = o.posting_number
            ))::int as orders_without_items
       from ozon_assemblies a
       left join ozon_assembly_orders o on o.assembly_id = a.id and o.sent_to_delivering_at is null
       left join ozon_assembly_order_items i on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
      where a.id = any($1::text[])
      group by a.id
      order by a.store_id`,
    [ids]
  );
  console.log(JSON.stringify({ backupDir, assemblies: result.rows }, null, 2));
} catch (error) {
  await client.query("rollback").catch(() => {});
  throw error;
} finally {
  client.release();
  await db.end();
}
