import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.trim().replace(/^\uFEFF/, "");
  if (!line || line.startsWith("#")) continue;
  const separator = line.indexOf("=");
  if (separator < 1) continue;
  const key = line.slice(0, separator).trim();
  let value = line.slice(separator + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[key] ??= value;
}

const ids = [
  "ozon-1788592910741-irina-aa53b0",
  "ozon-hold-magnesium-irina-1788593522339-28f1cc",
  "ozon-1788592910812-evgeny-92b047",
  "ozon-hold-magnesium-evgeny-1788593522355-5fa6a5",
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

try {
  const stats = await db.query(
    `select a.id, a.name, a.store_id, a.access_mode, a.labels_status, a.labels_loaded, a.labels_total,
            count(distinct o.posting_number)::int as orders,
            coalesce(sum(i.quantity), 0)::int as units,
            count(distinct o.posting_number) filter (where o.assigned_user_id is not null)::int as assigned,
            count(distinct o.posting_number) filter (where o.collected_at is not null)::int as collected,
            count(distinct o.posting_number) filter (where exists (
              select 1 from ozon_assembly_order_items mi
               where mi.assembly_id=o.assembly_id and mi.posting_number=o.posting_number
                 and (
                   upper(coalesce(mi.offer_id, ''))='GRA-01503'
                   or lower(coalesce(mi.name, '')) ~ '(магний|magnesium).*(b6|в6|б6)'
                   or lower(coalesce(mi.name, '')) ~ '(b6|в6|б6).*(магний|magnesium)'
                 )
            ))::int as magnesium_orders
       from ozon_assemblies a
       left join ozon_assembly_orders o on o.assembly_id=a.id
       left join ozon_assembly_order_items i on i.assembly_id=o.assembly_id and i.posting_number=o.posting_number
      where a.id=any($1::text[])
      group by a.id
      order by a.store_id, a.id`,
    [ids]
  );
  const violations = [];
  for (const row of stats.rows) {
    const isHold = row.id.includes("hold-magnesium");
    if (isHold && Number(row.magnesium_orders) !== Number(row.orders)) violations.push(`${row.id}: не каждый заказ содержит магний`);
    if (!isHold && Number(row.magnesium_orders) !== 0) violations.push(`${row.id}: в основной сборке остался магний`);
    if (Number(row.labels_loaded) !== Number(row.labels_total)) violations.push(`${row.id}: загружены не все этикетки`);
    if (Number(row.assigned) !== 0 || Number(row.collected) !== 0) violations.push(`${row.id}: найден активный прогресс`);
  }
  console.log(JSON.stringify({ ok: violations.length === 0, violations, assemblies: stats.rows }, null, 2));
} finally {
  await db.end();
}
