import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/YM/node_modules/pg/lib/index.js";

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
  const rows = await db.query(
    `select a.id as assembly_id, a.name as assembly_name, a.status as assembly_status,
            a.allow_zero_stock, a.campaign_id, a.created_at, a.launched_at,
            o.order_id, o.local_state, o.yandex_status, o.yandex_substatus,
            u.id as user_id, trim(concat(u.surname, ' ', u.name)) as employee,
            u.external_employee_id, u.external_full_name,
            i.item_id, i.offer_id, i.name as item_name, i.quantity, i.picked_qty,
            i.packed_qty, i.barcodes, i.warehouse_product_id, i.picture_url
       from ym_assemblies a
       join ym_orders o on o.assembly_id=a.id
       left join users u on u.id=o.assigned_user_id
       join ym_order_items i on i.order_id=o.order_id
      where a.name ilike '%04.09 12:42%'
      order by a.created_at desc, o.order_id, i.item_id`
  );
  const recentScans = await db.query(
    `select r.created_at, r.action, r.assembly_id, r.order_id, r.barcode, r.status,
            r.response, trim(concat(u.surname, ' ', u.name)) as employee
       from ym_scan_requests r join users u on u.id=r.user_id
      where r.created_at > now() - interval '24 hours'
      order by r.created_at desc limit 30`
  );
  const recentAudit = await db.query(
    `select l.created_at, l.action, l.entity_type, l.entity_id, l.details,
            trim(concat(u.surname, ' ', u.name)) as employee
       from ym_audit_log l left join users u on u.id=l.user_id
      where l.created_at > now() - interval '24 hours'
      order by l.created_at desc limit 50`
  );
  const warehouseDb = new Pool({
    host: process.env.SKLAD_DB_HOST,
    port: Number(process.env.SKLAD_DB_PORT || 5432),
    user: process.env.SKLAD_DB_USER,
    password: process.env.SKLAD_DB_PASSWORD,
    database: process.env.SKLAD_DB_NAME,
    ssl: String(process.env.SKLAD_DB_SSL || "auto").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
    max: 1,
  });
  let warehouseProducts = [];
  try {
    const warehouse = await warehouseDb.query(
      `select id, article, code, name, production_barcode, barcode_list, marketplace_barcodes_json, archived
         from products_s where id=any($1::int[]) order by id`,
      [[4, 27]]
    );
    warehouseProducts = warehouse.rows;
  } finally {
    await warehouseDb.end();
  }
  console.log(JSON.stringify({ rows: rows.rows, warehouseProducts, recentScans: recentScans.rows, recentAudit: recentAudit.rows }, null, 2));
} finally {
  await db.end();
}
