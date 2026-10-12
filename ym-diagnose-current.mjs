import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/YM/node_modules/pg/lib/index.js";

const { Pool } = pg;
const servicePid = Number(process.argv[2]);
if (servicePid > 0) {
  for (const entry of fs.readFileSync(`/proc/${servicePid}/environ`).toString().split("\0")) {
    const index = entry.indexOf("=");
    if (index > 0) process.env[entry.slice(0, index)] = entry.slice(index + 1);
  }
}

const db = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  options: process.env.DB_OPTIONS || undefined,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable"
    ? undefined
    : { rejectUnauthorized: false },
});

const { resolveWarehouseProductMeta } = await import(
  "/opt/grasklad-sborka/apps/OZ/server/services/sklad-movements.mjs"
);
const warehouseMeta = await resolveWarehouseProductMeta([
  "GRA-08090",
  "GRA-08240",
  "MRKT51289260056",
  "MRKT52639406237",
]);
for (const [key, value] of warehouseMeta) {
  console.log("WAREHOUSE", JSON.stringify({ key, value }));
}

const warehouseDb = new Pool({
  host: process.env.SKLAD_DB_HOST,
  port: Number(process.env.SKLAD_DB_PORT || 5432),
  user: process.env.SKLAD_DB_USER,
  password: process.env.SKLAD_DB_PASSWORD,
  database: process.env.SKLAD_DB_NAME,
  ssl: String(process.env.SKLAD_DB_SSL || "auto").toLowerCase() === "disable"
    ? undefined
    : { rejectUnauthorized: false },
});
const warehouseProducts = await warehouseDb.query(`
  select id, article, code, name, production_barcode, barcode_list, marketplace_barcodes_json
    from products_s
   where id = any($1::bigint[])
   order by id
`, [[4, 27]]);
for (const row of warehouseProducts.rows) console.log("WAREHOUSE_PRODUCT", JSON.stringify(row));
await warehouseDb.end();

const assemblies = await db.query(`
  select a.id, a.name, a.status, a.campaign_id, a.allow_zero_stock,
         a.created_at, a.updated_at,
         count(distinct o.order_id)::int orders,
         coalesce(sum(i.quantity), 0)::int units,
         coalesce(sum(i.picked_qty), 0)::int picked,
         string_agg(distinct concat(coalesce(u.external_full_name, trim(concat(u.surname, ' ', u.name))), ':', u.id), ', ') users
    from ym_assemblies a
    join ym_orders o on o.assembly_id = a.id
    join ym_order_items i on i.order_id = o.order_id
    left join users u on u.id = o.assigned_user_id
   where a.created_at >= timestamptz '2026-09-03 00:00:00+03'
   group by a.id
  having count(distinct o.order_id) = 3
   order by a.created_at desc
`);

for (const assembly of assemblies.rows) {
  console.log("ASSEMBLY", JSON.stringify(assembly));
  for (const probe of [
    { barcode: "400000000848", productId: 4 },
    { barcode: "400000000862", productId: 27 },
  ]) {
    const match = await db.query(`
      select i.offer_id, i.warehouse_product_id
        from ym_order_items i
        join ym_orders o on o.order_id = i.order_id
       where o.assembly_id = $1 and o.assigned_user_id = 129 and o.local_state = 'assigned'
         and i.picked_qty < i.quantity
         and (
           exists (select 1 from jsonb_array_elements_text(i.barcodes) b where upper(regexp_replace(b, '\\s+', '', 'g')) = upper($2))
           or ($3::bigint is not null and i.warehouse_product_id = $3)
         )
       order by o.order_id, i.item_id
       limit 1
    `, [assembly.id, probe.barcode, probe.productId]);
    console.log("DRY_MATCH", JSON.stringify({ ...probe, match: match.rows[0] || null }));
  }
  const orders = await db.query(`
    select o.order_id, o.local_state, o.yandex_status, o.yandex_substatus,
           o.assigned_user_id, coalesce(u.external_full_name, trim(concat(u.surname, ' ', u.name))) employee, i.item_id, i.offer_id, i.name,
           i.quantity, i.picked_qty, i.packed_qty, i.warehouse_product_id,
           i.barcodes, i.picture_url
      from ym_orders o
      join ym_order_items i on i.order_id = o.order_id
      left join users u on u.id = o.assigned_user_id
     where o.assembly_id = $1
     order by o.order_id, i.item_id
  `, [assembly.id]);
  for (const row of orders.rows) console.log("ITEM", JSON.stringify(row));
  const scans = await db.query(`
    select request_id, user_id, action, order_id, barcode, status, response, created_at, updated_at
      from ym_scan_requests
     where assembly_id = $1
     order by created_at desc
     limit 30
  `, [assembly.id]);
  for (const row of scans.rows) console.log("SCAN", JSON.stringify(row));
  const audit = await db.query(`
    select user_id, action, entity_type, entity_id, details, created_at
      from ym_audit_log
     where entity_id = $1
     order by created_at desc
     limit 30
  `, [String(assembly.id)]);
  for (const row of audit.rows) console.log("AUDIT", JSON.stringify(row));
}

await db.end();
