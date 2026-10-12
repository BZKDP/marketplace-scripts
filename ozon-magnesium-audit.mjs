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
    `with matched_orders as (
       select distinct i.assembly_id, i.posting_number
         from ozon_assembly_order_items i
         join ozon_assembly_orders o
           on o.assembly_id = i.assembly_id and o.posting_number = i.posting_number
        where o.sent_to_delivering_at is null
          and o.collected_at is null
          and (
            upper(coalesce(i.offer_id, '')) = 'GRA-01503'
            or lower(coalesce(i.name, '')) ~ '(магний|magnesium).*(b6|в6|б6)'
            or lower(coalesce(i.name, '')) ~ '(b6|в6|б6).*(магний|magnesium)'
          )
     )
     select o.store_id, o.store_name, o.assembly_id, a.name as assembly_name,
            a.access_mode, a.employee_workflow_stage,
            o.posting_number, o.assigned_user_id, o.collected_at,
            coalesce(sum(i.quantity), 0)::int as order_units,
            json_agg(json_build_object(
              'article', i.offer_id, 'name', i.name, 'quantity', i.quantity,
              'scannedQty', i.scanned_qty
            ) order by i.line_no) as items
       from matched_orders m
       join ozon_assembly_orders o on o.assembly_id=m.assembly_id and o.posting_number=m.posting_number
       join ozon_assemblies a on a.id=o.assembly_id
       join ozon_assembly_order_items i on i.assembly_id=o.assembly_id and i.posting_number=o.posting_number
      group by o.store_id, o.store_name, o.assembly_id, a.name, a.access_mode,
               a.employee_workflow_stage, o.posting_number, o.assigned_user_id, o.collected_at
      order by o.store_id, o.assembly_id, o.posting_number`
  );
  const matchedItems = await db.query(
    `select upper(coalesce(i.offer_id, '')) as article, i.name,
            count(distinct i.posting_number)::int as orders,
            coalesce(sum(i.quantity), 0)::int as units
       from ozon_assembly_order_items i
       join ozon_assembly_orders o on o.assembly_id=i.assembly_id and o.posting_number=i.posting_number
      where o.sent_to_delivering_at is null and o.collected_at is null
        and (
          upper(coalesce(i.offer_id, '')) = 'GRA-01503'
          or lower(coalesce(i.name, '')) ~ '(магний|magnesium).*(b6|в6|б6)'
          or lower(coalesce(i.name, '')) ~ '(b6|в6|б6).*(магний|magnesium)'
        )
      group by upper(coalesce(i.offer_id, '')), i.name
      order by article, i.name`
  );
  const summary = {};
  for (const row of rows.rows) {
    const key = row.store_id || "unknown";
    summary[key] ??= { orders: 0, totalOrderUnits: 0, sourceAssemblies: {} };
    summary[key].orders += 1;
    summary[key].totalOrderUnits += Number(row.order_units || 0);
    summary[key].sourceAssemblies[row.assembly_id] = (summary[key].sourceAssemblies[row.assembly_id] || 0) + 1;
  }
  console.log(JSON.stringify({ summary, matchedItems: matchedItems.rows, orders: rows.rows }, null, 2));
} finally {
  await db.end();
}
