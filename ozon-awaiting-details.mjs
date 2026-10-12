import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.trim().replace(/^\uFEFF/, "");
  if (!line || line.startsWith("#")) continue;
  const separator = line.indexOf("=");
  if (separator < 1) continue;
  const key = line.slice(0, separator).trim();
  let value = line.slice(separator + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1);
  }
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
  const assemblies = await db.query(
    `select a.*,
            count(distinct o.posting_number)::int as orders,
            count(distinct o.posting_number) filter (where o.assigned_user_id is not null)::int as assigned,
            count(distinct o.posting_number) filter (where o.collected_at is not null)::int as collected,
            count(distinct o.posting_number) filter (where o.sent_to_delivering_at is not null)::int as sent,
            coalesce(sum(i.quantity), 0)::int as units,
            coalesce(sum(i.scanned_qty), 0)::int as scanned_units
       from ozon_assemblies a
       left join ozon_assembly_orders o on o.assembly_id = a.id
       left join ozon_assembly_order_items i
         on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
      where a.id like 'ozon-sync-awaiting-deliver-%'
         or a.id = 'ozon-1788514972753-75a445'
      group by a.id
      order by a.store_id, a.created_at desc`
  );
  const foreign = await db.query(
    `select o.assembly_id, o.posting_number, o.assigned_user_id, o.collected_at,
            o.sent_to_delivering_at, o.store_id,
            coalesce(sum(i.quantity), 0)::int as units,
            coalesce(sum(i.scanned_qty), 0)::int as scanned_units
       from ozon_assembly_orders o
       left join ozon_assembly_order_items i
         on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
      where o.posting_number = '0137510476-0173-1'
      group by o.assembly_id, o.posting_number, o.assigned_user_id, o.collected_at,
               o.sent_to_delivering_at, o.store_id`
  );
  const references = await db.query(
    `select conrelid::regclass::text as child_table,
            pg_get_constraintdef(oid) as definition
       from pg_constraint
      where confrelid in ('ozon_assemblies'::regclass, 'ozon_assembly_orders'::regclass)
      order by conrelid::regclass::text, conname`
  );
  const postingTables = await db.query(
    `select table_name, string_agg(column_name, ',' order by ordinal_position) as columns
       from information_schema.columns
      where table_schema = 'public'
        and table_name like 'ozon_%'
        and table_name in (
          select table_name from information_schema.columns
           where table_schema = 'public' and column_name = 'posting_number'
        )
      group by table_name
      order by table_name`
  );
  console.log(JSON.stringify({ assemblies: assemblies.rows, foreignOrder: foreign.rows, references: references.rows, postingTables: postingTables.rows }, null, 2));
} finally {
  await db.end();
}
