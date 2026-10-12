import pg from "/opt/grasklad-sborka/apps/WB/node_modules/pg/lib/index.js";
import fs from "node:fs";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.trim();
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

const {
  DB_HOST,
  DB_PORT,
  DB_USER,
  DB_PASSWORD,
  DB_NAME,
  DB_SSL,
} = await import("/opt/grasklad-sborka/apps/WB/server/config.mjs");

const { Pool } = pg;

async function openPool() {
  const sslModes = DB_SSL === "require" ? [true] : DB_SSL === "disable" ? [false] : [true, false];
  let lastError;
  for (const useSsl of sslModes) {
    const pool = new Pool({
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USER,
      password: DB_PASSWORD,
      database: DB_NAME,
      ssl: useSsl ? { rejectUnauthorized: false } : undefined,
      max: 1,
      connectionTimeoutMillis: 8000,
    });
    try {
      await pool.query("select 1");
      return pool;
    } catch (error) {
      lastError = error;
      await pool.end().catch(() => {});
    }
  }
  throw lastError;
}

const pool = await openPool();
try {
  const exactish = await pool.query(
    `select o.supply_id, s.supply_name, s.store_id, s.store_name,
            o.wb_order_id, o.order_created_at, o.article, o.product_name, o.nm_id,
            o.barcode, o.sticker_barcode, o.label_scan_barcode,
            o.sticker_url, o.sticker_s3_key, o.sticker_loaded_at,
            o.assigned_at, o.collected_at
       from supply_orders o
       left join supply_settings s on s.supply_id = o.supply_id
      where coalesce(o.sticker_barcode, '') ilike any(array['%579352%', '%2482%'])
         or coalesce(o.label_scan_barcode, '') ilike any(array['%579352%', '%2482%'])
         or cast(o.wb_order_id as text) ilike any(array['%579352%', '%2482%'])
      order by coalesce(o.collected_at, o.sticker_loaded_at, o.order_created_at) desc
      limit 100`
  );
  const recent = await pool.query(
    `select o.supply_id, s.supply_name, s.store_id, s.store_name,
            o.wb_order_id, o.order_created_at, o.article, o.product_name, o.nm_id,
            o.barcode, o.sticker_barcode, o.label_scan_barcode,
            o.sticker_url, o.sticker_s3_key, o.sticker_loaded_at,
            o.assigned_at, o.collected_at
       from supply_orders o
       left join supply_settings s on s.supply_id = o.supply_id
      where greatest(
              coalesce(o.collected_at, '-infinity'::timestamptz),
              coalesce(o.sticker_loaded_at, '-infinity'::timestamptz),
              coalesce(o.assigned_at, '-infinity'::timestamptz),
              coalesce(o.order_created_at, '-infinity'::timestamptz)
            ) >= timestamptz '2026-09-01 00:00:00+03'
        and o.sticker_barcode is not null
      order by coalesce(o.collected_at, o.sticker_loaded_at, o.order_created_at) desc
      limit 2500`
  );
  console.log(JSON.stringify({ exactish: exactish.rows, recent: recent.rows }, null, 2));
} finally {
  await pool.end();
}
