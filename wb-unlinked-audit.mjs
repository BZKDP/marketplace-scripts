import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/WB/node_modules/pg/lib/index.js";

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

const { resolveWarehouseProductMeta } = await import("/opt/grasklad-sborka/apps/WB/server/services/sklad-movements.mjs");
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
  const active = await db.query(
    `select es.id as session_id, es.supply_id, ss.supply_name, ss.store_id, ss.store_name,
            es.user_id, concat_ws(' ', u.surname, u.name) as employee,
            es.status, es.picking_status, es.picking_required_qty, es.picking_collected_qty,
            es.assigned_at, es.started_at, es.finished_at
       from employee_supply_sessions es
       join supply_settings ss on ss.supply_id = es.supply_id
       left join users u on u.id = es.user_id
      where es.finished_at is null
        and (es.status in ('assigned', 'started') or es.picking_status in ('pending', 'started'))
      order by ss.store_id, es.assigned_at desc`
  );

  const picks = await db.query(
    `select pi.id, pi.session_id, pi.supply_id, ss.supply_name, ss.store_id, ss.store_name,
            pi.user_id, concat_ws(' ', u.surname, u.name) as employee,
            pi.item_key, pi.article, pi.barcode, pi.product_name, pi.nm_id,
            pi.required_qty, pi.picked_qty,
            greatest(pi.required_qty - pi.picked_qty, 0)::int as remaining_qty,
            pi.bypass_without_scan_allowed
       from employee_supply_pick_items pi
       join employee_supply_sessions es on es.id = pi.session_id
       join supply_settings ss on ss.supply_id = pi.supply_id
       left join users u on u.id = pi.user_id
      where es.finished_at is null
        and pi.required_qty > pi.picked_qty
      order by ss.store_id, pi.supply_id, pi.product_name, pi.article`
  );

  const tokens = [...new Set(picks.rows.flatMap((row) => [row.barcode, row.article]).map((value) => String(value || "").trim()).filter(Boolean))];
  const meta = await resolveWarehouseProductMeta(tokens);
  const unresolved = [];
  for (const row of picks.rows) {
    const candidates = [row.barcode, row.article].map((value) => String(value || "").replace(/\s+/g, "").trim().toUpperCase()).filter(Boolean);
    const match = candidates.map((token) => meta.get(token)).find(Boolean) || null;
    if (!match) unresolved.push(row);
  }

  const openIssues = await db.query(
    `select i.id, i.supply_id, ss.store_id, ss.store_name, i.issue_stage, i.error_message,
            i.article, i.product_name, i.nm_id, i.scanned_barcode, i.marketplace_article,
            i.warehouse_product_id, i.warehouse_article, i.warehouse_product_name,
            i.mapping_barcode, i.remaining_quantity, i.created_at
       from employee_scan_issues i
       left join supply_settings ss on ss.supply_id = i.supply_id
      where i.status = 'open'
      order by i.created_at desc`
  );

  console.log(JSON.stringify({ activeSessions: active.rows, remainingPickItems: picks.rows, unresolvedPickItems: unresolved, openIssues: openIssues.rows }, null, 2));
} finally {
  await db.end();
}
