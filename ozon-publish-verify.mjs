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

const ids = {
  irina: "ozon-1788592910741-irina-aa53b0",
  evgeny: "ozon-1788592910812-evgeny-92b047",
};
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
  const session = await db.query(
    `select s.token from sessions s join users u on u.id = s.user_id
      where s.expires_at > now() and u.role in ('super_admin', 'admin')
      order by case when u.role = 'super_admin' then 0 else 1 end, s.expires_at desc limit 1`
  );
  if (!session.rowCount) throw new Error("Нет активной административной сессии");
  const baseUrl = `http://127.0.0.1:${Number(process.env.OZ_PORT || 3008)}/${process.env.OZ_ROOT_SITE || "OZ"}/api`;
  const api = [];
  for (const [storeId, assemblyId] of Object.entries(ids)) {
    const response = await fetch(`${baseUrl}/assemblies?force=1`, {
      headers: {
        "X-Store-Id": storeId,
        Cookie: `${process.env.COOKIE_NAME || "sid"}=${encodeURIComponent(session.rows[0].token)}`,
      },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`${storeId}: HTTP ${response.status}: ${payload.error || "unknown"}`);
    const row = (payload.assemblies || []).find((assembly) => assembly.id === assemblyId);
    api.push({ storeId, visible: Boolean(row), name: row?.name, orderCount: row?.orderCount, remainingCount: row?.remainingCount });
  }
  const stats = await db.query(
    `select a.id, a.name, a.store_id, a.access_mode, a.employee_workflow_mode, a.employee_workflow_stage,
            a.stock_method_enabled, a.allow_no_scan, a.desktop_print, a.allow_stock_shortage,
            a.labels_status, a.labels_loaded, a.labels_total,
            count(distinct o.posting_number)::int as orders,
            count(distinct o.posting_number) filter (where o.assigned_user_id is not null)::int as assigned,
            count(distinct o.posting_number) filter (where o.collected_at is not null)::int as collected,
            coalesce(sum(i.quantity), 0)::int as units,
            coalesce(sum(i.scanned_qty), 0)::int as scanned_units
       from ozon_assemblies a
       left join ozon_assembly_orders o on o.assembly_id = a.id
       left join ozon_assembly_order_items i on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
      where a.id = any($1::text[])
      group by a.id
      order by a.store_id`,
    [Object.values(ids)]
  );
  console.log(JSON.stringify({ api, stats: stats.rows }, null, 2));
} finally {
  await db.end();
}
