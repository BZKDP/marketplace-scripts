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

const supplies = ["WB-GI-273800643", "WB-GI-273968512"];
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
    `select s.token
       from sessions s
       join users u on u.id = s.user_id
      where s.expires_at > now() and u.role in ('super_admin', 'admin')
      order by case when u.role = 'super_admin' then 0 else 1 end, s.expires_at desc
      limit 1`
  );
  if (!session.rowCount) throw new Error("Нет активной административной сессии");
  const base = `http://127.0.0.1:${Number(process.env.WB_PORT || 3007)}`;
  const started = [];
  for (const supplyId of supplies) {
    const response = await fetch(`${base}/api/supplies/${encodeURIComponent(supplyId)}/sync`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: `${process.env.COOKIE_NAME || "sid"}=${encodeURIComponent(session.rows[0].token)}`,
      },
      body: JSON.stringify({}),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`${supplyId}: HTTP ${response.status}: ${payload.error || "unknown error"}`);
    started.push({ supplyId, started: payload.started === true });
  }

  let state = [];
  for (let attempt = 0; attempt < 90; attempt += 1) {
    const status = await db.query(
      `select supply_id, manual_sync_status, manual_sync_message, manual_sync_error,
              manual_sync_removed_orders, manual_sync_released_orders,
              manual_sync_reconciled_sessions, manual_sync_finished_at
         from supply_settings
        where supply_id = any($1::text[])
        order by supply_id`,
      [supplies]
    );
    state = status.rows;
    if (state.length === supplies.length && state.every((row) => String(row.manual_sync_status || "") !== "running")) break;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  console.log(JSON.stringify({ started, state }, null, 2));
} finally {
  await db.end();
}
