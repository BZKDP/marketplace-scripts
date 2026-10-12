import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

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

const stores = [
  process.env.OZON_STORE_1_ID || "irina",
  process.env.OZON_STORE_2_ID || "evgeny",
];

try {
  const session = await db.query(
    `select s.token
       from sessions s
       join users u on u.id = s.user_id
      where s.expires_at > now() and u.role in ('super_admin', 'admin')
      order by case when u.role = 'super_admin' then 0 else 1 end, s.expires_at desc
      limit 1`
  );
  if (!session.rowCount) throw new Error("Нет активной административной сессии для запуска штатной синхронизации");

  const baseUrl = `http://127.0.0.1:${Number(process.env.OZ_PORT || 3008)}/${process.env.OZ_ROOT_SITE || "OZ"}/api`;
  const results = [];
  for (const storeId of stores) {
    const response = await fetch(`${baseUrl}/assemblies/sync-awaiting-deliver`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Store-Id": storeId,
        Cookie: `${process.env.COOKIE_NAME || "sid"}=${encodeURIComponent(session.rows[0].token)}`,
      },
      body: JSON.stringify({ loadLabels: true }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`${storeId}: HTTP ${response.status}: ${payload.error || "unknown error"}`);
    results.push(payload);
  }

  console.log(JSON.stringify(results, null, 2));
} finally {
  await db.end();
}
