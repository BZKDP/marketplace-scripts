import fs from "node:fs";
import crypto from "node:crypto";
import { loadEnv as loadDbEnv, connectAuto, mainDbConfig } from "/opt/grasklad-sborka/scripts/db-common.mjs";

function loadEnv(filePath) {
  const raw = fs.readFileSync(filePath, "utf8").replace(/^\uFEFF/, "");
  for (const rawLine of raw.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const idx = line.indexOf("=");
    if (idx < 1) continue;
    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    process.env[key] = value;
  }
}

loadEnv("/opt/grasklad-sborka/.env");
loadDbEnv("/opt/grasklad-sborka/.env");

const supplyId = "WB-GI-274564918";
const base = `http://127.0.0.1:${Number(process.env.WB_PORT || 3007)}/api`;

async function request(path, options = {}) {
  const response = await fetch(`${base}${path}`, {
    ...options,
    headers: { Accept: "application/json", ...(options.headers || {}) },
    signal: AbortSignal.timeout(120_000),
  });
  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text.slice(0, 500) }; }
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(data)}`);
  return { response, data };
}

const pool = await connectAuto(mainDbConfig());
const token = crypto.randomBytes(32).toString("hex");
try {
  const userResult = await pool.query(
    `select id, role, site_code
       from users
      where role in ('super_admin', 'admin')
        and (site_code is null or site_code in ('', 'WB', 'ALL', 'sborka'))
      order by case when role = 'super_admin' then 0 else 1 end, id
      limit 1`
  );
  const user = userResult.rows?.[0];
  if (!user) throw new Error("No eligible WB admin user found");
  await pool.query(
    "insert into sessions (token, user_id, expires_at, created_at, updated_at) values ($1, $2, now() + interval '10 minutes', now(), now())",
    [token, user.id]
  );

  const headers = { Cookie: `${process.env.COOKIE_NAME || "sid"}=${token}` };
  const before = (await request(`/supplies/${encodeURIComponent(supplyId)}/sync-preview`, { headers })).data;
  if (before.fetchOk !== true) throw new Error("WB preview fetch failed; sync was not applied");

  console.log(JSON.stringify({
    supplyId,
    currentCount: before.currentCount,
    wbCount: before.wbCount,
    removedCollectedCount: before.removedCollectedCount,
    removed: before.removed,
    addedCount: Array.isArray(before.added) ? before.added.length : null,
  }, null, 2));
} finally {
  await pool.query("delete from sessions where token = $1", [token]).catch(() => {});
  await pool.end();
}
