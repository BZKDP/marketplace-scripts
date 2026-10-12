import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

function loadEnv(file) {
  for (const rawLine of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
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
}

loadEnv("/opt/grasklad-sborka/.env");

const stores = [
  {
    id: process.env.OZON_STORE_1_ID || "store1",
    name: process.env.OZON_STORE_1_NAME || "store1",
    clientId: process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID,
    apiKey: process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY,
  },
  {
    id: process.env.OZON_STORE_2_ID || "store2",
    name: process.env.OZON_STORE_2_NAME || "store2",
    clientId: process.env.OZON_STORE_2_CLIENT_ID,
    apiKey: process.env.OZON_STORE_2_API_KEY,
  },
].filter((store) => store.clientId && store.apiKey);

async function listAwaiting(store) {
  const now = new Date();
  const since = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);
  const rows = [];
  let offset = 0;
  while (true) {
    const response = await fetch("https://api-seller.ozon.ru/v3/posting/fbs/list", {
      method: "POST",
      headers: {
        "Client-Id": store.clientId,
        "Api-Key": store.apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        dir: "ASC",
        filter: { since: since.toISOString(), to: now.toISOString(), status: "awaiting_deliver" },
        limit: 1000,
        offset,
        with: { analytics_data: true, barcodes: false },
      }),
    });
    if (!response.ok) throw new Error(`${store.name}: Ozon HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`);
    const payload = await response.json();
    const page = Array.isArray(payload?.result?.postings) ? payload.result.postings : [];
    rows.push(...page);
    if (payload?.result?.has_next !== true || page.length === 0) break;
    offset += 1000;
  }
  return rows;
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
  const report = [];
  for (const store of stores) {
    const apiRows = await listAwaiting(store);
    const numbers = [...new Set(apiRows.map((row) => String(row.posting_number || "").trim()).filter(Boolean))];
    const local = numbers.length
      ? await db.query(
          `select o.posting_number, o.assembly_id, o.ozon_status, o.assigned_user_id,
                  o.collected_at, o.sent_to_delivering_at,
                  a.name as assembly_name, a.access_mode, a.created_at as assembly_created_at,
                  a.store_id as assembly_store_id, a.store_name as assembly_store_name,
                  coalesce((select count(*) from ozon_employee_assembly_sessions s
                            where s.assembly_id = a.id and s.finished_at is null), 0)::int as open_sessions
             from ozon_assembly_orders o
             join ozon_assemblies a on a.id = o.assembly_id
            where o.posting_number = any($1::text[])
            order by o.posting_number, a.created_at desc`,
          [numbers]
        )
      : { rows: [] };
    const byPosting = new Map(local.rows.map((row) => [row.posting_number, row]));
    const distribution = {};
    const warehouseDistribution = {};
    let activeLocal = 0;
    let staleLocal = 0;
    let assigned = 0;
    let collected = 0;
    for (const number of numbers) {
      const row = byPosting.get(number);
      if (!row) continue;
      if (row.sent_to_delivering_at) staleLocal += 1;
      else activeLocal += 1;
      if (row.assigned_user_id) assigned += 1;
      if (row.collected_at) collected += 1;
      const key = `${row.assembly_id} | ${row.assembly_name} | ${row.access_mode} | sessions=${row.open_sessions}`;
      distribution[key] = (distribution[key] || 0) + 1;
    }
    for (const posting of apiRows) {
      const method = posting.delivery_method || {};
      const key = `${method.warehouse_id || "unknown"} | ${method.warehouse || method.name || "unknown"}`;
      warehouseDistribution[key] = (warehouseDistribution[key] || 0) + 1;
    }
    report.push({
      storeId: store.id,
      storeName: store.name,
      apiOrders: numbers.length,
      apiUnits: apiRows.reduce((sum, row) => sum + (row.products || []).reduce((n, item) => n + Number(item.quantity || 0), 0), 0),
      noLocal: numbers.filter((number) => !byPosting.has(number)).length,
      activeLocal,
      staleLocal,
      assigned,
      collected,
      distribution,
      warehouseDistribution,
      outsideSyncAssembly: numbers.map((number) => byPosting.get(number)).filter((row) => row && row.assembly_id !== `ozon-sync-awaiting-deliver-${store.id}`),
      postingNumbers: numbers,
    });
  }
  const mappings = await db.query(
    `select marketplace, store_id, location_code, marketplace_warehouse_id,
            marketplace_warehouse_name, sklad_warehouse_id, sklad_warehouse_name, active
       from site_location_warehouse_settings
      where marketplace = 'oz'
      order by store_id, location_code, marketplace_warehouse_id`
  );
  report.push({ warehouseMappings: mappings.rows });
  console.log(JSON.stringify(report, null, 2));
} finally {
  await db.end();
}
