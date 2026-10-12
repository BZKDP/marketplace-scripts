import pg from "pg";

const { Pool } = pg;
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  options: process.env.DB_OPTIONS || undefined,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
});
const API = String(process.env.YANDEX_MARKET_API || "https://api.partner.market.yandex.ru").replace(/\/$/, "");
const KEY = String(process.env.YANDEX_API_KEY || "").trim();
const BUSINESS = Number(process.env.YANDEX_BUSINESS_ID);
const CAMPAIGN = Number(process.env.YANDEX_CAMPAIGN_ID);
const INTERNAL = `http://127.0.0.1:${Number(process.env.YANDEX_PORT || 3009)}`;
const COOKIE = process.env.COOKIE_NAME || "sid";

async function market(path, options = {}) {
  return fetch(`${API}${path}`, { ...options, headers: { "Api-Key": KEY, ...(options.headers || {}) } });
}

async function processingOrders() {
  const orders = [];
  let pageToken = "";
  for (let page = 0; page < 100; page += 1) {
    const params = new URLSearchParams({ limit: "50" });
    if (pageToken) params.set("pageToken", pageToken);
    const response = await market(`/v1/businesses/${BUSINESS}/orders?${params}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ campaignIds: [CAMPAIGN], statuses: ["PROCESSING"] }),
    });
    const body = await response.json();
    if (!response.ok) throw new Error(`Orders HTTP ${response.status}: ${JSON.stringify(body)}`);
    orders.push(...(body.orders || []));
    pageToken = String(body?.paging?.nextPageToken || "");
    if (!pageToken) break;
  }
  return orders;
}

async function main() {
  const orders = await processingOrders();
  const orderIds = orders.map((order) => Number(order.orderId || order.id));
  const readyIds = orders.filter((order) => order.substatus === "READY_TO_SHIP").map((order) => Number(order.orderId || order.id));
  const rows = await pool.query(
    `select o.order_id, o.local_state, o.problem_code, a.id as assembly_id, a.name, a.status,
            coalesce(u.external_login, u.external_full_name) as assigned_to
       from ym_orders o
       join ym_assemblies a on a.id = o.assembly_id
       left join users u on u.id = o.assigned_user_id
      where o.order_id = any($1::bigint[])
      order by o.order_id`,
    [orderIds]
  );
  const local = new Map(rows.rows.map((row) => [String(row.order_id), row]));
  const missingLocal = orderIds.filter((id) => !local.has(String(id)));
  const wrongAssignee = rows.rows.filter((row) => row.assigned_to !== "stag").map((row) => Number(row.order_id));
  const unavailable = rows.rows.filter((row) => row.local_state === "external_changed").map((row) => Number(row.order_id));

  const session = await pool.query(
    `select s.token from sessions s join users u on u.id = s.user_id
      where lower(coalesce(u.external_login, '')) = 'stag' and s.expires_at > now()
      order by s.expires_at desc limit 1`
  );
  if (!session.rowCount) throw new Error("No active stag session");
  const employeeResponse = await fetch(`${INTERNAL}/employee/assemblies`, {
    headers: { cookie: `${COOKIE}=${session.rows[0].token}` },
  });
  const employeeBody = await employeeResponse.json();
  if (!employeeResponse.ok) throw new Error(`Employee assemblies HTTP ${employeeResponse.status}: ${JSON.stringify(employeeBody)}`);

  const detailChecks = [];
  for (const assembly of employeeBody.assemblies || []) {
    const response = await fetch(`${INTERNAL}/employee/assemblies/${assembly.id}`, {
      headers: { cookie: `${COOKIE}=${session.rows[0].token}` },
    });
    const body = await response.json();
    detailChecks.push({
      id: assembly.id,
      name: assembly.name,
      status: assembly.status,
      http: response.status,
      orders: (body.orders || []).length,
    });
  }

  const labelChecks = [];
  for (const orderId of readyIds) {
    const response = await market(`/v2/campaigns/${CAMPAIGN}/orders/${orderId}/delivery/labels?format=A9_HORIZONTALLY`);
    const bytes = Buffer.from(await response.arrayBuffer()).length;
    labelChecks.push({ orderId, http: response.status, contentType: response.headers.get("content-type"), bytes });
  }
  console.log(JSON.stringify({
    processingOrders: orders.length,
    processingUnits: orders.reduce((sum, order) => sum + (order.items || []).reduce((inner, item) => inner + Number(item.count || 0), 0), 0),
    startedOrders: orders.filter((order) => order.substatus === "STARTED").length,
    readyOrders: readyIds.length,
    localOrders: rows.rowCount,
    missingLocal,
    wrongAssignee,
    unavailable,
    employeeAssemblies: detailChecks,
    labelChecks,
  }, null, 2));
}

main().finally(() => pool.end()).catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
