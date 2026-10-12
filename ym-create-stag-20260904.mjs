import pg from "pg";

const { Pool } = pg;
const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  options: process.env.DB_OPTIONS || undefined,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable"
    ? undefined
    : { rejectUnauthorized: false },
});

const baseUrl = `http://127.0.0.1:${Number(process.env.YANDEX_PORT || 3009)}`;
const cookieName = process.env.COOKIE_NAME || "sid";

async function api(path, token, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      cookie: `${cookieName}=${token}`,
      ...(options.body ? { "content-type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(`${options.method || "GET"} ${path}: HTTP ${response.status}: ${body.error || "unknown error"}`);
    error.response = body;
    throw error;
  }
  return body;
}

async function main() {
  const adminSession = await pool.query(
    `select s.token
       from sessions s
       join users u on u.id = s.user_id
      where s.expires_at > now() and u.role in ('admin', 'super_admin')
      order by s.expires_at desc
      limit 1`
  );
  if (!adminSession.rowCount) throw new Error("No active admin session");
  const adminToken = adminSession.rows[0].token;

  const stagResult = await pool.query(
    `select id, external_full_name, surname, name
       from users
      where role = 'employee' and lower(coalesce(external_login, '')) = 'stag'
      limit 1`
  );
  if (!stagResult.rowCount) throw new Error("Employee stag was not found");
  const stag = stagResult.rows[0];

  const waiting = await api("/waiting?force=1", adminToken);
  const allOrders = waiting.orders || [];
  const blocked = allOrders.filter((order) => order.hasMappingProblem || order.hasMandatoryMarking);
  if (blocked.length) {
    throw new Error(`Blocked Yandex orders detected: ${blocked.map((order) => order.id).join(", ")}`);
  }
  if (!allOrders.length) {
    console.log(JSON.stringify({ ok: true, action: "nothing_to_create", waiting: 0 }, null, 2));
    return;
  }

  const orderIds = allOrders.map((order) => Number(order.id));
  const units = allOrders.reduce(
    (sum, order) => sum + (order.items || []).reduce((inner, item) => inner + Number(item.quantity || 0), 0),
    0
  );
  const now = new Date();
  const stamp = new Intl.DateTimeFormat("ru-RU", {
    timeZone: "Europe/Moscow",
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(now).replace(",", "");
  const name = `Сборка Яндекс ${stamp} · ${orderIds.length} заказов`;

  const created = await api("/assemblies", adminToken, {
    method: "POST",
    body: JSON.stringify({
      orderIds,
      name,
      allowZeroStock: true,
      draft: true,
    }),
  });
  await api(`/assemblies/${created.id}/assignment`, adminToken, {
    method: "PATCH",
    body: JSON.stringify({ userIds: [Number(stag.id)] }),
  });
  await api(`/assemblies/${created.id}/start`, adminToken, {
    method: "POST",
    body: JSON.stringify({}),
  });

  const detail = await api(`/assemblies/${created.id}`, adminToken);
  const remaining = await api("/waiting?force=1", adminToken);
  const stagSession = await pool.query(
    `select s.token
       from sessions s
      where s.user_id = $1 and s.expires_at > now()
      order by s.expires_at desc
      limit 1`,
    [Number(stag.id)]
  );
  let employeeVisible = null;
  if (stagSession.rowCount) {
    const employeeAssemblies = await api("/employee/assemblies", stagSession.rows[0].token);
    employeeVisible = (employeeAssemblies.assemblies || []).some((assembly) => String(assembly.id) === String(created.id));
  }

  const state = await pool.query(
    `select a.id, a.name, a.status, a.allow_zero_stock, a.launched_at,
            count(distinct o.order_id)::int as orders,
            coalesce(sum(i.quantity), 0)::int as units,
            count(distinct au.user_id)::int as assigned_users,
            coalesce(array_agg(distinct o.shipment_id) filter (where o.shipment_id is not null), '{}') as shipment_ids
       from ym_assemblies a
       left join ym_orders o on o.assembly_id = a.id
       left join ym_order_items i on i.order_id = o.order_id
       left join ym_assembly_users au on au.assembly_id = a.id
      where a.id = $1
      group by a.id`,
    [created.id]
  );
  console.log(JSON.stringify({
    ok: true,
    created: state.rows[0],
    expected: { orders: orderIds.length, units, stagId: Number(stag.id) },
    detailOrders: (detail.orders || []).length,
    remainingWaiting: (remaining.orders || []).length,
    employeeVisibilityVerified: employeeVisible,
    employeeActiveSessionPresent: Boolean(stagSession.rowCount),
  }, null, 2));
}

main().finally(() => pool.end()).catch((error) => {
  console.error(error.stack || error.message || String(error));
  if (error.response) console.error(JSON.stringify(error.response));
  process.exitCode = 1;
});
