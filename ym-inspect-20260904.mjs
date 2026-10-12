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

function summarizeOrder(order) {
  return {
    id: Number(order.id),
    shipmentId: order.shipmentId == null ? null : Number(order.shipmentId),
    status: order.status,
    substatus: order.substatus,
    items: (order.items || []).map((item) => ({
      offerId: item.offerId,
      name: item.name,
      quantity: Number(item.quantity || 0),
      warehouseProductId: item.warehouseProductId == null ? null : Number(item.warehouseProductId),
      barcodeCount: Array.isArray(item.barcodes) ? item.barcodes.length : 0,
      requiredInstanceTypes: item.requiredInstanceTypes || [],
      available: item.available == null ? null : Number(item.available),
    })),
    hasMappingProblem: Boolean(order.hasMappingProblem),
    hasMandatoryMarking: Boolean(order.hasMandatoryMarking),
    mappingProblems: order.mappingProblems || [],
  };
}

async function main() {
  const stagResult = await pool.query(
    `select id, surname, name, role, external_full_name, location_code
       from users
      where lower(coalesce(external_login, '')) = 'stag'
      limit 1`
  );
  const adminSession = await pool.query(
    `select s.token
       from sessions s
       join users u on u.id = s.user_id
      where s.expires_at > now() and u.role in ('admin', 'super_admin')
      order by s.expires_at desc
      limit 1`
  );
  if (!adminSession.rowCount) throw new Error("No active admin session");

  const baseUrl = `http://127.0.0.1:${Number(process.env.YANDEX_PORT || 3009)}`;
  const response = await fetch(`${baseUrl}/waiting?force=1`, {
    headers: { cookie: `${process.env.COOKIE_NAME || "sid"}=${adminSession.rows[0].token}` },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`Waiting HTTP ${response.status}: ${body.error || "unknown error"}`);

  const orders = (body.orders || []).map(summarizeOrder);
  const stockByOffer = new Map();
  for (const order of orders) {
    for (const item of order.items) {
      const key = String(item.offerId || "");
      const current = stockByOffer.get(key) || { offerId: key, name: item.name, required: 0, available: item.available };
      current.required += item.quantity;
      if (item.available != null) current.available = item.available;
      stockByOffer.set(key, current);
    }
  }
  const stockProblems = [...stockByOffer.values()].filter((item) => item.available != null && item.required > item.available);
  const existing = await pool.query(
    `select a.id, a.name, a.status, a.allow_zero_stock, a.created_at, a.launched_at,
            count(distinct o.order_id)::int as orders,
            coalesce(sum(i.quantity), 0)::int as units,
            coalesce(array_agg(distinct coalesce(u.external_login, u.external_full_name, concat_ws(' ', u.surname, u.name)))
              filter (where u.id is not null), '{}') as users
       from ym_assemblies a
       left join ym_orders o on o.assembly_id = a.id
       left join ym_order_items i on i.order_id = o.order_id
       left join ym_assembly_users au on au.assembly_id = a.id
       left join users u on u.id = au.user_id
      group by a.id
      order by a.created_at desc
      limit 15`
  );

  const eligible = orders.filter((order) => !order.hasMappingProblem && !order.hasMandatoryMarking);
  const result = {
    stag: stagResult.rows[0] || null,
    waiting: {
      count: orders.length,
      units: orders.reduce((sum, order) => sum + order.items.reduce((inner, item) => inner + item.quantity, 0), 0),
      eligibleCount: eligible.length,
      eligibleUnits: eligible.reduce((sum, order) => sum + order.items.reduce((inner, item) => inner + item.quantity, 0), 0),
      mappingProblemIds: orders.filter((order) => order.hasMappingProblem).map((order) => order.id),
      mandatoryMarkingIds: orders.filter((order) => order.hasMandatoryMarking).map((order) => order.id),
      shipmentGroups: Object.entries(eligible.reduce((acc, order) => {
        const key = String(order.shipmentId ?? "none");
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {})).map(([shipmentId, count]) => ({ shipmentId, count })),
      stockProblems,
      error: body.error || "",
      cachedAt: body.cachedAt || null,
    },
    problemOrders: orders.filter((order) => order.hasMappingProblem || order.hasMandatoryMarking),
    existing: existing.rows,
  };
  console.log(JSON.stringify(result, null, 2));
}

main().finally(() => pool.end()).catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
