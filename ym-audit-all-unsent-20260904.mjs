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

const API = String(process.env.YANDEX_MARKET_API || "https://api.partner.market.yandex.ru").replace(/\/$/, "");
const API_KEY = String(process.env.YANDEX_API_KEY || "").trim();
const BUSINESS_ID = Number(process.env.YANDEX_BUSINESS_ID);
const CAMPAIGN_ID = Number(process.env.YANDEX_CAMPAIGN_ID);

async function market(path, { method = "GET", body } = {}) {
  const response = await fetch(`${API}${path}`, {
    method,
    headers: { "Api-Key": API_KEY, ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${method} ${path}: HTTP ${response.status}: ${text.slice(0, 500)}`);
  return text ? JSON.parse(text) : {};
}

async function listAllOrders() {
  const orders = [];
  let pageToken = "";
  for (let page = 0; page < 100; page += 1) {
    const params = new URLSearchParams({ limit: "50" });
    if (pageToken) params.set("pageToken", pageToken);
    const data = await market(`/v1/businesses/${BUSINESS_ID}/orders?${params}`, {
      method: "POST",
      body: { campaignIds: [CAMPAIGN_ID] },
    });
    orders.push(...(data.orders || []));
    pageToken = String(data?.paging?.nextPageToken || "");
    if (!pageToken) break;
  }
  return orders;
}

function day(value) {
  return new Date(value).toISOString().slice(0, 10);
}

async function listShipments() {
  const statuses = [
    "OUTBOUND_CREATED", "OUTBOUND_READY_FOR_CONFIRMATION", "OUTBOUND_CONFIRMED",
    "OUTBOUND_SIGNED", "ACCEPTED", "ACCEPTED_WITH_DISCREPANCIES", "FINISHED", "ERROR",
  ];
  const data = await market(`/v2/campaigns/${CAMPAIGN_ID}/first-mile/shipments`, {
    method: "PUT",
    body: {
      dateFrom: day(Date.now() - 30 * 86400000),
      dateTo: day(Date.now() + 30 * 86400000),
      statuses,
      cancelledOrders: false,
    },
  });
  return data?.result?.shipments || [];
}

async function main() {
  const [orders, shipments, localResult] = await Promise.all([
    listAllOrders(),
    listShipments(),
    pool.query(
      `select o.order_id, o.shipment_id, o.local_state, o.problem_code, o.last_error, o.assigned_user_id,
              a.id as assembly_id, a.name as assembly_name, a.status as assembly_status,
              coalesce(u.external_login, u.external_full_name, concat_ws(' ', u.surname, u.name)) as assigned_to
         from ym_orders o
         join ym_assemblies a on a.id = o.assembly_id
         left join users u on u.id = o.assigned_user_id`
    ),
  ]);
  const local = new Map(localResult.rows.map((row) => [String(row.order_id), row]));
  const byStatus = {};
  const actionable = [];
  for (const order of orders) {
    const statusKey = `${order.status || "UNKNOWN"}/${order.substatus || "UNKNOWN"}`;
    byStatus[statusKey] = (byStatus[statusKey] || 0) + 1;
    if (order.status !== "PROCESSING") continue;
    const shipment = order?.delivery?.shipment || order?.delivery?.shipments?.[0] || {};
    const localRow = local.get(String(order.orderId || order.id)) || null;
    actionable.push({
      orderId: Number(order.orderId || order.id),
      substatus: order.substatus || null,
      creationDate: order.creationDate || null,
      updateDate: order.updateDate || null,
      shipmentId: shipment.id == null ? null : Number(shipment.id),
      shipmentDate: shipment.shipmentDate || order?.delivery?.shipments?.[0]?.shipmentDate || null,
      units: (order.items || []).reduce((sum, item) => sum + Number(item.count || 0), 0),
      local: localRow ? {
        assemblyId: localRow.assembly_id,
        assemblyName: localRow.assembly_name,
        assemblyStatus: localRow.assembly_status,
        localState: localRow.local_state,
        problemCode: localRow.problem_code,
        lastError: localRow.last_error,
        assignedTo: localRow.assigned_to,
      } : null,
    });
  }
  actionable.sort((a, b) => String(a.shipmentDate || a.creationDate || "").localeCompare(String(b.shipmentDate || b.creationDate || "")));
  const normalizedShipments = shipments.map((shipment) => ({
    id: Number(shipment.id),
    status: shipment?.currentStatus?.status || shipment.status || null,
    statusUpdatedAt: shipment?.currentStatus?.updateTime || null,
    planIntervalFrom: shipment.planIntervalFrom || null,
    planIntervalTo: shipment.planIntervalTo || null,
    availableActions: shipment.availableActions || [],
    ordersCount: shipment.ordersCount ?? shipment.orderCount ?? null,
  })).sort((a, b) => String(a.planIntervalFrom || "").localeCompare(String(b.planIntervalFrom || "")));
  const shipmentDetails = [];
  for (const shipment of normalizedShipments) {
    const data = await market(`/v2/campaigns/${CAMPAIGN_ID}/first-mile/shipments/${shipment.id}?cancelledOrders=false`);
    const detail = data?.result || {};
    shipmentDetails.push({
      id: Number(detail.id || shipment.id),
      status: detail?.currentStatus?.status || null,
      statusDescription: detail?.currentStatus?.description || null,
      planIntervalFrom: detail.planIntervalFrom || null,
      planIntervalTo: detail.planIntervalTo || null,
      availableActions: detail.availableActions || [],
      orderIds: detail.orderIds || [],
      draftCount: detail.draftCount ?? null,
      plannedCount: detail.plannedCount ?? null,
      factCount: detail.factCount ?? null,
      palletsCount: detail.palletsCount || null,
    });
  }
  console.log(JSON.stringify({
    fetchedOrders: orders.length,
    byStatus,
    processingCount: actionable.length,
    processingUnits: actionable.reduce((sum, order) => sum + order.units, 0),
    processingBySubstatus: actionable.reduce((acc, order) => {
      acc[order.substatus || "UNKNOWN"] = (acc[order.substatus || "UNKNOWN"] || 0) + 1;
      return acc;
    }, {}),
    processingWithoutLocalAssembly: actionable.filter((order) => !order.local).length,
    processingWithLocalAssembly: actionable.filter((order) => order.local).length,
    actionable,
    shipments: normalizedShipments,
    shipmentDetails,
  }, null, 2));
}

main().finally(() => pool.end()).catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
