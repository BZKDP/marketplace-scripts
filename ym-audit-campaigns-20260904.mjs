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
const CAMPAIGNS = [148851177, 148949326];

async function market(path, { method = "GET", body } = {}) {
  const response = await fetch(`${API}${path}`, {
    method,
    headers: { "Api-Key": KEY, ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  const parsed = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(`${method} ${path}: HTTP ${response.status}: ${text.slice(0, 500)}`);
  return parsed;
}

async function listOrders(campaignId) {
  const orders = [];
  let pageToken = "";
  for (let page = 0; page < 100; page += 1) {
    const params = new URLSearchParams({ limit: "50" });
    if (pageToken) params.set("pageToken", pageToken);
    const data = await market(`/v1/businesses/${BUSINESS}/orders?${params}`, {
      method: "POST",
      body: { campaignIds: [campaignId] },
    });
    orders.push(...(data.orders || []));
    pageToken = String(data?.paging?.nextPageToken || "");
    if (!pageToken) break;
  }
  return orders;
}

async function listShipments(campaignId) {
  const data = await market(`/v2/campaigns/${campaignId}/first-mile/shipments`, {
    method: "PUT",
    body: { dateFrom: "2026-07-01", dateTo: "2026-10-31", cancelledOrders: false },
  });
  const summaries = data?.result?.shipments || [];
  const details = [];
  for (const summary of summaries) {
    const full = await market(`/v2/campaigns/${campaignId}/first-mile/shipments/${summary.id}?cancelledOrders=false`);
    details.push(full?.result || summary);
  }
  return details;
}

function shipmentId(order) {
  return Number(order?.delivery?.shipment?.id || order?.delivery?.shipments?.[0]?.id || 0) || null;
}

function orderUnits(order) {
  return (order.items || []).reduce((sum, item) => sum + Number(item.count || 0), 0);
}

async function main() {
  const localResult = await pool.query("select order_id, assembly_id, local_state from ym_orders");
  const local = new Map(localResult.rows.map((row) => [String(row.order_id), row]));
  const results = [];
  for (const campaignId of CAMPAIGNS) {
    const [campaignData, orders, shipments] = await Promise.all([
      market(`/v2/campaigns/${campaignId}`),
      listOrders(campaignId),
      listShipments(campaignId),
    ]);
    const byStatus = orders.reduce((acc, order) => {
      const key = `${order.status || "UNKNOWN"}/${order.substatus || "UNKNOWN"}`;
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const active = orders.filter((order) => order.status === "PROCESSING");
    const groups = new Map();
    for (const order of active) {
      const key = String(shipmentId(order) || "none");
      const current = groups.get(key) || { shipmentId: key, orders: 0, units: 0, substatuses: {}, localOrders: 0, orderIds: [] };
      current.orders += 1;
      current.units += orderUnits(order);
      current.substatuses[order.substatus || "UNKNOWN"] = (current.substatuses[order.substatus || "UNKNOWN"] || 0) + 1;
      current.localOrders += local.has(String(order.orderId || order.id)) ? 1 : 0;
      current.orderIds.push(Number(order.orderId || order.id));
      groups.set(key, current);
    }
    const shipmentRows = shipments.map((shipment) => ({
      id: Number(shipment.id),
      status: shipment?.currentStatus?.status || shipment.status || null,
      statusDescription: shipment?.currentStatus?.description || null,
      from: shipment.planIntervalFrom || null,
      to: shipment.planIntervalTo || null,
      draftCount: shipment.draftCount ?? null,
      plannedCount: shipment.plannedCount ?? null,
      factCount: shipment.factCount ?? null,
      activeOrderIds: shipment.orderIds || [],
      availableActions: shipment.availableActions || [],
    }));
    results.push({
      campaignId,
      campaign: {
        domain: campaignData?.campaign?.domain || campaignData?.result?.domain || null,
        placementType: campaignData?.campaign?.placementType || campaignData?.result?.placementType || null,
        warehouse: campaignData?.campaign?.warehouse || campaignData?.result?.warehouse || null,
      },
      fetchedOrders: orders.length,
      orderCampaignIds: [...new Set(orders.map((order) => Number(order.campaignId)).filter(Number.isFinite))],
      sampleOrderKeys: Object.keys(orders[0] || {}).sort(),
      byStatus,
      activeOrders: active.length,
      activeUnits: active.reduce((sum, order) => sum + orderUnits(order), 0),
      activeLocalOrders: active.filter((order) => local.has(String(order.orderId || order.id))).length,
      activeWithoutLocalOrders: active.filter((order) => !local.has(String(order.orderId || order.id))).length,
      activeByShipment: [...groups.values()].sort((a, b) => String(a.shipmentId).localeCompare(String(b.shipmentId))),
      shipments: shipmentRows.sort((a, b) => String(a.from || "").localeCompare(String(b.from || ""))),
    });
  }
  console.log(JSON.stringify({ results }, null, 2));
}

main().finally(() => pool.end()).catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
