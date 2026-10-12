const API = String(process.env.YANDEX_MARKET_API || "https://api.partner.market.yandex.ru").replace(/\/$/, "");
const response = await fetch(`${API}/v1/businesses/${Number(process.env.YANDEX_BUSINESS_ID)}/orders?limit=1`, {
  method: "POST",
  headers: { "Api-Key": String(process.env.YANDEX_API_KEY || "").trim(), "Content-Type": "application/json" },
  body: JSON.stringify({ campaignIds: [148949326], statuses: ["PROCESSING"] }),
});
const body = await response.json();
const order = body.orders?.[0] || {};
console.log(JSON.stringify({
  http: response.status,
  keys: Object.keys(order).sort(),
  orderId: order.orderId || order.id || null,
  campaignId: order.campaignId || null,
  substatus: order.substatus || null,
  shipmentId: order?.delivery?.shipment?.id || order?.delivery?.shipments?.[0]?.id || null,
}, null, 2));
