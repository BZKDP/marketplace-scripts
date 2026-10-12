const API = String(process.env.YANDEX_MARKET_API || "https://api.partner.market.yandex.ru").replace(/\/$/, "");
const KEY = String(process.env.YANDEX_API_KEY || "").trim();
const BUSINESS = Number(process.env.YANDEX_BUSINESS_ID);
const CONFIGURED_CAMPAIGN = Number(process.env.YANDEX_CAMPAIGN_ID);
const SCREENSHOT_SHIPMENTS = [85964187, 84214900, 86076552, 88420631, 88448808];

async function market(path, { method = "GET", body } = {}) {
  const response = await fetch(`${API}${path}`, {
    method,
    headers: { "Api-Key": KEY, ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  let parsed;
  try { parsed = text ? JSON.parse(text) : {}; } catch { parsed = { raw: text.slice(0, 500) }; }
  if (!response.ok) {
    const error = new Error(`${method} ${path}: HTTP ${response.status}`);
    error.details = parsed;
    throw error;
  }
  return parsed;
}

async function campaigns() {
  const data = await market("/v2/campaigns?limit=50");
  return data.campaigns || data.result?.campaigns || [];
}

function campaignSummary(campaign) {
  return {
    id: Number(campaign.id),
    domain: campaign.domain || null,
    businessId: Number(campaign.business?.id || campaign.businessId || 0) || null,
    businessName: campaign.business?.name || null,
    placementType: campaign.placementType || campaign.placement?.type || null,
    clientId: campaign.clientId || null,
  };
}

async function searchShipments(campaignId, body) {
  try {
    const data = await market(`/v2/campaigns/${campaignId}/first-mile/shipments`, { method: "PUT", body });
    return { ok: true, shipments: data?.result?.shipments || [] };
  } catch (error) {
    return { ok: false, error: error.details || error.message, shipments: [] };
  }
}

async function shipmentById(campaignId, shipmentId) {
  try {
    const data = await market(`/v2/campaigns/${campaignId}/first-mile/shipments/${shipmentId}?cancelledOrders=false`);
    const value = data?.result || {};
    return {
      ok: true,
      id: Number(value.id || shipmentId),
      status: value?.currentStatus?.status || value.status || null,
      statusDescription: value?.currentStatus?.description || null,
      planIntervalFrom: value.planIntervalFrom || null,
      planIntervalTo: value.planIntervalTo || null,
      orderIds: value.orderIds || [],
      draftCount: value.draftCount ?? null,
      plannedCount: value.plannedCount ?? null,
      factCount: value.factCount ?? null,
      availableActions: value.availableActions || [],
    };
  } catch (error) {
    return { ok: false, id: shipmentId, error: error.details || error.message };
  }
}

async function main() {
  const allCampaigns = await campaigns();
  const summaries = allCampaigns.map(campaignSummary);
  const fbs = summaries.filter((campaign) => !campaign.businessId || campaign.businessId === BUSINESS);
  const ids = [...new Set([CONFIGURED_CAMPAIGN, ...fbs.map((campaign) => campaign.id)].filter(Number.isFinite))];
  const results = [];
  for (const campaignId of ids) {
    const [iso, ru] = await Promise.all([
      searchShipments(campaignId, { dateFrom: "2026-08-01", dateTo: "2026-09-30", cancelledOrders: false }),
      searchShipments(campaignId, { dateFrom: "01-08-2026", dateTo: "30-09-2026", cancelledOrders: false }),
    ]);
    const direct = [];
    for (const shipmentId of SCREENSHOT_SHIPMENTS) direct.push(await shipmentById(campaignId, shipmentId));
    results.push({
      campaignId,
      campaign: summaries.find((campaign) => campaign.id === campaignId) || null,
      isoSearch: {
        ok: iso.ok,
        count: iso.shipments.length,
        ids: iso.shipments.map((shipment) => Number(shipment.id)),
        error: iso.error || null,
      },
      ruSearch: {
        ok: ru.ok,
        count: ru.shipments.length,
        ids: ru.shipments.map((shipment) => Number(shipment.id)),
        error: ru.error || null,
      },
      screenshotShipments: direct,
    });
  }
  console.log(JSON.stringify({ configuredCampaign: CONFIGURED_CAMPAIGN, configuredBusiness: BUSINESS, campaigns: summaries, results }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  if (error.details) console.error(JSON.stringify(error.details));
  process.exitCode = 1;
});
