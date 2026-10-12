import fs from "node:fs";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.replace(/^\uFEFF/, "").trim();
  if (!line || line.startsWith("#")) continue;
  const separator = line.indexOf("=");
  if (separator < 1) continue;
  const key = line.slice(0, separator).trim();
  let value = line.slice(separator + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[key] ??= value;
}

const rows = JSON.parse(fs.readFileSync("/tmp/warehouse-link-20260905-0914.json", "utf8"));
const config = await import("/opt/grasklad-sborka/apps/OZ/server/config.mjs");
const stores = new Map([
  ["Ирина", { id: config.OZON_STORE_1_ID, clientId: config.OZON_STORE_1_CLIENT_ID, apiKey: config.OZON_STORE_1_API_KEY }],
  ["Евгений", { id: config.OZON_STORE_2_ID, clientId: config.OZON_STORE_2_CLIENT_ID, apiKey: config.OZON_STORE_2_API_KEY }],
]);

async function request(store, offerIds) {
  const response = await fetch(`${config.OZON_API_BASE}/v3/product/info/list`, {
    method: "POST",
    headers: {
      "Client-Id": store.clientId,
      "Api-Key": store.apiKey,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({ offer_id: offerIds }),
  });
  if (!response.ok) throw new Error(`Ozon API ${response.status}: ${(await response.text()).slice(0, 300)}`);
  const data = await response.json();
  return Array.isArray(data?.items) ? data.items : Array.isArray(data?.result?.items) ? data.result.items : [];
}

const result = [];
for (const [cabinet, store] of stores) {
  const targetRows = rows.filter((row) => row["Платформа"] === "ozon" && row["Кабинет"] === cabinet);
  for (let offset = 0; offset < targetRows.length; offset += 100) {
    const batch = targetRows.slice(offset, offset + 100);
    const items = await request(store, batch.map((row) => String(row["Арт. продавца"] || "").trim()).filter(Boolean));
    const byOffer = new Map(items.map((item) => [String(item.offer_id || "").trim().toUpperCase(), item]));
    for (const row of batch) {
      const offerId = String(row["Арт. продавца"] || "").trim();
      const item = byOffer.get(offerId.toUpperCase()) || null;
      result.push({
        cabinet,
        offerId,
        spreadsheetMarketplaceId: String(row["ID МП"] ?? ""),
        found: Boolean(item),
        productId: item?.id ?? item?.product_id ?? null,
        sku: item?.sku ?? null,
        barcodes: Array.isArray(item?.barcodes) ? item.barcodes : [],
        name: item?.name || null,
      });
    }
  }
}

console.log(JSON.stringify({ count: result.length, result }, null, 2));
