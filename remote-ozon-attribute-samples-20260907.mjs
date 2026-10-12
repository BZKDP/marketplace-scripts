import fs from "node:fs";

const raw = fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").replace(/^\uFEFF/, "");
for (const sourceLine of raw.split(/\r?\n/)) {
  const line = sourceLine.trim();
  if (!line || line.startsWith("#")) continue;
  const index = line.indexOf("=");
  if (index < 1) continue;
  let value = line.slice(index + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[line.slice(0, index).trim()] = value;
}
const audit = JSON.parse(fs.readFileSync("/tmp/audit-marketplace-chz-20260907.json", "utf8"));
const stores = [
  { id: "irina", clientId: process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID, apiKey: process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY },
  { id: "evgeny", clientId: process.env.OZON_STORE_2_CLIENT_ID, apiKey: process.env.OZON_STORE_2_API_KEY },
];
const output = [];
for (const store of stores) {
  const cards = audit.products.filter((card) => card.marketplace === "ozon" && card.storeId === store.id && card.requiresChzByCategory && !card.archived);
  const samples = [...new Map(cards.map((card) => [String(card.typeId), card])).values()];
  const response = await fetch("https://api-seller.ozon.ru/v4/product/info/attributes", {
    method: "POST",
    headers: { "Client-Id": store.clientId, "Api-Key": store.apiKey, "Content-Type": "application/json" },
    body: JSON.stringify({ filter: { product_id: samples.map((card) => String(card.productId)), visibility: "ALL" }, limit: 100, sort_dir: "ASC" }),
  });
  const text = await response.text();
  if (!response.ok) {
    output.push({ store: store.id, status: response.status, error: text.slice(0, 1000) });
    continue;
  }
  const data = JSON.parse(text);
  output.push({ store: store.id, samples, items: data?.result || data?.items || [] });
}
console.log(JSON.stringify(output, null, 2));
