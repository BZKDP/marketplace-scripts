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
  { id: "irina", name: "ИП Ирина", clientId: process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID, apiKey: process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY },
  { id: "evgeny", name: "ИП Евгений", clientId: process.env.OZON_STORE_2_CLIENT_ID, apiKey: process.env.OZON_STORE_2_API_KEY },
];
const result = { generatedAt: new Date().toISOString(), stores: [], products: [] };
for (const store of stores) {
  const cards = audit.products.filter((card) => card.marketplace === "ozon" && card.storeId === store.id && !card.archived);
  const byProductId = new Map(cards.map((card) => [String(card.productId), card]));
  const response = await fetch("https://api-seller.ozon.ru/v4/product/info/attributes", {
    method: "POST",
    headers: { "Client-Id": store.clientId, "Api-Key": store.apiKey, "Content-Type": "application/json" },
    body: JSON.stringify({ filter: { product_id: [...byProductId.keys()], visibility: "ALL" }, limit: 1000, sort_dir: "ASC" }),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${store.id} ${response.status}: ${text.slice(0, 1000)}`);
  const data = JSON.parse(text);
  const items = Array.isArray(data?.result) ? data.result : Array.isArray(data?.items) ? data.items : [];
  let exactBad = 0;
  let nonBad = 0;
  let missing = 0;
  for (const item of items) {
    const card = byProductId.get(String(item.id ?? item.product_id)) || {};
    const attr = (item.attributes || []).find((entry) => Number(entry.id) === 8728);
    const values = (attr?.values || []).map((value) => String(value.value || "").trim()).filter(Boolean);
    const isBad = values.some((value) => /(^|[^а-яё])бад([^а-яё]|$)/iu.test(value));
    if (!attr || !values.length) missing += 1;
    else if (isBad) exactBad += 1;
    else nonBad += 1;
    result.products.push({
      storeId: store.id,
      storeName: store.name,
      productId: item.id ?? item.product_id,
      offerId: item.offer_id || card.offerId || null,
      name: item.name || card.name || null,
      categoryPath: card.categoryPath || null,
      typeId: item.type_id || card.typeId || null,
      badAttributeId: attr?.id || null,
      badAttributeValues: values,
      isBad,
    });
  }
  result.stores.push({ storeId: store.id, storeName: store.name, activeCards: cards.length, returned: items.length, exactBad, nonBad, missing });
}
console.log(JSON.stringify(result, null, 2));
