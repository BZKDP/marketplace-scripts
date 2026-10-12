import fs from "node:fs";
import { connectAuto, loadEnv, skladDbConfig } from "/opt/grasklad-sborka/scripts/db-common.mjs";

loadEnv("/opt/grasklad-sborka/.env");
loadEnv("/opt/grasklad-sborka/.env.yandex");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const text = (value) => String(value ?? "").trim();
const token = (value) => text(value).replace(/\s+/g, "").toUpperCase();
const uniq = (values) => [...new Set(values.filter(Boolean))];

async function requestJson(url, { method = "GET", headers = {}, body, attempts = 4 } = {}) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        method,
        headers: { Accept: "application/json", ...(body === undefined ? {} : { "Content-Type": "application/json" }), ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(30_000),
      });
      const raw = await response.text();
      const data = raw ? JSON.parse(raw) : null;
      if (!response.ok) throw new Error(`${response.status}: ${raw.slice(0, 500)}`);
      return data;
    } catch (error) {
      lastError = error;
      if (attempt < attempts) await sleep(750 * attempt);
    }
  }
  throw lastError;
}

function batches(values, size) {
  const result = [];
  for (let index = 0; index < values.length; index += size) result.push(values.slice(index, index + size));
  return result;
}

async function fetchOzon(store) {
  const listByVisibility = async (visibility) => {
    const listed = [];
    let lastId = "";
    for (let page = 0; page < 1000; page += 1) {
      const data = await requestJson(`${store.base}/v3/product/list`, {
        method: "POST",
        headers: store.headers,
        body: { filter: { visibility }, last_id: lastId, limit: 1000 },
      });
      const result = data?.result || {};
      const items = Array.isArray(result.items) ? result.items : [];
      listed.push(...items);
      const next = text(result.last_id);
      if (!items.length || !next || next === lastId) break;
      lastId = next;
    }
    return listed;
  };
  const [listed, visible] = await Promise.all([listByVisibility("ALL"), listByVisibility("VISIBLE")]);
  const visibleIds = new Set(visible.map((item) => String(item.product_id ?? item.id)));

  const details = [];
  for (const batch of batches(listed.map((item) => String(item.product_id || item.id)).filter(Boolean), 100)) {
    const data = await requestJson(`${store.base}/v3/product/info/list`, {
      method: "POST",
      headers: store.headers,
      body: { product_id: batch },
    });
    details.push(...(data?.items || data?.result?.items || []));
    await sleep(80);
  }

  return details.map((item) => ({
    marketplace: "Ozon",
    store: store.name,
    cardId: String(item.id ?? item.product_id ?? ""),
    article: text(item.offer_id),
    name: text(item.name),
    barcodes: uniq((item.barcodes || []).map(token)),
    status: text(item?.statuses?.status || item?.status),
    archived: Boolean(item.is_archived),
    active: !item.is_archived && visibleIds.has(String(item.id ?? item.product_id)),
  }));
}

async function fetchWbCards(store) {
  const cards = [];
  let updatedAt;
  let nmID;
  for (let page = 0; page < 2000; page += 1) {
    const cursor = { limit: 100 };
    if (updatedAt) cursor.updatedAt = updatedAt;
    if (nmID) cursor.nmID = nmID;
    const data = await requestJson("https://content-api.wildberries.ru/content/v2/get/cards/list", {
      method: "POST",
      headers: { Authorization: store.token },
      body: { settings: { cursor, sort: { ascending: true }, filter: { withPhoto: -1 } } },
    });
    const batch = Array.isArray(data?.cards) ? data.cards : [];
    cards.push(...batch);
    const total = Number(data?.cursor?.total ?? batch.length);
    const nextUpdatedAt = data?.cursor?.updatedAt;
    const nextNmID = data?.cursor?.nmID;
    if (!batch.length || total < 100 || (nextUpdatedAt === updatedAt && nextNmID === nmID)) break;
    updatedAt = nextUpdatedAt;
    nmID = nextNmID;
    await sleep(220);
  }
  const warehouses = await requestJson("https://marketplace-api.wildberries.ru/api/v3/warehouses", {
    headers: { Authorization: `Bearer ${store.token}` },
  });
  const warehouseIds = (Array.isArray(warehouses) ? warehouses : [])
    .filter((warehouse) => !warehouse.isDeleting)
    .map((warehouse) => Number(warehouse.id))
    .filter(Number.isFinite);
  const chrtIds = uniq(cards.flatMap((card) => (card.sizes || []).map((size) => Number(size.chrtID ?? size.chrtId)).filter(Number.isFinite)));
  const stockByChrtId = new Map();
  for (const warehouseId of warehouseIds) {
    for (const batch of batches(chrtIds, 1000)) {
      const data = await requestJson(`https://marketplace-api.wildberries.ru/api/v3/stocks/${warehouseId}`, {
        method: "POST",
        headers: { Authorization: `Bearer ${store.token}` },
        body: { chrtIds: batch },
      });
      for (const stock of data?.stocks || []) {
        const id = Number(stock.chrtId ?? stock.chrtID);
        stockByChrtId.set(id, (stockByChrtId.get(id) || 0) + Number(stock.amount || 0));
      }
      await sleep(220);
    }
  }
  return cards.map((card) => {
    const cardChrtIds = (card.sizes || []).map((size) => Number(size.chrtID ?? size.chrtId)).filter(Number.isFinite);
    const stock = cardChrtIds.reduce((sum, id) => sum + (stockByChrtId.get(id) || 0), 0);
    return {
    marketplace: "WB",
    store: store.name,
    cardId: String(card.nmID ?? card.nmId ?? ""),
    article: text(card.vendorCode),
    name: text(card.title),
    barcodes: uniq((card.sizes || []).flatMap((size) => size.skus || []).map(token)),
    status: stock > 0 ? "in_stock" : "zero_stock",
    stock,
    archived: false,
    active: stock > 0,
    };
  });
}

function parseBarcodeList(row) {
  const values = [row.production_barcode];
  values.push(...text(row.barcode_list).split(/[;,\s]+/));
  const json = Array.isArray(row.marketplace_barcodes_json) ? row.marketplace_barcodes_json : [];
  for (const entry of json) values.push(typeof entry === "string" ? entry : entry?.value);
  return uniq(values.map(token));
}

function buildWarehouseIndex(products) {
  const exactArticle = new Map();
  const barcode = new Map();
  const add = (map, key, product) => {
    if (!key) return;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(product);
  };
  for (const product of products) {
    add(exactArticle, token(product.article), product);
    add(exactArticle, token(product.code), product);
    for (const value of parseBarcodeList(product)) add(barcode, value, product);
  }
  return { exactArticle, barcode };
}

function uniqueCandidate(candidates) {
  const unique = [...new Map((candidates || []).map((item) => [Number(item.id), item])).values()];
  return unique.length === 1 ? unique[0] : null;
}

function resolveCard(card, index) {
  const byArticle = uniqueCandidate(index.exactArticle.get(token(card.article)));
  if (byArticle) return { product: byArticle, method: "article" };
  const barcodeCandidates = card.barcodes.flatMap((value) => index.barcode.get(value) || []);
  const byBarcode = uniqueCandidate(barcodeCandidates);
  if (byBarcode) return { product: byBarcode, method: "barcode" };
  return { product: null, method: null };
}

function expandPhysical(product, productById, componentsByBundle, multiplier = 1, trail = []) {
  const id = Number(product?.id);
  if (!id || trail.includes(id)) throw new Error(`Cyclic/invalid bundle ${trail.join(" -> ")} -> ${id}`);
  if (text(product.entity_type).toLowerCase() !== "bundle") {
    return [{ product, quantity: multiplier }];
  }
  const components = componentsByBundle.get(id) || [];
  if (!components.length) throw new Error(`Bundle ${product.article || product.code || id} has no components`);
  return components.flatMap((entry) => {
    const child = productById.get(Number(entry.component_id));
    if (!child) throw new Error(`Bundle ${id}: component ${entry.component_id} is missing`);
    return expandPhysical(child, productById, componentsByBundle, multiplier * Number(entry.quantity), [...trail, id]);
  });
}

const pool = await connectAuto(skladDbConfig());
try {
  const [productResult, componentResult] = await Promise.all([
    pool.query(`select id, article, code, name, entity_type, production_barcode, barcode_list, marketplace_barcodes_json, archived from products_s order by id`),
    pool.query(`select bundle_id, component_id, quantity from bundle_components_s order by bundle_id, id`),
  ]);
  const products = productResult.rows;
  const productById = new Map(products.map((row) => [Number(row.id), row]));
  const componentsByBundle = new Map();
  for (const row of componentResult.rows) {
    const id = Number(row.bundle_id);
    if (!componentsByBundle.has(id)) componentsByBundle.set(id, []);
    componentsByBundle.get(id).push(row);
  }
  const index = buildWarehouseIndex(products);

  const ozonBase = text(process.env.OZON_API_BASE || "https://api-seller.ozon.ru").replace(/\/$/, "");
  const sources = [
    fetchOzon({ name: process.env.OZON_STORE_1_NAME || "ИП Ирина", base: ozonBase, headers: { "Client-Id": process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID, "Api-Key": process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY } }),
    fetchOzon({ name: process.env.OZON_STORE_2_NAME || "ИП Евгений", base: ozonBase, headers: { "Client-Id": process.env.OZON_STORE_2_CLIENT_ID, "Api-Key": process.env.OZON_STORE_2_API_KEY } }),
    fetchWbCards({ name: process.env.WB_STORE_1_NAME || "ИП Ирина", token: process.env.WB_API_TOKEN }),
    fetchWbCards({ name: process.env.WB_STORE_2_NAME || "ИП Евгений", token: process.env.WB_API_TOKEN_2 }),
  ];
  const settled = await Promise.allSettled(sources);
  const sourceNames = ["Ozon Ирина", "Ozon Евгений", "WB Ирина", "WB Евгений"];
  const errors = settled.flatMap((entry, indexValue) => entry.status === "rejected" ? [{ source: sourceNames[indexValue], error: entry.reason?.message || String(entry.reason) }] : []);
  const cards = settled.flatMap((entry) => entry.status === "fulfilled" ? entry.value : []);
  const activeCards = cards.filter((card) => card.active);
  const rows = [];
  const unresolved = [];
  for (const card of activeCards) {
    const resolved = resolveCard(card, index);
    if (!resolved.product) {
      unresolved.push(card);
      continue;
    }
    try {
      for (const physical of expandPhysical(resolved.product, productById, componentsByBundle)) {
        rows.push({
          warehouseId: Number(physical.product.id),
          article: text(physical.product.article || physical.product.code),
          name: text(physical.product.name),
          unitsPerCard: Number(physical.quantity),
          marketplace: card.marketplace,
          store: card.store,
          sourceArticle: card.article,
          sourceCardId: card.cardId,
          sourceName: card.name,
          sourceEntityType: text(resolved.product.entity_type),
          resolvedBy: resolved.method,
        });
      }
    } catch (error) {
      unresolved.push({ ...card, reason: error.message });
    }
  }

  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.warehouseId)) grouped.set(row.warehouseId, {
      warehouseId: row.warehouseId,
      article: row.article,
      name: row.name,
      boxQuantity: 50,
      maxUnitsInOneMarketplaceCard: 0,
      marketplaceCards: 0,
      marketplaces: new Set(),
      stores: new Set(),
      sourceExamples: [],
    });
    const item = grouped.get(row.warehouseId);
    item.maxUnitsInOneMarketplaceCard = Math.max(item.maxUnitsInOneMarketplaceCard, row.unitsPerCard);
    item.marketplaceCards += 1;
    item.marketplaces.add(row.marketplace);
    item.stores.add(`${row.marketplace} ${row.store}`);
    if (item.sourceExamples.length < 8) item.sourceExamples.push({ article: row.sourceArticle, units: row.unitsPerCard, store: `${row.marketplace} ${row.store}` });
  }
  const nomenclature = [...grouped.values()]
    .map((item) => ({ ...item, marketplaces: [...item.marketplaces].sort(), stores: [...item.stores].sort() }))
    .sort((left, right) => left.name.localeCompare(right.name, "ru"));

  const summary = {
    generatedAt: new Date().toISOString(),
    definition: {
      ozon: "returned by product/list with visibility=VISIBLE",
      wb: "present in content cards list and has positive stock in at least one live seller FBS warehouse",
    },
    sourceCounts: sourceNames.map((name, indexValue) => ({
      source: name,
      fetched: settled[indexValue].status === "fulfilled" ? settled[indexValue].value.length : 0,
      active: settled[indexValue].status === "fulfilled" ? settled[indexValue].value.filter((card) => card.active).length : 0,
    })),
    activeCards: activeCards.length,
    mappedActiveCards: activeCards.length - unresolved.length,
    unresolvedActiveCards: unresolved.length,
    physicalNomenclatureCount: nomenclature.length,
    errors,
  };
  console.log(JSON.stringify({ summary, nomenclature, unresolved }, null, 2));
} finally {
  await pool.end();
}
