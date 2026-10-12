import fs from "node:fs";

function loadEnv(file) {
  for (const rawLine of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const line = rawLine.trim().replace(/^\uFEFF/, "");
    if (!line || line.startsWith("#")) continue;
    const separator = line.indexOf("=");
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    let value = line.slice(separator + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    process.env[key] ??= value;
  }
}

loadEnv("/opt/grasklad-sborka/.env");

const ozStores = [
  {
    marketplace: "Ozon",
    id: process.env.OZON_STORE_1_ID || "irina",
    name: process.env.OZON_STORE_1_NAME || "ИП Ирина",
    clientId: process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID,
    apiKey: process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY,
  },
  {
    marketplace: "Ozon",
    id: process.env.OZON_STORE_2_ID || "evgeny",
    name: process.env.OZON_STORE_2_NAME || "ИП Евгений",
    clientId: process.env.OZON_STORE_2_CLIENT_ID,
    apiKey: process.env.OZON_STORE_2_API_KEY,
  },
].filter((store) => store.clientId && store.apiKey);

const wbStores = [
  { marketplace: "WB", id: process.env.WB_STORE_1_ID || "store1", name: process.env.WB_STORE_1_NAME || "ИП Ирина", token: process.env.WB_API_TOKEN },
  { marketplace: "WB", id: process.env.WB_STORE_2_ID || "store2", name: process.env.WB_STORE_2_NAME || "ИП Евгений", token: process.env.WB_API_TOKEN_2 },
].filter((store) => store.token);

function text(value) {
  return String(value || "").trim();
}

function isMagnesiumB6(article, name) {
  const a = text(article).toUpperCase();
  const n = text(name).toLowerCase().replace(/ё/g, "е");
  if (a === "GRA-01503") return true;
  if (!/(магни|magni|magnesium)/i.test(n)) return false;
  return /(b6|в6|б6)/i.test(n);
}

function categoryAndCans(article, name) {
  const a = text(article).toUpperCase();
  const n = text(name).toLowerCase().replace(/ё/g, "е");
  if (a === "GRA-01503" || /фол/.test(n) || /fol/.test(n)) return { category: "GRA-01503 / магний B6 + фолиевая", cans: 1 };
  if (/(^|[^0-9])3\s*(шт|штуки|бан|bottles?)/i.test(n) || /набор\s*3/i.test(n)) return { category: "Магний B6 комплект 3 шт", cans: 3 };
  if (/(^|[^0-9])2\s*(шт|штуки|бан|bottles?)/i.test(n) || /набор\s*2/i.test(n)) return { category: "Магний B6 комплект 2 шт", cans: 2 };
  return { category: "Магний B6 1 шт", cans: 1 };
}

async function ozonList(store, status) {
  const now = new Date();
  const since = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);
  const rows = [];
  let offset = 0;
  while (true) {
    const response = await fetch("https://api-seller.ozon.ru/v3/posting/fbs/list", {
      method: "POST",
      headers: {
        "Client-Id": store.clientId,
        "Api-Key": store.apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        dir: "ASC",
        filter: { since: since.toISOString(), to: now.toISOString(), status },
        limit: 1000,
        offset,
        with: { analytics_data: true, barcodes: false },
      }),
    });
    if (!response.ok) throw new Error(`${store.name} ${status}: HTTP ${response.status} ${await response.text()}`);
    const payload = await response.json();
    const page = Array.isArray(payload?.result?.postings) ? payload.result.postings : [];
    rows.push(...page);
    if (payload?.result?.has_next !== true || page.length === 0) break;
    offset += 1000;
  }
  const unique = new Map();
  for (const row of rows) {
    const id = text(row.posting_number);
    if (id && !unique.has(id)) unique.set(id, row);
  }
  return [...unique.values()];
}

async function wbGet(store, endpoint) {
  const response = await fetch(`https://marketplace-api.wildberries.ru${endpoint}`, {
    headers: { Authorization: store.token },
  });
  if (!response.ok) throw new Error(`${store.name} ${endpoint}: HTTP ${response.status} ${(await response.text()).slice(0, 300)}`);
  return response.json();
}

async function wbListSupplies(store) {
  const rows = [];
  let next = 0;
  for (let page = 0; page < 50; page += 1) {
    const data = await wbGet(store, `/api/v3/supplies?limit=1000&next=${next}`);
    const batch = Array.isArray(data.supplies) ? data.supplies : [];
    rows.push(...batch);
    if (!data.next || data.next === next || batch.length === 0) break;
    next = data.next;
  }
  return rows;
}

async function wbListOrders(store) {
  const rows = [];
  let next = 0;
  const dateFrom = Math.floor(Date.now() / 1000) - 60 * 24 * 3600;
  for (let page = 0; page < 50; page += 1) {
    const data = await wbGet(store, `/api/v3/orders?limit=1000&next=${next}&dateFrom=${dateFrom}`);
    const batch = Array.isArray(data.orders) ? data.orders : [];
    rows.push(...batch);
    if (!data.next || data.next === next || batch.length === 0) break;
    next = data.next;
  }
  return rows;
}

function addLine(lines, base, article, name, quantity) {
  if (!isMagnesiumB6(article, name)) return;
  const qty = Math.max(1, Number(quantity || 1));
  const { category, cans } = categoryAndCans(article, name);
  lines.push({
    ...base,
    article: text(article),
    name: text(name),
    quantity: qty,
    category,
    cansPerUnit: cans,
    physicalCans: qty * cans,
  });
}

try {
  const lines = [];

  for (const store of ozStores) {
    for (const status of ["awaiting_packaging", "awaiting_deliver"]) {
      const postings = await ozonList(store, status);
      for (const posting of postings) {
        for (const product of Array.isArray(posting.products) ? posting.products : []) {
          addLine(
            lines,
            {
              marketplace: "Ozon",
              storeId: store.id,
              storeName: store.name,
              source: status === "awaiting_packaging" ? "Новые" : "В сборках / ожидают отгрузки",
              orderId: text(posting.posting_number),
            },
            product.offer_id,
            product.name,
            product.quantity
          );
        }
      }
    }
  }

  for (const store of wbStores) {
    const [newData, supplies, orders] = await Promise.all([
      wbGet(store, "/api/v3/orders/new"),
      wbListSupplies(store),
      wbListOrders(store),
    ]);
    const byId = new Map(orders.map((order) => [text(order.id), order]));
    for (const order of Array.isArray(newData.orders) ? newData.orders : []) {
      addLine(
        lines,
        { marketplace: "WB", storeId: store.id, storeName: store.name, source: "Новые", orderId: text(order.id) },
        order.article,
        order.productName || order.name,
        order.quantity || 1
      );
    }

    const activeSupplies = supplies.filter((supply) => supply?.id && supply.done !== true);
    const supplyPairs = [];
    for (const supply of activeSupplies) {
      const payload = await wbGet(store, `/api/marketplace/v3/supplies/${encodeURIComponent(supply.id)}/order-ids`);
      const ids = Array.isArray(payload) ? payload : Array.isArray(payload.orders) ? payload.orders : Array.isArray(payload.orderIds) ? payload.orderIds : [];
      for (const value of ids) {
        const id = text(typeof value === "object" ? value.id ?? value.orderId : value);
        if (!id) continue;
        supplyPairs.push({ supplyId: text(supply.id), orderId: id });
      }
    }

    for (const pair of supplyPairs) {
      const order = byId.get(pair.orderId);
      if (!order) continue;
      addLine(
        lines,
        { marketplace: "WB", storeId: store.id, storeName: store.name, source: "В поставках", sourceId: pair.supplyId, orderId: pair.orderId },
        order.article,
        order.productName || order.name,
        order.quantity || 1
      );
    }
  }

  const dedup = new Map();
  for (const line of lines) {
    const key = `${line.marketplace}|${line.storeId}|${line.source}|${line.sourceId || ""}|${line.orderId}|${line.article}|${line.name}`;
    if (!dedup.has(key)) dedup.set(key, line);
  }
  const finalLines = [...dedup.values()];

  function summarize(rows) {
    return {
      orders: new Set(rows.map((line) => line.orderId)).size,
      productUnits: rows.reduce((sum, line) => sum + line.quantity, 0),
      physicalCans: rows.reduce((sum, line) => sum + line.physicalCans, 0),
    };
  }

  const byCategory = {};
  const byStore = {};
  const bySource = {};
  for (const line of finalLines) {
    (byCategory[line.category] ??= []).push(line);
    (byStore[`${line.marketplace} ${line.storeName}`] ??= []).push(line);
    (bySource[`${line.marketplace} ${line.storeName} / ${line.source}`] ??= []).push(line);
  }

  console.log(JSON.stringify({
    generatedAt: new Date().toISOString(),
    rule: "Ozon API awaiting_packaging + awaiting_deliver; WB API new + active supplies(done!=true). Physical cans: set 2=2, set 3=3, GRA-01503=1.",
    total: summarize(finalLines),
    byCategory: Object.fromEntries(Object.entries(byCategory).map(([key, rows]) => [key, summarize(rows)])),
    byStore: Object.fromEntries(Object.entries(byStore).map(([key, rows]) => [key, summarize(rows)])),
    bySource: Object.fromEntries(Object.entries(bySource).map(([key, rows]) => [key, summarize(rows)])),
    lines: finalLines,
  }, null, 2));
} finally {
}
