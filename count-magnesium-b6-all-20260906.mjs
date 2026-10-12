import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

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
  { marketplace: "WB", id: process.env.WB_STORE_1_ID || "store1", name: process.env.WB_STORE_1_NAME || "WB 1", token: process.env.WB_API_TOKEN },
  { marketplace: "WB", id: process.env.WB_STORE_2_ID || "store2", name: process.env.WB_STORE_2_NAME || "WB 2", token: process.env.WB_API_TOKEN_2 },
].filter((store) => store.token);

function normalize(value) {
  return String(value || "").trim();
}

function isMagnesiumB6(article, name) {
  const a = normalize(article).toUpperCase();
  const n = normalize(name).toLowerCase().replace(/ё/g, "е");
  if (a === "GRA-01503") return true;
  if (!/(магни|magni|magnesium)/i.test(n)) return false;
  return /(b6|в6|б6)/i.test(n);
}

function classify(article, name) {
  const a = normalize(article).toUpperCase();
  const n = normalize(name).toLowerCase().replace(/ё/g, "е");
  if (a === "GRA-01503" || /фол/.test(n) || /fol/.test(n)) return "Магний B6 + фолиевая / GRA-01503";
  if (/(^|[^0-9])3\s*(шт|штуки|бан|bottles?)/i.test(n) || /набор\s*3/i.test(n)) return "Магний B6 комплект 3 шт";
  if (/(^|[^0-9])2\s*(шт|штуки|бан|bottles?)/i.test(n) || /набор\s*2/i.test(n)) return "Магний B6 комплект 2 шт";
  return "Магний B6 1 шт";
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
    if (!response.ok) throw new Error(`${store.name} Ozon ${status}: HTTP ${response.status} ${await response.text()}`);
    const payload = await response.json();
    const page = Array.isArray(payload?.result?.postings) ? payload.result.postings : [];
    rows.push(...page);
    if (payload?.result?.has_next !== true || page.length === 0) break;
    offset += 1000;
  }
  const unique = new Map();
  for (const row of rows) {
    const id = normalize(row?.posting_number);
    if (id && !unique.has(id)) unique.set(id, row);
  }
  return [...unique.values()];
}

async function wbGet(store, endpoint) {
  const response = await fetch(`https://marketplace-api.wildberries.ru${endpoint}`, {
    headers: { Authorization: store.token },
  });
  if (!response.ok) throw new Error(`${store.name} WB ${endpoint}: HTTP ${response.status} ${(await response.text()).slice(0, 300)}`);
  return response.json();
}

function addLine(bucket, row) {
  const key = `${bucket.marketplace}|${bucket.storeId}|${bucket.storeName}|${bucket.source}|${row.orderId}|${row.article}|${row.name}`;
  if (bucket.seen.has(key)) return;
  bucket.seen.add(key);
  const qty = Math.max(0, Number(row.quantity || 0)) || 1;
  const category = classify(row.article, row.name);
  bucket.lines.push({ ...row, category, quantity: qty });
}

const { Pool } = pg;
const db = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
  max: 1,
});

try {
  const buckets = [];

  for (const store of ozStores) {
    const bucket = { marketplace: "Ozon", storeId: store.id, storeName: store.name, source: "Новые Ozon", seen: new Set(), lines: [] };
    const postings = await ozonList(store, "awaiting_packaging");
    for (const posting of postings) {
      for (const product of Array.isArray(posting.products) ? posting.products : []) {
        const article = normalize(product.offer_id);
        const name = normalize(product.name);
        if (!isMagnesiumB6(article, name)) continue;
        addLine(bucket, {
          orderId: normalize(posting.posting_number),
          article,
          name,
          quantity: Number(product.quantity || 0),
        });
      }
    }
    buckets.push(bucket);
  }

  const ozDb = await db.query(
    `select o.store_id, o.store_name, o.assembly_id, a.name as assembly_name, o.posting_number,
            i.offer_id as article, i.name, i.quantity
       from ozon_assembly_order_items i
       join ozon_assembly_orders o on o.assembly_id = i.assembly_id and o.posting_number = i.posting_number
       join ozon_assemblies a on a.id = o.assembly_id
      where o.sent_to_delivering_at is null
        and o.collected_at is null
        and coalesce(a.access_mode, '') <> 'archived'
      order by o.store_id, a.created_at desc, o.posting_number`
  );
  const ozAsmBuckets = new Map();
  for (const row of ozDb.rows) {
    if (!isMagnesiumB6(row.article, row.name)) continue;
    const key = row.store_id || "unknown";
    if (!ozAsmBuckets.has(key)) {
      ozAsmBuckets.set(key, {
        marketplace: "Ozon",
        storeId: key,
        storeName: row.store_name || key,
        source: "В сборках Ozon",
        seen: new Set(),
        lines: [],
      });
    }
    addLine(ozAsmBuckets.get(key), {
      orderId: row.posting_number,
      sourceId: row.assembly_id,
      sourceName: row.assembly_name,
      article: row.article,
      name: row.name,
      quantity: row.quantity,
    });
  }
  buckets.push(...ozAsmBuckets.values());

  for (const store of wbStores) {
    const bucket = { marketplace: "WB", storeId: store.id, storeName: store.name, source: "Новые WB", seen: new Set(), lines: [] };
    const data = await wbGet(store, "/api/v3/orders/new");
    for (const order of Array.isArray(data?.orders) ? data.orders : []) {
      const article = normalize(order.article);
      const name = normalize(order.productName || order.name);
      if (!isMagnesiumB6(article, name)) continue;
      addLine(bucket, {
        orderId: normalize(order.id),
        article,
        name,
        quantity: Number(order.quantity || 1),
      });
    }
    buckets.push(bucket);
  }

  const wbDb = await db.query(
    `select coalesce(s.store_id, '') as store_id,
            coalesce(s.store_name, '') as store_name,
            o.supply_id, o.supply_id as supply_name,
            o.wb_order_id::text as order_id, o.article, o.product_name as name, o.quantity
       from supply_orders o
       left join supply_settings s on s.supply_id = o.supply_id
      where o.collected_at is null
        and coalesce(s.wb_supply_done, false) = false
      order by store_id, o.supply_id, o.wb_order_id`
  );
  const wbSupplyBuckets = new Map();
  for (const row of wbDb.rows) {
    if (!isMagnesiumB6(row.article, row.name)) continue;
    const key = row.store_id || "unknown";
    if (!wbSupplyBuckets.has(key)) {
      wbSupplyBuckets.set(key, {
        marketplace: "WB",
        storeId: key,
        storeName: row.store_name || key,
        source: "В поставках WB",
        seen: new Set(),
        lines: [],
      });
    }
    addLine(wbSupplyBuckets.get(key), {
      orderId: row.order_id,
      sourceId: row.supply_id,
      sourceName: row.supply_name,
      article: row.article,
      name: row.name,
      quantity: row.quantity,
    });
  }
  buckets.push(...wbSupplyBuckets.values());

  const summary = {};
  const categoryTotals = {};
  let grandOrders = 0;
  let grandUnits = 0;
  for (const bucket of buckets) {
    const lines = bucket.lines;
    const units = lines.reduce((sum, row) => sum + Number(row.quantity || 0), 0);
    const orders = new Set(lines.map((row) => row.orderId)).size;
    const key = `${bucket.marketplace} ${bucket.storeName} / ${bucket.source}`;
    summary[key] = { orders, units };
    grandOrders += orders;
    grandUnits += units;
    for (const line of lines) {
      categoryTotals[line.category] ??= { orders: new Set(), units: 0 };
      categoryTotals[line.category].orders.add(line.orderId);
      categoryTotals[line.category].units += Number(line.quantity || 0);
    }
  }
  const categories = Object.fromEntries(Object.entries(categoryTotals).map(([key, value]) => [key, { orders: value.orders.size, units: value.units }]));

  console.log(JSON.stringify({
    generatedAt: new Date().toISOString(),
    countedRule: "new API orders + local assembly/supply rows with collected_at is null and not sent/done",
    grand: { orders: grandOrders, units: grandUnits },
    categories,
    summary,
    details: buckets.map((bucket) => ({
      marketplace: bucket.marketplace,
      storeId: bucket.storeId,
      storeName: bucket.storeName,
      source: bucket.source,
      orders: new Set(bucket.lines.map((line) => line.orderId)).size,
      units: bucket.lines.reduce((sum, line) => sum + Number(line.quantity || 0), 0),
      lines: bucket.lines,
    })),
  }, null, 2));
} finally {
  await db.end();
}
