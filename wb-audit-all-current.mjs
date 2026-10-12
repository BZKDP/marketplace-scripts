import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/WB/node_modules/pg/lib/index.js";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.trim();
  if (!line || line.startsWith("#")) continue;
  const separator = line.indexOf("=");
  if (separator < 1) continue;
  const key = line.slice(0, separator).trim();
  let value = line.slice(separator + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[key] ??= value;
}

const stores = [
  { id: process.env.WB_STORE_1_ID || "store1", name: process.env.WB_STORE_1_NAME || "store1", token: process.env.WB_API_TOKEN },
  { id: process.env.WB_STORE_2_ID || "store2", name: process.env.WB_STORE_2_NAME || "store2", token: process.env.WB_API_TOKEN_2 },
].filter((store) => store.token);
const apiBase = "https://marketplace-api.wildberries.ru";

async function wbGet(store, endpoint) {
  const response = await fetch(`${apiBase}${endpoint}`, { headers: { Authorization: store.token } });
  if (!response.ok) throw new Error(`${store.name} ${endpoint}: HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`);
  return response.json();
}

async function listSupplies(store) {
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

async function listOrders(store) {
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

function normalizeOrder(order, scope, supplyId = null) {
  const skus = Array.isArray(order?.skus) ? order.skus.map(String).filter(Boolean) : [];
  return {
    scope,
    supplyId,
    orderId: String(order?.id ?? ""),
    article: String(order?.article || "").trim().toUpperCase(),
    barcode: String(order?.barcode || skus[0] || "").replace(/\s+/g, "").trim(),
    nmId: order?.nmId ?? null,
    name: String(order?.productName || order?.name || "").trim(),
  };
}

const { Pool } = pg;
const appDb = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
  max: 1,
});

try {
  const report = [];
  const { resolveWarehouseProductMeta } = await import("/opt/grasklad-sborka/apps/WB/server/services/sklad-movements.mjs");
  for (const store of stores) {
    const [newData, supplies, orderRows] = await Promise.all([
      wbGet(store, "/api/v3/orders/new"),
      listSupplies(store),
      listOrders(store),
    ]);
    const byOrderId = new Map(orderRows.map((order) => [String(order.id), order]));
    const activeSupplies = supplies.filter((supply) => supply && supply.id && supply.done !== true);
    const suppliedOrders = [];
    const missingDetails = [];
    for (const supply of activeSupplies) {
      const data = await wbGet(store, `/api/marketplace/v3/supplies/${encodeURIComponent(supply.id)}/order-ids`);
      const ids = Array.isArray(data) ? data : Array.isArray(data.orders) ? data.orders : Array.isArray(data.orderIds) ? data.orderIds : [];
      for (const value of ids) {
        const id = String(typeof value === "object" ? value.id ?? value.orderId ?? "" : value).trim();
        if (!id) continue;
        const order = byOrderId.get(id);
        if (order) suppliedOrders.push(normalizeOrder(order, "supply", String(supply.id)));
        else missingDetails.push({ supplyId: String(supply.id), orderId: id });
      }
    }
    const newOrders = (Array.isArray(newData.orders) ? newData.orders : []).map((order) => normalizeOrder(order, "new"));
    const ids = [...new Set([
      ...newOrders.map((row) => row.orderId),
      ...suppliedOrders.map((row) => row.orderId),
      ...missingDetails.map((row) => row.orderId),
    ].filter(Boolean))];
    const local = ids.length
      ? await appDb.query(
          `select wb_order_id::text as order_id, article, barcode, product_name, nm_id
             from supply_orders
            where wb_order_id::text = any($1::text[])`,
          [ids]
        )
      : { rows: [] };
    const localById = new Map(local.rows.map((row) => [row.order_id, row]));
    const unresolvedDetails = [];
    for (const missing of missingDetails) {
      const saved = localById.get(missing.orderId);
      if (!saved) {
        unresolvedDetails.push(missing);
        continue;
      }
      suppliedOrders.push({
        scope: "supply",
        supplyId: missing.supplyId,
        orderId: missing.orderId,
        article: String(saved.article || "").trim().toUpperCase(),
        barcode: String(saved.barcode || "").replace(/\s+/g, "").trim(),
        nmId: saved.nm_id ?? null,
        name: String(saved.product_name || "").trim(),
      });
    }
    const all = [...newOrders, ...suppliedOrders];
    for (const row of all) {
      const saved = localById.get(row.orderId);
      if (!row.article && saved?.article) row.article = String(saved.article).trim().toUpperCase();
      if (!row.barcode && saved?.barcode) row.barcode = String(saved.barcode).replace(/\s+/g, "").trim();
      if (!row.name && saved?.product_name) row.name = String(saved.product_name).trim();
      if (!row.nmId && saved?.nm_id) row.nmId = saved.nm_id;
    }
    const tokens = [...new Set(all.flatMap((row) => [row.barcode, row.article]).filter(Boolean))];
    const meta = await resolveWarehouseProductMeta(tokens);
    const unresolved = [];
    for (const row of all) {
      const keys = [row.barcode, row.article].map((value) => String(value || "").replace(/\s+/g, "").trim().toUpperCase()).filter(Boolean);
      if (!keys.some((key) => meta.has(key))) unresolved.push(row);
    }
    const uniqueProblems = [...new Map(unresolved.map((row) => [`${row.article}|${row.barcode}`, row])).values()];
    report.push({
      storeId: store.id,
      storeName: store.name,
      newOrders: newOrders.length,
      activeSupplies: activeSupplies.length,
      supplyOrders: suppliedOrders.length,
      missingOrderDetails: unresolvedDetails.length,
      unresolvedOrders: unresolved.length,
      unresolvedProducts: uniqueProblems.length,
      problems: uniqueProblems,
    });
  }
  console.log(JSON.stringify(report, null, 2));
} finally {
  await appDb.end();
}
