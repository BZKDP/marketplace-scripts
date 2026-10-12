import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import express from "express";
import { Pool } from "pg";
import { PDFDocument } from "pdf-lib";
const movementModuleSpecifier = process.env.YANDEX_MOVEMENT_MODULE
  ? pathToFileURL(path.resolve(process.env.YANDEX_MOVEMENT_MODULE)).href
  : "../OZ/server/services/sklad-movements.mjs";
const {
  getWarehouseProductAvailability,
  moveProductFromShelfToEmployee,
  resolveWarehouseProductMeta,
  writeOffEmployeeInventoryBatch,
} = await import(movementModuleSpecifier);
import {
  collectableOrdersForShipment,
  externalOrderProblem,
  isConfirmedShipmentStatus,
  isReadyToShipOrder,
  isStartedOrder,
  markingRequirement,
  normalizeRequestId,
  normalizeShipment,
  shipmentConfirmationBlockers,
} from "./logic.mjs";

const HOST = process.env.YANDEX_HOST || "127.0.0.1";
const PORT = Number(process.env.YANDEX_PORT || 3009);
const ENABLED = process.env.YANDEX_ENABLED === "1";
const API_KEY = String(process.env.YANDEX_API_KEY || "").trim();
const BUSINESS_ID = Number(process.env.YANDEX_BUSINESS_ID || 1124700);
const CAMPAIGN_ID = Number(process.env.YANDEX_CAMPAIGN_ID || 148851177);
const CAMPAIGN_IDS = [...new Set(
  String(process.env.YANDEX_CAMPAIGN_IDS || CAMPAIGN_ID)
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0)
)];
if (!CAMPAIGN_IDS.includes(CAMPAIGN_ID)) CAMPAIGN_IDS.unshift(CAMPAIGN_ID);
const CAMPAIGN_NAMES = new Map([
  [148851177, "GraFLab 2"],
  [148949326, "Склад Ижевск"],
]);
const WAREHOUSE_ID = Number(process.env.YANDEX_WAREHOUSE_ID || 2090504);
const SKLAD_WAREHOUSE_ID = Number(process.env.YANDEX_SKLAD_WAREHOUSE_ID || process.env.SKLAD_FBS_WAREHOUSE_ID || 38);
const COOKIE_NAME = process.env.COOKIE_NAME || "sid";
const LABEL_SECRET = String(process.env.YANDEX_LABEL_SECRET || process.env.DESKTOP_SECRET || API_KEY).trim();
const MARKET_API = String(process.env.YANDEX_MARKET_API || "https://api.partner.market.yandex.ru").replace(/\/$/, "");
const API_TIMEOUT_MS = Math.max(5000, Number(process.env.YANDEX_API_TIMEOUT_MS || 30000));
const RECONCILE_STALE_MS = Math.max(15000, Number(process.env.YANDEX_RECONCILE_STALE_MS || 45000));
const WAITING_CACHE_MS = Math.max(15000, Number(process.env.YANDEX_WAITING_CACHE_MS || 45000));
const SHIPMENT_CACHE_MS = Math.max(5000, Number(process.env.YANDEX_SHIPMENT_CACHE_MS || 30000));
const DOCUMENT_DIR = path.resolve(process.env.YANDEX_DOCUMENT_DIR || path.join(process.cwd(), "storage", "yandex"));
const LABEL_FORMAT = "A9_HORIZONTALLY";
const shipmentCache = new Map();
let waitingCache = { orders: [], fetchedAt: 0, error: "" };
let waitingCacheGeneration = 0;
let waitingRefresh = null;

const db = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  options: process.env.DB_OPTIONS || undefined,
  application_name: "grasklad-yandex",
  max: Math.max(2, Number(process.env.YANDEX_DB_POOL_MAX || 4)),
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 8000,
  keepAlive: true,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable"
    ? undefined
    : { rejectUnauthorized: false },
});

db.on("error", (error) => console.error("[ym-db] idle client error", error));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function campaignName(campaignId) {
  const id = Number(campaignId);
  return CAMPAIGN_NAMES.get(id) || `Магазин ${id}`;
}

function parseCookies(raw = "") {
  const result = {};
  for (const part of String(raw).split(";")) {
    const index = part.indexOf("=");
    if (index < 0) continue;
    const key = part.slice(0, index).trim();
    if (!key) continue;
    try {
      result[key] = decodeURIComponent(part.slice(index + 1).trim());
    } catch {
      result[key] = part.slice(index + 1).trim();
    }
  }
  return result;
}

function publicUser(row) {
  const fullName = String(row.external_full_name || `${row.surname || ""} ${row.name || ""}`).trim();
  return {
    id: Number(row.user_id || row.id),
    surname: row.surname || "",
    name: row.name || "",
    fullName,
    role: row.role,
    position: row.position || "",
    locationCode: row.location_code || "",
    externalEmployeeId: row.external_employee_id != null ? Number(row.external_employee_id) : null,
  };
}

function createLabelToken(payload) {
  const body = Buffer.from(JSON.stringify({ ...payload, exp: Date.now() + 15 * 60 * 1000 })).toString("base64url");
  const signature = crypto.createHmac("sha256", LABEL_SECRET).update(body).digest("base64url");
  return `${body}.${signature}`;
}

function parseLabelToken(token) {
  const [body, signature] = String(token || "").split(".");
  if (!body || !signature || !LABEL_SECRET) return null;
  const expected = crypto.createHmac("sha256", LABEL_SECRET).update(body).digest();
  let actual;
  try {
    actual = Buffer.from(signature, "base64url");
  } catch {
    return null;
  }
  if (expected.length !== actual.length || !crypto.timingSafeEqual(expected, actual)) return null;
  try {
    const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
    return Number(payload.exp) > Date.now() ? payload : null;
  } catch {
    return null;
  }
}

async function sessionMiddleware(req, res, next) {
  try {
    const token = parseCookies(req.headers.cookie || "")[COOKIE_NAME];
    if (!token) return res.status(401).json({ error: "Требуется авторизация" });
    const result = await db.query(
      `select u.id as user_id, u.surname, u.name, u.role, u.position,
              u.location_code, u.external_employee_id, u.external_full_name
         from sessions s
         join users u on u.id = s.user_id
        where s.token = $1 and s.expires_at > now() and u.role <> 'archived'
        limit 1`,
      [token]
    );
    if (!result.rowCount) return res.status(401).json({ error: "Сессия истекла" });
    req.sessionUser = publicUser(result.rows[0]);
    next();
  } catch (error) {
    next(error);
  }
}

function requireAdmin(req, res, next) {
  if (!new Set(["admin", "super_admin"]).has(req.sessionUser?.role)) {
    return res.status(403).json({ error: "Действие доступно администратору" });
  }
  next();
}

function requireEmployee(req, res, next) {
  if (req.sessionUser?.role !== "employee") {
    return res.status(403).json({ error: "Действие доступно сотруднику" });
  }
  next();
}

function cleanApiErrorText(text) {
  const value = String(text || "").replace(/\s+/g, " ").trim();
  return value.slice(0, 800) || "Яндекс Маркет вернул ошибку";
}

async function marketRequest(path, options = {}) {
  if (!API_KEY) throw Object.assign(new Error("API-ключ Яндекс Маркета не настроен"), { statusCode: 503 });
  const method = String(options.method || "GET").toUpperCase();
  const isRead = method === "GET" || options.retrySafe === true;
  const attempts = isRead ? 4 : 1;
  let lastError = null;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), options.timeoutMs || API_TIMEOUT_MS);
    try {
      const headers = { "Api-Key": API_KEY, ...(options.headers || {}) };
      let body;
      if (options.body !== undefined) {
        headers["Content-Type"] = "application/json";
        body = JSON.stringify(options.body);
      }
      const response = await fetch(`${MARKET_API}${path}`, { method, headers, body, signal: controller.signal });
      if (!response.ok) {
        const text = await response.text();
        const error = Object.assign(new Error(cleanApiErrorText(text)), {
          statusCode: response.status >= 500 || response.status === 420 || response.status === 429 ? 503 : 409,
          marketStatus: response.status,
        });
        if (isRead && [420, 429, 500, 502, 503, 504].includes(response.status) && attempt + 1 < attempts) {
          lastError = error;
          await sleep(700 * (attempt + 1));
          continue;
        }
        throw error;
      }
      if (options.responseType === "buffer") {
        return { buffer: Buffer.from(await response.arrayBuffer()), contentType: response.headers.get("content-type") || "application/pdf" };
      }
      if (response.status === 204) return {};
      return await response.json();
    } catch (error) {
      const normalized = error?.name === "AbortError"
        ? Object.assign(new Error("Яндекс Маркет не ответил вовремя"), { statusCode: 503 })
        : error;
      if (isRead && attempt + 1 < attempts && (!normalized?.marketStatus || normalized.statusCode === 503)) {
        lastError = normalized;
        await sleep(700 * (attempt + 1));
        continue;
      }
      throw normalized;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error("Яндекс Маркет недоступен");
}

async function listStartedOrders() {
  const orders = [];
  let pageToken = "";
  for (let page = 0; page < 20; page += 1) {
    const params = new URLSearchParams({ limit: "50" });
    if (pageToken) params.set("pageToken", pageToken);
    const data = await marketRequest(`/v1/businesses/${BUSINESS_ID}/orders?${params}`, {
      method: "POST",
      retrySafe: true,
      body: { campaignIds: CAMPAIGN_IDS, statuses: ["PROCESSING"], substatuses: ["STARTED", "READY_TO_SHIP"] },
    });
    orders.push(...(Array.isArray(data?.orders) ? data.orders : []));
    pageToken = String(data?.paging?.nextPageToken || "");
    if (!pageToken) break;
  }
  return orders.map((order) => ({ ...order, id: Number(order.orderId || order.id) }));
}

async function listOrdersByIds(orderIds) {
  const unique = [...new Set((orderIds || []).map(Number).filter((value) => Number.isFinite(value) && value > 0))];
  const orders = [];
  for (let offset = 0; offset < unique.length; offset += 50) {
    const chunk = unique.slice(offset, offset + 50);
    const data = await marketRequest(`/v1/businesses/${BUSINESS_ID}/orders?limit=50`, {
      method: "POST",
      retrySafe: true,
      body: { campaignIds: CAMPAIGN_IDS, orderIds: chunk },
    });
    orders.push(...(Array.isArray(data?.orders) ? data.orders : []));
  }
  return orders.map((order) => ({ ...order, id: Number(order.orderId || order.id) }));
}

async function getOrder(orderId, campaignId = CAMPAIGN_ID) {
  const data = await marketRequest(`/v2/campaigns/${campaignId}/orders/${encodeURIComponent(orderId)}`);
  return data?.order || null;
}

async function getOfferMappings(offerIds) {
  const result = new Map();
  const unique = [...new Set(offerIds.map((value) => String(value || "").trim()).filter(Boolean))];
  for (let offset = 0; offset < unique.length; offset += 200) {
    const chunk = unique.slice(offset, offset + 200);
    const data = await marketRequest(`/v2/businesses/${BUSINESS_ID}/offer-mappings?limit=200`, {
      method: "POST",
      retrySafe: true,
      body: { offerIds: chunk },
    });
    for (const mapping of data?.result?.offerMappings || []) {
      const offer = mapping?.offer || {};
      const id = String(offer.offerId || "").trim();
      if (!id) continue;
      result.set(id, {
        offerId: id,
        name: String(offer.name || "").trim(),
        barcodes: [...new Set((offer.barcodes || []).map((value) => String(value || "").trim()).filter(Boolean))],
        picture: String((offer.pictures || [])[0] || "").trim(),
      });
    }
  }
  return result;
}

function lookupKey(value) {
  return String(value || "").replace(/\s+/g, "").trim().toUpperCase();
}

const scannedWarehouseProductCache = new Map();

async function resolveScannedWarehouseProductId(barcode) {
  const key = lookupKey(barcode);
  if (!key) return null;
  const cached = scannedWarehouseProductCache.get(key);
  if (cached?.expiresAt > Date.now()) return cached.productId;
  try {
    const matches = await resolveWarehouseProductMeta([key]);
    const productId = Number(matches.get(key)?.id);
    if (!Number.isInteger(productId) || productId < 1) return null;
    scannedWarehouseProductCache.set(key, { productId, expiresAt: Date.now() + 5 * 60 * 1000 });
    return productId;
  } catch (error) {
    console.error("[ym] warehouse barcode resolution failed:", error);
    return null;
  }
}

async function enrichOrders(orders) {
  const offerIds = orders.flatMap((order) => (order.items || []).map((item) => item.offerId || item.shopSku));
  const mappings = await getOfferMappings(offerIds);
  const tokens = [];
  for (const id of offerIds) {
    const mapping = mappings.get(String(id));
    tokens.push(id, ...(mapping?.barcodes || []));
  }
  let warehouse = new Map();
  let availability = new Map();
  let warehouseError = "";
  try {
    warehouse = await resolveWarehouseProductMeta(tokens);
    const productIds = [...new Set([...warehouse.values()].map((meta) => Number(meta?.id)).filter((value) => Number.isInteger(value) && value > 0))];
    availability = await getWarehouseProductAvailability(productIds, SKLAD_WAREHOUSE_ID);
  } catch (error) {
    warehouseError = error?.message || "Складская БД недоступна";
  }
  return orders.map((order) => {
    const shipment = order?.delivery?.shipment || order?.delivery?.shipments?.[0] || null;
    const campaignId = Number(order.campaignId || CAMPAIGN_ID);
    const items = (order.items || []).map((item) => {
      const offerId = String(item.offerId || item.shopSku || "").trim();
      const mapping = mappings.get(offerId) || { barcodes: [] };
      const meta = [offerId, ...(mapping.barcodes || [])]
        .map((token) => warehouse.get(lookupKey(token)))
        .find(Boolean) || null;
      return {
        id: Number(item.id),
        offerId,
        name: mapping.name || item.offerName || offerId,
        quantity: Number(item.count || 0),
        barcodes: mapping.barcodes || [],
        picture: mapping.picture || "",
        requiredInstanceTypes: item.requiredInstanceTypes || [],
        warehouseProductId: meta?.id || null,
        availableStock: meta?.id ? Number(availability.get(Number(meta.id)) || 0) : null,
      };
    });
    const marking = markingRequirement({ ...order, items });
    return {
      id: Number(order.orderId || order.id),
      campaignId,
      campaignName: campaignName(campaignId),
      status: order.status,
      substatus: order.substatus,
      buyerType: order.buyerType || null,
      updateDate: order.updateDate || null,
      creationDate: order.creationDate || null,
      shipmentId: shipment?.id ? Number(shipment.id) : null,
      shipmentDate: shipment?.shipmentDate || order?.delivery?.shipments?.[0]?.shipmentDate || null,
      items,
      units: items.reduce((sum, item) => sum + item.quantity, 0),
      hasMappingProblem: items.some((item) => !item.barcodes.length || !item.warehouseProductId),
      hasStockProblem: items.some((item) => item.warehouseProductId && Number(item.availableStock) < Number(item.quantity)),
      hasMandatoryMarking: marking.mandatory,
      mandatoryMarkingTypes: marking.types,
      warehouseError,
    };
  });
}

function invalidateWaitingCache() {
  waitingCacheGeneration += 1;
  waitingCache = { ...waitingCache, fetchedAt: 0 };
}

function waitingCacheStatus() {
  const ageMs = waitingCache.fetchedAt ? Date.now() - waitingCache.fetchedAt : null;
  return {
    orders: waitingCache.orders,
    cachedAt: waitingCache.fetchedAt ? new Date(waitingCache.fetchedAt).toISOString() : null,
    loading: !waitingCache.fetchedAt || ageMs > WAITING_CACHE_MS || Boolean(waitingRefresh),
    error: waitingCache.error,
  };
}

async function loadWaitingOrders({ force = false } = {}) {
  const generation = waitingCacheGeneration;
  const cacheIsFresh = waitingCache.fetchedAt && Date.now() - waitingCache.fetchedAt <= WAITING_CACHE_MS;
  if (waitingRefresh?.generation === generation) return waitingRefresh.promise;
  if (!force && cacheIsFresh) return waitingCacheStatus();

  const promise = (async () => {
    try {
      const [started, localOrders] = await Promise.all([
        listStartedOrders(),
        db.query("select order_id from ym_orders"),
      ]);
      const assigned = new Set(localOrders.rows.map((row) => String(row.order_id)));
      const orders = await enrichOrders(started.filter((order) => !assigned.has(String(order.id))));
      orders.sort((left, right) => String(left.creationDate || "").localeCompare(String(right.creationDate || "")));
      if (generation === waitingCacheGeneration) {
        waitingCache = { orders, fetchedAt: Date.now(), error: "" };
      }
      return {
        orders,
        cachedAt: new Date().toISOString(),
        loading: false,
        error: "",
      };
    } catch (error) {
      if (generation === waitingCacheGeneration) {
        waitingCache = { ...waitingCache, error: String(error?.message || error) };
      }
      throw error;
    } finally {
      if (waitingRefresh?.generation === generation) waitingRefresh = null;
    }
  })();
  waitingRefresh = { generation, promise };
  return promise;
}

function stockShortages(orders) {
  const totals = new Map();
  for (const order of orders || []) {
    for (const item of order.items || []) {
      const productId = Number(item.warehouseProductId);
      if (!Number.isInteger(productId) || productId <= 0) continue;
      const current = totals.get(productId) || { productId, name: item.name, offerId: item.offerId, required: 0, available: Number(item.availableStock || 0) };
      current.required += Number(item.quantity || 0);
      current.available = Math.min(current.available, Number(item.availableStock || 0));
      totals.set(productId, current);
    }
  }
  return [...totals.values()].filter((item) => item.required > item.available);
}

async function listShipments() {
  const from = new Date(Date.now() - 60 * 86400000).toISOString().slice(0, 10);
  const to = new Date(Date.now() + 120 * 86400000).toISOString().slice(0, 10);
  const statuses = [
    "OUTBOUND_CREATED", "OUTBOUND_READY_FOR_CONFIRMATION", "OUTBOUND_CONFIRMED",
    "OUTBOUND_SIGNED", "ACCEPTED", "ACCEPTED_WITH_DISCREPANCIES", "FINISHED", "ERROR",
  ];
  const summaries = [];
  for (const campaignId of CAMPAIGN_IDS) {
    let pageToken = "";
    for (let page = 0; page < 50; page += 1) {
      const params = new URLSearchParams({ limit: "30" });
      if (pageToken) params.set("pageToken", pageToken);
      const data = await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments?${params}`, {
        method: "PUT",
        retrySafe: true,
        body: { dateFrom: from, dateTo: to, statuses, cancelledOrders: false },
      });
      summaries.push(...(data?.result?.shipments || []).map((shipment) => normalizeShipment({
        ...shipment,
        campaignId,
        campaignName: campaignName(campaignId),
      })));
      pageToken = String(data?.result?.paging?.nextPageToken || "");
      if (!pageToken) break;
    }
  }
  const detailed = [];
  const uniqueSummaries = [...new Map(summaries.map((shipment) => [`${shipment.campaignId}:${shipment.id}`, shipment])).values()];
  for (const summary of uniqueSummaries) {
    if (["OUTBOUND_CREATED", "OUTBOUND_READY_FOR_CONFIRMATION"].includes(summary.status)) {
      const detail = await getShipment(summary.id, { campaignId: summary.campaignId }).catch(() => null);
      detailed.push(detail ? { ...summary, ...detail } : summary);
    } else {
      detailed.push(summary);
    }
  }
  return detailed.sort((left, right) => String(right.planIntervalFrom || "").localeCompare(String(left.planIntervalFrom || "")));
}

async function resolveShipmentCampaignId(shipmentId) {
  const local = await db.query(
    `select a.campaign_id
       from ym_orders o join ym_assemblies a on a.id = o.assembly_id
      where o.shipment_id = $1
      order by a.created_at desc
      limit 1`,
    [shipmentId]
  );
  if (local.rowCount) return Number(local.rows[0].campaign_id);
  for (const campaignId of CAMPAIGN_IDS) {
    try {
      await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments/${encodeURIComponent(shipmentId)}?cancelledOrders=false`);
      return campaignId;
    } catch (error) {
      if (Number(error?.marketStatus) !== 404) throw error;
    }
  }
  throw Object.assign(new Error("Отгрузка не найдена ни в одном магазине Яндекса"), { statusCode: 404 });
}

async function getShipment(shipmentId, { fresh = false, campaignId = null } = {}) {
  const resolvedCampaignId = Number(campaignId) || await resolveShipmentCampaignId(shipmentId);
  const key = `${resolvedCampaignId}:${shipmentId}`;
  const cached = shipmentCache.get(key);
  if (!fresh && cached && cached.expiresAt > Date.now()) return cached.value;
  const data = await marketRequest(`/v2/campaigns/${resolvedCampaignId}/first-mile/shipments/${encodeURIComponent(shipmentId)}?cancelledOrders=false`);
  const value = data?.result ? normalizeShipment({
    ...data.result,
    campaignId: resolvedCampaignId,
    campaignName: campaignName(resolvedCampaignId),
  }) : null;
  shipmentCache.set(key, { value, expiresAt: Date.now() + SHIPMENT_CACHE_MS });
  return value;
}

function invalidateShipment(shipmentId, campaignId = null) {
  if (campaignId) {
    shipmentCache.delete(`${campaignId}:${shipmentId}`);
    return;
  }
  for (const key of shipmentCache.keys()) {
    if (key.endsWith(`:${shipmentId}`)) shipmentCache.delete(key);
  }
}

async function ensureSchema() {
  await db.query(`
    create table if not exists ym_assemblies (
      id uuid primary key,
      name text not null,
      business_id bigint not null,
      campaign_id bigint not null,
      warehouse_id bigint not null,
      allow_zero_stock boolean not null default false,
      allow_no_scan boolean not null default true,
      status text not null default 'assigned',
      created_by bigint not null references users(id),
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      completed_at timestamptz
    );
    alter table ym_assemblies add column if not exists allow_zero_stock boolean not null default false;
    alter table ym_assemblies add column if not exists allow_no_scan boolean not null default true;
    alter table ym_assemblies alter column allow_no_scan set default true;
    alter table ym_assemblies add column if not exists launched_at timestamptz;
    update ym_assemblies set launched_at = coalesce(created_at, now()) where status <> 'draft' and launched_at is null;
    create table if not exists ym_assembly_users (
      assembly_id uuid not null references ym_assemblies(id) on delete cascade,
      user_id bigint not null references users(id),
      primary key (assembly_id, user_id)
    );
    create table if not exists ym_orders (
      order_id bigint primary key,
      assembly_id uuid not null references ym_assemblies(id) on delete cascade,
      assigned_user_id bigint not null references users(id),
      shipment_id bigint,
      yandex_status text not null,
      yandex_substatus text not null,
      local_state text not null default 'assigned',
      snapshot jsonb not null default '{}'::jsonb,
      label_fetched_at timestamptz,
      box_layout_sent_at timestamptz,
      ready_to_ship_at timestamptz,
      stock_writeoff_status text not null default 'pending',
      last_error text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    alter table ym_orders alter column assigned_user_id drop not null;
    alter table ym_orders add column if not exists external_synced_at timestamptz;
    alter table ym_orders add column if not exists external_changed_at timestamptz;
    alter table ym_orders add column if not exists external_snapshot jsonb;
    alter table ym_orders add column if not exists problem_code text;
    create index if not exists ym_orders_assembly_idx on ym_orders(assembly_id, assigned_user_id, local_state);
    create index if not exists ym_orders_external_sync_idx on ym_orders(local_state, external_synced_at);
    create table if not exists ym_order_items (
      order_id bigint not null references ym_orders(order_id) on delete cascade,
      item_id bigint not null,
      offer_id text not null,
      name text not null,
      quantity integer not null check (quantity > 0),
      picked_qty integer not null default 0 check (picked_qty >= 0),
      stock_picked_qty integer not null default 0 check (stock_picked_qty >= 0),
      packed_qty integer not null default 0 check (packed_qty >= 0),
      barcodes jsonb not null default '[]'::jsonb,
      picture_url text,
      required_instance_types jsonb not null default '[]'::jsonb,
      warehouse_product_id bigint,
      primary key (order_id, item_id),
      check (picked_qty <= quantity),
      check (packed_qty <= quantity)
    );
    alter table ym_order_items add column if not exists stock_picked_qty integer not null default 0;
    create table if not exists ym_audit_log (
      id bigserial primary key,
      user_id bigint references users(id),
      action text not null,
      entity_type text not null,
      entity_id text not null,
      details jsonb not null default '{}'::jsonb,
      created_at timestamptz not null default now()
    );
    create index if not exists ym_audit_entity_idx on ym_audit_log(entity_type, entity_id, created_at desc);
    create table if not exists ym_scan_requests (
      request_id uuid primary key,
      user_id bigint not null references users(id),
      action text not null,
      assembly_id uuid not null references ym_assemblies(id) on delete cascade,
      order_id bigint references ym_orders(order_id) on delete cascade,
      barcode text not null,
      status text not null default 'processing',
      response jsonb,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
    create index if not exists ym_scan_requests_created_idx on ym_scan_requests(created_at);
    create table if not exists ym_shipment_documents (
      shipment_id bigint primary key,
      places_count integer not null check (places_count >= 0),
      pallet_labels_path text,
      pallet_labels_sha256 text,
      pallet_labels_fetched_at timestamptz,
      confirmed_at timestamptz,
      confirmed_by bigint references users(id),
      updated_at timestamptz not null default now()
    );
  `);
  await fs.mkdir(DOCUMENT_DIR, { recursive: true });
  await db.query("delete from ym_scan_requests where created_at < now() - interval '90 days'");
}

function asyncRoute(handler) {
  return (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);
}

async function audit(userId, action, entityType, entityId, details = {}) {
  await db.query(
    `insert into ym_audit_log(user_id, action, entity_type, entity_id, details)
     values ($1, $2, $3, $4, $5::jsonb)`,
    [userId || null, action, entityType, String(entityId), JSON.stringify(details)]
  );
}

async function refreshAssemblyStatuses(assemblyIds) {
  const ids = [...new Set((assemblyIds || []).map(String).filter(Boolean))];
  if (!ids.length) return;
  await db.query(
    `update ym_assemblies a
        set status = case
              when exists (select 1 from ym_orders o where o.assembly_id = a.id and o.local_state = 'packing') then 'packing'
              when exists (select 1 from ym_orders o where o.assembly_id = a.id and o.local_state = 'assigned') then a.status
              when exists (select 1 from ym_orders o where o.assembly_id = a.id and o.local_state = 'draft') then 'draft'
              when exists (select 1 from ym_orders o where o.assembly_id = a.id and o.local_state = 'external_changed') then 'needs_attention'
              else 'completed'
            end,
            completed_at = case
              when not exists (select 1 from ym_orders o where o.assembly_id = a.id and o.local_state <> 'completed')
                then coalesce(a.completed_at, now())
              else null
            end,
            updated_at = now()
      where a.id = any($1::uuid[])`,
    [ids]
  );
}

async function reconcileOrders({ assemblyId = null, force = false } = {}) {
  const params = [];
  const where = ["o.local_state in ('draft', 'assigned', 'packing', 'external_changed')"];
  if (assemblyId) {
    params.push(String(assemblyId));
    where.push(`o.assembly_id = $${params.length}`);
  }
  if (!force) {
    params.push(new Date(Date.now() - RECONCILE_STALE_MS));
    where.push(`(o.external_synced_at is null or o.external_synced_at < $${params.length})`);
  }
  const local = await db.query(
    `select o.order_id, o.assembly_id, o.local_state, o.last_error,
            json_agg(json_build_object(
              'item_id', i.item_id, 'offer_id', i.offer_id, 'quantity', i.quantity,
              'picked_qty', i.picked_qty, 'packed_qty', i.packed_qty
            ) order by i.item_id) as items
       from ym_orders o
       join ym_order_items i on i.order_id = o.order_id
      where ${where.join(" and ")}
      group by o.order_id`,
    params
  );
  if (!local.rowCount) return { checked: 0, changed: 0, problems: 0 };

  const liveOrders = await listOrdersByIds(local.rows.map((row) => row.order_id));
  const liveMap = new Map(liveOrders.map((order) => [String(order.id), order]));
  const assemblyIds = [];
  let changed = 0;
  let problems = 0;
  for (const row of local.rows) {
    const live = liveMap.get(String(row.order_id)) || null;
    const problem = externalOrderProblem(live, row.items || []);
    assemblyIds.push(row.assembly_id);
    if (problem) {
      problems += 1;
      if (row.local_state !== "external_changed" || row.last_error !== problem.message) changed += 1;
      await db.query(
        `update ym_orders
            set local_state = 'external_changed', problem_code = $2, last_error = $3,
                external_changed_at = coalesce(external_changed_at, now()), external_synced_at = now(),
                external_snapshot = $4::jsonb, yandex_status = $5, yandex_substatus = $6, updated_at = now()
          where order_id = $1`,
        [row.order_id, problem.code, problem.message, JSON.stringify(live || {}), live?.status || "MISSING", live?.substatus || "MISSING"]
      );
      continue;
    }
    if (row.local_state === "external_changed") {
      problems += 1;
      await db.query(
        `update ym_orders
            set external_synced_at = now(), external_snapshot = $2::jsonb,
                yandex_status = $3, yandex_substatus = $4, updated_at = now()
          where order_id = $1`,
        [row.order_id, JSON.stringify(live), live.status, live.substatus]
      );
      continue;
    }
    await db.query(
      `update ym_orders
          set external_synced_at = now(), external_snapshot = $2::jsonb,
              yandex_status = $3, yandex_substatus = $4, updated_at = now()
        where order_id = $1`,
      [row.order_id, JSON.stringify(live), live.status, live.substatus]
    );
  }
  await refreshAssemblyStatuses(assemblyIds);
  return { checked: local.rowCount, changed, problems };
}

async function beginScanRequest(client, { requestId: rawRequestId, userId, action, assemblyId, orderId = null, barcode }) {
  if (rawRequestId && !normalizeRequestId(rawRequestId)) {
    throw Object.assign(new Error("Некорректный идентификатор операции сканирования"), { statusCode: 400 });
  }
  const requestId = normalizeRequestId(rawRequestId) || crypto.randomUUID();
  const inserted = await client.query(
    `insert into ym_scan_requests(request_id, user_id, action, assembly_id, order_id, barcode)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (request_id) do nothing
     returning request_id`,
    [requestId, userId, action, assemblyId, orderId, barcode]
  );
  if (inserted.rowCount) return { requestId, duplicate: false, response: null };
  const existing = await client.query("select * from ym_scan_requests where request_id = $1 for update", [requestId]);
  const row = existing.rows[0];
  const sameRequest = row
    && Number(row.user_id) === Number(userId)
    && row.action === action
    && String(row.assembly_id) === String(assemblyId)
    && Number(row.order_id || 0) === Number(orderId || 0)
    && row.barcode === barcode;
  if (!sameRequest) {
    throw Object.assign(new Error("Идентификатор сканирования уже использован для другой операции"), { statusCode: 409 });
  }
  if (row.status !== "completed") {
    throw Object.assign(new Error("Предыдущий скан ещё обрабатывается, повторите через секунду"), { statusCode: 409 });
  }
  return { requestId, duplicate: true, response: row.response || { ok: true, idempotent: true } };
}

async function finishScanRequest(client, requestId, response) {
  await client.query(
    "update ym_scan_requests set status = 'completed', response = $2::jsonb, updated_at = now() where request_id = $1",
    [requestId, JSON.stringify(response)]
  );
}

async function loadEmployeeOrder(orderId, assemblyId, userId) {
  const detail = await db.query(
    `select o.*, a.campaign_id, a.allow_no_scan, json_agg(json_build_object(
       'itemId', i.item_id, 'offerId', i.offer_id, 'quantity', i.quantity,
       'pickedQty', i.picked_qty, 'stockPickedQty', i.stock_picked_qty, 'packedQty', i.packed_qty, 'barcodes', i.barcodes,
       'warehouseProductId', i.warehouse_product_id, 'requiredInstanceTypes', i.required_instance_types
     ) order by i.item_id) as items
       from ym_orders o join ym_order_items i on i.order_id = o.order_id
       join ym_assemblies a on a.id = o.assembly_id
      where o.order_id = $1 and o.assembly_id = $2 and o.assigned_user_id = $3
      group by o.order_id, a.campaign_id, a.allow_no_scan`,
    [orderId, assemblyId, userId]
  );
  return detail.rows[0] || null;
}

async function markExternalProblem(orderId, live, problem) {
  await db.query(
    `update ym_orders
        set local_state = 'external_changed', problem_code = $2, last_error = $3,
            external_changed_at = coalesce(external_changed_at, now()), external_synced_at = now(),
            external_snapshot = $4::jsonb, yandex_status = $5, yandex_substatus = $6, updated_at = now()
      where order_id = $1`,
    [orderId, problem.code, problem.message, JSON.stringify(live || {}), live?.status || "MISSING", live?.substatus || "MISSING"]
  );
}

async function ensureOrderBoxLayout(orderRow) {
  if (!orderRow) throw Object.assign(new Error("Заказ не найден"), { statusCode: 404 });
  if (orderRow.local_state === "external_changed") {
    throw Object.assign(new Error(orderRow.last_error || "Заказ изменён в Яндекс Маркете"), { statusCode: 409 });
  }
  if (orderRow.allow_no_scan !== true && (orderRow.items || []).some((item) => Number(item.packedQty) !== Number(item.quantity))) {
    throw Object.assign(new Error("Сначала отсканируйте все товары заказа"), { statusCode: 409 });
  }
  if (orderRow.box_layout_sent_at) return orderRow;
  const campaignId = Number(orderRow.campaign_id || orderRow.campaignId || CAMPAIGN_ID);
  const live = await getOrder(orderRow.order_id, campaignId);
  const problem = externalOrderProblem(live, orderRow.items || []);
  if (problem) {
    await markExternalProblem(orderRow.order_id, live, problem);
    throw Object.assign(new Error(problem.message), { statusCode: 409 });
  }
  if (isReadyToShipOrder(live)) {
    await db.query(
      `update ym_orders set box_layout_sent_at = coalesce(box_layout_sent_at, now()), external_synced_at = now(),
              yandex_status = 'PROCESSING', yandex_substatus = 'READY_TO_SHIP', updated_at = now()
        where order_id = $1`,
      [orderRow.order_id]
    );
    return { ...orderRow, box_layout_sent_at: orderRow.box_layout_sent_at || new Date().toISOString() };
  }
  await marketRequest(`/v2/campaigns/${campaignId}/orders/${orderRow.order_id}/boxes`, {
    method: "PUT",
    retrySafe: true,
    body: { boxes: [{ items: orderRow.items.map((item) => ({ id: Number(item.itemId), fullCount: Number(item.quantity) })) }] },
  });
  await db.query(
    `update ym_orders set box_layout_sent_at = now(), external_synced_at = now(),
            yandex_status = 'PROCESSING', yandex_substatus = 'STARTED', updated_at = now()
      where order_id = $1`,
    [orderRow.order_id]
  );
  return { ...orderRow, box_layout_sent_at: new Date().toISOString() };
}

async function downloadSignedFile(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      throw Object.assign(new Error(`Не удалось скачать готовый документ: HTTP ${response.status}`), { statusCode: 503 });
    }
    return {
      buffer: Buffer.from(await response.arrayBuffer()),
      contentType: response.headers.get("content-type") || "application/octet-stream",
    };
  } finally {
    clearTimeout(timer);
  }
}

async function generateReportFile(path, body) {
  const generated = await marketRequest(path, { method: "POST", body });
  const reportId = String(generated?.result?.reportId || "");
  if (!reportId) throw Object.assign(new Error("Яндекс не вернул идентификатор документа"), { statusCode: 503 });
  const deadline = Date.now() + 60000;
  const initialDelay = Math.min(3000, Math.max(400, Number(generated?.result?.estimatedGenerationTime || 600)));
  await sleep(initialDelay);
  while (Date.now() < deadline) {
    const info = await marketRequest(`/v2/reports/info/${encodeURIComponent(reportId)}`);
    const status = info?.result?.status;
    if (status === "DONE" && info?.result?.file) return downloadSignedFile(info.result.file);
    if (status === "FAILED" || status === "NO_DATA") {
      throw Object.assign(new Error(`Яндекс не сформировал документ: ${info?.result?.subStatus || status}`), { statusCode: 409 });
    }
    await sleep(Math.min(2500, Math.max(600, Number(info?.result?.estimatedGenerationTime || 1000))));
  }
  throw Object.assign(new Error("Яндекс формирует документ дольше минуты, повторите скачивание"), { statusCode: 503 });
}

function isConfirmedShipment(shipment) {
  const actions = Array.isArray(shipment?.availableActions) ? shipment.availableActions : [];
  if (actions.includes("CONFIRM") || actions.includes("CHANGE_PALLETS_COUNT")) return false;
  return isConfirmedShipmentStatus(shipment?.status);
}

async function shipmentDocument(shipmentId) {
  const result = await db.query("select * from ym_shipment_documents where shipment_id = $1", [shipmentId]);
  return result.rows[0] || null;
}

async function assertEmployeeShipmentAccess(assemblyId, shipmentId, userId) {
  const result = await db.query(
    `select 1
       from ym_assembly_users au
      where au.assembly_id = $1 and au.user_id = $2
        and exists (select 1 from ym_orders o where o.assembly_id = au.assembly_id and o.shipment_id = $3)
      limit 1`,
    [assemblyId, userId, shipmentId]
  );
  if (!result.rowCount) throw Object.assign(new Error("Отгрузка не относится к вашей сборке"), { statusCode: 403 });
}

async function shipmentReadiness(shipment, activeOrders = null) {
  const campaignId = Number(shipment?.campaignId || await resolveShipmentCampaignId(shipment?.id));
  const liveOrders = collectableOrdersForShipment(activeOrders || await listStartedOrders(), { ...shipment, campaignId });
  const orderIds = liveOrders.map((order) => Number(order.id)).filter(Number.isFinite);
  const notReadyOrderIds = liveOrders.filter((order) => !isReadyToShipOrder(order)).map((order) => Number(order.id));
  const unfinished = orderIds.length
    ? await db.query(
      "select order_id from ym_orders where order_id = any($1::bigint[]) and local_state in ('assigned', 'packing', 'external_changed')",
      [orderIds]
    )
    : { rows: [] };
  return { orderIds, notReadyOrderIds, unfinishedOrderIds: unfinished.rows.map((row) => Number(row.order_id)) };
}

async function shipmentWorkflow(shipmentId, { fresh = false } = {}) {
  const shipment = await getShipment(shipmentId, { fresh });
  if (!shipment) throw Object.assign(new Error("Отгрузка не найдена"), { statusCode: 404 });
  const document = await shipmentDocument(shipmentId);
  const plannedPlaces = Number(shipment.palletsCount?.planned || 0);
  const labelsSaved = Boolean(
    document?.pallet_labels_fetched_at
    && document?.pallet_labels_path
    && Number(document.places_count) === plannedPlaces
  );
  return {
    ...shipment,
    plannedPlaces,
    packageLabelsSaved: labelsSaved,
    packageLabelsSavedAt: labelsSaved ? document.pallet_labels_fetched_at : null,
  };
}

async function decorateShipmentList(shipments, { includeReadiness = false } = {}) {
  const ids = (shipments || []).map((shipment) => Number(shipment.id)).filter(Number.isFinite);
  const documents = ids.length
    ? await db.query("select * from ym_shipment_documents where shipment_id = any($1::bigint[])", [ids])
    : { rows: [] };
  const documentMap = new Map(documents.rows.map((row) => [String(row.shipment_id), row]));
  const activeOrders = includeReadiness ? await listStartedOrders() : null;
  const result = [];
  for (const shipment of shipments || []) {
    const document = documentMap.get(String(shipment.id));
    const plannedPlaces = Number(shipment.palletsCount?.planned || 0);
    const packageLabelsSaved = Boolean(
      document?.pallet_labels_fetched_at
      && document?.pallet_labels_path
      && Number(document.places_count) === plannedPlaces
    );
    const value = {
      ...shipment,
      plannedPlaces,
      packageLabelsSaved,
      packageLabelsSavedAt: packageLabelsSaved ? document.pallet_labels_fetched_at : null,
    };
    if (includeReadiness) {
      const readiness = await shipmentReadiness(shipment, activeOrders);
      value.notReadyCount = readiness.notReadyOrderIds.length;
      value.localUnfinishedCount = readiness.unfinishedOrderIds.length;
    }
    result.push(value);
  }
  return result;
}

async function setShipmentPlaces(shipmentId, placesCount, userId) {
  if (!Number.isInteger(placesCount) || placesCount < 1 || placesCount > 200) {
    throw Object.assign(new Error("Укажите количество транспортных упаковок от 1 до 200"), { statusCode: 400 });
  }
  const campaignId = await resolveShipmentCampaignId(shipmentId);
  const shipment = await getShipment(shipmentId, { fresh: true, campaignId });
  if (!shipment) throw Object.assign(new Error("Отгрузка не найдена"), { statusCode: 404 });
  if (isConfirmedShipment(shipment)) {
    throw Object.assign(new Error("Подтверждённую отгрузку изменить нельзя"), { statusCode: 409 });
  }
  if (!(shipment.availableActions || []).includes("CHANGE_PALLETS_COUNT")) {
    throw Object.assign(new Error("Яндекс сейчас не разрешает изменить количество упаковок"), { statusCode: 409 });
  }
  await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments/${shipmentId}/pallets`, {
    method: "PUT", retrySafe: true, body: { placesCount },
  });
  invalidateShipment(shipmentId, campaignId);
  await db.query(
    `insert into ym_shipment_documents(shipment_id, places_count, updated_at)
     values ($1, $2, now())
     on conflict (shipment_id) do update
       set places_count = excluded.places_count,
           pallet_labels_path = null,
           pallet_labels_sha256 = null,
           pallet_labels_fetched_at = null,
           updated_at = now()`,
    [shipmentId, placesCount]
  );
  await audit(userId, "shipment_places_set", "shipment", shipmentId, { placesCount });
  return shipmentWorkflow(shipmentId, { fresh: true });
}

async function duplicatePdfPages(buffer, copies = 2) {
  const source = await PDFDocument.load(buffer);
  if (!source.getPageCount()) throw Object.assign(new Error("Яндекс вернул пустой PDF ярлыков"), { statusCode: 503 });
  const output = await PDFDocument.create();
  for (let pageIndex = 0; pageIndex < source.getPageCount(); pageIndex += 1) {
    for (let copy = 0; copy < copies; copy += 1) {
      const [page] = await output.copyPages(source, [pageIndex]);
      output.addPage(page);
    }
  }
  return Buffer.from(await output.save({ useObjectStreams: false }));
}

async function readExistingPalletLabels(document, plannedPlaces) {
  if (!document?.pallet_labels_path || Number(document.places_count) !== Number(plannedPlaces)) return null;
  try {
    return await fs.readFile(document.pallet_labels_path);
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

async function getPalletLabels(shipmentId) {
  const campaignId = await resolveShipmentCampaignId(shipmentId);
  let shipment = null;
  let marketError = null;
  try {
    shipment = await getShipment(shipmentId, { fresh: true, campaignId });
  } catch (error) {
    marketError = error;
  }
  const document = await shipmentDocument(shipmentId);
  const plannedPlaces = shipment ? Number(shipment.palletsCount?.planned || 0) : Number(document?.places_count || 0);
  const existing = await readExistingPalletLabels(document, plannedPlaces);
  if (existing) return { buffer: existing, contentType: "application/pdf", cached: true, placesCount: plannedPlaces };
  if (marketError) throw marketError;
  if (!shipment) throw Object.assign(new Error("Отгрузка не найдена"), { statusCode: 404 });
  if (plannedPlaces < 1) {
    throw Object.assign(new Error("Сначала укажите количество транспортных упаковок"), { statusCode: 409 });
  }
  if (isConfirmedShipment(shipment)) {
    throw Object.assign(new Error("Ярлык не был сохранён до подтверждения отгрузки и больше недоступен в Яндексе"), { statusCode: 409 });
  }
  const marketFile = await marketRequest(
    `/v2/campaigns/${campaignId}/first-mile/shipments/${shipmentId}/pallet/labels?format=A8`,
    { responseType: "buffer" }
  );
  const buffer = await duplicatePdfPages(marketFile.buffer, 2);
  const directory = path.join(DOCUMENT_DIR, "shipments", String(shipmentId));
  await fs.mkdir(directory, { recursive: true });
  const filePath = path.join(directory, `pallet-labels-A8-${plannedPlaces}-places-2copies.pdf`);
  const temporary = `${filePath}.${crypto.randomUUID()}.tmp`;
  await fs.writeFile(temporary, buffer);
  await fs.rename(temporary, filePath);
  const checksum = crypto.createHash("sha256").update(buffer).digest("hex");
  await db.query(
    `insert into ym_shipment_documents(shipment_id, places_count, pallet_labels_path, pallet_labels_sha256, pallet_labels_fetched_at, updated_at)
     values ($1, $2, $3, $4, now(), now())
     on conflict (shipment_id) do update
       set places_count = excluded.places_count, pallet_labels_path = excluded.pallet_labels_path,
           pallet_labels_sha256 = excluded.pallet_labels_sha256, pallet_labels_fetched_at = now(), updated_at = now()`,
    [shipmentId, plannedPlaces, filePath, checksum]
  );
  return { buffer, contentType: "application/pdf", cached: false, placesCount: plannedPlaces };
}

async function confirmShipment(shipmentId, userId) {
  const campaignId = await resolveShipmentCampaignId(shipmentId);
  const shipment = await getShipment(shipmentId, { fresh: true, campaignId });
  if (!shipment) throw Object.assign(new Error("Отгрузка не найдена"), { statusCode: 404 });
  if (isConfirmedShipment(shipment)) return { ok: true, idempotent: true, status: shipment.status };
  const actions = shipment.availableActions || [];
  let labelsSaved = true;
  if (actions.includes("CHANGE_PALLETS_COUNT")) {
    const plannedPlaces = Number(shipment.palletsCount?.planned || 0);
    const document = await shipmentDocument(shipmentId);
    const saved = await readExistingPalletLabels(document, plannedPlaces);
    labelsSaved = Boolean(document?.pallet_labels_fetched_at && saved);
  }
  const readiness = await shipmentReadiness(shipment);
  const blockers = shipmentConfirmationBlockers({
    shipment,
    labelsSaved,
    notReadyCount: readiness.notReadyOrderIds.length,
    unfinishedCount: readiness.unfinishedOrderIds.length,
  });
  if (blockers.includes("confirm_unavailable")) {
    throw Object.assign(new Error("Яндекс сейчас не разрешает подтвердить эту отгрузку"), { statusCode: 409 });
  }
  if (blockers.includes("places_missing") || blockers.includes("package_labels_missing")) {
    throw Object.assign(new Error("Сначала укажите количество коробов и сохраните ярлыки упаковок"), { statusCode: 409 });
  }
  if (blockers.includes("orders_not_ready")) {
    throw Object.assign(new Error(`В отгрузке ещё ${readiness.notReadyOrderIds.length} заказов не готовы к отправке`), { statusCode: 409 });
  }
  if (blockers.includes("local_orders_unfinished")) {
    throw Object.assign(new Error("Есть незавершённые заказы во внутренних сборках"), { statusCode: 409 });
  }
  try {
    await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments/${shipmentId}/confirm`, {
      method: "POST", body: {},
    });
  } catch (error) {
    invalidateShipment(shipmentId, campaignId);
    const reconciled = await getShipment(shipmentId, { fresh: true, campaignId }).catch(() => null);
    if (!isConfirmedShipment(reconciled)) throw error;
  }
  invalidateShipment(shipmentId, campaignId);
  await db.query(
    `insert into ym_shipment_documents(shipment_id, places_count, confirmed_at, confirmed_by, updated_at)
     values ($1, $2, now(), $3, now())
     on conflict (shipment_id) do update set confirmed_at = now(), confirmed_by = excluded.confirmed_by, updated_at = now()`,
    [shipmentId, Number(shipment.palletsCount?.planned || 0), userId]
  );
  await audit(userId, "shipment_confirm", "shipment", shipmentId, { orderIds: readiness.orderIds });
  return { ok: true };
}

async function assemblySummaries() {
  const result = await db.query(`
    select a.id, a.name, a.campaign_id, a.status, a.created_at, a.completed_at,
           count(distinct o.order_id)::int as orders_total,
           count(distinct o.order_id) filter (where o.local_state = 'completed')::int as orders_completed,
           count(distinct o.order_id) filter (where o.local_state = 'external_changed')::int as orders_problem,
           coalesce(sum(i.quantity), 0)::int as units_total,
           coalesce(sum(i.picked_qty), 0)::int as units_picked,
           string_agg(distinct trim(concat(u.surname, ' ', u.name)), ', ') as employees
      from ym_assemblies a
      left join ym_orders o on o.assembly_id = a.id
      left join ym_order_items i on i.order_id = o.order_id
      left join users u on u.id = o.assigned_user_id
     group by a.id
     order by a.created_at desc
     limit 100
  `);
  return result.rows.map((row) => ({ ...row, campaign_name: campaignName(row.campaign_id) }));
}

async function getAssemblyDetail(assemblyId, userId = null) {
  const params = [assemblyId];
  let userFilter = "";
  if (userId) {
    params.push(userId);
    userFilter = "and o.assigned_user_id = $2 and o.local_state in ('assigned', 'packing', 'completed')";
  }
  const assemblyResult = await db.query("select * from ym_assemblies where id = $1", [assemblyId]);
  if (!assemblyResult.rowCount) return null;
  const ordersResult = await db.query(
    `select o.*, trim(concat(u.surname, ' ', u.name)) as employee_name
       from ym_orders o left join users u on u.id = o.assigned_user_id
      where o.assembly_id = $1 ${userFilter}
      order by o.order_id`,
    params
  );
  const orderIds = ordersResult.rows.map((row) => row.order_id);
  const itemsResult = orderIds.length
    ? await db.query("select * from ym_order_items where order_id = any($1::bigint[]) order by order_id, item_id", [orderIds])
    : { rows: [] };
  const itemsByOrder = new Map();
  for (const item of itemsResult.rows) {
    const key = String(item.order_id);
    if (!itemsByOrder.has(key)) itemsByOrder.set(key, []);
    itemsByOrder.get(key).push(item);
  }
  const orders = ordersResult.rows.map((row) => ({ ...row, items: itemsByOrder.get(String(row.order_id)) || [] }));
  let assignment = null;
  if (!userId) {
    const assignedUsers = await db.query(
      "select user_id from ym_assembly_users where assembly_id = $1 order by user_id",
      [assemblyId]
    );
    const locked = orders.some((order) =>
      ["packing", "completed", "external_changed"].includes(order.local_state)
      || order.items.some((item) => Number(item.picked_qty) > 0 || Number(item.packed_qty) > 0)
    );
    assignment = {
      userIds: assignedUsers.rows.map((row) => Number(row.user_id)),
      locked,
      lockReason: locked ? "Сборка уже начата. Назначение сотрудников заблокировано." : "",
    };
  }
  const assembly = assemblyResult.rows[0];
  return { assembly: { ...assembly, campaign_name: campaignName(assembly.campaign_id) }, orders, assignment };
}

async function employeeProductRows(assemblyId, userId) {
  const result = await db.query(
    `select i.offer_id, min(i.name) as name, min(i.picture_url) as picture_url,
            min(i.barcodes::text)::jsonb as barcodes,
            sum(i.quantity)::int as quantity, sum(i.picked_qty)::int as picked_qty
       from ym_order_items i
       join ym_orders o on o.order_id = i.order_id
      where o.assembly_id = $1 and o.assigned_user_id = $2 and o.local_state in ('assigned', 'packing')
      group by i.offer_id
      order by min(i.name)`,
    [assemblyId, userId]
  );
  return result.rows;
}

function productImageUrl(rawUrl) {
  const value = String(rawUrl || "").trim();
  if (!value) throw Object.assign(new Error("Адрес изображения не указан"), { statusCode: 400 });
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw Object.assign(new Error("Некорректный адрес изображения"), { statusCode: 400 });
  }
  if (parsed.protocol !== "https:" || parsed.hostname !== "avatars.mds.yandex.net" || parsed.username || parsed.password) {
    throw Object.assign(new Error("Источник изображения не разрешён"), { statusCode: 403 });
  }
  return parsed;
}

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

app.get("/health", asyncRoute(async (_req, res) => {
  await db.query("select 1");
  res.json({
    ok: true,
    enabled: ENABLED,
    campaignId: CAMPAIGN_ID,
    campaignIds: CAMPAIGN_IDS,
    campaigns: CAMPAIGN_IDS.map((id) => ({ id, name: campaignName(id) })),
    warehouseId: WAREHOUSE_ID,
  });
}));

app.use((req, res, next) => {
  if (!ENABLED) return res.status(503).json({ error: "Яндекс Маркет временно выключен" });
  next();
});

app.get("/labels/:token", asyncRoute(async (req, res) => {
  const payload = parseLabelToken(req.params.token);
  if (!payload?.orderId || !payload?.assemblyId || !payload?.userId) {
    return res.status(403).json({ error: "Ссылка на этикетку истекла" });
  }
  const order = await loadEmployeeOrder(Number(payload.orderId), String(payload.assemblyId), Number(payload.userId));
  if (!order) return res.status(404).json({ error: "Заказ не найден" });
  await ensureOrderBoxLayout(order);
  const file = await marketRequest(`/v2/campaigns/${Number(order.campaign_id || CAMPAIGN_ID)}/orders/${Number(payload.orderId)}/delivery/labels?format=${LABEL_FORMAT}`, { responseType: "buffer" });
  await db.query("update ym_orders set label_fetched_at = now(), updated_at = now() where order_id = $1", [Number(payload.orderId)]);
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.get("/shipment-labels/:token", asyncRoute(async (req, res) => {
  const payload = parseLabelToken(req.params.token);
  const shipmentId = Number(payload?.shipmentId);
  if (payload?.documentType !== "pallet" || !Number.isInteger(shipmentId) || shipmentId < 1) {
    return res.status(403).json({ error: "Ссылка на ярлыки истекла" });
  }
  const file = await getPalletLabels(shipmentId);
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.use(sessionMiddleware);

app.get("/images/product", asyncRoute(async (req, res) => {
  const url = productImageUrl(req.query.url);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.min(API_TIMEOUT_MS, 15000));
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      redirect: "error",
      headers: { "User-Agent": "Grasklad-Yandex/1.0" },
    });
    if (!response.ok) {
      throw Object.assign(new Error(`Изображение Яндекса недоступно: HTTP ${response.status}`), { statusCode: 502 });
    }
    const contentType = String(response.headers.get("content-type") || "").split(";", 1)[0].trim().toLowerCase();
    if (!["image/jpeg", "image/png", "image/webp", "image/avif", "image/gif"].includes(contentType)) {
      throw Object.assign(new Error("Яндекс вернул не изображение"), { statusCode: 502 });
    }
    const buffer = Buffer.from(await response.arrayBuffer());
    if (!buffer.length || buffer.length > 10 * 1024 * 1024) {
      throw Object.assign(new Error("Некорректный размер изображения"), { statusCode: 502 });
    }
    res.set("Cache-Control", "private, max-age=86400, stale-while-revalidate=604800");
    res.type(contentType).send(buffer);
  } finally {
    clearTimeout(timer);
  }
}));

app.get("/dashboard", requireAdmin, asyncRoute(async (_req, res) => {
  const [fastAssemblies, fastEmployees] = await Promise.all([
    assemblySummaries(),
    db.query(`select id, surname, name, external_full_name, external_employee_id
                from users where role = 'employee'
               order by lower(surname), lower(name)`),
  ]);
  const waitingSnapshot = waitingCacheStatus();
  if (waitingSnapshot.loading) {
    loadWaitingOrders().catch((error) => console.error("[ym] waiting orders refresh failed:", error));
  }
  return res.json({
    shop: {
      businessId: BUSINESS_ID,
      campaignId: CAMPAIGN_ID,
      campaignIds: CAMPAIGN_IDS,
      campaigns: CAMPAIGN_IDS.map((id) => ({ id, name: campaignName(id) })),
      warehouseId: WAREHOUSE_ID,
      name: "GraFLab · все FBS",
    },
    waiting: waitingSnapshot.orders,
    waitingStatus: {
      cachedAt: waitingSnapshot.cachedAt,
      loading: waitingSnapshot.loading,
      error: waitingSnapshot.error,
    },
    assemblies: fastAssemblies,
    employees: fastEmployees.rows.map(publicUser),
    syncedAt: new Date().toISOString(),
  });

}));

app.get("/waiting", requireAdmin, asyncRoute(async (req, res) => {
  const result = await loadWaitingOrders({ force: req.query.force === "1" });
  res.json({ orders: result.orders, cachedAt: result.cachedAt, loading: false, error: result.error });
}));

app.post("/sync", requireAdmin, asyncRoute(async (req, res) => {
  const result = await reconcileOrders({ force: true });
  invalidateWaitingCache();
  await audit(req.sessionUser.id, "orders_sync", "campaign", CAMPAIGN_IDS.join(","), result);
  res.json({ ok: true, ...result });
}));

app.get("/assemblies/:id", requireAdmin, asyncRoute(async (req, res) => {
  const detail = await getAssemblyDetail(req.params.id);
  if (!detail) return res.status(404).json({ error: "Сборка не найдена" });
  res.json(detail);
}));

app.patch("/assemblies/:id/settings", requireAdmin, asyncRoute(async (req, res) => {
  if (typeof req.body?.allowNoScan !== "boolean") {
    return res.status(400).json({ error: "Некорректное значение настройки сборки без сканирования" });
  }
  const updated = await db.query(
    "update ym_assemblies set allow_no_scan = $2, updated_at = now() where id = $1 returning id",
    [req.params.id, req.body.allowNoScan]
  );
  if (!updated.rowCount) return res.status(404).json({ error: "Сборка не найдена" });
  await audit(req.sessionUser.id, "assembly_settings", "assembly", req.params.id, {
    allowNoScan: req.body.allowNoScan,
  });
  res.json(await getAssemblyDetail(req.params.id));
}));

app.patch("/assemblies/:id/assignment", requireAdmin, asyncRoute(async (req, res) => {
  const userIds = [...new Set((req.body?.userIds || []).map(Number).filter(Number.isFinite))];
  if (!userIds.length) return res.status(400).json({ error: "Выберите хотя бы одного сотрудника" });
  const client = await db.connect();
  try {
    await client.query("begin");
    const assembly = await client.query("select id from ym_assemblies where id = $1 for update", [req.params.id]);
    if (!assembly.rowCount) throw Object.assign(new Error("Сборка не найдена"), { statusCode: 404 });
    const users = await client.query(
      "select id from users where role = 'employee' and id = any($1::bigint[])",
      [userIds]
    );
    if (users.rowCount !== userIds.length) {
      throw Object.assign(new Error("Один из сотрудников недоступен"), { statusCode: 409 });
    }
    const orders = await client.query(
      `select o.order_id, o.local_state,
              exists (
                select 1 from ym_order_items i
                 where i.order_id = o.order_id and (i.picked_qty > 0 or i.packed_qty > 0)
              ) as has_progress
         from ym_orders o
        where o.assembly_id = $1
        order by o.order_id
        for update`,
      [req.params.id]
    );
    const started = orders.rows.some((order) =>
      !["draft", "assigned"].includes(order.local_state) || order.has_progress
    );
    if (started) {
      throw Object.assign(new Error("Сборка уже начата. Переназначение заблокировано."), { statusCode: 409 });
    }
    await client.query("delete from ym_assembly_users where assembly_id = $1", [req.params.id]);
    for (const userId of userIds) {
      await client.query("insert into ym_assembly_users(assembly_id, user_id) values ($1, $2)", [req.params.id, userId]);
    }
    for (let index = 0; index < orders.rows.length; index += 1) {
      await client.query(
        "update ym_orders set assigned_user_id = $2, updated_at = now() where order_id = $1",
        [orders.rows[index].order_id, userIds[index % userIds.length]]
      );
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
  await audit(req.sessionUser.id, "assembly_reassign", "assembly", req.params.id, { userIds });
  res.json(await getAssemblyDetail(req.params.id));
}));

app.post("/assemblies/:id/start", requireAdmin, asyncRoute(async (req, res) => {
  const client = await db.connect();
  let started = false;
  try {
    await client.query("begin");
    const assembly = await client.query("select id, status from ym_assemblies where id = $1 for update", [req.params.id]);
    if (!assembly.rowCount) throw Object.assign(new Error("Сборка не найдена"), { statusCode: 404 });
    const orders = await client.query(
      "select order_id, assigned_user_id, local_state from ym_orders where assembly_id = $1 order by order_id for update",
      [req.params.id]
    );
    if (!orders.rowCount) throw Object.assign(new Error("В сборке нет заказов"), { statusCode: 409 });
    if (orders.rows.some((order) => order.local_state === "external_changed")) {
      throw Object.assign(new Error("Сначала исправьте заказы с расхождениями Яндекса"), { statusCode: 409 });
    }
    if (orders.rows.some((order) => !order.assigned_user_id)) {
      throw Object.assign(new Error("Сначала выберите сотрудников и распределите заказы"), { statusCode: 409 });
    }
    if (assembly.rows[0].status === "draft") {
      await client.query(
        "update ym_orders set local_state = 'assigned', updated_at = now() where assembly_id = $1 and local_state = 'draft'",
        [req.params.id]
      );
      await client.query(
        "update ym_assemblies set status = 'assigned', launched_at = now(), completed_at = null, updated_at = now() where id = $1",
        [req.params.id]
      );
      started = true;
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
  if (started) await audit(req.sessionUser.id, "assembly_start", "assembly", req.params.id, {});
  const detail = await getAssemblyDetail(req.params.id);
  res.json({ ...detail, started });
}));

app.post("/assemblies/:id/sync", requireAdmin, asyncRoute(async (req, res) => {
  const exists = await db.query("select 1 from ym_assemblies where id = $1", [req.params.id]);
  if (!exists.rowCount) return res.status(404).json({ error: "Сборка не найдена" });
  const result = await reconcileOrders({ assemblyId: req.params.id, force: true });
  await audit(req.sessionUser.id, "assembly_sync", "assembly", req.params.id, result);
  res.json({ ok: true, ...result });
}));

app.get("/assemblies/:id/shipments", requireAdmin, asyncRoute(async (req, res) => {
  const rows = await db.query(
    `select distinct o.shipment_id, a.campaign_id
       from ym_orders o join ym_assemblies a on a.id = o.assembly_id
      where o.assembly_id = $1 and o.shipment_id is not null
      order by o.shipment_id`,
    [req.params.id]
  );
  const shipments = [];
  for (const row of rows.rows) {
    const shipment = await getShipment(Number(row.shipment_id), { campaignId: Number(row.campaign_id) });
    if (shipment) shipments.push(shipment);
  }
  res.json({ shipments: await decorateShipmentList(shipments, { includeReadiness: true }) });
}));

app.get("/assemblies/:id/documents/pick-list", requireAdmin, asyncRoute(async (req, res) => {
  const orders = await db.query(
    `select o.order_id, a.campaign_id
       from ym_orders o join ym_assemblies a on a.id = o.assembly_id
      where o.assembly_id = $1 order by o.order_id`,
    [req.params.id]
  );
  if (!orders.rowCount) return res.status(404).json({ error: "Сборка не найдена" });
  const file = await generateReportFile("/v2/reports/documents/shipment-list/generate", {
    campaignId: Number(orders.rows[0].campaign_id),
    orderIds: orders.rows.map((row) => Number(row.order_id)),
  });
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.get("/assemblies/:id/documents/labels", requireAdmin, asyncRoute(async (req, res) => {
  const orders = await db.query(
    "select order_id, assigned_user_id from ym_orders where assembly_id = $1 and local_state <> 'external_changed' order by order_id",
    [req.params.id]
  );
  if (!orders.rowCount) return res.status(404).json({ error: "Сборка не найдена" });
  for (const row of orders.rows) {
    const order = await loadEmployeeOrder(Number(row.order_id), req.params.id, Number(row.assigned_user_id));
    await ensureOrderBoxLayout(order);
  }
  const file = await generateReportFile(`/v2/reports/documents/labels/generate?format=${LABEL_FORMAT}`, {
    businessId: BUSINESS_ID,
    orderIds: orders.rows.map((row) => Number(row.order_id)),
    sortingType: "SORT_BY_GIVEN_ORDER",
  });
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.post("/assemblies/:id/orders/:orderId/resolve", requireAdmin, asyncRoute(async (req, res) => {
  const orderId = Number(req.params.orderId);
  const local = await db.query(
    `select o.*, coalesce(sum(i.picked_qty), 0)::int as picked, coalesce(sum(i.packed_qty), 0)::int as packed
       from ym_orders o join ym_order_items i on i.order_id = o.order_id
      where o.order_id = $1 and o.assembly_id = $2
      group by o.order_id`,
    [orderId, req.params.id]
  );
  if (!local.rowCount) return res.status(404).json({ error: "Заказ не найден" });
  if (local.rows[0].local_state !== "external_changed") return res.status(409).json({ error: "У заказа нет внешней ошибки" });
  if (Number(local.rows[0].picked) > 0 || Number(local.rows[0].packed) > 0) {
    return res.status(409).json({ error: "По заказу уже перемещён товар. Сначала верните его сотрудником на склад, затем исправьте заказ вручную." });
  }

  const live = (await listOrdersByIds([orderId]))[0] || null;
  if (!isStartedOrder(live) && !isReadyToShipOrder(live)) {
    await db.query("delete from ym_orders where order_id = $1 and assembly_id = $2", [orderId, req.params.id]);
    await refreshAssemblyStatuses([req.params.id]);
    await audit(req.sessionUser.id, "external_order_remove", "order", orderId, { assemblyId: req.params.id, status: live?.status || "MISSING", substatus: live?.substatus || "MISSING" });
    return res.json({ ok: true, action: "removed" });
  }

  const refreshed = (await enrichOrders([live]))[0];
  if (refreshed.hasMappingProblem) return res.status(409).json({ error: "В обновлённом заказе есть товар без складской привязки или штрихкода" });
  if (refreshed.hasMandatoryMarking) return res.status(409).json({ error: "Обновлённый заказ требует обязательную маркировку" });
  const client = await db.connect();
  try {
    await client.query("begin");
    await client.query("delete from ym_order_items where order_id = $1", [orderId]);
    for (const item of refreshed.items) {
      await client.query(
        `insert into ym_order_items(order_id, item_id, offer_id, name, quantity, barcodes, picture_url,
                                    required_instance_types, warehouse_product_id)
         values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9)`,
        [orderId, item.id, item.offerId, item.name, item.quantity, JSON.stringify(item.barcodes), item.picture,
          JSON.stringify(item.requiredInstanceTypes), item.warehouseProductId]
      );
    }
    await client.query(
      `update ym_orders
          set local_state = case
                when (select launched_at from ym_assemblies where id = ym_orders.assembly_id) is null then 'draft'
                else 'assigned'
              end,
              shipment_id = $2, yandex_status = $3, yandex_substatus = $4,
              snapshot = $5::jsonb, external_snapshot = $5::jsonb, external_synced_at = now(),
              external_changed_at = null, problem_code = null, last_error = null, updated_at = now()
        where order_id = $1`,
      [orderId, refreshed.shipmentId, refreshed.status, refreshed.substatus, JSON.stringify(refreshed)]
    );
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
  await db.query(
    "update ym_assemblies set status = case when launched_at is null then 'draft' else 'assigned' end, completed_at = null, updated_at = now() where id = $1",
    [req.params.id]
  );
  await audit(req.sessionUser.id, "external_order_refresh", "order", orderId, { assemblyId: req.params.id });
  res.json({ ok: true, action: "refreshed" });
}));

app.post("/assemblies", requireAdmin, asyncRoute(async (req, res) => {
  const orderIds = [...new Set((req.body?.orderIds || []).map(Number).filter(Number.isFinite))];
  const userIds = [...new Set((req.body?.userIds || []).map(Number).filter(Number.isFinite))];
  const allowZeroStock = req.body?.allowZeroStock === true;
  const allowNoScan = req.body?.allowNoScan !== false;
  const createAsDraft = req.body?.draft === true || !userIds.length;
  if (!orderIds.length) return res.status(400).json({ error: "Выберите заказы" });
  if (userIds.length) {
    const users = await db.query(
      "select id from users where role = 'employee' and id = any($1::bigint[])",
      [userIds]
    );
    if (users.rowCount !== userIds.length) return res.status(409).json({ error: "Один из сотрудников недоступен" });
  }

  const live = await listStartedOrders();
  const liveMap = new Map(live.map((order) => [String(order.id), order]));
  const missing = orderIds.filter((id) => !liveMap.has(String(id)));
  if (missing.length) return res.status(409).json({ error: `Заказы уже изменились в Яндексе: ${missing.join(", ")}` });
  const selected = await enrichOrders(orderIds.map((id) => liveMap.get(String(id))));
  const selectedCampaignIds = [...new Set(selected.map((order) => Number(order.campaignId)).filter(Number.isFinite))];
  if (selectedCampaignIds.length !== 1) {
    return res.status(409).json({ error: "Заказы разных магазинов Яндекса нужно создавать отдельными сборками" });
  }
  const selectedCampaignId = selectedCampaignIds[0];
  const mappingProblems = selected.filter((order) => order.hasMappingProblem);
  if (mappingProblems.length) {
    return res.status(409).json({
      error: "Есть товары без складской привязки или штрихкода",
      problemOrderIds: mappingProblems.map((order) => order.id),
    });
  }
  const marked = selected.filter((order) => order.hasMandatoryMarking);
  if (marked.length) {
    return res.status(409).json({
      error: "В выбранных заказах обязательная маркировка. Этот сценарий пока заблокирован, чтобы не отправить заказ без кода.",
      problemOrderIds: marked.map((order) => order.id),
    });
  }
  const shortages = stockShortages(selected);
  if (shortages.length && !allowZeroStock) {
    return res.status(409).json({
      error: `Недостаточно товара на складе: ${shortages.map((item) => `${item.name || item.offerId} — нужно ${item.required}, есть ${item.available}`).join("; ")}`,
      stockShortages: shortages,
    });
  }

  const id = crypto.randomUUID();
  const defaultName = `Сборка Яндекс · ${campaignName(selectedCampaignId)} · ${new Date().toLocaleString("ru-RU", { timeZone: "Europe/Moscow", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" })}`;
  const name = String(req.body?.name || defaultName).trim().slice(0, 160) || defaultName;
  const client = await db.connect();
  try {
    await client.query("begin");
    const duplicate = await client.query("select order_id from ym_orders where order_id = any($1::bigint[])", [orderIds]);
    if (duplicate.rowCount) throw Object.assign(new Error("Часть заказов уже добавлена в другую сборку"), { statusCode: 409 });
    await client.query(
      `insert into ym_assemblies(id, name, business_id, campaign_id, warehouse_id, allow_zero_stock, allow_no_scan, status, launched_at, created_by)
       values ($1, $2, $3, $4, $5, $6, $7, $8, case when $8 = 'draft' then null else now() end, $9)`,
      [id, name, BUSINESS_ID, selectedCampaignId, WAREHOUSE_ID, allowZeroStock, allowNoScan, createAsDraft ? "draft" : "assigned", req.sessionUser.id]
    );
    for (const userId of userIds) {
      await client.query("insert into ym_assembly_users(assembly_id, user_id) values ($1, $2)", [id, userId]);
    }
    for (let index = 0; index < selected.length; index += 1) {
      const order = selected[index];
      const assignedUserId = userIds.length ? userIds[index % userIds.length] : null;
      await client.query(
        `insert into ym_orders(order_id, assembly_id, assigned_user_id, shipment_id, yandex_status, yandex_substatus, local_state, snapshot, external_synced_at)
         values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now())`,
        [order.id, id, assignedUserId, order.shipmentId, order.status, order.substatus, createAsDraft ? "draft" : "assigned", JSON.stringify(order)]
      );
      for (const item of order.items) {
        await client.query(
          `insert into ym_order_items(order_id, item_id, offer_id, name, quantity, barcodes, picture_url,
                                      required_instance_types, warehouse_product_id)
           values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9)`,
          [order.id, item.id, item.offerId, item.name, item.quantity, JSON.stringify(item.barcodes), item.picture,
            JSON.stringify(item.requiredInstanceTypes), item.warehouseProductId]
        );
      }
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
  invalidateWaitingCache();
  await audit(req.sessionUser.id, "assembly_create", "assembly", id, { orderIds, userIds, campaignId: selectedCampaignId, allowZeroStock, allowNoScan, createAsDraft, stockShortages: shortages });
  res.status(201).json({ ok: true, id, name, campaignId: selectedCampaignId, allowZeroStock, allowNoScan, status: createAsDraft ? "draft" : "assigned" });
}));

app.get("/shipments", requireAdmin, asyncRoute(async (_req, res) => {
  const shipments = await decorateShipmentList(await listShipments());
  res.json({ shipments, syncedAt: new Date().toISOString() });
}));

app.post("/shipments/:id/places", requireAdmin, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.id);
  const placesCount = Number(req.body?.placesCount);
  res.json({ ok: true, shipment: await setShipmentPlaces(shipmentId, placesCount, req.sessionUser.id) });
}));

app.post("/shipments/:id/confirm", requireAdmin, asyncRoute(async (req, res) => {
  res.json(await confirmShipment(Number(req.params.id), req.sessionUser.id));
}));

app.get("/shipments/:id/waybill", requireAdmin, asyncRoute(async (req, res) => {
  const campaignId = await resolveShipmentCampaignId(Number(req.params.id));
  const file = await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments/${encodeURIComponent(req.params.id)}/transportation-waybill`, { responseType: "buffer" });
  res.type(file.contentType).send(file.buffer);
}));

app.get("/shipments/:id/pallet-labels", requireAdmin, asyncRoute(async (req, res) => {
  const file = await getPalletLabels(Number(req.params.id));
  await audit(req.sessionUser.id, "shipment_pallet_labels_download", "shipment", req.params.id, {
    placesCount: file.placesCount, cached: file.cached, copiesPerPackage: 2,
  });
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.get("/shipments/:id/pallet-label-link", requireAdmin, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.id);
  const file = await getPalletLabels(shipmentId);
  const token = createLabelToken({ documentType: "pallet", shipmentId, userId: req.sessionUser.id });
  await audit(req.sessionUser.id, "shipment_pallet_labels_print", "shipment", shipmentId, {
    placesCount: file.placesCount, cached: file.cached, copiesPerPackage: 2,
  });
  res.json({
    url: `/sborka/api/ym/shipment-labels/${encodeURIComponent(token)}`,
    placesCount: file.placesCount,
    copiesPerPackage: 2,
  });
}));

app.get("/shipments/:id/act", requireAdmin, asyncRoute(async (req, res) => {
  const campaignId = await resolveShipmentCampaignId(Number(req.params.id));
  const file = await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments/${encodeURIComponent(req.params.id)}/act`, { responseType: "buffer" });
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.get("/employee/assemblies", requireEmployee, asyncRoute(async (req, res) => {
  const result = await db.query(
    `select a.id, a.name, a.campaign_id, a.status, a.created_at,
            count(distinct o.order_id)::int as orders_total,
            count(distinct o.order_id) filter (where o.local_state = 'completed')::int as orders_completed,
            coalesce(sum(i.quantity), 0)::int as units_total,
            coalesce(sum(i.picked_qty), 0)::int as units_picked
      from ym_assemblies a
      join ym_orders o on o.assembly_id = a.id and o.assigned_user_id = $1
      left join ym_order_items i on i.order_id = o.order_id
      where a.status not in ('cancelled', 'needs_attention')
        and o.local_state in ('assigned', 'packing', 'completed')
        and (o.local_state <> 'completed' or a.completed_at > now() - interval '14 days')
      group by a.id
      order by a.created_at`,
    [req.sessionUser.id]
  );
  res.json({ assemblies: result.rows.map((row) => ({ ...row, campaign_name: campaignName(row.campaign_id) })) });
}));

app.get("/employee/assemblies/:id", requireEmployee, asyncRoute(async (req, res) => {
  await reconcileOrders({ assemblyId: req.params.id }).catch((error) => {
    console.error(`[ym] employee reconciliation ${req.params.id}:`, error);
  });
  const detail = await getAssemblyDetail(req.params.id, req.sessionUser.id);
  if (!detail || !detail.orders.length) return res.status(404).json({ error: "Сборка не найдена или не назначена вам" });
  detail.products = await employeeProductRows(req.params.id, req.sessionUser.id);
  res.json(detail);
}));

app.get("/employee/assemblies/:id/shipments", requireEmployee, asyncRoute(async (req, res) => {
  const access = await db.query(
    "select 1 from ym_assembly_users where assembly_id = $1 and user_id = $2",
    [req.params.id, req.sessionUser.id]
  );
  if (!access.rowCount) return res.status(404).json({ error: "Сборка не найдена или не назначена вам" });
  const rows = await db.query(
    `select distinct o.shipment_id, a.campaign_id
       from ym_orders o join ym_assemblies a on a.id = o.assembly_id
      where o.assembly_id = $1 and o.shipment_id is not null
      order by o.shipment_id`,
    [req.params.id]
  );
  const shipments = [];
  for (const row of rows.rows) {
    const shipment = await getShipment(Number(row.shipment_id), { campaignId: Number(row.campaign_id) });
    if (shipment) shipments.push(shipment);
  }
  res.json({ shipments: await decorateShipmentList(shipments, { includeReadiness: true }) });
}));

app.post("/employee/assemblies/:id/shipments/:shipmentId/places", requireEmployee, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.shipmentId);
  await assertEmployeeShipmentAccess(req.params.id, shipmentId, req.sessionUser.id);
  const placesCount = Number(req.body?.placesCount);
  res.json({ ok: true, shipment: await setShipmentPlaces(shipmentId, placesCount, req.sessionUser.id) });
}));

app.get("/employee/assemblies/:id/shipments/:shipmentId/pallet-labels", requireEmployee, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.shipmentId);
  await assertEmployeeShipmentAccess(req.params.id, shipmentId, req.sessionUser.id);
  const file = await getPalletLabels(shipmentId);
  await audit(req.sessionUser.id, "shipment_pallet_labels_download", "shipment", shipmentId, {
    assemblyId: req.params.id, placesCount: file.placesCount, cached: file.cached, copiesPerPackage: 2,
  });
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.get("/employee/assemblies/:id/shipments/:shipmentId/pallet-label-link", requireEmployee, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.shipmentId);
  await assertEmployeeShipmentAccess(req.params.id, shipmentId, req.sessionUser.id);
  const file = await getPalletLabels(shipmentId);
  const token = createLabelToken({ documentType: "pallet", shipmentId, userId: req.sessionUser.id });
  await audit(req.sessionUser.id, "shipment_pallet_labels_print", "shipment", shipmentId, {
    assemblyId: req.params.id, placesCount: file.placesCount, cached: file.cached, copiesPerPackage: 2,
  });
  res.json({
    url: `/sborka/api/ym/shipment-labels/${encodeURIComponent(token)}`,
    placesCount: file.placesCount,
    copiesPerPackage: 2,
  });
}));

app.post("/employee/assemblies/:id/shipments/:shipmentId/confirm", requireEmployee, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.shipmentId);
  await assertEmployeeShipmentAccess(req.params.id, shipmentId, req.sessionUser.id);
  res.json(await confirmShipment(shipmentId, req.sessionUser.id));
}));

app.get("/employee/assemblies/:id/shipments/:shipmentId/act", requireEmployee, asyncRoute(async (req, res) => {
  const shipmentId = Number(req.params.shipmentId);
  await assertEmployeeShipmentAccess(req.params.id, shipmentId, req.sessionUser.id);
  const campaignId = await resolveShipmentCampaignId(shipmentId);
  const file = await marketRequest(`/v2/campaigns/${campaignId}/first-mile/shipments/${shipmentId}/act`, { responseType: "buffer" });
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.post("/employee/assemblies/:id/pick-scan", requireEmployee, asyncRoute(async (req, res) => {
  const barcode = String(req.body?.barcode || "").replace(/\s+/g, "").trim();
  if (!barcode) return res.status(400).json({ error: "Пустой штрихкод" });
  if (!req.sessionUser.externalEmployeeId) return res.status(409).json({ error: "У сотрудника нет складской привязки" });
  const scannedWarehouseProductId = await resolveScannedWarehouseProductId(barcode);
  await reconcileOrders({ assemblyId: req.params.id }).catch((error) => {
    console.error(`[ym] pick reconciliation ${req.params.id}:`, error);
  });
  const client = await db.connect();
  let requestId = "";
  let duplicate = false;
  try {
    await client.query("begin");
    const scanRequest = await beginScanRequest(client, {
      requestId: req.body?.requestId,
      userId: req.sessionUser.id,
      action: "pick",
      assemblyId: req.params.id,
      barcode,
    });
    requestId = scanRequest.requestId;
    duplicate = scanRequest.duplicate;
    if (duplicate) {
      await client.query("commit");
      return res.json({ ...scanRequest.response, ok: true, idempotent: true, requestId, products: await employeeProductRows(req.params.id, req.sessionUser.id) });
    }
    const result = await client.query(
      `select i.*, o.assembly_id, a.allow_zero_stock
         from ym_order_items i
         join ym_orders o on o.order_id = i.order_id
         join ym_assemblies a on a.id = o.assembly_id
        where o.assembly_id = $1 and o.assigned_user_id = $2 and o.local_state = 'assigned'
          and i.picked_qty < i.quantity
          and (
            exists (select 1 from jsonb_array_elements_text(i.barcodes) b where upper(regexp_replace(b, '\\s+', '', 'g')) = upper($3))
            or ($4::bigint is not null and i.warehouse_product_id = $4)
          )
        order by o.order_id, i.item_id
        for update of i, o
        limit 1`,
      [req.params.id, req.sessionUser.id, barcode, scannedWarehouseProductId]
    );
    if (!result.rowCount) throw Object.assign(new Error("Этот товар не требуется или уже полностью собран"), { statusCode: 409 });
    const item = result.rows[0];
    const operationKey = `ym_pick_request:${requestId}`;
    const movement = await moveProductFromShelfToEmployee({
      externalEmployeeId: req.sessionUser.externalEmployeeId,
      quantity: 1,
      barcodes: item.barcodes,
      article: item.offer_id,
      resolvedProductIds: [Number(item.warehouse_product_id)],
      allowArticleFallback: true,
      warehouseId: SKLAD_WAREHOUSE_ID,
      preventNegative: true,
      skipUnavailable: item.allow_zero_stock === true,
      idempotencyKey: operationKey,
      source: operationKey,
      notes: `Яндекс Маркет: сбор товара для заказа ${item.order_id}`,
    });
    await client.query(
      "update ym_order_items set picked_qty = picked_qty + 1, stock_picked_qty = stock_picked_qty + $3 where order_id = $1 and item_id = $2",
      [item.order_id, item.item_id, Number(movement?.movedQuantity || 0)]
    );
    await client.query(
      "update ym_assemblies set status = 'picking', updated_at = now() where id = $1 and status = 'assigned'",
      [req.params.id]
    );
    await finishScanRequest(client, requestId, {
      ok: true,
      orderId: Number(item.order_id),
      itemId: Number(item.item_id),
      stockMoved: Number(movement?.movedQuantity || 0),
      stockSkipped: Number(movement?.skippedQuantity || 0),
    });
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
  res.json({ ok: true, idempotent: duplicate, requestId, products: await employeeProductRows(req.params.id, req.sessionUser.id) });
}));

app.post("/employee/assemblies/:id/pick-no-scan", requireEmployee, asyncRoute(async (req, res) => {
  const offerId = String(req.body?.offerId || "").trim();
  if (!offerId) return res.status(400).json({ error: "Товар не указан" });
  if (!req.sessionUser.externalEmployeeId) return res.status(409).json({ error: "У сотрудника нет складской привязки" });
  await reconcileOrders({ assemblyId: req.params.id }).catch((error) => {
    console.error(`[ym] manual pick reconciliation ${req.params.id}:`, error);
  });
  const operationBarcode = `NO_SCAN:${offerId}`;
  const client = await db.connect();
  let requestId = "";
  let response = null;
  try {
    await client.query("begin");
    const scanRequest = await beginScanRequest(client, {
      requestId: req.body?.requestId,
      userId: req.sessionUser.id,
      action: "pick_no_scan",
      assemblyId: req.params.id,
      barcode: operationBarcode,
    });
    requestId = scanRequest.requestId;
    if (scanRequest.duplicate) {
      await client.query("commit");
      return res.json({ ...scanRequest.response, ok: true, idempotent: true, requestId, products: await employeeProductRows(req.params.id, req.sessionUser.id) });
    }
    const result = await client.query(
      `select i.*, a.allow_zero_stock, a.allow_no_scan
         from ym_order_items i
         join ym_orders o on o.order_id = i.order_id
         join ym_assemblies a on a.id = o.assembly_id
        where o.assembly_id = $1 and o.assigned_user_id = $2 and o.local_state = 'assigned'
          and i.offer_id = $3 and i.picked_qty < i.quantity
        order by o.order_id, i.item_id
        for update of i, o, a
        limit 1`,
      [req.params.id, req.sessionUser.id, offerId]
    );
    if (!result.rowCount) throw Object.assign(new Error("Этот товар не требуется или уже полностью собран"), { statusCode: 409 });
    const item = result.rows[0];
    if (item.allow_no_scan !== true) {
      throw Object.assign(new Error("В этой сборке запрещена сборка без сканирования"), { statusCode: 403 });
    }
    const operationKey = `ym_pick_request:${requestId}`;
    const movement = await moveProductFromShelfToEmployee({
      externalEmployeeId: req.sessionUser.externalEmployeeId,
      quantity: 1,
      barcodes: item.barcodes,
      article: item.offer_id,
      resolvedProductIds: [Number(item.warehouse_product_id)],
      allowArticleFallback: true,
      warehouseId: SKLAD_WAREHOUSE_ID,
      preventNegative: true,
      skipUnavailable: item.allow_zero_stock === true,
      idempotencyKey: operationKey,
      source: operationKey,
      notes: `Яндекс Маркет: ручной сбор товара для заказа ${item.order_id}`,
    });
    await client.query(
      "update ym_order_items set picked_qty = picked_qty + 1, stock_picked_qty = stock_picked_qty + $3 where order_id = $1 and item_id = $2",
      [item.order_id, item.item_id, Number(movement?.movedQuantity || 0)]
    );
    await client.query(
      "update ym_assemblies set status = 'picking', updated_at = now() where id = $1 and status = 'assigned'",
      [req.params.id]
    );
    response = {
      ok: true,
      orderId: Number(item.order_id),
      itemId: String(item.item_id),
      offerId: item.offer_id,
      stockMoved: Number(movement?.movedQuantity || 0),
      stockSkipped: Number(movement?.skippedQuantity || 0),
    };
    await finishScanRequest(client, requestId, response);
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
  res.json({ ...response, requestId, products: await employeeProductRows(req.params.id, req.sessionUser.id) });
}));

app.post("/employee/assemblies/:id/finish-picking", requireEmployee, asyncRoute(async (req, res) => {
  await reconcileOrders({ assemblyId: req.params.id, force: true });
  const active = await db.query(
    "select count(*)::int as count from ym_orders where assembly_id = $1 and assigned_user_id = $2 and local_state = 'assigned'",
    [req.params.id, req.sessionUser.id]
  );
  if (!Number(active.rows[0]?.count)) return res.status(409).json({ error: "В сборке не осталось активных заказов; сообщите администратору" });
  const pending = await db.query(
    `select count(*)::int as count
       from ym_order_items i join ym_orders o on o.order_id = i.order_id
      where o.assembly_id = $1 and o.assigned_user_id = $2 and o.local_state = 'assigned' and i.picked_qty < i.quantity`,
    [req.params.id, req.sessionUser.id]
  );
  if (Number(pending.rows[0]?.count) > 0) return res.status(409).json({ error: "Сначала соберите все товары" });
  await db.query(
    `update ym_orders set local_state = 'packing', updated_at = now()
      where assembly_id = $1 and assigned_user_id = $2 and local_state = 'assigned'`,
    [req.params.id, req.sessionUser.id]
  );
  await db.query("update ym_assemblies set status = 'packing', updated_at = now() where id = $1", [req.params.id]);
  await audit(req.sessionUser.id, "picking_finish", "assembly", req.params.id);
  res.json({ ok: true });
}));

app.post("/employee/assemblies/:id/orders/:orderId/scan", requireEmployee, asyncRoute(async (req, res) => {
  const barcode = String(req.body?.barcode || "").replace(/\s+/g, "").trim();
  const orderId = Number(req.params.orderId);
  if (!barcode) return res.status(400).json({ error: "Пустой штрихкод" });
  const scannedWarehouseProductId = await resolveScannedWarehouseProductId(barcode);
  await reconcileOrders({ assemblyId: req.params.id }).catch((error) => {
    console.error(`[ym] pack reconciliation ${req.params.id}:`, error);
  });
  const client = await db.connect();
  try {
    await client.query("begin");
    const scanRequest = await beginScanRequest(client, {
      requestId: req.body?.requestId,
      userId: req.sessionUser.id,
      action: "pack",
      assemblyId: req.params.id,
      orderId,
      barcode,
    });
    if (scanRequest.duplicate) {
      await client.query("commit");
      return res.json({ ...scanRequest.response, ok: true, idempotent: true, requestId: scanRequest.requestId });
    }
    const orderAccess = await client.query(
      "select local_state from ym_orders where order_id = $1 and assembly_id = $2 and assigned_user_id = $3 for update",
      [orderId, req.params.id, req.sessionUser.id]
    );
    if (!orderAccess.rowCount) throw Object.assign(new Error("Заказ не найден"), { statusCode: 404 });
    if (orderAccess.rows[0].local_state !== "packing") {
      throw Object.assign(new Error("Заказ изменён или больше не доступен для упаковки"), { statusCode: 409 });
    }
    const result = await client.query(
      `update ym_order_items i set packed_qty = packed_qty + 1
        from ym_orders o
       where i.order_id = $1 and o.order_id = i.order_id and o.assembly_id = $2 and o.assigned_user_id = $3
         and o.local_state = 'packing' and i.packed_qty < i.quantity
         and (
           exists (select 1 from jsonb_array_elements_text(i.barcodes) b where upper(regexp_replace(b, '\\s+', '', 'g')) = upper($4))
           or ($5::bigint is not null and i.warehouse_product_id = $5)
         )
       returning i.*`,
      [orderId, req.params.id, req.sessionUser.id, barcode, scannedWarehouseProductId]
    );
    if (!result.rowCount) throw Object.assign(new Error("Этот товар не входит в заказ или уже отсканирован"), { statusCode: 409 });
    const response = { ok: true, item: result.rows[0], requestId: scanRequest.requestId };
    await finishScanRequest(client, scanRequest.requestId, response);
    await client.query("commit");
    res.json(response);
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}));

app.get("/employee/assemblies/:id/orders/:orderId/label", requireEmployee, asyncRoute(async (req, res) => {
  const orderId = Number(req.params.orderId);
  const order = await loadEmployeeOrder(orderId, req.params.id, req.sessionUser.id);
  if (!order) return res.status(404).json({ error: "Заказ не найден" });
  await ensureOrderBoxLayout(order);
  const file = await marketRequest(`/v2/campaigns/${Number(order.campaign_id || CAMPAIGN_ID)}/orders/${orderId}/delivery/labels?format=${LABEL_FORMAT}`, { responseType: "buffer" });
  await db.query("update ym_orders set label_fetched_at = now(), updated_at = now() where order_id = $1", [orderId]);
  res.set("Cache-Control", "private, no-store");
  res.type(file.contentType).send(file.buffer);
}));

app.get("/employee/assemblies/:id/orders/:orderId/label-link", requireEmployee, asyncRoute(async (req, res) => {
  const orderId = Number(req.params.orderId);
  const order = await loadEmployeeOrder(orderId, req.params.id, req.sessionUser.id);
  if (!order) return res.status(404).json({ error: "Заказ не найден" });
  await ensureOrderBoxLayout(order);
  const token = createLabelToken({ orderId, assemblyId: req.params.id, userId: req.sessionUser.id });
  res.json({ url: `/sborka/api/ym/labels/${encodeURIComponent(token)}` });
}));

app.post("/employee/assemblies/:id/orders/:orderId/complete", requireEmployee, asyncRoute(async (req, res) => {
  const orderId = Number(req.params.orderId);
  let orderRow = await loadEmployeeOrder(orderId, req.params.id, req.sessionUser.id);
  if (!orderRow) return res.status(404).json({ error: "Заказ не найден" });
  if (orderRow.local_state === "completed") return res.json({ ok: true, idempotent: true });
  if (orderRow.local_state === "external_changed") return res.status(409).json({ error: orderRow.last_error || "Заказ изменён в Яндекс Маркете" });
  if (orderRow.local_state !== "packing") return res.status(409).json({ error: "Сначала завершите сбор товаров" });
  if (!orderRow.label_fetched_at) return res.status(409).json({ error: "Сначала напечатайте этикетку" });
  if (orderRow.allow_no_scan === true) {
    await db.query(
      "update ym_order_items set packed_qty = quantity where order_id = $1 and packed_qty < quantity",
      [orderId]
    );
    orderRow = await loadEmployeeOrder(orderId, req.params.id, req.sessionUser.id);
  }
  if ((orderRow.items || []).some((item) => Number(item.packedQty) !== Number(item.quantity))) {
    return res.status(409).json({ error: "Не все товары заказа отсканированы" });
  }
  orderRow = await ensureOrderBoxLayout(orderRow);

  const campaignId = Number(orderRow.campaign_id || CAMPAIGN_ID);
  const live = await getOrder(orderId, campaignId);
  if (!live) return res.status(409).json({ error: "Заказ отсутствует в Яндекс Маркете" });
  if (isStartedOrder(live)) {
    try {
      await marketRequest(`/v2/campaigns/${campaignId}/orders/${orderId}/status`, {
        method: "PUT", retrySafe: true, body: { order: { status: "PROCESSING", substatus: "READY_TO_SHIP" } },
      });
    } catch (error) {
      const reconciled = await getOrder(orderId, campaignId).catch(() => null);
      if (!isReadyToShipOrder(reconciled)) throw error;
    }
  } else if (!isReadyToShipOrder(live)) {
    const problem = externalOrderProblem(live, orderRow.items || []) || {
      code: "status_changed",
      message: `Статус изменён вне системы: ${live.status}/${live.substatus}`,
    };
    await markExternalProblem(orderId, live, problem);
    return res.status(409).json({ error: problem.message });
  }

  const writeoffKey = `ym_writeoff:${orderId}`;
  try {
    const stockItems = orderRow.items
      .map((item) => ({
        quantity: Number(item.stockPickedQty || 0),
        barcodes: item.barcodes,
        article: item.offerId,
        resolvedProductIds: [Number(item.warehouseProductId)],
        allowArticleFallback: true,
      }))
      .filter((item) => item.quantity > 0);
    if (stockItems.length) {
      await writeOffEmployeeInventoryBatch({
        externalEmployeeId: req.sessionUser.externalEmployeeId,
        preventNegative: true,
        idempotencyKey: writeoffKey,
        source: writeoffKey,
        notes: `Яндекс Маркет: завершён заказ ${orderId}`,
        items: stockItems,
      });
    }
  } catch (error) {
    await db.query(
      "update ym_orders set stock_writeoff_status = 'error', last_error = $2, ready_to_ship_at = now(), updated_at = now() where order_id = $1",
      [orderId, String(error?.message || error).slice(0, 1000)]
    );
    throw Object.assign(new Error(`Заказ готов в Яндексе, но складское списание требует повтора: ${error?.message || error}`), { statusCode: 409 });
  }
  await db.query(
    `update ym_orders set local_state = 'completed', yandex_status = 'PROCESSING', yandex_substatus = 'READY_TO_SHIP',
            stock_writeoff_status = 'done', ready_to_ship_at = now(), last_error = null, problem_code = null,
            external_synced_at = now(), updated_at = now()
      where order_id = $1`,
    [orderId]
  );
  await refreshAssemblyStatuses([req.params.id]);
  await audit(req.sessionUser.id, "order_complete", "order", orderId, {
    assemblyId: req.params.id,
    withoutScan: orderRow.allow_no_scan === true,
  });
  res.json({ ok: true });
}));

app.use((error, req, res, _next) => {
  const status = Number(error?.statusCode) || (error?.code === "23505" ? 409 : 500);
  console.error(`[ym] ${req.method} ${req.path}:`, error);
  res.status(status).json({ error: error?.message || "Внутренняя ошибка Яндекс Маркета" });
});

await ensureSchema();
const server = app.listen(PORT, HOST, () => {
  console.log(`[ym] listening on http://${HOST}:${PORT}; enabled=${ENABLED}; campaigns=${CAMPAIGN_IDS.join(",")}`);
});
const reconciliationTimer = setInterval(() => {
  reconcileOrders().catch((error) => console.error("[ym] background reconciliation failed:", error));
}, Math.max(60000, RECONCILE_STALE_MS));
reconciliationTimer.unref();
const waitingTimer = setInterval(() => {
  loadWaitingOrders({ force: true }).catch((error) => console.error("[ym] background waiting refresh failed:", error));
}, Math.max(60000, WAITING_CACHE_MS));
waitingTimer.unref();
loadWaitingOrders({ force: true }).catch((error) => console.error("[ym] initial waiting refresh failed:", error));

async function shutdown() {
  clearInterval(reconciliationTimer);
  clearInterval(waitingTimer);
  server.close(async () => {
    await db.end().catch(() => {});
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
