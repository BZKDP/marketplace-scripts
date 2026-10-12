import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import net from "node:net";
import { fileURLToPath } from "node:url";
import { Pool } from "pg";
import { PDFDocument } from "pdf-lib";
import {
  API_BASE,
  CARDS_CACHE_FILE,
  CARDS_CACHE_MS,
  CACHE_MS,
  CACHE_NEW_ORDERS_MS,
  CACHE_ORDERS_MS,
  CACHE_SUPPLIES_MS,
  CONTENT_BASE,
  COOKIE_NAME,
  COOKIE_PATH,
  COOKIE_SECURE,
  DB_HOST,
  DB_NAME,
  DB_PASSWORD,
  DB_PORT,
  DB_SSL,
  DB_USER,
  DEFAULT_SITE,
  HOST,
  LABEL_BATCH_SIZE,
  LABEL_HEIGHT,
  LABEL_TYPE,
  LABEL_WIDTH,
  MAX_CREATE_COUNT,
  NAME_BACKFILL_BATCH,
  NAME_BACKFILL_DELAY_MS,
  NAME_BACKFILL_ENABLED,
  NAME_BACKFILL_INTERVAL_MS,
  ORDER_BATCH_SIZE,
  PORT,
  PRODUCT_CACHE_MS,
  PRODUCT_NEGATIVE_CACHE_MS,
  PRODUCT_RESOLVE_BATCH,
  ROLE_ADMIN,
  ROLE_EMPLOYEE,
  ROLE_SUPER,
  ROOT_SITE,
  SESSION_TTL_MS,
  SITES_ROOT,
  SKLAD_FBS_WAREHOUSE_ID,
  SKLAD_FBS_WAREHOUSE_NAME,
  SUPPLY_SYNC_MS,
  WB_API_TOKEN,
  WB_API_TOKEN_2,
  WB_CLIENT_SECRET,
  WB_CLIENT_SECRET_2,
  WB_STORE_1_ID,
  WB_STORE_1_NAME,
  WB_STORE_2_ID,
  WB_STORE_2_NAME,
} from "./server/config.mjs";
import {
  buildLabelsPrefix,
  buildS3Key,
  buildS3Url,
  getLocalFilesDir,
  isLocalStorageKey,
  pngBase64ToPdf,
  resolveLocalFilePath,
  uploadPdfToS3,
} from "./server/services/s3.mjs";
import {
  getMsProductStatusByArticle,
  getMsProductStatusByBarcode,
  getMsStockStatusByBarcode,
  getMsStockStatusesByBarcode,
  clearMsCaches,
  isMsApiAccessError,
} from "./server/services/ms.mjs";
import {
  addWarehouseProductBarcode,
  getStockProblemReport,
  isSkladMovementAccessError,
  moveProductFromShelfToEmployee,
  transferEmployeeInventory,
  writeSborkaLiveEvent,
  writeOffEmployeeInventory,
  resolveWarehouseProductMeta,
} from "./server/services/sklad-movements.mjs";
import {
  getExternalUserById,
  isExternalAuthConfigured,
  listActiveExternalUsers,
  verifyExternalUserPassword,
} from "./server/services/external-auth.mjs";
import {
  assertSkladDbAvailable,
  clearSkladFbsCaches,
  getSkladFbsStockStatusByArticle,
  getSkladFbsStockStatusByBarcode,
  isSkladDbAccessError,
} from "./server/services/sklad-fbs.mjs";
import {
  escapeHtml,
  normalizeArticleKey,
  normalizeBarcode,
  normalizeSkus,
  normalizeStoreId,
  normalizeSupplyId,
  normalizeText,
} from "./server/utils.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOCAL_FILES_DIR = getLocalFilesDir();
try {
  fs.mkdirSync(LOCAL_FILES_DIR, { recursive: true });
} catch {}
const PRODUCT_IMAGE_CACHE_DIR = path.join(LOCAL_FILES_DIR, "_product-images");
const remoteImageJobs = new Map();
try {
  fs.mkdirSync(PRODUCT_IMAGE_CACHE_DIR, { recursive: true });
} catch {}
const storeList = [];
const storeMap = new Map();

function writeSborkaLiveEventSafe(options = {}) {
  void writeSborkaLiveEvent(options).catch((error) => {
    try {
      console.warn(
        JSON.stringify({
          tag: "WB_SBORKA_LIVE_EVENT_ERROR",
          at: new Date().toISOString(),
          eventType: options?.eventType || null,
          orderId: options?.orderId || null,
          externalEmployeeId: options?.externalEmployeeId || null,
          error: error?.message || String(error),
        })
      );
    } catch {}
  });
}

function registerStore(store) {
  if (!store || !store.id || !store.token) return;
  storeList.push(store);
  storeMap.set(store.id, store);
}

registerStore({
  id: WB_STORE_1_ID,
  name: WB_STORE_1_NAME,
  token: WB_API_TOKEN,
  clientSecret: WB_CLIENT_SECRET,
});
if (WB_API_TOKEN_2) {
  registerStore({
    id: WB_STORE_2_ID,
    name: WB_STORE_2_NAME,
    token: WB_API_TOKEN_2,
    clientSecret: WB_CLIENT_SECRET_2,
  });
}

const DEFAULT_STORE = storeList[0] || null;

function resolveStore(storeId) {
  if (!storeId) return DEFAULT_STORE;
  return storeMap.get(storeId) || DEFAULT_STORE;
}

const storeCaches = new Map();
const DB_POOL_MAX = Math.max(2, Number(process.env.DB_POOL_MAX || 3) || 3);
const DB_POOL_CONNECTION_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.DB_POOL_CONNECTION_TIMEOUT_MS || 8000) || 8000
);
const AUTH_SESSION_CACHE_MS = Math.max(
  60 * 1000,
  Number(process.env.AUTH_SESSION_CACHE_MS || 5 * 60 * 1000) || 5 * 60 * 1000
);
const AUTH_SESSION_REFRESH_THRESHOLD_MS = Math.min(30 * 60 * 1000, Math.max(2 * 60 * 1000, Math.floor(SESSION_TTL_MS / 4)));
const INTERNAL_PROXY_TOKEN = (process.env.INTERNAL_PROXY_TOKEN || "");
const authSessionCache = new Map();
function createStoreCache() {
  return {
    newOrders: { ts: 0, data: [], error: null, loading: null },
    orders: { ts: 0, data: [], error: null, loading: null },
    supplies: { ts: 0, data: [], deliveringData: [], error: null, loading: null },
    productCache: new Map(),
    cardsCache: { ts: 0, map: new Map(), loading: null },
    contentBackoffUntil: 0,
  };
}
function getStoreCache(storeId) {
  const key = storeId || DEFAULT_STORE?.id || "default";
  if (!storeCaches.has(key)) {
    storeCaches.set(key, createStoreCache());
  }
  return storeCaches.get(key);
}

function invalidateStoreListCaches(storeId) {
  const cache = getStoreCache(storeId);
  cache.newOrders.ts = 0;
  cache.orders.ts = 0;
  cache.supplies.ts = 0;
  cache.supplies.deliveringData = [];
}

const labelJobs = new Map();
const labelRepairCooldowns = new Map();
const supplySyncJobs = new Map();
const supplyTrbxSyncJobs = new Map();
const supplyManualSyncJobs = new Map();
const SUPPLY_MANUAL_SYNC_TOTAL_STEPS = 5;
const LABEL_AUTO_REPAIR_COOLDOWN_MS = 30 * 1000;
const MOSCOW_TZ = "Europe/Moscow";
const CURRENT_SITE_CODE = normalizeStoreId(ROOT_SITE) || ROOT_SITE.toLowerCase();
const EMPLOYEE_POSITION_PACKER = "packer";
const EMPLOYEE_POSITION_COLLECTOR = "collector";
const EMPLOYEE_WORKFLOW_MODE_LEGACY = "legacy";
const EMPLOYEE_WORKFLOW_MODE_PICKING = "picking";
const EMPLOYEE_WORKFLOW_MODES = new Set([EMPLOYEE_WORKFLOW_MODE_LEGACY, EMPLOYEE_WORKFLOW_MODE_PICKING]);
const EMPLOYEE_WORKFLOW_ORDERS_ONLY = "orders_only";
const EMPLOYEE_WORKFLOW_PICK_PENDING = "pick_pending";
const EMPLOYEE_WORKFLOW_PICK_ACTIVE = "pick_active";
const EMPLOYEE_WORKFLOW_STAGES = new Set([
  EMPLOYEE_WORKFLOW_ORDERS_ONLY,
  EMPLOYEE_WORKFLOW_PICK_PENDING,
  EMPLOYEE_WORKFLOW_PICK_ACTIVE,
]);
const PICKING_STATUS_NOT_REQUIRED = "not_required";
const PICKING_STATUS_PENDING = "pending";
const PICKING_STATUS_STARTED = "started";
const PICKING_STATUS_FINISHED = "finished";
const PICKING_STATUS_CANCELLED = "cancelled";
const PICKING_STATUSES = new Set([
  PICKING_STATUS_NOT_REQUIRED,
  PICKING_STATUS_PENDING,
  PICKING_STATUS_STARTED,
  PICKING_STATUS_FINISHED,
  PICKING_STATUS_CANCELLED,
]);
const ASSIGNMENT_PART_MODE_PICK_PACK = "pick_pack";
const ASSIGNMENT_PART_MODE_SPLIT = "split";
const ASSIGNMENT_PART_MODES = new Set([ASSIGNMENT_PART_MODE_PICK_PACK, ASSIGNMENT_PART_MODE_SPLIT]);
const SESSION_ROLE_PICK = "pick";
const SESSION_ROLE_PACK = "pack";
const SESSION_ROLE_PICK_PACK = "pick_pack";
const SESSION_ROLES = new Set([SESSION_ROLE_PICK, SESSION_ROLE_PACK, SESSION_ROLE_PICK_PACK]);
const HANDOFF_STATUS_NOT_REQUIRED = "not_required";
const HANDOFF_STATUS_WAITING_PICK = "waiting_pick";
const HANDOFF_STATUS_READY = "ready";
const HANDOFF_STATUS_TRANSFERRED = "transferred";
const HANDOFF_STATUSES = new Set([
  HANDOFF_STATUS_NOT_REQUIRED,
  HANDOFF_STATUS_WAITING_PICK,
  HANDOFF_STATUS_READY,
  HANDOFF_STATUS_TRANSFERRED,
]);
const ASSIGNMENT_TABLE_NUMBERS = new Set([1, 2, 3]);
const SCAN_ISSUE_STAGE_ORDER = "order_scan";
const SCAN_ISSUE_STAGE_PICK = "pick_scan";
const SCAN_ISSUE_STAGES = new Set([SCAN_ISSUE_STAGE_ORDER, SCAN_ISSUE_STAGE_PICK]);
const SCAN_LOOKUP_CACHE_MS = 20000;
const SCAN_LOOKUP_CACHE_MAX = 2000;
const scanLookupCache = new Map();
const ACCEPTED_ORDER_BARCODES_CACHE_MS = 5 * 60 * 1000;
const ACCEPTED_ORDER_BARCODES_CACHE_MAX = 5000;
const acceptedOrderBarcodesCache = new Map();
const EMPLOYEE_ORDER_SCAN_INDEX_CACHE_MS = 10 * 60 * 1000;
const EMPLOYEE_ORDER_SCAN_INDEX_MAX = 200;
const EMPLOYEE_ORDER_SCAN_INDEX_CONCURRENCY = 8;
const employeeOrderScanIndexCache = new Map();
const EMPLOYEE_PICK_SCAN_INDEX_CACHE_MS = 2 * 60 * 1000;
const EMPLOYEE_PICK_SCAN_INDEX_MAX = 300;
const EMPLOYEE_PICK_SCAN_INDEX_CONCURRENCY = 8;
const EMPLOYEE_PICK_CELLS_CONCURRENCY = 10;
const employeePickScanIndexCache = new Map();
const USER_AUTH_SOURCE_LOCAL = "local";
const USER_AUTH_SOURCE_EXTERNAL = "external";
const USER_AUTH_SOURCE_LEGACY = "legacy";
const LOCATION_IZHEVSK = "izhevsk";
const LOCATION_MOSCOW = "moscow";
const LOCATION_CODES = new Set([LOCATION_IZHEVSK, LOCATION_MOSCOW]);
const FBS_STOCK_WAREHOUSE_NAME = SKLAD_FBS_WAREHOUSE_NAME || "Ижевск FBS";
const fbsStockCheckJobs = new Map();

const sseClients = new Map();
let sseClientId = 1;
const SSE_PING_MS = 20000;
const sseLastNotify = new Map();

function getCachedAcceptedOrderBarcodes(cacheKey, force = false) {
  if (!cacheKey || force) return null;
  const cached = acceptedOrderBarcodesCache.get(cacheKey);
  if (!cached) return null;
  if (Date.now() - Number(cached.ts || 0) >= ACCEPTED_ORDER_BARCODES_CACHE_MS) {
    acceptedOrderBarcodesCache.delete(cacheKey);
    return null;
  }
  return { ...cached, barcodes: buildAcceptedScanBarcodes(cached.barcodes || []) };
}

function setCachedAcceptedOrderBarcodes(cacheKey, value = {}) {
  if (!cacheKey) return;
  if (acceptedOrderBarcodesCache.has(cacheKey)) {
    acceptedOrderBarcodesCache.delete(cacheKey);
  }
  while (acceptedOrderBarcodesCache.size >= ACCEPTED_ORDER_BARCODES_CACHE_MAX) {
    const oldestKey = acceptedOrderBarcodesCache.keys().next().value;
    if (!oldestKey) break;
    acceptedOrderBarcodesCache.delete(oldestKey);
  }
  acceptedOrderBarcodesCache.set(cacheKey, {
    ts: Date.now(),
    found: Boolean(value?.found),
    error: Boolean(value?.error),
    source: value?.source || null,
    barcodes: buildAcceptedScanBarcodes(value?.barcodes || []),
  });
}

function normalizeEmployeePosition(value) {
  const normalized = normalizeText(value).toLowerCase();
  if (!normalized) return null;
  if (normalized === EMPLOYEE_POSITION_COLLECTOR) return EMPLOYEE_POSITION_COLLECTOR;
  return normalized === EMPLOYEE_POSITION_PACKER ? EMPLOYEE_POSITION_PACKER : null;
}

function normalizeEmployeeWorkflowStage(value) {
  const normalized = normalizeText(value);
  if (!normalized) return EMPLOYEE_WORKFLOW_ORDERS_ONLY;
  return EMPLOYEE_WORKFLOW_STAGES.has(normalized)
    ? normalized
    : EMPLOYEE_WORKFLOW_ORDERS_ONLY;
}

function normalizeEmployeeWorkflowMode(value) {
  const normalized = normalizeText(value);
  if (!normalized) return EMPLOYEE_WORKFLOW_MODE_LEGACY;
  return EMPLOYEE_WORKFLOW_MODES.has(normalized) ? normalized : EMPLOYEE_WORKFLOW_MODE_LEGACY;
}

function isCollectorAllowedForSupply(settings) {
  return normalizeEmployeeWorkflowMode(settings?.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING;
}

function getEmployeeWorkflowStageForMode(mode, accessMode = "selected_split") {
  if (normalizeEmployeeWorkflowMode(mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING) {
    return EMPLOYEE_WORKFLOW_ORDERS_ONLY;
  }
  return accessMode === "selected_split" ? EMPLOYEE_WORKFLOW_PICK_PENDING : EMPLOYEE_WORKFLOW_ORDERS_ONLY;
}

function isEmployeeProductPickingEnabled(stage) {
  const normalized = normalizeEmployeeWorkflowStage(stage);
  return normalized === EMPLOYEE_WORKFLOW_PICK_PENDING || normalized === EMPLOYEE_WORKFLOW_PICK_ACTIVE;
}

function isStockMethodEnabled(settings) {
  return settings?.stock_method_enabled === true || settings?.stock_method_enabled === "t";
}

function isAllowStockShortage(settings) {
  return settings?.allow_stock_shortage === true || settings?.allow_stock_shortage === "t";
}

async function recordStockShortageSkip({ supplyId, orderId = null, stage, article = null, barcode = null, requestedQuantity, movementResult, userId = null }) {
  const skippedQuantity = Math.max(0, Number(movementResult?.skippedQuantity || 0));
  if (!skippedQuantity) return;
  const processedQuantity = Math.max(0, Number(requestedQuantity || 0) - skippedQuantity);
  try {
    await dbQuery(
      `insert into wb_stock_shortage_skips (
         supply_id, wb_order_id, stage, article, barcode,
         requested_quantity, processed_quantity, skipped_quantity, user_id, created_at
       ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())`,
      [supplyId, orderId, stage, article, barcode, Number(requestedQuantity || 0), processedQuantity, skippedQuantity, userId]
    );
  } catch (error) {
    console.error("WB stock shortage skip log failed", { supplyId, orderId, stage, error: error?.message || String(error) });
  }
}

function isEmployeeProductPickingActive(stage) {
  return normalizeEmployeeWorkflowStage(stage) === EMPLOYEE_WORKFLOW_PICK_ACTIVE;
}

function normalizePickingStatus(value) {
  const normalized = normalizeText(value);
  if (!normalized) return PICKING_STATUS_NOT_REQUIRED;
  return PICKING_STATUSES.has(normalized) ? normalized : PICKING_STATUS_NOT_REQUIRED;
}

function normalizeSessionRole(value) {
  const normalized = normalizeText(value);
  if (!normalized) return SESSION_ROLE_PICK_PACK;
  return SESSION_ROLES.has(normalized) ? normalized : SESSION_ROLE_PICK_PACK;
}

function normalizeAssignmentPartMode(value) {
  const normalized = normalizeText(value);
  if (!normalized) return ASSIGNMENT_PART_MODE_PICK_PACK;
  return ASSIGNMENT_PART_MODES.has(normalized) ? normalized : ASSIGNMENT_PART_MODE_PICK_PACK;
}

function normalizeHandoffStatus(value) {
  const normalized = normalizeText(value);
  if (!normalized) return HANDOFF_STATUS_NOT_REQUIRED;
  return HANDOFF_STATUSES.has(normalized) ? normalized : HANDOFF_STATUS_NOT_REQUIRED;
}

function normalizeAssignmentTableNo(value) {
  const tableNo = Number(value || 0);
  return ASSIGNMENT_TABLE_NUMBERS.has(tableNo) ? tableNo : null;
}

function getAssignmentPartEffectivePickerUserId(part) {
  const pickerUserId = Number(part?.picker_user_id || part?.pickerUserId || 0);
  const packerUserId = Number(part?.packer_user_id || part?.packerUserId || 0);
  return pickerUserId > 0 ? pickerUserId : packerUserId;
}

function getAssignmentPartEffectivePackerUserId(part) {
  const packerUserId = Number(part?.packer_user_id || part?.packerUserId || 0);
  return packerUserId > 0 ? packerUserId : null;
}

function isSplitAssignmentPart(part) {
  return (
    normalizeAssignmentPartMode(part?.role_mode || part?.roleMode) === ASSIGNMENT_PART_MODE_SPLIT &&
    Number(part?.picker_user_id || part?.pickerUserId || 0) > 0 &&
    Number(part?.packer_user_id || part?.packerUserId || 0) > 0 &&
    Number(part?.picker_user_id || part?.pickerUserId || 0) !== Number(part?.packer_user_id || part?.packerUserId || 0)
  );
}

function isRoleBasedSession(session) {
  return Number(session?.assignment_part_id || 0) > 0;
}

function normalizeScanIssueStage(value) {
  const normalized = normalizeText(value);
  if (!normalized) return SCAN_ISSUE_STAGE_ORDER;
  return SCAN_ISSUE_STAGES.has(normalized) ? normalized : SCAN_ISSUE_STAGE_ORDER;
}

function scanIssueStageLabel(value) {
  return normalizeScanIssueStage(value) === SCAN_ISSUE_STAGE_PICK ? "Сборка товара" : "Сборка заказов";
}

function getEmployeePositionForUser(user) {
  if (!user || user.role !== ROLE_EMPLOYEE) return null;
  return normalizeEmployeePosition(user.position) || EMPLOYEE_POSITION_PACKER;
}

function isPackerUser(user) {
  return user?.role === ROLE_EMPLOYEE && getEmployeePositionForUser(user) === EMPLOYEE_POSITION_PACKER;
}

function isCollectorUser(user) {
  return user?.role === ROLE_EMPLOYEE && getEmployeePositionForUser(user) === EMPLOYEE_POSITION_COLLECTOR;
}

function normalizeAuthSource(value) {
  return normalizeText(value).toLowerCase();
}

function normalizeSiteCode(value) {
  return normalizeStoreId(value);
}

function isLocalPrivilegedUserRow(row) {
  return (
    (row?.role === ROLE_SUPER || row?.role === ROLE_ADMIN) &&
    normalizeAuthSource(row?.auth_source || USER_AUTH_SOURCE_LOCAL) === USER_AUTH_SOURCE_LOCAL
  );
}

function isLegacyUserRow(row) {
  return normalizeAuthSource(row?.auth_source) === USER_AUTH_SOURCE_LEGACY;
}

function isCurrentSiteExternalUserRow(row) {
  return normalizeAuthSource(row?.auth_source) === USER_AUTH_SOURCE_EXTERNAL;
}

function canUseUserOnCurrentSite(row) {
  return Boolean(row) && (isLocalPrivilegedUserRow(row) || isCurrentSiteExternalUserRow(row) || isLegacyUserRow(row));
}

function splitDisplayName(fullName, fallbackLogin = "") {
  const source = normalizeText(fullName) || normalizeText(fallbackLogin);
  if (!source) {
    return { surname: "", name: "", fullName: "" };
  }
  const parts = source.split(/\s+/).filter(Boolean);
  if (parts.length === 1) {
    return { surname: parts[0], name: "", fullName: source };
  }
  return {
    surname: parts[0],
    name: parts.slice(1).join(" "),
    fullName: source,
  };
}

function getUserDisplayProfile(row) {
  const login = normalizeText(row?.external_login) || normalizeText(row?.surname);
  const rawFullName =
    normalizeText(row?.external_full_name) ||
    normalizeText([row?.surname, row?.name].filter(Boolean).join(" ")) ||
    login;
  const split = splitDisplayName(rawFullName, login);
  return {
    login,
    fullName: split.fullName,
    surname: split.surname,
    name: split.name,
  };
}

function sendSse(res, event, data) {
  const payload = data ? JSON.stringify(data) : "{}";
  res.write(`event: ${event}\n`);
  res.write(`data: ${payload}\n\n`);
}

function addSseClient(res, user) {
  const id = sseClientId++;
  const client = { id, res, userId: Number(user.id), role: user.role };
  sseClients.set(id, client);
  res.on("close", () => {
    sseClients.delete(id);
  });
  sendSse(res, "hello", { time: new Date().toISOString() });
  return client;
}

function broadcastSse(event, data, filter) {
  for (const [id, client] of sseClients) {
    if (filter && !filter(client)) continue;
    try {
      sendSse(client.res, event, data);
    } catch {
      sseClients.delete(id);
    }
  }
}

function notifySupplyUpdate(supplyId, force = false) {
  const key = String(supplyId || "");
  const now = Date.now();
  const last = sseLastNotify.get(key) || 0;
  if (!force && now - last < 800) return;
  sseLastNotify.set(key, now);
  broadcastSse("supply_update", { supplyId: key });
}

function notifyScanIssueUpdate() {
  broadcastSse("scan_issue_update", { at: new Date().toISOString() }, (client) => client.role === ROLE_ADMIN);
}

function notifyScanIssueFixed(issue) {
  if (!issue) return;
  const issueStage = normalizeScanIssueStage(issue.issue_stage);
  broadcastSse(
    "scan_issue_fixed",
    {
      issueId: Number(issue.id),
      issueStage,
      supplyId: issue.supply_id,
      orderId: issue.wb_order_id == null ? null : Number(issue.wb_order_id),
      pickItemId: issue.pick_item_id == null ? null : Number(issue.pick_item_id),
      employeeId: Number(issue.employee_id),
      scanBarcode: issue.scanned_barcode || null,
      scanPassedAt: new Date().toISOString(),
    },
    (client) =>
      (client.role === ROLE_EMPLOYEE && Number(client.userId) === Number(issue.employee_id)) ||
      client.role === ROLE_ADMIN
  );
}

setInterval(() => {
  for (const [id, client] of sseClients) {
    try {
      client.res.write(`event: ping\ndata: ${Date.now()}\n\n`);
    } catch {
      sseClients.delete(id);
    }
  }
}, SSE_PING_MS);

for (const store of storeList) {
  loadCardsCacheFromDisk(store.id);
  getCardsCache(store).catch(() => {});
}

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".pdf": "application/pdf",
  ".txt": "text/plain; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
};

function siteDist(site) {
  return path.join(SITES_ROOT, site, "dist");
}

function siteIndex(site) {
  return path.join(siteDist(site), "index.html");
}

function siteExists(site) {
  return fs.existsSync(siteIndex(site));
}

function hasExt(p) {
  return path.extname(p || "") !== "";
}

function safeJoin(base, rel) {
  const basePath = path.resolve(base);
  const target = path.resolve(base, rel);
  const baseLower = basePath.toLowerCase();
  const targetLower = target.toLowerCase();
  if (targetLower === baseLower) return target;
  if (!targetLower.startsWith(baseLower + path.sep.toLowerCase())) return null;
  return target;
}

function resolveSite(pathname) {
  if (pathname === "/" || pathname === "") {
    return { site: ROOT_SITE, subpath: "/" };
  }
  const parts = pathname.split("/").filter(Boolean);
  const candidate = parts[0];
  if (candidate && siteExists(candidate)) {
    const rest = parts.slice(1).join("/");
    return { site: candidate, subpath: rest ? `/${rest}` : "/" };
  }
  return { site: ROOT_SITE, subpath: pathname };
}

function send(res, status, body, headers = {}) {
  res.writeHead(status, { "Content-Type": "text/plain; charset=utf-8", ...headers });
  res.end(body);
}

function sendJson(res, status, data) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(JSON.stringify(data));
}

function streamFile(req, res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const type = MIME[ext] || "application/octet-stream";
  const headers = { "Content-Type": type };
  if (path.basename(filePath).toLowerCase() === "index.html") {
    headers["Cache-Control"] = "no-store";
  }
  res.writeHead(200, headers);
  if (req.method === "HEAD") {
    res.end();
    return;
  }
  fs.createReadStream(filePath).pipe(res);
}

function cacheFresh(entry, ttl = CACHE_MS) {
  return entry.data && Date.now() - entry.ts < ttl;
}

function cacheHasData(entry) {
  return Array.isArray(entry.data) && entry.data.length > 0;
}

function getCardsCacheFile(storeId) {
  if (!storeId) return CARDS_CACHE_FILE;
  const ext = path.extname(CARDS_CACHE_FILE) || ".json";
  const base = CARDS_CACHE_FILE.replace(ext, "");
  return `${base}-${storeId}${ext}`;
}

function loadCardsCacheFromDisk(storeId) {
  try {
    const filePath = getCardsCacheFile(storeId);
    if (!fs.existsSync(filePath)) return;
    const raw = fs.readFileSync(filePath, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.items)) return;
    const map = new Map();
    for (const item of parsed.items) {
      const vendorCode = normalizeArticleKey(item?.vendorCode);
      if (!vendorCode) continue;
      map.set(vendorCode, {
        ts: parsed.ts || Date.now(),
        title: item.title || null,
        barcodes: Array.isArray(item.barcodes) ? item.barcodes : [],
      });
    }
    if (map.size > 0) {
      const cache = getStoreCache(storeId);
      cache.cardsCache.map = map;
      cache.cardsCache.ts = parsed.ts || Date.now();
    }
  } catch {
    // ignore cache load errors
  }
}

function saveCardsCacheToDisk(storeId, map) {
  try {
    const items = Array.from(map.entries()).map(([vendorCode, info]) => ({
      vendorCode,
      title: info?.title || null,
      barcodes: Array.isArray(info?.barcodes) ? info.barcodes : [],
    }));
    const payload = JSON.stringify({ ts: Date.now(), items });
    const filePath = getCardsCacheFile(storeId);
    fs.writeFileSync(filePath, payload, "utf8");
  } catch {
    // ignore cache save errors
  }
}

function parseBooleanFlag(value) {
  if (value === true || value === 1) return true;
  if (value === false || value === 0 || value === null || value === undefined) return false;
  const normalized = String(value).trim().toLowerCase();
  return normalized === "true" || normalized === "1" || normalized === "yes";
}

function normalizeOrder(order, fallbackSupplyId = null) {
  if (!order) return null;
  const quantityRaw = order.quantity ?? order.qty ?? order.count ?? 1;
  const quantity = Number.isFinite(Number(quantityRaw)) ? Number(quantityRaw) : 1;
  const skus = normalizeSkus(order.skus ?? order.sku ?? order.barcodes ?? null);
  const rawBarcode = normalizeBarcode(order.barcode ?? order.barCode ?? null);
  const barcode = rawBarcode || skus[0] || null;
  const supplyId = normalizeSupplyId(order.supplyId ?? order.supplyID ?? order.supply_id ?? null);
  const resolvedSupplyId = supplyId || normalizeSupplyId(fallbackSupplyId);
  const createdAt =
    order.createdAt ?? order.created_at ?? order.dateCreated ?? order.date_created ?? null;
  const isB2B = parseBooleanFlag(
    order?.options?.isB2B ??
      order?.options?.isB2b ??
      order?.isB2B ??
      order?.isB2b ??
      order?.b2b ??
      false
  );
  return {
    id: order.id ?? order.orderId ?? order.orderID ?? order.order_id ?? null,
    createdAt,
    article: normalizeArticleKey(order.article ?? order.vendorCode ?? order.vendor_code ?? null),
    nmId: order.nmId ?? order.nmID ?? order.nm_id ?? null,
    warehouseId: order.warehouseId ?? order.warehouse_id ?? null,
    cargoType: order.cargoType ?? order.cargo_type ?? null,
    supplyId: resolvedSupplyId,
    quantity: quantity > 0 ? quantity : 1,
    skus,
    barcode,
    productName: order.productName ?? order.product_name ?? order.name ?? order.goodsName ?? null,
    isB2B,
    customerType: isB2B ? "business" : "individual",
  };
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  if (!chunks.length) return null;
  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw) return null;
  return JSON.parse(raw);
}

function parseCookies(header = "") {
  const out = {};
  if (!header) return out;
  for (const part of header.split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (!key) continue;
    out[key] = decodeURIComponent(value);
  }
  return out;
}

function isPlaceholderName(name, article) {
  const raw = name === null || name === undefined ? "" : String(name).trim();
  if (!raw) return true;
  if (raw === "—" || raw === "-" || raw === "_") return true;
  const junkMatch = raw.match(/[�\?]/g);
  if (junkMatch && junkMatch.length >= Math.max(2, Math.floor(raw.length * 0.3))) {
    return true;
  }
  const art = normalizeArticleKey(article);
  if (art && raw.toLowerCase() === art.toLowerCase()) return true;
  return false;
}

function extractCardTitle(card) {
  if (!card) return null;
  if (card.title) return card.title;
  const chars = Array.isArray(card.characteristics) ? card.characteristics : [];
  const pickValue = (value) => {
    if (Array.isArray(value)) {
      const first = value.find((v) => v !== null && v !== undefined && String(v).trim());
      return first ? String(first).trim() : null;
    }
    if (value === null || value === undefined) return null;
    const str = String(value).trim();
    return str || null;
  };
  for (const c of chars) {
    if (Number(c?.id) === 15000785) {
      const picked = pickValue(c.value);
      if (picked) return picked;
    }
  }
  for (const c of chars) {
    const name = String(c?.name || "").toLowerCase();
    if (name.includes("торгов") && name.includes("наимен")) {
      const picked = pickValue(c.value);
      if (picked) return picked;
    }
    if (name === "наименование" || name.includes("наименование")) {
      const picked = pickValue(c.value);
      if (picked) return picked;
    }
  }
  const direct = card.subjectName || card.brand || null;
  if (direct) return direct;
  return null;
}

function extractCardInfo(card) {
  if (!card) return null;
  const title = extractCardTitle(card);
  const barcodes = [];
  if (Array.isArray(card.sizes)) {
    for (const size of card.sizes) {
      const skus = normalizeSkus(size?.skus);
      for (const sku of skus) {
        if (!barcodes.includes(sku)) barcodes.push(sku);
      }
    }
  }
  const vendorCode = normalizeArticleKey(card.vendorCode || null);
  const nmId = card.nmID ?? card.nmId ?? card.nm_id ?? null;
  return { title, barcodes, vendorCode, nmId };
}

function getStoreFromRequest(req) {
  const raw = req.headers["x-store-id"] || req.headers["x-store"] || "";
  const id = normalizeStoreId(raw);
  return resolveStore(id);
}

function formatPgError(err) {
  if (!err) return null;
  const keys = [
    "code",
    "message",
    "detail",
    "hint",
    "severity",
    "where",
    "position",
    "schema",
    "table",
    "column",
    "dataType",
    "constraint",
  ];
  const out = {};
  for (const key of keys) {
    if (err[key]) out[key] = err[key];
  }
  if (!out.message) out.message = String(err);
  return out;
}

function testTcp(host, port, timeoutMs = 3000) {
  return new Promise((resolve) => {
    const started = Date.now();
    const socket = new net.Socket();
    let done = false;
    const finish = (ok, error) => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve({ ok, error: error || null, elapsedMs: Date.now() - started });
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish(true, null));
    socket.once("timeout", () => finish(false, "timeout"));
    socket.once("error", (err) => finish(false, err?.message || String(err)));
    try {
      socket.connect(port, host);
    } catch (err) {
      finish(false, err?.message || String(err));
    }
  });
}

async function testDbConnection(useSsl) {
  const pool = new Pool({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    ssl: useSsl ? { rejectUnauthorized: false } : undefined,
    connectionTimeoutMillis: 4000,
  });
  try {
    await pool.query("select 1 as ok");
    return { ok: true };
  } catch (err) {
    return { ok: false, error: formatPgError(err) };
  } finally {
    try {
      await pool.end();
    } catch {}
  }
}

async function handleDebugPage(req, res, url) {
  const started = Date.now();
  const [tcp, dbSsl, dbNoSsl] = await Promise.all([
    testTcp(DB_HOST, DB_PORT, 3000),
    testDbConnection(true),
    testDbConnection(false),
  ]);

  const rows = [
    ["time", new Date().toISOString()],
    ["node", process.version],
    ["pid", process.pid],
    ["cwd", process.cwd()],
    ["__dirname", __dirname],
    ["request_url", url?.href || req.url],
    ["host_header", req.headers.host || ""],
    ["client_ip", req.socket?.remoteAddress || ""],
    ["root_site", ROOT_SITE],
    ["cookie_path", COOKIE_PATH],
    ["db_host", DB_HOST],
    ["db_port", DB_PORT],
    ["db_user", DB_USER],
    ["db_name", DB_NAME],
    ["db_ssl_mode", DB_SSL],
    ["db_password_set", DB_PASSWORD ? "yes" : "no"],
    ["db_password_len", DB_PASSWORD ? DB_PASSWORD.length : 0],
    ["tcp_ok", String(tcp.ok)],
    ["tcp_error", tcp.error || ""],
    ["tcp_elapsed_ms", String(tcp.elapsedMs || 0)],
    ["db_ssl_true_ok", String(dbSsl.ok)],
    ["db_ssl_true_error", dbSsl.error ? JSON.stringify(dbSsl.error) : ""],
    ["db_ssl_false_ok", String(dbNoSsl.ok)],
    ["db_ssl_false_error", dbNoSsl.error ? JSON.stringify(dbNoSsl.error) : ""],
    ["elapsed_ms", String(Date.now() - started)],
  ];

  const body = `<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DB Debug</title>
  <style>
    body { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; padding: 16px; }
    table { border-collapse: collapse; width: 100%; }
    td, th { border: 1px solid #ddd; padding: 6px 8px; vertical-align: top; }
    th { text-align: left; background: #f5f5f5; }
    .muted { color: #666; }
    pre { white-space: pre-wrap; margin: 0; }
  </style>
</head>
<body>
  <h1>DB Debug</h1>
  <p class="muted">Пароль не показывается, только длина.</p>
  <table>
    <thead><tr><th>Key</th><th>Value</th></tr></thead>
    <tbody>
      ${rows.map(([k, v]) => `<tr><td>${escapeHtml(k)}</td><td><pre>${escapeHtml(v)}</pre></td></tr>`).join("")}
    </tbody>
  </table>
</body>
</html>`;
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
  res.end(body);
}
let pool = null;
let poolPromise = null;

function makePool(useSsl) {
  const nextPool = new Pool({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    application_name: "wb-main",
    ssl: useSsl ? { rejectUnauthorized: false } : undefined,
    max: DB_POOL_MAX,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: DB_POOL_CONNECTION_TIMEOUT_MS,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000,
  });
  nextPool.on("error", (error) => {
    console.error("[db-pool] unexpected idle client error:", error);
  });
  return nextPool;
}

function isRetryablePgConnectionError(error) {
  const code = String(error?.code || "").trim().toUpperCase();
  const message = String(error?.message || "").toLowerCase();
  return (
    code === "ECONNRESET" ||
    code === "53300" ||
    code === "57P05" ||
    code === "57P01" ||
    code === "57P02" ||
    code === "57P03" ||
    message.includes("remaining connection slots are reserved") ||
    message.includes("idle-session timeout") ||
    message.includes("timeout exceeded when trying to connect") ||
    message.includes("connection terminated unexpectedly") ||
    message.includes("terminating connection") ||
    message.includes("server closed the connection unexpectedly") ||
    message.includes("read econnreset") ||
    message.includes("socket hang up")
  );
}

async function closePool(currentPool = null) {
  const existingPool = currentPool || pool;
  if (existingPool && existingPool === pool) {
    pool = null;
  }
  poolPromise = null;
  try {
    if (existingPool) await existingPool.end();
  } catch {}
}

async function createAndTestPool(useSsl) {
  const nextPool = makePool(useSsl);
  try {
    await nextPool.query("select 1 as ok");
    return nextPool;
  } catch (err) {
    try {
      await nextPool.end();
    } catch {}
    throw err;
  }
}

async function getPool() {
  if (pool) return pool;
  if (poolPromise) return poolPromise;
  poolPromise = (async () => {
    try {
      let nextPool;
      if (DB_SSL === "disable") {
        nextPool = await createAndTestPool(false);
      } else if (DB_SSL === "require") {
        nextPool = await createAndTestPool(true);
      } else {
        try {
          nextPool = await createAndTestPool(true);
        } catch {
          nextPool = await createAndTestPool(false);
        }
      }
      pool = nextPool;
      return nextPool;
    } catch (error) {
      throw error;
    } finally {
      poolPromise = null;
    }
  })();
  return poolPromise;
}

async function retryDbOperation(run, label) {
  try {
    return await run();
  } catch (error) {
    if (!isRetryablePgConnectionError(error) || String(error?.code || "").trim().toUpperCase() === "53300") {
      throw error;
    }
    console.warn(`[db-pool] retrying ${label} after transient error:`, error?.message || error);
    await new Promise((resolve) => setTimeout(resolve, 150));
    return run();
  }
}

async function dbQuery(text, params) {
  return retryDbOperation(async () => {
    const activePool = await getPool();
    return activePool.query(text, params);
  }, "query");
}

async function ensureUsersSchema() {
  await dbQuery("alter table users add column if not exists position text", []);
  await dbQuery("alter table users add column if not exists site_code text", []);
  await dbQuery("alter table users add column if not exists auth_source text", []);
  await dbQuery("alter table users add column if not exists external_user_id integer", []);
  await dbQuery("alter table users add column if not exists external_employee_id integer", []);
  await dbQuery("alter table users add column if not exists external_login text", []);
  await dbQuery("alter table users add column if not exists external_full_name text", []);
  await dbQuery("alter table users add column if not exists location_code text", []);
  await dbQuery("alter table users add column if not exists location_configured_at timestamptz", []);
  await dbQuery("alter table users add column if not exists location_configured_by bigint references users(id) on delete set null", []).catch(() => {});
  await dbQuery(
    "update users set position = $1 where role = $2 and (position is null or btrim(position) = '')",
    [EMPLOYEE_POSITION_PACKER, ROLE_EMPLOYEE]
  );
  await dbQuery("update users set position = null where role <> $1 and position is not null", [ROLE_EMPLOYEE]);
  await dbQuery(
    "update users set auth_source = $1 where role = $2 and (auth_source is null or btrim(auth_source) = '')",
    [USER_AUTH_SOURCE_LOCAL, ROLE_SUPER]
  );
  await dbQuery(
    "update users set auth_source = $1 where role <> $2 and (auth_source is null or btrim(auth_source) = '')",
    [USER_AUTH_SOURCE_LEGACY, ROLE_SUPER]
  );
  await dbQuery("alter table users drop constraint if exists users_position_check", []);
  await dbQuery(
    "alter table users add constraint users_position_check check (position is null or position = 'packer')",
    []
  ).catch(() => {});
  await dbQuery(
    "create unique index if not exists idx_users_site_external_user on users(site_code, external_user_id) where external_user_id is not null and site_code is not null",
    []
  );
  await dbQuery(
    "create unique index if not exists idx_users_site_external_login on users(site_code, lower(external_login)) where external_login is not null and site_code is not null",
    []
  );
  await dbQuery(
    "create unique index if not exists idx_users_external_user_global on users(external_user_id) where auth_source = 'external' and external_user_id is not null",
    []
  );
  await dbQuery(
    "create unique index if not exists idx_users_external_login_global on users(lower(external_login)) where auth_source = 'external' and external_login is not null",
    []
  );
}

async function ensureLocationSchema() {
  await dbQuery(
    `create table if not exists site_locations (
      location_code text primary key,
      label text not null,
      sort_order integer not null default 100,
      active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )`,
    []
  );
  await dbQuery(
    `insert into site_locations (location_code, label, sort_order)
     values ('izhevsk', 'Ижевск', 10), ('moscow', 'Москва', 20)
     on conflict (location_code) do update
     set label = excluded.label,
         sort_order = excluded.sort_order,
         active = true,
         updated_at = now()`,
    []
  );
  await dbQuery(
    `create table if not exists site_location_warehouse_settings (
      id bigserial primary key,
      location_code text not null references site_locations(location_code),
      marketplace text not null check (marketplace in ('all', 'wb', 'oz')),
      store_id text,
      marketplace_warehouse_id text,
      marketplace_warehouse_name text,
      sklad_warehouse_id integer,
      sklad_warehouse_name text,
      active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      updated_by bigint references users(id) on delete set null
    )`,
    []
  );
  await dbQuery(
    `create unique index if not exists idx_site_location_warehouse_settings_unique
     on site_location_warehouse_settings (
       location_code,
       marketplace,
       coalesce(store_id, ''),
       coalesce(marketplace_warehouse_id, '')
     )`,
    []
  );
  await dbQuery("alter table supply_settings add column if not exists location_code text", []);
  await dbQuery("alter table supply_settings add column if not exists sklad_warehouse_id integer", []);
  await dbQuery("alter table supply_settings add column if not exists sklad_warehouse_name text", []);
}

async function ensureStoreSchema() {
  await dbQuery("alter table supply_settings add column if not exists store_id text", []);
  await dbQuery("alter table supply_settings add column if not exists store_name text", []);
  if (DEFAULT_STORE?.id) {
    await dbQuery(
      "update supply_settings set store_id = $1, store_name = $2 where store_id is null",
      [DEFAULT_STORE.id, DEFAULT_STORE.name]
    );
  }
  for (const store of storeList) {
    if (!store?.id) continue;
    await dbQuery(
      "update supply_settings set store_name = $2 where store_id = $1",
      [store.id, store.name]
    );
  }
}

async function ensureSupplyTrbxSchema() {
  await dbQuery("alter table supply_settings add column if not exists trbx_status text", []);
  await dbQuery("alter table supply_settings add column if not exists trbx_count integer", []);
  await dbQuery("alter table supply_settings add column if not exists trbx_last_synced_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists trbx_error text", []);
  await dbQuery("alter table supply_settings add column if not exists supply_barcode text", []);
  await dbQuery("alter table supply_settings add column if not exists supply_qr_pdf_url text", []);
  await dbQuery("alter table supply_settings add column if not exists supply_qr_pdf_s3_key text", []);
  await dbQuery("alter table supply_settings add column if not exists supply_qr_pdf_format text", []);
  await dbQuery("alter table supply_settings add column if not exists supply_qr_loaded_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists supply_qr_error text", []);
  await dbQuery("alter table supply_settings add column if not exists wb_supply_done boolean", []);
  await dbQuery("alter table supply_settings add column if not exists wb_supply_created_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists wb_supply_closed_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists wb_supply_scan_dt timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists wb_destination_office_id bigint", []);
  await dbQuery("alter table supply_settings add column if not exists wb_cargo_type integer", []);
  await dbQuery("alter table supply_settings add column if not exists allow_no_scan boolean", []);
  await dbQuery("update supply_settings set allow_no_scan = false where allow_no_scan is null", []);
  await dbQuery("alter table supply_settings alter column allow_no_scan set default false", []).catch(() => {});
  await dbQuery("alter table supply_settings add column if not exists partial_safe_scan boolean", []);
  await dbQuery("update supply_settings set partial_safe_scan = false where partial_safe_scan is null", []);
  await dbQuery("alter table supply_settings alter column partial_safe_scan set default false", []).catch(() => {});
  await dbQuery("alter table supply_settings alter column partial_safe_scan set not null", []).catch(() => {});
  await dbQuery("alter table supply_settings add column if not exists desktop_print boolean", []);
  await dbQuery("update supply_settings set desktop_print = false where desktop_print is null", []);
  await dbQuery("alter table supply_settings alter column desktop_print set default false", []).catch(() => {});
  await dbQuery("alter table supply_settings add column if not exists allow_employee_close boolean", []);
  await dbQuery("update supply_settings set allow_employee_close = false where allow_employee_close is null", []);
  await dbQuery("alter table supply_settings alter column allow_employee_close set default false", []).catch(() => {});
  await dbQuery("alter table supply_settings add column if not exists employee_close_completed_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists employee_close_completed_by bigint", []);
  await dbQuery("alter table supply_settings add column if not exists allow_stock_shortage boolean", []);
  await dbQuery("update supply_settings set allow_stock_shortage = false where allow_stock_shortage is null", []);
  await dbQuery("alter table supply_settings alter column allow_stock_shortage set default false", []).catch(() => {});
  await dbQuery(
    `create table if not exists wb_stock_shortage_skips (
       id bigserial primary key,
       supply_id text not null references supply_settings(supply_id) on delete cascade,
       wb_order_id bigint,
       stage text not null,
       article text,
       barcode text,
       requested_quantity integer not null,
       processed_quantity integer not null,
       skipped_quantity integer not null,
       user_id bigint,
       created_at timestamptz not null default now()
     )`,
    []
  );
  await dbQuery("create index if not exists idx_wb_stock_shortage_skips_supply on wb_stock_shortage_skips(supply_id, created_at desc)", []);
  await dbQuery("update supply_settings set trbx_status = 'idle' where trbx_status is null or btrim(trbx_status) = ''", []);
  await dbQuery("update supply_settings set trbx_count = 0 where trbx_count is null", []);
  await dbQuery("update supply_settings set wb_supply_done = false where wb_supply_done is null", []);
  await dbQuery("alter table supply_settings alter column trbx_status set default 'idle'", []);
  await dbQuery("alter table supply_settings alter column trbx_count set default 0", []);
  await dbQuery("alter table supply_settings alter column wb_supply_done set default false", []);
  await dbQuery(
    `create table if not exists supply_trbxes (
      id bigserial primary key,
      supply_id text not null references supply_settings(supply_id) on delete cascade,
      trbx_id text not null,
      orders_count integer not null default 0,
      wb_barcode text,
      pdf_url text,
      pdf_s3_key text,
      pdf_format text,
      qr_loaded_at timestamptz,
      qr_error text,
      created_at timestamptz not null default now(),
      synced_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      unique (supply_id, trbx_id)
    )`,
    []
  );
  await dbQuery(
    "create index if not exists idx_supply_trbxes_supply_created on supply_trbxes(supply_id, created_at asc, trbx_id asc)",
    []
  );
}

async function ensureSupplyManualSyncSchema() {
  await dbQuery("alter table supply_settings add column if not exists manual_sync_status text", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_total_steps integer", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_completed_steps integer", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_message text", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_error text", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_started_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_finished_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_last_synced_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_removed_orders integer", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_released_orders integer", []);
  await dbQuery("alter table supply_settings add column if not exists manual_sync_reconciled_sessions integer", []);
  await dbQuery(
    "update supply_settings set manual_sync_status = 'idle' where manual_sync_status is null or btrim(manual_sync_status) = ''",
    []
  );
  await dbQuery("update supply_settings set manual_sync_total_steps = 0 where manual_sync_total_steps is null", []);
  await dbQuery("update supply_settings set manual_sync_completed_steps = 0 where manual_sync_completed_steps is null", []);
  await dbQuery("update supply_settings set manual_sync_removed_orders = 0 where manual_sync_removed_orders is null", []);
  await dbQuery("update supply_settings set manual_sync_released_orders = 0 where manual_sync_released_orders is null", []);
  await dbQuery("update supply_settings set manual_sync_reconciled_sessions = 0 where manual_sync_reconciled_sessions is null", []);
  await dbQuery("alter table supply_settings alter column manual_sync_status set default 'idle'", []);
  await dbQuery("alter table supply_settings alter column manual_sync_total_steps set default 0", []);
  await dbQuery("alter table supply_settings alter column manual_sync_completed_steps set default 0", []);
  await dbQuery("alter table supply_settings alter column manual_sync_removed_orders set default 0", []);
  await dbQuery("alter table supply_settings alter column manual_sync_released_orders set default 0", []);
  await dbQuery("alter table supply_settings alter column manual_sync_reconciled_sessions set default 0", []);
}

async function ensureWorkTrackingSchema() {
  await dbQuery("alter table supply_settings add column if not exists stock_method_enabled boolean", []);
  await dbQuery("update supply_settings set stock_method_enabled = true where stock_method_enabled is null", []);
  await dbQuery(
    `alter table supply_settings
     alter column stock_method_enabled set default true`,
    []
  );
  await dbQuery(
    `alter table supply_settings
     alter column stock_method_enabled set not null`,
    []
  );
  await dbQuery("alter table supply_settings add column if not exists employee_workflow_mode text", []);
  await dbQuery("alter table supply_settings add column if not exists employee_workflow_stage text", []);
  await dbQuery("alter table supply_settings add column if not exists product_picking_started_at timestamptz", []);
  await dbQuery("alter table supply_settings add column if not exists product_picking_started_by bigint", []);
  await dbQuery(
    `do $$
     begin
       if not exists (
         select 1 from pg_constraint where conname = 'fk_supply_settings_product_picking_started_by'
       ) then
         alter table supply_settings
         add constraint fk_supply_settings_product_picking_started_by
         foreign key (product_picking_started_by) references users(id) on delete set null;
       end if;
     end $$`,
    []
  );
  await dbQuery(
    `update supply_settings
     set employee_workflow_mode = $1
     where employee_workflow_mode is null or employee_workflow_mode not in ('legacy', 'picking')`,
    [EMPLOYEE_WORKFLOW_MODE_PICKING]
  );
  await dbQuery(
    `alter table supply_settings
     alter column employee_workflow_mode set default 'picking'`,
    []
  );
  await dbQuery(
    `alter table supply_settings
     alter column employee_workflow_mode set not null`,
    []
  );
  await dbQuery("alter table supply_settings drop constraint if exists supply_settings_employee_workflow_mode_check", []);
  await dbQuery(
    "alter table supply_settings add constraint supply_settings_employee_workflow_mode_check check (employee_workflow_mode in ('legacy', 'picking'))",
    []
  );
  await dbQuery(
    `update supply_settings
     set employee_workflow_stage = case
       when coalesce(access_mode, 'hidden') = 'selected_split' then $1
       else $2
     end
     where employee_workflow_stage is null or employee_workflow_stage not in ('orders_only', 'pick_pending', 'pick_active')`,
    [EMPLOYEE_WORKFLOW_PICK_PENDING, EMPLOYEE_WORKFLOW_ORDERS_ONLY]
  );
  await dbQuery(
    `alter table supply_settings
     alter column employee_workflow_stage set default 'orders_only'`,
    []
  );
  await dbQuery(
    `alter table supply_settings
     alter column employee_workflow_stage set not null`,
    []
  );
  await dbQuery("alter table supply_settings drop constraint if exists supply_settings_employee_workflow_stage_check", []);
  await dbQuery(
    "alter table supply_settings add constraint supply_settings_employee_workflow_stage_check check (employee_workflow_stage in ('orders_only', 'pick_pending', 'pick_active'))",
    []
  );
  await dbQuery("alter table supply_orders add column if not exists collected_session_id bigint", []);
  await dbQuery(
    `create table if not exists employee_supply_sessions (
      id bigserial primary key,
      supply_id text not null references supply_settings(supply_id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      assigned_by bigint references users(id) on delete set null,
      assignment_mode text not null default 'split' check (assignment_mode in ('split', 'redistribute')),
      status text not null default 'assigned' check (status in ('assigned', 'started', 'finished', 'reassigned')),
      assigned_at timestamptz not null default now(),
      started_at timestamptz,
      finished_at timestamptz,
      assigned_order_count integer not null default 0,
      collected_order_count integer not null default 0,
      total_seconds integer not null default 0,
      first_collected_at timestamptz,
      last_collected_at timestamptz,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )`,
    []
  );
  await dbQuery("alter table employee_supply_sessions add column if not exists picking_status text", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists picking_started_at timestamptz", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists picking_finished_at timestamptz", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists picking_total_seconds integer not null default 0", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists picking_required_qty integer not null default 0", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists picking_collected_qty integer not null default 0", []);
  await dbQuery(
    "update employee_supply_sessions set picking_status = $1 where picking_status is null or picking_status not in ('not_required', 'pending', 'started', 'finished', 'cancelled')",
    [PICKING_STATUS_NOT_REQUIRED]
  );
  await dbQuery(
    `alter table employee_supply_sessions
     alter column picking_status set default 'not_required'`,
    []
  );
  await dbQuery(
    `alter table employee_supply_sessions
     alter column picking_status set not null`,
    []
  );
  await dbQuery("alter table employee_supply_sessions drop constraint if exists employee_supply_sessions_picking_status_check", []);
  await dbQuery(
    "alter table employee_supply_sessions add constraint employee_supply_sessions_picking_status_check check (picking_status in ('not_required', 'pending', 'started', 'finished', 'cancelled'))",
    []
  );
  await dbQuery(
    `create table if not exists employee_supply_pick_items (
      id bigserial primary key,
      session_id bigint not null references employee_supply_sessions(id) on delete cascade,
      supply_id text not null references supply_settings(supply_id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      item_key text not null,
      article text,
      barcode text,
      product_name text,
      nm_id bigint,
      required_qty integer not null default 0,
      picked_qty integer not null default 0,
      bypass_without_scan_allowed boolean not null default false,
      bypass_authorized_at timestamptz,
      bypass_authorized_by bigint references users(id) on delete set null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      unique (session_id, item_key)
    )`,
    []
  );
  await dbQuery("alter table employee_supply_pick_items add column if not exists bypass_without_scan_allowed boolean not null default false", []);
  await dbQuery("alter table employee_supply_pick_items add column if not exists bypass_authorized_at timestamptz", []);
  await dbQuery("alter table employee_supply_pick_items add column if not exists bypass_authorized_by bigint references users(id) on delete set null", []);
  await dbQuery(
    "create index if not exists idx_employee_supply_pick_items_session on employee_supply_pick_items(session_id, updated_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_supply_pick_items_supply_user on employee_supply_pick_items(supply_id, user_id, updated_at desc)",
    []
  );
  await dbQuery(
    `do $$
     begin
       if not exists (
         select 1 from pg_constraint where conname = 'fk_supply_orders_collected_session'
       ) then
         alter table supply_orders
         add constraint fk_supply_orders_collected_session
         foreign key (collected_session_id) references employee_supply_sessions(id) on delete set null;
       end if;
     end $$`,
    []
  );
  await dbQuery("create index if not exists idx_supply_orders_collected_session on supply_orders(collected_session_id)", []);
  await dbQuery(
    "create index if not exists idx_employee_supply_sessions_supply_user on employee_supply_sessions(supply_id, user_id, assigned_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_supply_sessions_user_started on employee_supply_sessions(user_id, started_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_supply_sessions_user_assigned on employee_supply_sessions(user_id, assigned_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_supply_sessions_user_picking on employee_supply_sessions(user_id, picking_started_at desc)",
    []
  );
  await dbQuery(
    `create table if not exists supply_assignment_parts (
      id bigserial primary key,
      supply_id text not null references supply_settings(supply_id) on delete cascade,
      part_no integer not null,
      role_mode text not null default 'pick_pack',
      picker_user_id bigint references users(id) on delete set null,
      packer_user_id bigint not null references users(id) on delete cascade,
      table_no integer,
      handoff_status text not null default 'not_required',
      picker_started_at timestamptz,
      picker_finished_at timestamptz,
      placed_on_table_at timestamptz,
      transfer_started_at timestamptz,
      transferred_at timestamptz,
      created_by bigint references users(id) on delete set null,
      updated_by bigint references users(id) on delete set null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      unique (supply_id, part_no)
    )`,
    []
  );
  await dbQuery("alter table supply_assignment_parts add column if not exists role_mode text", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists picker_user_id bigint references users(id) on delete set null", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists packer_user_id bigint references users(id) on delete cascade", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists table_no integer", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists handoff_status text", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists picker_started_at timestamptz", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists picker_finished_at timestamptz", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists placed_on_table_at timestamptz", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists transfer_started_at timestamptz", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists transferred_at timestamptz", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists created_by bigint references users(id) on delete set null", []);
  await dbQuery("alter table supply_assignment_parts add column if not exists updated_by bigint references users(id) on delete set null", []);
  await dbQuery(
    `update supply_assignment_parts
     set role_mode = case
       when coalesce(picker_user_id, 0) > 0 and picker_user_id <> packer_user_id then 'split'
       else 'pick_pack'
     end
     where role_mode is null or role_mode not in ('pick_pack', 'split')`,
    []
  );
  await dbQuery(
    `update supply_assignment_parts
     set handoff_status = case
       when coalesce(picker_user_id, 0) > 0 and picker_user_id <> packer_user_id
         then case when transferred_at is not null then 'transferred' else 'waiting_pick' end
       else 'not_required'
     end
     where handoff_status is null or handoff_status not in ('not_required', 'waiting_pick', 'ready', 'transferred')`,
    []
  );
  await dbQuery("alter table supply_assignment_parts alter column role_mode set default 'pick_pack'", []);
  await dbQuery("alter table supply_assignment_parts alter column handoff_status set default 'not_required'", []);
  await dbQuery("alter table supply_assignment_parts alter column role_mode set not null", []);
  await dbQuery("alter table supply_assignment_parts alter column handoff_status set not null", []);
  await dbQuery("alter table supply_assignment_parts drop constraint if exists supply_assignment_parts_role_mode_check", []);
  await dbQuery(
    "alter table supply_assignment_parts add constraint supply_assignment_parts_role_mode_check check (role_mode in ('pick_pack', 'split'))",
    []
  );
  await dbQuery("alter table supply_assignment_parts drop constraint if exists supply_assignment_parts_handoff_status_check", []);
  await dbQuery(
    "alter table supply_assignment_parts add constraint supply_assignment_parts_handoff_status_check check (handoff_status in ('not_required', 'waiting_pick', 'ready', 'transferred'))",
    []
  );
  await dbQuery("alter table supply_assignment_parts drop constraint if exists supply_assignment_parts_table_no_check", []);
  await dbQuery(
    "alter table supply_assignment_parts add constraint supply_assignment_parts_table_no_check check (table_no is null or table_no in (1, 2, 3))",
    []
  );
  await dbQuery("create index if not exists idx_supply_assignment_parts_supply on supply_assignment_parts(supply_id, part_no)", []);
  await dbQuery("create index if not exists idx_supply_assignment_parts_packer on supply_assignment_parts(packer_user_id, created_at desc)", []);
  await dbQuery("create index if not exists idx_supply_assignment_parts_picker on supply_assignment_parts(picker_user_id, created_at desc)", []);
  await dbQuery("alter table supply_orders add column if not exists assignment_part_id bigint", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists assignment_part_id bigint", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists session_role text", []);
  await dbQuery("alter table employee_supply_sessions add column if not exists table_no integer", []);
  await dbQuery(
    `update employee_supply_sessions
     set session_role = 'pick_pack'
     where session_role is null or session_role not in ('pick', 'pack', 'pick_pack')`,
    []
  );
  await dbQuery("alter table employee_supply_sessions alter column session_role set default 'pick_pack'", []);
  await dbQuery("alter table employee_supply_sessions alter column session_role set not null", []);
  await dbQuery("alter table employee_supply_sessions drop constraint if exists employee_supply_sessions_session_role_check", []);
  await dbQuery(
    "alter table employee_supply_sessions add constraint employee_supply_sessions_session_role_check check (session_role in ('pick', 'pack', 'pick_pack'))",
    []
  );
  await dbQuery("alter table employee_supply_sessions drop constraint if exists employee_supply_sessions_table_no_check", []);
  await dbQuery(
    "alter table employee_supply_sessions add constraint employee_supply_sessions_table_no_check check (table_no is null or table_no in (1, 2, 3))",
    []
  );
  await dbQuery(
    `do $$
     begin
       if not exists (
         select 1 from pg_constraint where conname = 'fk_supply_orders_assignment_part'
       ) then
         alter table supply_orders
         add constraint fk_supply_orders_assignment_part
         foreign key (assignment_part_id) references supply_assignment_parts(id) on delete set null;
       end if;
     end $$`,
    []
  );
  await dbQuery(
    `do $$
     begin
       if not exists (
         select 1 from pg_constraint where conname = 'fk_employee_supply_sessions_assignment_part'
       ) then
         alter table employee_supply_sessions
         add constraint fk_employee_supply_sessions_assignment_part
         foreign key (assignment_part_id) references supply_assignment_parts(id) on delete set null;
       end if;
     end $$`,
    []
  );
  await dbQuery("create index if not exists idx_supply_orders_assignment_part on supply_orders(assignment_part_id)", []);
  await dbQuery("create index if not exists idx_employee_supply_sessions_assignment_part on employee_supply_sessions(assignment_part_id)", []);
  await dbQuery(
    `create table if not exists employee_scan_issues (
      id bigserial primary key,
      supply_id text not null references supply_settings(supply_id) on delete cascade,
      wb_order_id bigint,
      employee_id bigint not null references users(id) on delete cascade,
      issue_stage text not null default 'order_scan' check (issue_stage in ('order_scan', 'pick_scan')),
      pick_item_id bigint,
      pick_item_key text,
      error_message text,
      context_note text,
      order_barcode text,
      scanned_barcode text,
      article text,
      product_name text,
      nm_id bigint,
      status text not null default 'open' check (status in ('open', 'resolved', 'fixed')),
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      resolved_at timestamptz,
      resolved_by bigint references users(id) on delete set null,
      fixed_at timestamptz,
      fixed_by bigint references users(id) on delete set null
    )`,
    []
  );
  await dbQuery("alter table employee_scan_issues alter column wb_order_id drop not null", []);
  await dbQuery("alter table employee_scan_issues add column if not exists issue_stage text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists pick_item_id bigint", []);
  await dbQuery("alter table employee_scan_issues add column if not exists pick_item_key text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists error_message text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists context_note text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists original_scanned_barcode text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists confirmation_scanned_barcode text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists marketplace_article text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists warehouse_product_id integer", []);
  await dbQuery("alter table employee_scan_issues add column if not exists warehouse_article text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists warehouse_product_name text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists mapping_barcode text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists image_url text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists remaining_quantity integer", []);
  await dbQuery("alter table employee_scan_issues add column if not exists resolution_type text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists resolution_note text", []);
  await dbQuery("alter table employee_scan_issues add column if not exists resolved_quantity integer", []);
  await dbQuery(
    "update employee_scan_issues set issue_stage = $1 where issue_stage is null or issue_stage not in ('order_scan', 'pick_scan')",
    [SCAN_ISSUE_STAGE_ORDER]
  );
  await dbQuery(
    `alter table employee_scan_issues
     alter column issue_stage set default 'order_scan'`,
    []
  );
  await dbQuery(
    `alter table employee_scan_issues
     alter column issue_stage set not null`,
    []
  );
  await dbQuery("alter table employee_scan_issues drop constraint if exists employee_scan_issues_issue_stage_check", []);
  await dbQuery(
    "alter table employee_scan_issues add constraint employee_scan_issues_issue_stage_check check (issue_stage in ('order_scan', 'pick_scan'))",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_scan_issues_open on employee_scan_issues(status, created_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_scan_issues_supply_order on employee_scan_issues(supply_id, wb_order_id, status)",
    []
  );
  await dbQuery(
    "create index if not exists idx_employee_scan_issues_employee on employee_scan_issues(employee_id, created_at desc)",
    []
  );
  await dbQuery(
    `create table if not exists collector_supply_sessions (
      id bigserial primary key,
      supply_id text not null references supply_settings(supply_id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      status text not null default 'started' check (status in ('started', 'finished', 'cancelled')),
      started_at timestamptz not null default now(),
      finished_at timestamptz,
      total_seconds integer not null default 0,
      order_count integer not null default 0,
      item_count integer not null default 0,
      supply_name text,
      store_id text,
      store_name text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )`,
    []
  );
  await dbQuery(
    "create index if not exists idx_collector_supply_sessions_user_started on collector_supply_sessions(user_id, started_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_collector_supply_sessions_supply_user on collector_supply_sessions(supply_id, user_id, started_at desc)",
    []
  );
  await dbQuery(
    "update supply_settings set access_mode = 'hidden' where access_mode not in ('hidden', 'selected_split')",
    []
  );
  await dbQuery("alter table supply_settings drop constraint if exists supply_settings_access_mode_check", []);
  await dbQuery(
    "alter table supply_settings add constraint supply_settings_access_mode_check check (access_mode in ('hidden', 'selected_split'))",
    []
  );
  await dbQuery(
    "create index if not exists idx_supply_orders_collected_by_at on supply_orders(collected_by, collected_at desc)",
    []
  );
}

async function ensureAnalyticsSchema() {
  await dbQuery(
    `create or replace view analytics_employee_session_fact as
     with session_base as (
       select s.*,
              case
                when s.status = 'started' and s.finished_at is null and s.started_at is not null
                  then extract(epoch from (now() - s.started_at))::bigint
                else coalesce(s.total_seconds, 0)::bigint
              end as live_total_seconds
       from employee_supply_sessions s
     )
     select s.id as session_id,
            s.supply_id,
            ss.supply_name,
            ss.store_id,
            ss.store_name,
            s.user_id as employee_id,
            u.surname as employee_surname,
            u.name as employee_name,
            trim(concat_ws(' ', u.surname, u.name)) as employee_full_name,
            s.assigned_by as assigned_by_id,
            a.surname as assigned_by_surname,
            a.name as assigned_by_name,
            trim(concat_ws(' ', a.surname, a.name)) as assigned_by_full_name,
            s.assignment_mode,
            case when s.assignment_mode = 'redistribute' then 'Перераздача' else 'Разделение' end as assignment_mode_label,
            s.status,
            case
              when s.status = 'assigned' then 'Выдано'
              when s.status = 'started' then 'В работе'
              when s.status = 'finished' then 'Завершено'
              when s.status = 'reassigned' then 'Перераспределено'
              else s.status
            end as status_label,
            s.assigned_at,
            timezone('${MOSCOW_TZ}', s.assigned_at) as assigned_at_msk,
            timezone('${MOSCOW_TZ}', s.assigned_at)::date as assigned_date_msk,
            s.started_at,
            timezone('${MOSCOW_TZ}', s.started_at) as started_at_msk,
            timezone('${MOSCOW_TZ}', s.started_at)::date as started_date_msk,
            s.finished_at,
            timezone('${MOSCOW_TZ}', s.finished_at) as finished_at_msk,
            timezone('${MOSCOW_TZ}', s.finished_at)::date as finished_date_msk,
            s.assigned_order_count,
            s.collected_order_count,
            greatest(s.assigned_order_count - s.collected_order_count, 0) as remaining_order_count,
            s.live_total_seconds as total_seconds,
            round(s.live_total_seconds::numeric / 60.0, 2) as total_minutes,
            case
              when s.collected_order_count > 0
                then round(s.live_total_seconds::numeric / s.collected_order_count, 2)
              else null
            end as avg_seconds_per_order,
            case
              when s.live_total_seconds > 0
                then round((s.collected_order_count::numeric * 3600.0) / s.live_total_seconds, 2)
              else null
            end as orders_per_hour,
            case
              when s.assigned_order_count > 0
                then round((s.collected_order_count::numeric * 100.0) / s.assigned_order_count, 2)
              else null
            end as completion_rate_pct,
            case
              when s.started_at is not null
                then extract(epoch from (s.started_at - s.assigned_at))::bigint
              else null
            end as start_delay_seconds,
            case
              when s.first_collected_at is not null and s.started_at is not null
                then extract(epoch from (s.first_collected_at - s.started_at))::bigint
              else null
            end as first_collection_delay_seconds,
            case
              when s.last_collected_at is not null and s.first_collected_at is not null
                then extract(epoch from (s.last_collected_at - s.first_collected_at))::bigint
              else null
            end as collection_span_seconds,
            s.first_collected_at,
            timezone('${MOSCOW_TZ}', s.first_collected_at) as first_collected_at_msk,
            timezone('${MOSCOW_TZ}', s.first_collected_at)::date as first_collected_date_msk,
            s.last_collected_at,
            timezone('${MOSCOW_TZ}', s.last_collected_at) as last_collected_at_msk,
            timezone('${MOSCOW_TZ}', s.last_collected_at)::date as last_collected_date_msk,
            s.created_at,
            s.updated_at
     from session_base s
     left join supply_settings ss on ss.supply_id = s.supply_id
     left join users u on u.id = s.user_id
     left join users a on a.id = s.assigned_by`,
    []
  );

  await dbQuery(
    `create or replace view analytics_employee_order_fact as
     with session_base as (
       select s.*,
              case
                when s.status = 'started' and s.finished_at is null and s.started_at is not null
                  then extract(epoch from (now() - s.started_at))::bigint
                else coalesce(s.total_seconds, 0)::bigint
              end as live_total_seconds
       from employee_supply_sessions s
     )
     select o.supply_id,
            ss.supply_name,
            ss.store_id,
            ss.store_name,
            o.wb_order_id,
            o.order_created_at,
            timezone('${MOSCOW_TZ}', o.order_created_at) as order_created_at_msk,
            timezone('${MOSCOW_TZ}', o.order_created_at)::date as order_created_date_msk,
            date_trunc('month', timezone('${MOSCOW_TZ}', o.order_created_at))::date as order_created_month_msk,
            o.article,
            o.barcode as wb_barcode,
            o.product_name,
            o.nm_id,
            o.quantity,
            o.warehouse_id,
            o.cargo_type,
            o.synced_at,
            o.assigned_user_id as assigned_employee_id,
            au.surname as assigned_employee_surname,
            au.name as assigned_employee_name,
            trim(concat_ws(' ', au.surname, au.name)) as assigned_employee_full_name,
            o.assigned_at,
            timezone('${MOSCOW_TZ}', o.assigned_at) as assigned_at_msk,
            timezone('${MOSCOW_TZ}', o.assigned_at)::date as assigned_date_msk,
            o.scan_passed_at,
            timezone('${MOSCOW_TZ}', o.scan_passed_at) as scan_passed_at_msk,
            o.scan_passed_by,
            su.surname as scan_passed_by_surname,
            su.name as scan_passed_by_name,
            trim(concat_ws(' ', su.surname, su.name)) as scan_passed_by_full_name,
            o.scan_barcode,
            o.scan_error,
            o.label_scan_passed_at,
            timezone('${MOSCOW_TZ}', o.label_scan_passed_at) as label_scan_passed_at_msk,
            o.label_scan_passed_by,
            lu.surname as label_scan_passed_by_surname,
            lu.name as label_scan_passed_by_name,
            trim(concat_ws(' ', lu.surname, lu.name)) as label_scan_passed_by_full_name,
            o.label_scan_barcode,
            o.label_scan_error,
            o.sticker_barcode,
            o.sticker_url,
            o.sticker_format,
            o.sticker_loaded_at,
            o.sticker_error,
            o.collected_at,
            timezone('${MOSCOW_TZ}', o.collected_at) as collected_at_msk,
            timezone('${MOSCOW_TZ}', o.collected_at)::date as collected_date_msk,
            date_trunc('week', timezone('${MOSCOW_TZ}', o.collected_at))::date as collected_week_msk,
            date_trunc('month', timezone('${MOSCOW_TZ}', o.collected_at))::date as collected_month_msk,
            o.collected_by as collected_employee_id,
            cu.surname as collected_employee_surname,
            cu.name as collected_employee_name,
            trim(concat_ws(' ', cu.surname, cu.name)) as collected_employee_full_name,
            o.collected_via,
            o.collected_session_id as session_id,
            sb.assignment_mode as session_assignment_mode,
            case when sb.assignment_mode = 'redistribute' then 'Перераздача' else 'Разделение' end as session_assignment_mode_label,
            sb.status as session_status,
            case
              when sb.status = 'assigned' then 'Выдано'
              when sb.status = 'started' then 'В работе'
              when sb.status = 'finished' then 'Завершено'
              when sb.status = 'reassigned' then 'Перераспределено'
              else sb.status
            end as session_status_label,
            sb.assigned_at as session_assigned_at,
            timezone('${MOSCOW_TZ}', sb.assigned_at) as session_assigned_at_msk,
            sb.started_at as session_started_at,
            timezone('${MOSCOW_TZ}', sb.started_at) as session_started_at_msk,
            sb.finished_at as session_finished_at,
            timezone('${MOSCOW_TZ}', sb.finished_at) as session_finished_at_msk,
            sb.assigned_by as session_assigned_by_id,
            ab.surname as session_assigned_by_surname,
            ab.name as session_assigned_by_name,
            trim(concat_ws(' ', ab.surname, ab.name)) as session_assigned_by_full_name,
            sb.assigned_order_count as session_assigned_order_count,
            sb.collected_order_count as session_collected_order_count,
            sb.live_total_seconds as session_total_seconds,
            round(sb.live_total_seconds::numeric / 60.0, 2) as session_total_minutes,
            case
              when sb.collected_order_count > 0
                then round(sb.live_total_seconds::numeric / sb.collected_order_count, 2)
              else null
            end as session_avg_seconds_per_order,
            case
              when o.assigned_at is not null and o.collected_at is not null
                then extract(epoch from (o.collected_at - o.assigned_at))::bigint
              else null
            end as assignment_to_collection_seconds,
            case
              when sb.started_at is not null and o.collected_at is not null
                then extract(epoch from (o.collected_at - sb.started_at))::bigint
              else null
            end as session_start_to_collection_seconds,
            case
              when o.order_created_at is not null and o.collected_at is not null
                then extract(epoch from (o.collected_at - o.order_created_at))::bigint
              else null
            end as order_created_to_collection_seconds,
            case
              when o.scan_passed_at is not null and o.label_scan_passed_at is not null
                then extract(epoch from (o.label_scan_passed_at - o.scan_passed_at))::bigint
              else null
            end as item_scan_to_label_scan_seconds,
            case
              when o.label_scan_passed_at is not null and o.collected_at is not null
                then extract(epoch from (o.collected_at - o.label_scan_passed_at))::bigint
              else null
            end as label_scan_to_collection_seconds,
            case
              when o.collected_at is not null then 'collected'
              when o.label_scan_passed_at is not null then 'label_scanned'
              when o.scan_passed_at is not null then 'item_scanned'
              when o.assigned_user_id is not null then 'assigned'
              else 'unassigned'
            end as order_status,
            case
              when o.collected_at is not null then 'Собран'
              when o.label_scan_passed_at is not null then 'Этикетка проверена'
              when o.scan_passed_at is not null then 'Товар проверен'
              when o.assigned_user_id is not null then 'Назначен'
              else 'Без назначения'
            end as order_status_label,
            o.updated_at
     from supply_orders o
     left join supply_settings ss on ss.supply_id = o.supply_id
     left join users au on au.id = o.assigned_user_id
     left join users cu on cu.id = o.collected_by
     left join users su on su.id = o.scan_passed_by
     left join users lu on lu.id = o.label_scan_passed_by
     left join session_base sb on sb.id = o.collected_session_id
     left join users ab on ab.id = sb.assigned_by
     where o.assigned_user_id is not null
        or o.scan_passed_at is not null
        or o.label_scan_passed_at is not null
        or o.collected_at is not null`,
    []
  );

  await dbQuery(
    `create or replace view analytics_employee_daily_fact as
     with session_base as (
       select s.*,
              case
                when s.status = 'started' and s.finished_at is null and s.started_at is not null
                  then extract(epoch from (now() - s.started_at))::bigint
                else coalesce(s.total_seconds, 0)::bigint
              end as live_total_seconds
       from employee_supply_sessions s
     ),
     session_daily as (
       select s.user_id as employee_id,
              timezone('${MOSCOW_TZ}', s.started_at)::date as report_date_msk,
              count(*)::int as sessions_started,
              count(*) filter (where s.finished_at is not null)::int as sessions_finished,
              count(distinct s.supply_id) filter (where s.collected_order_count > 0)::int as supplies_worked,
              coalesce(sum(s.assigned_order_count), 0)::bigint as assigned_orders,
              coalesce(sum(s.collected_order_count), 0)::bigint as collected_orders_from_sessions,
              coalesce(sum(s.live_total_seconds), 0)::bigint as total_work_seconds,
              min(timezone('${MOSCOW_TZ}', s.started_at)) as first_started_at_msk,
              max(timezone('${MOSCOW_TZ}', s.finished_at)) as last_finished_at_msk
       from session_base s
       where s.started_at is not null
       group by s.user_id, timezone('${MOSCOW_TZ}', s.started_at)::date
     ),
     order_daily as (
       select o.collected_by as employee_id,
              timezone('${MOSCOW_TZ}', o.collected_at)::date as report_date_msk,
              count(*)::int as orders_collected,
              count(distinct o.supply_id)::int as supplies_collected,
              min(timezone('${MOSCOW_TZ}', o.collected_at)) as first_collected_at_msk,
              max(timezone('${MOSCOW_TZ}', o.collected_at)) as last_collected_at_msk
       from supply_orders o
       where o.collected_by is not null and o.collected_at is not null
       group by o.collected_by, timezone('${MOSCOW_TZ}', o.collected_at)::date
     ),
     days as (
       select employee_id, report_date_msk from session_daily
       union
       select employee_id, report_date_msk from order_daily
     )
     select d.report_date_msk,
            u.id as employee_id,
            u.surname as employee_surname,
            u.name as employee_name,
            trim(concat_ws(' ', u.surname, u.name)) as employee_full_name,
            coalesce(sd.sessions_started, 0) as sessions_started,
            coalesce(sd.sessions_finished, 0) as sessions_finished,
            coalesce(sd.supplies_worked, 0) as supplies_worked,
            coalesce(od.supplies_collected, 0) as supplies_collected,
            coalesce(sd.assigned_orders, 0) as assigned_orders,
            coalesce(od.orders_collected, 0) as orders_collected,
            coalesce(sd.collected_orders_from_sessions, 0) as collected_orders_from_sessions,
            coalesce(sd.total_work_seconds, 0) as total_work_seconds,
            round(coalesce(sd.total_work_seconds, 0)::numeric / 60.0, 2) as total_work_minutes,
            case
              when coalesce(od.orders_collected, 0) > 0
                then round(coalesce(sd.total_work_seconds, 0)::numeric / od.orders_collected, 2)
              else null
            end as avg_seconds_per_order,
            case
              when coalesce(sd.total_work_seconds, 0) > 0
                then round((coalesce(od.orders_collected, 0)::numeric * 3600.0) / sd.total_work_seconds, 2)
              else null
            end as orders_per_hour,
            sd.first_started_at_msk,
            sd.last_finished_at_msk,
            od.first_collected_at_msk,
            od.last_collected_at_msk
     from days d
     join users u on u.id = d.employee_id
     left join session_daily sd on sd.employee_id = d.employee_id and sd.report_date_msk = d.report_date_msk
     left join order_daily od on od.employee_id = d.employee_id and od.report_date_msk = d.report_date_msk`,
    []
  );

  await dbQuery(
    `create or replace view analytics_employee_supply_fact as
     with session_base as (
       select s.*,
              case
                when s.status = 'started' and s.finished_at is null and s.started_at is not null
                  then extract(epoch from (now() - s.started_at))::bigint
                else coalesce(s.total_seconds, 0)::bigint
              end as live_total_seconds
       from employee_supply_sessions s
     )
     select s.supply_id,
            ss.supply_name,
            ss.store_id,
            ss.store_name,
            s.user_id as employee_id,
            u.surname as employee_surname,
            u.name as employee_name,
            trim(concat_ws(' ', u.surname, u.name)) as employee_full_name,
            count(*)::int as sessions_count,
            count(*) filter (where s.started_at is not null)::int as sessions_started,
            count(*) filter (where s.finished_at is not null)::int as sessions_finished,
            min(s.assigned_at) as first_assigned_at,
            timezone('${MOSCOW_TZ}', min(s.assigned_at)) as first_assigned_at_msk,
            min(s.started_at) as first_started_at,
            timezone('${MOSCOW_TZ}', min(s.started_at)) as first_started_at_msk,
            max(s.finished_at) as last_finished_at,
            timezone('${MOSCOW_TZ}', max(s.finished_at)) as last_finished_at_msk,
            min(s.first_collected_at) as first_collected_at,
            timezone('${MOSCOW_TZ}', min(s.first_collected_at)) as first_collected_at_msk,
            max(s.last_collected_at) as last_collected_at,
            timezone('${MOSCOW_TZ}', max(s.last_collected_at)) as last_collected_at_msk,
            coalesce(sum(s.assigned_order_count), 0)::bigint as assigned_orders,
            coalesce(sum(s.collected_order_count), 0)::bigint as collected_orders,
            coalesce(sum(s.live_total_seconds), 0)::bigint as total_seconds,
            round(coalesce(sum(s.live_total_seconds), 0)::numeric / 60.0, 2) as total_minutes,
            case
              when coalesce(sum(s.collected_order_count), 0) > 0
                then round(coalesce(sum(s.live_total_seconds), 0)::numeric / sum(s.collected_order_count), 2)
              else null
            end as avg_seconds_per_order,
            case
              when coalesce(sum(s.live_total_seconds), 0) > 0
                then round((coalesce(sum(s.collected_order_count), 0)::numeric * 3600.0) / sum(s.live_total_seconds), 2)
              else null
            end as orders_per_hour,
            case
              when coalesce(sum(s.assigned_order_count), 0) > 0
                then round((coalesce(sum(s.collected_order_count), 0)::numeric * 100.0) / sum(s.assigned_order_count), 2)
              else null
            end as completion_rate_pct,
            string_agg(distinct case when s.assignment_mode = 'redistribute' then 'Перераздача' else 'Разделение' end, ', ') as assignment_mode_labels,
            string_agg(
              distinct case
                when s.status = 'assigned' then 'Выдано'
                when s.status = 'started' then 'В работе'
                when s.status = 'finished' then 'Завершено'
                when s.status = 'reassigned' then 'Перераспределено'
                else s.status
              end,
              ', '
            ) as session_status_labels,
            string_agg(distinct trim(concat_ws(' ', a.surname, a.name)), ', ') filter (where a.id is not null) as assigned_by_people
     from session_base s
     left join supply_settings ss on ss.supply_id = s.supply_id
     left join users u on u.id = s.user_id
     left join users a on a.id = s.assigned_by
     group by s.supply_id, ss.supply_name, ss.store_id, ss.store_name, s.user_id, u.surname, u.name`,
    []
  );
}

async function ensureAppSchema() {
  await ensureUsersSchema();
  await ensureLocationSchema();
  await ensureStoreSchema();
  await ensureSupplyTrbxSchema();
  await ensureSupplyManualSyncSchema();
  await ensureWorkTrackingSchema();
  await ensureAnalyticsSchema();
}

ensureAppSchema().catch((err) => {
  console.error("Schema init failed", err);
});

function hashPassword(password, salt) {
  return crypto.scryptSync(password, salt, 64).toString("hex");
}

function createPasswordHash(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = hashPassword(password, salt);
  return { salt, hash };
}

function verifyPassword(password, salt, hash) {
  const derived = crypto.scryptSync(password, salt, 64);
  const stored = Buffer.from(hash, "hex");
  if (stored.length !== derived.length) return false;
  return crypto.timingSafeEqual(derived, stored);
}

function toPublicUser(row) {
  const profile = getUserDisplayProfile(row);
  return {
    id: Number(row.id),
    surname: profile.surname,
    name: profile.name,
    fullName: profile.fullName,
    login: profile.login,
    role: row.role,
    position: getEmployeePositionForUser(row),
    locationCode: normalizeLocationCode(row.location_code),
    externalEmployeeId: row.external_employee_id ? Number(row.external_employee_id) : null,
    createdAt: row.created_at,
  };
}

function toEmployeeUser(row) {
  const profile = getUserDisplayProfile(row);
  return {
    id: Number(row.id),
    surname: profile.surname,
    name: profile.name,
    fullName: profile.fullName,
    login: profile.login,
    locationCode: normalizeLocationCode(row.location_code),
  };
}

function createPlaceholderPasswordHash() {
  const seed = crypto.randomBytes(24).toString("hex");
  return createPasswordHash(seed);
}

function normalizeLocationCode(value) {
  const normalized = normalizeText(value).toLowerCase();
  return LOCATION_CODES.has(normalized) ? normalized : null;
}

function getSkladWarehouseOptionsFromSettings(row = {}) {
  const warehouseId = Number(row?.sklad_warehouse_id || 0) || SKLAD_FBS_WAREHOUSE_ID;
  const warehouseName = normalizeText(row?.sklad_warehouse_name) || FBS_STOCK_WAREHOUSE_NAME;
  return { warehouseId, storeName: warehouseName, warehouseName };
}

async function getConfiguredEmployeeLocation(userId) {
  const result = await dbQuery(
    `select id, role, location_code
     from users
     where id = $1
     limit 1`,
    [Number(userId)]
  );
  const row = result.rows?.[0] || null;
  if (!row || row.role !== ROLE_EMPLOYEE) {
    return { errorStatus: 403, error: "FORBIDDEN" };
  }
  const locationCode = normalizeLocationCode(row.location_code);
  if (!locationCode) {
    return { errorStatus: 409, error: "Сотрудник не настроен. Обратитесь к Superadmin." };
  }
  return { locationCode };
}

async function resolveSkladWarehouseForLocation({
  locationCode,
  marketplace = "wb",
  storeId = null,
  marketplaceWarehouseId = null,
} = {}) {
  const normalizedLocation = normalizeLocationCode(locationCode);
  if (!normalizedLocation) {
    const error = new Error("Выберите город");
    error.statusCode = 400;
    throw error;
  }
  const normalizedMarketplace = marketplace === "oz" ? "oz" : "wb";
  const normalizedStoreId = normalizeStoreId(storeId) || "";
  const normalizedMarketplaceWarehouseId = normalizeText(marketplaceWarehouseId) || "";
  const result = await dbQuery(
    `select id, sklad_warehouse_id, sklad_warehouse_name
     from site_location_warehouse_settings
     where active = true
       and location_code = $1
       and marketplace in ('all', $2)
       and (coalesce(store_id, '') = '' or store_id = $3)
       and (coalesce(marketplace_warehouse_id, '') = '' or marketplace_warehouse_id = $4)
     order by
       case when marketplace = $2 then 8 else 0 end +
       case when coalesce(store_id, '') = $3 and $3 <> '' then 4 else 0 end +
       case when coalesce(marketplace_warehouse_id, '') = $4 and $4 <> '' then 2 else 0 end desc,
       id desc
     limit 1`,
    [normalizedLocation, normalizedMarketplace, normalizedStoreId, normalizedMarketplaceWarehouseId]
  );
  const row = result.rows?.[0] || null;
  const warehouseId = Number(row?.sklad_warehouse_id || 0);
  if (!Number.isInteger(warehouseId) || warehouseId < 1) {
    const error = new Error("Для выбранного города не настроен склад в глобальных настройках");
    error.statusCode = 409;
    throw error;
  }
  return {
    settingId: Number(row.id),
    skladWarehouseId: warehouseId,
    skladWarehouseName: normalizeText(row.sklad_warehouse_name) || FBS_STOCK_WAREHOUSE_NAME,
  };
}

// --- Названия товаров из складской БД (products_s) при сборке товаров/заказов ---
// Матч ПО БАРКОДУ (row.barcode), фолбэк по артикулу. Кэш по токену с TTL.
const WAREHOUSE_NAME_TTL_MS = 5 * 60 * 1000;
const warehouseNameCache = new Map(); // UPPER(token) -> { name, at }

function normWhKey(v) {
  return String(v == null ? "" : v).replace(/\s+/g, "").trim().toUpperCase();
}
function warehouseTokensForRow(row) {
  const tokens = [];
  if (row.barcode) tokens.push(normWhKey(row.barcode)); // приоритет — штрихкод
  if (row.article) tokens.push(normWhKey(row.article));
  return tokens.filter(Boolean);
}
function warehouseNameForRow(row) {
  const meta = warehouseProductMetaForRow(row);
  if (meta?.name) return meta.name;
  return null;
}
function warehouseProductMetaForRow(row) {
  for (const token of warehouseTokensForRow(row)) {
    const hit = warehouseNameCache.get(token);
    if (hit) return hit;
  }
  return null;
}
function warehouseEntityTypeForRow(row) {
  return warehouseProductMetaForRow(row)?.entityType || null;
}
function warehouseIsBundleForRow(row) {
  return warehouseProductMetaForRow(row)?.isBundle === true;
}
function warehouseBundleComponentsForRow(row) {
  const list = warehouseProductMetaForRow(row)?.components;
  return Array.isArray(list) && list.length ? list : null;
}
function warehouseBundleJarCountForRow(row) {
  const n = warehouseProductMetaForRow(row)?.jarCount;
  return n != null && Number.isFinite(Number(n)) ? Number(n) : null;
}
async function warmWarehouseNames(rows) {
  const now = Date.now();
  const tokens = [];
  for (const row of Array.isArray(rows) ? rows : []) tokens.push(...warehouseTokensForRow(row));
  const missing = [...new Set(tokens)].filter((token) => {
    const hit = warehouseNameCache.get(token);
    return !hit || now - hit.at > WAREHOUSE_NAME_TTL_MS;
  });
  if (!missing.length) return;
  try {
    const map = await resolveWarehouseProductMeta(missing);
    for (const token of missing) {
      const meta = map.get(token) || {};
      warehouseNameCache.set(token, {
        name: meta?.name || null,
        entityType: meta?.entityType || null,
        isBundle: meta?.isBundle === true,
        components: Array.isArray(meta?.components) ? meta.components : null,
        jarCount: meta?.jarCount != null ? Number(meta.jarCount) : null,
        at: now,
      });
    }
  } catch (error) {
    console.warn("warmWarehouseNames failed", error?.message || error);
  }
}

function toEmployeeOrderPayload(row) {
  return {
    id: Number(row.wb_order_id),
    createdAt: row.order_created_at,
    article: row.article ?? null,
    barcode: row.barcode ?? null,
    productName: warehouseNameForRow(row) || row.product_name || null,
    entityType: warehouseEntityTypeForRow(row),
    isBundle: warehouseIsBundleForRow(row),
    bundleComponents: warehouseBundleComponentsForRow(row),
    bundleJarCount: warehouseBundleJarCountForRow(row),
    nmId: row.nm_id ?? null,
    imageUrl: proxifyWbCardImage(getWbCachedImage(row.nm_id)),
    quantity: row.quantity ?? 1,
    stickerUrl: row.sticker_url ?? null,
    stickerBarcode: row.sticker_barcode ?? null,
    scanPassedAt: row.scan_passed_at ?? null,
    scanBarcode: row.scan_barcode ?? null,
    labelScanPassedAt: row.label_scan_passed_at ?? null,
    labelScanBarcode: row.label_scan_barcode ?? null,
  };
}

function toEmployeeSessionPayload(row) {
  if (!row) return null;
  const pickingStatus = normalizePickingStatus(row.picking_status);
  const pickingRequiredQty = Number(row.picking_required_qty || 0);
  const pickingCollectedQty = Number(row.picking_collected_qty || 0);
  const pickingRemainingQty = Math.max(0, pickingRequiredQty - pickingCollectedQty);
  const activePhase =
    pickingStatus === PICKING_STATUS_STARTED && !row.picking_finished_at
      ? "picking"
      : row.status === "started" && !row.finished_at
      ? "packing"
      : null;
  return {
    id: Number(row.id),
    supplyId: row.supply_id,
    userId: Number(row.user_id),
    assignmentPartId: row.assignment_part_id != null ? Number(row.assignment_part_id) : null,
    sessionRole: normalizeSessionRole(row.session_role),
    tableNo: normalizeAssignmentTableNo(row.table_no),
    assignedBy: row.assigned_by != null ? Number(row.assigned_by) : null,
    assignmentMode: row.assignment_mode,
    status: row.status,
    assignedAt: row.assigned_at ?? null,
    startedAt: row.started_at ?? null,
    finishedAt: row.finished_at ?? null,
    assignedOrderCount: Number(row.assigned_order_count || 0),
    collectedOrderCount: Number(row.collected_order_count || 0),
    totalSeconds: Number(row.total_seconds || 0),
    pickingStatus,
    pickingStartedAt: row.picking_started_at ?? null,
    pickingFinishedAt: row.picking_finished_at ?? null,
    pickingTotalSeconds: Number(row.picking_total_seconds || 0),
    pickingRequiredQty,
    pickingCollectedQty,
    pickingRemainingQty,
    activePhase,
    firstCollectedAt: row.first_collected_at ?? null,
    lastCollectedAt: row.last_collected_at ?? null,
  };
}

const wbCardImageCache = new Map(); // nmId -> { url, ts }
const WB_CARD_IMAGE_TTL_MS = 24 * 60 * 60 * 1000;
const WB_CARD_IMAGE_NEGATIVE_TTL_MS = 2 * 60 * 1000;
const wbImageResolveJobs = new Map();
const wbCardMetadataCache = new Map(); // nmId -> { title, ts }
const WB_CARD_METADATA_TTL_MS = 24 * 60 * 60 * 1000;
// Картинки WB CDN (basket-*.wbbasket.ru) не всегда доступны из webview/сети рабочего ПК —
// отдаём через свой origin (тот же хост/http). Путь /WB/files/_img маршрутизируется nginx сюда.
function proxifyWbCardImage(url) {
  const u = url == null ? "" : String(url).trim();
  if (!u) return null;
  if (!/^https?:\/\//i.test(u)) return u;
  return `/${ROOT_SITE}/files/_img?u=${encodeURIComponent(u)}`;
}
function wbBasketProbeOrder(vol) {
  let est;
  if (vol <= 143) est = 1; else if (vol <= 287) est = 2; else if (vol <= 431) est = 3; else if (vol <= 719) est = 4;
  else if (vol <= 1007) est = 5; else if (vol <= 1061) est = 6; else if (vol <= 1115) est = 7; else if (vol <= 1169) est = 8;
  else if (vol <= 1313) est = 9; else if (vol <= 1601) est = 10; else if (vol <= 1655) est = 11; else if (vol <= 1919) est = 12;
  else if (vol <= 2045) est = 13; else if (vol <= 2189) est = 14; else if (vol <= 2405) est = 15; else if (vol <= 2621) est = 16;
  else if (vol <= 2837) est = 17; else if (vol <= 3053) est = 18; else if (vol <= 3269) est = 19; else if (vol <= 3485) est = 20;
  else if (vol <= 3701) est = 21; else if (vol <= 3917) est = 22; else if (vol <= 4133) est = 23; else if (vol <= 4349) est = 24;
  else if (vol <= 4565) est = 25; else est = Math.min(50, 25 + Math.ceil((vol - 4565) / 400));
  const order = [est];
  for (let d = 1; d <= 50; d += 1) { if (est + d <= 50) order.push(est + d); if (est - d >= 1) order.push(est - d); }
  return [...new Set(order)];
}
async function resolveWbCardImage(nmId) {
  const nm = Number(nmId);
  if (!Number.isFinite(nm) || nm <= 0) return null;
  const cached = wbCardImageCache.get(nm);
  const cacheTtl = cached?.url ? WB_CARD_IMAGE_TTL_MS : WB_CARD_IMAGE_NEGATIVE_TTL_MS;
  if (cached && Date.now() - cached.ts < cacheTtl) return cached.url || null;
  const vol = Math.floor(nm / 100000), part = Math.floor(nm / 1000);
  const candidates = wbBasketProbeOrder(vol);
  const deadline = Date.now() + 8000;
  for (let offset = 0; offset < candidates.length && Date.now() < deadline; offset += 5) {
    const urls = candidates.slice(offset, offset + 5).map((basketNo) => {
      const bb = String(basketNo).padStart(2, "0");
      return `https://basket-${bb}.wbbasket.ru/vol${vol}/part${part}/${nm}/images/big/1.webp`;
    });
    const remainingMs = Math.max(500, Math.min(2500, deadline - Date.now()));
    const results = await Promise.all(
      urls.map(async (url) => {
        try {
          const response = await fetch(url, { method: "HEAD", signal: AbortSignal.timeout(remainingMs) });
          return response.status === 200 ? url : null;
        } catch {
          return null;
        }
      })
    );
    const resolvedUrl = results.find(Boolean);
    if (resolvedUrl) {
      wbCardImageCache.set(nm, { url: resolvedUrl, ts: Date.now() });
      return resolvedUrl;
    }
  }
  wbCardImageCache.set(nm, { url: null, ts: Date.now() });
  return null;
}
function getWbCachedImage(nmId) {
  const nm = Number(nmId);
  if (!Number.isFinite(nm) || nm <= 0) return null;
  const c = wbCardImageCache.get(nm);
  const cacheTtl = c?.url ? WB_CARD_IMAGE_TTL_MS : WB_CARD_IMAGE_NEGATIVE_TTL_MS;
  return c && Date.now() - c.ts < cacheTtl ? (c.url || null) : null;
}
async function resolveWbCardMetadata(nmId, resolvedImageUrl = null) {
  const nm = Number(nmId);
  if (!Number.isFinite(nm) || nm <= 0) return null;
  const cached = wbCardMetadataCache.get(nm);
  if (cached && Date.now() - cached.ts < WB_CARD_METADATA_TTL_MS) {
    return cached.title ? cached : null;
  }

  const imageUrl = resolvedImageUrl || getWbCachedImage(nm) || (await resolveWbCardImage(nm));
  if (!imageUrl) return null;
  const cardUrl = imageUrl.replace(/\/images\/big\/1\.webp(?:\?.*)?$/i, "/info/ru/card.json");
  if (cardUrl === imageUrl) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 4000);
  try {
    const response = await fetch(cardUrl, { signal: controller.signal });
    if (!response.ok) return null;
    const card = await response.json();
    const title = normalizeText(
      card?.imt_name || card?.imtName || card?.title || card?.object_name || card?.subj_name || card?.subjName || ""
    );
    if (!title) return null;
    const payload = { title, ts: Date.now() };
    wbCardMetadataCache.set(nm, payload);
    return payload;
  } finally {
    clearTimeout(timeout);
  }
}
function queueWbImageResolve(nmId) {
  const nm = Number(nmId);
  if (!Number.isFinite(nm) || nm <= 0) return Promise.resolve(null);
  const cached = wbCardImageCache.get(nm);
  const cacheTtl = cached?.url ? WB_CARD_IMAGE_TTL_MS : WB_CARD_IMAGE_NEGATIVE_TTL_MS;
  if (cached && Date.now() - cached.ts < cacheTtl) return Promise.resolve(cached.url || null);
  if (wbImageResolveJobs.has(nm)) return wbImageResolveJobs.get(nm);
  const job = resolveWbCardImage(nm)
    .catch(() => null)
    .finally(() => wbImageResolveJobs.delete(nm));
  wbImageResolveJobs.set(nm, job);
  return job;
}

function ensureWbImages(nmIds) {
  const now = Date.now();
  for (const raw of nmIds || []) {
    const nm = Number(raw);
    if (!Number.isFinite(nm) || nm <= 0) continue;
    const c = wbCardImageCache.get(nm);
    const cacheTtl = c?.url ? WB_CARD_IMAGE_TTL_MS : WB_CARD_IMAGE_NEGATIVE_TTL_MS;
    if (c && now - c.ts < cacheTtl) continue;
    void queueWbImageResolve(nm);
  }
}

async function warmWbImages(rows) {
  const nmIds = [...new Set((Array.isArray(rows) ? rows : []).map((row) => Number(row?.nm_id ?? row?.nmId)).filter((nm) => Number.isFinite(nm) && nm > 0))];
  await mapWithConcurrency(nmIds, 8, (nmId) => queueWbImageResolve(nmId));
}

function toEmployeePickItemPayload(row) {
  if (!row) return null;
  const requiredQty = Number(row.required_qty || 0);
  const pickedQty = Number(row.picked_qty || 0);
  const msCells = Array.isArray(row.ms_cells)
    ? row.ms_cells.map((entry) => ({
        id: entry?.id || null,
        name: entry?.name || null,
        stock: Number(entry?.stock || 0),
      }))
    : [];
  return {
    id: Number(row.id),
    sessionId: Number(row.session_id),
    supplyId: row.supply_id,
    userId: Number(row.user_id),
    itemKey: row.item_key,
    article: row.article || null,
    barcode: row.barcode || null,
    productName: warehouseNameForRow(row) || row.product_name || null,
    entityType: warehouseEntityTypeForRow(row),
    isBundle: warehouseIsBundleForRow(row),
    bundleComponents: warehouseBundleComponentsForRow(row),
    bundleJarCount: warehouseBundleJarCountForRow(row),
    nmId: row.nm_id != null ? Number(row.nm_id) : null,
    imageUrl: proxifyWbCardImage(getWbCachedImage(row.nm_id)),
    requiredQty,
    pickedQty,
    remainingQty: Math.max(0, requiredQty - pickedQty),
    done: pickedQty >= requiredQty && requiredQty > 0,
    bypassWithoutScanAllowed: row.bypass_without_scan_allowed === true,
    bypassAuthorizedAt: row.bypass_authorized_at ?? null,
    msCells,
    updatedAt: row.updated_at ?? null,
  };
}

function toScanIssuePayload(row) {
  if (!row) return null;
  const employeeName = `${row.employee_surname || ""} ${row.employee_name || ""}`.trim();
  const issueStage = normalizeScanIssueStage(row.issue_stage);
  return {
    id: Number(row.id),
    supplyId: row.supply_id,
    supplyName: row.supply_name || null,
    orderId: row.wb_order_id == null ? null : Number(row.wb_order_id),
    pickItemId: row.pick_item_id == null ? null : Number(row.pick_item_id),
    pickItemKey: row.pick_item_key || null,
    employeeId: Number(row.employee_id),
    employeeSurname: row.employee_surname || null,
    employeeName,
    issueStage,
    issueStageLabel: scanIssueStageLabel(issueStage),
    errorMessage: row.error_message || null,
    contextNote: row.context_note || null,
    article: row.article || null,
    productName: row.product_name || null,
    nmId: row.nm_id != null ? Number(row.nm_id) : null,
    orderBarcode: row.order_barcode || null,
    scannedBarcode: row.scanned_barcode || null,
    originalScannedBarcode: row.original_scanned_barcode || row.scanned_barcode || null,
    confirmationScannedBarcode: row.confirmation_scanned_barcode || null,
    marketplaceArticle: row.marketplace_article || row.article || null,
    warehouseProductId: row.warehouse_product_id == null ? null : Number(row.warehouse_product_id),
    warehouseArticle: row.warehouse_article || null,
    warehouseProductName: row.warehouse_product_name || null,
    mappingBarcode: row.mapping_barcode || null,
    imageUrl: row.image_url ? proxifyWbCardImage(row.image_url) : proxifyWbCardImage(getWbCachedImage(row.nm_id)),
    remainingQuantity: Math.max(0, Number(row.live_remaining_quantity ?? row.remaining_quantity ?? 0)),
    resolvedQuantity: Math.max(0, Number(row.resolved_quantity || 0)),
    resolutionType: row.resolution_type || null,
    resolutionNote: row.resolution_note || null,
    warehouseProductUrl: null,
    status: row.status,
    createdAt: row.created_at ?? null,
    updatedAt: row.updated_at ?? null,
  };
}

async function listEmployees() {
  const result = await dbQuery(
    `select id, surname, name, location_code
     from users
     where role = $1 and coalesce(position, $2) = $2
       and coalesce(auth_source, $3) = any($4::text[])
     order by surname, name, id`,
    [ROLE_EMPLOYEE, EMPLOYEE_POSITION_PACKER, USER_AUTH_SOURCE_EXTERNAL, [USER_AUTH_SOURCE_EXTERNAL, USER_AUTH_SOURCE_LOCAL]]
  );
  return result.rows || [];
}

async function getUserExternalEmployeeId(userId) {
  const normalizedUserId = Number(userId || 0);
  if (!Number.isInteger(normalizedUserId) || normalizedUserId <= 0) return null;
  const result = await dbQuery("select external_employee_id from users where id = $1 limit 1", [normalizedUserId]);
  const value = Number(result.rows?.[0]?.external_employee_id || 0);
  return Number.isInteger(value) && value > 0 ? value : null;
}

function toSupplyAssignmentPartPayload(row) {
  if (!row) return null;
  const roleMode = normalizeAssignmentPartMode(row.role_mode);
  const pickerUserId = Number(row.picker_user_id || 0) || null;
  const packerUserId = Number(row.packer_user_id || 0) || null;
  const isSplit = roleMode === ASSIGNMENT_PART_MODE_SPLIT && pickerUserId && packerUserId && pickerUserId !== packerUserId;
  return {
    id: Number(row.id),
    supplyId: row.supply_id,
    partNo: Number(row.part_no || 0),
    roleMode,
    pickerUserId: isSplit ? pickerUserId : null,
    packerUserId,
    tableNo: isSplit ? normalizeAssignmentTableNo(row.table_no) : null,
    handoffStatus: normalizeHandoffStatus(row.handoff_status),
    pickerStartedAt: row.picker_started_at || null,
    pickerFinishedAt: row.picker_finished_at || null,
    placedOnTableAt: row.placed_on_table_at || null,
    transferStartedAt: row.transfer_started_at || null,
    transferredAt: row.transferred_at || null,
    pickerName: isSplit ? `${row.picker_surname || ""} ${row.picker_name || ""}`.trim() || null : null,
    packerName: `${row.packer_surname || ""} ${row.packer_name || ""}`.trim() || null,
  };
}

async function listSupplyAssignmentParts(supplyId) {
  const result = await dbQuery(
    `select p.*,
            picker.surname as picker_surname,
            picker.name as picker_name,
            packer.surname as packer_surname,
            packer.name as packer_name
     from supply_assignment_parts p
     left join users picker on picker.id = p.picker_user_id
     left join users packer on packer.id = p.packer_user_id
     where p.supply_id = $1
     order by p.part_no asc, p.id asc`,
    [supplyId]
  );
  return (result.rows || []).map(toSupplyAssignmentPartPayload);
}

async function supplyHasAssignmentParts(supplyId) {
  const result = await dbQuery(
    "select 1 from supply_assignment_parts where supply_id = $1 limit 1",
    [supplyId]
  );
  return Boolean(result.rows?.[0]);
}

async function getSupplyAssignmentPartById(partId) {
  const normalizedPartId = Number(partId || 0);
  if (!Number.isInteger(normalizedPartId) || normalizedPartId <= 0) return null;
  const result = await dbQuery(
    `select p.*,
            picker.surname as picker_surname,
            picker.name as picker_name,
            packer.surname as packer_surname,
            packer.name as packer_name
     from supply_assignment_parts p
     left join users picker on picker.id = p.picker_user_id
     left join users packer on packer.id = p.packer_user_id
     where p.id = $1
     limit 1`,
    [normalizedPartId]
  );
  return toSupplyAssignmentPartPayload(result.rows?.[0] || null);
}

function normalizeSupplyAssignmentPartsInput(parts, selectedUserIds = []) {
  const rawParts = Array.isArray(parts) ? parts : [];
  const allowedUserIds = new Set(
    (Array.isArray(selectedUserIds) ? selectedUserIds : [])
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0)
  );
  const usedUsers = new Map();
  const normalizedParts = [];
  for (let index = 0; index < rawParts.length; index += 1) {
    const rawPart = rawParts[index] || {};
    const packerUserId = Number(rawPart.packerUserId || rawPart.packer_user_id || 0);
    const pickerCandidate = Number(rawPart.pickerUserId || rawPart.picker_user_id || 0);
    if (!Number.isInteger(packerUserId) || packerUserId <= 0) {
      throw new Error(`В части ${index + 1} не выбран упаковщик`);
    }
    if (allowedUserIds.size > 0 && !allowedUserIds.has(packerUserId)) {
      throw new Error(`Упаковщик в части ${index + 1} не входит в выбранных сотрудников`);
    }
    const isSplit = Number.isInteger(pickerCandidate) && pickerCandidate > 0 && pickerCandidate !== packerUserId;
    if (isSplit && allowedUserIds.size > 0 && !allowedUserIds.has(pickerCandidate)) {
      throw new Error(`Сборщик товара в части ${index + 1} не входит в выбранных сотрудников`);
    }
    const tableNo = isSplit ? normalizeAssignmentTableNo(rawPart.tableNo || rawPart.table_no) : null;
    if (isSplit && !tableNo) {
      throw new Error(`Для части ${index + 1} выберите стол 1, 2 или 3`);
    }
    const normalizedPart = {
      partNo: index + 1,
      roleMode: isSplit ? ASSIGNMENT_PART_MODE_SPLIT : ASSIGNMENT_PART_MODE_PICK_PACK,
      pickerUserId: isSplit ? pickerCandidate : null,
      packerUserId,
      tableNo,
    };
    normalizedParts.push(normalizedPart);
    const roleUserIds = [packerUserId, ...(isSplit ? [pickerCandidate] : [])];
    for (const userId of roleUserIds) {
      usedUsers.set(userId, (usedUsers.get(userId) || 0) + 1);
    }
  }
  if (allowedUserIds.size > 0) {
    const duplicates = Array.from(usedUsers.entries()).filter(([, count]) => count > 1);
    if (duplicates.length) {
      throw new Error("Каждый выбранный сотрудник должен использоваться в распределении только один раз");
    }
    const missing = Array.from(allowedUserIds).filter((userId) => !usedUsers.has(userId));
    if (missing.length) {
      throw new Error("Все выбранные сотрудники должны быть распределены по частям");
    }
  }
  return normalizedParts;
}

async function clearSupplyAssignmentParts(supplyId) {
  await dbQuery("update supply_orders set assignment_part_id = null where supply_id = $1", [supplyId]);
  await dbQuery("delete from supply_assignment_parts where supply_id = $1", [supplyId]);
}

async function createSupplyAssignmentParts(supplyId, parts, adminId = null) {
  const normalizedParts = Array.isArray(parts) ? parts : [];
  if (!normalizedParts.length) return [];
  const values = [];
  const placeholders = normalizedParts.map((part, index) => {
    const base = index * 9;
    values.push(
      supplyId,
      Number(part.partNo || index + 1),
      normalizeAssignmentPartMode(part.roleMode),
      part.pickerUserId ? Number(part.pickerUserId) : null,
      Number(part.packerUserId),
      part.tableNo ? Number(part.tableNo) : null,
      normalizeAssignmentPartMode(part.roleMode) === ASSIGNMENT_PART_MODE_SPLIT ? HANDOFF_STATUS_WAITING_PICK : HANDOFF_STATUS_NOT_REQUIRED,
      adminId || null,
      adminId || null
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, now(), now())`;
  });
  const result = await dbQuery(
    `insert into supply_assignment_parts
      (supply_id, part_no, role_mode, picker_user_id, packer_user_id, table_no, handoff_status, created_by, updated_by, created_at, updated_at)
     values ${placeholders.join(", ")}
     returning *`,
    values
  );
  return (result.rows || []).map((row) => ({
    id: Number(row.id),
    supplyId: row.supply_id,
    partNo: Number(row.part_no || 0),
    roleMode: normalizeAssignmentPartMode(row.role_mode),
    pickerUserId: row.picker_user_id != null ? Number(row.picker_user_id) : null,
    packerUserId: Number(row.packer_user_id),
    tableNo: normalizeAssignmentTableNo(row.table_no),
    handoffStatus: normalizeHandoffStatus(row.handoff_status),
  }));
}

async function assignSupplyOrdersToParts(supplyId, parts, mode = "split") {
  const normalizedParts = Array.isArray(parts) ? parts : [];
  if (!normalizedParts.length) {
    return { total: 0, assigned: 0, parts: [] };
  }
  if (mode === "redistribute") {
    await dbQuery(
      "update supply_orders set assigned_user_id = null, assignment_part_id = null, assigned_at = null where supply_id = $1 and collected_at is null",
      [supplyId]
    );
  }
  const result = await dbQuery(
    `select wb_order_id, order_created_at
     from supply_orders
     where supply_id = $1 and collected_at is null
     order by order_created_at asc nulls last, wb_order_id asc`,
    [supplyId]
  );
  const orders = result.rows || [];
  if (!orders.length) {
    return {
      total: 0,
      assigned: 0,
      parts: normalizedParts.map((part) => ({ ...part, count: 0 })),
    };
  }
  const total = orders.length;
  const base = Math.floor(total / normalizedParts.length);
  const extra = total % normalizedParts.length;
  let offset = 0;
  const summary = [];
  for (let index = 0; index < normalizedParts.length; index += 1) {
    const part = normalizedParts[index];
    const count = base + (index < extra ? 1 : 0);
    const slice = orders.slice(offset, offset + count).map((row) => Number(row.wb_order_id));
    offset += count;
    if (slice.length) {
      await dbQuery(
        `update supply_orders
         set assigned_user_id = $3,
             assignment_part_id = $4,
             assigned_at = now()
         where supply_id = $1 and wb_order_id = any($2)`,
        [supplyId, slice, getAssignmentPartEffectivePickerUserId(part), Number(part.id)]
      );
    }
    summary.push({
      ...part,
      count: slice.length,
    });
  }
  return {
    total,
    assigned: total,
    parts: summary,
  };
}

async function createSupplyAssignmentSessionsFromParts(supplyId, adminId, mode, parts, workflowStage) {
  const normalizedParts = Array.isArray(parts) ? parts : [];
  const values = [];
  const pickingRequired = isEmployeeProductPickingEnabled(workflowStage);
  const pushSession = (session) => {
    const base = values.length;
    values.push(
      supplyId,
      Number(session.userId),
      adminId || null,
      mode,
      Number(session.assignedOrderCount || 0),
      normalizePickingStatus(session.pickingStatus),
      Number(session.assignmentPartId || 0),
      normalizeSessionRole(session.sessionRole),
      session.tableNo ? Number(session.tableNo) : null
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, now(), $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, now(), now())`;
  };
  const placeholders = [];
  for (const part of normalizedParts) {
    const count = Number(part.count || 0);
    if (count < 1) continue;
    if (isSplitAssignmentPart(part)) {
      placeholders.push(
        pushSession({
          userId: part.pickerUserId,
          assignedOrderCount: count,
          pickingStatus: pickingRequired ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED,
          assignmentPartId: part.id,
          sessionRole: SESSION_ROLE_PICK,
          tableNo: part.tableNo,
        })
      );
      placeholders.push(
        pushSession({
          userId: part.packerUserId,
          assignedOrderCount: count,
          pickingStatus: PICKING_STATUS_NOT_REQUIRED,
          assignmentPartId: part.id,
          sessionRole: SESSION_ROLE_PACK,
          tableNo: part.tableNo,
        })
      );
      continue;
    }
    placeholders.push(
      pushSession({
        userId: part.packerUserId,
        assignedOrderCount: count,
        pickingStatus: pickingRequired ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED,
        assignmentPartId: part.id,
        sessionRole: SESSION_ROLE_PICK_PACK,
        tableNo: null,
      })
    );
  }
  if (!placeholders.length) return [];
  const result = await dbQuery(
    `insert into employee_supply_sessions
      (supply_id, user_id, assigned_by, assignment_mode, assigned_at, assigned_order_count, picking_status, assignment_part_id, session_role, table_no, created_at, updated_at)
     values ${placeholders.join(", ")}
     returning *`,
    values
  );
  return result.rows || [];
}


async function getEmployeeOpenSession(supplyId, userId) {
  const result = await dbQuery(
    `select *
     from employee_supply_sessions
     where supply_id = $1 and user_id = $2 and finished_at is null
     order by assigned_at desc, id desc
     limit 1`,
    [supplyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getSupplyAssignmentPartForSession(session) {
  const assignmentPartId = Number(session?.assignment_part_id || 0);
  if (!assignmentPartId) return null;
  return await getSupplyAssignmentPartById(assignmentPartId);
}

async function getEmployeeStartedSession(userId) {
  const result = await dbQuery(
    `select *
     from employee_supply_sessions
     where user_id = $1 and status = 'started' and finished_at is null
     order by started_at desc, id desc
     limit 1`,
    [userId]
  );
  return result.rows?.[0] || null;
}

async function getEmployeeActiveSession(userId) {
  const result = await dbQuery(
    `select *,
            case
              when picking_status = 'started' and picking_finished_at is null then 'picking'
              when status = 'started' and finished_at is null then 'packing'
              else null
            end as active_phase
     from employee_supply_sessions
     where user_id = $1
       and (
         (picking_status = 'started' and picking_finished_at is null)
         or (status = 'started' and finished_at is null)
       )
     order by greatest(
              extract(epoch from coalesce(started_at, to_timestamp(0))),
              extract(epoch from coalesce(picking_started_at, to_timestamp(0)))
            ) desc,
            id desc
     limit 1`,
    [userId]
  );
  return result.rows?.[0] || null;
}

async function getEmployeeStartedSessionForSupply(supplyId, userId) {
  const result = await dbQuery(
    `select *
     from employee_supply_sessions
     where supply_id = $1 and user_id = $2 and status = 'started' and finished_at is null
     order by started_at desc, id desc
     limit 1`,
    [supplyId, userId]
  );
  return result.rows?.[0] || null;
}

async function closeOpenEmployeeSessionsForSupply(supplyId, status = "reassigned") {
  await dbQuery(
    `update employee_supply_sessions
     set status = $2,
         finished_at = coalesce(finished_at, now()),
         total_seconds = case
           when started_at is not null then greatest(total_seconds, extract(epoch from (now() - started_at))::int)
           else total_seconds
         end,
         picking_status = case
           when picking_status = 'started' and picking_finished_at is null then 'cancelled'
           else picking_status
         end,
         picking_finished_at = case
           when picking_status = 'started' and picking_finished_at is null then now()
           else picking_finished_at
         end,
         picking_total_seconds = case
           when picking_started_at is not null and picking_finished_at is null
             then greatest(picking_total_seconds, extract(epoch from (now() - picking_started_at))::int)
           else picking_total_seconds
         end,
         updated_at = now()
     where supply_id = $1 and finished_at is null`,
    [supplyId, status]
  );
}

async function createEmployeeAssignmentSessions(supplyId, adminId, mode, perUser) {
  const rows = (Array.isArray(perUser) ? perUser : [])
    .map((entry) => ({
      userId: Number(entry?.userId),
      count: Number(entry?.count || 0),
    }))
    .filter((entry) => Number.isFinite(entry.userId) && entry.userId > 0 && entry.count > 0);
  if (!rows.length) return [];
  const values = [];
  const placeholders = rows.map((entry, index) => {
    const base = index * 5;
    values.push(supplyId, entry.userId, adminId || null, mode, entry.count);
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, now(), $${base + 5}, now(), now())`;
  });
  const result = await dbQuery(
    `insert into employee_supply_sessions
      (supply_id, user_id, assigned_by, assignment_mode, assigned_at, assigned_order_count, created_at, updated_at)
     values ${placeholders.join(", ")}
     returning *`,
    values
  );
  return result.rows || [];
}

async function ensureEmployeeAssignmentSession(supplyId, userId, assignedOrderCount, assignedBy = null, mode = "split") {
  const existing = await getEmployeeOpenSession(supplyId, userId);
  if (existing) return existing;
  if (!Number.isFinite(Number(assignedOrderCount)) || Number(assignedOrderCount) < 1) {
    return null;
  }
  const result = await dbQuery(
    `insert into employee_supply_sessions
      (supply_id, user_id, assigned_by, assignment_mode, assigned_at, assigned_order_count, created_at, updated_at)
     values ($1, $2, $3, $4, now(), $5, now(), now())
     returning *`,
    [supplyId, userId, assignedBy, mode, Number(assignedOrderCount)]
  );
  return result.rows?.[0] || null;
}

async function startEmployeeSupplySession(supplyId, userId) {
  const openSession = await getEmployeeOpenSession(supplyId, userId);
  if (!openSession) {
    return { session: null, error: "Поставка не назначена сотруднику" };
  }
  if (openSession.started_at) {
    return { session: openSession, error: null };
  }
  // Разрешаем сотруднику работать несколько назначенных поставок (можно переключаться между ними).
  const settings = await getSupplySettings(supplyId);
  const assignmentPart = await getSupplyAssignmentPartForSession(openSession);
  if (normalizeSessionRole(openSession.session_role) === SESSION_ROLE_PACK && assignmentPart) {
    const handoffResult = await performSupplyPartHandoffToPacker(supplyId, openSession, assignmentPart, settings);
    if (!handoffResult.ok) {
      return { session: null, error: handoffResult.error || "Не удалось передать товар упаковщику" };
    }
  }
  const result = await dbQuery(
    `update employee_supply_sessions
     set status = 'started', started_at = now(), updated_at = now()
     where id = $1
     returning *`,
    [openSession.id]
  );
  return { session: result.rows?.[0] || openSession, error: null };
}

async function recordCollectedOrderForSession(sessionId, collectedAt = new Date()) {
  await dbQuery(
    `update employee_supply_sessions
     set collected_order_count = collected_order_count + 1,
         first_collected_at = coalesce(first_collected_at, $2),
         last_collected_at = $2,
         updated_at = now()
     where id = $1`,
    [sessionId, collectedAt]
  );
}

async function finalizeEmployeeSupplySessionIfCompleted(supplyId, userId, assignmentPartId = null) {
  const session = await getEmployeeStartedSessionForSupply(supplyId, userId);
  if (!session) return null;
  const remainingResult = await dbQuery(
    `select count(*) as remaining
     from supply_orders
     where supply_id = $1
       and collected_at is null
       ${Number(assignmentPartId || session.assignment_part_id || 0) > 0 ? "and assignment_part_id = $2" : "and assigned_user_id = $2"}`,
    [supplyId, Number(assignmentPartId || session.assignment_part_id || 0) > 0 ? Number(assignmentPartId || session.assignment_part_id) : userId]
  );
  const remaining = Number(remainingResult.rows?.[0]?.remaining || 0);
  if (remaining > 0) return null;
  const result = await dbQuery(
    `update employee_supply_sessions
     set status = 'finished',
         finished_at = now(),
         total_seconds = greatest(total_seconds, extract(epoch from (now() - started_at))::int),
         updated_at = now()
     where id = $1
     returning *`,
    [session.id]
  );
  return result.rows?.[0] || session;
}

async function refreshEmployeeSessionOrderCounts(sessionId, supplyId, userId, assignmentPartId = null) {
  const counts = await getEmployeeSupplyCounts(supplyId, userId, assignmentPartId);
  const result = await dbQuery(
    `update employee_supply_sessions
     set assigned_order_count = $2,
         collected_order_count = $3,
         updated_at = now()
     where id = $1
     returning *`,
    [sessionId, counts.total, counts.collected]
  );
  return {
    session: result.rows?.[0] || null,
    counts,
  };
}

async function getEmployeeSupplyCounts(supplyId, userId, assignmentPartId = null) {
  const normalizedPartId = Number(assignmentPartId || 0);
  const result = await dbQuery(
    `select count(*) as total,
            count(*) filter (where collected_at is not null) as collected,
            count(*) filter (where collected_at is null) as remaining
     from supply_orders
     where supply_id = $1
       ${normalizedPartId > 0 ? "and assignment_part_id = $2" : "and assigned_user_id = $2"}`,
    normalizedPartId > 0 ? [supplyId, normalizedPartId] : [supplyId, userId]
  );
  return {
    total: Number(result.rows?.[0]?.total || 0),
    collected: Number(result.rows?.[0]?.collected || 0),
    remaining: Number(result.rows?.[0]?.remaining || 0),
  };
}

async function listEmployeePickItemsForSession(sessionId) {
  const result = await dbQuery(
    `select *
     from employee_supply_pick_items
     where session_id = $1
     order by (picked_qty >= required_qty and required_qty > 0)::int asc, required_qty desc nulls last, id asc`,
    [sessionId]
  );
  const rows = result.rows || [];
  // Прогреваем складские названия, чтобы сериализаторы показали название из products_s.
  await warmWarehouseNames(rows);
  await warmWbImages(rows);
  return rows;
}

function buildPickIssueContextNote(items) {
  const pending = (Array.isArray(items) ? items : []).filter(
    (item) => Number(item.picked_qty || 0) < Number(item.required_qty || 0)
  );
  if (!pending.length) return null;
  const names = pending
    .map((item) => item.article || item.product_name || item.barcode || "")
    .filter(Boolean)
    .slice(0, 6);
  if (!names.length) return null;
  return pending.length > names.length
    ? `${names.join(", ")} и ещё ${pending.length - names.length}`
    : names.join(", ");
}

function buildOrderIssueContextNote(order = null, matches = []) {
  const parts = [];
  if (order?.wb_order_id != null) parts.push(`Заказ ${Number(order.wb_order_id)}`);
  const orderLabel = [
    normalizeArticleKey(order?.article),
    String(order?.product_name || order?.productName || "").trim(),
  ]
    .filter(Boolean)
    .join(" · ");
  if (orderLabel) parts.push(orderLabel);
  const matchLabels = limitWarningLabels((Array.isArray(matches) ? matches : []).map((item) => formatSupplyMatchLabel(item)));
  if (matchLabels.length) {
    parts.push(`Подходящие товары в поставке: ${matchLabels.join(", ")}`);
  }
  return parts.join(". ") || null;
}

async function reportAutoEmployeeOrderScanIssue({
  supplyId,
  employeeId,
  order = null,
  matches = [],
  errorMessage,
  scannedBarcode = null,
}) {
  try {
    const issue = await createOrUpdateEmployeeScanIssue({
      supplyId,
      orderId: order?.wb_order_id == null ? null : Number(order.wb_order_id),
      employeeId,
      issueStage: SCAN_ISSUE_STAGE_ORDER,
      errorMessage: errorMessage || null,
      contextNote: buildOrderIssueContextNote(order, matches),
      orderBarcode: normalizeBarcode(order?.barcode),
      scannedBarcode: normalizeBarcode(scannedBarcode),
      article: order?.article || null,
      productName: order?.product_name || order?.productName || null,
      nmId: order?.nm_id || order?.nmId || null,
    });
    notifyScanIssueUpdate();
    return issue;
  } catch {
    return null;
  }
}

async function refreshEmployeePickingSessionSummary(sessionId) {
  const totalsResult = await dbQuery(
    `select coalesce(sum(required_qty), 0)::int as required_qty,
            coalesce(sum(picked_qty), 0)::int as picked_qty
     from employee_supply_pick_items
     where session_id = $1`,
    [sessionId]
  );
  const requiredQty = Number(totalsResult.rows?.[0]?.required_qty || 0);
  const pickedQty = Number(totalsResult.rows?.[0]?.picked_qty || 0);
  const result = await dbQuery(
    `update employee_supply_sessions
     set picking_required_qty = $2,
         picking_collected_qty = $3,
         updated_at = now()
     where id = $1
     returning *`,
    [sessionId, requiredQty, pickedQty]
  );
  return result.rows?.[0] || null;
}

async function applyEmployeePickingSessionDelta(sessionId, requiredDelta = 0, collectedDelta = 0) {
  const normalizedRequiredDelta = Number.isFinite(Number(requiredDelta)) ? Number(requiredDelta) : 0;
  const normalizedCollectedDelta = Number.isFinite(Number(collectedDelta)) ? Number(collectedDelta) : 0;
  const result = await dbQuery(
    `update employee_supply_sessions
     set picking_required_qty =
           greatest(0, coalesce(picking_required_qty, 0) + $2),
         picking_collected_qty =
           greatest(
             0,
             least(
               greatest(0, coalesce(picking_required_qty, 0) + $2),
               coalesce(picking_collected_qty, 0) + $3
             )
           ),
         updated_at = now()
     where id = $1
     returning *`,
    [sessionId, normalizedRequiredDelta, normalizedCollectedDelta]
  );
  return result.rows?.[0] || null;
}

async function getEmployeePickItemById(sessionId, itemId) {
  const result = await dbQuery(
    `select *
     from employee_supply_pick_items
     where session_id = $1 and id = $2
     limit 1`,
    [sessionId, itemId]
  );
  return result.rows?.[0] || null;
}

async function allowEmployeePickItemWithoutScan(itemId, adminId) {
  const result = await dbQuery(
    `update employee_supply_pick_items
     set bypass_without_scan_allowed = true,
         bypass_authorized_at = now(),
         bypass_authorized_by = $2,
         updated_at = now()
     where id = $1
     returning *`,
    [itemId, adminId]
  );
  return result.rows?.[0] || null;
}

async function collectEmployeePickItemWithoutScan(itemId, { requireBypass = true } = {}) {
  const result = await dbQuery(
    `update employee_supply_pick_items
     set picked_qty = required_qty,
         updated_at = now()
     where id = $1
       and ($2::boolean = false or bypass_without_scan_allowed = true)
       and picked_qty < required_qty
     returning *`,
    [itemId, requireBypass !== false]
  );
  return result.rows?.[0] || null;
}

async function syncEmployeeProductPickItems(session) {
  if (!session?.id) return [];
  if (normalizeSessionRole(session.session_role) === SESSION_ROLE_PACK) {
    await dbQuery("delete from employee_supply_pick_items where session_id = $1", [session.id]);
    await refreshEmployeePickingSessionSummary(session.id);
    return [];
  }
  const assignmentPartId = Number(session.assignment_part_id || 0);
  const result = await dbQuery(
    `select article, barcode, product_name, nm_id,
            coalesce(sum(greatest(coalesce(quantity, 1), 1)), 0)::int as required_qty
     from supply_orders
     where supply_id = $1
       and collected_at is null
       ${assignmentPartId > 0 ? "and assignment_part_id = $2" : "and assigned_user_id = $2"}
     group by article, barcode, product_name, nm_id
     order by required_qty desc, product_name asc`,
    assignmentPartId > 0 ? [session.supply_id, assignmentPartId] : [session.supply_id, session.user_id]
  );
  const rows = result.rows || [];
  const existing = await listEmployeePickItemsForSession(session.id);
  if (!rows.length) {
    if (existing.length) {
      await dbQuery("delete from employee_supply_pick_items where session_id = $1", [session.id]);
    }
    await refreshEmployeePickingSessionSummary(session.id);
    return [];
  }

  const desiredItems = rows
    .map((row) => {
      const itemKey = getOrderIdentityKey({
        article: row.article,
        barcode: row.barcode,
        nm_id: row.nm_id,
        product_name: row.product_name,
      });
      if (!itemKey) return null;
      return {
        itemKey,
        article: row.article || null,
        barcode: row.barcode || null,
        productName: row.product_name || null,
        nmId: row.nm_id ?? null,
        requiredQty: Math.max(0, Number(row.required_qty || 0)),
      };
    })
    .filter(Boolean);

  const desiredByKey = new Map(desiredItems.map((item) => [item.itemKey, item]));
  const existingByKey = new Map(existing.map((row) => [String(row.item_key || ""), row]));

  const staleIds = existing
    .filter((row) => !desiredByKey.has(String(row.item_key || "")))
    .map((row) => Number(row.id))
    .filter((id) => Number.isFinite(id) && id > 0);
  if (staleIds.length) {
    await dbQuery(
      "delete from employee_supply_pick_items where session_id = $1 and id = any($2::bigint[])",
      [session.id, staleIds]
    );
  }

  for (const item of desiredItems) {
    const existingRow = existingByKey.get(item.itemKey);
    if (!existingRow) {
      await dbQuery(
        `insert into employee_supply_pick_items
          (session_id, supply_id, user_id, item_key, article, barcode, product_name, nm_id, required_qty, picked_qty, created_at, updated_at)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, now(), now())
         on conflict (session_id, item_key)
         do update set
           article = excluded.article,
           barcode = excluded.barcode,
           product_name = excluded.product_name,
           nm_id = excluded.nm_id,
           required_qty = excluded.required_qty,
           updated_at = now()`,
        [
          session.id,
          session.supply_id,
          session.user_id,
          item.itemKey,
          item.article,
          item.barcode,
          item.productName,
          item.nmId,
          item.requiredQty,
        ]
      );
      continue;
    }
    const nextPickedQty = Math.min(
      Math.max(0, Number(existingRow.picked_qty || 0)),
      item.requiredQty
    );
    await dbQuery(
      `update employee_supply_pick_items
       set article = $3,
           barcode = $4,
           product_name = $5,
           nm_id = $6,
           required_qty = $7,
           picked_qty = $8,
           updated_at = now()
       where id = $1 and session_id = $2`,
      [
        Number(existingRow.id),
        session.id,
        item.article,
        item.barcode,
        item.productName,
        item.nmId,
        item.requiredQty,
        nextPickedQty,
      ]
    );
  }

  await refreshEmployeePickingSessionSummary(session.id);
  return await listEmployeePickItemsForSession(session.id);
}

async function finishEmployeeProductPickingIfComplete(session) {
  if (!session?.id) return session;
  const refreshed = (await refreshEmployeePickingSessionSummary(session.id)) || session;
  if (
    normalizePickingStatus(refreshed.picking_status) !== PICKING_STATUS_STARTED ||
    !refreshed.picking_started_at ||
    refreshed.picking_finished_at
  ) {
    return refreshed;
  }
  const remainingQty = Math.max(
    0,
    Number(refreshed.picking_required_qty || 0) - Number(refreshed.picking_collected_qty || 0)
  );
  if (remainingQty > 0) {
    return refreshed;
  }
  const result = await dbQuery(
    `update employee_supply_sessions
     set picking_status = $2,
         picking_finished_at = now(),
         picking_total_seconds = case
           when picking_started_at is not null
             then greatest(picking_total_seconds, extract(epoch from (now() - picking_started_at))::int)
           else picking_total_seconds
         end,
         updated_at = now()
     where id = $1
     returning *`,
    [refreshed.id, PICKING_STATUS_FINISHED]
  );
  return result.rows?.[0] || refreshed;
}

async function closeEmptyEmployeeSupplySession(session) {
  if (!session?.id || session.finished_at || session.started_at) return session;
  const result = await dbQuery(
    `update employee_supply_sessions
     set status = 'reassigned',
         finished_at = now(),
         picking_status = case
           when picking_status in ('pending', 'started') then 'cancelled'
           else picking_status
         end,
         picking_finished_at = case
           when picking_status in ('pending', 'started') and picking_finished_at is null then now()
           else picking_finished_at
         end,
         picking_total_seconds = case
           when picking_status = 'started' and picking_started_at is not null and picking_finished_at is null
             then greatest(picking_total_seconds, extract(epoch from (now() - picking_started_at))::int)
           else picking_total_seconds
         end,
         updated_at = now()
     where id = $1 and finished_at is null and started_at is null
     returning *`,
    [session.id]
  );
  return result.rows?.[0] || session;
}

async function ensureEmployeeProductPickItems(session, options = {}) {
  if (!session?.id) return [];
  if (options?.force === true) return syncEmployeeProductPickItems(session);
  const existing = await listEmployeePickItemsForSession(session.id);
  if (existing.length) return existing;
  return syncEmployeeProductPickItems(session);
}

function normalizeEmployeePickCellName(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "")
    .replace(/C/g, "С")
    .replace(/P/g, "Р")
    .replace(/K/g, "К");
}

function parseEmployeePickCellSortValue(value) {
  const normalized = normalizeEmployeePickCellName(value);
  if (!normalized) {
    return { known: false, rack: Number.MAX_SAFE_INTEGER, shelf: Number.MAX_SAFE_INTEGER, box: Number.MAX_SAFE_INTEGER, tail: "", raw: "" };
  }
  const match = normalized.match(/^С(\d+)П(\d+)(?:К(\d+))?(.*)$/u);
  if (match) {
    return {
      known: true,
      rack: Number(match[1]),
      shelf: Number(match[2]),
      box: match[3] ? Number(match[3]) : -1,
      tail: match[4] || "",
      raw: normalized,
    };
  }
  return {
    known: false,
    rack: Number.MAX_SAFE_INTEGER,
    shelf: Number.MAX_SAFE_INTEGER,
    box: Number.MAX_SAFE_INTEGER,
    tail: "",
    raw: normalized,
  };
}

function compareEmployeePickCellNames(left, right) {
  const a = parseEmployeePickCellSortValue(left);
  const b = parseEmployeePickCellSortValue(right);
  if (a.known !== b.known) return a.known ? -1 : 1;
  if (a.rack !== b.rack) return a.rack - b.rack;
  if (a.shelf !== b.shelf) return a.shelf - b.shelf;
  if (a.box !== b.box) return a.box - b.box;
  if (a.tail !== b.tail) {
    return a.tail.localeCompare(b.tail, "ru", { numeric: true, sensitivity: "base" });
  }
  return a.raw.localeCompare(b.raw, "ru", { numeric: true, sensitivity: "base" });
}

function sortEmployeePickCells(cells) {
  return [...(Array.isArray(cells) ? cells : [])].sort((left, right) =>
    compareEmployeePickCellNames(left?.name || left?.id || "", right?.name || right?.id || "")
  );
}

function getEmployeePickRowPrimaryCellName(row) {
  const firstCell = sortEmployeePickCells(row?.ms_cells || [])[0];
  return firstCell?.name || firstCell?.id || "";
}

function compareEmployeePickRows(left, right) {
  const byCell = compareEmployeePickCellNames(
    getEmployeePickRowPrimaryCellName(left),
    getEmployeePickRowPrimaryCellName(right)
  );
  if (byCell !== 0) return byCell;
  const byArticle = String(left?.article || "").localeCompare(String(right?.article || ""), "ru", {
    numeric: true,
    sensitivity: "base",
  });
  if (byArticle !== 0) return byArticle;
  const byName = String(left?.product_name || "").localeCompare(String(right?.product_name || ""), "ru", {
    numeric: true,
    sensitivity: "base",
  });
  if (byName !== 0) return byName;
  return Number(left?.id || 0) - Number(right?.id || 0);
}

function sortEmployeePickRows(rows) {
  return [...(Array.isArray(rows) ? rows : [])].sort(compareEmployeePickRows);
}

async function sortEmployeePickMatchRows(rows, options = {}) {
  const items = Array.isArray(rows) ? rows : [];
  if (items.length <= 1) return items;
  try {
    return await enrichEmployeePickItemsWithMsCells(items, options);
  } catch {
    return sortEmployeePickRows(items);
  }
}

function normalizePickItemMsCells(status) {
  if (!Array.isArray(status?.slots)) return [];
  return sortEmployeePickCells(
    status.slots.map((entry) => ({
      id: entry?.id || null,
      name: entry?.name || null,
      stock: Number(entry?.stock || 0),
    }))
  );
}

async function loadFallbackPickItemMsCells(row, options = {}) {
  const warehouseOptions = getSkladWarehouseOptionsFromSettings(options);
  const barcodeKey = normalizeBarcode(row?.barcode);
  const articleKey = normalizeArticleKey(row?.article);
  if (!barcodeKey && !articleKey) return [];
  try {
    const status = barcodeKey
      ? await getSkladFbsStockStatusByBarcode(barcodeKey, {
          fallbackArticle: articleKey,
          warehouseId: warehouseOptions.warehouseId,
          storeName: warehouseOptions.storeName,
          force: true,
        })
      : await getSkladFbsStockStatusByArticle(articleKey, {
          warehouseId: warehouseOptions.warehouseId,
          storeName: warehouseOptions.storeName,
          marketplaceType: "wb",
          force: true,
        });
    return normalizePickItemMsCells(status);
  } catch {
    return [];
  }
}

async function enrichEmployeePickItemsWithMsCells(rows, options = {}) {
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) return [];
  const warehouseOptions = getSkladWarehouseOptionsFromSettings(options);
  const rowCacheKeys = new Map();
  const lookupRows = new Map();
  for (const row of items) {
    const rowId = row?.id || row?.item_key || "";
    const barcodeKey = normalizeBarcode(row?.barcode);
    const cacheKey = barcodeKey ? `barcode:${barcodeKey}` : "";
    if (!rowId) continue;
    rowCacheKeys.set(rowId, cacheKey);
    if (cacheKey && !lookupRows.has(cacheKey)) {
      lookupRows.set(cacheKey, row);
    }
  }
  const cellsByCacheKey = new Map();
  const barcodeRows = [...lookupRows.entries()]
    .map(([cacheKey, row]) => ({ cacheKey, row, barcode: normalizeBarcode(row?.barcode) }))
    .filter((entry) => entry.barcode);
  try {
    const bulkStatuses = await getMsStockStatusesByBarcode(
      barcodeRows.map((entry) => entry.barcode),
      { warehouseId: warehouseOptions.warehouseId, storeName: warehouseOptions.storeName }
    );
    for (const entry of barcodeRows) {
      cellsByCacheKey.set(entry.cacheKey, normalizePickItemMsCells(bulkStatuses.get(entry.barcode)));
    }
  } catch {
    // Fall back to per-barcode lookup below.
  }
  const missingRows = barcodeRows.filter((entry) => !cellsByCacheKey.has(entry.cacheKey) || !cellsByCacheKey.get(entry.cacheKey)?.length);
  await mapWithConcurrency(
    missingRows,
    EMPLOYEE_PICK_CELLS_CONCURRENCY,
    async ({ cacheKey, row, barcode }) => {
      try {
        let ms = await getMsStockStatusByBarcode(barcode, {
          warehouseId: warehouseOptions.warehouseId,
          storeName: warehouseOptions.storeName,
        });
        let cells = normalizePickItemMsCells(ms);
        if (!cells.length) {
          ms = await getMsStockStatusByBarcode(barcode, {
            warehouseId: warehouseOptions.warehouseId,
            storeName: warehouseOptions.storeName,
            force: true,
          });
          cells = normalizePickItemMsCells(ms);
        }
        if (!cells.length) {
          cells = await loadFallbackPickItemMsCells(row, warehouseOptions);
        }
        cellsByCacheKey.set(cacheKey, cells);
      } catch {
        cellsByCacheKey.set(cacheKey, await loadFallbackPickItemMsCells(row, warehouseOptions));
      }
      return null;
    }
  );
  return sortEmployeePickRows(
    items.map((row) => ({
      ...row,
      ms_cells: sortEmployeePickCells(cellsByCacheKey.get(rowCacheKeys.get(row?.id || row?.item_key)) || []),
    }))
  );
}

async function enrichSingleEmployeePickItemWithMsCells(row, options = {}) {
  if (!row) return null;
  const [enriched] = await enrichEmployeePickItemsWithMsCells([row], options);
  return enriched || row;
}

async function resetEmployeePickingForSupply(supplyId, pickingStatus = PICKING_STATUS_NOT_REQUIRED) {
  await dbQuery("delete from employee_supply_pick_items where supply_id = $1", [supplyId]);
  await dbQuery(
    `update employee_supply_sessions
     set picking_status = $2,
         picking_started_at = null,
         picking_finished_at = null,
         picking_total_seconds = 0,
         picking_required_qty = 0,
         picking_collected_qty = 0,
         updated_at = now()
     where supply_id = $1 and finished_at is null`,
    [supplyId, pickingStatus]
  );
}

async function supplyHasActiveEmployeeWork(supplyId) {
  const result = await dbQuery(
    `select 1
     from employee_supply_sessions
     where supply_id = $1
       and finished_at is null
       and (started_at is not null or picking_started_at is not null)
     limit 1`,
    [supplyId]
  );
  return Boolean(result.rows?.[0]);
}

async function supplyHasBlockingRedistributeWork(supplyId) {
  const result = await dbQuery(
    `select 1
     from employee_supply_sessions
     where supply_id = $1
       and finished_at is null
       and (
         started_at is not null
         or (picking_status = $2 and picking_finished_at is null)
       )
     limit 1`,
    [supplyId, PICKING_STATUS_STARTED]
  );
  return Boolean(result.rows?.[0]);
}

async function supplyHasStartedPackingWork(supplyId) {
  const result = await dbQuery(
    `select 1
     from employee_supply_sessions
     where supply_id = $1
       and started_at is not null
       and finished_at is null
     limit 1`,
    [supplyId]
  );
  return Boolean(result.rows?.[0]);
}

async function captureSupplyRedistributeCarryover(supplyId) {
  const parts = await listSupplyAssignmentParts(supplyId);
  if (!parts.length) {
    return { pickedOrders: [], partStates: new Map() };
  }

  const sessionResult = await dbQuery(
    `select assignment_part_id,
            bool_or(
              session_role in ($2, $3)
              and picking_status = $4
              and picking_finished_at is not null
            ) as picking_finished
     from employee_supply_sessions
     where supply_id = $1
       and assignment_part_id is not null
     group by assignment_part_id`,
    [supplyId, SESSION_ROLE_PICK, SESSION_ROLE_PICK_PACK, PICKING_STATUS_FINISHED]
  );
  const sessionStateByPartId = new Map(
    (sessionResult.rows || []).map((row) => [
      Number(row.assignment_part_id),
      {
        pickingFinished: row.picking_finished === true,
      },
    ])
  );

  const partStates = new Map();
  for (const part of parts) {
    const normalizedPartId = Number(part.id || 0);
    if (!normalizedPartId) continue;
    const sessionState = sessionStateByPartId.get(normalizedPartId) || null;
    const pickedComplete = Boolean(sessionState?.pickingFinished || part.pickerFinishedAt);
    const inventoryUserId = pickedComplete ? getAssignmentPartEffectivePickerUserId(part) : null;
    partStates.set(normalizedPartId, {
      ...part,
      pickedComplete,
      inventoryUserId,
    });
  }

  const pickedPartIds = Array.from(partStates.values())
    .filter((entry) => entry.pickedComplete && Number(entry.inventoryUserId || 0) > 0)
    .map((entry) => Number(entry.id));
  if (!pickedPartIds.length) {
    return { pickedOrders: [], partStates };
  }

  const orderResult = await dbQuery(
    `select wb_order_id,
            assignment_part_id,
            article,
            barcode,
            greatest(coalesce(quantity, 1), 1)::int as qty
     from supply_orders
     where supply_id = $1
       and collected_at is null
       and assignment_part_id = any($2)
     order by wb_order_id asc`,
    [supplyId, pickedPartIds]
  );

  const pickedOrders = [];
  for (const row of orderResult.rows || []) {
    const normalizedPartId = Number(row.assignment_part_id || 0);
    const state = partStates.get(normalizedPartId);
    if (!state?.pickedComplete || !state.inventoryUserId) continue;
    pickedOrders.push({
      orderId: Number(row.wb_order_id),
      oldPartId: normalizedPartId,
      oldHolderUserId: Number(state.inventoryUserId),
      article: normalizeArticleKey(row.article),
      barcode: normalizeBarcode(row.barcode),
      qty: Math.max(1, Number(row.qty || 0)),
    });
  }

  return { pickedOrders, partStates };
}

async function applySupplyPickedCarryoverToSession(session, items = []) {
  const normalizedSessionId = Number(session?.id || 0);
  if (!normalizedSessionId || !Array.isArray(items) || !items.length) {
    return session || null;
  }

  await ensureEmployeeProductPickItems(session);
  const pickItems = await listEmployeePickItemsForSession(normalizedSessionId);
  const pickItemByBarcode = new Map();
  const pickItemByArticle = new Map();
  for (const pickItem of pickItems) {
    const barcodeKey = normalizeBarcode(pickItem?.barcode);
    const articleKey = normalizeArticleKey(pickItem?.article);
    if (barcodeKey && !pickItemByBarcode.has(barcodeKey)) pickItemByBarcode.set(barcodeKey, pickItem);
    if (articleKey && !pickItemByArticle.has(articleKey)) pickItemByArticle.set(articleKey, pickItem);
  }

  const pickedQtyByItemId = new Map();
  for (const item of items) {
    const barcodeKey = normalizeBarcode(item?.barcode);
    const articleKey = normalizeArticleKey(item?.article);
    const targetPickItem =
      (barcodeKey ? pickItemByBarcode.get(barcodeKey) : null) ||
      (articleKey ? pickItemByArticle.get(articleKey) : null) ||
      null;
    if (!targetPickItem) continue;
    const itemId = Number(targetPickItem.id || 0);
    if (!itemId) continue;
    pickedQtyByItemId.set(itemId, (pickedQtyByItemId.get(itemId) || 0) + Math.max(1, Number(item?.qty || 0)));
  }

  for (const [itemId, qty] of pickedQtyByItemId.entries()) {
    await dbQuery(
      `update employee_supply_pick_items
       set picked_qty = least(required_qty, picked_qty + $2),
           updated_at = now()
       where id = $1`,
      [itemId, Math.max(1, Number(qty || 0))]
    );
  }

  const refreshedSession = (await refreshEmployeePickingSessionSummary(normalizedSessionId)) || session;
  if (
    Number(refreshedSession?.picking_required_qty || 0) > 0 &&
    Number(refreshedSession?.picking_collected_qty || 0) >= Number(refreshedSession?.picking_required_qty || 0)
  ) {
    const finishedResult = await dbQuery(
      `update employee_supply_sessions
       set picking_status = $2,
           picking_started_at = coalesce(picking_started_at, now()),
           picking_finished_at = coalesce(picking_finished_at, now()),
           updated_at = now()
       where id = $1
       returning *`,
      [normalizedSessionId, PICKING_STATUS_FINISHED]
    );
    const finishedSession = finishedResult.rows?.[0] || refreshedSession;
    if (
      normalizeSessionRole(finishedSession?.session_role) === SESSION_ROLE_PICK &&
      Number(finishedSession?.assignment_part_id || 0) > 0
    ) {
      await dbQuery(
        `update supply_assignment_parts
         set picker_finished_at = coalesce(picker_finished_at, now()),
             handoff_status = case
               when handoff_status = $2 then handoff_status
               else $3
             end,
             updated_at = now()
         where id = $1`,
        [Number(finishedSession.assignment_part_id), HANDOFF_STATUS_NOT_REQUIRED, HANDOFF_STATUS_WAITING_PICK]
      );
    }
    return finishedSession;
  }

  return refreshedSession;
}

async function applySupplyRedistributeCarryover(supplyId, carryover, parts, sessions, settings) {
  const pickedOrders = Array.isArray(carryover?.pickedOrders) ? carryover.pickedOrders : [];
  if (!pickedOrders.length) return;

  const normalizedOrderIds = Array.from(
    new Set(
      pickedOrders
        .map((item) => Number(item.orderId || 0))
        .filter((value) => Number.isInteger(value) && value > 0)
    )
  );
  if (!normalizedOrderIds.length) return;

  const reassignedOrdersResult = await dbQuery(
    `select wb_order_id, assignment_part_id
     from supply_orders
     where supply_id = $1
       and wb_order_id = any($2)`,
    [supplyId, normalizedOrderIds]
  );
  const newPartIdByOrderId = new Map(
    (reassignedOrdersResult.rows || []).map((row) => [
      Number(row.wb_order_id),
      Number(row.assignment_part_id || 0),
    ])
  );
  const partById = new Map(
    (Array.isArray(parts) ? parts : [])
      .map((part) => [Number(part?.id || 0), part])
      .filter(([partId]) => partId > 0)
  );
  const pickingSessionByPartId = new Map();
  for (const session of Array.isArray(sessions) ? sessions : []) {
    const partId = Number(session?.assignment_part_id || 0);
    const sessionRole = normalizeSessionRole(session?.session_role);
    if (!partId) continue;
    if (![SESSION_ROLE_PICK, SESSION_ROLE_PICK_PACK].includes(sessionRole)) continue;
    if (!pickingSessionByPartId.has(partId)) {
      pickingSessionByPartId.set(partId, session);
    }
  }

  const transferGroups = new Map();
  const carryoverBySessionId = new Map();
  for (const item of pickedOrders) {
    const orderId = Number(item.orderId || 0);
    const newPartId = Number(newPartIdByOrderId.get(orderId) || 0);
    const newPart = partById.get(newPartId) || null;
    if (!newPart) continue;
    const targetSession = pickingSessionByPartId.get(newPartId) || null;
    if (!targetSession) continue;
    const newHolderUserId = Number(getAssignmentPartEffectivePickerUserId(newPart) || 0);
    if (!newHolderUserId) continue;

    const sessionKey = Number(targetSession.id || 0);
    if (sessionKey > 0) {
      if (!carryoverBySessionId.has(sessionKey)) carryoverBySessionId.set(sessionKey, []);
      carryoverBySessionId.get(sessionKey).push({
        article: item.article,
        barcode: item.barcode,
        qty: Math.max(1, Number(item.qty || 0)),
      });
    }

    const oldHolderUserId = Number(item.oldHolderUserId || 0);
    if (!oldHolderUserId || oldHolderUserId === newHolderUserId) continue;
    const transferKey = [
      oldHolderUserId,
      newHolderUserId,
      item.article || "",
      item.barcode || "",
    ].join("::");
    const current = transferGroups.get(transferKey) || {
      fromUserId: oldHolderUserId,
      toUserId: newHolderUserId,
      article: item.article || null,
      barcode: item.barcode || null,
      qty: 0,
    };
    current.qty += Math.max(1, Number(item.qty || 0));
    transferGroups.set(transferKey, current);
  }

  if (isStockMethodEnabled(settings) && transferGroups.size > 0) {
    const externalEmployeeIdCache = new Map();
    const getExternalEmployeeIdCached = async (userId) => {
      const normalizedUserId = Number(userId || 0);
      if (!normalizedUserId) return null;
      if (externalEmployeeIdCache.has(normalizedUserId)) {
        return externalEmployeeIdCache.get(normalizedUserId);
      }
      const externalEmployeeId = await getUserExternalEmployeeId(normalizedUserId);
      externalEmployeeIdCache.set(normalizedUserId, externalEmployeeId);
      return externalEmployeeId;
    };

    for (const group of transferGroups.values()) {
      const fromExternalEmployeeId = await getExternalEmployeeIdCached(group.fromUserId);
      const toExternalEmployeeId = await getExternalEmployeeIdCached(group.toUserId);
      if (!fromExternalEmployeeId || !toExternalEmployeeId) {
        throw new Error("Сотрудник не привязан к складской системе");
      }
        const movementResult = await transferEmployeeInventory({
        fromExternalEmployeeId,
        toExternalEmployeeId,
        barcodes: group.barcode ? [group.barcode] : [],
        article: group.article || null,
        allowArticleFallback: true,
        quantity: Math.max(1, Number(group.qty || 0)),
        skipUnavailable: isAllowStockShortage(settings),
        source: "wb_redistribute",
          notes: `WB: перенос уже собранного товара после перераспределения. Поставка ${supplyId}.`,
          negativeNotes: `WB: перенос уже собранного товара после перераспределения с уходом источника в минус. Поставка ${supplyId}.`,
        });
        await recordStockShortageSkip({ supplyId, stage: "redistribute", article: group.article, barcode: group.barcode, requestedQuantity: Math.max(1, Number(group.qty || 0)), movementResult });
    }
  }

  for (const session of Array.isArray(sessions) ? sessions : []) {
    const sessionId = Number(session?.id || 0);
    if (!sessionId || !carryoverBySessionId.has(sessionId)) continue;
    await applySupplyPickedCarryoverToSession(session, carryoverBySessionId.get(sessionId));
  }
}

async function setSupplyEmployeeWorkflowStage(supplyId, stage, userId = null) {
  const normalizedStage = normalizeEmployeeWorkflowStage(stage);
  const pickingStartedAt = normalizedStage === EMPLOYEE_WORKFLOW_PICK_ACTIVE ? new Date() : null;
  const result = await dbQuery(
    `update supply_settings
     set employee_workflow_stage = $2,
         product_picking_started_at = case
           when $2 = 'pick_active' then coalesce(product_picking_started_at, $3)
           else null
         end,
         product_picking_started_by = case
           when $2 = 'pick_active' then coalesce(product_picking_started_by, $4)
           else null
         end,
         updated_at = now()
     where supply_id = $1
     returning *`,
    [supplyId, normalizedStage, pickingStartedAt, userId]
  );
  return result.rows?.[0] || null;
}

function buildEmployeeWorkflowState(settings, session, activeSession, assignmentPart = null) {
  const workflowMode = normalizeEmployeeWorkflowMode(settings?.employee_workflow_mode);
  const workflowStage = normalizeEmployeeWorkflowStage(settings?.employee_workflow_stage);
  const sessionRole = normalizeSessionRole(session?.session_role);
  const partRoleMode = normalizeAssignmentPartMode(assignmentPart?.roleMode || assignmentPart?.role_mode);
  const splitAssignment = partRoleMode === ASSIGNMENT_PART_MODE_SPLIT && sessionRole !== SESSION_ROLE_PICK_PACK;
  const tableNo = normalizeAssignmentTableNo(assignmentPart?.tableNo || assignmentPart?.table_no || session?.table_no);
  const handoffStatus = normalizeHandoffStatus(assignmentPart?.handoffStatus || assignmentPart?.handoff_status);
  const productPickingRequired =
    (sessionRole === SESSION_ROLE_PACK ? false : workflowMode === EMPLOYEE_WORKFLOW_MODE_PICKING) &&
    isEmployeeProductPickingEnabled(workflowStage);
  const productPickingActive = isEmployeeProductPickingActive(workflowStage);
  const pickingStatus = normalizePickingStatus(session?.picking_status);
  const pickingFinished = pickingStatus === PICKING_STATUS_FINISHED && Boolean(session?.picking_finished_at);
  const pickingStarted = pickingStatus === PICKING_STATUS_STARTED && Boolean(session?.picking_started_at) && !session?.picking_finished_at;
  const pickerReadyForHandoff = splitAssignment && handoffStatus === HANDOFF_STATUS_READY;
  const handoffTransferred = splitAssignment && handoffStatus === HANDOFF_STATUS_TRANSFERRED;
  const waitingForPicker = splitAssignment && sessionRole === SESSION_ROLE_PACK && !pickerReadyForHandoff && !handoffTransferred;
  const waitingForTable = waitingForPicker && Boolean(assignmentPart?.pickerFinishedAt || assignmentPart?.picker_finished_at);
  const canStartPicking =
    productPickingRequired &&
    productPickingActive &&
    !pickingFinished &&
    !pickingStarted &&
    sessionRole !== SESSION_ROLE_PACK;
  const canFinishPicking = productPickingRequired && pickingStarted;
  const canConfirmTablePlacement =
    splitAssignment &&
    sessionRole === SESSION_ROLE_PICK &&
    pickingFinished &&
    !pickerReadyForHandoff &&
    !handoffTransferred;
  const canStartPacking =
    Boolean(session) &&
    !session?.started_at &&
    (sessionRole === SESSION_ROLE_PICK
      ? false
      : sessionRole === SESSION_ROLE_PACK
      ? pickerReadyForHandoff || handoffTransferred
      : (!productPickingRequired || pickingFinished));
  const canWork = Boolean(session && session.started_at && !session.finished_at);
  const isWaitingAdmin =
    sessionRole !== SESSION_ROLE_PACK &&
    productPickingRequired &&
    !productPickingActive &&
    !pickingFinished;
  const currentStage = canWork
    ? "packing"
    : pickingStarted
    ? "picking"
    : canConfirmTablePlacement
    ? "table_confirm"
    : waitingForTable
    ? "waiting_table"
    : waitingForPicker
    ? "waiting_pick"
    : pickingFinished
    ? "packing_ready"
    : productPickingRequired
    ? productPickingActive
      ? "picking_ready"
      : "waiting_admin"
    : "packing_ready";

  return {
    workflowMode,
    workflowStage,
    sessionRole,
    splitAssignment,
    handoffStatus,
    tableNo,
    allowNoScan: settings?.allow_no_scan === true,
    partialSafeScan: settings?.partial_safe_scan === true,
    desktopPrint: settings?.desktop_print === true,
    allowEmployeeClose: settings?.allow_employee_close === true,
    productPickingRequired,
    productPickingActive,
    pickingStatus,
    currentStage,
    canStartPicking,
    canFinishPicking,
    canConfirmTablePlacement,
    canStartPacking,
    canWork,
    isWaitingAdmin,
    isWaitingForPicker: waitingForPicker,
    isWaitingForTable: waitingForTable,
  };
}

async function ensureEmployeeSessionPickingStatus(settings, session) {
  if (!session) return session;
  if (normalizeSessionRole(session.session_role) === SESSION_ROLE_PACK) return session;
  if (!isEmployeeProductPickingEnabled(settings?.employee_workflow_stage)) return session;
  if (normalizePickingStatus(session.picking_status) !== PICKING_STATUS_NOT_REQUIRED) return session;
  const result = await dbQuery(
    `update employee_supply_sessions
     set picking_status = $2,
         updated_at = now()
     where id = $1
     returning *`,
    [session.id, PICKING_STATUS_PENDING]
  );
  return result.rows?.[0] || session;
}

async function startEmployeeProductPicking(supplyId, userId) {
  const openSession = await getEmployeeOpenSession(supplyId, userId);
  if (!openSession) {
    return { session: null, pickItems: [], error: "Поставка не назначена сотруднику" };
  }
  if (normalizeSessionRole(openSession.session_role) === SESSION_ROLE_PACK) {
    return { session: null, pickItems: [], error: "Для этой части поставки товар собирает другой сотрудник" };
  }
  const settings = await getSupplySettings(supplyId);
  const workflowStage = normalizeEmployeeWorkflowStage(settings?.employee_workflow_stage);
  if (!isEmployeeProductPickingEnabled(workflowStage)) {
    return { session: openSession, pickItems: [], error: "Для этой поставки сборка товара не требуется" };
  }
  if (!isEmployeeProductPickingActive(workflowStage)) {
    return { session: null, pickItems: [], error: "Администратор ещё не запустил сборку товара" };
  }
  if (normalizePickingStatus(openSession.picking_status) === PICKING_STATUS_FINISHED && openSession.picking_finished_at) {
    return { session: openSession, pickItems: [], error: null };
  }
  if (normalizePickingStatus(openSession.picking_status) === PICKING_STATUS_STARTED && openSession.picking_started_at && !openSession.picking_finished_at) {
    const pickItems = await ensureEmployeeProductPickItems(openSession);
    const refreshed = (await getEmployeeOpenSession(supplyId, userId)) || openSession;
    return { session: refreshed, pickItems, error: null };
  }
  // Категоричное правило: нельзя начать сборку товара, пока не загружены ВСЕ этикетки заказов.
  const labelStat = await dbQuery(
    `select count(*)::int as total,
            count(*) filter (where coalesce(sticker_url, '') <> '')::int as ready
     from supply_orders
     where supply_id = $1`,
    [supplyId]
  );
  const lt = labelStat.rows?.[0];
  if (lt && Number(lt.total) > 0 && Number(lt.ready) < Number(lt.total)) {
    return {
      session: openSession,
      pickItems: [],
      error: `Этикетки ещё загружаются (${lt.ready}/${lt.total}). Дождитесь загрузки всех этикеток, затем начните сборку товара.`,
      labelsPending: true,
    };
  }
  // Разрешаем начинать сборку товара по любой назначенной поставке (несколько параллельно).
  const updated = await dbQuery(
    `update employee_supply_sessions
     set picking_status = $2,
         picking_started_at = now(),
         picking_finished_at = null,
         picking_total_seconds = 0,
         updated_at = now()
     where id = $1
     returning *`,
    [openSession.id, PICKING_STATUS_STARTED]
  );
  const session = updated.rows?.[0] || openSession;
  if (Number(session.assignment_part_id || 0) > 0 && normalizeSessionRole(session.session_role) === SESSION_ROLE_PICK) {
    await dbQuery(
      `update supply_assignment_parts
       set picker_started_at = coalesce(picker_started_at, now()),
           updated_at = now()
       where id = $1`,
      [Number(session.assignment_part_id)]
    );
  }
  const pickItems = await ensureEmployeeProductPickItems(session);
  const refreshed = (await getEmployeeOpenSession(supplyId, userId)) || session;
  return { session: refreshed, pickItems, error: null };
}

async function finishEmployeeProductPicking(supplyId, userId) {
  const session = await getEmployeeOpenSession(supplyId, userId);
  if (!session) {
    return { session: null, error: "Поставка не назначена сотруднику" };
  }
  if (normalizeSessionRole(session.session_role) === SESSION_ROLE_PACK) {
    return { session: null, error: "Для этой части поставки товар собирает другой сотрудник" };
  }
  if (normalizePickingStatus(session.picking_status) !== PICKING_STATUS_STARTED || !session.picking_started_at || session.picking_finished_at) {
    return { session: null, error: "Сборка товара ещё не начата" };
  }
  const refreshed = (await refreshEmployeePickingSessionSummary(session.id)) || session;
  const remainingQty = Math.max(
    0,
    Number(refreshed.picking_required_qty || 0) - Number(refreshed.picking_collected_qty || 0)
  );
  if (remainingQty > 0) {
    return { session: refreshed, error: `Осталось собрать ${remainingQty} шт` };
  }
  const result = await dbQuery(
    `update employee_supply_sessions
     set picking_status = $2,
         picking_finished_at = now(),
         picking_total_seconds = greatest(picking_total_seconds, extract(epoch from (now() - picking_started_at))::int),
         updated_at = now()
     where id = $1
     returning *`,
    [session.id, PICKING_STATUS_FINISHED]
  );
  const finishedSession = result.rows?.[0] || refreshed;
  if (Number(finishedSession.assignment_part_id || 0) > 0 && normalizeSessionRole(finishedSession.session_role) === SESSION_ROLE_PICK) {
    await dbQuery(
      `update supply_assignment_parts
       set picker_finished_at = now(),
           updated_at = now()
       where id = $1`,
      [Number(finishedSession.assignment_part_id)]
    );
  }
  return { session: finishedSession, error: null };
}

async function confirmEmployeeSupplyTablePlacement(supplyId, userId) {
  const session = await getEmployeeOpenSession(supplyId, userId);
  if (!session) {
    return { session: null, part: null, error: "Поставка не назначена сотруднику" };
  }
  if (normalizeSessionRole(session.session_role) !== SESSION_ROLE_PICK || Number(session.assignment_part_id || 0) < 1) {
    return { session, part: null, error: "Для этой части поставки подтверждение стола не требуется" };
  }
  if (normalizePickingStatus(session.picking_status) !== PICKING_STATUS_FINISHED || !session.picking_finished_at) {
    return { session, part: null, error: "Сначала завершите сборку товара" };
  }
  const part = await getSupplyAssignmentPartById(session.assignment_part_id);
  if (!part || !isSplitAssignmentPart(part)) {
    return { session, part: null, error: "Часть поставки не найдена" };
  }
  const currentStatus = normalizeHandoffStatus(part.handoffStatus);
  if (currentStatus === HANDOFF_STATUS_TRANSFERRED) {
    return { session, part, error: "Товар уже передан упаковщику" };
  }
  await dbQuery(
    `update supply_assignment_parts
     set handoff_status = $2,
         placed_on_table_at = coalesce(placed_on_table_at, now()),
         updated_at = now()
     where id = $1`,
    [Number(part.id), HANDOFF_STATUS_READY]
  );
  const sessionResult = await dbQuery(
    `update employee_supply_sessions
     set status = 'finished',
         finished_at = coalesce(finished_at, now()),
         updated_at = now()
     where id = $1
     returning *`,
    [session.id]
  );
  return {
    session: sessionResult.rows?.[0] || session,
    part: await getSupplyAssignmentPartById(part.id),
    error: null,
  };
}

async function performSupplyPartHandoffToPacker(supplyId, session, assignmentPart, settings) {
  if (!session || normalizeSessionRole(session.session_role) !== SESSION_ROLE_PACK || !isSplitAssignmentPart(assignmentPart)) {
    return { ok: true, part: assignmentPart };
  }
  const handoffStatus = normalizeHandoffStatus(assignmentPart.handoffStatus);
  if (handoffStatus === HANDOFF_STATUS_TRANSFERRED) {
    return { ok: true, part: assignmentPart };
  }
  if (handoffStatus !== HANDOFF_STATUS_READY) {
    return { ok: false, error: "Сначала дождитесь, пока сборщик товара закончит и подтвердит стол" };
  }
  await dbQuery(
    `update supply_assignment_parts
     set transfer_started_at = coalesce(transfer_started_at, now()),
         updated_at = now()
     where id = $1`,
    [Number(assignmentPart.id)]
  );
  if (isStockMethodEnabled(settings)) {
    const fromExternalEmployeeId = await getUserExternalEmployeeId(assignmentPart.pickerUserId);
    const toExternalEmployeeId = await getUserExternalEmployeeId(session.user_id);
    if (!fromExternalEmployeeId || !toExternalEmployeeId) {
      return { ok: false, error: "Сотрудник не привязан к складской системе" };
    }
    const itemsResult = await dbQuery(
      `select article, barcode, coalesce(sum(greatest(coalesce(quantity, 1), 1)), 0)::int as qty
       from supply_orders
       where supply_id = $1
         and assignment_part_id = $2
         and collected_at is null
       group by article, barcode
       order by article asc nulls last, barcode asc nulls last`,
      [supplyId, Number(assignmentPart.id)]
    );
    for (const row of itemsResult.rows || []) {
      const movementResult = await transferEmployeeInventory({
        fromExternalEmployeeId,
        toExternalEmployeeId,
        barcodes: [row.barcode],
        article: row.article,
        allowArticleFallback: true,
        quantity: Math.max(1, Number(row.qty || 0)),
        skipUnavailable: isAllowStockShortage(settings),
        source: "wb_handoff",
        notes: `WB: передача товара между сотрудниками. Поставка ${supplyId}, часть ${assignmentPart.partNo}, стол ${assignmentPart.tableNo || "-"}.`,
        negativeNotes: `WB: передача товара между сотрудниками с уходом источника в минус. Поставка ${supplyId}, часть ${assignmentPart.partNo}, стол ${assignmentPart.tableNo || "-"}.`,
      });
      await recordStockShortageSkip({ supplyId, stage: "handoff", article: row.article, barcode: row.barcode, requestedQuantity: Math.max(1, Number(row.qty || 0)), movementResult, userId: session.user_id });
    }
    writeSborkaLiveEventSafe({
      externalEmployeeId: toExternalEmployeeId,
      eventType: "handoff",
      orderId: String(supplyId),
      productName: `Поставка ${supplyId}`,
      barcode: null,
      quantity: 1,
      marketplace: "wb",
    });
  }
  await dbQuery(
    `update supply_orders
     set assigned_user_id = $3,
         assigned_at = now()
     where supply_id = $1
       and assignment_part_id = $2
       and collected_at is null`,
    [supplyId, Number(assignmentPart.id), Number(session.user_id)]
  );
  await dbQuery(
    `update supply_assignment_parts
     set handoff_status = $2,
         transferred_at = coalesce(transferred_at, now()),
         updated_at = now()
     where id = $1`,
    [Number(assignmentPart.id), HANDOFF_STATUS_TRANSFERRED]
  );
  return { ok: true, part: await getSupplyAssignmentPartById(assignmentPart.id) };
}

function toCollectorSessionPayload(row) {
  if (!row) return null;
  const totalSecondsStored = Number(row.total_seconds || 0);
  const startedAt = row.started_at ? new Date(row.started_at).getTime() : NaN;
  const isRunning = row.status === "started" && !row.finished_at && Number.isFinite(startedAt);
  const liveSeconds = isRunning ? Math.max(0, Math.round((Date.now() - startedAt) / 1000)) : 0;
  return {
    id: Number(row.id),
    supplyId: row.supply_id,
    status: row.status || "started",
    startedAt: row.started_at || null,
    finishedAt: row.finished_at || null,
    totalSeconds: isRunning ? Math.max(totalSecondsStored, liveSeconds) : totalSecondsStored,
    orderCount: Number(row.order_count || 0),
    itemCount: Number(row.item_count || 0),
    supplyName: row.supply_name || null,
    storeId: row.store_id || null,
    storeName: row.store_name || null,
  };
}

async function getCollectorOpenSupplySession(supplyId, userId) {
  const result = await dbQuery(
    `select *
     from collector_supply_sessions
     where supply_id = $1 and user_id = $2 and status = 'started' and finished_at is null
     order by started_at desc, id desc
     limit 1`,
    [supplyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getLatestCollectorSupplySession(supplyId, userId) {
  const result = await dbQuery(
    `select *
     from collector_supply_sessions
     where supply_id = $1 and user_id = $2
     order by started_at desc, id desc
     limit 1`,
    [supplyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getCollectorSupplyTotals(supplyId) {
  const result = await dbQuery(
    `select count(*)::int as orders,
            coalesce(sum(greatest(coalesce(quantity, 1), 1)), 0)::int as items
     from supply_orders
     where supply_id = $1
       and assigned_user_id is not null
       and collected_at is null`,
    [supplyId]
  );
  return {
    orders: Number(result.rows?.[0]?.orders || 0),
    items: Number(result.rows?.[0]?.items || 0),
  };
}

async function ensureCollectorSupplySession(supplyId, userId) {
  const openSession = await getCollectorOpenSupplySession(supplyId, userId);
  if (openSession) {
    return { session: openSession, error: null };
  }
  const latest = await getLatestCollectorSupplySession(supplyId, userId);
  if (latest?.status === "finished") {
    return { session: null, error: "Поставка уже отмечена как собранная" };
  }
  const totals = await getCollectorSupplyTotals(supplyId);
  if (totals.orders < 1) {
    return { session: null, error: "Нет заказов для сборки товаров" };
  }
  const settings = await getSupplySettings(supplyId);
  const result = await dbQuery(
    `insert into collector_supply_sessions
      (supply_id, user_id, status, started_at, order_count, item_count, supply_name, store_id, store_name, created_at, updated_at)
     values ($1, $2, 'started', now(), $3, $4, $5, $6, $7, now(), now())
     returning *`,
    [
      supplyId,
      userId,
      totals.orders,
      totals.items,
      settings?.supply_name || null,
      settings?.store_id || null,
      settings?.store_name || null,
    ]
  );
  return { session: result.rows?.[0] || null, error: null };
}

async function finishCollectorSupplySession(supplyId, userId) {
  const openSession = await getCollectorOpenSupplySession(supplyId, userId);
  if (!openSession) {
    const latest = await getLatestCollectorSupplySession(supplyId, userId);
    if (latest?.status === "finished") {
      return { session: latest, error: null, alreadyFinished: true };
    }
    return { session: null, error: "Сессия сборщика не запущена" };
  }
  const result = await dbQuery(
    `update collector_supply_sessions
     set status = 'finished',
         finished_at = now(),
         total_seconds = greatest(total_seconds, extract(epoch from (now() - started_at))::int),
         updated_at = now()
     where id = $1
     returning *`,
    [openSession.id]
  );
  return { session: result.rows?.[0] || openSession, error: null, alreadyFinished: false };
}

async function getCollectorSupplyContext(supplyId, auth = null) {
  const employeeLocation = auth?.id ? await getConfiguredEmployeeLocation(auth.id) : null;
  if (employeeLocation?.error) {
    return employeeLocation;
  }
  const settings = await getSupplySettings(supplyId);
  if (!settings || settings.access_mode !== "selected_split") {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  const supplyLocation = normalizeLocationCode(settings.location_code);
  if (!supplyLocation) {
    return { errorStatus: 409, error: "Поставка не настроена по городу. Обратитесь к администратору." };
  }
  if (employeeLocation?.locationCode && supplyLocation !== employeeLocation.locationCode) {
    return { errorStatus: 404, error: "Поставка недоступна для вашего города" };
  }
  if (!isCollectorAllowedForSupply(settings)) {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  if (!(await isSupplyActiveForSettings(supplyId, settings))) {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  return { settings };
}

async function getCollectorSupplyStats(userId, rangeStart, rangeEnd) {
  const durationSql = `case
    when s.status = 'started' and s.finished_at is null then greatest(s.total_seconds, extract(epoch from (now() - s.started_at))::int)
    else s.total_seconds
  end`;
  const baseWhere = "s.user_id = $1 and s.started_at is not null";
  const periodParams = [userId];
  let periodWhere = baseWhere;
  let dailyWhere = "s.user_id = $1 and s.started_at is not null";
  if (rangeStart && rangeEnd) {
    periodParams.push(rangeStart, rangeEnd);
    periodWhere += " and s.started_at >= $2 and s.started_at < $3";
    dailyWhere += " and s.started_at >= $5 and s.started_at < $6";
  }

  const todaySummaryPromise = dbQuery(
    `select count(*)::int as sessions,
            count(distinct s.supply_id)::int as supplies,
            coalesce(sum(s.order_count), 0)::bigint as orders,
            coalesce(sum(s.item_count), 0)::bigint as items,
            coalesce(sum(${durationSql}), 0)::bigint as total_seconds
     from collector_supply_sessions s
     where s.user_id = $1
       and s.started_at is not null
       and timezone($2, s.started_at)::date = timezone($2, now())::date`,
    [userId, MOSCOW_TZ]
  );

  const allTimeSummaryPromise = dbQuery(
    `select count(*)::int as sessions,
            count(distinct s.supply_id)::int as supplies,
            coalesce(sum(s.order_count), 0)::bigint as orders,
            coalesce(sum(s.item_count), 0)::bigint as items,
            coalesce(sum(${durationSql}), 0)::bigint as total_seconds
     from collector_supply_sessions s
     where s.user_id = $1 and s.started_at is not null`,
    [userId]
  );

  const periodSummaryPromise = dbQuery(
    `select count(*)::int as sessions,
            count(distinct s.supply_id)::int as supplies,
            coalesce(sum(s.order_count), 0)::bigint as orders,
            coalesce(sum(s.item_count), 0)::bigint as items,
            coalesce(sum(${durationSql}), 0)::bigint as total_seconds
     from collector_supply_sessions s
     where ${periodWhere}`,
    periodParams
  );

  const dailyPromise = dbQuery(
    `with days as (
       select generate_series(
                timezone($2, coalesce($3::timestamptz, now() - interval '6 day'))::date,
                timezone($2, coalesce($4::timestamptz, now()))::date,
                interval '1 day'
              )::date as day
     ),
     day_sessions as (
       select timezone($2, s.started_at)::date as day,
              count(*)::int as sessions,
              coalesce(sum(s.order_count), 0)::bigint as orders,
              coalesce(sum(s.item_count), 0)::bigint as items,
              coalesce(sum(${durationSql}), 0)::bigint as total_seconds
       from collector_supply_sessions s
       where ${dailyWhere}
       group by 1
     )
     select to_char(days.day, 'YYYY-MM-DD') as day,
            coalesce(day_sessions.sessions, 0) as sessions,
            coalesce(day_sessions.orders, 0)::bigint as orders,
            coalesce(day_sessions.items, 0)::bigint as items,
            coalesce(day_sessions.total_seconds, 0)::bigint as total_seconds
     from days
     left join day_sessions on day_sessions.day = days.day
     order by days.day`,
    [
      userId,
      MOSCOW_TZ,
      rangeStart || null,
      rangeEnd || null,
      ...(rangeStart && rangeEnd ? [rangeStart, rangeEnd] : []),
    ]
  );

  const sessionsPromise = dbQuery(
    `select s.id,
            s.supply_id,
            s.supply_name,
            s.store_id,
            s.store_name,
            s.status,
            s.started_at,
            s.finished_at,
            ${durationSql}::int as total_seconds,
            s.order_count,
            s.item_count
     from collector_supply_sessions s
     where ${periodWhere}
     order by s.started_at desc, s.id desc
     limit 100`,
    periodParams
  );

  const [todaySummary, allTimeSummary, periodSummary, dailyResult, sessionsResult] = await Promise.all([
    todaySummaryPromise,
    allTimeSummaryPromise,
    periodSummaryPromise,
    dailyPromise,
    sessionsPromise,
  ]);

  const toSummary = (row) => {
    const orders = Number(row?.orders || 0);
    const items = Number(row?.items || 0);
    const totalSeconds = Number(row?.total_seconds || 0);
    return {
      sessions: Number(row?.sessions || 0),
      supplies: Number(row?.supplies || 0),
      orders,
      items,
      totalSeconds,
      avgSecondsPerOrder: orders > 0 ? totalSeconds / orders : 0,
      avgSecondsPerItem: items > 0 ? totalSeconds / items : 0,
    };
  };

  return {
    today: toSummary(todaySummary.rows?.[0]),
    allTime: toSummary(allTimeSummary.rows?.[0]),
    period: toSummary(periodSummary.rows?.[0]),
    daily: (dailyResult.rows || []).map((row) => ({
      day: row.day,
      sessions: Number(row.sessions || 0),
      orders: Number(row.orders || 0),
      items: Number(row.items || 0),
      totalSeconds: Number(row.total_seconds || 0),
    })),
    sessions: (sessionsResult.rows || []).map((row) => ({
      id: Number(row.id),
      supplyId: row.supply_id,
      supplyName: row.supply_name || null,
      storeId: row.store_id || null,
      storeName: row.store_name || null,
      assignedByName: "Система",
      assignmentMode: "collector",
      status: row.status || "finished",
      assignedAt: row.started_at || null,
      startedAt: row.started_at || null,
      finishedAt: row.finished_at || null,
      totalSeconds: Number(row.total_seconds || 0),
      assignedOrderCount: Number(row.order_count || 0),
      collectedOrderCount: Number(row.order_count || 0),
      itemCount: Number(row.item_count || 0),
    })),
  };
}

async function getEmployeeSupplyContext(supplyId, auth, options = {}) {
  const requireStartedSession = options.requireStartedSession === true;
  const employeeLocation = await getConfiguredEmployeeLocation(auth.id);
  if (employeeLocation.error) {
    return employeeLocation;
  }
  const settings = await getSupplySettings(supplyId);
  if (!settings || settings.access_mode !== "selected_split") {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  const supplyLocation = normalizeLocationCode(settings.location_code);
  if (!supplyLocation) {
    return { errorStatus: 409, error: "Поставка не настроена по городу. Обратитесь к администратору." };
  }
  if (supplyLocation !== employeeLocation.locationCode) {
    return { errorStatus: 404, error: "Поставка недоступна для вашего города" };
  }
  const store = await getSupplyStore(supplyId, settings.supply_name || null);
  const [accessUserIds, countCheck] = await Promise.all([
    getSupplyAccessUserIds(supplyId),
    dbQuery("select count(*) as count from supply_orders where supply_id = $1", [supplyId]),
  ]);
  if (!accessUserIds.includes(Number(auth.id))) {
    return { errorStatus: 403, error: "FORBIDDEN" };
  }
  if (Number(countCheck.rows?.[0]?.count || 0) === 0) {
    await ensureSupplySnapshot(supplyId, settings.supply_name || null);
  }
  const activeSession = await getEmployeeActiveSession(auth.id);
  let session = await getEmployeeOpenSession(supplyId, auth.id);
  let assignmentPart = await getSupplyAssignmentPartForSession(session);
  const hasAssignmentParts = !session ? await supplyHasAssignmentParts(supplyId) : Number(session.assignment_part_id || 0) > 0;
  let counts = await getEmployeeSupplyCounts(
    supplyId,
    auth.id,
    session?.assignment_part_id != null ? Number(session.assignment_part_id) : null
  );
  if (!session && counts.total > 0 && !hasAssignmentParts) {
    session = await ensureEmployeeAssignmentSession(supplyId, auth.id, counts.remaining);
    assignmentPart = await getSupplyAssignmentPartForSession(session);
    counts = await getEmployeeSupplyCounts(
      supplyId,
      auth.id,
      session?.assignment_part_id != null ? Number(session.assignment_part_id) : null
    );
  }
  if (!session && hasAssignmentParts) {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  if (counts.total < 1 && !session) {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  session = await ensureEmployeeSessionPickingStatus(settings, session);
  assignmentPart = assignmentPart || (await getSupplyAssignmentPartForSession(session));
  const workflow = buildEmployeeWorkflowState(settings, session, activeSession, assignmentPart);
  if (requireStartedSession && !workflow.canWork) {
    if (workflow.isWaitingAdmin) {
      return { errorStatus: 409, error: "Администратор ещё не запустил сборку товара" };
    }
    if (workflow.isWaitingForPicker) {
      return { errorStatus: 409, error: "Сначала дождитесь, пока сборщик товара закончит и положит товары на стол" };
    }
    if (workflow.isWaitingForTable) {
      return { errorStatus: 409, error: `Сборщик товара ещё не подтвердил передачу на стол ${workflow.tableNo || "—"}` };
    }
    if (workflow.productPickingRequired && workflow.pickingStatus !== PICKING_STATUS_FINISHED) {
      return { errorStatus: 409, error: "Сначала завершите сборку товара" };
    }
    return { errorStatus: 409, error: "Сначала нажмите «Старт»" };
  }
  return { settings, store, session, counts, activeSession, workflow, assignmentPart };
}

async function getEmployeeSupplyClosingContext(supplyId, auth, options = {}) {
  const employeeLocation = await getConfiguredEmployeeLocation(auth.id);
  if (employeeLocation.error) return employeeLocation;

  const settings = await getSupplySettings(supplyId);
  if (!settings || settings.access_mode !== "selected_split") {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  if (settings.allow_employee_close !== true) {
    return { errorStatus: 403, error: "Самостоятельное закрытие этой поставки не разрешено" };
  }

  const supplyLocation = normalizeLocationCode(settings.location_code);
  if (!supplyLocation || supplyLocation !== employeeLocation.locationCode) {
    return { errorStatus: 404, error: "Поставка недоступна для вашего города" };
  }

  const [allowed, countsResult] = await Promise.all([
    dbQuery(
      "select 1 from supply_access_users where supply_id = $1 and user_id = $2 limit 1",
      [supplyId, auth.id]
    ),
    dbQuery(
      `select count(*)::int as total,
              count(*) filter (where collected_at is not null)::int as collected
       from supply_orders
       where supply_id = $1`,
      [supplyId]
    ),
  ]);
  if (!allowed.rows?.length) {
    return { errorStatus: 403, error: "FORBIDDEN" };
  }

  const total = Number(countsResult.rows?.[0]?.total || 0);
  const collected = Number(countsResult.rows?.[0]?.collected || 0);
  if (total < 1 || collected < total) {
    return {
      errorStatus: 409,
      error: `Сначала завершите все заказы поставки (${collected}/${total})`,
    };
  }
  if (settings.employee_close_completed_at && options.allowCompleted !== true) {
    return { errorStatus: 409, error: "Поставка уже завершена сотрудником" };
  }

  const store = await getSupplyStore(supplyId, settings.supply_name || null);
  return { settings, store, total, collected };
}

function toEmployeeSupplyClosingPayload(payload, context) {
  return {
    ...(payload || {}),
    closing: {
      allowed: true,
      ready: true,
      total: Number(context?.total || 0),
      collected: Number(context?.collected || 0),
      completedAt: context?.settings?.employee_close_completed_at || null,
      completedBy:
        context?.settings?.employee_close_completed_by != null
          ? Number(context.settings.employee_close_completed_by)
          : null,
    },
  };
}

async function getEmployeeSupplyLabelScanContext(supplyId, auth) {
  const settings = await getSupplySettings(supplyId);
  if (!settings || settings.access_mode !== "selected_split") {
    return { errorStatus: 404, error: "Поставка недоступна" };
  }
  const allowed = await dbQuery(
    "select 1 from supply_access_users where supply_id = $1 and user_id = $2 limit 1",
    [supplyId, auth.id]
  );
  if (!allowed.rows?.length) {
    return { errorStatus: 403, error: "FORBIDDEN" };
  }
  let session = await getEmployeeStartedSessionForSupply(supplyId, auth.id);
  if (!session) {
    session = await getEmployeeOpenSession(supplyId, auth.id);
  }
  session = await ensureEmployeeSessionPickingStatus(settings, session);
  const assignmentPart = await getSupplyAssignmentPartForSession(session);
  const workflow = buildEmployeeWorkflowState(settings, session, session || null, assignmentPart);
  if (!workflow.canWork) {
    if (workflow.isWaitingAdmin) {
      return { errorStatus: 409, error: "Администратор ещё не запустил сборку товара" };
    }
    if (workflow.isWaitingForPicker) {
      return { errorStatus: 409, error: "Сначала дождитесь передачи товара на стол" };
    }
    if (workflow.isWaitingForTable) {
      return { errorStatus: 409, error: "Сборщик товара ещё не подтвердил передачу на стол" };
    }
    if (workflow.productPickingRequired && workflow.pickingStatus !== PICKING_STATUS_FINISHED) {
      return { errorStatus: 409, error: "Сначала завершите сборку товара" };
    }
    return { errorStatus: 409, error: "Сначала нажмите «Старт»" };
  }
  return { settings, session, workflow, assignmentPart };
}

async function listOpenScanIssues() {
  const result = await dbQuery(
    `select i.*,
            u.surname as employee_surname,
            u.name as employee_name,
            s.supply_name,
            greatest(coalesce(pi.required_qty, 0) - coalesce(pi.picked_qty, 0), 0)::int as live_remaining_quantity
     from employee_scan_issues i
     join users u on u.id = i.employee_id
     left join supply_settings s on s.supply_id = i.supply_id
     left join employee_supply_pick_items pi on pi.id = i.pick_item_id
     where i.status = 'open'
     order by i.created_at desc, i.id desc`,
    []
  );
  return (result.rows || []).map(toScanIssuePayload);
}

async function getOpenScanIssueById(issueId) {
  const result = await dbQuery(
    `select *
     from employee_scan_issues
     where id = $1 and status = 'open'
     limit 1`,
    [issueId]
  );
  return result.rows?.[0] || null;
}

async function resolveIssueWarehouseProductSnapshot({ article = null, barcodes = [] } = {}) {
  const candidates = uniqueTextList(barcodes).map((value) => normalizeBarcode(value)).filter(Boolean);
  for (const candidate of candidates) {
    try {
      const status = await getMsProductStatusByBarcode(candidate, { force: true });
      if (status?.found && !status?.multiple && Array.isArray(status.matches) && status.matches.length === 1) {
        const match = status.matches[0];
        return {
          warehouseProductId: Number(match.id) || null,
          warehouseArticle: match.article || null,
          warehouseProductName: match.name || null,
          mappingBarcode: candidate,
        };
      }
    } catch {}
  }
  if (article) {
    try {
      const status = await getMsProductStatusByArticle(article, { force: true });
      if (status?.found && !status?.multiple && Array.isArray(status.matches) && status.matches.length === 1) {
        const match = status.matches[0];
        return {
          warehouseProductId: Number(match.id) || null,
          warehouseArticle: match.article || null,
          warehouseProductName: match.name || null,
          mappingBarcode: null,
        };
      }
    } catch {}
  }
  return {
    warehouseProductId: null,
    warehouseArticle: null,
    warehouseProductName: null,
    mappingBarcode: null,
  };
}

async function createOrUpdateEmployeeScanIssue({
  supplyId,
  orderId = null,
  pickItemId = null,
  pickItemKey = null,
  employeeId,
  issueStage = SCAN_ISSUE_STAGE_ORDER,
  errorMessage = null,
  contextNote = null,
  orderBarcode,
  scannedBarcode,
  article,
  productName,
  nmId,
  originalScannedBarcode = null,
  confirmationScannedBarcode = null,
  marketplaceArticle = null,
  warehouseProductId = null,
  warehouseArticle = null,
  warehouseProductName = null,
  mappingBarcode = null,
  imageUrl = null,
  remainingQuantity = null,
}) {
  const normalizedStage = normalizeScanIssueStage(issueStage);
  const existing = await dbQuery(
    `select id
     from employee_scan_issues
     where supply_id = $1
       and wb_order_id is not distinct from $2
       and pick_item_id is not distinct from $3
       and employee_id = $4
       and issue_stage = $5
       and status = 'open'
     order by created_at desc, id desc
     limit 1`,
    [supplyId, orderId, pickItemId, employeeId, normalizedStage]
  );
  if (existing.rows?.[0]?.id) {
    const result = await dbQuery(
      `update employee_scan_issues
       set pick_item_key = $2,
           error_message = $3,
           context_note = $4,
           order_barcode = $5,
           scanned_barcode = $6,
           article = $7,
           product_name = $8,
           nm_id = $9,
           original_scanned_barcode = $10,
           confirmation_scanned_barcode = $11,
           marketplace_article = $12,
           warehouse_product_id = $13,
           warehouse_article = $14,
           warehouse_product_name = $15,
           mapping_barcode = $16,
           image_url = $17,
           remaining_quantity = $18,
           updated_at = now()
       where id = $1
       returning *`,
      [
        existing.rows[0].id,
        pickItemKey || null,
        errorMessage || null,
        contextNote || null,
        orderBarcode || null,
        scannedBarcode || null,
        article || null,
        productName || null,
        nmId || null,
        originalScannedBarcode || scannedBarcode || null,
        confirmationScannedBarcode || scannedBarcode || null,
        marketplaceArticle || article || null,
        warehouseProductId || null,
        warehouseArticle || null,
        warehouseProductName || null,
        mappingBarcode || null,
        imageUrl || null,
        remainingQuantity == null ? null : Math.max(0, Number(remainingQuantity || 0)),
      ]
    );
    return result.rows?.[0] || null;
  }
  const result = await dbQuery(
    `insert into employee_scan_issues
      (supply_id, wb_order_id, pick_item_id, pick_item_key, employee_id, issue_stage, error_message, context_note, order_barcode, scanned_barcode, article, product_name, nm_id,
       original_scanned_barcode, confirmation_scanned_barcode, marketplace_article, warehouse_product_id, warehouse_article, warehouse_product_name, mapping_barcode, image_url, remaining_quantity, created_at, updated_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, now(), now())
     returning *`,
    [
      supplyId,
      orderId,
      pickItemId,
      pickItemKey || null,
      employeeId,
      normalizedStage,
      errorMessage || null,
      contextNote || null,
      orderBarcode || null,
      scannedBarcode || null,
      article || null,
      productName || null,
      nmId || null,
      originalScannedBarcode || scannedBarcode || null,
      confirmationScannedBarcode || scannedBarcode || null,
      marketplaceArticle || article || null,
      warehouseProductId || null,
      warehouseArticle || null,
      warehouseProductName || null,
      mappingBarcode || null,
      imageUrl || null,
      remainingQuantity == null ? null : Math.max(0, Number(remainingQuantity || 0)),
    ]
  );
  return result.rows?.[0] || null;
}

async function resolveScanIssue(issueId, adminId, status, options = {}) {
  const result = await dbQuery(
    `update employee_scan_issues
     set status = $2,
         resolved_at = now(),
         resolved_by = $3,
         fixed_at = case when $2 = 'fixed' then now() else fixed_at end,
         fixed_by = case when $2 = 'fixed' then $3 else fixed_by end,
         resolution_type = $4,
         resolution_note = $5,
         resolved_quantity = $6,
         updated_at = now()
     where id = $1 and status = 'open'
     returning *`,
    [
      issueId,
      status,
      adminId,
      options?.resolutionType || (status === "fixed" ? "fixed" : "resolved"),
      options?.resolutionNote || null,
      options?.resolvedQuantity == null ? null : Math.max(0, Number(options.resolvedQuantity || 0)),
    ]
  );
  return result.rows?.[0] || null;
}

async function completeWbPickIssueItem(issue, adminId, confirmedQuantity) {
  const pickItemId = Number(issue?.pick_item_id || 0);
  if (!pickItemId) throw new Error("Для этой ошибки не выбрана строка сборки товара");
  const itemResult = await dbQuery("select * from employee_supply_pick_items where id = $1 limit 1", [pickItemId]);
  const item = itemResult.rows?.[0] || null;
  if (!item) throw new Error("Строка сборки товара не найдена");
  const remainingQuantity = Math.max(0, Number(item.required_qty || 0) - Number(item.picked_qty || 0));
  if (remainingQuantity > 1 && Number(confirmedQuantity) !== remainingQuantity) {
    const error = new Error(`Подтвердите пропуск ${remainingQuantity} шт. этого товара`);
    error.fixConfirmationRequired = true;
    error.remainingQuantity = remainingQuantity;
    throw error;
  }
  if (!remainingQuantity) return { item, resolvedQuantity: 0 };

  const sessionResult = await dbQuery("select * from employee_supply_sessions where id = $1 limit 1", [item.session_id]);
  const session = sessionResult.rows?.[0] || null;
  const settings = await getSupplySettings(item.supply_id);
  if (!session || !settings) throw new Error("Сессия сборки товара не найдена");
  const stockMethodEnabled = isStockMethodEnabled(settings);
  const externalEmployeeId = stockMethodEnabled ? await getUserExternalEmployeeId(issue.employee_id) : null;
  if (stockMethodEnabled && !externalEmployeeId) {
    throw new Error("Сотрудник не привязан к складской системе");
  }

  const updatedItem = await collectEmployeePickItemWithoutScan(pickItemId, { requireBypass: false });
  if (!updatedItem) return { item, resolvedQuantity: 0 };
  if (stockMethodEnabled) {
    try {
      const movementHints = await buildWbPickItemMovementHints(item, { ...settings, force: true });
      const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0];
      const movementResult = await moveProductFromShelfToEmployee({
        externalEmployeeId,
        warehouseId: getSkladWarehouseOptionsFromSettings(settings).warehouseId,
        barcodes: [item?.barcode, issue?.mapping_barcode].filter(Boolean),
        resolvedProductIds: movementHints?.productIds || [],
        quantity: remainingQuantity,
        skipUnavailable: isAllowStockShortage(settings),
        preferredShelfId: movementHints?.preferredShelfId || firstCell?.id || null,
        source: "wb_pick_admin_fix",
        notes: `WB: admin fixed product picking. Supply ${item.supply_id}, item ${item.article || "-"}.`,
        negativeNotes: `WB: admin fixed product picking with stock shortage. Supply ${item.supply_id}, item ${item.article || "-"}.`,
      });
      await recordStockShortageSkip({
        supplyId: item.supply_id,
        stage: "pick_admin_fix",
        article: item.article,
        barcode: item.barcode,
        requestedQuantity: remainingQuantity,
        movementResult,
        userId: adminId,
      });
    } catch (error) {
      await dbQuery("update employee_supply_pick_items set picked_qty = $2, updated_at = now() where id = $1", [
        pickItemId,
        Number(item.picked_qty || 0),
      ]);
      throw error;
    }
  }
  syncEmployeePickScanIndexItem(session.id, updatedItem);
  await refreshEmployeePickingSessionSummary(session.id);
  return { item: updatedItem, resolvedQuantity: remainingQuantity };
}

function toEmployeeStatsSummary(row) {
  const totalSeconds = Number(row?.total_seconds || 0);
  const packingSeconds = Number(row?.packing_seconds || 0);
  const pickingSeconds = Number(row?.picking_seconds || 0);
  const orders = Number(row?.orders || 0);
  const pickedItems = Number(row?.picked_items || 0);
  return {
    supplies: Number(row?.supplies || 0),
    sessions: Number(row?.sessions || 0),
    orders,
    pickedItems,
    totalSeconds,
    packingSeconds,
    pickingSeconds,
    avgSecondsPerOrder: orders > 0 ? Math.round(packingSeconds / orders) : 0,
    avgPickSecondsPerItem: pickedItems > 0 ? Math.round(pickingSeconds / pickedItems) : 0,
  };
}

async function getEmployeeStats(userId, rangeStart, rangeEnd) {
  const packingDurationSql = `case
    when s.status = 'started' and s.finished_at is null and s.started_at is not null
      then extract(epoch from (now() - s.started_at))::int
    else s.total_seconds
  end`;
  const pickingDurationSql = `case
    when s.picking_status = 'started' and s.picking_finished_at is null and s.picking_started_at is not null
      then extract(epoch from (now() - s.picking_started_at))::int
    else s.picking_total_seconds
  end`;
  const hasRange = Boolean(rangeStart && rangeEnd);
  const periodParams = hasRange ? [userId, rangeStart, rangeEnd] : [userId];
  const packingPeriodSql = hasRange ? "s.started_at >= $2 and s.started_at < $3" : "s.started_at is not null";
  const pickingPeriodSql = hasRange
    ? "s.picking_started_at >= $2 and s.picking_started_at < $3"
    : "s.picking_started_at is not null";
  const orderPeriodWhere = hasRange
    ? "o.collected_by = $1 and o.collected_at is not null and o.collected_at >= $2 and o.collected_at < $3"
    : "o.collected_by = $1 and o.collected_at is not null";

  const todaySummaryPromise = dbQuery(
    `with today_orders as (
       select count(*) as orders
       from supply_orders o
       where o.collected_by = $1
         and o.collected_at is not null
         and timezone($2, o.collected_at)::date = timezone($2, now())::date
     ),
     today_sessions as (
       select count(*) filter (
                where (s.started_at is not null and timezone($2, s.started_at)::date = timezone($2, now())::date)
                   or (s.picking_started_at is not null and timezone($2, s.picking_started_at)::date = timezone($2, now())::date)
              )::int as sessions,
              count(distinct s.supply_id) filter (
                where (s.started_at is not null and timezone($2, s.started_at)::date = timezone($2, now())::date)
                   or (s.picking_started_at is not null and timezone($2, s.picking_started_at)::date = timezone($2, now())::date)
              )::int as supplies,
              coalesce(sum(
                case
                  when s.started_at is not null and timezone($2, s.started_at)::date = timezone($2, now())::date
                    then ${packingDurationSql}
                  else 0
                end
              ), 0)::bigint as packing_seconds,
              coalesce(sum(
                case
                  when s.picking_started_at is not null and timezone($2, s.picking_started_at)::date = timezone($2, now())::date
                    then ${pickingDurationSql}
                  else 0
                end
              ), 0)::bigint as picking_seconds,
              coalesce(sum(
                case
                  when s.picking_started_at is not null and timezone($2, s.picking_started_at)::date = timezone($2, now())::date
                    then s.picking_collected_qty
                  else 0
                end
              ), 0)::bigint as picked_items
       from employee_supply_sessions s
       where s.user_id = $1
     )
     select today_sessions.supplies,
            today_sessions.sessions,
            today_orders.orders,
            today_sessions.picked_items,
            today_sessions.picking_seconds,
            today_sessions.packing_seconds,
            (today_sessions.picking_seconds + today_sessions.packing_seconds)::bigint as total_seconds
     from today_sessions, today_orders`,
    [userId, MOSCOW_TZ]
  );

  const allTimeSummaryPromise = dbQuery(
    `select count(distinct s.supply_id) filter (
              where s.started_at is not null or s.picking_started_at is not null
            )::int as supplies,
            count(*) filter (
              where s.started_at is not null or s.picking_started_at is not null
            )::int as sessions,
            coalesce(sum(s.collected_order_count), 0)::bigint as orders,
            coalesce(sum(s.picking_collected_qty), 0)::bigint as picked_items,
            coalesce(sum(
              case when s.started_at is not null then ${packingDurationSql} else 0 end
            ), 0)::bigint as packing_seconds,
            coalesce(sum(
              case when s.picking_started_at is not null then ${pickingDurationSql} else 0 end
            ), 0)::bigint as picking_seconds,
            (
              coalesce(sum(case when s.started_at is not null then ${packingDurationSql} else 0 end), 0)
              + coalesce(sum(case when s.picking_started_at is not null then ${pickingDurationSql} else 0 end), 0)
            )::bigint as total_seconds
     from employee_supply_sessions s
     where s.user_id = $1`,
    [userId]
  );

  const periodSummaryPromise = dbQuery(
    `with period_orders as (
       select count(*) as orders
       from supply_orders o
       where ${orderPeriodWhere}
     ),
     period_sessions as (
       select count(*) filter (
                where (s.started_at is not null and ${packingPeriodSql})
                   or (s.picking_started_at is not null and ${pickingPeriodSql})
              )::int as sessions,
              count(distinct s.supply_id) filter (
                where (s.started_at is not null and ${packingPeriodSql})
                   or (s.picking_started_at is not null and ${pickingPeriodSql})
              )::int as supplies,
              coalesce(sum(
                case when s.started_at is not null and ${packingPeriodSql} then ${packingDurationSql} else 0 end
              ), 0)::bigint as packing_seconds,
              coalesce(sum(
                case when s.picking_started_at is not null and ${pickingPeriodSql} then ${pickingDurationSql} else 0 end
              ), 0)::bigint as picking_seconds,
              coalesce(sum(
                case when s.picking_started_at is not null and ${pickingPeriodSql} then s.picking_collected_qty else 0 end
              ), 0)::bigint as picked_items
       from employee_supply_sessions s
       where s.user_id = $1
     )
     select period_sessions.supplies,
            period_sessions.sessions,
            period_orders.orders,
            period_sessions.picked_items,
            period_sessions.picking_seconds,
            period_sessions.packing_seconds,
            (period_sessions.picking_seconds + period_sessions.packing_seconds)::bigint as total_seconds
     from period_sessions, period_orders`,
    periodParams
  );

  const dailyPromise = dbQuery(
    `with days as (
       select generate_series(
                timezone($2, coalesce($3::timestamptz, now() - interval '6 day'))::date,
                timezone($2, coalesce(($4::timestamptz - interval '1 second'), now()))::date,
                interval '1 day'
              )::date as day
     ),
     day_orders as (
       select timezone($2, o.collected_at)::date as day,
              count(*)::int as orders
       from supply_orders o
       where o.collected_by = $1
         and o.collected_at is not null
         ${hasRange ? "and o.collected_at >= $3 and o.collected_at < $4" : ""}
       group by 1
     ),
     day_packing as (
       select timezone($2, s.started_at)::date as day,
              coalesce(sum(${packingDurationSql}), 0)::bigint as packing_seconds
       from employee_supply_sessions s
       where s.user_id = $1
         and s.started_at is not null
         ${hasRange ? "and s.started_at >= $3 and s.started_at < $4" : ""}
       group by 1
     ),
     day_picking as (
       select timezone($2, s.picking_started_at)::date as day,
              coalesce(sum(${pickingDurationSql}), 0)::bigint as picking_seconds
       from employee_supply_sessions s
       where s.user_id = $1
         and s.picking_started_at is not null
         ${hasRange ? "and s.picking_started_at >= $3 and s.picking_started_at < $4" : ""}
       group by 1
     )
     select to_char(days.day, 'YYYY-MM-DD') as day,
            coalesce(day_orders.orders, 0)::int as orders,
            coalesce(day_picking.picking_seconds, 0)::bigint as picking_seconds,
            coalesce(day_packing.packing_seconds, 0)::bigint as packing_seconds,
            (coalesce(day_picking.picking_seconds, 0) + coalesce(day_packing.packing_seconds, 0))::bigint as total_seconds
     from days
     left join day_orders on day_orders.day = days.day
     left join day_picking on day_picking.day = days.day
     left join day_packing on day_packing.day = days.day
     order by days.day`,
    [userId, MOSCOW_TZ, rangeStart || null, rangeEnd || null]
  );

  const sessionsPromise = dbQuery(
    `select s.id, s.supply_id, ss.supply_name, ss.store_name,
            s.assignment_mode, s.status, s.assigned_at, s.started_at, s.finished_at,
            s.assigned_order_count, s.collected_order_count,
            s.picking_status, s.picking_started_at, s.picking_finished_at,
            s.picking_required_qty, s.picking_collected_qty,
            ${packingDurationSql}::bigint as packing_total_seconds,
            ${pickingDurationSql}::bigint as picking_total_seconds,
            s.first_collected_at, s.last_collected_at,
            a.surname as assigned_by_surname, a.name as assigned_by_name
     from employee_supply_sessions s
     left join supply_settings ss on ss.supply_id = s.supply_id
     left join users a on a.id = s.assigned_by
     where s.user_id = $1
       and (
         s.started_at is not null
         or s.assigned_order_count > 0
         or s.picking_started_at is not null
         or s.picking_required_qty > 0
       )
     order by s.assigned_at desc, s.id desc
     limit 200`,
    [userId]
  );

  const [todaySummary, allTimeSummary, periodSummary, dailySummary, sessions] = await Promise.all([
    todaySummaryPromise,
    allTimeSummaryPromise,
    periodSummaryPromise,
    dailyPromise,
    sessionsPromise,
  ]);

  return {
    today: toEmployeeStatsSummary(todaySummary.rows?.[0] || null),
    allTime: toEmployeeStatsSummary(allTimeSummary.rows?.[0] || null),
    period: toEmployeeStatsSummary(periodSummary.rows?.[0] || null),
    daily:
      (dailySummary.rows || []).map((row) => ({
        day: row.day,
        orders: Number(row.orders || 0),
        totalSeconds: Number(row.total_seconds || 0),
        pickingSeconds: Number(row.picking_seconds || 0),
        packingSeconds: Number(row.packing_seconds || 0),
      })) || [],
    sessions:
      (sessions.rows || []).map((row) => ({
        id: Number(row.id),
        supplyId: row.supply_id,
        supplyName: row.supply_name || null,
        storeName: row.store_name || null,
        assignmentMode: row.assignment_mode,
        status: row.status,
        assignedAt: row.assigned_at ?? null,
        startedAt: row.started_at ?? null,
        finishedAt: row.finished_at ?? null,
        assignedOrderCount: Number(row.assigned_order_count || 0),
        collectedOrderCount: Number(row.collected_order_count || 0),
        pickingStatus: row.picking_status || null,
        pickingStartedAt: row.picking_started_at ?? null,
        pickingFinishedAt: row.picking_finished_at ?? null,
        pickingRequiredQty: Number(row.picking_required_qty || 0),
        pickingCollectedQty: Number(row.picking_collected_qty || 0),
        pickingTotalSeconds: Number(row.picking_total_seconds || 0),
        packingTotalSeconds: Number(row.packing_total_seconds || 0),
        totalSeconds: Number(row.picking_total_seconds || 0) + Number(row.packing_total_seconds || 0),
        firstCollectedAt: row.first_collected_at ?? null,
        lastCollectedAt: row.last_collected_at ?? null,
        assignedByName:
          row.assigned_by_surname || row.assigned_by_name
            ? `${row.assigned_by_surname || ""} ${row.assigned_by_name || ""}`.trim()
            : null,
      })) || [],
  };
}

async function ensureSupplySettings(supplyId, supplyName = null, store = null, options = {}) {
  if (!supplyId) return null;
  const storeId = store?.id || null;
  const storeName = store?.name || null;
  const locationCode = normalizeLocationCode(options?.locationCode);
  const skladWarehouseId = Number(options?.skladWarehouseId || 0) || null;
  const skladWarehouseName = normalizeText(options?.skladWarehouseName) || null;
  const hasAllowStockShortage = typeof options?.allowStockShortage === "boolean";
  const allowStockShortage = options?.allowStockShortage === true;
  const result = await dbQuery(
    `insert into supply_settings (
       supply_id, supply_name, store_id, store_name,
       location_code, sklad_warehouse_id, sklad_warehouse_name,
       stock_method_enabled, allow_stock_shortage, employee_workflow_mode, employee_workflow_stage,
       updated_at
      )
      values ($1, $2, $3, $4, $5, $6, $7, true, $8, $9, $10, now())
     on conflict (supply_id)
     do update set supply_name = coalesce(excluded.supply_name, supply_settings.supply_name),
                   store_id = coalesce(excluded.store_id, supply_settings.store_id),
                   store_name = coalesce(excluded.store_name, supply_settings.store_name),
                   location_code = coalesce(excluded.location_code, supply_settings.location_code),
                    sklad_warehouse_id = coalesce(excluded.sklad_warehouse_id, supply_settings.sklad_warehouse_id),
                    sklad_warehouse_name = coalesce(excluded.sklad_warehouse_name, supply_settings.sklad_warehouse_name),
                    allow_stock_shortage = case when $11 then excluded.allow_stock_shortage else supply_settings.allow_stock_shortage end,
                    updated_at = now()
     returning *`,
    [
      supplyId,
      supplyName,
      storeId,
      storeName,
      locationCode,
      skladWarehouseId,
      skladWarehouseName,
      allowStockShortage,
      EMPLOYEE_WORKFLOW_MODE_PICKING,
      EMPLOYEE_WORKFLOW_ORDERS_ONLY,
      hasAllowStockShortage,
    ]
  );
  return result.rows?.[0] || null;
}

// Авто-присвоение города/склада WB-поставке по WB-складу («Мой склад»/«Ижевск склад») её заказов.
// Для поставок, созданных напрямую на WB (без города): «Мой склад»→Москва, «Ижевск склад»/«Ижевск»→Ижевск
// (по привязкам site_location_warehouse_settings). Не трогаем активно собираемые поставки.
async function autoAssignWbSupplyLocation(supplyId) {
  try {
    const act = await dbQuery("select 1 from employee_supply_sessions where supply_id = $1 and status = 'started' and finished_at is null limit 1", [supplyId]);
    if (act.rows?.length) return null;
    const whRes = await dbQuery("select distinct warehouse_id from supply_orders where supply_id = $1 and warehouse_id is not null limit 3", [supplyId]);
    const whs = (whRes.rows || []).map((r) => String(r.warehouse_id)).filter(Boolean);
    if (whs.length !== 1) return null; // 0 или несколько WB-складов — не мапим
    const bind = await dbQuery(
      `select location_code, sklad_warehouse_id, sklad_warehouse_name
       from site_location_warehouse_settings
       where active = true and marketplace = 'wb' and coalesce(marketplace_warehouse_id, '') = $1
       limit 1`,
      [whs[0]]
    );
    const b = bind.rows?.[0];
    if (!b?.location_code) return null;
    await dbQuery(
      `update supply_settings set location_code = $2, sklad_warehouse_id = $3, sklad_warehouse_name = $4, updated_at = now()
       where supply_id = $1 and coalesce(location_code, '') = ''`,
      [supplyId, b.location_code, b.sklad_warehouse_id ?? null, b.sklad_warehouse_name ?? null]
    );
    return { location_code: b.location_code, sklad_warehouse_id: b.sklad_warehouse_id, sklad_warehouse_name: b.sklad_warehouse_name };
  } catch { return null; }
}

async function getSupplySettings(supplyId) {
  const result = await dbQuery(
    `select supply_id, supply_name, access_mode, labels_status, labels_format,
            labels_s3_prefix, labels_total, labels_loaded, labels_error,
            store_id, store_name, stock_method_enabled, allow_no_scan, partial_safe_scan, desktop_print, allow_employee_close,
            employee_close_completed_at, employee_close_completed_by,
            allow_stock_shortage, employee_workflow_mode, employee_workflow_stage,
            location_code, sklad_warehouse_id, sklad_warehouse_name,
            product_picking_started_at, product_picking_started_by,
            labels_started_at, labels_finished_at,
            trbx_status, trbx_count, trbx_last_synced_at, trbx_error,
            supply_barcode, supply_qr_pdf_url, supply_qr_pdf_s3_key, supply_qr_pdf_format,
            supply_qr_loaded_at, supply_qr_error,
            manual_sync_status, manual_sync_total_steps, manual_sync_completed_steps,
            manual_sync_message, manual_sync_error, manual_sync_started_at,
            manual_sync_finished_at, manual_sync_last_synced_at, manual_sync_removed_orders,
            manual_sync_released_orders, manual_sync_reconciled_sessions,
            wb_supply_done, wb_supply_created_at, wb_supply_closed_at, wb_supply_scan_dt,
            wb_destination_office_id, wb_cargo_type,
            created_at, updated_at
     from supply_settings where supply_id = $1`,
    [supplyId]
  );
  const row = result.rows?.[0] || null;
  if (row && !normalizeLocationCode(row.location_code)) {
    const assigned = await autoAssignWbSupplyLocation(supplyId);
    if (assigned) {
      row.location_code = assigned.location_code;
      row.sklad_warehouse_id = assigned.sklad_warehouse_id;
      row.sklad_warehouse_name = assigned.sklad_warehouse_name;
    }
  }
  return row;
}

async function getSupplyLabelStats(supplyId) {
  const result = await dbQuery(
    `select count(*)::int as total,
            count(*) filter (where coalesce(sticker_url, '') <> '')::int as loaded
     from supply_orders
     where supply_id = $1`,
    [supplyId]
  );
  const row = result.rows?.[0] || {};
  return {
    total: Number(row.total || 0),
    loaded: Number(row.loaded || 0),
  };
}

async function readSupplyOrderLabelPdf(row) {
  const storageKey = normalizeText(row?.sticker_s3_key);
  if (storageKey && isLocalStorageKey(storageKey)) {
    return fs.promises.readFile(resolveLocalFilePath(storageKey));
  }
  const rawUrl = normalizeText(row?.sticker_url) || (storageKey ? buildS3Url(storageKey) : "");
  if (!rawUrl) throw new Error(`Этикетка заказа ${row?.wb_order_id || ""} не загружена`);
  const url = /^https?:\/\//i.test(rawUrl)
    ? rawUrl
    : new URL(rawUrl, `http://127.0.0.1:${PORT}`).toString();
  const response = await fetch(url, { signal: AbortSignal.timeout(15000) });
  if (!response.ok) {
    throw new Error(`Не удалось прочитать этикетку заказа ${row?.wb_order_id || ""}: HTTP ${response.status}`);
  }
  return Buffer.from(await response.arrayBuffer());
}

async function buildSupplyOrderLabelsPdf(supplyId) {
  const result = await dbQuery(
    `select wb_order_id, sticker_url, sticker_s3_key
     from supply_orders
     where supply_id = $1
     order by order_created_at asc nulls last, wb_order_id asc`,
    [supplyId]
  );
  const rows = result.rows || [];
  if (!rows.length) return { total: 0, loaded: 0, buffer: null };
  const loadedRows = rows.filter((row) => row.sticker_s3_key || row.sticker_url);
  if (loadedRows.length !== rows.length) {
    return { total: rows.length, loaded: loadedRows.length, buffer: null };
  }

  const merged = await PDFDocument.create();
  for (const row of loadedRows) {
    const sourceBytes = await readSupplyOrderLabelPdf(row);
    const source = await PDFDocument.load(sourceBytes);
    const pages = await merged.copyPages(source, source.getPageIndices());
    for (const page of pages) merged.addPage(page);
  }
  return {
    total: rows.length,
    loaded: loadedRows.length,
    buffer: Buffer.from(await merged.save()),
  };
}

async function reconcileSupplySettingsLabels(supplyId, settings) {
  if (!settings?.supply_id) return settings;
  const stats = await getSupplyLabelStats(supplyId);
  const storedTotal = Math.max(0, Number(settings.labels_total || 0));
  const storedLoaded = Math.max(0, Number(settings.labels_loaded || 0));
  const actualTotal = Math.max(storedTotal, stats.total);
  const actualLoaded = Math.min(actualTotal, Math.max(0, stats.loaded));
  let nextStatus = normalizeText(settings.labels_status) || "none";
  let nextError = settings.labels_error || null;
  let dirty = false;

  if (storedTotal !== actualTotal) {
    settings.labels_total = actualTotal;
    dirty = true;
  }
  if (storedLoaded !== actualLoaded) {
    settings.labels_loaded = actualLoaded;
    dirty = true;
  }

  if (actualTotal > 0 && actualLoaded >= actualTotal) {
    if (nextStatus !== "ready" || nextError) {
      nextStatus = "ready";
      nextError = null;
      dirty = true;
    }
  } else if (nextStatus === "ready" && actualLoaded < actualTotal) {
    nextStatus = actualLoaded > 0 ? "loading" : "none";
    nextError = null;
    dirty = true;
  }

  if (dirty) {
    await dbQuery(
      `update supply_settings
       set labels_status = $2,
           labels_total = $3,
           labels_loaded = $4,
           labels_error = $5,
           updated_at = now()
       where supply_id = $1`,
      [supplyId, nextStatus, actualTotal, actualLoaded, nextError]
    );
    settings.labels_status = nextStatus;
    settings.labels_total = actualTotal;
    settings.labels_loaded = actualLoaded;
    settings.labels_error = nextError;
    settings.updated_at = new Date().toISOString();
    console.warn("WB labels settings reconciled", {
      supplyId,
      storedTotal,
      storedLoaded,
      actualTotal,
      actualLoaded,
      nextStatus,
    });
  }

  return settings;
}

async function ensureSupplyLabelsHealthy(supplyId, supplyName = null, settings = null) {
  let nextSettings = settings || (await getSupplySettings(supplyId));
  nextSettings = await reconcileSupplySettingsLabels(supplyId, nextSettings);
  if (!nextSettings?.supply_id) return nextSettings;

  const total = Math.max(0, Number(nextSettings.labels_total || 0));
  const loaded = Math.max(0, Number(nextSettings.labels_loaded || 0));
  const status = normalizeText(nextSettings.labels_status) || "none";
  if (total < 1 || loaded >= total || status === "loading") {
    return nextSettings;
  }

  const now = Date.now();
  const cooldownUntil = Number(labelRepairCooldowns.get(supplyId) || 0);
  if (cooldownUntil > now) {
    return nextSettings;
  }
  labelRepairCooldowns.set(supplyId, now + LABEL_AUTO_REPAIR_COOLDOWN_MS);

  console.warn("WB labels auto-repair scheduled", {
    supplyId,
    total,
    loaded,
    status,
  });
  void startLabelsJob(supplyId, supplyName || nextSettings.supply_name || null, false).catch((error) => {
    console.error("WB labels auto-repair failed", {
      supplyId,
      error: error?.message || String(error || ""),
    });
  });
  return nextSettings;
}

async function getSupplyStore(supplyId, supplyName = null, fallbackStore = null) {
  if (!supplyId) return fallbackStore || DEFAULT_STORE;
  await ensureSupplySettings(supplyId, supplyName, fallbackStore);
  const settings = await getSupplySettings(supplyId);
  const storedId = normalizeStoreId(settings?.store_id);
  const store = resolveStore(storedId) || fallbackStore || DEFAULT_STORE;
  if (store && (!storedId || !settings?.store_name)) {
    await dbQuery(
      "update supply_settings set store_id = $2, store_name = $3, updated_at = now() where supply_id = $1",
      [supplyId, store.id, store.name]
    );
  }
  return store;
}

function normalizeManualSyncStatus(value) {
  const normalized = normalizeText(value) || "idle";
  if (normalized === "running" || normalized === "ready" || normalized === "error") {
    return normalized;
  }
  return "idle";
}

function toManualSyncPayload(settings) {
  if (!settings) return null;
  return {
    status: normalizeManualSyncStatus(settings.manual_sync_status),
    totalSteps: Number(settings.manual_sync_total_steps || 0),
    completedSteps: Number(settings.manual_sync_completed_steps || 0),
    message: settings.manual_sync_message || null,
    error: settings.manual_sync_error || null,
    startedAt: settings.manual_sync_started_at || null,
    finishedAt: settings.manual_sync_finished_at || null,
    lastSyncedAt: settings.manual_sync_last_synced_at || null,
    removedOrders: Number(settings.manual_sync_removed_orders || 0),
    releasedOrders: Number(settings.manual_sync_released_orders || 0),
    reconciledSessions: Number(settings.manual_sync_reconciled_sessions || 0),
  };
}

async function markSupplyManualSyncRunning(supplyId, totalSteps, message) {
  await dbQuery(
    `update supply_settings
     set manual_sync_status = 'running',
         manual_sync_total_steps = $2,
         manual_sync_completed_steps = 0,
         manual_sync_message = $3,
         manual_sync_error = null,
         manual_sync_started_at = now(),
         manual_sync_finished_at = null,
         manual_sync_removed_orders = 0,
         manual_sync_released_orders = 0,
         manual_sync_reconciled_sessions = 0,
         updated_at = now()
     where supply_id = $1`,
    [supplyId, Math.max(0, Number(totalSteps || 0)), normalizeText(message) || null]
  );
}

async function updateSupplyManualSyncProgress(supplyId, completedSteps, totalSteps, message) {
  await dbQuery(
    `update supply_settings
     set manual_sync_total_steps = $2,
         manual_sync_completed_steps = $3,
         manual_sync_message = $4,
         updated_at = now()
     where supply_id = $1`,
    [
      supplyId,
      Math.max(0, Number(totalSteps || 0)),
      Math.max(0, Number(completedSteps || 0)),
      normalizeText(message) || null,
    ]
  );
}

async function markSupplyManualSyncReady(supplyId, totalSteps, message, summary = {}) {
  await dbQuery(
    `update supply_settings
     set manual_sync_status = 'ready',
         manual_sync_total_steps = $2,
         manual_sync_completed_steps = $2,
         manual_sync_message = $3,
         manual_sync_error = null,
         manual_sync_finished_at = now(),
         manual_sync_last_synced_at = now(),
         manual_sync_removed_orders = $4,
         manual_sync_released_orders = $5,
         manual_sync_reconciled_sessions = $6,
         updated_at = now()
     where supply_id = $1`,
    [
      supplyId,
      Math.max(0, Number(totalSteps || 0)),
      normalizeText(message) || null,
      Math.max(0, Number(summary?.removedOrdersCount || 0)),
      Math.max(0, Number(summary?.releasedAssignedOrdersCount || 0)),
      Math.max(0, Number(summary?.reconciledSessions || 0)),
    ]
  );
}

async function markSupplyManualSyncError(supplyId, completedSteps, totalSteps, message, errorText) {
  await dbQuery(
    `update supply_settings
     set manual_sync_status = 'error',
         manual_sync_total_steps = $2,
         manual_sync_completed_steps = $3,
         manual_sync_message = $4,
         manual_sync_error = $5,
         manual_sync_finished_at = now(),
         updated_at = now()
     where supply_id = $1`,
    [
      supplyId,
      Math.max(0, Number(totalSteps || 0)),
      Math.max(0, Number(completedSteps || 0)),
      normalizeText(message) || null,
      normalizeText(errorText) || "Не удалось завершить синхронизацию",
    ]
  );
}

async function setSupplyAccessMode(supplyId, mode) {
  await dbQuery(
    "update supply_settings set access_mode = $2, updated_at = now() where supply_id = $1",
    [supplyId, mode]
  );
}

async function setSupplyEmployeeWorkflowMode(supplyId, mode) {
  const normalizedMode = normalizeEmployeeWorkflowMode(mode);
  const result = await dbQuery(
    `update supply_settings
     set employee_workflow_mode = $2,
         updated_at = now()
     where supply_id = $1
     returning *`,
    [supplyId, normalizedMode]
  );
  return result.rows?.[0] || null;
}

async function setSupplyStockMethodEnabled(supplyId, enabled) {
  const result = await dbQuery(
    `update supply_settings
     set stock_method_enabled = $2,
         updated_at = now()
     where supply_id = $1
     returning *`,
    [supplyId, enabled === true]
  );
  return result.rows?.[0] || null;
}

async function getSupplyAccessUserIds(supplyId) {
  const result = await dbQuery(
    "select user_id from supply_access_users where supply_id = $1 order by user_id",
    [supplyId]
  );
  return result.rows.map((row) => Number(row.user_id));
}

async function setSupplyAccessUsers(supplyId, userIds) {
  await dbQuery("delete from supply_access_users where supply_id = $1", [supplyId]);
  if (!userIds.length) return;
  const values = [];
  const placeholders = userIds.map((userId, idx) => {
    values.push(userId);
    return `($1, $${idx + 2})`;
  });
  await dbQuery(
    `insert into supply_access_users (supply_id, user_id) values ${placeholders.join(", ")}`,
    [supplyId, ...values]
  );
}

async function upsertSupplyOrders(supplyId, orders) {
  if (!orders.length) return;
  const deduped = new Map();
  for (const order of orders) {
    const id = Number(order?.id);
    if (!Number.isFinite(id)) continue;
    deduped.set(id, order);
  }
  if (deduped.size === 0) return;
  const values = [];
  const rows = Array.from(deduped.values()).map((order, idx) => {
    const base = idx * 11;
    const qty = Number(order.quantity ?? 1);
    values.push(
      supplyId,
      Number(order.id),
      order.createdAt ? new Date(order.createdAt) : null,
      order.article ?? null,
      order.barcode ?? null,
      order.productName ?? null,
      order.nmId ?? null,
      Number.isFinite(qty) && qty > 0 ? qty : 1,
      order.warehouseId ?? null,
      order.cargoType ?? null,
      new Date()
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11})`;
  });

  await dbQuery(
    `insert into supply_orders
      (supply_id, wb_order_id, order_created_at, article, barcode, product_name, nm_id, quantity, warehouse_id, cargo_type, synced_at)
     values ${rows.join(", ")}
     on conflict (supply_id, wb_order_id) do update set
       order_created_at = coalesce(excluded.order_created_at, supply_orders.order_created_at),
       article = coalesce(excluded.article, supply_orders.article),
       barcode = coalesce(excluded.barcode, supply_orders.barcode),
       product_name = coalesce(excluded.product_name, supply_orders.product_name),
       nm_id = coalesce(excluded.nm_id, supply_orders.nm_id),
       quantity = coalesce(excluded.quantity, supply_orders.quantity),
       warehouse_id = coalesce(excluded.warehouse_id, supply_orders.warehouse_id),
       cargo_type = coalesce(excluded.cargo_type, supply_orders.cargo_type),
       synced_at = now(),
       updated_at = now()`,
    values
  );
}

async function pruneMissingOpenSupplyOrders(supplyId, orders) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId) return;
  const orderIds = Array.from(
    new Set(
      (Array.isArray(orders) ? orders : [])
        .map((order) => Number(order?.id))
        .filter((id) => Number.isFinite(id) && id > 0)
    )
  );
  if (orderIds.length) {
    await dbQuery(
      `delete from employee_scan_issues
       where supply_id = $1
         and wb_order_id is not null
         and wb_order_id <> all($2::bigint[])
         and resolved_at is null`,
      [normalizedSupplyId, orderIds]
    );
    await dbQuery(
      `delete from supply_orders
       where supply_id = $1
         and collected_at is null
         and wb_order_id <> all($2::bigint[])`,
      [normalizedSupplyId, orderIds]
    );
    return;
  }
  await dbQuery(
    "delete from employee_scan_issues where supply_id = $1 and resolved_at is null",
    [normalizedSupplyId]
  );
  await dbQuery(
    "delete from supply_orders where supply_id = $1 and collected_at is null",
    [normalizedSupplyId]
  );
}

async function refreshSupplySnapshot(supplyId, supplyName = null) {
  if (!supplyId) return [];
  if (supplySyncJobs.has(supplyId)) return supplySyncJobs.get(supplyId);
  const job = (async () => {
    const store = await getSupplyStore(supplyId, supplyName);
    await ensureSupplySettings(supplyId, supplyName, store);
    let ordersRaw = [];
    let snapshotFetchFailed = false;
    try {
      ordersRaw = await fetchSupplyOrders(store, supplyId);
    } catch (err) {
      snapshotFetchFailed = true;
      try {
        const fallback = await getOrders(store?.id);
        ordersRaw = fallback.filter((order) => String(order.supplyId || "") === String(supplyId));
      } catch (fallbackErr) {
        console.error("Supply snapshot fallback failed", {
          supplyId,
          error: fallbackErr?.message || String(fallbackErr),
        });
        ordersRaw = [];
      }
    }
    const normalized = ordersRaw
      .map((order) => normalizeOrder(order, supplyId))
      .filter((order) => order && order.id);
    const existingOpenResult = await dbQuery(
      `select count(*) filter (where collected_at is null)::int as open_orders
       from supply_orders
       where supply_id = $1`,
      [supplyId]
    );
    const existingOpenOrders = Number(existingOpenResult.rows?.[0]?.open_orders || 0);
    if (!normalized.length) {
      if (existingOpenOrders > 0) {
        console.warn("WB snapshot refresh skipped destructive empty prune", {
          supplyId,
          existingOpenOrders,
          snapshotFetchFailed,
        });
        return [];
      }
    }
    if (snapshotFetchFailed && existingOpenOrders > 0 && normalized.length > 0 && normalized.length < existingOpenOrders) {
      const shrinkRatio = normalized.length / Math.max(1, existingOpenOrders);
      if (shrinkRatio < 0.75) {
        console.warn("WB snapshot refresh skipped suspicious fallback prune", {
          supplyId,
          existingOpenOrders,
          fetchedOrders: normalized.length,
          shrinkRatio,
        });
        await upsertSupplyOrders(supplyId, normalized);
        await fillMissingOrderInfo(supplyId, normalized, store);
        await ensureSupplyLabelsHealthy(supplyId, supplyName);
        return normalized;
      }
    }
    await pruneMissingOpenSupplyOrders(supplyId, normalized);
    if (normalized.length) {
      // Не добавляем автоматически заказы, ДОБАВЛЕННЫЕ на WB в уже наполненную поставку:
      // они не должны попадать в сборку в обход ручной синхронизации (которая их блокирует
      // сообщением «синхронизация невозможна, уберите заказ на WB»). При первичном наполнении
      // (в БД ещё нет заказов этой поставки) вставляем весь состав.
      const existingSnapRes = await dbQuery(
        "select wb_order_id from supply_orders where supply_id = $1",
        [supplyId]
      );
      const existingSnapIds = new Set(
        (existingSnapRes.rows || []).map((r) => String(r.wb_order_id))
      );
      const ordersToWrite =
        existingSnapIds.size === 0
          ? normalized
          : normalized.filter((o) => existingSnapIds.has(String(o.id)));
      if (ordersToWrite.length) {
        await upsertSupplyOrders(supplyId, ordersToWrite);
        await fillMissingOrderInfo(supplyId, ordersToWrite, store);
      }
    }
    await ensureSupplyLabelsHealthy(supplyId, supplyName);
    return normalized;
  })();
  supplySyncJobs.set(supplyId, job);
  job.finally(() => supplySyncJobs.delete(supplyId));
  return job;
}

async function ensureSupplySnapshot(supplyId, supplyName = null) {
  return refreshSupplySnapshot(supplyId, supplyName);
}

async function ensureSupplySnapshotIfStale(supplyId, supplyName = null) {
  if (!supplyId) return;
  const result = await dbQuery(
    "select max(synced_at) as last_sync from supply_orders where supply_id = $1",
    [supplyId]
  );
  const last = result.rows?.[0]?.last_sync;
  if (!last) {
    try {
      await refreshSupplySnapshot(supplyId, supplyName);
    } catch {}
    return;
  }
  const lastTs = new Date(last).getTime();
  if (!Number.isFinite(lastTs) || Date.now() - lastTs > SUPPLY_SYNC_MS) {
    try {
      await refreshSupplySnapshot(supplyId, supplyName);
    } catch {}
  }
}

async function ensureSupplySnapshotIfEmpty(supplyId, supplyName = null) {
  if (!supplyId) return;
  const check = await dbQuery(
    "select 1 from supply_orders where supply_id = $1 limit 1",
    [supplyId]
  );
  if (!check.rows || check.rows.length === 0) {
    await refreshSupplySnapshot(supplyId, supplyName);
  }
}

async function fillMissingOrderInfo(supplyId, orders, store) {
  const missing = orders.filter((order) => {
    if (!order.article) return false;
    const rawName = order.product_name || order.productName || "";
    const needsName = isPlaceholderName(rawName, order.article);
    return needsName || !order.barcode;
  });
  if (!missing.length) return orders;
  const infoMap = await resolveProductInfoForOrders(store, missing);
  const updates = [];
  for (const order of missing) {
    const info = infoMap.get(order.article);
    if (!info) continue;
    const rawName = order.product_name || order.productName || "";
    const needsName = isPlaceholderName(rawName, order.article);
    const nextName = needsName ? info.title || null : rawName || null;
    const nextBarcode = order.barcode || (info.barcodes && info.barcodes[0]) || null;
    if (!nextName && !nextBarcode) continue;
    if (nextName) order.product_name = nextName;
    order.barcode = nextBarcode;
    updates.push({ id: order.wb_order_id, name: nextName, barcode: nextBarcode });
  }
  for (const update of updates) {
    await dbQuery(
      `update supply_orders set product_name = coalesce($3, product_name),
       barcode = coalesce($4, barcode), updated_at = now()
       where supply_id = $1 and wb_order_id = $2`,
      [supplyId, update.id, update.name, update.barcode]
    );
  }
  return orders;
}

let nameBackfillRunning = false;
let nameBackfillTimer = null;

async function backfillMissingNamesBatch() {
  if (nameBackfillRunning) return;
  nameBackfillRunning = true;
  try {
    const result = await dbQuery(
      `select o.article, o.nm_id, s.store_id, count(*) as cnt
       from supply_orders o
       left join supply_settings s on s.supply_id = o.supply_id
       where o.article is not null
         and (o.product_name is null or o.product_name = '' or lower(o.product_name) = lower(o.article))
       group by o.article, o.nm_id, s.store_id
       order by cnt desc
       limit $1`,
      [NAME_BACKFILL_BATCH]
    );
    const rows = result.rows || [];
    if (!rows.length) return;
    const byStore = new Map();
    for (const row of rows) {
      const article = normalizeArticleKey(row.article);
      if (!article) continue;
      const storeId = normalizeStoreId(row.store_id) || DEFAULT_STORE?.id || null;
      if (!storeId) continue;
      if (!byStore.has(storeId)) byStore.set(storeId, []);
      byStore.get(storeId).push({
        article,
        nmId: row.nm_id,
        productName: null,
      });
    }
    for (const [storeId, orders] of byStore) {
      const store = resolveStore(storeId) || DEFAULT_STORE;
      if (!store) continue;
      const infoMap = await resolveProductInfoForOrders(store, orders);
      for (const order of orders) {
        const info = infoMap.get(order.article);
        const title = info?.title || null;
        if (!title) continue;
        await dbQuery(
          `update supply_orders o
           set product_name = $1, updated_at = now()
           from supply_settings s
           where o.supply_id = s.supply_id
             and s.store_id = $2
             and o.article = $3
             and (o.product_name is null or o.product_name = '' or lower(o.product_name) = lower(o.article))`,
          [title, store.id, order.article]
        );
      }
    }
  } catch (err) {
    console.error("Name backfill failed", err?.message || err);
  } finally {
    nameBackfillRunning = false;
  }
}

function startNameBackfill() {
  if (!NAME_BACKFILL_ENABLED) return;
  if (nameBackfillTimer) return;
  const run = () => {
    backfillMissingNamesBatch().catch(() => {});
  };
  setTimeout(run, NAME_BACKFILL_DELAY_MS);
  nameBackfillTimer = setInterval(run, NAME_BACKFILL_INTERVAL_MS);
}

function countDistinctItems(orders) {
  if (!orders || !orders.length) return 0;
  const set = new Set();
  for (const order of orders) {
    const key = [
      order.productName || order.product_name || "",
      order.barcode || "",
      order.article || "",
      order.nmId || order.nm_id || "",
    ].join("||");
    set.add(key);
  }
  return set.size;
}

async function getSupplyAggregates(supplyIds) {
  if (!supplyIds.length) return new Map();
  const result = await dbQuery(
    `select supply_id,
            count(*) as order_count,
            count(*) filter (where collected_at is not null) as collected_count,
            count(distinct (coalesce(product_name, ''), coalesce(barcode, ''), coalesce(article, ''), coalesce(nm_id::text, ''))) as item_count,
            count(distinct assigned_user_id) filter (where assigned_user_id is not null) as assigned_users
     from supply_orders
     where supply_id = any($1)
     group by supply_id`,
    [supplyIds]
  );
  const map = new Map();
  for (const row of result.rows || []) {
    map.set(row.supply_id, {
      orderCount: Number(row.order_count || 0),
      collectedCount: Number(row.collected_count || 0),
      itemCount: Number(row.item_count || 0),
      assignedUsers: Number(row.assigned_users || 0),
    });
  }
  return map;
}

async function getSupplyAccessStats(supplyIds) {
  if (!supplyIds.length) {
    return { accessUsers: new Map(), accessModes: new Map() };
  }
  const usersResult = await dbQuery(
    `select supply_id, count(*) as access_users
     from supply_access_users
     where supply_id = any($1)
     group by supply_id`,
    [supplyIds]
  );
  const modeResult = await dbQuery(
    "select supply_id, access_mode from supply_settings where supply_id = any($1)",
    [supplyIds]
  );
  const accessUsers = new Map();
  for (const row of usersResult.rows || []) {
    accessUsers.set(row.supply_id, Number(row.access_users || 0));
  }
  const accessModes = new Map();
  for (const row of modeResult.rows || []) {
    accessModes.set(row.supply_id, row.access_mode);
  }
  return { accessUsers, accessModes };
}

async function getSupplyProgress(supplyId, accessMode) {
  let rows = [];
  let baseUserIds = [];
  if (accessMode === "selected_split") {
    const result = await dbQuery(
      `select assigned_user_id as user_id,
              count(*) as total,
              count(*) filter (where collected_at is not null) as collected
       from supply_orders
       where supply_id = $1 and assigned_user_id is not null
       group by assigned_user_id`,
      [supplyId]
    );
    rows = result.rows || [];
    baseUserIds = await getSupplyAccessUserIds(supplyId);
  } else {
    const result = await dbQuery(
      `select collected_by as user_id,
              count(*) as collected
       from supply_orders
       where supply_id = $1 and collected_by is not null
       group by collected_by`,
      [supplyId]
    );
    rows = result.rows || [];
    if (accessMode === "selected") {
      baseUserIds = await getSupplyAccessUserIds(supplyId);
    }
  }

  const pickingResult = await dbQuery(
    `select user_id,
            coalesce(sum(greatest(picking_required_qty, 0)), 0)::int as picking_total,
            coalesce(sum(least(greatest(picking_collected_qty, 0), greatest(picking_required_qty, 0))), 0)::int as picking_collected
     from employee_supply_sessions
     where supply_id = $1
       and coalesce(status, '') <> 'reassigned'
       and session_role in ('pick', 'pick_pack')
     group by user_id`,
    [supplyId]
  );

  const rowMap = new Map(rows.map((row) => [Number(row.user_id), row]));
  const pickingMap = new Map((pickingResult.rows || []).map((row) => [Number(row.user_id), row]));
  const userIds = Array.from(
    new Set([
      ...baseUserIds.map((id) => Number(id)).filter(Boolean),
      ...rowMap.keys(),
      ...pickingMap.keys(),
    ])
  );
  if (!userIds.length) return [];

  const users = await dbQuery(
    "select id, surname, name from users where id = any($1)",
    [userIds]
  );
  const byId = new Map((users.rows || []).map((row) => [Number(row.id), row]));

  return userIds
    .map((userId) => {
      const user = byId.get(Number(userId));
      if (!user) return null;
      const row = rowMap.get(Number(userId));
      const picking = pickingMap.get(Number(userId));
      const pickingTotal = Number(picking?.picking_total || 0);
      const pickingCollected = Number(picking?.picking_collected || 0);
      return {
        userId: Number(userId),
        surname: user.surname,
        name: user.name,
        total: row?.total ? Number(row.total) : accessMode === "selected_split" ? 0 : null,
        collected: row?.collected ? Number(row.collected) : 0,
        pickingTotal,
        pickingCollected,
        pickingRemaining: Math.max(0, pickingTotal - pickingCollected),
      };
    })
    .filter(Boolean);
}

async function getExpectedWbEmployeeInventoryByArticle() {
  const result = await dbQuery(
    `select u.external_employee_id,
            o.article,
            coalesce(sum(greatest(coalesce(o.quantity, 1), 1)), 0)::int as qty
     from supply_orders o
     join supply_settings ss on ss.supply_id = o.supply_id
     join users u on u.id = o.assigned_user_id
     where ss.stock_method_enabled = true
       and o.assigned_user_id is not null
       and o.collected_at is null
       and u.external_employee_id is not null
       and coalesce(nullif(btrim(o.article), ''), '') <> ''
     group by u.external_employee_id, o.article`,
    []
  );
  const expected = new Map();
  for (const row of result.rows || []) {
    const employeeId = Number(row.external_employee_id || 0);
    const article = normalizeArticleKey(row.article);
    const qty = Number(row.qty || 0);
    if (!employeeId || !article || qty <= 0) continue;
    expected.set(`${employeeId}|${article}`, qty);
  }
  return expected;
}

async function getExpectedOzonEmployeeInventoryByArticle() {
  const result = await dbQuery(
    `select u.external_employee_id,
            i.offer_id as article,
            coalesce(sum(greatest(coalesce(i.quantity, 1), 1)), 0)::int as qty
     from ozon_assembly_orders o
     join ozon_assemblies a on a.id = o.assembly_id
     join ozon_assembly_order_items i on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
     join users u on u.id = o.assigned_user_id
     where a.stock_method_enabled = true
       and o.assigned_user_id is not null
       and o.collected_at is null
       and o.sent_to_delivering_at is null
       and u.external_employee_id is not null
       and coalesce(nullif(btrim(i.offer_id), ''), '') <> ''
     group by u.external_employee_id, i.offer_id`,
    []
  );
  const expected = new Map();
  for (const row of result.rows || []) {
    const employeeId = Number(row.external_employee_id || 0);
    const article = normalizeArticleKey(row.article);
    const qty = Number(row.qty || 0);
    if (!employeeId || !article || qty <= 0) continue;
    expected.set(`${employeeId}|${article}`, qty);
  }
  return expected;
}

async function getExpectedEmployeeInventoryByArticle() {
  const [wbExpected, ozExpected] = await Promise.all([
    getExpectedWbEmployeeInventoryByArticle(),
    getExpectedOzonEmployeeInventoryByArticle(),
  ]);
  const combined = new Map();
  for (const source of [wbExpected, ozExpected]) {
    for (const [key, qty] of source.entries()) {
      combined.set(key, Number(combined.get(key) || 0) + Number(qty || 0));
    }
  }
  return combined;
}

async function buildStockProblemReport() {
  const report = await getStockProblemReport();
  const expected = await getExpectedEmployeeInventoryByArticle();
  return {
    ...report,
    leftoverEmployees: (Array.isArray(report.leftoverEmployees) ? report.leftoverEmployees : [])
      .map((row) => {
        const externalEmployeeId = Number(row?.externalEmployeeId || 0);
        const article = normalizeArticleKey(row?.article);
        const actualQty = Number(row?.quantity || 0);
        const expectedQty =
          externalEmployeeId && article ? Number(expected.get(`${externalEmployeeId}|${article}`) || 0) : 0;
        const leftoverQty = actualQty - expectedQty;
        if (leftoverQty <= 0) return null;
        return {
          ...row,
          actualQty,
          expectedQty,
          quantity: leftoverQty,
        };
      })
      .filter(Boolean),
  };
}

// The stock-problem report runs several heavy aggregations over the (slow,
// shared) sklad DB. The admin tab polls it every 15s and several admins may
// open it at once, so recomputing per request caused REQUEST_TIMEOUT under
// load. Cache ~30s, dedupe concurrent builds, serve last good on failure.
let stockProblemReportCache = { ts: 0, data: null, loading: null };
const STOCK_PROBLEM_REPORT_CACHE_MS = 30000;

async function getStockProblemReportCached(force = false) {
  const now = Date.now();
  if (
    !force &&
    stockProblemReportCache.data &&
    now - stockProblemReportCache.ts < STOCK_PROBLEM_REPORT_CACHE_MS
  ) {
    return stockProblemReportCache.data;
  }
  if (stockProblemReportCache.loading) return stockProblemReportCache.loading;
  stockProblemReportCache.loading = (async () => {
    try {
      const data = await buildStockProblemReport();
      stockProblemReportCache = { ts: Date.now(), data, loading: null };
      return data;
    } catch (error) {
      stockProblemReportCache.loading = null;
      if (stockProblemReportCache.data) return stockProblemReportCache.data; // serve stale
      throw error;
    }
  })();
  return stockProblemReportCache.loading;
}

async function splitSupplyOrders(supplyId, userIds, mode = "split") {
  if (!userIds.length) {
    throw new Error("Не выбраны сотрудники для распределения");
  }
  if (mode === "redistribute") {
    await dbQuery(
      "update supply_orders set assigned_user_id = null, assigned_at = null where supply_id = $1 and collected_at is null",
      [supplyId]
    );
  }
  const result = await dbQuery(
    `select wb_order_id, order_created_at
     from supply_orders
     where supply_id = $1 and collected_at is null ${mode === "split" ? "and assigned_user_id is null" : ""}`,
    [supplyId]
  );
  const orders = (result.rows || []).sort((a, b) => {
    const aTime = a.order_created_at ? new Date(a.order_created_at).getTime() : 0;
    const bTime = b.order_created_at ? new Date(b.order_created_at).getTime() : 0;
    if (aTime === bTime) return Number(a.wb_order_id) - Number(b.wb_order_id);
    return aTime - bTime;
  });
  if (!orders.length) {
    return { total: 0, assigned: 0, perUser: [] };
  }

  const sortedUsers = [...userIds].map(Number).sort((a, b) => a - b);
  const total = orders.length;
  const perUser = [];
  const base = Math.floor(total / sortedUsers.length);
  const extra = total % sortedUsers.length;

  let offset = 0;
  for (let i = 0; i < sortedUsers.length; i += 1) {
    const userId = sortedUsers[i];
    const count = base + (i < extra ? 1 : 0);
    const slice = orders.slice(offset, offset + count).map((row) => Number(row.wb_order_id));
    offset += count;
    if (slice.length) {
      await dbQuery(
        "update supply_orders set assigned_user_id = $3, assigned_at = now() where supply_id = $1 and wb_order_id = any($2)",
        [supplyId, slice, userId]
      );
    }
    perUser.push({ userId, count: slice.length });
  }

  return {
    total,
    assigned: orders.length,
    perUser,
  };
}

async function resetSupplyAccess(supplyId) {
  await setSupplyAccessMode(supplyId, "hidden");
  await setSupplyEmployeeWorkflowStage(supplyId, EMPLOYEE_WORKFLOW_ORDERS_ONLY);
  await resetEmployeePickingForSupply(supplyId, PICKING_STATUS_NOT_REQUIRED);
  await dbQuery("delete from supply_access_users where supply_id = $1", [supplyId]);
  await clearSupplyAssignmentParts(supplyId);
  await dbQuery(
    "update supply_orders set assigned_user_id = null, assignment_part_id = null, assigned_at = null where supply_id = $1",
    [supplyId]
  );
}

function buildAcceptedScanBarcodes(rawBarcodes) {
  return Array.from(
    new Set(
      (Array.isArray(rawBarcodes) ? rawBarcodes : [])
        .map(normalizeBarcode)
        .filter(Boolean)
    )
  );
}

function normalizeLookupProductName(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildLookupProductNameKeys(matches = []) {
  return Array.from(
    new Set(
      (Array.isArray(matches) ? matches : [])
        .map((item) => normalizeLookupProductName(item?.name || item?.product_name || ""))
        .filter(Boolean)
    )
  );
}

function getOrderIdentityKey(order) {
  return (
    normalizeBarcode(order?.barcode) ||
    normalizeArticleKey(order?.article) ||
    (Number.isFinite(Number(order?.nm_id)) ? `nm:${Number(order.nm_id)}` : "") ||
    String(order?.product_name || order?.wb_order_id || "")
  );
}

function limitWarningLabels(values = [], limit = 6) {
  const filtered = Array.from(new Set((Array.isArray(values) ? values : []).filter(Boolean)));
  if (filtered.length <= limit) return filtered;
  return [...filtered.slice(0, limit), `+ещё ${filtered.length - limit}`];
}

function formatWarehouseScanProductLabel(item) {
  const article = normalizeArticleKey(item?.article || item?.code || null);
  const name = String(item?.name || item?.product_name || "").trim();
  if (article && name) return `${article}`;
  return article || name || "";
}

function formatSupplyMatchLabel(item) {
  const article = normalizeArticleKey(item?.article);
  const barcode = normalizeBarcode(item?.barcode);
  const name = String(item?.product_name || "").trim();
  if (article && barcode) return `${article} (${barcode})`;
  if (article && name) return article;
  return article || barcode || name || "";
}

function buildMultipleScanProductsReason(inputBarcode, scanProducts = []) {
  const scanned = normalizeBarcode(inputBarcode);
  const products = limitWarningLabels(
    (Array.isArray(scanProducts) ? scanProducts : []).map((item) => formatWarehouseScanProductLabel(item))
  );
  const suffix = products.length ? `: ${products.join(", ")}` : "";
  return `ШК ${scanned || "—"} есть у нескольких товаров в складской БД${suffix}. Система не может однозначно зачесть этот скан. Нажмите «Ошибка» и отправьте админу.`;
}

function buildAmbiguousScanWarning(inputBarcode, matches, options = {}) {
  const scanned = normalizeBarcode(inputBarcode);
  const matchedItems = limitWarningLabels(
    (Array.isArray(matches) ? matches : []).map((item) => formatSupplyMatchLabel(item))
  );
  const parts = [];
  if (options?.multipleScanProducts) {
    parts.push(buildMultipleScanProductsReason(inputBarcode, options?.scanProducts));
  }
  if (matchedItems.length) {
    parts.push(`В этой поставке по этому скану подходят несколько товаров: ${matchedItems.join(", ")}.`);
  }
  if (parts.length) return parts.join(" ");
  return `ШК ${scanned || "—"} подходит к нескольким товарам в этой поставке. Нажмите «Ошибка» и отправьте админу.`;
}

function buildAmbiguousPickWarning(inputBarcode, matches, options = {}) {
  const items = limitWarningLabels(
    (Array.isArray(matches) ? matches : []).map((item) =>
      normalizeArticleKey(item?.article) || String(item?.product_name || item?.barcode || item?.item_key || "").trim()
    )
  );
  const parts = [];
  if (options?.multipleScanProducts) {
    parts.push(buildMultipleScanProductsReason(inputBarcode, options?.scanProducts));
  }
  if (items.length) {
    parts.push(`В вашей сборке товара по этому скану подходят несколько позиций: ${items.join(", ")}.`);
  }
  if (parts.length) return parts.join(" ");
  return `ШК ${normalizeBarcode(inputBarcode) || "—"} подходит к нескольким позициям в вашей сборке товара. Нажмите «Ошибка» и отправьте админу.`;
}

function getCachedScanLookup(barcode) {
  const key = normalizeBarcode(barcode);
  if (!key) return null;
  const cached = scanLookupCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.ts > SCAN_LOOKUP_CACHE_MS) {
    scanLookupCache.delete(key);
    return null;
  }
  return cached;
}

function setCachedScanLookup(barcode, value) {
  const key = normalizeBarcode(barcode);
  if (!key) return;
  if (scanLookupCache.has(key)) scanLookupCache.delete(key);
  while (scanLookupCache.size >= SCAN_LOOKUP_CACHE_MAX) {
    const oldestKey = scanLookupCache.keys().next().value;
    if (!oldestKey) break;
    scanLookupCache.delete(oldestKey);
  }
  scanLookupCache.set(key, {
    ts: Date.now(),
    barcodes: buildAcceptedScanBarcodes(value?.barcodes || []),
    found: Boolean(value?.found),
    multiple: Boolean(value?.multiple),
    matches: Array.isArray(value?.matches) ? value.matches : [],
    lookupFailed: Boolean(value?.lookupFailed),
  });
}

async function getScanLookupBarcodes(inputBarcode, options = {}) {
  const normalizedInput = normalizeBarcode(inputBarcode);
  if (!normalizedInput) {
    return { barcodes: [], found: false, multiple: false, matches: [], lookupFailed: false };
  }
  const force = options?.force === true;
  if (!force) {
    const cached = getCachedScanLookup(normalizedInput);
    if (cached) {
      return {
        barcodes: cached.barcodes,
        found: cached.found,
        multiple: cached.multiple,
        matches: cached.matches,
        lookupFailed: cached.lookupFailed,
      };
    }
  }
  try {
    const status = await getMsProductStatusByBarcode(normalizedInput, force ? { force: true } : {});
    const barcodes = buildAcceptedScanBarcodes([normalizedInput, ...(status?.barcodes || [])]);
    const result = {
      barcodes,
      found: Boolean(status?.found),
      multiple: Boolean(status?.multiple),
      matches: Array.isArray(status?.matches) ? status.matches : [],
      lookupFailed: false,
    };
    setCachedScanLookup(normalizedInput, result);
    return result;
  } catch {
    const stale = scanLookupCache.get(normalizedInput);
    if (stale && Array.isArray(stale.barcodes) && stale.barcodes.length > 0) {
      return {
        barcodes: stale.barcodes,
        found: stale.found,
        multiple: stale.multiple,
        matches: stale.matches,
        lookupFailed: true,
      };
    }
    const fallback = { barcodes: [normalizedInput], found: false, multiple: false, matches: [], lookupFailed: true };
    setCachedScanLookup(normalizedInput, fallback);
    return fallback;
  }
}

async function getAcceptedScanBarcodesForOrder(order, options = {}) {
  const force = options?.force === true;
  const barcodeCache = options?.barcodeCache || null;
  const articleCache = options?.articleCache || null;
  const wbBarcode = normalizeBarcode(order?.barcode);
  const article = normalizeArticleKey(order?.article);
  let lookupFailed = false;

  if (wbBarcode) {
    const globalCacheKey = `barcode:${wbBarcode}`;
    let cached = force ? null : barcodeCache?.get(wbBarcode);
    if (!cached) {
      cached = getCachedAcceptedOrderBarcodes(globalCacheKey, force);
      if (cached && barcodeCache) barcodeCache.set(wbBarcode, cached);
    }
    if (!cached) {
      try {
        const status = await getMsProductStatusByBarcode(wbBarcode, force ? { force: true } : {});
        cached = {
          found: Boolean(status?.found),
          barcodes: buildAcceptedScanBarcodes(status?.barcodes || [], wbBarcode),
          source: "wb_barcode",
        };
      } catch {
        cached = { found: false, barcodes: [], error: true, source: "wb_barcode" };
        lookupFailed = true;
      }
      if (barcodeCache) barcodeCache.set(wbBarcode, cached);
      setCachedAcceptedOrderBarcodes(globalCacheKey, cached);
    } else if (cached.error) {
      lookupFailed = true;
    }

    if (cached.found) {
      return { barcodes: cached.barcodes, lookupFailed, source: "wb_barcode" };
    }

    return { barcodes: [], lookupFailed, source: "wb_barcode_missing" };
  }

  if (!article) {
    return { barcodes: [], lookupFailed, source: null };
  }

  const globalCacheKey = `article:${article}`;
  let cached = force ? null : articleCache?.get(article);
  if (!cached) {
    cached = getCachedAcceptedOrderBarcodes(globalCacheKey, force);
    if (cached && articleCache) articleCache.set(article, cached);
  }
  if (!cached) {
    try {
      const status = await getMsProductStatusByArticle(article, force ? { force: true } : {});
      cached = {
        found: Boolean(status?.found),
        barcodes: buildAcceptedScanBarcodes(status?.barcodes || [], wbBarcode),
        source: "article_fallback",
      };
    } catch {
      cached = { found: false, barcodes: [], error: true, source: "article_fallback" };
      lookupFailed = true;
    }
    if (articleCache) articleCache.set(article, cached);
    setCachedAcceptedOrderBarcodes(globalCacheKey, cached);
  } else if (cached.error) {
    lookupFailed = true;
  }

  if (cached.found) {
    return { barcodes: cached.barcodes, lookupFailed, source: "article_fallback" };
  }

  return { barcodes: [], lookupFailed, source: null };
}

function getEmployeeOrderScanIndexKey(supplyId, auth, settings) {
  const normalizedSupplyId = normalizeSupplyId(supplyId) || String(supplyId || "").trim();
  const userKey = settings?.access_mode === "selected_split" ? `u:${Number(auth?.id) || 0}` : "all";
  return `${normalizedSupplyId}|${userKey}`;
}

function getCachedEmployeeOrderScanIndexEntry(cacheKey) {
  const entry = employeeOrderScanIndexCache.get(cacheKey);
  if (!entry) return null;
  if (entry.loading) return entry;
  if (Date.now() - Number(entry.ts || 0) >= EMPLOYEE_ORDER_SCAN_INDEX_CACHE_MS) {
    employeeOrderScanIndexCache.delete(cacheKey);
    return null;
  }
  return entry;
}

function rememberEmployeeOrderScanIndexEntry(cacheKey, entry) {
  if (employeeOrderScanIndexCache.has(cacheKey)) {
    employeeOrderScanIndexCache.delete(cacheKey);
  }
  while (employeeOrderScanIndexCache.size >= EMPLOYEE_ORDER_SCAN_INDEX_MAX) {
    const oldestKey = employeeOrderScanIndexCache.keys().next().value;
    if (!oldestKey) break;
    employeeOrderScanIndexCache.delete(oldestKey);
  }
  employeeOrderScanIndexCache.set(cacheKey, entry);
  return entry;
}

async function buildEmployeeOrderScanIndex(supplyId, auth, settings) {
  const isSplit = settings?.access_mode === "selected_split";
  const baseParams = [supplyId];
  let where = "supply_id = $1 and collected_at is null";
  if (isSplit) {
    baseParams.push(auth.id);
    where += " and assigned_user_id = $2";
  }
  const result = await dbQuery(
    `select wb_order_id, order_created_at, article, barcode, product_name, nm_id,
            quantity, sticker_url, sticker_barcode,
            scan_passed_at, scan_barcode,
            label_scan_passed_at, label_scan_barcode
     from supply_orders
     where ${where}
     order by order_created_at asc, wb_order_id asc`,
    baseParams
  );
  const orders = result.rows || [];
  const sources = [];
  const sourceKeys = new Set();
  for (const order of orders) {
    const wbBarcode = normalizeBarcode(order?.barcode);
    const article = normalizeArticleKey(order?.article);
    const sourceKey = wbBarcode ? `barcode:${wbBarcode}` : article ? `article:${article}` : "";
    if (!sourceKey || sourceKeys.has(sourceKey)) continue;
    sourceKeys.add(sourceKey);
    sources.push({
      sourceKey,
      order: {
        article: order?.article || null,
        barcode: order?.barcode || null,
        nm_id: order?.nm_id ?? null,
        product_name: order?.product_name || null,
      },
    });
  }

  const barcodeCache = new Map();
  const articleCache = new Map();
  const acceptedBySource = new Map();
  await mapWithConcurrency(sources, EMPLOYEE_ORDER_SCAN_INDEX_CONCURRENCY, async (entry) => {
    const accepted = await getAcceptedScanBarcodesForOrder(entry.order, {
      barcodeCache,
      articleCache,
    });
    acceptedBySource.set(entry.sourceKey, accepted);
    return accepted;
  });

  const barcodeMatches = new Map();
  const missingBarcodeMatches = new Map();
  for (const order of orders) {
    const wbBarcode = normalizeBarcode(order?.barcode);
    const article = normalizeArticleKey(order?.article);
    const sourceKey = wbBarcode ? `barcode:${wbBarcode}` : article ? `article:${article}` : "";
    const accepted = sourceKey ? acceptedBySource.get(sourceKey) : null;
    const candidateBarcodes = buildAcceptedScanBarcodes(accepted?.barcodes || [], wbBarcode);
    if (!candidateBarcodes.length) continue;
    const target = wbBarcode ? barcodeMatches : missingBarcodeMatches;
    for (const code of candidateBarcodes) {
      if (!target.has(code)) target.set(code, []);
      target.get(code).push(order);
    }
  }

  return {
    ts: Date.now(),
    barcodeMatches,
    missingBarcodeMatches,
  };
}

function ensureEmployeeOrderScanIndexWarm(supplyId, auth, settings) {
  if (!supplyId || !auth?.id) return null;
  const cacheKey = getEmployeeOrderScanIndexKey(supplyId, auth, settings);
  const cached = getCachedEmployeeOrderScanIndexEntry(cacheKey);
  if (cached?.loading) return cached.loading;
  if (cached?.barcodeMatches || cached?.missingBarcodeMatches) {
    return Promise.resolve(cached);
  }
  const entry = cached || { ts: 0, barcodeMatches: null, missingBarcodeMatches: null, loading: null };
  const loading = buildEmployeeOrderScanIndex(supplyId, auth, settings)
    .then((built) => {
      const readyEntry = {
        ts: Date.now(),
        barcodeMatches: built?.barcodeMatches || new Map(),
        missingBarcodeMatches: built?.missingBarcodeMatches || new Map(),
        loading: null,
      };
      rememberEmployeeOrderScanIndexEntry(cacheKey, readyEntry);
      return readyEntry;
    })
    .catch(() => {
      const fallbackEntry = {
        ...entry,
        loading: null,
      };
      rememberEmployeeOrderScanIndexEntry(cacheKey, fallbackEntry);
      return fallbackEntry;
    });
  const loadingEntry = {
    ...entry,
    loading,
  };
  rememberEmployeeOrderScanIndexEntry(cacheKey, loadingEntry);
  return loading;
}

function getEmployeeOrderScanIndexMatches(supplyId, auth, settings, inputBarcode) {
  const normalizedInput = normalizeBarcode(inputBarcode);
  if (!normalizedInput) return null;
  const cacheKey = getEmployeeOrderScanIndexKey(supplyId, auth, settings);
  const cached = getCachedEmployeeOrderScanIndexEntry(cacheKey);
  if (!cached || cached.loading || !cached.barcodeMatches || !cached.missingBarcodeMatches) {
    return null;
  }
  const directMatches = cached.barcodeMatches.get(normalizedInput) || [];
  if (directMatches.length) {
    return {
      matches: [...directMatches],
      lookupFailed: false,
      ambiguous: new Set(directMatches.map(getOrderIdentityKey)).size > 1,
    };
  }
  const fallbackMatches = cached.missingBarcodeMatches.get(normalizedInput) || [];
  if (fallbackMatches.length) {
    return {
      matches: [...fallbackMatches],
      lookupFailed: false,
      ambiguous: new Set(fallbackMatches.map(getOrderIdentityKey)).size > 1,
    };
  }
  return null;
}

function getEmployeePickScanIndexKey(sessionOrSessionId) {
  const sessionId =
    typeof sessionOrSessionId === "object" && sessionOrSessionId !== null
      ? Number(sessionOrSessionId.id || 0)
      : Number(sessionOrSessionId || 0);
  return sessionId > 0 ? String(sessionId) : "";
}

function getCachedEmployeePickScanIndexEntry(cacheKey) {
  if (!cacheKey) return null;
  const entry = employeePickScanIndexCache.get(cacheKey);
  if (!entry) return null;
  if (entry.loading) return entry;
  if (Date.now() - Number(entry.ts || 0) >= EMPLOYEE_PICK_SCAN_INDEX_CACHE_MS) {
    employeePickScanIndexCache.delete(cacheKey);
    return null;
  }
  return entry;
}

function rememberEmployeePickScanIndexEntry(cacheKey, entry) {
  if (!cacheKey) return entry;
  if (employeePickScanIndexCache.has(cacheKey)) {
    employeePickScanIndexCache.delete(cacheKey);
  }
  while (employeePickScanIndexCache.size >= EMPLOYEE_PICK_SCAN_INDEX_MAX) {
    const oldestKey = employeePickScanIndexCache.keys().next().value;
    if (!oldestKey) break;
    employeePickScanIndexCache.delete(oldestKey);
  }
  employeePickScanIndexCache.set(cacheKey, entry);
  return entry;
}

function invalidateEmployeePickScanIndex(sessionOrSessionId) {
  const cacheKey = getEmployeePickScanIndexKey(sessionOrSessionId);
  if (!cacheKey) return;
  employeePickScanIndexCache.delete(cacheKey);
}

function getCachedEmployeePickItems(sessionOrSessionId) {
  const cacheKey = getEmployeePickScanIndexKey(sessionOrSessionId);
  const cached = getCachedEmployeePickScanIndexEntry(cacheKey);
  if (!cached || cached.loading || !cached.itemsById) return null;
  const items = Array.from(cached.itemsById.values()).filter(Boolean);
  return items.length ? sortEmployeePickRows(items) : null;
}

function extractMsMatchProductIds(status, article = null) {
  const matches = Array.isArray(status?.matches) ? status.matches : [];
  const normalizedArticle = normalizeArticleKey(article);
  const filteredMatches = normalizedArticle
    ? (() => {
        const exact = matches.filter((entry) => normalizeArticleKey(entry?.article) === normalizedArticle);
        return exact.length ? exact : matches;
      })()
    : matches;
  return Array.from(
    new Set(
      filteredMatches
        .map((entry) => Number(entry?.id))
        .filter((value) => Number.isInteger(value) && value > 0)
    )
  );
}

async function buildWbPickItemMovementHints(item, options = {}) {
  const warehouseOptions = getSkladWarehouseOptionsFromSettings(options);
  const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0] || null;
  let productIds = extractMsMatchProductIds(options?.scanLookup, item?.article);
  if (!productIds.length && item?.barcode) {
    try {
      const status = await getMsProductStatusByBarcode(item.barcode, {
        warehouseId: warehouseOptions.warehouseId,
        storeName: warehouseOptions.storeName,
        force: options?.force === true,
      });
      productIds = extractMsMatchProductIds(status, item?.article);
    } catch {}
  }
  if (!productIds.length && item?.article) {
    try {
      const status = await getMsProductStatusByArticle(item.article, {
        warehouseId: warehouseOptions.warehouseId,
        storeName: warehouseOptions.storeName,
        force: options?.force === true,
      });
      productIds = extractMsMatchProductIds(status, item?.article);
    } catch {}
  }
  return {
    productIds,
    preferredShelfId: firstCell?.id || null,
  };
}

function getEmployeePickScanMovementHints(sessionOrSessionId, item) {
  const itemId = Number(item?.id || 0);
  if (!Number.isFinite(itemId) || itemId <= 0) return null;
  const cacheKey = getEmployeePickScanIndexKey(sessionOrSessionId);
  const cached = getCachedEmployeePickScanIndexEntry(cacheKey);
  if (!cached || cached.loading || !cached.movementHintsByItemId) return null;
  return cached.movementHintsByItemId.get(itemId) || null;
}

function clearEmployeePickWarmCaches() {
  scanLookupCache.clear();
  acceptedOrderBarcodesCache.clear();
  employeeOrderScanIndexCache.clear();
  employeePickScanIndexCache.clear();
  clearMsCaches();
  clearSkladFbsCaches();
}

function isPendingEmployeePickItem(row) {
  return Number(row?.picked_qty || row?.pickedQty || 0) < Number(row?.required_qty || row?.requiredQty || 0);
}

function getEmployeePickScanIndexMatches(sessionOrSessionId, inputBarcodes = [], lookupNames = []) {
  const barcodes = buildAcceptedScanBarcodes(Array.isArray(inputBarcodes) ? inputBarcodes : [inputBarcodes]);
  const lookupNameKeys = Array.from(
    new Set(
      (Array.isArray(lookupNames) ? lookupNames : [lookupNames])
        .map((name) => normalizeLookupProductName(name))
        .filter(Boolean)
    )
  );
  if (!barcodes.length && !lookupNameKeys.length) return null;
  const cacheKey = getEmployeePickScanIndexKey(sessionOrSessionId);
  const cached = getCachedEmployeePickScanIndexEntry(cacheKey);
  if (!cached || cached.loading || (!cached.acceptedMatches && !cached.productNameMatches)) {
    return null;
  }
  const matches = [];
  const seenIds = new Set();
  const itemKeys = new Set();
  for (const barcode of barcodes) {
    const current = cached.acceptedMatches.get(normalizeBarcode(barcode)) || [];
    for (const item of current) {
      const itemId = Number(item?.id || 0);
      if (!Number.isFinite(itemId) || itemId <= 0 || seenIds.has(itemId) || !isPendingEmployeePickItem(item)) continue;
      seenIds.add(itemId);
      matches.push(item);
      itemKeys.add(item.item_key || String(itemId));
    }
  }
  if (cached.productNameMatches && lookupNameKeys.length) {
    for (const nameKey of lookupNameKeys) {
      const current = cached.productNameMatches.get(nameKey) || [];
      for (const item of current) {
        const itemId = Number(item?.id || 0);
        if (!Number.isFinite(itemId) || itemId <= 0 || seenIds.has(itemId) || !isPendingEmployeePickItem(item)) continue;
        seenIds.add(itemId);
        matches.push(item);
        itemKeys.add(item.item_key || String(itemId));
      }
    }
  }
  if (!matches.length) return null;
  return {
    matches,
    ambiguous: itemKeys.size > 1,
  };
}

function syncEmployeePickScanIndexItem(sessionOrSessionId, item) {
  const itemId = Number(item?.id || 0);
  if (!Number.isFinite(itemId) || itemId <= 0) return;
  const cacheKey = getEmployeePickScanIndexKey(sessionOrSessionId);
  const cached = getCachedEmployeePickScanIndexEntry(cacheKey);
  if (!cached || cached.loading || !cached.itemsById) return;
  const existing = cached.itemsById.get(itemId);
  if (!existing) {
    invalidateEmployeePickScanIndex(sessionOrSessionId);
    return;
  }
  Object.assign(existing, item);
  cached.ts = Date.now();
  rememberEmployeePickScanIndexEntry(cacheKey, cached);
}

async function buildEmployeePickScanIndex(session, sourceItems = null, options = {}) {
  const cacheKey = getEmployeePickScanIndexKey(session);
  const itemsById = new Map();
  const acceptedMatches = new Map();
  const productNameMatches = new Map();
  const movementHintsByItemId = new Map();
  if (!cacheKey) {
    return { ts: Date.now(), acceptedMatches, productNameMatches, itemsById, movementHintsByItemId, loading: null };
  }
  const force = options?.force === true;
  const sourceList = Array.isArray(sourceItems) ? sourceItems : await listEmployeePickItemsForSession(Number(session.id));
  const pendingSourceItems = sourceList.filter((item) => isPendingEmployeePickItem(item));
  const sourceAlreadyHasCells = pendingSourceItems.every((item) => Array.isArray(item?.ms_cells));
  const items = sourceAlreadyHasCells
    ? sortEmployeePickRows(pendingSourceItems)
    : await enrichEmployeePickItemsWithMsCells(pendingSourceItems, options);
  if (!items.length) {
    return { ts: Date.now(), acceptedMatches, productNameMatches, itemsById, movementHintsByItemId, loading: null };
  }

  const sourceEntries = new Map();
  for (const item of items) {
    const itemId = Number(item?.id || 0);
    if (Number.isFinite(itemId) && itemId > 0) {
      itemsById.set(itemId, item);
    }
    const sourceKey = normalizeBarcode(item?.barcode) || `item:${itemId || 0}`;
    if (!sourceEntries.has(sourceKey)) {
      sourceEntries.set(sourceKey, { item, accepted: { barcodes: [] } });
    }
  }

  await mapWithConcurrency([...sourceEntries.entries()], EMPLOYEE_PICK_SCAN_INDEX_CONCURRENCY, async ([sourceKey, entry]) => {
    const scanLookup = await getScanLookupBarcodes(entry.item?.barcode, force ? { force: true } : {});
    const movementHints = await buildWbPickItemMovementHints(entry.item, { ...options, force, scanLookup });
    sourceEntries.set(sourceKey, {
      ...entry,
      accepted: { barcodes: scanLookup?.barcodes || [] },
      lookupNameKeys: buildLookupProductNameKeys(scanLookup?.matches || []),
      movementHints,
    });
    return scanLookup;
  });

  for (const item of items) {
    const itemId = Number(item?.id || 0);
    const sourceKey = normalizeBarcode(item?.barcode) || `item:${itemId || 0}`;
    const entry = sourceEntries.get(sourceKey);
    for (const barcode of buildAcceptedScanBarcodes([item?.barcode, ...((entry?.accepted?.barcodes || []))])) {
      if (!acceptedMatches.has(barcode)) {
        acceptedMatches.set(barcode, []);
      }
      acceptedMatches.get(barcode).push(item);
    }
    for (const nameKey of Array.isArray(entry?.lookupNameKeys) ? entry.lookupNameKeys : []) {
      if (!productNameMatches.has(nameKey)) {
        productNameMatches.set(nameKey, []);
      }
      productNameMatches.get(nameKey).push(item);
    }
    if (Number.isFinite(itemId) && itemId > 0) {
      movementHintsByItemId.set(itemId, entry?.movementHints || { productIds: [], preferredShelfId: null });
    }
  }

  return {
    ts: Date.now(),
    acceptedMatches,
    productNameMatches,
    itemsById,
    movementHintsByItemId,
    loading: null,
  };
}

function ensureEmployeePickScanIndexWarm(session, sourceItems = null, options = {}) {
  const cacheKey = getEmployeePickScanIndexKey(session);
  if (!cacheKey) return null;
  const force = options?.force === true;
  const cached = force ? null : getCachedEmployeePickScanIndexEntry(cacheKey);
  if (cached?.loading) return cached.loading;
  if (cached?.acceptedMatches && !force) {
    return Promise.resolve(cached);
  }
  const baseEntry = force
    ? { ts: 0, acceptedMatches: null, productNameMatches: null, itemsById: null, movementHintsByItemId: null, loading: null }
    : cached || { ts: 0, acceptedMatches: null, productNameMatches: null, itemsById: null, movementHintsByItemId: null, loading: null };
  const loading = buildEmployeePickScanIndex(session, sourceItems, { ...options, force })
    .then((built) => {
      const readyEntry = {
        ts: Date.now(),
        acceptedMatches: built?.acceptedMatches || new Map(),
        productNameMatches: built?.productNameMatches || new Map(),
        itemsById: built?.itemsById || new Map(),
        movementHintsByItemId: built?.movementHintsByItemId || new Map(),
        loading: null,
      };
      rememberEmployeePickScanIndexEntry(cacheKey, readyEntry);
      return readyEntry;
    })
    .catch(() => {
      const fallbackEntry = {
        ...baseEntry,
        loading: null,
      };
      rememberEmployeePickScanIndexEntry(cacheKey, fallbackEntry);
      return fallbackEntry;
    });
  rememberEmployeePickScanIndexEntry(cacheKey, { ...baseEntry, loading });
  return loading;
}

async function listEmployeeOrderMatchesByBarcode(supplyId, auth, settings, store, inputBarcode, options = {}) {
  const isSplit = settings?.access_mode === "selected_split";
  const forceFresh = options?.forceFresh === true;
  const normalizedInput = normalizeBarcode(inputBarcode);
  if (!normalizedInput) {
    return { matches: [], lookupFailed: false, ambiguous: false, multipleScanProducts: false, scanProducts: [] };
  }

  const cachedIndexMatch = forceFresh ? null : getEmployeeOrderScanIndexMatches(supplyId, auth, settings, normalizedInput);
  if (cachedIndexMatch?.matches?.length) {
    return { ...cachedIndexMatch, multipleScanProducts: false, scanProducts: [] };
  }

  const baseParams = [supplyId];
  let where = "supply_id = $1 and collected_at is null";
  if (isSplit) {
    baseParams.push(auth.id);
    where += " and assigned_user_id = $2";
  }
  const selectOrdersSql = `select wb_order_id, order_created_at, article, barcode, product_name, nm_id,
          quantity, sticker_url, sticker_barcode,
          scan_passed_at, scan_barcode,
          label_scan_passed_at, label_scan_barcode
   from supply_orders
   where ${where}`;
  const orderBySql = " order by order_created_at asc, wb_order_id asc";

  let lookupFailed = false;
  let multipleScanProducts = false;
  const scanProducts = [];
  const scanProductKeys = new Set();
  const matches = [];
  const productKeys = new Set();
  const matchedOrderIds = new Set();

  const rememberScanProducts = (scanLookup) => {
    if (!scanLookup?.multiple) return;
    multipleScanProducts = true;
    for (const item of Array.isArray(scanLookup.matches) ? scanLookup.matches : []) {
      const key = JSON.stringify({
        article: normalizeArticleKey(item?.article || item?.code || null),
        name: String(item?.name || item?.product_name || "").trim().toLowerCase(),
        barcodes: buildAcceptedScanBarcodes(item?.barcodes || []),
      });
      if (!key || scanProductKeys.has(key)) continue;
      scanProductKeys.add(key);
      scanProducts.push(item);
    }
  };

  const appendMatches = (orders) => {
    for (const order of Array.isArray(orders) ? orders : []) {
      const orderId = Number(order?.wb_order_id);
      if (!Number.isFinite(orderId) || matchedOrderIds.has(orderId)) continue;
      matchedOrderIds.add(orderId);
      matches.push(order);
      productKeys.add(getOrderIdentityKey(order));
    }
  };

  const loadByCandidateBarcodes = async (scanLookup) => {
    const candidateBarcodes = scanLookup?.multiple
      ? [normalizedInput]
      : buildAcceptedScanBarcodes([normalizedInput, ...((scanLookup?.barcodes || []))]);
    if (!candidateBarcodes.length) return [];
    const params = [...baseParams, candidateBarcodes];
    const rows = await dbQuery(
      `${selectOrdersSql}
       and barcode = any($${params.length}::text[])${orderBySql}`,
      params
    );
    return rows.rows || [];
  };

  const initialLookup = await getScanLookupBarcodes(normalizedInput, forceFresh ? { force: true } : {});
  if (initialLookup.lookupFailed) lookupFailed = true;
  rememberScanProducts(initialLookup);
  appendMatches(await loadByCandidateBarcodes(initialLookup));

  if (!matches.length && !initialLookup.lookupFailed && !forceFresh) {
    const forcedLookup = await getScanLookupBarcodes(normalizedInput, { force: true });
    if (forcedLookup.lookupFailed) lookupFailed = true;
    rememberScanProducts(forcedLookup);
    appendMatches(await loadByCandidateBarcodes(forcedLookup));
  }

  if (!matches.length) {
    const missingResult = await dbQuery(
      `${selectOrdersSql}
       and (barcode is null or barcode = '')${orderBySql}`,
      baseParams
    );
    const missingOrders = missingResult.rows || [];
    if (missingOrders.length) {
      const barcodeCache = new Map();
      const articleBarcodeCache = new Map();
      for (const order of missingOrders) {
        const initial = await getAcceptedScanBarcodesForOrder(order, {
          barcodeCache,
          articleCache: articleBarcodeCache,
          force: forceFresh,
        });
        if (initial.lookupFailed) lookupFailed = true;
        if (initial.barcodes.includes(normalizedInput)) {
          appendMatches([order]);
          continue;
        }
        if (!forceFresh) {
          const fresh = await getAcceptedScanBarcodesForOrder(order, {
            force: true,
            barcodeCache,
            articleCache: articleBarcodeCache,
          });
          if (fresh.lookupFailed) lookupFailed = true;
          if (fresh.barcodes.includes(normalizedInput)) {
            appendMatches([order]);
          }
        }
      }
    }
  }

  return {
    matches,
    lookupFailed,
    ambiguous: productKeys.size > 1,
    multipleScanProducts,
    scanProducts,
  };
}

async function findFirstEmployeeOrderByBarcode(supplyId, auth, settings, store, inputBarcode, options = {}) {
  const result = await listEmployeeOrderMatchesByBarcode(supplyId, auth, settings, store, inputBarcode, options);
  return {
    order: result.matches[0] || null,
    matches: result.matches,
    lookupFailed: result.lookupFailed,
    ambiguous: result.ambiguous,
    multipleScanProducts: result.multipleScanProducts,
    scanProducts: result.scanProducts,
    warning: result.ambiguous
      ? buildAmbiguousScanWarning(inputBarcode, result.matches, {
          multipleScanProducts: result.multipleScanProducts,
          scanProducts: result.scanProducts,
        })
      : "",
  };
}

async function findEmployeePickItemMatchesByBarcode(session, inputBarcode, options = {}) {
  const forceFresh = options?.forceFresh === true;
  const normalizedInput = normalizeBarcode(inputBarcode);
  if (!normalizedInput || !session?.id) {
    return { matches: [], lookupFailed: false, ambiguous: false, multipleScanProducts: false, scanProducts: [] };
  }

  const sourceItems = Array.isArray(session?.pick_items) ? session.pick_items : await listEmployeePickItemsForSession(session.id);
  const items = sourceItems.filter(
    (item) => Number(item.picked_qty || 0) < Number(item.required_qty || 0)
  );
  if (!items.length) {
    return { matches: [], lookupFailed: false, ambiguous: false, multipleScanProducts: false, scanProducts: [] };
  }
  if (forceFresh) {
    try {
      await ensureEmployeePickScanIndexWarm(session, sourceItems, { ...options, force: true });
    } catch {}
  } else {
    const warmingEntry = getCachedEmployeePickScanIndexEntry(getEmployeePickScanIndexKey(session));
    if (warmingEntry?.loading) {
      try {
        await warmingEntry.loading;
      } catch {}
    }
  }

  const directIndexMatch = getEmployeePickScanIndexMatches(session, [normalizedInput]);
  if (directIndexMatch?.matches?.length) {
    const orderedDirectIndexMatches = await sortEmployeePickMatchRows(directIndexMatch.matches, options);
    return {
      matches: orderedDirectIndexMatches,
      lookupFailed: false,
      ambiguous: directIndexMatch.ambiguous,
      multipleScanProducts: false,
    };
  }

  const directMatches = items.filter((item) => normalizeBarcode(item.barcode) === normalizedInput);
  if (directMatches.length) {
    const orderedDirectMatches = await sortEmployeePickMatchRows(directMatches, options);
    const directKeys = new Set(orderedDirectMatches.map((item) => item.item_key || String(item.id)));
    return {
      matches: orderedDirectMatches,
      lookupFailed: false,
      ambiguous: directKeys.size > 1,
      multipleScanProducts: false,
    };
  }

  const barcodeCache = new Map();
  let lookupFailed = false;
  let multipleScanProducts = false;
  const scanProducts = [];
  const scanProductKeys = new Set();

  const tryMatchWithLookup = async (scanLookup, forceAccepted = false) => {
    if (scanLookup.lookupFailed) lookupFailed = true;
    if (scanLookup.multiple) {
      multipleScanProducts = true;
      for (const item of Array.isArray(scanLookup.matches) ? scanLookup.matches : []) {
        const key = JSON.stringify({
          article: normalizeArticleKey(item?.article || item?.code || null),
          name: String(item?.name || item?.product_name || "").trim().toLowerCase(),
          barcodes: buildAcceptedScanBarcodes(item?.barcodes || []),
        });
        if (!key || scanProductKeys.has(key)) continue;
        scanProductKeys.add(key);
        scanProducts.push(item);
      }
    }
    const lookupBarcodes = new Set(
      scanLookup.multiple
        ? [normalizedInput]
        : buildAcceptedScanBarcodes([
            normalizedInput,
            ...(scanLookup.barcodes || []),
            ...((Array.isArray(scanLookup.matches) ? scanLookup.matches : []).flatMap((match) => match?.barcodes || [])),
          ])
    );
    const indexedMatches = getEmployeePickScanIndexMatches(
      session,
      [...lookupBarcodes],
      buildLookupProductNameKeys(scanLookup?.matches || [])
    );
    if (indexedMatches?.matches?.length) {
      return {
        matches: await sortEmployeePickMatchRows(indexedMatches.matches, options),
        ambiguous: indexedMatches.ambiguous,
      };
    }
    const msMatches = items.filter((item) => {
      const barcode = normalizeBarcode(item.barcode);
      return barcode && lookupBarcodes.has(barcode);
    });
    if (msMatches.length) {
      const orderedMsMatches = await sortEmployeePickMatchRows(msMatches, options);
      const matchedKeys = new Set(orderedMsMatches.map((item) => item.item_key || String(item.id)));
      return {
        matches: orderedMsMatches,
        ambiguous: matchedKeys.size > 1,
      };
    }

    const scanCandidateBarcodes = scanLookup.multiple
      ? [normalizedInput]
      : buildAcceptedScanBarcodes([normalizedInput, ...(scanLookup.barcodes || [])]);
    const itemChecks = await Promise.all(
      items.map(async (item) => {
        const accepted = await getAcceptedScanBarcodesForOrder(
          {
            barcode: item.barcode,
          },
          { barcodeCache, force: forceAccepted }
        );
        return { item, accepted };
      })
    );
    const matches = [];
    const matchedKeys = new Set();
    for (const { item, accepted } of itemChecks) {
      if (accepted.lookupFailed) lookupFailed = true;
      const candidateBarcodes = buildAcceptedScanBarcodes([
        item.barcode,
        ...(accepted.barcodes || []),
      ]);
      if (!candidateBarcodes.length) continue;
      const matched = candidateBarcodes.some((barcode) => scanCandidateBarcodes.includes(barcode));
      if (!matched) continue;
      matches.push(item);
      matchedKeys.add(item.item_key || String(item.id));
    }
    return {
      matches: await sortEmployeePickMatchRows(matches, options),
      ambiguous: matchedKeys.size > 1,
    };
  };

  const initialLookup = await getScanLookupBarcodes(normalizedInput, forceFresh ? { force: true } : {});
  const initialMatch = await tryMatchWithLookup(initialLookup, false);
  if (initialMatch.matches.length) {
    return {
      matches: initialMatch.matches,
      lookupFailed,
      ambiguous: initialMatch.ambiguous,
      multipleScanProducts,
      scanProducts,
    };
  }

  if (!initialLookup.lookupFailed && !forceFresh) {
    const forcedLookup = await getScanLookupBarcodes(normalizedInput, { force: true });
    const forcedMatch = await tryMatchWithLookup(forcedLookup, true);
    if (forcedMatch.matches.length) {
      return {
        matches: forcedMatch.matches,
        lookupFailed,
        ambiguous: forcedMatch.ambiguous,
        multipleScanProducts,
        scanProducts,
      };
    }
  }

  return {
    matches: [],
    lookupFailed,
    ambiguous: false,
    multipleScanProducts,
    scanProducts,
  };
}

async function findEmployeePickItemsForIssue(session, inputBarcode) {
  const normalizedInput = normalizeBarcode(inputBarcode);
  if (!normalizedInput || !session?.id) return [];
  const items = Array.isArray(session?.pick_items) ? session.pick_items : await listEmployeePickItemsForSession(session.id);
  const direct = items.filter((item) => normalizeBarcode(item.barcode) === normalizedInput);
  if (direct.length) return await sortEmployeePickMatchRows(direct);
  const barcodeCache = new Map();

  const tryFind = async (scanLookup, forceAccepted = false) => {
    const lookupBarcodes = new Set(
      scanLookup.multiple
        ? [normalizedInput]
        : buildAcceptedScanBarcodes([
            normalizedInput,
            ...(scanLookup.barcodes || []),
            ...((Array.isArray(scanLookup.matches) ? scanLookup.matches : []).flatMap((match) => match?.barcodes || [])),
          ])
    );
    const msMatches = items.filter((item) => {
      const barcode = normalizeBarcode(item.barcode);
      return barcode && lookupBarcodes.has(barcode);
    });
    if (msMatches.length) {
      return await sortEmployeePickMatchRows(msMatches);
    }
    const checks = await Promise.all(
      items.map(async (item) => {
        const accepted = await getAcceptedScanBarcodesForOrder(
          {
            barcode: item.barcode,
          },
          { barcodeCache, force: forceAccepted }
        );
        return { item, accepted };
      })
    );
    const scanCandidateBarcodes = scanLookup.multiple
      ? [normalizedInput]
      : buildAcceptedScanBarcodes([normalizedInput, ...(scanLookup.barcodes || [])]);
    const matchedItems = checks
      .filter(({ accepted }) => {
        const candidateBarcodes = buildAcceptedScanBarcodes([...(accepted?.barcodes || [])]);
        return candidateBarcodes.some((barcode) => scanCandidateBarcodes.includes(barcode));
      })
      .map(({ item }) => item);
    return await sortEmployeePickMatchRows(matchedItems);
  };

  const initialLookup = await getScanLookupBarcodes(normalizedInput);
  const initialMatches = await tryFind(initialLookup, false);
  if (initialMatches.length || initialLookup.lookupFailed) {
    return initialMatches;
  }
  const forcedLookup = await getScanLookupBarcodes(normalizedInput, { force: true });
  return await tryFind(forcedLookup, true);
}

async function startLabelsJob(supplyId, supplyName, force = false) {
  if (labelJobs.has(supplyId)) return labelJobs.get(supplyId);
  const job = (async () => {
    try {
      const store = await getSupplyStore(supplyId, supplyName);
      await ensureSupplySettings(supplyId, supplyName, store);
      await ensureSupplySnapshot(supplyId, supplyName);
      const settings = await getSupplySettings(supplyId);
      const prefix = settings?.labels_s3_prefix || buildLabelsPrefix(supplyId, settings?.supply_name || supplyName);
      if (!settings?.labels_s3_prefix || force) {
        await dbQuery(
          "update supply_settings set labels_s3_prefix = $2, updated_at = now() where supply_id = $1",
          [supplyId, prefix]
        );
      }

      const ordersResult = await dbQuery(
        "select wb_order_id, sticker_url from supply_orders where supply_id = $1 order by wb_order_id",
        [supplyId]
      );
      const allOrders = ordersResult.rows || [];
      const total = allOrders.length;
      const alreadyLoaded = allOrders.filter((row) => row.sticker_url).length;
      const toFetch = force
        ? allOrders
        : allOrders.filter((row) => !row.sticker_url);

      await dbQuery(
        `update supply_settings set labels_status = 'loading', labels_total = $2,
         labels_loaded = $3, labels_error = null, labels_started_at = now(),
         labels_finished_at = null, updated_at = now() where supply_id = $1`,
        [supplyId, total, force ? 0 : alreadyLoaded]
      );
      notifySupplyUpdate(supplyId, true);

      let loaded = force ? 0 : alreadyLoaded;
      let errors = 0;

      for (let i = 0; i < toFetch.length; i += LABEL_BATCH_SIZE) {
        const batch = toFetch.slice(i, i + LABEL_BATCH_SIZE).map((row) => Number(row.wb_order_id));
        if (!batch.length) continue;
        let data = null;
        try {
          data = await wbRequest(
            store,
            `/api/v3/orders/stickers?type=${LABEL_TYPE}&width=${LABEL_WIDTH}&height=${LABEL_HEIGHT}`,
            {
              method: "POST",
              body: { orders: batch },
            }
          );
        } catch (batchError) {
          const batchErrorText = normalizeText(batchError?.message || "") || "Не удалось получить этикетки из WB";
          errors += batch.length;
          for (const orderId of batch) {
            await dbQuery(
              `update supply_orders
               set sticker_error = $3,
                   updated_at = now()
               where supply_id = $1 and wb_order_id = $2`,
              [supplyId, orderId, batchErrorText]
            );
          }
          notifySupplyUpdate(supplyId, true);
          continue;
        }
        const stickers = Array.isArray(data?.stickers) ? data.stickers : [];
        const byId = new Map(stickers.map((item) => [Number(item.orderId), item]));

        for (const orderId of batch) {
          const sticker = byId.get(Number(orderId));
          if (!sticker?.file) {
            errors += 1;
            await dbQuery(
              `update supply_orders set sticker_error = $3, updated_at = now()
               where supply_id = $1 and wb_order_id = $2`,
              [supplyId, orderId, "Sticker not returned by WB"]
            );
            continue;
          }
          try {
            const pdfBytes = await pngBase64ToPdf(sticker.file);
            const key = buildS3Key(prefix, orderId);
            const storedKey = await uploadPdfToS3(key, pdfBytes);
            const url = buildS3Url(storedKey);
            await dbQuery(
              `update supply_orders set sticker_url = $3, sticker_format = 'pdf',
               sticker_s3_key = $4, sticker_barcode = $5, sticker_loaded_at = now(),
               sticker_error = null, updated_at = now()
               where supply_id = $1 and wb_order_id = $2`,
              [supplyId, orderId, url, storedKey, sticker.barcode || null]
            );
            loaded += 1;
          } catch (err) {
            errors += 1;
            await dbQuery(
              `update supply_orders set sticker_error = $3, updated_at = now()
               where supply_id = $1 and wb_order_id = $2`,
              [supplyId, orderId, err?.message || "Sticker upload failed"]
            );
          }
          await dbQuery(
            "update supply_settings set labels_loaded = $2, updated_at = now() where supply_id = $1",
            [supplyId, loaded]
          );
          notifySupplyUpdate(supplyId);
        }
      }

      const status = errors > 0 ? "error" : "ready";
      const errorText = errors > 0 ? `Ошибок: ${errors}` : null;
      await dbQuery(
        `update supply_settings set labels_status = $2, labels_error = $3,
         labels_finished_at = now(), updated_at = now() where supply_id = $1`,
        [supplyId, status, errorText]
      );
      notifySupplyUpdate(supplyId, true);
      return { ok: true, status, errors, loaded };
    } catch (err) {
      const errorText = normalizeText(err?.message || "") || "Не удалось загрузить этикетки";
      console.error("Labels job failed", { supplyId, error: errorText });
      try {
        await dbQuery(
          `update supply_settings set labels_status = 'error', labels_error = $2,
           labels_finished_at = now(), updated_at = now() where supply_id = $1`,
          [supplyId, errorText]
        );
        notifySupplyUpdate(supplyId, true);
      } catch (updateErr) {
        console.error("Failed to persist labels job error", {
          supplyId,
          error: updateErr?.message || String(updateErr || ""),
        });
      }
      return { ok: false, status: "error", error: errorText };
    }
  })();

  labelJobs.set(supplyId, job);
  job.finally(() => labelJobs.delete(supplyId));
  return job;
}

function buildSupplyTrbxPdfKey(prefix, trbxId) {
  const safeTrbxId = String(trbxId || "trbx")
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `${prefix}/trbx/${safeTrbxId || "trbx"}.pdf`;
}

function buildSupplyBarcodePdfKey(prefix, supplyId) {
  const safeSupplyId = String(supplyId || "supply")
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `${prefix}/supply/${safeSupplyId || "supply"}.pdf`;
}

function toIsoOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function normalizeSupplyTrbxStatus(summary) {
  if (summary?.error) return "error";
  if (summary?.done) return "delivered";
  if (Number(summary?.boxCount || 0) > 0) return "boxes_ready";
  return "idle";
}

function toSupplyTrbxPayloadRow(row) {
  if (!row) return null;
  return {
    id: row.trbx_id,
    ordersCount: Number(row.orders_count || 0),
    barcode: row.wb_barcode || null,
    pdfUrl: row.pdf_url || null,
    pdfFormat: row.pdf_format || null,
    qrLoadedAt: row.qr_loaded_at || null,
    qrError: row.qr_error || null,
    createdAt: row.created_at || null,
    syncedAt: row.synced_at || null,
    updatedAt: row.updated_at || null,
  };
}

async function fetchSupplyMeta(store, supplyId) {
  return await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}`);
}

async function fetchSupplyTrbxes(store, supplyId) {
  const data = await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}/trbx`);
  return Array.isArray(data?.trbxes) ? data.trbxes : [];
}

async function createSupplyTrbxes(store, supplyId, amount) {
  return await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}/trbx`, {
    method: "POST",
    body: { amount },
  });
}

async function fetchSupplyTrbxStickers(store, supplyId, trbxIds) {
  return await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}/trbx/stickers?type=png`, {
    method: "POST",
    body: { trbxIds },
  });
}

async function fetchSupplyBarcode(store, supplyId) {
  return await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}/barcode?type=png`);
}

async function transferSupplyToDelivery(store, supplyId) {
  return await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}/deliver`, {
    method: "PATCH",
  });
}

async function upsertSupplyTrbxRows(supplyId, trbxes) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId) return;
  const liveIds = [];
  for (const trbx of trbxes) {
    const trbxId = normalizeText(trbx?.id);
    if (!trbxId) continue;
    liveIds.push(trbxId);
    const ordersCount = Array.isArray(trbx?.orders) ? trbx.orders.length : 0;
    await dbQuery(
      `insert into supply_trbxes (supply_id, trbx_id, orders_count, synced_at, updated_at)
       values ($1, $2, $3, now(), now())
       on conflict (supply_id, trbx_id)
       do update set orders_count = excluded.orders_count,
                     synced_at = now(),
                     updated_at = now()`,
      [normalizedSupplyId, trbxId, ordersCount]
    );
  }
  if (liveIds.length) {
    await dbQuery(
      "delete from supply_trbxes where supply_id = $1 and not (trbx_id = any($2::text[]))",
      [normalizedSupplyId, liveIds]
    );
  } else {
    await dbQuery("delete from supply_trbxes where supply_id = $1", [normalizedSupplyId]);
  }
}

async function listSupplyTrbxRows(supplyId) {
  const result = await dbQuery(
    `select trbx_id, orders_count, wb_barcode, pdf_url, pdf_s3_key, pdf_format,
            qr_loaded_at, qr_error, created_at, synced_at, updated_at
     from supply_trbxes
     where supply_id = $1
     order by created_at asc, trbx_id asc`,
    [supplyId]
  );
  return result.rows || [];
}

async function ensureSupplyBarcodePdf(store, supplyId, prefix, settings, { force = false } = {}) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId || !store?.token) return;
  const currentSettings = settings || (await getSupplySettings(normalizedSupplyId));
  if (!force && currentSettings?.supply_qr_pdf_url) return;
  try {
    const payload = await fetchSupplyBarcode(store, normalizedSupplyId);
    if (!payload?.file) {
      throw new Error("WB did not return supply QR");
    }
    const normalizedBarcode = normalizeText(payload.barcode);
    const persistedBarcode = /^WB-GI/i.test(normalizedBarcode || "") ? normalizedBarcode : normalizedSupplyId;
    const pdfBytes = await pngBase64ToPdf(payload.file);
    const key = buildSupplyBarcodePdfKey(prefix, normalizedSupplyId);
    const storedKey = await uploadPdfToS3(key, pdfBytes);
    await dbQuery(
      `update supply_settings
       set supply_barcode = $2,
           supply_qr_pdf_url = $3,
           supply_qr_pdf_s3_key = $4,
           supply_qr_pdf_format = 'pdf',
           supply_qr_loaded_at = now(),
           supply_qr_error = null,
           updated_at = now()
       where supply_id = $1`,
      [normalizedSupplyId, persistedBarcode, buildS3Url(storedKey), storedKey]
    );
  } catch (err) {
    await dbQuery(
      `update supply_settings
       set supply_qr_error = $2,
           updated_at = now()
       where supply_id = $1`,
      [normalizedSupplyId, err?.message || "Supply QR upload failed"]
    );
  }
}

async function ensureSupplyTrbxPdfs(store, supplyId, prefix, rows, { force = false } = {}) {
  for (const row of rows) {
    const trbxId = normalizeText(row?.trbx_id);
    if (!trbxId) continue;
    if (!force && row?.pdf_url) continue;
    try {
      const data = await fetchSupplyTrbxStickers(store, supplyId, [trbxId]);
      const sticker = Array.isArray(data?.stickers) ? data.stickers[0] : null;
      if (!sticker?.file) {
        await dbQuery(
          `update supply_trbxes
           set qr_error = $3, updated_at = now()
           where supply_id = $1 and trbx_id = $2`,
          [supplyId, trbxId, "WB did not return QR sticker"]
        );
        continue;
      }
      const pdfBytes = await pngBase64ToPdf(sticker.file);
      const key = buildSupplyTrbxPdfKey(prefix, trbxId);
      const storedKey = await uploadPdfToS3(key, pdfBytes);
      await dbQuery(
        `update supply_trbxes
         set wb_barcode = $3,
             pdf_url = $4,
             pdf_s3_key = $5,
             pdf_format = 'pdf',
             qr_loaded_at = now(),
             qr_error = null,
             updated_at = now()
         where supply_id = $1 and trbx_id = $2`,
        [supplyId, trbxId, sticker.barcode || null, buildS3Url(storedKey), storedKey]
      );
    } catch (err) {
      await dbQuery(
        `update supply_trbxes
         set qr_error = $3, updated_at = now()
         where supply_id = $1 and trbx_id = $2`,
        [supplyId, trbxId, err?.message || "QR upload failed"]
      );
    }
  }
}

async function syncSupplyTrbxState(supplyId, supplyName = null, options = {}) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId) return null;
  if (supplyTrbxSyncJobs.has(normalizedSupplyId)) return supplyTrbxSyncJobs.get(normalizedSupplyId);
  const { loadMissingPdf = true, forceStickerRefresh = false } = options || {};
  const job = (async () => {
    const store = await getSupplyStore(normalizedSupplyId, supplyName);
    const ensured = await ensureSupplySettings(normalizedSupplyId, supplyName, store);
    const prefix =
      ensured?.labels_s3_prefix || buildLabelsPrefix(normalizedSupplyId, ensured?.supply_name || supplyName || normalizedSupplyId);
    if (!ensured?.labels_s3_prefix) {
      await dbQuery(
        "update supply_settings set labels_s3_prefix = $2, updated_at = now() where supply_id = $1",
        [normalizedSupplyId, prefix]
      );
    }
    try {
      const meta = await fetchSupplyMeta(store, normalizedSupplyId);
      const trbxes = await fetchSupplyTrbxes(store, normalizedSupplyId);
      await upsertSupplyTrbxRows(normalizedSupplyId, trbxes);
      const summary = {
        boxCount: trbxes.length,
        done: meta?.done === true,
        createdAt: toIsoOrNull(meta?.createdAt),
        closedAt: toIsoOrNull(meta?.closedAt),
        scanDt: toIsoOrNull(meta?.scanDt),
        destinationOfficeId: Number(meta?.destinationOfficeId || 0) || null,
        cargoType: Number(meta?.cargoType || 0) || null,
        error: null,
      };
      await dbQuery(
        `update supply_settings
         set supply_name = coalesce($2, supply_name),
             trbx_status = $3,
             trbx_count = $4,
             trbx_last_synced_at = now(),
             trbx_error = null,
             wb_supply_done = $5,
             wb_supply_created_at = $6,
             wb_supply_closed_at = $7,
             wb_supply_scan_dt = $8,
             wb_destination_office_id = $9,
             wb_cargo_type = $10,
             updated_at = now()
         where supply_id = $1`,
        [
          normalizedSupplyId,
          normalizeText(meta?.name) || null,
          normalizeSupplyTrbxStatus(summary),
          summary.boxCount,
          summary.done,
          summary.createdAt,
          summary.closedAt,
          summary.scanDt,
          summary.destinationOfficeId,
          summary.cargoType,
        ]
      );
      if (loadMissingPdf) {
        const rows = await listSupplyTrbxRows(normalizedSupplyId);
        await ensureSupplyTrbxPdfs(store, normalizedSupplyId, prefix, rows, { force: forceStickerRefresh });
        if (summary.done) {
          const freshSettings = await getSupplySettings(normalizedSupplyId);
          await ensureSupplyBarcodePdf(store, normalizedSupplyId, prefix, freshSettings, {
            force: forceStickerRefresh,
          });
        }
      }
      notifySupplyUpdate(normalizedSupplyId, true);
      return await getSupplyTrbxState(normalizedSupplyId);
    } catch (err) {
      await dbQuery(
        `update supply_settings
         set trbx_status = 'error',
             trbx_last_synced_at = now(),
             trbx_error = $2,
             updated_at = now()
         where supply_id = $1`,
        [normalizedSupplyId, err?.message || "WB trbx sync failed"]
      );
      notifySupplyUpdate(normalizedSupplyId, true);
      return await getSupplyTrbxState(normalizedSupplyId);
    }
  })();
  supplyTrbxSyncJobs.set(normalizedSupplyId, job);
  job.finally(() => supplyTrbxSyncJobs.delete(normalizedSupplyId));
  return job;
}

async function getSupplyTrbxState(supplyId) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId) return null;
  const settings = await getSupplySettings(normalizedSupplyId);
  const rows = await listSupplyTrbxRows(normalizedSupplyId);
  const summary = {
    status: normalizeText(settings?.trbx_status) || normalizeSupplyTrbxStatus({
      done: settings?.wb_supply_done === true,
      boxCount: Number(settings?.trbx_count || rows.length || 0),
      error: settings?.trbx_error,
    }),
    boxCount: Number(settings?.trbx_count || rows.length || 0),
    lastSyncedAt: settings?.trbx_last_synced_at || null,
    error: settings?.trbx_error || null,
    done: settings?.wb_supply_done === true,
    createdAt: settings?.wb_supply_created_at || null,
    closedAt: settings?.wb_supply_closed_at || null,
    scanDt: settings?.wb_supply_scan_dt || null,
    destinationOfficeId: settings?.wb_destination_office_id != null ? Number(settings.wb_destination_office_id) : null,
    cargoType: settings?.wb_cargo_type != null ? Number(settings.wb_cargo_type) : null,
    canCreateBoxes: settings?.wb_supply_done !== true && Number(settings?.trbx_count || rows.length || 0) < 1,
    canDeliver: settings?.wb_supply_done !== true,
    canLoadSupplyQr: settings?.wb_supply_done === true,
    supplyBarcode: settings?.supply_barcode || null,
    supplyQrPdfUrl: settings?.supply_qr_pdf_url || null,
    supplyQrPdfFormat: settings?.supply_qr_pdf_format || null,
    supplyQrLoadedAt: settings?.supply_qr_loaded_at || null,
    supplyQrError: settings?.supply_qr_error || null,
    singleBoxMode: true,
  };
  return {
    supply: settings
      ? {
          id: settings.supply_id,
          name: settings.supply_name || null,
          storeId: settings.store_id || null,
          storeName: settings.store_name || null,
        }
      : { id: normalizedSupplyId, name: null, storeId: null, storeName: null },
    summary,
    boxes: rows.map(toSupplyTrbxPayloadRow).filter(Boolean),
  };
}

async function findUserById(id) {
  const result = await dbQuery(
    `select
       id,
       surname,
       name,
       role,
       position,
       password_hash,
       password_salt,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code
     from users
     where id = $1`,
    [id]
  );
  return result.rows?.[0] || null;
}

async function findManagedUserById(id) {
  const result = await dbQuery(
    `select
       id,
       surname,
       name,
       role,
       position,
       password_hash,
       password_salt,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code
     from users
     where id = $1
       and auth_source = $2`,
    [id, USER_AUTH_SOURCE_EXTERNAL]
  );
  return result.rows?.[0] || null;
}

async function findLocalLoginUsersByLogin(login) {
  const normalizedLogin = normalizeText(login);
  if (!normalizedLogin) return [];
  const result = await dbQuery(
    `select
       id,
       surname,
       name,
       role,
       position,
       password_hash,
       password_salt,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code
     from users
     where role = any($1::text[])
       and coalesce(auth_source, $2) = $2
       and (lower(surname) = lower($3) or lower(coalesce(external_login, '')) = lower($3))
     order by id`,
    [[ROLE_SUPER, ROLE_ADMIN, ROLE_EMPLOYEE], USER_AUTH_SOURCE_LOCAL, normalizedLogin]
  );
  return result.rows || [];
}

async function findSiteUserByExternalLogin(login) {
  const normalizedLogin = normalizeText(login);
  if (!normalizedLogin) return null;
  const result = await dbQuery(
    `select
       id,
       surname,
       name,
       role,
       position,
       password_hash,
       password_salt,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code
     from users
     where auth_source = $1
       and lower(external_login) = lower($2)
     order by id desc
     limit 1`,
    [USER_AUTH_SOURCE_EXTERNAL, normalizedLogin]
  );
  return result.rows?.[0] || null;
}

async function findSiteUserByExternalUserId(externalUserId) {
  const normalizedId = Number(externalUserId || 0);
  if (!Number.isFinite(normalizedId) || normalizedId < 1) return null;
  const result = await dbQuery(
    `select
       id,
       surname,
       name,
       role,
       position,
       password_hash,
       password_salt,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code
     from users
     where auth_source = $1
       and external_user_id = $2
     order by id desc
     limit 1`,
    [USER_AUTH_SOURCE_EXTERNAL, normalizedId]
  );
  return result.rows?.[0] || null;
}

async function listManageableUsers(roleFilter = null) {
  const params = [USER_AUTH_SOURCE_EXTERNAL];
  let query =
    `select
       id,
       surname,
       name,
       role,
       position,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code
     from users
     where auth_source = $1`;
  if (roleFilter) {
    params.push(roleFilter);
    query += ` and role = $${params.length}`;
  }
  query += " order by created_at desc, id desc";
  const result = await dbQuery(query, params);
  return result.rows || [];
}

async function listDirectoryUsersForCurrentSite() {
  const [externalUsers, existingRows] = await Promise.all([
    listActiveExternalUsers(),
    dbQuery(
      `select external_user_id
       from users
       where auth_source = $1
         and external_user_id is not null`,
      [USER_AUTH_SOURCE_EXTERNAL]
    ),
  ]);
  const existingIds = new Set((existingRows.rows || []).map((row) => Number(row.external_user_id || 0)));
  return externalUsers.map((user) => ({
    externalUserId: user.id,
    externalEmployeeId: user.employeeId,
    login: user.login,
    fullName: user.fullName,
    externalRole: user.externalRole,
    status: user.status,
    alreadyAdded: existingIds.has(Number(user.id)),
  }));
}

async function syncLocalExternalUser(localUserId, externalUser) {
  if (!localUserId || !externalUser) return await findUserById(localUserId);
  const profile = splitDisplayName(externalUser.fullName, externalUser.login);
  const result = await dbQuery(
    `update users
     set surname = $1,
         name = $2,
         external_login = $3,
         external_full_name = $4,
         external_employee_id = $5,
         updated_at = now()
     where id = $6
     returning
       id,
       surname,
       name,
       role,
       position,
       password_hash,
       password_salt,
       created_at,
       site_code,
       auth_source,
       external_user_id,
       external_employee_id,
       external_login,
       external_full_name,
       location_code`,
    [
      profile.surname || externalUser.login,
      profile.name || "",
      externalUser.login,
      externalUser.fullName,
      externalUser.employeeId,
      localUserId,
    ]
  );
  return result.rows?.[0] || null;
}

async function createSession(userId) {
  const token = crypto.randomBytes(32).toString("hex");
  const expires = new Date(Date.now() + SESSION_TTL_MS);
  await dbQuery(
    "insert into sessions (token, user_id, expires_at, created_at, updated_at) values ($1, $2, $3, now(), now())",
    [token, userId, expires]
  );
  return { token, expires };
}

async function getSession(token) {
  const result = await dbQuery(
    `select
       s.token,
       s.expires_at,
       u.id as user_id,
       u.surname,
       u.name,
       u.role,
       u.position,
       u.site_code,
       u.auth_source,
       u.location_code,
       u.external_user_id,
       u.external_employee_id,
       u.external_login,
       u.external_full_name
     from sessions s
     join users u on u.id = s.user_id
     where s.token = $1`,
    [token]
  );
  return result.rows?.[0] || null;
}

async function refreshSession(token, expires) {
  await dbQuery("update sessions set expires_at = $1, updated_at = now() where token = $2", [expires, token]);
}

async function deleteSession(token) {
  authSessionCache.delete(token);
  await dbQuery("delete from sessions where token = $1", [token]);
}

function getCachedAuthSession(token) {
  if (!token) return null;
  const cached = authSessionCache.get(token);
  if (!cached) return null;
  const now = Date.now();
  if (cached.cacheUntilMs <= now || cached.sessionExpiresAtMs <= now) {
    authSessionCache.delete(token);
    return null;
  }
  return cached.user;
}

function setCachedAuthSession(token, user, expiresAt) {
  if (!token || !user || !expiresAt) return;
  const expiresAtMs = new Date(expiresAt).getTime();
  if (!Number.isFinite(expiresAtMs)) return;
  const now = Date.now();
  authSessionCache.set(token, {
    user,
    sessionExpiresAtMs: expiresAtMs,
    cacheUntilMs: Math.min(expiresAtMs, now + AUTH_SESSION_CACHE_MS),
  });
}

function getInternalProxyAuth(req) {
  const token = String(req.headers["x-internal-proxy-token"] || "");
  const encodedUser = String(req.headers["x-internal-user"] || "");
  if (!token || token !== INTERNAL_PROXY_TOKEN || !encodedUser) return null;
  try {
    const payload = JSON.parse(Buffer.from(encodedUser, "base64url").toString("utf8"));
    if (!payload || typeof payload !== "object") return null;
    const userId = Number(payload.id || 0);
    if (!Number.isInteger(userId) || userId < 1) return null;
    return {
      id: userId,
      surname: normalizeText(payload.surname),
      name: normalizeText(payload.name),
      fullName: normalizeText(payload.fullName),
      login: normalizeText(payload.login),
      role: normalizeText(payload.role),
      position: normalizeText(payload.position) || null,
      locationCode: normalizeLocationCode(payload.locationCode),
      externalEmployeeId: Number(payload.externalEmployeeId || 0) || null,
    };
  } catch {
    return null;
  }
}

function shouldUseSecureCookie(req) {
  if (COOKIE_SECURE) return true;
  const proto = String(req.headers["x-forwarded-proto"] || "").toLowerCase();
  return proto === "https";
}

function getUnifiedSessionCookiePaths() {
  return Array.from(new Set([COOKIE_PATH, "/sklad", "/WB", "/OZ"].filter(Boolean)));
}

function setSessionCookie(res, token, expires, req) {
  const secure = shouldUseSecureCookie(req);
  const cookies = getUnifiedSessionCookiePaths().map((cookiePath) => {
    const parts = [
      `${COOKIE_NAME}=${token}`,
      `Path=${cookiePath}`,
      "HttpOnly",
      "SameSite=Lax",
      `Expires=${expires.toUTCString()}`,
    ];
    if (secure) parts.push("Secure");
    return parts.join("; ");
  });
  res.setHeader("Set-Cookie", cookies);
}

function clearSessionCookie(res, req) {
  const secure = shouldUseSecureCookie(req);
  const cookies = getUnifiedSessionCookiePaths().map((cookiePath) => {
    const parts = [
      `${COOKIE_NAME}=`,
      `Path=${cookiePath}`,
      "HttpOnly",
      "SameSite=Lax",
      "Expires=Thu, 01 Jan 1970 00:00:00 GMT",
    ];
    if (secure) parts.push("Secure");
    return parts.join("; ");
  });
  res.setHeader("Set-Cookie", cookies);
}async function getAuth(req, res) {
  const internalAuth = getInternalProxyAuth(req);
  if (internalAuth) return internalAuth;
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[COOKIE_NAME];
  if (!token) return null;
  const cachedUser = getCachedAuthSession(token);
  if (cachedUser) return cachedUser;
  const session = await getSession(token);
  if (!session) {
    clearSessionCookie(res, req);
    return null;
  }
  if (!canUseUserOnCurrentSite({ ...session, id: Number(session.user_id) })) {
    clearSessionCookie(res, req);
    return null;
  }
  const expiresAt = new Date(session.expires_at);
  if (!Number.isFinite(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    await deleteSession(token);
    clearSessionCookie(res, req);
    return null;
  }
  let effectiveExpires = expiresAt;
  if (expiresAt.getTime() - Date.now() <= AUTH_SESSION_REFRESH_THRESHOLD_MS) {
    const newExpires = new Date(Date.now() + SESSION_TTL_MS);
    await refreshSession(token, newExpires);
    setSessionCookie(res, token, newExpires, req);
    effectiveExpires = newExpires;
  }
  const user = toPublicUser({ ...session, id: Number(session.user_id) });
  setCachedAuthSession(token, user, effectiveExpires);
  return user;
}

async function requireAuth(req, res, roles = []) {
  const user = await getAuth(req, res);
  if (!user) {
    sendJson(res, 401, { error: "AUTH_REQUIRED" });
    return null;
  }
  if (roles.length && !roles.includes(user.role)) {
    sendJson(res, 403, { error: "FORBIDDEN" });
    return null;
  }
  return user;
}

function ensurePackerAccess(res, auth) {
  if (isPackerUser(auth)) return true;
  sendJson(res, 403, { error: "Доступно только упаковщикам" });
  return false;
}

function ensureCollectorAccess(res, auth) {
  if (isCollectorUser(auth)) return true;
  sendJson(res, 403, { error: "Доступно только сборщикам" });
  return false;
}

function getAuthExternalEmployeeId(auth) {
  const value = Number(auth?.externalEmployeeId || 0);
  return Number.isInteger(value) && value > 0 ? value : null;
}

const wbWarehousesCache = new Map(); // storeId -> { ts, data }
const WB_WAREHOUSES_CACHE_MS = 60000;

// Live WB warehouses of a seller store (GET /api/v3/warehouses), cached ~60s.
// Shape matches what the global-settings frontend expects: warehouseId/warehouseName.
async function getWbWarehousesForStoreCached(store) {
  if (!store?.id) return [];
  const cached = wbWarehousesCache.get(store.id);
  if (cached && Date.now() - cached.ts < WB_WAREHOUSES_CACHE_MS) return cached.data;
  const data = await wbRequest(store, "/api/v3/warehouses");
  const arr = Array.isArray(data) ? data : Array.isArray(data?.warehouses) ? data.warehouses : [];
  const mapped = arr
    .map((w) => {
      const id = String(w?.id ?? w?.warehouseId ?? w?.officeId ?? "").trim();
      const name = normalizeText(w?.name || "") || (id ? `Склад ${id}` : "склад");
      return { warehouseId: id, warehouseName: name, id, name };
    })
    .filter((w) => w.warehouseId);
  wbWarehousesCache.set(store.id, { ts: Date.now(), data: mapped });
  return mapped;
}

async function wbRequest(store, endpoint, options = {}) {
  if (!store?.token) {
    throw new Error("WB_API_TOKEN is missing");
  }
  const { method = "GET", body } = options;
  const requestBody = body ? JSON.stringify(body) : undefined;
  const maxAttempts = endpoint.includes("/orders/stickers") ? 4 : 3;
  const timeoutMs = endpoint.includes("/orders/stickers") ? 20000 : 15000;
  const headers = {
    Authorization: `Bearer ${store.token}`,
    Accept: "application/json",
    "User-Agent": "my_sklad/1.0",
  };
  if (store.clientSecret) {
    headers["X-Client-Secret"] = store.clientSecret;
  }
  if (requestBody) {
    headers["Content-Type"] = "application/json";
  }
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const resp = await fetch(`${API_BASE}${endpoint}`, {
        method,
        headers,
        body: requestBody,
        signal: AbortSignal.timeout(timeoutMs),
      });
      const text = await resp.text();
      if (!resp.ok) {
        if ((resp.status === 429 || resp.status >= 500) && attempt < maxAttempts) {
          await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
          continue;
        }
        throw new Error(`WB API ${resp.status}: ${text.slice(0, 400)}`);
      }
      if (!text) return null;
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    } catch (error) {
      const message = error?.message || String(error || "");
      const transient =
        error?.name === "AbortError" ||
        error?.name === "TimeoutError" ||
        message.includes("Client network socket disconnected") ||
        message.includes("fetch failed") ||
        message.includes("ECONNRESET") ||
        message.includes("ETIMEDOUT") ||
        message.includes("UND_ERR_");
      if (transient && attempt < maxAttempts) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
        continue;
      }
      throw error;
    }
  }
}

async function wbContentRequest(store, endpoint, body) {
  if (!store?.token) {
    throw new Error("WB_API_TOKEN is missing");
  }
  const resp = await fetch(`${CONTENT_BASE}${endpoint}`, {
    method: "POST",
    headers: {
      Authorization: store.token,
      Accept: "application/json",
      "Content-Type": "application/json",
      "User-Agent": "my_sklad/1.0",
    },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`WB Content ${resp.status}: ${text.slice(0, 400)}`);
  }
  if (!text) return null;
  return JSON.parse(text);
}

async function fetchAllProductCards(store) {
  const map = new Map();
  let updatedAt = null;
  let nmID = null;
  const limit = 100;
  const cache = getStoreCache(store?.id);

  for (let i = 0; i < 2000; i += 1) {
    const cursor = { limit };
    if (updatedAt) cursor.updatedAt = updatedAt;
    if (nmID) cursor.nmID = nmID;
    const body = {
      settings: {
        cursor,
        sort: { ascending: true },
        filter: { withPhoto: -1 },
      },
    };
    let data;
    try {
      data = await wbContentRequest(store, "/content/v2/get/cards/list", body);
    } catch (err) {
      const msg = err?.message || "";
      if (msg.includes("429")) {
        cache.contentBackoffUntil = Date.now() + 60_000;
        break;
      }
      throw err;
    }
    const cards = Array.isArray(data?.cards) ? data.cards : [];
    for (const card of cards) {
      const vendorCode = normalizeArticleKey(card?.vendorCode);
      if (!vendorCode) continue;
      const title = extractCardTitle(card);
      const barcodes = [];
      if (Array.isArray(card.sizes)) {
        for (const size of card.sizes) {
          const skus = normalizeSkus(size?.skus);
          for (const sku of skus) {
            if (!barcodes.includes(sku)) barcodes.push(sku);
          }
        }
      }
      map.set(vendorCode, { ts: Date.now(), title, barcodes });
    }

    const nextUpdatedAt = data?.cursor?.updatedAt ?? null;
    const nextNmID = data?.cursor?.nmID ?? null;
    const total = data?.cursor?.total ?? cards.length;

    if (cards.length === 0 || total < limit) break;
    if (!nextUpdatedAt && !nextNmID) break;
    if (nextUpdatedAt === updatedAt && nextNmID === nmID) break;
    updatedAt = nextUpdatedAt;
    nmID = nextNmID;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  return map;
}

async function getCardsCache(store, force = false) {
  const cache = getStoreCache(store?.id);
  if (!force && cache.cardsCache.map.size > 0 && Date.now() - cache.cardsCache.ts < CARDS_CACHE_MS) {
    return cache.cardsCache.map;
  }
  if (cache.cardsCache.loading) return cache.cardsCache.loading;
  cache.cardsCache.loading = (async () => {
    const map = await fetchAllProductCards(store);
    cache.cardsCache.map = map;
    cache.cardsCache.ts = Date.now();
    saveCardsCacheToDisk(store?.id, map);
    cache.cardsCache.loading = null;
    return map;
  })();
  try {
    return await cache.cardsCache.loading;
  } catch (err) {
    cache.cardsCache.loading = null;
    throw err;
  }
}

async function fetchProductInfoBySearch(store, search, options = {}) {
  const query = normalizeArticleKey(search);
  if (!query) return null;
  const body = {
    settings: {
      cursor: { limit: 10 },
      filter: { textSearch: query, withPhoto: -1 },
    },
  };
  const data = await wbContentRequest(store, "/content/v2/get/cards/list", body);
  const cards = Array.isArray(data?.cards) ? data.cards : [];
  if (cards.length === 0) return null;

  const expectedVendor = normalizeArticleKey(options.expectedVendorCode || null);
  const expectedNmIdRaw = options.expectedNmId ?? null;
  const expectedNmId = expectedNmIdRaw === null || expectedNmIdRaw === undefined ? null : Number(expectedNmIdRaw);
  let card = null;

  if (expectedVendor) {
    const lower = expectedVendor.toLowerCase();
    card = cards.find(
      (c) => String(normalizeArticleKey(c.vendorCode || "") || "").toLowerCase() === lower
    );
  }
  if (!card && Number.isFinite(expectedNmId)) {
    card = cards.find((c) => Number(c.nmID ?? c.nmId ?? c.nm_id) === expectedNmId);
  }
  if (!card) {
    card = cards[0] || null;
  }
  if (!card) return null;

  return extractCardInfo(card);
}

async function fetchProductInfoByArticle(store, article) {
  const primary = await fetchProductInfoBySearch(store, article, { expectedVendorCode: article });
  if (primary && primary.title) return primary;
  const fallback = await fetchProductInfoFromTrash(store, article);
  if (!fallback) return primary;
  if (!primary) return fallback;
  return {
    title: primary.title || fallback.title || null,
    barcodes: (primary.barcodes && primary.barcodes.length ? primary.barcodes : fallback.barcodes) || [],
    vendorCode: primary.vendorCode || fallback.vendorCode || null,
    nmId: primary.nmId || fallback.nmId || null,
  };
}

async function fetchProductInfoFromTrash(store, article) {
  const search = normalizeArticleKey(article);
  if (!search) return null;
  const cache = getStoreCache(store?.id);
  if (Date.now() < cache.contentBackoffUntil) return null;

  try {
    const body = {
      settings: {
        cursor: { limit: 20 },
        filter: { textSearch: search, withPhoto: -1 },
        sort: { ascending: true },
      },
    };
    const data = await wbContentRequest(store, "/content/v2/get/cards/trash", body);
    const cards = Array.isArray(data?.cards) ? data.cards : [];
    const lower = search.toLowerCase();
    const match = cards.find(
      (c) => String(normalizeArticleKey(c.vendorCode || "") || "").toLowerCase() === lower
    );
    if (match) return extractCardInfo(match);
  } catch (err) {
    const msg = err?.message || "";
    if (msg.includes("429")) {
      cache.contentBackoffUntil = Date.now() + 60_000;
      return null;
    }
  }

  let cursor = { limit: 100 };
  const maxPages = 20;
  for (let page = 0; page < maxPages; page += 1) {
    let data = null;
    try {
      const body = { settings: { cursor, sort: { ascending: true } } };
      data = await wbContentRequest(store, "/content/v2/get/cards/trash", body);
    } catch (err) {
      const msg = err?.message || "";
      if (msg.includes("429")) {
        cache.contentBackoffUntil = Date.now() + 60_000;
      }
      break;
    }
    const cards = Array.isArray(data?.cards) ? data.cards : [];
    const lower = search.toLowerCase();
    const match = cards.find(
      (c) => String(normalizeArticleKey(c.vendorCode || "") || "").toLowerCase() === lower
    );
    if (match) return extractCardInfo(match);
    const next = data?.cursor || {};
    if (!cards.length) break;
    if (!next.trashedAt && !next.nmID) break;
    cursor = { limit: cursor.limit, trashedAt: next.trashedAt, nmID: next.nmID };
    await new Promise((resolve) => setTimeout(resolve, 650));
  }

  return null;
}

async function resolveProductInfo(store, articles, options = {}) {
  const unique = Array.from(new Set((articles || []).map(normalizeArticleKey).filter(Boolean)));
  const forceArticles = options?.forceArticles instanceof Set ? options.forceArticles : new Set();
  const results = new Map();
  const missing = [];
  const now = Date.now();
  const cache = getStoreCache(store?.id);
  const cardsMap = cache.cardsCache.map;
  const hasCardsCache = cardsMap.size > 0;
  const cardsFresh = hasCardsCache && now - cache.cardsCache.ts < CARDS_CACHE_MS;

  for (const article of unique) {
    const cached = cache.productCache.get(article);
    const age = cached ? now - cached.ts : Infinity;
    const isFresh = cached && age < PRODUCT_CACHE_MS;
    const hasData = cached && (cached.title || (cached.barcodes && cached.barcodes.length));
    if (cached && isFresh && hasData) {
      results.set(article, cached);
      continue;
    }
    if (cached && cached.missing && age < PRODUCT_NEGATIVE_CACHE_MS && !forceArticles.has(article)) {
      if (hasCardsCache) {
        const card = cardsMap.get(article);
        if (card && (card.title || (card.barcodes && card.barcodes.length))) {
          const payload = {
            ts: Date.now(),
            title: card.title || null,
            barcodes: Array.isArray(card.barcodes) ? card.barcodes : [],
          };
          results.set(article, payload);
          cache.productCache.set(article, payload);
        }
      }
      continue;
    }
    if (hasCardsCache) {
      const card = cardsMap.get(article);
      if (card && (card.title || (card.barcodes && card.barcodes.length))) {
        const payload = {
          ts: Date.now(),
          title: card.title || null,
          barcodes: Array.isArray(card.barcodes) ? card.barcodes : [],
        };
        results.set(article, payload);
        cache.productCache.set(article, payload);
        continue;
      }
    }
    missing.push(article);
  }

  if (missing.length === 0) {
    return results;
  }

  if (!cardsFresh && !cache.cardsCache.loading) {
    getCardsCache(store, true).catch(() => {});
  }

  if (now < cache.contentBackoffUntil) {
    return results;
  }

  const maxPerBatch = PRODUCT_RESOLVE_BATCH;
  for (let i = 0; i < Math.min(missing.length, maxPerBatch); i += 1) {
    const article = missing[i];
    try {
      const info = await fetchProductInfoByArticle(store, article);
      if (info) {
        const payload = {
          ts: Date.now(),
          title: info.title || null,
          barcodes: Array.isArray(info.barcodes) ? info.barcodes : [],
        };
        cache.productCache.set(article, payload);
        results.set(article, payload);
      } else {
        cache.productCache.set(article, { ts: Date.now(), title: null, barcodes: [], missing: true });
      }
    } catch (err) {
      const msg = err?.message || "";
      if (msg.includes("429")) {
        cache.contentBackoffUntil = Date.now() + 60_000;
        break;
      }
    }
  }

  return results;
}

async function resolveProductInfoForOrders(store, orders) {
  const forceArticles = new Set();
  const list = [];
  for (const order of orders || []) {
    const article = normalizeArticleKey(order?.article);
    if (!article) continue;
    list.push(article);
    const rawName = order?.product_name || order?.productName || "";
    if (isPlaceholderName(rawName, article)) {
      forceArticles.add(article);
    }
  }
  const infoMap = await resolveProductInfo(store, list, { forceArticles });
  const cache = getStoreCache(store?.id);
  const now = Date.now();
  const contentAvailable = now >= cache.contentBackoffUntil;

  const missingArticles = new Set();
  for (const order of orders || []) {
    const article = normalizeArticleKey(order?.article);
    if (!article) continue;
    if (infoMap.has(article)) continue;
    missingArticles.add(article);
  }
  if (contentAvailable && missingArticles.size > 0 && (!cache.cardsCache.map.size || now - cache.cardsCache.ts > CARDS_CACHE_MS)) {
    try {
      await getCardsCache(store, true);
    } catch {}
  }
  if (missingArticles.size > 0 && cache.cardsCache.map.size) {
    for (const article of missingArticles) {
      if (infoMap.has(article)) continue;
      const card = cache.cardsCache.map.get(article);
      if (card && (card.title || (card.barcodes && card.barcodes.length))) {
        const payload = {
          ts: Date.now(),
          title: card.title || null,
          barcodes: Array.isArray(card.barcodes) ? card.barcodes : [],
        };
        cache.productCache.set(article, payload);
        infoMap.set(article, payload);
      }
    }
  }

  const missing = (orders || []).filter((order) => {
    const article = normalizeArticleKey(order?.article);
    if (!article) return false;
    const rawName = order?.product_name || order?.productName || "";
    if (!isPlaceholderName(rawName, article)) return false;
    const currentInfo = infoMap.get(article);
    if (currentInfo?.title && !isPlaceholderName(currentInfo.title, article)) return false;
    const nmId = order?.nmId ?? order?.nmID ?? order?.nm_id ?? null;
    return nmId !== null && nmId !== undefined;
  });
  if (!missing.length) return infoMap;

  const byNmId = new Map();
  for (const order of missing) {
    const nmId = order?.nmId ?? order?.nmID ?? order?.nm_id ?? null;
    const article = normalizeArticleKey(order?.article);
    if (!article || nmId === null || nmId === undefined) continue;
    const key = String(nmId);
    if (!byNmId.has(key)) byNmId.set(key, new Set());
    byNmId.get(key).add(article);
  }

  const maxPerBatch = Math.max(5, Math.floor(PRODUCT_RESOLVE_BATCH / 2));
  let processed = 0;
  if (contentAvailable) {
    for (const [nmId, articles] of byNmId) {
      if (processed >= maxPerBatch) break;
      processed += 1;
      try {
        const info = await fetchProductInfoBySearch(store, nmId, { expectedNmId: Number(nmId) });
        if (!info) continue;
        if (info.nmId !== null && info.nmId !== undefined && Number(info.nmId) !== Number(nmId)) {
          continue;
        }
        for (const article of articles) {
          const currentInfo = infoMap.get(article);
          const payload = {
            ts: Date.now(),
            title: info.title || null,
            barcodes: Array.isArray(info.barcodes) && info.barcodes.length
              ? info.barcodes
              : Array.isArray(currentInfo?.barcodes) ? currentInfo.barcodes : [],
          };
          cache.productCache.set(article, payload);
          infoMap.set(article, payload);
        }
      } catch (err) {
        const msg = err?.message || "";
        if (msg.includes("429")) {
          cache.contentBackoffUntil = Date.now() + 60_000;
          break;
        }
      }
    }
  }

  const cdnMissing = new Map();
  for (const order of missing) {
    const article = normalizeArticleKey(order?.article);
    const nmId = Number(order?.nmId ?? order?.nmID ?? order?.nm_id);
    if (!article || !Number.isFinite(nmId) || nmId <= 0) continue;
    if (!cdnMissing.has(nmId)) cdnMissing.set(nmId, new Set());
    cdnMissing.get(nmId).add(article);
  }

  await mapWithConcurrency([...cdnMissing.entries()], 8, async ([nmId, articles]) => {
    const metadata = await resolveWbCardMetadata(nmId).catch(() => null);
    if (!metadata?.title) return;
    for (const article of articles) {
      const currentInfo = infoMap.get(article);
      const payload = {
        ts: Date.now(),
        title: metadata.title,
        barcodes: Array.isArray(currentInfo?.barcodes) ? currentInfo.barcodes : [],
      };
        cache.productCache.set(article, payload);
        infoMap.set(article, payload);
    }
  });

  return infoMap;
}

async function ensureStickerBarcode(supplyId, orderId) {
  const existing = await dbQuery(
    "select sticker_barcode from supply_orders where supply_id = $1 and wb_order_id = $2",
    [supplyId, orderId]
  );
  const current = existing.rows?.[0]?.sticker_barcode || null;
  if (current) return current;
  const store = await getSupplyStore(supplyId);
  const data = await wbRequest(
    store,
    `/api/v3/orders/stickers?type=${LABEL_TYPE}&width=${LABEL_WIDTH}&height=${LABEL_HEIGHT}`,
    {
      method: "POST",
      body: { orders: [Number(orderId)] },
    }
  );
  const sticker = Array.isArray(data?.stickers) ? data.stickers[0] : null;
  const barcode = sticker?.barcode || null;
  if (barcode) {
    await dbQuery(
      "update supply_orders set sticker_barcode = $3, updated_at = now() where supply_id = $1 and wb_order_id = $2",
      [supplyId, orderId, barcode]
    );
  }
  return barcode;
}

async function findOrderByStickerBarcode({ supplyId, storeId = null, barcode, excludeOrderId = null, employeeId = null }) {
  const normalizedBarcode = normalizeBarcode(barcode);
  if (!normalizedBarcode) return null;

  const sameSupply = await dbQuery(
    `select o.supply_id, o.wb_order_id, o.article, o.product_name, o.assigned_user_id,
            o.label_scan_passed_at, o.collected_at, null::text as store_id, null::text as store_name
     from supply_orders o
     where o.supply_id = $1
       and o.sticker_barcode = $2
       and ($3::bigint is null or o.wb_order_id <> $3)
     order by
       case when $4::bigint is not null and o.assigned_user_id = $4 then 0 else 1 end,
       case when o.collected_at is null then 0 else 1 end,
       o.updated_at desc
     limit 1`,
    [supplyId, normalizedBarcode, excludeOrderId, employeeId]
  );
  if (sameSupply.rows?.[0]) {
    return { scope: "same_supply", ...sameSupply.rows[0] };
  }

  if (!storeId) return null;

  const otherSupply = await dbQuery(
    `select o.supply_id, o.wb_order_id, o.article, o.product_name, o.assigned_user_id,
            o.label_scan_passed_at, o.collected_at, s.store_id, s.store_name
     from supply_orders o
     join supply_settings s on s.supply_id = o.supply_id
     where s.store_id = $1
       and o.sticker_barcode = $2
       and o.supply_id <> $3
     order by
       case when $4::bigint is not null and o.assigned_user_id = $4 then 0 else 1 end,
       case when o.collected_at is null then 0 else 1 end,
       o.updated_at desc
     limit 1`,
    [storeId, normalizedBarcode, supplyId, employeeId]
  );
  if (otherSupply.rows?.[0]) {
    return { scope: "other_supply", ...otherSupply.rows[0] };
  }

  return null;
}

function buildWrongLabelScanMessage({ currentSupplyId, currentOrderId, matchedOrder }) {
  if (!matchedOrder) return "Отсканируйте повторно";

  const matchedOrderId = Number(matchedOrder.wb_order_id || 0) || matchedOrder.wb_order_id;
  const matchedSupplyId = String(matchedOrder.supply_id || "").trim();
  const itemLabel = [normalizeArticleKey(matchedOrder.article), String(matchedOrder.product_name || "").trim()]
    .filter(Boolean)
    .join(" · ");

  let message =
    matchedSupplyId && matchedSupplyId !== String(currentSupplyId || "").trim()
      ? `Это этикетка другой поставки: ${matchedSupplyId}, заказ ${matchedOrderId}.`
      : `Это этикетка другого заказа: ${matchedOrderId}.`;

  message += ` Сейчас открыт заказ ${currentOrderId}.`;
  if (itemLabel) {
    message += ` Верный заказ: ${itemLabel}.`;
  }
  if (matchedOrder.collected_at) {
    message += " Этот заказ уже собран.";
  } else if (matchedOrder.label_scan_passed_at) {
    message += " Этикетка по нему уже подтверждена.";
  } else {
    message += " Переключитесь на верный заказ или возьмите этикетку от текущего заказа.";
  }
  return message;
}

async function refreshNewOrders(store) {
  const data = await wbRequest(store, "/api/v3/orders/new");
  const orders = Array.isArray(data?.orders) ? data.orders : [];
  const normalized = orders
    .map((order) => normalizeOrder(order))
    .filter((o) => o && o.id && o.createdAt)
    .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
  const infoMap = await resolveProductInfoForOrders(store, normalized);
  const enriched = normalized.map((order) => {
    const info = infoMap.get(order.article);
    const barcode = order.barcode || info?.barcodes?.[0] || null;
    const currentName = order.productName;
    const needsName = isPlaceholderName(currentName, order.article);
    return {
      ...order,
      productName: needsName ? info?.title || null : currentName,
      barcode,
    };
  });
  await mapWithConcurrency(enriched, 8, async (order) => {
    const imageUrl = await resolveWbCardImage(order.nmId).catch(() => null);
    order.imageUrl = proxifyWbCardImage(imageUrl);
  });
  const cache = getStoreCache(store?.id).newOrders;
  Object.assign(cache, { ts: Date.now(), data: enriched, error: null });
  return enriched;
}

async function getNewOrders(storeId) {
  const store = resolveStore(storeId);
  const cache = getStoreCache(store?.id).newOrders;
  if (cacheFresh(cache, CACHE_NEW_ORDERS_MS)) return cache.data;
  if (cacheHasData(cache)) {
    if (!cache.loading) {
      cache.loading = refreshNewOrders(store)
        .catch((err) => {
          cache.error = err.message;
        })
        .finally(() => {
          cache.loading = null;
        });
    }
    return cache.data;
  }
  try {
    return await refreshNewOrders(store);
  } catch (err) {
    Object.assign(cache, { ts: Date.now(), error: err.message });
    throw err;
  }
}

async function fetchOrdersRange(store) {
  const nowSec = Math.floor(Date.now() / 1000);
  const dateFrom = nowSec - 30 * 24 * 3600;
  const limit = 1000;
  let next = 0;
  const all = [];
  for (let i = 0; i < 50; i += 1) {
    const resp = await wbRequest(
      store,
      `/api/v3/orders?limit=${limit}&next=${next}&dateFrom=${dateFrom}&dateTo=${nowSec}`
    );
    const orders = Array.isArray(resp?.orders) ? resp.orders : [];
    all.push(...orders);
    if (!resp?.next || resp.next === next || orders.length === 0) break;
    next = resp.next;
  }
  return all;
}

async function refreshOrders(store) {
  const orders = await fetchOrdersRange(store);
  const normalized = orders
    .map((order) => normalizeOrder(order))
    .filter((o) => o && o.id && o.createdAt);
  const infoMap = await resolveProductInfoForOrders(store, normalized);
  const enriched = normalized.map((order) => {
    const info = infoMap.get(order.article);
    const barcode = order.barcode || info?.barcodes?.[0] || null;
    const currentName = order.productName;
    const needsName = isPlaceholderName(currentName, order.article);
    return {
      ...order,
      productName: needsName ? info?.title || null : currentName,
      barcode,
    };
  });
  const cache = getStoreCache(store?.id).orders;
  Object.assign(cache, { ts: Date.now(), data: enriched, error: null });
  return enriched;
}

async function getOrders(storeId) {
  const store = resolveStore(storeId);
  const cache = getStoreCache(store?.id).orders;
  if (cacheFresh(cache, CACHE_ORDERS_MS)) return cache.data;
  if (cacheHasData(cache)) {
    if (!cache.loading) {
      cache.loading = refreshOrders(store)
        .catch((err) => {
          cache.error = err.message;
        })
        .finally(() => {
          cache.loading = null;
        });
    }
    return cache.data;
  }
  try {
    return await refreshOrders(store);
  } catch (err) {
    Object.assign(cache, { ts: Date.now(), error: err.message });
    throw err;
  }
}

async function fetchSupplies(store) {
  const limit = 1000;
  let next = 0;
  const all = [];
  for (let i = 0; i < 50; i += 1) {
    const resp = await wbRequest(store, `/api/v3/supplies?limit=${limit}&next=${next}`);
    const supplies = Array.isArray(resp?.supplies) ? resp.supplies : [];
    all.push(...supplies);
    if (!resp?.next || resp.next === next || supplies.length === 0) break;
    next = resp.next;
  }
  return all;
}

async function fetchSupplyOrders(store, supplyId) {
  // WB removed GET /api/v3/supplies/{id}/orders (now returns 404 "path not
  // found"). The store's recent orders (from /api/v3/orders via getOrders) carry
  // a supplyId, so list them and filter by this supply instead. getOrders is
  // cached (CACHE_ORDERS_MS), so repeated syncs don't hammer the WB API.
  const target = String(supplyId || "");
  if (!target) return [];
  const orders = await getOrders(store?.id);
  return (Array.isArray(orders) ? orders : []).filter(
    (order) => String(order?.supplyId ?? order?.supplyID ?? order?.supply_id ?? "") === target
  );
}

async function fetchSupplyOrderIds(store, supplyId) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId) return [];
  const data = await wbRequest(
    store,
    `/api/marketplace/v3/supplies/${encodeURIComponent(normalizedSupplyId)}/order-ids`
  );
  let values = [];
  if (Array.isArray(data)) {
    values = data;
  } else if (Array.isArray(data?.orders)) {
    values = data.orders;
  } else if (Array.isArray(data?.orderIds)) {
    values = data.orderIds;
  } else if (Array.isArray(data?.ids)) {
    values = data.ids;
  }
  return Array.from(
    new Set(
      values
        .map((value) => {
          // WB отдаёт order-id ЧИСЛАМИ (напр. 5544219292); normalizeText() возвращает
          // "" для не-строк, из-за чего раньше все id отбрасывались и состав получался
          // пустым ("WB вернул пустой состав поставки"). Приводим к строке явно.
          const id = typeof value === "object" ? value?.id ?? value?.orderId : value;
          return id === null || id === undefined ? "" : String(id).trim();
        })
        .filter(Boolean)
    )
  );
}

async function fetchFullSupplyOrders(store, supplyId) {
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!normalizedSupplyId) return [];
  const orderIds = await fetchSupplyOrderIds(store, normalizedSupplyId);
  if (!orderIds.length) return [];
  const wanted = new Set(orderIds.map((id) => String(id)));
  const rangeOrders = await fetchOrdersRange(store);
  const byId = new Map();
  for (const order of Array.isArray(rangeOrders) ? rangeOrders : []) {
    // order.id тоже число — приводим к строке, иначе normalizeText вернёт "" и
    // деталей заказа не найдётся ("детали не найдены для N").
    const rawId = order?.id ?? order?.orderId;
    const orderId = rawId === null || rawId === undefined ? "" : String(rawId).trim();
    if (orderId) byId.set(orderId, order);
  }
  const missingIds = [];
  const rows = [];
  for (const orderId of wanted) {
    const row = byId.get(orderId);
    if (row) {
      rows.push(row);
    } else {
      missingIds.push(orderId);
    }
  }
  if (missingIds.length) {
    const err = new Error(
      `WB вернул ${orderIds.length} order-id, но детали не найдены для ${missingIds.length}. Повторите позже.`
    );
    err.userFacing = true;
    err.missingOrderIds = missingIds;
    throw err;
  }
  return rows;
}

async function refreshSupplies(store) {
  const supplies = await fetchSupplies(store);

  const normalized = supplies
    .filter((s) => s && s.id)
    .map((s) => {
      const supplyId = normalizeSupplyId(s.id);
      return {
        id: supplyId,
        createdAt: s.createdAt,
        name: s.name,
        done: s.done,
        storeId: store?.id || null,
        storeName: store?.name || null,
      };
    })
    .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));

  const activeSupplies = normalized.filter((s) => s.done === false || s.done === 0 || s.done === null || s.done === undefined);
  const reopenedSupplies = await listReopenedDeliveredSuppliesForStore(store, 20);
  const reopenedMap = new Map(reopenedSupplies.map((supply) => [supply.id, supply]));
  for (const supply of activeSupplies) {
    reopenedMap.delete(supply.id);
  }
  const visibleActiveSupplies = [...activeSupplies, ...reopenedMap.values()].sort(
    (a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0)
  );

  if (visibleActiveSupplies.length) {
    await Promise.all(
      visibleActiveSupplies.map((supply) => ensureSupplySettings(supply.id, supply.name || null, store))
    );
    const supplyIds = visibleActiveSupplies.map((item) => item.id);
    const aggregates = await getSupplyAggregates(supplyIds);
    const accessStats = await getSupplyAccessStats(supplyIds);
    for (const supply of visibleActiveSupplies) {
      const agg = aggregates.get(supply.id);
      if (agg) {
        supply.orderCount = agg.orderCount;
        supply.itemCount = agg.itemCount;
        supply.collectedCount = agg.collectedCount;
        supply.remainingCount = Math.max(0, agg.orderCount - agg.collectedCount);
        supply.assignedUsers = agg.assignedUsers;
      } else {
        supply.orderCount = 0;
        supply.itemCount = 0;
        supply.collectedCount = 0;
        supply.remainingCount = 0;
        supply.assignedUsers = 0;
      }
      supply.accessUserCount = accessStats.accessUsers.get(supply.id) || 0;
      supply.accessMode = accessStats.accessModes.get(supply.id) || "hidden";
      if (!agg || agg.orderCount === 0) {
        ensureSupplySnapshotIfStale(supply.id, supply.name || null).catch(() => {});
      }
    }
  }

  const deliveringSupplies = await listDeliveredSuppliesForStore(store, 20);
  const cache = getStoreCache(store?.id).supplies;
  Object.assign(cache, { ts: Date.now(), data: visibleActiveSupplies, deliveringData: deliveringSupplies, error: null });
  return { supplies: visibleActiveSupplies, deliveringSupplies };
}

function ensureSuppliesRefresh(store, cache) {
  if (!store || cache.loading) return;
  cache.loading = refreshSupplies(store)
    .catch((err) => {
      cache.error = err.message;
    })
    .finally(() => {
      cache.loading = null;
    });
}

function getActiveSupplyIdSetFromCache(storeId) {
  const store = resolveStore(storeId);
  const cache = getStoreCache(store?.id).supplies;
  if (!store) return null;
  if (cacheFresh(cache, CACHE_SUPPLIES_MS)) {
    return new Set((Array.isArray(cache.data) ? cache.data : []).map((supply) => supply.id));
  }
  if (Array.isArray(cache.data) && cache.ts > 0) {
    ensureSuppliesRefresh(store, cache);
    return new Set(cache.data.map((supply) => supply.id));
  }
  ensureSuppliesRefresh(store, cache);
  return null;
}

async function getSupplies(storeId) {
  const store = resolveStore(storeId);
  const cache = getStoreCache(store?.id).supplies;
  if (cacheFresh(cache, CACHE_SUPPLIES_MS)) return { supplies: cache.data, deliveringSupplies: cache.deliveringData || [] };
  if (cacheHasData(cache)) {
    ensureSuppliesRefresh(store, cache);
    return { supplies: cache.data, deliveringSupplies: cache.deliveringData || [] };
  }
  try {
    return await refreshSupplies(store);
  } catch (err) {
    Object.assign(cache, { ts: Date.now(), error: err.message });
    throw err;
  }
}

async function listDeliveredSuppliesForStore(store, limit = 20) {
  const normalizedStoreId = normalizeStoreId(store?.id);
  const safeLimit = Math.max(1, Math.min(100, Number(limit || 20) || 20));
  if (!normalizedStoreId) return [];
  const result = await dbQuery(
    `select supply_id, supply_name, wb_supply_created_at, store_id, store_name
     from supply_settings
     where store_id = $1
       and coalesce(wb_supply_done, false) = true
       and not exists (
         select 1
         from supply_orders so
         where so.supply_id = supply_settings.supply_id
           and so.collected_at is null
       )
     order by coalesce(wb_supply_created_at, updated_at, created_at) desc nulls last
     limit $2`,
    [normalizedStoreId, safeLimit]
  );
  const supplies = (result.rows || []).map((row) => ({
    id: normalizeSupplyId(row.supply_id),
    createdAt: row.wb_supply_created_at || null,
    name: row.supply_name || null,
    done: true,
    storeId: row.store_id || normalizedStoreId,
    storeName: row.store_name || store?.name || null,
  })).filter((row) => row.id);
  if (!supplies.length) return [];
  const supplyIds = supplies.map((item) => item.id);
  const aggregates = await getSupplyAggregates(supplyIds);
  const accessStats = await getSupplyAccessStats(supplyIds);
  for (const supply of supplies) {
    const agg = aggregates.get(supply.id);
    if (agg) {
      supply.orderCount = agg.orderCount;
      supply.itemCount = agg.itemCount;
      supply.collectedCount = agg.collectedCount;
      supply.remainingCount = Math.max(0, agg.orderCount - agg.collectedCount);
      supply.assignedUsers = agg.assignedUsers;
    } else {
      supply.orderCount = 0;
      supply.itemCount = 0;
      supply.collectedCount = 0;
      supply.remainingCount = 0;
      supply.assignedUsers = 0;
    }
    supply.accessUserCount = accessStats.accessUsers.get(supply.id) || 0;
    supply.accessMode = accessStats.accessModes.get(supply.id) || "hidden";
  }
  return supplies;
}

async function listReopenedDeliveredSuppliesForStore(store, limit = 20) {
  const normalizedStoreId = normalizeStoreId(store?.id);
  const safeLimit = Math.max(1, Math.min(100, Number(limit || 20) || 20));
  if (!normalizedStoreId) return [];
  const result = await dbQuery(
    `select supply_id, supply_name, wb_supply_created_at, store_id, store_name
     from supply_settings
     where store_id = $1
       and coalesce(wb_supply_done, false) = true
       and coalesce(access_mode, 'hidden') = 'selected_split'
       and exists (
         select 1
         from supply_orders so
         where so.supply_id = supply_settings.supply_id
           and so.collected_at is null
       )
     order by coalesce(wb_supply_created_at, updated_at, created_at) desc nulls last
     limit $2`,
    [normalizedStoreId, safeLimit]
  );
  return (result.rows || [])
    .map((row) => ({
      id: normalizeSupplyId(row.supply_id),
      createdAt: row.wb_supply_created_at || null,
      name: row.supply_name || null,
      done: false,
      reopenedLocally: true,
      storeId: row.store_id || normalizedStoreId,
      storeName: row.store_name || store?.name || null,
    }))
    .filter((row) => row.id);
}

function resolveSettingsStoreId(settings) {
  return normalizeStoreId(settings?.store_id) || DEFAULT_STORE?.id || null;
}

async function getActiveSupplyIdSet(storeId, options = {}) {
  if (!storeId) return null;
  if (options.preferCache) {
    return getActiveSupplyIdSetFromCache(storeId);
  }
  try {
    const data = await getSupplies(storeId);
    const supplies = Array.isArray(data?.supplies) ? data.supplies : [];
    return new Set(supplies.map((supply) => supply.id));
  } catch {
    return null;
  }
}

async function isSupplyActiveForSettings(supplyId, settings, options = {}) {
  const storeId = resolveSettingsStoreId(settings);
  if (!storeId) return true;
  const activeSet = options.preferCache
    ? getActiveSupplyIdSetFromCache(storeId)
    : await getActiveSupplyIdSet(storeId);
  if (!activeSet) return true;
  return activeSet.has(supplyId);
}

async function getSupplyOrders(supplyId, supplyName = null) {
  const normalizedId = normalizeSupplyId(supplyId);
  if (!normalizedId) return [];
  const store = await getSupplyStore(normalizedId, supplyName);
  await ensureSupplySettings(normalizedId, supplyName, store);
  const selectText =
    `select wb_order_id, order_created_at, article, barcode, product_name, nm_id, quantity, assigned_user_id, collected_at, sticker_url, sticker_barcode
     from supply_orders
     where supply_id = $1
     order by order_created_at asc, wb_order_id asc`;
  let result = await dbQuery(selectText, [normalizedId]);
  let rows = result.rows || [];
  if (!rows.length) {
    await ensureSupplySnapshotIfEmpty(normalizedId, supplyName);
    result = await dbQuery(selectText, [normalizedId]);
    rows = result.rows || [];
  } else {
    ensureSupplySnapshotIfStale(normalizedId, supplyName).catch(() => {});
  }
  if (rows.length) {
    await fillMissingOrderInfo(normalizedId, rows, store);
    await warmWbImages(rows);
  }
  return rows.map((o) => ({
    id: Number(o.wb_order_id),
    createdAt: o.order_created_at,
    article: o.article ?? null,
    nmId: o.nm_id ?? null,
    quantity: o.quantity ?? 1,
    productName: o.product_name ?? null,
    barcode: o.barcode ?? null,
    imageUrl: proxifyWbCardImage(getWbCachedImage(o.nm_id)),
    stickerUrl: o.sticker_url ?? null,
    stickerBarcode: o.sticker_barcode ?? null,
    assignedUserId: o.assigned_user_id ? Number(o.assigned_user_id) : null,
    collectedAt: o.collected_at ?? null,
  }));
}

async function syncOpenEmployeeSupplySessions(supplyId) {
  const result = await dbQuery(
    `select *
     from employee_supply_sessions
     where supply_id = $1 and finished_at is null
     order by id asc`,
    [supplyId]
  );
  const openSessions = result.rows || [];
  let reconciledSessions = 0;
  let closedEmptySessions = 0;
  let autoFinishedPickingSessions = 0;
  let autoFinishedPackingSessions = 0;

  for (const row of openSessions) {
    let session = row;
    const refreshedCounts = await refreshEmployeeSessionOrderCounts(session.id, supplyId, session.user_id);
    session = refreshedCounts.session || session;

    if (normalizePickingStatus(session.picking_status) !== PICKING_STATUS_NOT_REQUIRED) {
      await syncEmployeeProductPickItems(session);
      const openAfterPickSync = await getEmployeeOpenSession(supplyId, session.user_id);
      if (openAfterPickSync) {
        const beforePickingStatus = normalizePickingStatus(openAfterPickSync.picking_status);
        const pickedSession = await finishEmployeeProductPickingIfComplete(openAfterPickSync);
        if (
          beforePickingStatus === PICKING_STATUS_STARTED &&
          normalizePickingStatus(pickedSession?.picking_status) === PICKING_STATUS_FINISHED
        ) {
          autoFinishedPickingSessions += 1;
        }
      }
    }

    const finishedSession = await finalizeEmployeeSupplySessionIfCompleted(supplyId, session.user_id);
    if (finishedSession) {
      autoFinishedPackingSessions += 1;
    }

    const openAfterFinalize = await getEmployeeOpenSession(supplyId, session.user_id);
    if (!openAfterFinalize) {
      reconciledSessions += 1;
      continue;
    }

    const nextCounts = await getEmployeeSupplyCounts(supplyId, session.user_id);
    if (nextCounts.total < 1) {
      const closedSession = await closeEmptyEmployeeSupplySession(openAfterFinalize);
      if (closedSession?.finished_at) {
        closedEmptySessions += 1;
      }
    } else if (
      Number(openAfterFinalize.assigned_order_count || 0) !== nextCounts.total ||
      Number(openAfterFinalize.collected_order_count || 0) !== nextCounts.collected
    ) {
      await refreshEmployeeSessionOrderCounts(openAfterFinalize.id, supplyId, openAfterFinalize.user_id);
    }

    reconciledSessions += 1;
  }

  return {
    reconciledSessions,
    closedEmptySessions,
    autoFinishedPickingSessions,
    autoFinishedPackingSessions,
  };
}

async function ensureSupplyPickingProgressInitialized(supplyId, settings = null) {
  if (!isEmployeeProductPickingActive(settings?.employee_workflow_stage)) return 0;
  const result = await dbQuery(
    `select *
     from employee_supply_sessions
     where supply_id = $1
       and finished_at is null
       and coalesce(status, '') <> 'reassigned'
       and session_role in ('pick', 'pick_pack')
       and coalesce(assigned_order_count, 0) > 0
       and coalesce(picking_required_qty, 0) = 0
     order by id asc`,
    [supplyId]
  );
  let initialized = 0;
  for (const session of result.rows || []) {
    const items = await syncEmployeeProductPickItems(session);
    if (items.length > 0) initialized += 1;
  }
  return initialized;
}

async function syncSupplyStateWithWb(supplyId, supplyName = null, options = {}) {
  const onProgress = typeof options?.onProgress === "function" ? options.onProgress : null;
  const reportProgress = async (completedSteps, message) => {
    if (!onProgress) return;
    await onProgress({
      completedSteps,
      totalSteps: SUPPLY_MANUAL_SYNC_TOTAL_STEPS,
      message,
    });
  };
  const normalizedId = normalizeSupplyId(supplyId);
  if (!normalizedId) {
    return {
      supplyId: null,
      removedOrderIds: [],
      removedOrdersCount: 0,
      releasedAssignedOrdersCount: 0,
      reconciledSessions: 0,
      closedEmptySessions: 0,
      autoFinishedPickingSessions: 0,
      autoFinishedPackingSessions: 0,
      totalOrders: 0,
    };
  }

  await reportProgress(1, "Подготавливаем поставку к синхронизации...");
  const store = await getSupplyStore(normalizedId, supplyName);
  const beforeResult = await dbQuery(
    `select wb_order_id, assigned_user_id
     from supply_orders
     where supply_id = $1 and collected_at is null`,
    [normalizedId]
  );
  const beforeRows = beforeResult.rows || [];

  // Обычная синхронизация только вычитает заказы, которых больше нет в поставке WB.
  // Новые заказы намеренно игнорируются: добавлять их может только полная синхронизация
  // до открытия поставки сотрудникам.
  await reportProgress(2, "Сверяем состав поставки с WB...");
  let wbOrderIds;
  try {
    wbOrderIds = await fetchSupplyOrderIds(store, normalizedId);
  } catch (fetchErr) {
    const e = new Error("Не удалось получить состав поставки с WB. Повторите синхронизацию позже.");
    e.userFacing = true;
    throw e;
  }
  const curAllRes = await dbQuery("select wb_order_id from supply_orders where supply_id = $1", [normalizedId]);
  const curAllIds = new Set((curAllRes.rows || []).map((r) => String(r.wb_order_id)));
  const wbOrderIdSet = new Set(wbOrderIds.map((id) => String(id)));
  const ignoredAddedOrdersCount = wbOrderIds.filter((id) => !curAllIds.has(String(id))).length;

  await reportProgress(3, "Обновляем состав (убираем снятые на WB)...");
  await pruneMissingOpenSupplyOrders(
    normalizedId,
    Array.from(wbOrderIdSet, (id) => ({ id }))
  );

  const afterResult = await dbQuery(
    `select wb_order_id
     from supply_orders
     where supply_id = $1 and collected_at is null`,
    [normalizedId]
  );
  const afterRows = afterResult.rows || [];
  const afterOrderIds = new Set(
    afterRows
      .map((row) => Number(row.wb_order_id))
      .filter((id) => Number.isFinite(id) && id > 0)
  );
  const removedOrderIds = beforeRows
    .map((row) => Number(row.wb_order_id))
    .filter((id) => Number.isFinite(id) && id > 0 && !afterOrderIds.has(id));
  const removedOrderIdSet = new Set(removedOrderIds);
  const releasedAssignedOrdersCount = beforeRows.filter(
    (row) =>
      Number.isFinite(Number(row?.assigned_user_id)) &&
      Number(row.assigned_user_id) > 0 &&
      removedOrderIdSet.has(Number(row.wb_order_id))
  ).length;

  await reportProgress(4, "Пересчитываем сотрудников и сборку товара...");
  const sessionSummary = await syncOpenEmployeeSupplySessions(normalizedId);
  await reportProgress(5, "Очищаем кэш и завершаем синхронизацию...");
  clearEmployeePickWarmCaches();
  invalidateStoreListCaches(store?.id);
  notifySupplyUpdate(normalizedId, true);

  return {
    supplyId: normalizedId,
    removedOrderIds,
    removedOrdersCount: removedOrderIds.length,
    releasedAssignedOrdersCount,
    totalOrders: afterRows.length,
    wbOrdersCount: wbOrderIds.length,
    ignoredAddedOrdersCount,
    ...sessionSummary,
  };
}

async function getSupplyFullSyncGuard(supplyId) {
  const normalizedId = normalizeSupplyId(supplyId);
  if (!normalizedId) {
    return { allowed: false, reason: "Поставка не найдена." };
  }
  const result = await dbQuery(
    `select ss.supply_id,
            coalesce(ss.access_mode, 'hidden') as access_mode,
            coalesce(ss.wb_supply_done, false) as wb_supply_done,
            coalesce(ss.manual_sync_status, 'idle') as manual_sync_status,
            count(o.*)::int as order_count,
            count(*) filter (where o.assigned_user_id is not null)::int as assigned_count,
            count(*) filter (where o.collected_at is not null)::int as collected_count,
            count(*) filter (
              where o.scan_passed_at is not null
                 or o.label_scan_passed_at is not null
                 or o.collected_session_id is not null
            )::int as touched_count,
            (
              select count(*)::int
              from employee_supply_sessions s
              where s.supply_id = ss.supply_id
                and coalesce(s.status, '') <> 'reassigned'
            ) as session_count
     from supply_settings ss
     left join supply_orders o on o.supply_id = ss.supply_id
     where ss.supply_id = $1
     group by ss.supply_id`,
    [normalizedId]
  );
  const row = result.rows?.[0];
  if (!row) {
    return { allowed: false, reason: "Поставка не найдена в настройках." };
  }
  if (row.wb_supply_done === true) {
    return { allowed: false, reason: "Поставка уже передана в доставку." };
  }
  if (
    normalizeManualSyncStatus(row.manual_sync_status) === "running" ||
    supplyManualSyncJobs.has(normalizedId)
  ) {
    return { allowed: false, reason: "Сейчас выполняется обычная синхронизация." };
  }
  if (normalizeText(row.access_mode || "hidden") !== "hidden") {
    return { allowed: false, reason: "Поставка уже открыта для сотрудников." };
  }
  if (Number(row.assigned_count || 0) > 0) {
    return { allowed: false, reason: "В поставке уже есть назначенные заказы." };
  }
  if (Number(row.collected_count || 0) > 0 || Number(row.touched_count || 0) > 0) {
    return { allowed: false, reason: "По поставке уже началась сборка или сканирование." };
  }
  if (Number(row.session_count || 0) > 0) {
    return { allowed: false, reason: "Сотрудник уже открывал эту поставку." };
  }
  return {
    allowed: true,
    reason: null,
    currentCount: Number(row.order_count || 0),
  };
}

async function fullSyncSupplyStateWithWb(supplyId, supplyName = null) {
  const normalizedId = normalizeSupplyId(supplyId);
  const guard = await getSupplyFullSyncGuard(normalizedId);
  if (!guard.allowed) {
    const err = new Error(guard.reason || "Полная синхронизация заблокирована.");
    err.userFacing = true;
    err.fullSyncBlocked = true;
    throw err;
  }
  const store = await getSupplyStore(normalizedId, supplyName);
  await ensureSupplySettings(normalizedId, supplyName, store);
  const raw = await fetchFullSupplyOrders(store, normalizedId);
  const guardAfterFetch = await getSupplyFullSyncGuard(normalizedId);
  if (!guardAfterFetch.allowed) {
    const err = new Error(guardAfterFetch.reason || "Полная синхронизация заблокирована.");
    err.userFacing = true;
    err.fullSyncBlocked = true;
    throw err;
  }
  const normalized = raw.map((order) => normalizeOrder(order, normalizedId)).filter((order) => order && order.id);
  const beforeRes = await dbQuery("select wb_order_id from supply_orders where supply_id = $1", [normalizedId]);
  const beforeIds = new Set((beforeRes.rows || []).map((row) => String(row.wb_order_id)));
  if (!normalized.length && beforeIds.size > 0) {
    const err = new Error("WB вернул пустой состав поставки — полная синхронизация пропущена.");
    err.userFacing = true;
    throw err;
  }
  await pruneMissingOpenSupplyOrders(normalizedId, normalized);
  if (normalized.length) {
    await upsertSupplyOrders(normalizedId, normalized);
    await fillMissingOrderInfo(normalizedId, normalized, store);
  }
  await ensureSupplyLabelsHealthy(normalizedId, supplyName);
  const afterRes = await dbQuery("select wb_order_id from supply_orders where supply_id = $1", [normalizedId]);
  const afterRows = afterRes.rows || [];
  const afterIds = new Set(afterRows.map((row) => String(row.wb_order_id)));
  await dbQuery(
    `update supply_settings
     set manual_sync_status = 'idle',
         manual_sync_last_synced_at = now(),
         manual_sync_message = $2,
         manual_sync_error = null,
         manual_sync_removed_orders = $3,
         updated_at = now()
     where supply_id = $1`,
    [
      normalizedId,
      `Полная синхронизация: ${afterRows.length} заказов`,
      [...beforeIds].filter((id) => !afterIds.has(id)).length,
    ]
  );
  clearEmployeePickWarmCaches();
  invalidateStoreListCaches(store?.id);
  notifySupplyUpdate(normalizedId, true);
  return {
    supplyId: normalizedId,
    applied: true,
    beforeCount: beforeIds.size,
    afterCount: afterRows.length,
    wbCount: normalized.length,
    addedCount: afterRows.filter((row) => !beforeIds.has(String(row.wb_order_id))).length,
    removedCount: [...beforeIds].filter((id) => !afterIds.has(id)).length,
  };
}

async function startSupplyManualSyncJob(supplyId, supplyName = null) {
  const normalizedId = normalizeSupplyId(supplyId);
  if (!normalizedId) {
    return { started: false, sync: null };
  }

  const runningJob = supplyManualSyncJobs.get(normalizedId);
  if (runningJob) {
    const currentSettings = await getSupplySettings(normalizedId);
    return {
      started: false,
      sync: toManualSyncPayload(currentSettings),
    };
  }

  const store = await getSupplyStore(normalizedId, supplyName);
  await ensureSupplySettings(normalizedId, supplyName, store);
  await markSupplyManualSyncRunning(normalizedId, SUPPLY_MANUAL_SYNC_TOTAL_STEPS, "Подготавливаем синхронизацию...");
  notifySupplyUpdate(normalizedId, true);

  let lastCompletedStep = 0;
  const job = (async () => {
    try {
      const summary = await syncSupplyStateWithWb(normalizedId, supplyName, {
        onProgress: async ({ completedSteps, totalSteps, message }) => {
          lastCompletedStep = Math.max(0, Number(completedSteps || 0));
          await updateSupplyManualSyncProgress(normalizedId, completedSteps, totalSteps, message);
          notifySupplyUpdate(normalizedId, true);
        },
      });
      const successMessage =
        summary.removedOrdersCount > 0 || summary.releasedAssignedOrdersCount > 0
          ? `Готово. Убрано заказов: ${summary.removedOrdersCount}. Снято у сотрудников: ${summary.releasedAssignedOrdersCount}.`
          : `Готово. Активных заказов в поставке: ${summary.totalOrders}.`;
      const finalMessage = Number(summary.ignoredAddedOrdersCount || 0) > 0
        ? `${successMessage} Новые заказы WB не добавлены: ${summary.ignoredAddedOrdersCount}.`
        : successMessage;
      await markSupplyManualSyncReady(normalizedId, SUPPLY_MANUAL_SYNC_TOTAL_STEPS, finalMessage, summary);
      notifySupplyUpdate(normalizedId, true);
      return summary;
    } catch (err) {
      const errorText = normalizeText(err?.message || "") || "Не удалось завершить синхронизацию";
      // Пользовательские блокировки (добавлены заказы на WB / WB недоступен) показываем как есть,
      // а не как «остановлена из-за ошибки» — чтобы админ видел, что именно надо сделать.
      const displayMessage = err?.userFacing ? errorText : "Синхронизация остановлена из-за ошибки.";
      if (!err?.userFacing) {
        console.error("Supply manual sync job failed", {
          supplyId: normalizedId,
          error: errorText,
        });
      }
      await markSupplyManualSyncError(
        normalizedId,
        lastCompletedStep,
        SUPPLY_MANUAL_SYNC_TOTAL_STEPS,
        displayMessage,
        errorText
      );
      notifySupplyUpdate(normalizedId, true);
      return { ok: false, error: errorText };
    }
  })();

  supplyManualSyncJobs.set(normalizedId, job);
  job.finally(() => supplyManualSyncJobs.delete(normalizedId));

  const settings = await getSupplySettings(normalizedId);
  return {
    started: true,
    sync: toManualSyncPayload(settings),
  };
}

const CREATE_REQUEST_TTL_MS = 10 * 60 * 1000;
const CREATE_SUPPLY_NAME_TTL_MS = 10 * 60 * 1000;
const CREATE_SUPPLY_RECOVERY_WINDOW_MS = 10 * 60 * 1000;
const recentCreateSupplyRequests = new Map();
const recentCreateSupplyNames = new Map();

function buildCreateRequestKey(storeId, values = []) {
  const normalizedStore = normalizeStoreId(storeId) || "";
  const normalizedValues = Array.from(
    new Set((Array.isArray(values) ? values : [values]).map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))
  ).sort((a, b) => a - b);
  return JSON.stringify([normalizedStore, normalizedValues]);
}

function getRecentCreateRequest(cache, key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    cache.delete(key);
    return null;
  }
  return entry.promise;
}

function buildCreateSupplyNameKey(storeId, name) {
  const normalizedStore = normalizeStoreId(storeId) || "";
  const normalizedName = normalizeText(name).toLowerCase();
  if (!normalizedStore || !normalizedName) return "";
  return JSON.stringify([normalizedStore, normalizedName]);
}

function getRecentCreatedSupplyByName(storeId, name) {
  const key = buildCreateSupplyNameKey(storeId, name);
  if (!key) return null;
  const entry = recentCreateSupplyNames.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    recentCreateSupplyNames.delete(key);
    return null;
  }
  return entry;
}

function rememberRecentCreatedSupplyByName(storeId, name, supplyId, ttlMs = CREATE_SUPPLY_NAME_TTL_MS) {
  const key = buildCreateSupplyNameKey(storeId, name);
  const normalizedSupplyId = normalizeSupplyId(supplyId);
  if (!key || !normalizedSupplyId) return normalizedSupplyId;
  recentCreateSupplyNames.set(key, {
    supplyId: normalizedSupplyId,
    createdAt: new Date().toISOString(),
    expiresAt: Date.now() + ttlMs,
  });
  return normalizedSupplyId;
}

function isRecentSupplyDate(value, windowMs = CREATE_SUPPLY_RECOVERY_WINDOW_MS) {
  const ts = new Date(value || "").getTime();
  if (!Number.isFinite(ts)) return false;
  return Date.now() - ts <= windowMs;
}

async function findRecentLocalSupplyByName(storeId, name, options = {}) {
  const normalizedStoreId = normalizeStoreId(storeId);
  const normalizedName = normalizeText(name);
  if (!normalizedStoreId || !normalizedName) return null;
  const cached = getRecentCreatedSupplyByName(normalizedStoreId, normalizedName);
  if (cached?.supplyId) {
    return {
      supplyId: cached.supplyId,
      createdAt: cached.createdAt || null,
      source: "memory",
    };
  }
  const windowMs = Math.max(1000, Number(options.windowMs || CREATE_SUPPLY_RECOVERY_WINDOW_MS));
  const result = await dbQuery(
    `select supply_id, created_at
     from supply_settings
     where store_id = $1
       and lower(btrim(coalesce(supply_name, ''))) = lower(btrim($2))
       and created_at >= now() - ($3::text || ' milliseconds')::interval
     order by created_at desc, supply_id desc
     limit 1`,
    [normalizedStoreId, normalizedName, String(windowMs)]
  );
  const row = result.rows?.[0] || null;
  if (!row?.supply_id) return null;
  const normalizedSupplyId = rememberRecentCreatedSupplyByName(normalizedStoreId, normalizedName, row.supply_id);
  return {
    supplyId: normalizedSupplyId,
    createdAt: row.created_at ?? null,
    source: "local",
  };
}

async function findRecentLocalSupplyByOrders(storeId, orderIds, options = {}) {
  const normalizedStoreId = normalizeStoreId(storeId);
  const normalizedOrderIds = Array.from(
    new Set(
      (Array.isArray(orderIds) ? orderIds : [orderIds])
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0)
    )
  );
  if (!normalizedStoreId || !normalizedOrderIds.length) return null;
  const windowMs = Math.max(1000, Number(options.windowMs || CREATE_SUPPLY_RECOVERY_WINDOW_MS));
  const result = await dbQuery(
    `with candidate_supplies as (
       select s.supply_id,
              max(s.created_at) as created_at,
              count(*) filter (where o.wb_order_id = any($2::bigint[]))::int as overlap_count
       from supply_settings s
       join supply_orders o on o.supply_id = s.supply_id
       where s.store_id = $1
         and s.created_at >= now() - ($3::text || ' milliseconds')::interval
       group by s.supply_id
     )
     select supply_id, created_at, overlap_count
     from candidate_supplies
     where overlap_count > 0
     order by overlap_count desc, created_at desc, supply_id desc
     limit 1`,
    [normalizedStoreId, normalizedOrderIds, String(windowMs)]
  );
  const row = result.rows?.[0] || null;
  if (!row?.supply_id) return null;
  return {
    supplyId: normalizeSupplyId(row.supply_id),
    createdAt: row.created_at ?? null,
    overlapCount: Number(row.overlap_count || 0),
    source: "local_orders",
  };
}

async function findRecentRemoteSupplyByName(store, name, options = {}) {
  const normalizedStoreId = normalizeStoreId(store?.id);
  const normalizedName = normalizeText(name);
  if (!normalizedStoreId || !normalizedName) return null;
  const cached = getRecentCreatedSupplyByName(normalizedStoreId, normalizedName);
  if (cached?.supplyId) {
    return {
      supplyId: cached.supplyId,
      createdAt: cached.createdAt || null,
      source: "memory",
    };
  }
  const windowMs = Math.max(1000, Number(options.windowMs || CREATE_SUPPLY_RECOVERY_WINDOW_MS));
  const supplies = await fetchSupplies(store);
  const match = (Array.isArray(supplies) ? supplies : [])
    .map((item) => ({
      supplyId: normalizeSupplyId(item?.id),
      name: normalizeText(item?.name),
      createdAt: item?.createdAt ?? null,
    }))
    .filter((item) => item.supplyId && item.name && item.name.toLowerCase() === normalizedName.toLowerCase())
    .filter((item) => isRecentSupplyDate(item.createdAt, windowMs))
    .sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0))[0] || null;
  if (!match?.supplyId) return null;
  rememberRecentCreatedSupplyByName(normalizedStoreId, normalizedName, match.supplyId);
  return {
    supplyId: match.supplyId,
    createdAt: match.createdAt,
    source: "remote",
  };
}

async function findRecentSupplyByName(store, name, options = {}) {
  const localMatch = await findRecentLocalSupplyByName(store?.id, name, options).catch(() => null);
  if (localMatch?.supplyId) return localMatch;
  const remoteMatch = await findRecentRemoteSupplyByName(store, name, options).catch(() => null);
  if (remoteMatch?.supplyId) return remoteMatch;
  return null;
}

async function findRecentSupplyByOrders(store, orderIds, options = {}) {
  const localMatch = await findRecentLocalSupplyByOrders(store?.id, orderIds, options).catch(() => null);
  if (localMatch?.supplyId) return localMatch;
  return null;
}

function rememberRecentCreateRequest(cache, key, promise, ttlMs = CREATE_REQUEST_TTL_MS) {
  cache.set(key, { promise, expiresAt: Date.now() + ttlMs });
  promise
    .then(() => {
      const entry = cache.get(key);
      if (entry?.promise === promise) entry.expiresAt = Date.now() + ttlMs;
    })
    .catch(() => {
      const entry = cache.get(key);
      if (entry?.promise === promise) cache.delete(key);
    });
  return promise;
}

// Авто-имя WB-поставки: «МСК {день}/{N}», где N — сквозной номер поставки за СЕГОДНЯ
// по ОБОИМ WB-кабинетам (Евгений+Ирина). Счётчик — по всем строкам supply_settings за день (МСК).
async function generateWbAutoSupplyName() {
  try {
    const r = await dbQuery(
      `select to_char(now() at time zone 'Europe/Moscow','FMDD') as day,
              (select count(*)::int from supply_settings
                 where (created_at at time zone 'Europe/Moscow')::date = (now() at time zone 'Europe/Moscow')::date) as cnt`,
      []
    );
    const day = String(r?.rows?.[0]?.day || "").trim();
    const seq = (Number(r?.rows?.[0]?.cnt) || 0) + 1;
    if (day) return `МСК ${day}/${seq}`;
  } catch (e) {
    console.error("generateWbAutoSupplyName failed", e?.message || e);
  }
  const d = new Date();
  return `МСК ${d.getUTCDate()}/${(Date.now() % 1000) + 1}`;
}

async function createSupply(store, name, orderIds, options = {}) {
  const recentSupplyByOrders = await findRecentSupplyByOrders(store, orderIds, {
    windowMs: CREATE_SUPPLY_RECOVERY_WINDOW_MS,
  });
  if (recentSupplyByOrders?.supplyId) {
    return createSupplyWithExisting(store, name, orderIds, recentSupplyByOrders.supplyId, options);
  }
  const recentSupply = await findRecentSupplyByName(store, name, { windowMs: CREATE_SUPPLY_NAME_TTL_MS });
  if (recentSupply?.supplyId) {
    return createSupplyWithExisting(store, name, orderIds, recentSupply.supplyId, options);
  }
  let created = null;
  let useLegacy = false;
  try {
    created = await wbRequest(store, "/api/v3/supplies", {
      method: "POST",
      body: { name },
    });
  } catch (err) {
    const recoveredByOrders = await findRecentSupplyByOrders(store, orderIds);
    if (recoveredByOrders?.supplyId) {
      return createSupplyWithExisting(store, name, orderIds, recoveredByOrders.supplyId, options);
    }
    const recovered = await findRecentSupplyByName(store, name);
    if (recovered?.supplyId) {
      return createSupplyWithExisting(store, name, orderIds, recovered.supplyId, options);
    }
    try {
      created = await wbRequest(store, "/api/marketplace/v3/supplies", {
        method: "POST",
        body: { name },
      });
      useLegacy = true;
    } catch (legacyErr) {
      const recoveredByOrdersAfterLegacy = await findRecentSupplyByOrders(store, orderIds);
      if (recoveredByOrdersAfterLegacy?.supplyId) {
        return createSupplyWithExisting(store, name, orderIds, recoveredByOrdersAfterLegacy.supplyId, options);
      }
      const recoveredAfterLegacy = await findRecentSupplyByName(store, name);
      if (recoveredAfterLegacy?.supplyId) {
        return createSupplyWithExisting(store, name, orderIds, recoveredAfterLegacy.supplyId, options);
      }
      throw legacyErr;
    }
  }
  const supplyId = normalizeSupplyId(created?.id);
  if (!supplyId) {
    throw new Error("Не удалось создать поставку");
  }
  rememberRecentCreatedSupplyByName(store?.id, name, supplyId);
  return createSupplyWithExisting(store, name, orderIds, supplyId, { ...options, useLegacy });
}

async function createSupplyWithExisting(store, name, orderIds, supplyId, options = {}) {
  let useLegacy = options?.useLegacy === true;
  const batchSize = Number.isFinite(ORDER_BATCH_SIZE) && ORDER_BATCH_SIZE > 0 ? ORDER_BATCH_SIZE : 100;
  const uniqueOrders = Array.from(new Set(orderIds));

  const addBatch = async (batch) => {
    if (useLegacy) {
      await wbRequest(store, `/api/marketplace/v3/supplies/${encodeURIComponent(supplyId)}/orders`, {
        method: "PATCH",
        body: { orders: batch },
      });
      return;
    }
    try {
      await wbRequest(store, `/api/v3/supplies/${encodeURIComponent(supplyId)}/orders`, {
        method: "PATCH",
        body: { orders: batch },
      });
    } catch (err) {
      await wbRequest(store, `/api/marketplace/v3/supplies/${encodeURIComponent(supplyId)}/orders`, {
        method: "PATCH",
        body: { orders: batch },
      });
      useLegacy = true;
    }
  };

  const added = [];
  const failed = [];
  const addWithSplit = async (batch) => {
    try {
      await addBatch(batch);
      added.push(...batch);
      return;
    } catch (err) {
      if (batch.length <= 1) {
        failed.push({ id: batch[0], error: err?.message || "WB error" });
        return;
      }
      const mid = Math.ceil(batch.length / 2);
      await addWithSplit(batch.slice(0, mid));
      await addWithSplit(batch.slice(mid));
    }
  };

  for (let i = 0; i < uniqueOrders.length; i += batchSize) {
    const batch = uniqueOrders.slice(i, i + batchSize);
    await addWithSplit(batch);
    if (i + batchSize < uniqueOrders.length) {
      await new Promise((resolve) => setTimeout(resolve, 120));
    }
  }

  // Устойчивость к сбою WB /api/v3/orders (500): сразу пишем выбранные заказы напрямую,
  // не полагаясь на перечитывание состава из WB. Иначе при недоступности WB-списка заказов
  // поставка приходит пустой (на WB заказы есть, у нас 0) — как случилось 11.08.
  try {
    const provided = options?.orderObjects;
    if (provided && added.length) {
      const getObj = typeof provided.get === "function"
        ? (id) => provided.get(Number(id))
        : (id) => provided[Number(id)];
      const normalizedSelected = added
        .map((id) => normalizeOrder(getObj(id), supplyId))
        .filter((o) => o && o.id);
      if (normalizedSelected.length) {
        await ensureSupplySettings(supplyId, name, store, options);
        await upsertSupplyOrders(supplyId, normalizedSelected);
      }
    }
  } catch (persistErr) {
    console.error("WB create: direct order persist failed", {
      supplyId,
      error: persistErr?.message || String(persistErr),
    });
  }

  try {
    await ensureSupplySettings(supplyId, name, store, options);
    await refreshSupplySnapshot(supplyId, name);
  } catch {
    // ignore snapshot failures
  }

  const cache = getStoreCache(store?.id);
  cache.newOrders.ts = 0;
  cache.orders.ts = 0;
  cache.supplies.ts = 0;
  const failedIds = failed.slice(0, 20).map((item) => item.id);
  const failedReason = failed[0]?.error || null;
  rememberRecentCreatedSupplyByName(store?.id, name, supplyId);
  return {
    supplyId,
    addedCount: added.length,
    failedCount: failed.length,
    failedIds,
    failedReason,
  };
}

function normalizeDemandQuantity(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return 1;
  return num;
}

function uniqueTextList(values = []) {
  const out = [];
  for (const value of Array.isArray(values) ? values : [values]) {
    const text = String(value ?? "").trim();
    if (!text || out.includes(text)) continue;
    out.push(text);
  }
  return out;
}

function normalizeExcludedArticleValue(value) {
  const article = normalizeArticleKey(value);
  return article ? String(article).trim().toUpperCase() : "";
}

function normalizeExcludedArticleFamily(value) {
  const article = normalizeExcludedArticleValue(value);
  return article ? article.replace(/_\d+$/i, "") : "";
}

function buildExcludedArticleMatcher(values = []) {
  const exact = new Set();
  const families = new Set();
  for (const value of Array.isArray(values) ? values : [values]) {
    const exactKey = normalizeExcludedArticleValue(value);
    if (!exactKey) continue;
    exact.add(exactKey);
    const familyKey = normalizeExcludedArticleFamily(exactKey);
    if (familyKey) families.add(familyKey);
  }
  return { exact, families };
}

function orderMatchesExcludedArticle(order, matcher = null) {
  if (!matcher || (!(matcher.exact instanceof Set) && !(matcher.families instanceof Set))) return false;
  const exactKey = normalizeExcludedArticleValue(order?.article);
  if (!exactKey) return false;
  if (matcher.exact instanceof Set && matcher.exact.has(exactKey)) return true;
  const familyKey = normalizeExcludedArticleFamily(exactKey);
  return Boolean(familyKey && matcher.families instanceof Set && matcher.families.has(familyKey));
}

function buildFbsDemandItemKey(item) {
  const barcode = normalizeBarcode(item?.barcode || "") || "";
  const article = normalizeArticleKey(item?.article) || "";
  const nmId = item?.nmId != null ? String(item.nmId) : "";
  const name = normalizeText(item?.productName || "") || "";
  if (barcode || article || nmId) {
    return `${barcode}||${article}||${nmId}`;
  }
  return `name:${name || "unknown"}`;
}

function aggregateFbsDemandItems({ assemblingOrders, newOrders }) {
  const byOrderId = new Map();
  for (const order of Array.isArray(assemblingOrders) ? assemblingOrders : []) {
    const orderId = Number(order?.id);
    if (!Number.isFinite(orderId)) continue;
    byOrderId.set(orderId, {
      id: orderId,
      source: "assembling",
      barcode: normalizeBarcode(order?.barcode || "") || "",
      article: normalizeArticleKey(order?.article) || "",
      nmId: order?.nmId ?? null,
      productName: normalizeText(order?.productName || "") || "",
      quantity: normalizeDemandQuantity(order?.quantity),
      storeId: normalizeStoreId(order?.storeId) || null,
    });
  }
  for (const order of Array.isArray(newOrders) ? newOrders : []) {
    const orderId = Number(order?.id);
    if (!Number.isFinite(orderId)) continue;
    if (byOrderId.has(orderId)) continue;
    byOrderId.set(orderId, {
      id: orderId,
      source: "new",
      barcode: normalizeBarcode(order?.barcode || "") || "",
      article: normalizeArticleKey(order?.article) || "",
      nmId: order?.nmId ?? null,
      productName: normalizeText(order?.productName || "") || "",
      quantity: normalizeDemandQuantity(order?.quantity),
      storeId: normalizeStoreId(order?.storeId) || null,
    });
  }

  const byItem = new Map();
  for (const order of byOrderId.values()) {
    const key = buildFbsDemandItemKey(order);
    const entry = byItem.get(key) || {
      key,
      productName: order.productName || order.article || "—",
      barcode: order.barcode || "",
      article: order.article || "",
      nmId: order.nmId ?? null,
      requiredQty: 0,
      newQty: 0,
      assemblingQty: 0,
      orderCount: 0,
      byStore: {},
    };
    if (!entry.productName && order.productName) entry.productName = order.productName;
    if (!entry.barcode && order.barcode) entry.barcode = order.barcode;
    if (!entry.article && order.article) entry.article = order.article;
    if (!entry.nmId && order.nmId != null) entry.nmId = order.nmId;
    const qty = normalizeDemandQuantity(order.quantity);
    entry.requiredQty += qty;
    entry.orderCount += 1;
    if (order.storeId) {
      entry.byStore[order.storeId] = Number(entry.byStore[order.storeId] || 0) + qty;
    }
    if (order.source === "assembling") {
      entry.assemblingQty += qty;
    } else {
      entry.newQty += qty;
    }
    byItem.set(key, entry);
  }

  return Array.from(byItem.values()).sort((a, b) => {
    if (b.requiredQty !== a.requiredQty) return b.requiredQty - a.requiredQty;
    return String(a.productName || "").localeCompare(String(b.productName || ""), "ru-RU");
  });
}

function buildResolvedFbsRowKey(match, row) {
  const numericId = Number(match?.id);
  if (Number.isInteger(numericId) && numericId > 0) return `id:${numericId}`;
  const article = normalizeArticleKey(match?.article || row?.article) || "";
  const name = normalizeText(match?.name || row?.productName || "") || "";
  return `${article}||${name}`;
}

function mergeStoreQuantities(target, source) {
  for (const [storeId, qty] of Object.entries(source || {})) {
    const normalizedStoreId = String(storeId || "").trim();
    if (!normalizedStoreId) continue;
    target[normalizedStoreId] = Number(target[normalizedStoreId] || 0) + Number(qty || 0);
  }
  return target;
}

function groupResolvedWbFbsRows(rows = []) {
  const grouped = new Map();
  const passthrough = [];

  for (const sourceRow of Array.isArray(rows) ? rows : []) {
    const row = {
      ...sourceRow,
      barcodes: uniqueTextList([...(Array.isArray(sourceRow?.barcodes) ? sourceRow.barcodes : []), sourceRow?.barcode || ""]),
      articles: uniqueTextList([...(Array.isArray(sourceRow?.articles) ? sourceRow.articles : []), sourceRow?.article || ""]),
      nmIds: uniqueTextList([...(Array.isArray(sourceRow?.nmIds) ? sourceRow.nmIds : []), sourceRow?.nmId != null ? String(sourceRow.nmId) : ""]),
    };
    const match = Array.isArray(row.msMatches) && row.msMatches.length === 1 ? row.msMatches[0] : null;
    const isResolvedRow = (row.msStatus === "ok" || row.msStatus === "insufficient") && match;
    if (!isResolvedRow) {
      passthrough.push({
        ...row,
        barcode: row.barcodes[0] || "",
        article: row.articles[0] || "",
        nmId: row.nmIds[0] || row.nmId || null,
      });
      continue;
    }

    const resolvedKey = buildResolvedFbsRowKey(match, row);
    if (!grouped.has(resolvedKey)) {
      grouped.set(resolvedKey, {
        ...row,
        key: `resolved:${resolvedKey}`,
        productName: match?.name || row.productName || "—",
        barcodes: uniqueTextList(row.barcodes),
        barcode: row.barcodes[0] || "",
        articles: uniqueTextList([match?.article || "", ...row.articles]),
        article: normalizeArticleKey(match?.article || row.article) || row.articles[0] || "",
        nmIds: uniqueTextList(row.nmIds),
        nmId: row.nmIds[0] || row.nmId || null,
        requiredQty: 0,
        newQty: 0,
        assemblingQty: 0,
        orderCount: 0,
        byStore: {},
        msMatches: match ? [match] : [],
        msStatus: "ok",
        isShortage: false,
        msError: null,
      });
    }

    const entry = grouped.get(resolvedKey);
    entry.productName = entry.productName || match?.name || row.productName || "—";
    entry.barcodes = uniqueTextList([...(entry.barcodes || []), ...(row.barcodes || [])]);
    entry.barcode = entry.barcodes[0] || entry.barcode || "";
    entry.articles = uniqueTextList([...(entry.articles || []), ...(row.articles || []), match?.article || ""]);
    entry.article = entry.articles[0] || entry.article || "";
    entry.nmIds = uniqueTextList([...(entry.nmIds || []), ...(row.nmIds || [])]);
    entry.nmId = entry.nmIds[0] || entry.nmId || null;
    entry.requiredQty += Number(row.requiredQty || 0);
    entry.newQty += Number(row.newQty || 0);
    entry.assemblingQty += Number(row.assemblingQty || 0);
    entry.orderCount += Number(row.orderCount || 0);
    mergeStoreQuantities(entry.byStore, row.byStore);
  }

  const resolvedRows = Array.from(grouped.values()).map((entry) => {
    const stockQty = entry.msStockQty == null ? null : Number(entry.msStockQty || 0);
    const requiredQty = Number(entry.requiredQty || 0);
    const isShortage = stockQty != null && stockQty < requiredQty;
    return {
      ...entry,
      barcode: entry.barcodes[0] || "",
      article: entry.articles[0] || "",
      nmId: entry.nmIds[0] || entry.nmId || null,
      msStatus: stockQty == null ? entry.msStatus : isShortage ? "insufficient" : "ok",
      isShortage,
      msError: null,
    };
  });

  return [...resolvedRows, ...passthrough];
}

async function mapWithConcurrency(items, limit, worker, options = {}) {
  const source = Array.isArray(items) ? items : [];
  if (!source.length) return [];
  const concurrency = Math.max(1, Number(limit) || 1);
  const onStart = typeof options?.onStart === "function" ? options.onStart : null;
  const onProgress = typeof options?.onProgress === "function" ? options.onProgress : null;
  const total = source.length;
  const results = new Array(source.length);
  let cursor = 0;
  let done = 0;
  const runners = Array.from({ length: Math.min(concurrency, source.length) }, async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= source.length) return;
      const item = source[index];
      if (onStart) {
        onStart({ item, index, done, total });
      }
      results[index] = await worker(item, index);
      done += 1;
      if (onProgress) {
        onProgress({ item, index, done, total });
      }
    }
  });
  await Promise.all(runners);
  return results;
}

async function listPendingAssemblyOrdersForSupplies(supplyIds) {
  if (!Array.isArray(supplyIds) || supplyIds.length === 0) return [];
  const result = await dbQuery(
    `select o.wb_order_id,
            o.article,
            o.barcode,
            o.product_name,
            o.nm_id,
            o.quantity,
            ss.store_id
     from supply_orders o
     left join supply_settings ss on ss.supply_id = o.supply_id
     where o.supply_id = any($1)
       and o.collected_at is null`,
    [supplyIds]
  );
  return (result.rows || []).map((row) => ({
    id: Number(row.wb_order_id),
    article: row.article || null,
    barcode: row.barcode || null,
    productName: row.product_name || null,
    nmId: row.nm_id ?? null,
    quantity: row.quantity ?? 1,
    storeId: normalizeStoreId(row.store_id) || DEFAULT_STORE?.id || null,
  }));
}

function listFbsScopeStores(fallbackStoreId = null) {
  const stores = Array.isArray(storeList) ? storeList.filter((store) => store?.id) : [];
  if (stores.length > 0) return stores;
  const fallbackStore = resolveStore(fallbackStoreId);
  return fallbackStore?.id ? [fallbackStore] : [];
}

async function getFbsStockCheckReport(storeId, options = {}) {
  const stores = listFbsScopeStores(storeId);
  const force = options?.force === true;
  const onProgress = typeof options?.onProgress === "function" ? options.onProgress : null;
  if (onProgress) {
    onProgress({
      stage: "load_orders",
      message: "Загружаем новые заказы и активные поставки по всем магазинам",
      done: 0,
      total: stores.length,
      current: null,
    });
  }
  if (!stores.length) {
    return {
      warehouseName: FBS_STOCK_WAREHOUSE_NAME,
      items: [],
      totals: { all: 0, ok: 0, shortage: 0, errors: 0 },
      updatedAt: new Date().toISOString(),
    };
  }

  if (onProgress) {
    onProgress({
      stage: "ms_access",
      message: "Проверяем доступ к складской БД",
      done: 0,
      total: 1,
      current: null,
    });
  }
  await assertSkladDbAvailable(FBS_STOCK_WAREHOUSE_NAME, { force });

  const storeLoadResults = await mapWithConcurrency(
    stores,
    2,
    async (store) => {
      const [newOrders, supplies] = await Promise.all([
        getNewOrders(store.id),
        getSupplies(store.id),
      ]);
      return {
        storeId: store.id,
        storeName: store.name || null,
        newOrders: Array.isArray(newOrders) ? newOrders : [],
        supplies: Array.isArray(supplies) ? supplies : [],
      };
    },
    {
      onStart: ({ item, done, total }) => {
        if (!onProgress) return;
        onProgress({
          stage: "load_orders",
          message: "Загружаем новые заказы и активные поставки по всем магазинам",
          done,
          total,
          current: {
            storeId: item?.id || null,
            storeName: item?.name || null,
          },
        });
      },
      onProgress: ({ item, done, total }) => {
        if (!onProgress) return;
        onProgress({
          stage: "load_orders",
          message: "Загружаем новые заказы и активные поставки по всем магазинам",
          done,
          total,
          current: {
            storeId: item?.id || null,
            storeName: item?.name || null,
          },
        });
      },
    }
  );

  const newOrders = storeLoadResults.flatMap((entry) =>
    (entry.newOrders || []).map((order) => ({
      ...order,
      storeId: entry.storeId || null,
    }))
  );
  const supplies = storeLoadResults.flatMap((entry) => entry.supplies || []);

  const activeSupplyIds = (Array.isArray(supplies) ? supplies : [])
    .map((supply) => normalizeSupplyId(supply?.id))
    .filter(Boolean);
  if (activeSupplyIds.length) {
    await mapWithConcurrency(
      activeSupplyIds,
      3,
      async (supplyId) => {
        try {
          await ensureSupplySnapshotIfEmpty(supplyId);
        } catch {
          // ignore snapshot refresh errors for partial report
        }
      },
      {
        onStart: ({ item, done, total }) => {
          if (!onProgress) return;
          onProgress({
            stage: "snapshot",
            message: "Актуализируем состав поставок",
            done,
            total,
            current: { supplyId: item },
          });
        },
        onProgress: ({ item, done, total }) => {
          if (!onProgress) return;
          onProgress({
            stage: "snapshot",
            message: "Актуализируем состав поставок",
            done,
            total,
            current: { supplyId: item },
          });
        },
      }
    );
  }

  const assemblingOrders = await listPendingAssemblyOrdersForSupplies(activeSupplyIds);
  const aggregatedItems = aggregateFbsDemandItems({
    assemblingOrders,
    newOrders: Array.isArray(newOrders) ? newOrders : [],
  });
  if (onProgress) {
    onProgress({
      stage: "ms_lookup",
      message: "Проверяем остатки в складской БД",
      done: 0,
      total: aggregatedItems.length,
      current: null,
    });
  }

  const rows = await mapWithConcurrency(
    aggregatedItems,
    2,
    async (item) => {
      const base = {
        key: item.key,
        productName: item.productName || "—",
        barcode: item.barcode || "",
        article: item.article || "",
        nmId: item.nmId ?? null,
        requiredQty: Number(item.requiredQty || 0),
        byStore: item.byStore || {},
        newQty: Number(item.newQty || 0),
        assemblingQty: Number(item.assemblingQty || 0),
        orderCount: Number(item.orderCount || 0),
        msStockQty: null,
        msCells: [],
        msStatus: "ok",
        msError: null,
        msMatches: [],
        isShortage: false,
      };

      if (!base.barcode) {
        return {
          ...base,
          msStatus: "no_barcode",
          msError: "У товара нет баркода WB",
        };
      }

      const ms = await getSkladFbsStockStatusByBarcode(base.barcode, {
        warehouseId: SKLAD_FBS_WAREHOUSE_ID,
        storeName: FBS_STOCK_WAREHOUSE_NAME,
        force,
      });
      const matches = Array.isArray(ms?.matches)
        ? ms.matches.map((entry) => ({
            id: entry?.id || null,
            name: entry?.name || null,
            article: entry?.article || null,
          }))
        : [];
      const cells = Array.isArray(ms?.slots)
        ? ms.slots.map((entry) => ({
            id: entry?.id || null,
            name: entry?.name || null,
            stock: Number(entry?.stock || 0),
          }))
        : [];

      if (ms?.status === "ok") {
        const stockQty = Number(ms?.stock ?? 0);
        const isShortage = stockQty < base.requiredQty;
        return {
          ...base,
          msStockQty: stockQty,
          msCells: cells,
          msStatus: isShortage ? "insufficient" : "ok",
          msMatches: matches,
          isShortage,
        };
      }

      if (ms?.status === "multiple") {
        return {
          ...base,
          msStatus: "multiple",
          msError: ms?.error || "По баркоду найдено несколько товаров в складской БД",
          msMatches: matches,
        };
      }

      if (ms?.status === "not_found") {
        return {
          ...base,
          msStatus: "not_found",
          msError: ms?.error || "Товар по баркоду не найден в складской БД",
        };
      }

      if (ms?.status === "store_not_found") {
        return {
          ...base,
          msStatus: "store_not_found",
          msError: ms?.error || `Склад "${FBS_STOCK_WAREHOUSE_NAME}" не найден в складской БД`,
          msMatches: matches,
        };
      }

      if (ms?.status === "stock_error") {
        return {
          ...base,
          msStatus: "stock_error",
          msError: ms?.error || "Не удалось получить остаток по складу",
          msMatches: matches,
        };
      }

      return {
        ...base,
        msStatus: "error",
        msError: ms?.error || "Ошибка чтения складской БД",
        msMatches: matches,
      };
    },
    {
      onStart: ({ item, done, total }) => {
        if (!onProgress) return;
        onProgress({
          stage: "ms_lookup",
          message: "Проверяем остатки в складской БД",
          done,
          total,
          current: {
            productName: item?.productName || null,
            barcode: item?.barcode || null,
            article: item?.article || null,
            requiredQty: Number(item?.requiredQty || 0),
          },
        });
      },
      onProgress: ({ item, done, total }) => {
        if (!onProgress) return;
        onProgress({
          stage: "ms_lookup",
          message: "Проверяем остатки в складской БД",
          done,
          total,
          current: {
            productName: item?.productName || null,
            barcode: item?.barcode || null,
            article: item?.article || null,
            requiredQty: Number(item?.requiredQty || 0),
          },
        });
      },
    }
  );
  const groupedRows = groupResolvedWbFbsRows(rows);

  const statusRank = (row) => {
    if (row.msStatus === "insufficient") return 1;
    if (row.msStatus === "ok") return 2;
    return 0;
  };

  groupedRows.sort((a, b) => {
    const rankDiff = statusRank(a) - statusRank(b);
    if (rankDiff !== 0) return rankDiff;
    if (b.requiredQty !== a.requiredQty) return b.requiredQty - a.requiredQty;
    return String(a.productName || "").localeCompare(String(b.productName || ""), "ru-RU");
  });

  const totals = {
    all: groupedRows.length,
    ok: groupedRows.filter((row) => row.msStatus === "ok").length,
    shortage: groupedRows.filter((row) => row.msStatus === "insufficient").length,
    errors: groupedRows.filter((row) => row.msStatus !== "ok" && row.msStatus !== "insufficient").length,
  };

  return {
    warehouseName: FBS_STOCK_WAREHOUSE_NAME,
    items: groupedRows,
    totals,
    updatedAt: new Date().toISOString(),
    scopeStoreIds: stores.map((store) => store.id),
    scopeStoreNames: stores.map((store) => store.name || store.id),
  };
}

function getFbsStockJobKey(storeId) {
  return "all_stores";
}

function getEmptyFbsStockReport() {
  return {
    warehouseName: FBS_STOCK_WAREHOUSE_NAME,
    items: [],
    totals: { all: 0, ok: 0, shortage: 0, errors: 0 },
    updatedAt: null,
  };
}

function createFbsStockJobState(storeId) {
  return {
    storeId: normalizeStoreId(storeId) || null,
    running: false,
    report: null,
    error: null,
    updatedAt: null,
    startedAt: null,
    finishedAt: null,
    progress: {
      stage: "idle",
      message: "",
      done: 0,
      total: 0,
      percent: 0,
      current: null,
      updatedAt: new Date().toISOString(),
    },
    jobPromise: null,
  };
}

function getFbsStockJobState(storeId) {
  const key = getFbsStockJobKey(storeId);
  if (!fbsStockCheckJobs.has(key)) {
    fbsStockCheckJobs.set(key, createFbsStockJobState(storeId));
  }
  return fbsStockCheckJobs.get(key);
}

function toFbsPercent(done, total) {
  const safeTotal = Number(total) || 0;
  if (safeTotal <= 0) return 0;
  const percent = (Number(done) / safeTotal) * 100;
  if (!Number.isFinite(percent)) return 0;
  return Math.max(0, Math.min(100, Number(percent.toFixed(2))));
}

function setFbsStockProgress(state, patch = {}) {
  if (!state) return;
  const prev = state.progress || {};
  const done = Number.isFinite(Number(patch.done)) ? Number(patch.done) : Number(prev.done || 0);
  const total = Number.isFinite(Number(patch.total)) ? Number(patch.total) : Number(prev.total || 0);
  const next = {
    stage: patch.stage || prev.stage || "idle",
    message: patch.message || prev.message || "",
    done,
    total,
    percent: Number.isFinite(Number(patch.percent))
      ? Number(patch.percent)
      : toFbsPercent(done, total),
    current: patch.current !== undefined ? patch.current : prev.current || null,
    updatedAt: new Date().toISOString(),
  };
  state.progress = next;
}

function buildFbsStockJobPayload(state) {
  const report = state?.report || getEmptyFbsStockReport();
  return {
    warehouseName: report.warehouseName || FBS_STOCK_WAREHOUSE_NAME,
    items: Array.isArray(report.items) ? report.items : [],
    totals: report.totals || { all: 0, ok: 0, shortage: 0, errors: 0 },
    updatedAt: report.updatedAt || null,
    running: Boolean(state?.running),
    progress: state?.progress || {
      stage: "idle",
      message: "",
      done: 0,
      total: 0,
      percent: 0,
      current: null,
      updatedAt: new Date().toISOString(),
    },
    error: state?.error || null,
    cooldownUntil: null,
    cooldownRemainingMs: 0,
  };
}

function startFbsStockCheckJob(storeId, options = {}) {
  const force = options?.force === true;
  const state = getFbsStockJobState(storeId);
  if (state.running) return state;

  state.running = true;
  state.error = null;
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  setFbsStockProgress(state, {
    stage: "load_orders",
    message: "Загружаем новые заказы и активные поставки",
    done: 0,
    total: 0,
    current: null,
  });

  state.jobPromise = getFbsStockCheckReport(storeId, {
    force,
    onProgress: (progress) => setFbsStockProgress(state, progress),
  })
    .then((report) => {
      state.report = report || getEmptyFbsStockReport();
      state.error = null;
      state.running = false;
      state.finishedAt = new Date().toISOString();
      setFbsStockProgress(state, {
        stage: "done",
        message: "Проверка остатков завершена",
        done: Number(state.report?.totals?.all || 0),
        total: Number(state.report?.totals?.all || 0),
        percent: 100,
        current: null,
      });
    })
    .catch((err) => {
      state.error = err?.message || "Не удалось обновить проверку остатков";
      if (isMsApiAccessError(err) || isSkladDbAccessError(err)) {
        state.report = getEmptyFbsStockReport();
      }
      state.running = false;
      state.finishedAt = new Date().toISOString();
      setFbsStockProgress(state, {
        stage: "error",
        message: state.error,
      });
    })
    .finally(() => {
      state.updatedAt = new Date().toISOString();
      state.jobPromise = null;
    });

  return state;
}

async function handleApi(req, res, pathname, url) {
  if (req.method === "OPTIONS") {
    res.writeHead(204, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS" });
    res.end();
    return;
  }

    if (pathname === "/api/health") {
      sendJson(res, 200, { ok: true, time: new Date().toISOString() });
      return;
    }

    if (pathname === "/api/stores") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      // Attach live WB warehouses per store so superadmin can bind a city to a
      // specific WB warehouse of a specific store (not just the store).
      const stores = await Promise.all(
        storeList.map(async (store) => {
          let warehouses = [];
          try {
            warehouses = await getWbWarehousesForStoreCached(store);
          } catch {
            warehouses = [];
          }
          return { id: store.id, name: store.name, warehouses };
        })
      );
      sendJson(res, 200, {
        stores,
        defaultStoreId: DEFAULT_STORE?.id || null,
      });
      return;
    }

  try {
    if (pathname === "/api/auth/login" && req.method === "POST") {
      const body = await readJson(req);
      const login = normalizeText(body?.login ?? body?.surname);
      const password = typeof body?.password === "string" ? body.password : "";
      if (!login || !password) {
        sendJson(res, 400, { error: "Введите логин и пароль" });
        return;
      }

      const localUsers = await findLocalLoginUsersByLogin(login);
      const matchedLocal = localUsers.find((user) => verifyPassword(password, user.password_salt, user.password_hash));
      if (matchedLocal) {
        const session = await createSession(matchedLocal.id);
        setSessionCookie(res, session.token, session.expires, req);
        sendJson(res, 200, { user: toPublicUser(matchedLocal) });
        return;
      }

      if (!isExternalAuthConfigured()) {
        sendJson(res, 500, { error: "Внешняя база авторизации не настроена" });
        return;
      }

      const externalUser = await verifyExternalUserPassword(login, password);
      if (!externalUser) {
        sendJson(res, 401, { error: "Неверные данные для входа" });
        return;
      }

      const siteUser =
        (await findSiteUserByExternalUserId(externalUser.id)) ||
        (await findSiteUserByExternalLogin(login));
      if (!siteUser) {
        sendJson(res, 403, { error: "Пользователь не добавлен" });
        return;
      }

      const syncedUser = await syncLocalExternalUser(siteUser.id, externalUser);
      const session = await createSession(syncedUser?.id || siteUser.id);
      setSessionCookie(res, session.token, session.expires, req);
      sendJson(res, 200, { user: toPublicUser(syncedUser || siteUser) });
      return;
    }

    if (pathname === "/api/auth/logout") {
      const cookies = parseCookies(req.headers.cookie || "");
      const token = cookies[COOKIE_NAME];
      if (token) {
        await deleteSession(token);
      }
      clearSessionCookie(res, req);
      sendJson(res, 200, { ok: true });
      return;
    }

    if (pathname === "/api/auth/me") {
      const user = await getAuth(req, res);
      if (!user) {
        // Return 200 to avoid noisy console errors on initial auth check.
        sendJson(res, 200, { user: null });
        return;
      }
      sendJson(res, 200, { user });
      return;
    }

    if (pathname === "/api/links/wb-articles") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const store = getStoreFromRequest(req);
      const force = url?.searchParams?.get("force") === "1";
      const cardsMap = await getCardsCache(store, force);
      const items = Array.from(cardsMap.entries()).map(([article, info]) => ({
        article,
        title: info?.title || null,
        wbBarcodes: Array.isArray(info?.barcodes) ? info.barcodes : [],
      }));
      items.sort((a, b) => a.article.localeCompare(b.article, "ru-RU"));
      sendJson(res, 200, { updatedAt: new Date().toISOString(), items });
      return;
    }

    if (pathname === "/api/links/ms-barcodes" && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const body = await readJson(req);
      const raw = Array.isArray(body?.articles) ? body.articles : [];
      const articles = raw.map(normalizeArticleKey).filter(Boolean);
      if (articles.length === 0) {
        sendJson(res, 200, { items: [] });
        return;
      }
      const unique = Array.from(new Set(articles));
      const results = new Map();
      const concurrency = 6;
      for (let i = 0; i < unique.length; i += concurrency) {
        const batch = unique.slice(i, i + concurrency);
        const batchResults = await Promise.all(
          batch.map(async (article) => {
            try {
              const info = await getMsProductStatusByArticle(article);
              return { article, barcodes: info.barcodes || [], found: info.found };
            } catch (err) {
              return { article, barcodes: [], found: false, error: err?.message || "MS error" };
            }
          })
        );
        for (const item of batchResults) {
          results.set(item.article, item);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      const items = articles.map((article) => {
        const entry = results.get(article);
        if (!entry) return { article, barcodes: [], missing: true };
        return {
          article,
          barcodes: entry.barcodes || [],
          missing: !entry.found,
          error: entry.error || null,
        };
      });
      sendJson(res, 200, { items });
      return;
    }

    if (pathname === "/api/fbs-stock-check" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const store = getStoreFromRequest(req);
      const force = url?.searchParams?.get("refresh") === "1";
      const state = getFbsStockJobState(store?.id);
      if (force) {
        startFbsStockCheckJob(store?.id, { force: true });
      } else if (!state.report && !state.running) {
        startFbsStockCheckJob(store?.id, { force: false });
      }
      const payload = buildFbsStockJobPayload(getFbsStockJobState(store?.id));
      sendJson(res, 200, payload);
      return;
    }

    if (pathname === "/api/stock-problems" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      try {
        const report = await getStockProblemReportCached(url?.searchParams?.get("force") === "1");
        sendJson(res, 200, report);
      } catch (error) {
        const message = isSkladMovementAccessError(error)
          ? "Не удалось получить отчёт по проблемам остатков"
          : error?.message || "Не удалось получить отчёт по проблемам остатков";
        sendJson(res, isSkladMovementAccessError(error) ? 503 : 500, { error: message });
      }
      return;
    }

    if (pathname === "/api/events") {
      const auth = await requireAuth(req, res, [ROLE_SUPER, ROLE_ADMIN, ROLE_EMPLOYEE]);
      if (!auth) return;
      res.writeHead(200, {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no", // tell nginx not to buffer the SSE stream
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      });
      if (res.flushHeaders) res.flushHeaders();
      addSseClient(res, auth);
      return;
    }

    if (pathname === "/api/users/directory" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_SUPER]);
      if (!auth) return;
      if (!isExternalAuthConfigured()) {
        sendJson(res, 500, { error: "Внешняя база авторизации не настроена" });
        return;
      }
      const users = await listDirectoryUsersForCurrentSite();
      sendJson(res, 200, { users });
      return;
    }

    if (pathname === "/api/users" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_SUPER, ROLE_ADMIN]);
      if (!auth) return;
      let roleFilter = null;
      if (auth.role === ROLE_ADMIN) {
        roleFilter = ROLE_EMPLOYEE;
      } else {
        const requested = url?.searchParams?.get("role");
        if ([ROLE_ADMIN, ROLE_EMPLOYEE, ROLE_SUPER].includes(requested)) {
          roleFilter = requested;
        }
      }
      const users = await listManageableUsers(roleFilter);
      sendJson(res, 200, { users: users.map(toPublicUser) });
      return;
    }

    if (pathname === "/api/users" && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_SUPER]);
      if (!auth) return;
      if (!isExternalAuthConfigured()) {
        sendJson(res, 500, { error: "Внешняя база авторизации не настроена" });
        return;
      }
      const body = await readJson(req);
      const externalUserId = Number(body?.externalUserId);
      let role = body?.role;
      if (auth.role === ROLE_ADMIN) {
        role = ROLE_EMPLOYEE;
      }
      if (!Number.isFinite(externalUserId) || externalUserId < 1) {
        sendJson(res, 400, { error: "Выберите сотрудника из общей базы" });
        return;
      }
      if (![ROLE_ADMIN, ROLE_EMPLOYEE].includes(role)) {
        sendJson(res, 400, { error: "Некорректная роль" });
        return;
      }
      const externalUser = await getExternalUserById(externalUserId);
      if (!externalUser?.employeeId) {
        sendJson(res, 400, { error: "У сотрудника нет учётной записи для входа" });
        return;
      }
      const existing = await dbQuery(
        `select id
         from users
         where auth_source = $1
           and external_user_id = $2
         limit 1`,
        [USER_AUTH_SOURCE_EXTERNAL, externalUser.id]
      );
      if (existing.rows?.[0]?.id) {
        sendJson(res, 409, { error: "Этот сотрудник уже добавлен" });
        return;
      }
      const profile = splitDisplayName(externalUser.fullName, externalUser.login);
      const placeholder = createPlaceholderPasswordHash();
      const result = await dbQuery(
        `insert into users (
           surname,
           name,
           role,
           position,
           password_hash,
           password_salt,
           created_at,
           updated_at,
           site_code,
           auth_source,
           external_user_id,
           external_employee_id,
           external_login,
           external_full_name
         )
         values ($1, $2, $3, $4, $5, $6, now(), now(), $7, $8, $9, $10, $11, $12)
         returning
           id,
           surname,
           name,
           role,
           position,
           created_at,
           site_code,
           auth_source,
           external_user_id,
           external_employee_id,
           external_login,
           external_full_name,
           location_code`,
        [
          profile.surname || externalUser.login,
          profile.name || "",
          role,
          role === ROLE_EMPLOYEE ? EMPLOYEE_POSITION_PACKER : null,
          placeholder.hash,
          placeholder.salt,
          null,
          USER_AUTH_SOURCE_EXTERNAL,
          externalUser.id,
          externalUser.employeeId,
          externalUser.login,
          externalUser.fullName,
        ]
      );
      sendJson(res, 200, { user: toPublicUser(result.rows[0]) });
      return;
    }

    const userStatsMatch = pathname.match(/^\/api\/users\/(\d+)\/stats$/);
    if (userStatsMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_SUPER, ROLE_ADMIN]);
      if (!auth) return;
      const userId = Number(userStatsMatch[1]);
      const target = await findManagedUserById(userId);
      if (!target) {
        sendJson(res, 404, { error: "Пользователь не найден" });
        return;
      }
      if (target.role !== ROLE_EMPLOYEE) {
        sendJson(res, 400, { error: "Статистика доступна только для сотрудников" });
        return;
      }
      const from = normalizeText(url?.searchParams?.get("from"));
      const to = normalizeText(url?.searchParams?.get("to"));
      const parseStatsBoundary = (value, boundary) => {
        const normalized = normalizeText(value);
        if (!normalized) return null;
        if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
          if (boundary === "start") return new Date(`${normalized}T00:00:00+03:00`);
          if (boundary === "end") return new Date(`${normalized}T23:59:59.999+03:00`);
        }
        return new Date(normalized);
      };
      const rangeStart = parseStatsBoundary(from, "start");
      const rangeEndParsed = parseStatsBoundary(to, "end");
      if ((from && Number.isNaN(rangeStart?.getTime())) || (to && Number.isNaN(rangeEndParsed?.getTime()))) {
        sendJson(res, 400, { error: "Некорректный период" });
        return;
      }
      const rangeEnd = rangeEndParsed && /^\d{4}-\d{2}-\d{2}$/.test(String(to || "").trim())
        ? new Date(rangeEndParsed.getTime() + 1)
        : rangeEndParsed;
      const isCollector = getEmployeePositionForUser(target) === EMPLOYEE_POSITION_COLLECTOR;
      const stats = isCollector
        ? await getCollectorSupplyStats(
          userId,
          rangeStart ? rangeStart.toISOString() : null,
          rangeEnd ? rangeEnd.toISOString() : null
        )
        : await getEmployeeStats(
          userId,
          rangeStart ? rangeStart.toISOString() : null,
          rangeEnd ? rangeEnd.toISOString() : null
        );
      sendJson(res, 200, {
        user: toPublicUser(target),
        timezone: MOSCOW_TZ,
        range: {
          from: rangeStart ? rangeStart.toISOString() : null,
          to: rangeEnd ? rangeEnd.toISOString() : null,
        },
        stats,
      });
      return;
    }

    const userMatch = pathname.match(/^\/api\/users\/(\d+)$/);
    if (userMatch) {
      const userId = Number(userMatch[1]);
      const auth = await requireAuth(req, res, [ROLE_SUPER]);
      if (!auth) return;
      const target = await findManagedUserById(userId);
      if (!target) {
        sendJson(res, 404, { error: "Пользователь не найден" });
        return;
      }
      if (auth.role === ROLE_ADMIN && target.role !== ROLE_EMPLOYEE) {
        sendJson(res, 403, { error: "FORBIDDEN" });
        return;
      }

      if (req.method === "DELETE") {
        if (auth.id === target.id) {
          sendJson(res, 400, { error: "Нельзя удалить себя" });
          return;
        }
        await dbQuery("delete from users where id = $1", [target.id]);
        sendJson(res, 200, { ok: true });
        return;
      }

      if (req.method === "PATCH") {
        const body = await readJson(req);
        let role = body?.role ?? target.role;
        if (auth.role === ROLE_ADMIN) {
          role = ROLE_EMPLOYEE;
        }
        if (![ROLE_ADMIN, ROLE_EMPLOYEE].includes(role)) {
          sendJson(res, 400, { error: "Некорректная роль" });
          return;
        }
        const externalUser = await getExternalUserById(target.external_user_id);
        if (!externalUser?.employeeId) {
          sendJson(res, 400, { error: "Сотрудник больше недоступен во внешней базе" });
          return;
        }
        const profile = splitDisplayName(externalUser.fullName, externalUser.login);
        const result = await dbQuery(
          `update users
           set surname = $1,
               name = $2,
               role = $3,
               position = $4,
               external_login = $5,
               external_full_name = $6,
               external_employee_id = $7,
               updated_at = now()
           where id = $8
           returning
             id,
             surname,
             name,
             role,
             position,
             created_at,
             site_code,
             auth_source,
             external_user_id,
             external_employee_id,
             external_login,
             external_full_name,
             location_code`,
          [
            profile.surname || externalUser.login,
            profile.name || "",
            role,
            role === ROLE_EMPLOYEE ? EMPLOYEE_POSITION_PACKER : null,
            externalUser.login,
            externalUser.fullName,
            externalUser.employeeId,
            target.id,
          ]
        );
        sendJson(res, 200, { user: toPublicUser(result.rows[0]) });
        return;
      }
    }

    if (pathname === "/api/settings/scan-issues" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const issues = await listOpenScanIssues();
      sendJson(res, 200, { issues, total: issues.length });
      return;
    }

    const scanIssueAdminMatch = pathname.match(/^\/api\/settings\/scan-issues\/(\d+)\/(resolve|fix|auto-fix)$/);
    if (scanIssueAdminMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const issueId = Number(scanIssueAdminMatch[1]);
      const action = scanIssueAdminMatch[2];
      const issue = await getOpenScanIssueById(issueId);
    if (!issue) {
        sendJson(res, 404, { error: "Ошибка не найдена" });
        return;
      }
      clearEmployeePickWarmCaches();
      let resolved = null;
      if (action === "auto-fix") {
        if (normalizeScanIssueStage(issue.issue_stage) !== SCAN_ISSUE_STAGE_PICK) {
          sendJson(res, 409, { error: "Автоматическая привязка доступна только для сборки товара" });
          return;
        }
        if (!issue.warehouse_product_id) {
          sendJson(res, 409, { error: "Складская карточка товара не определена автоматически" });
          return;
        }
        const barcode = normalizeBarcode(issue.confirmation_scanned_barcode || issue.scanned_barcode || "");
        if (!barcode) {
          sendJson(res, 409, { error: "В ошибке нет подтверждённого штрихкода" });
          return;
        }
        let barcodeResult;
        try {
          barcodeResult = await addWarehouseProductBarcode({ productId: issue.warehouse_product_id, barcode });
        } catch (error) {
          if (error?.isWarehouseBarcodeConflict) {
            sendJson(res, 409, { error: error.message });
            return;
          }
          throw error;
        }
        clearEmployeePickWarmCaches();
        resolved = await resolveScanIssue(issueId, auth.id, "fixed", {
          resolutionType: "auto_barcode",
          resolutionNote: barcodeResult.added
            ? `Штрихкод ${barcode} добавлен в складскую карточку`
            : `Штрихкод ${barcode} уже был в складской карточке`,
          resolvedQuantity: 0,
        });
      } else if (action === "fix") {
        const issueStage = normalizeScanIssueStage(issue.issue_stage);
        const issueOrderId = issue.wb_order_id == null ? null : Number(issue.wb_order_id);
        if (issueStage === SCAN_ISSUE_STAGE_ORDER && Number.isFinite(issueOrderId)) {
          const updateResult = await dbQuery(
            `update supply_orders
             set scan_passed_at = coalesce(scan_passed_at, now()),
                 scan_passed_by = $3,
                 scan_barcode = $4,
                 scan_error = null,
                 updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [issue.supply_id, issueOrderId, auth.id, issue.scanned_barcode || issue.order_barcode || null]
          );
          if (!updateResult.rowCount) {
            sendJson(res, 404, { error: "Заказ не найден" });
            return;
          }
        } else if (issueStage === SCAN_ISSUE_STAGE_PICK) {
          const body = await readJson(req);
          let fixResult;
          try {
            fixResult = await completeWbPickIssueItem(issue, auth.id, body?.confirmedQuantity);
          } catch (error) {
            if (error?.fixConfirmationRequired) {
              sendJson(res, 409, {
                error: error.message,
                code: "FIX_CONFIRM_REQUIRED",
                remainingQuantity: error.remainingQuantity,
              });
              return;
            }
            throw error;
          }
          resolved = await resolveScanIssue(issueId, auth.id, "fixed", {
            resolutionType: "fixed",
            resolutionNote: `Администратор завершил остаток товара: ${fixResult.resolvedQuantity} шт.`,
            resolvedQuantity: fixResult.resolvedQuantity,
          });
        }
      }
      if (!resolved) {
        resolved = await resolveScanIssue(issueId, auth.id, action === "fix" ? "fixed" : "resolved", {
          resolutionType: action === "fix" ? "fixed" : "resolved",
        });
      }
      notifyScanIssueUpdate();
      notifySupplyUpdate(issue.supply_id, true);
      if (action === "fix" || action === "auto-fix") {
        notifyScanIssueFixed(issue);
      }
      sendJson(res, 200, { ok: true, issue: toScanIssuePayload(resolved) });
      return;
    }

    const supplyTrbxMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/trbx$/);
    if (supplyTrbxMatch) {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(supplyTrbxMatch[1]);
      const currentSettings = (await getSupplySettings(supplyId)) || (await ensureSupplySettings(supplyId));
      await getSupplyStore(supplyId, currentSettings?.supply_name || null);
      if (req.method === "POST") {
        const existingState = await getSupplyTrbxState(supplyId);
        const existingBoxCount = Math.max(
          Number(existingState?.summary?.boxCount || 0),
          Array.isArray(existingState?.boxes) ? existingState.boxes.length : 0
        );
        if (existingBoxCount < 1) {
          const store = await getSupplyStore(supplyId, currentSettings?.supply_name || null);
          await createSupplyTrbxes(store, supplyId, 1);
        }
      } else if (req.method !== "GET") {
        sendJson(res, 405, { error: "Method not allowed" });
        return;
      }
      const payload = await syncSupplyTrbxState(supplyId, currentSettings?.supply_name || null, {
        loadMissingPdf: true,
      });
      sendJson(res, 200, payload);
      return;
    }

    const supplyDeliverMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/deliver$/);
    if (supplyDeliverMatch) {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      if (req.method !== "PATCH") {
        sendJson(res, 405, { error: "Method not allowed" });
        return;
      }
      const body = await readJson(req).catch(() => null);
      if (body?.confirmIrreversible !== true) {
        sendJson(res, 400, {
          error: "Подтвердите необратимую передачу поставки в доставку",
        });
        return;
      }
      const supplyId = decodeURIComponent(supplyDeliverMatch[1]);
      const currentSettings = (await getSupplySettings(supplyId)) || (await ensureSupplySettings(supplyId));
      const store = await getSupplyStore(supplyId, currentSettings?.supply_name || null);
      const currentState = await getSupplyTrbxState(supplyId);
      if (currentState?.summary?.done !== true) {
        await transferSupplyToDelivery(store, supplyId);
      }
      const payload = await syncSupplyTrbxState(supplyId, currentSettings?.supply_name || null, {
        loadMissingPdf: true,
      });
      sendJson(res, 200, payload);
      return;
    }

    const supplyBarcodeMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/barcode$/);
    if (supplyBarcodeMatch) {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(supplyBarcodeMatch[1]);
      if (req.method === "GET") {
        const payload = await getSupplyTrbxState(supplyId);
        sendJson(res, 200, payload);
        return;
      }
      if (req.method !== "POST") {
        sendJson(res, 405, { error: "Method not allowed" });
        return;
      }
      const currentSettings = (await getSupplySettings(supplyId)) || (await ensureSupplySettings(supplyId));
      const syncedState = await syncSupplyTrbxState(supplyId, currentSettings?.supply_name || null, {
        loadMissingPdf: true,
      });
      if (syncedState?.summary?.done !== true) {
        sendJson(res, 409, { error: "Поставка ещё не передана в доставку" });
        return;
      }
      if (syncedState?.summary?.supplyQrPdfUrl) {
        sendJson(res, 200, syncedState);
        return;
      }
      const freshSettings = (await getSupplySettings(supplyId)) || currentSettings;
      const store = await getSupplyStore(supplyId, freshSettings?.supply_name || null);
      const prefix =
        freshSettings?.labels_s3_prefix ||
        buildLabelsPrefix(supplyId, freshSettings?.supply_name || supplyId);
      if (!freshSettings?.labels_s3_prefix) {
        await dbQuery(
          "update supply_settings set labels_s3_prefix = $2, updated_at = now() where supply_id = $1",
          [supplyId, prefix]
        );
      }
      await ensureSupplyBarcodePdf(store, supplyId, prefix, freshSettings, { force: true });
      const payload = await getSupplyTrbxState(supplyId);
      sendJson(res, 200, payload);
      return;
    }

    // Preview WB re-sync: fetch the supply's current orders from WB (no DB change)
    // and diff against what we have, so the admin sees exactly what would change.
    const syncPreviewMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/sync-preview$/);
    if (syncPreviewMatch) {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(syncPreviewMatch[1]);
      const settings = (await getSupplySettings(supplyId)) || (await ensureSupplySettings(supplyId));
      const store = await getSupplyStore(supplyId, settings?.supply_name || null);
      const curRes = await dbQuery(
        `select wb_order_id, article, product_name, quantity,
                collected_at is not null as collected, assigned_user_id is not null as assigned
         from supply_orders where supply_id = $1`,
        [supplyId]
      );
      const current = curRes.rows || [];
      const currentById = new Map(current.map((r) => [String(r.wb_order_id), r]));
      let wbOrders = [];
      let fetchOk = true;
      try {
        const orderIds = await fetchSupplyOrderIds(store, supplyId);
        wbOrders = orderIds.map((id) => ({ id }));
      } catch (error) {
        fetchOk = false;
      }
      const wbById = new Map(wbOrders.map((o) => [String(o.id), o]));
      const removed = current
        .filter((r) => !wbById.has(String(r.wb_order_id)))
        .map((r) => ({
          orderId: String(r.wb_order_id),
          article: r.article || null,
          productName: r.product_name || null,
          quantity: Number(r.quantity || 1),
          collected: r.collected === true,
          assigned: r.assigned === true,
        }));
      const added = wbOrders
        .filter((o) => !currentById.has(String(o.id)))
        .map((o) => ({
          orderId: String(o.id),
          article: o.article || null,
          productName: o.productName || null,
          quantity: Number(o.quantity || 1),
        }));
      const fullSyncGuard = await getSupplyFullSyncGuard(supplyId);
      sendJson(res, 200, {
        supplyId,
        fetchOk,
        currentCount: current.length,
        wbCount: wbOrders.length,
        unchangedCount: wbOrders.filter((o) => currentById.has(String(o.id))).length,
        removed,
        added,
        removedCollectedCount: removed.filter((r) => r.collected).length,
        canFullSync: fullSyncGuard.allowed === true,
        fullSyncBlockedReason: fullSyncGuard.allowed === true ? null : fullSyncGuard.reason,
      });
      return;
    }

    // Apply ordinary re-sync: only subtract orders missing from WB.
    const syncApplyMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/sync-apply$/);
    if (syncApplyMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(syncApplyMatch[1]);
      const settings = (await getSupplySettings(supplyId)) || (await ensureSupplySettings(supplyId));
      const beforeRes = await dbQuery("select wb_order_id from supply_orders where supply_id = $1", [supplyId]);
      const beforeIds = new Set((beforeRes.rows || []).map((r) => String(r.wb_order_id)));
      const summary = await syncSupplyStateWithWb(supplyId, settings?.supply_name || null);
      const afterRes = await dbQuery("select wb_order_id from supply_orders where supply_id = $1", [supplyId]);
      const after = afterRes.rows || [];
      sendJson(res, 200, {
        supplyId,
        applied: true,
        beforeCount: beforeIds.size,
        afterCount: after.length,
        addedCount: 0,
        removedCount: Number(summary.removedOrdersCount || 0),
        ignoredAddedCount: Number(summary.ignoredAddedOrdersCount || 0),
      });
      return;
    }

    const fullSyncApplyMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/sync-full-apply$/);
    if (fullSyncApplyMatch) {
      if (req.method !== "POST") {
        sendJson(res, 405, { error: "Метод не поддерживается" });
        return;
      }
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(fullSyncApplyMatch[1]);
      const settings = (await getSupplySettings(supplyId)) || (await ensureSupplySettings(supplyId));
      try {
        const payload = await fullSyncSupplyStateWithWb(supplyId, settings?.supply_name || null);
        sendJson(res, 200, payload);
      } catch (err) {
        sendJson(res, err?.userFacing ? 409 : 500, {
          error: err?.message || "Не удалось выполнить полную синхронизацию.",
          fullSyncBlocked: err?.fullSyncBlocked === true,
        });
      }
      return;
    }

    // Exclude a whole product (article) from a supply: drop its orders and the
    // employee picking items. Used when a product is out of stock / unscannable
    // and blocks assembly (previously fixed by hand in the DB). Order stays in WB.
    const excludeMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/exclude-product$/);
    if (excludeMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(excludeMatch[1]);
      const body = await readJson(req);
      const article = normalizeText(body?.article || "");
      if (!article) {
        sendJson(res, 400, { error: "Не указан артикул" });
        return;
      }
      try {
        const picks = await dbQuery(
          "delete from employee_supply_pick_items where supply_id = $1 and upper(article) = upper($2)",
          [supplyId, article]
        );
        const orders = await dbQuery(
          "delete from supply_orders where supply_id = $1 and upper(article) = upper($2)",
          [supplyId, article]
        );
        sendJson(res, 200, {
          supplyId,
          article,
          removedOrders: orders.rowCount || 0,
          removedPickItems: picks.rowCount || 0,
        });
      } catch (error) {
        sendJson(res, 500, { error: error?.message || "Не удалось исключить товар" });
      }
      return;
    }

    const settingsMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/settings$/);
    if (settingsMatch) {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(settingsMatch[1]);
      const currentSettings =
        (await getSupplySettings(supplyId)) ||
        (await ensureSupplySettings(supplyId));
      await getSupplyStore(supplyId, currentSettings?.supply_name || null);
      if (req.method === "PATCH") {
        const body = await readJson(req);
        const hasAccessMode = typeof body?.accessMode === "string";
        const accessMode = hasAccessMode ? body.accessMode : currentSettings?.access_mode || "hidden";
        if (hasAccessMode && !["hidden", "selected_split"].includes(accessMode)) {
          sendJson(res, 400, { error: "Некорректный режим доступа" });
          return;
        }
        const workflowModeInput =
          typeof body?.employeeWorkflowMode === "string"
            ? body.employeeWorkflowMode
            : typeof body?.workflowMode === "string"
            ? body.workflowMode
            : null;
        const hasWorkflowMode = workflowModeInput !== null;
        if (hasWorkflowMode && !EMPLOYEE_WORKFLOW_MODES.has(normalizeText(workflowModeInput))) {
          sendJson(res, 400, { error: "Некорректный режим сборки" });
          return;
        }
        const workflowMode = hasWorkflowMode
          ? normalizeEmployeeWorkflowMode(workflowModeInput)
          : normalizeEmployeeWorkflowMode(currentSettings?.employee_workflow_mode);
        const hasStockMethod = typeof body?.stockMethodEnabled === "boolean";
        const stockMethodEnabled = hasStockMethod
          ? body.stockMethodEnabled === true
          : isStockMethodEnabled(currentSettings);
        const hasAllowNoScan = typeof body?.allowNoScan === "boolean";
        const hasPartialSafeScan = typeof body?.partialSafeScan === "boolean";
        const hasDesktopPrint = typeof body?.desktopPrint === "boolean";
        const hasAllowEmployeeClose = typeof body?.allowEmployeeClose === "boolean";
        const hasAllowStockShortage = typeof body?.allowStockShortage === "boolean";
        const locationCode = typeof body?.locationCode === "string" ? normalizeLocationCode(body.locationCode) : null;
        const hasLocationCode = Boolean(locationCode);
        if (body?.allowNoScan === true && body?.partialSafeScan === true) {
          sendJson(res, 400, { error: "Полный и частичный режимы без сканирования нельзя включить одновременно" });
          return;
        }
        if (!hasAccessMode && !hasWorkflowMode && !hasStockMethod && !hasAllowNoScan && !hasPartialSafeScan && !hasDesktopPrint && !hasAllowEmployeeClose && !hasAllowStockShortage && !hasLocationCode) {
          sendJson(res, 400, { error: "Нет изменений" });
          return;
        }
        if (hasLocationCode) {
          // Город поставки + ПЕРЕСЧЁТ складского склада ГРАсклад по городу — иначе город менялся,
          // а склад/остатки оставались старыми (город Москва, а склад Ижевск, или наоборот).
          const cityStoreId = normalizeStoreId(currentSettings?.store_id) || "";
          let skladWid = null, skladWname = null;
          try {
            const whRes = await dbQuery(
              `select sklad_warehouse_id, sklad_warehouse_name
               from site_location_warehouse_settings
               where active = true and location_code = $1 and marketplace in ('all','wb')
                 and (coalesce(store_id,'') = '' or store_id = $2) and coalesce(sklad_warehouse_id,0) > 0
               order by (case when marketplace='wb' then 2 else 0 end) + (case when coalesce(store_id,'')=$2 and $2<>'' then 1 else 0 end) desc, id desc
               limit 1`,
              [locationCode, cityStoreId]
            );
            const w = whRes.rows?.[0];
            if (w?.sklad_warehouse_id) { skladWid = Number(w.sklad_warehouse_id); skladWname = w.sklad_warehouse_name || null; }
          } catch {}
          await dbQuery(
            "update supply_settings set location_code = $2, sklad_warehouse_id = coalesce($3, sklad_warehouse_id), sklad_warehouse_name = coalesce($4, sklad_warehouse_name), updated_at = now() where supply_id = $1",
            [supplyId, locationCode, skladWid, skladWname]
          );
        }
        if (hasAllowNoScan) {
          await dbQuery("update supply_settings set allow_no_scan = $2, partial_safe_scan = case when $2 then false else partial_safe_scan end, updated_at = now() where supply_id = $1", [
            supplyId,
            body.allowNoScan === true,
          ]);
        }
        if (hasPartialSafeScan) {
          await dbQuery("update supply_settings set partial_safe_scan = $2, allow_no_scan = case when $2 then false else allow_no_scan end, updated_at = now() where supply_id = $1", [
            supplyId,
            body.partialSafeScan === true,
          ]);
        }
        if (hasDesktopPrint) {
          await dbQuery("update supply_settings set desktop_print = $2, updated_at = now() where supply_id = $1", [
            supplyId,
            body.desktopPrint === true,
          ]);
        }
        if (hasAllowEmployeeClose) {
          await dbQuery("update supply_settings set allow_employee_close = $2, updated_at = now() where supply_id = $1", [
            supplyId,
            body.allowEmployeeClose === true,
          ]);
        }
        if (hasAllowStockShortage) {
          await dbQuery("update supply_settings set allow_stock_shortage = $2, updated_at = now() where supply_id = $1", [
            supplyId,
            body.allowStockShortage === true,
          ]);
        }
        const workflowModeChanged =
          hasWorkflowMode &&
          normalizeEmployeeWorkflowMode(currentSettings?.employee_workflow_mode) !== workflowMode;
        const stockMethodChanged = hasStockMethod && isStockMethodEnabled(currentSettings) !== stockMethodEnabled;
        if ((workflowModeChanged || stockMethodChanged) && (await supplyHasActiveEmployeeWork(supplyId))) {
          sendJson(res, 409, { error: "Нельзя менять метод, пока сотрудник уже работает с поставкой" });
          return;
        }
        if (hasAccessMode) {
          await setSupplyAccessMode(supplyId, accessMode);
        }
        if (accessMode === "hidden") {
          await dbQuery("delete from supply_access_users where supply_id = $1", [supplyId]);
          await clearSupplyAssignmentParts(supplyId);
          await dbQuery(
            "update supply_orders set assigned_user_id = null, assignment_part_id = null, assigned_at = null where supply_id = $1 and collected_at is null",
            [supplyId]
          );
          await setSupplyEmployeeWorkflowStage(supplyId, EMPLOYEE_WORKFLOW_ORDERS_ONLY);
          await resetEmployeePickingForSupply(supplyId, PICKING_STATUS_NOT_REQUIRED);
          await closeOpenEmployeeSessionsForSupply(supplyId, "reassigned");
        }
        if (hasWorkflowMode) {
          await setSupplyEmployeeWorkflowMode(supplyId, workflowMode);
          const nextStage = getEmployeeWorkflowStageForMode(workflowMode, accessMode);
          await setSupplyEmployeeWorkflowStage(supplyId, nextStage);
          await resetEmployeePickingForSupply(
            supplyId,
            nextStage === EMPLOYEE_WORKFLOW_PICK_PENDING ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED
          );
        }
        if (hasStockMethod) {
          await setSupplyStockMethodEnabled(supplyId, stockMethodEnabled);
        }
        notifySupplyUpdate(supplyId, true);
      }
      let settings = await getSupplySettings(supplyId);
      settings = await ensureSupplyLabelsHealthy(supplyId, settings?.supply_name || null, settings);
      let totalResult = await dbQuery(
        `select count(*) as total,
                count(*) filter (where collected_at is not null) as collected
         from supply_orders
         where supply_id = $1`,
        [supplyId]
      );
      let totalOrders = Number(totalResult.rows?.[0]?.total || 0);
      if (req.method !== "PATCH") {
        if (totalOrders === 0) {
          await ensureSupplySnapshotIfEmpty(supplyId, settings?.supply_name || null);
          totalResult = await dbQuery(
            `select count(*) as total,
                    count(*) filter (where collected_at is not null) as collected
             from supply_orders
             where supply_id = $1`,
            [supplyId]
          );
          totalOrders = Number(totalResult.rows?.[0]?.total || 0);
        } else {
          ensureSupplySnapshotIfStale(supplyId, settings?.supply_name || null).catch(() => {});
        }
      }
      const collectedOrders = Number(totalResult.rows?.[0]?.collected || 0);
      const remainingOrders = Math.max(0, totalOrders - collectedOrders);
      const effectiveLabelsTotal =
        settings && settings.labels_total && Number(settings.labels_total) > 0
          ? Number(settings.labels_total)
          : totalOrders;
      const supplyLocation = normalizeLocationCode(settings?.location_code);
      const employees = (await listEmployees())
        .filter((row) => !supplyLocation || normalizeLocationCode(row.location_code) === supplyLocation)
        .map(toEmployeeUser);
      const employeeIds = new Set(employees.map((user) => Number(user.id)));
      const accessUserIds = (await getSupplyAccessUserIds(supplyId))
        .filter((userId) => !supplyLocation || employeeIds.has(Number(userId)));
      await ensureSupplyPickingProgressInitialized(supplyId, settings);
      const progress = await getSupplyProgress(supplyId, settings?.access_mode);
      const assignmentParts = await listSupplyAssignmentParts(supplyId);
      const fullSyncGuard = await getSupplyFullSyncGuard(supplyId);
      sendJson(res, 200, {
        settings: settings
          ? {
              supplyId: settings.supply_id,
              supplyName: settings.supply_name,
              accessMode: settings.access_mode,
              labelsStatus: settings.labels_status,
              labelsFormat: settings.labels_format,
              labelsTotal: effectiveLabelsTotal,
              labelsLoaded: settings.labels_loaded,
              labelsError: settings.labels_error,
              storeId: settings.store_id,
              storeName: settings.store_name,
              locationCode: normalizeLocationCode(settings.location_code),
              skladWarehouseId: Number(settings.sklad_warehouse_id || 0) || null,
              skladWarehouseName: settings.sklad_warehouse_name || null,
              stockMethodEnabled: isStockMethodEnabled(settings),
              allowNoScan: settings.allow_no_scan === true,
              partialSafeScan: settings.partial_safe_scan === true,
              desktopPrint: settings.desktop_print === true,
              allowEmployeeClose: settings.allow_employee_close === true,
              employeeCloseCompletedAt: settings.employee_close_completed_at || null,
              employeeCloseCompletedBy: settings.employee_close_completed_by != null ? Number(settings.employee_close_completed_by) : null,
              allowStockShortage: isAllowStockShortage(settings),
              employeeWorkflowMode: normalizeEmployeeWorkflowMode(settings.employee_workflow_mode),
              employeeWorkflowStage: normalizeEmployeeWorkflowStage(settings.employee_workflow_stage),
              productPickingStartedAt: settings.product_picking_started_at,
              productPickingStartedBy: settings.product_picking_started_by != null ? Number(settings.product_picking_started_by) : null,
              labelsStartedAt: settings.labels_started_at,
              labelsFinishedAt: settings.labels_finished_at,
              manualSync: toManualSyncPayload(settings),
              canFullSync: fullSyncGuard.allowed === true,
              fullSyncBlockedReason: fullSyncGuard.allowed === true ? null : fullSyncGuard.reason,
              assignmentParts,
            }
          : null,
        totals: {
          total: totalOrders,
          collected: collectedOrders,
          remaining: remainingOrders,
        },
        accessUserIds,
        employees,
        progress,
      });
      return;
    }

    const accessMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/access$/);
    if (accessMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(accessMatch[1]);
      const body = await readJson(req);
      const userIds = Array.isArray(body?.userIds)
        ? body.userIds.map((id) => Number(id)).filter((id) => Number.isFinite(id))
        : [];
      await getSupplyStore(supplyId);
      const currentSettings = await getSupplySettings(supplyId);
      const supplyLocation = normalizeLocationCode(currentSettings?.location_code);
      if (!supplyLocation) {
        sendJson(res, 409, { error: "Сначала укажите город поставки" });
        return;
      }
      if (userIds.length) {
        const configuredUsers = await dbQuery(
          `select id
           from users
           where id = any($1::bigint[])
             and role = $2
             and location_code = $3`,
          [userIds, ROLE_EMPLOYEE, supplyLocation]
        );
        const configuredIds = new Set((configuredUsers.rows || []).map((row) => Number(row.id)));
        const invalidIds = userIds.filter((id) => !configuredIds.has(Number(id)));
        if (invalidIds.length) {
          sendJson(res, 400, { error: "В поставке можно выбирать только сотрудников того же города" });
          return;
        }
      }
      await setSupplyAccessUsers(supplyId, userIds);
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { ok: true });
      return;
    }

    const splitMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/split$/);
    if (splitMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(splitMatch[1]);
      await getSupplyStore(supplyId);
      await ensureSupplySnapshot(supplyId);
      const body = await readJson(req).catch(() => ({}));
      const userIds = await getSupplyAccessUserIds(supplyId);
      if (!userIds.length) {
        sendJson(res, 400, { error: "Не выбраны сотрудники" });
        return;
      }
      if (await supplyHasStartedPackingWork(supplyId)) {
        sendJson(res, 409, { error: "Нельзя перераспределять поставку, если упаковщик уже начал сборку заказов" });
        return;
      }
      if (await supplyHasBlockingRedistributeWork(supplyId)) {
        sendJson(res, 409, { error: "Нельзя перераспределять поставку, пока сотрудник ещё собирает товар" });
        return;
      }
      const requestedParts =
        Array.isArray(body?.parts) && body.parts.length
          ? normalizeSupplyAssignmentPartsInput(body.parts, userIds)
          : [...userIds]
              .map((userId, index) => ({
                partNo: index + 1,
                roleMode: ASSIGNMENT_PART_MODE_PICK_PACK,
                pickerUserId: null,
                packerUserId: Number(userId),
                tableNo: null,
              }));
      const settings = await getSupplySettings(supplyId);
      const hasSplitRoles = requestedParts.some(
        (part) => normalizeAssignmentPartMode(part?.roleMode) === ASSIGNMENT_PART_MODE_SPLIT
      );
      if (
        hasSplitRoles &&
        normalizeEmployeeWorkflowMode(settings?.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING
      ) {
        sendJson(res, 400, { error: "Для раздельной сборки включите «Метод сборки»" });
        return;
      }
      if (hasSplitRoles && !isStockMethodEnabled(settings)) {
        sendJson(res, 400, { error: "Для раздельной сборки включите «Складской метод»" });
        return;
      }
      // === ПРОВЕРКА ОСТАТКОВ ПЕРЕД ОТКРЫТИЕМ СБОРКИ ===
      // Заказ не должен попасть в сборку, если товара нет на складе (или меньше нужного).
      if (!isAllowStockShortage(settings)) {
        const stockRows = await dbQuery(
          `select barcode, max(article) as article, max(product_name) as product_name, sum(quantity)::int as qty
           from supply_orders
           where supply_id = $1 and collected_at is null and coalesce(barcode, '') <> ''
           group by barcode`,
          [supplyId]
        );
        const shortages = [];
        // Склад ГОРОДА поставки (а не хардкод Ижевска) — иначе для Москвы проверяли остатки Ижевска.
        const stockWh = getSkladWarehouseOptionsFromSettings(settings);
        await mapWithConcurrency(stockRows.rows || [], 3, async (row) => {
          const barcode = String(row.barcode || "").trim();
          if (!barcode) return;
          const required = Math.max(1, Number(row.qty || 1));
          try {
            const ms = await getSkladFbsStockStatusByBarcode(barcode, {
              warehouseId: stockWh.warehouseId,
              storeName: stockWh.warehouseName,
            });
            if (ms?.status === "ok") {
              const stock = Number(ms.stock ?? 0);
              if (stock < required) shortages.push({ article: row.article, productName: row.product_name, stock, required });
            } else if (ms?.status === "not_found") {
              shortages.push({ article: row.article, productName: row.product_name, stock: 0, required });
            }
          } catch { /* инфра-ошибка склада — не блокируем */ }
        });
        if (shortages.length) {
          const lines = shortages
            .sort((a, b) => String(a.article || "").localeCompare(String(b.article || "")))
            .map((s) => `${s.article || "—"} «${s.productName || "—"}»: на складе ${s.stock}, нужно ${s.required}`)
            .join("; ");
          sendJson(res, 409, {
            error: `Недостаточно товара на складе — эти позиции нельзя собрать. Занесите остатки на склад и откройте сборку заново. ${lines}`,
          });
          return;
        }
      }
      await setSupplyAccessMode(supplyId, "selected_split");
      await closeOpenEmployeeSessionsForSupply(supplyId, "reassigned");
      await clearSupplyAssignmentParts(supplyId);
      const parts = await createSupplyAssignmentParts(supplyId, requestedParts, auth.id);
      const result = await assignSupplyOrdersToParts(supplyId, parts, "split");
      const nextStage = getEmployeeWorkflowStageForMode(settings?.employee_workflow_mode, "selected_split");
      await createSupplyAssignmentSessionsFromParts(supplyId, auth.id, "split", result.parts, nextStage);
      await setSupplyEmployeeWorkflowStage(supplyId, nextStage);
      await resetEmployeePickingForSupply(
        supplyId,
        nextStage === EMPLOYEE_WORKFLOW_PICK_PENDING ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED
      );
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, {
        total: result.total,
        assigned: result.assigned,
        parts: result.parts,
      });
      return;
    }

    const redistributeMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/redistribute$/);
    if (redistributeMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(redistributeMatch[1]);
      await getSupplyStore(supplyId);
      await ensureSupplySnapshot(supplyId);
      const body = await readJson(req).catch(() => ({}));
      const userIds = await getSupplyAccessUserIds(supplyId);
      if (!userIds.length) {
        sendJson(res, 400, { error: "Не выбраны сотрудники" });
        return;
      }
      if (await supplyHasStartedPackingWork(supplyId)) {
        sendJson(res, 409, { error: "Нельзя перераспределять поставку, если упаковщик уже начал сборку заказов" });
        return;
      }
      if (await supplyHasBlockingRedistributeWork(supplyId)) {
        sendJson(res, 409, { error: "Нельзя перераспределять поставку, пока сотрудник ещё собирает товар" });
        return;
      }
      const carryover = await captureSupplyRedistributeCarryover(supplyId);
      const requestedParts =
        Array.isArray(body?.parts) && body.parts.length
          ? normalizeSupplyAssignmentPartsInput(body.parts, userIds)
          : [...userIds]
              .map((userId, index) => ({
                partNo: index + 1,
                roleMode: ASSIGNMENT_PART_MODE_PICK_PACK,
                pickerUserId: null,
                packerUserId: Number(userId),
                tableNo: null,
              }));
      const settings = await getSupplySettings(supplyId);
      const hasSplitRoles = requestedParts.some(
        (part) => normalizeAssignmentPartMode(part?.roleMode) === ASSIGNMENT_PART_MODE_SPLIT
      );
      if (
        hasSplitRoles &&
        normalizeEmployeeWorkflowMode(settings?.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING
      ) {
        sendJson(res, 400, { error: "Для раздельной сборки включите «Метод сборки»" });
        return;
      }
      if (hasSplitRoles && !isStockMethodEnabled(settings)) {
        sendJson(res, 400, { error: "Для раздельной сборки включите «Складской метод»" });
        return;
      }
      // === ПРОВЕРКА ОСТАТКОВ ПЕРЕД ОТКРЫТИЕМ СБОРКИ ===
      // Заказ не должен попасть в сборку, если товара нет на складе (или меньше нужного).
      if (!isAllowStockShortage(settings)) {
        const stockRows = await dbQuery(
          `select barcode, max(article) as article, max(product_name) as product_name, sum(quantity)::int as qty
           from supply_orders
           where supply_id = $1 and collected_at is null and coalesce(barcode, '') <> ''
           group by barcode`,
          [supplyId]
        );
        const shortages = [];
        // Склад ГОРОДА поставки (а не хардкод Ижевска) — иначе для Москвы проверяли остатки Ижевска.
        const stockWh = getSkladWarehouseOptionsFromSettings(settings);
        await mapWithConcurrency(stockRows.rows || [], 3, async (row) => {
          const barcode = String(row.barcode || "").trim();
          if (!barcode) return;
          const required = Math.max(1, Number(row.qty || 1));
          try {
            const ms = await getSkladFbsStockStatusByBarcode(barcode, {
              warehouseId: stockWh.warehouseId,
              storeName: stockWh.warehouseName,
            });
            if (ms?.status === "ok") {
              const stock = Number(ms.stock ?? 0);
              if (stock < required) shortages.push({ article: row.article, productName: row.product_name, stock, required });
            } else if (ms?.status === "not_found") {
              shortages.push({ article: row.article, productName: row.product_name, stock: 0, required });
            }
          } catch { /* инфра-ошибка склада — не блокируем */ }
        });
        if (shortages.length) {
          const lines = shortages
            .sort((a, b) => String(a.article || "").localeCompare(String(b.article || "")))
            .map((s) => `${s.article || "—"} «${s.productName || "—"}»: на складе ${s.stock}, нужно ${s.required}`)
            .join("; ");
          sendJson(res, 409, {
            error: `Недостаточно товара на складе — эти позиции нельзя собрать. Занесите остатки на склад и откройте сборку заново. ${lines}`,
          });
          return;
        }
      }
      await setSupplyAccessMode(supplyId, "selected_split");
      await closeOpenEmployeeSessionsForSupply(supplyId, "reassigned");
      await clearSupplyAssignmentParts(supplyId);
      const parts = await createSupplyAssignmentParts(supplyId, requestedParts, auth.id);
      const result = await assignSupplyOrdersToParts(supplyId, parts, "redistribute");
      const nextStage = getEmployeeWorkflowStageForMode(settings?.employee_workflow_mode, "selected_split");
      const createdSessions = await createSupplyAssignmentSessionsFromParts(
        supplyId,
        auth.id,
        "redistribute",
        result.parts,
        nextStage
      );
      await setSupplyEmployeeWorkflowStage(supplyId, nextStage);
      await resetEmployeePickingForSupply(
        supplyId,
        nextStage === EMPLOYEE_WORKFLOW_PICK_PENDING ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED
      );
      await applySupplyRedistributeCarryover(supplyId, carryover, result.parts, createdSessions, settings);
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, {
        total: result.total,
        assigned: result.assigned,
        parts: result.parts,
      });
      return;
    }

    const productPickingStartMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/product-picking\/start$/);
    if (productPickingStartMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(productPickingStartMatch[1]);
      await getSupplyStore(supplyId);
      await ensureSupplySnapshot(supplyId);
      const accessUserIds = await getSupplyAccessUserIds(supplyId);
      if (!accessUserIds.length) {
        sendJson(res, 400, { error: "Сначала распределите поставку между сотрудниками" });
        return;
      }
      const settings = await getSupplySettings(supplyId);
      if (!settings || settings.access_mode !== "selected_split") {
        sendJson(res, 400, { error: "Поставка должна быть доступна выбранным сотрудникам" });
        return;
      }
      if (normalizeEmployeeWorkflowMode(settings.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING) {
        sendJson(res, 400, { error: "Для этой поставки включён старый метод сборки" });
        return;
      }
      await setSupplyEmployeeWorkflowStage(supplyId, EMPLOYEE_WORKFLOW_PICK_ACTIVE, auth.id);
      await resetEmployeePickingForSupply(supplyId, PICKING_STATUS_PENDING);
      const activeSettings = await getSupplySettings(supplyId);
      await ensureSupplyPickingProgressInitialized(supplyId, activeSettings);
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { ok: true });
      return;
    }

    const resetMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/reset-access$/);
    if (resetMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(resetMatch[1]);
      await getSupplyStore(supplyId);
      await resetSupplyAccess(supplyId);
      await closeOpenEmployeeSessionsForSupply(supplyId, "reassigned");
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { ok: true });
      return;
    }

    const labelsPdfMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/labels\.pdf$/);
    if (labelsPdfMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const supplyId = decodeURIComponent(labelsPdfMatch[1]);
      await getSupplyStore(supplyId);
      const labels = await buildSupplyOrderLabelsPdf(supplyId);
      if (!labels.total) {
        sendJson(res, 400, { error: "В поставке нет заказов" });
        return;
      }
      if (!labels.buffer) {
        sendJson(res, 409, {
          error: `Загружены не все этикетки: ${labels.loaded} из ${labels.total}`,
        });
        return;
      }
      const safeSupplyId = String(supplyId).replace(/[^a-zA-Z0-9_-]+/g, "-");
      res.writeHead(200, {
        "Content-Type": "application/pdf",
        "Content-Disposition": `inline; filename="wb-order-labels-${safeSupplyId}.pdf"`,
        "Content-Length": labels.buffer.length,
        "Cache-Control": "no-store",
      });
      res.end(labels.buffer);
      return;
    }

    const labelsMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/labels$/);
    if (labelsMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(labelsMatch[1]);
      const body = await readJson(req);
      const force = body?.force === true;
      startLabelsJob(supplyId, body?.supplyName || null, force).catch(() => {});
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { ok: true });
      return;
    }

    const manualSyncMatch = pathname.match(/^\/api\/supplies\/([^/]+)\/sync$/);
    if (manualSyncMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(manualSyncMatch[1]);
      const body = await readJson(req).catch(() => null);
      const result = await startSupplyManualSyncJob(supplyId, body?.supplyName || null);
      sendJson(res, 200, {
        ok: true,
        started: result?.started === true,
        sync: result?.sync || null,
      });
      return;
    }

    const employeeSuppliesMatch = pathname === "/api/employee/supplies";
    if (employeeSuppliesMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const employeeLocation = await getConfiguredEmployeeLocation(auth.id);
      if (employeeLocation.error) {
        sendJson(res, employeeLocation.errorStatus || 409, { error: employeeLocation.error });
        return;
      }
      const settingsResult = await dbQuery(
         `select supply_id, supply_name, access_mode, store_id, store_name,
                 location_code, sklad_warehouse_id, sklad_warehouse_name,
                 employee_workflow_mode, employee_workflow_stage, allow_no_scan, partial_safe_scan, desktop_print,
                 allow_employee_close, employee_close_completed_at, employee_close_completed_by
         from supply_settings
         where access_mode = 'selected_split'
           and location_code = $1`,
        [employeeLocation.locationCode]
      );
      const allSettings = settingsResult.rows || [];
      if (!allSettings.length) {
        sendJson(res, 200, { supplies: [] });
        return;
      }
      const storeIds = new Set();
      for (const setting of allSettings) {
        const storeId = resolveSettingsStoreId(setting);
        if (storeId) storeIds.add(storeId);
      }
      const activeByStore = new Map();
      await Promise.all(
        Array.from(storeIds).map(async (storeId) => {
          activeByStore.set(storeId, await getActiveSupplyIdSet(storeId));
        })
      );
      const supplyTotalsResult = await dbQuery(
        `select supply_id,
                count(*)::int as total,
                count(*) filter (where collected_at is not null)::int as collected
         from supply_orders
         where supply_id = any($1::text[])
         group by supply_id`,
        [allSettings.map((row) => row.supply_id)]
      );
      const supplyTotalsById = new Map(
        (supplyTotalsResult.rows || []).map((row) => [
          row.supply_id,
          { total: Number(row.total || 0), collected: Number(row.collected || 0) },
        ])
      );
      const accessResult = await dbQuery(
        "select supply_id from supply_access_users where user_id = $1",
        [auth.id]
      );
      const allowedSet = new Set(accessResult.rows.map((row) => row.supply_id));
      const sessionsResult = await dbQuery(
        `select *
         from employee_supply_sessions
         where user_id = $1 and finished_at is null
         order by assigned_at desc, id desc`,
        [auth.id]
      );
      const sessionsBySupply = new Map();
      for (const row of sessionsResult.rows || []) {
        if (!sessionsBySupply.has(row.supply_id)) {
          sessionsBySupply.set(row.supply_id, row);
        }
      }
      const activeSession = await getEmployeeActiveSession(auth.id);
      const supplies = [];
      for (const setting of allSettings) {
        const storeKey = resolveSettingsStoreId(setting);
        const activeSet = storeKey ? activeByStore.get(storeKey) : null;
        if (activeSet && !activeSet.has(setting.supply_id)) {
          continue;
        }
        const allCounts = supplyTotalsById.get(setting.supply_id) || { total: 0, collected: 0 };
        const closeReady =
          setting.allow_employee_close === true &&
          !setting.employee_close_completed_at &&
          allCounts.total > 0 &&
          allCounts.collected >= allCounts.total;
        // Explicit local assignment remains valid after WB removes an older supply from its active list.
        if (!allowedSet.has(setting.supply_id)) {
          continue;
        }
        let openSession = sessionsBySupply.get(setting.supply_id) || null;
        const hasAssignmentParts = !openSession ? await supplyHasAssignmentParts(setting.supply_id) : Number(openSession.assignment_part_id || 0) > 0;
        let counts = await getEmployeeSupplyCounts(
          setting.supply_id,
          auth.id,
          openSession?.assignment_part_id != null ? Number(openSession.assignment_part_id) : null
        );
        if (!openSession && counts.remaining > 0 && !hasAssignmentParts) {
          openSession = await ensureEmployeeAssignmentSession(setting.supply_id, auth.id, counts.remaining);
          counts = await getEmployeeSupplyCounts(setting.supply_id, auth.id);
        }
        if (!openSession && hasAssignmentParts && !closeReady) continue;
        if (!openSession && counts.total < 1 && !closeReady) continue;
        const store = resolveStore(normalizeStoreId(setting.store_id));
        openSession = await ensureEmployeeSessionPickingStatus(setting, openSession);
        const assignmentPart = await getSupplyAssignmentPartForSession(openSession);
        const workflow = buildEmployeeWorkflowState(setting, openSession, activeSession, assignmentPart);
        const total = closeReady
          ? allCounts.total
          : counts.total > 0
          ? counts.total
          : Number(openSession?.assigned_order_count || 0);
        const collected = closeReady
          ? allCounts.collected
          : counts.collected > 0
          ? counts.collected
          : Number(openSession?.collected_order_count || 0);
        const remaining = Math.max(0, total - collected);
        if (total < 1 && !workflow.isWaitingForPicker && !workflow.isWaitingForTable && !closeReady) continue;
        supplies.push({
          id: setting.supply_id,
          name: setting.supply_name,
          accessMode: setting.access_mode,
          storeId: setting.store_id || null,
          storeName: setting.store_name || store?.name || null,
          locationCode: normalizeLocationCode(setting.location_code),
          skladWarehouseId: Number(setting.sklad_warehouse_id || 0) || null,
          skladWarehouseName: setting.sklad_warehouse_name || null,
          workflowMode: workflow.workflowMode,
          workflowStage: workflow.workflowStage,
          currentStage: closeReady ? "closing" : workflow.currentStage,
          sessionRole: workflow.sessionRole,
          tableNo: workflow.tableNo,
          handoffStatus: workflow.handoffStatus,
          allowNoScan: workflow.allowNoScan,
          desktopPrint: workflow.desktopPrint,
          allowEmployeeClose: setting.allow_employee_close === true,
          closeReady,
          employeeCloseCompletedAt: setting.employee_close_completed_at || null,
          productPickingRequired: workflow.productPickingRequired,
          productPickingActive: workflow.productPickingActive,
          remaining,
          total,
          collected,
          session: toEmployeeSessionPayload(openSession),
          assignmentPart,
          canStart: closeReady ? false : Boolean(workflow.canStartPicking || workflow.canStartPacking),
          canStartPicking: workflow.canStartPicking,
          canFinishPicking: workflow.canFinishPicking,
          canConfirmTablePlacement: workflow.canConfirmTablePlacement,
          canStartPacking: workflow.canStartPacking,
          canWork: closeReady ? false : workflow.canWork,
          isWaitingAdmin: workflow.isWaitingAdmin,
          isWaitingForPicker: workflow.isWaitingForPicker,
          isWaitingForTable: workflow.isWaitingForTable,
          hasAnotherActiveSession: Boolean(activeSession && activeSession.supply_id !== setting.supply_id),
        });
      }
      sendJson(res, 200, { supplies });
      return;
    }

    const employeeClosingMatch = pathname.match(
      /^\/api\/employee\/supplies\/([^/]+)\/closing(?:\/(trbx|deliver|barcode|complete))?$/
    );
    if (employeeClosingMatch) {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;

      const supplyId = decodeURIComponent(employeeClosingMatch[1]);
      const action = employeeClosingMatch[2] || "state";
      const expectedMethod =
        action === "state" ? "GET" : action === "deliver" ? "PATCH" : "POST";
      if (req.method !== expectedMethod) {
        sendJson(res, 405, { error: "Method not allowed" });
        return;
      }

      const context = await getEmployeeSupplyClosingContext(supplyId, auth, {
        allowCompleted: action === "state" || action === "complete",
      });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }

      if (action === "trbx") {
        const existingState = await getSupplyTrbxState(supplyId);
        const existingBoxCount = Math.max(
          Number(existingState?.summary?.boxCount || 0),
          Array.isArray(existingState?.boxes) ? existingState.boxes.length : 0
        );
        if (existingBoxCount < 1) {
          await createSupplyTrbxes(context.store, supplyId, 1);
        }
        const payload = await syncSupplyTrbxState(supplyId, context.settings?.supply_name || null, {
          loadMissingPdf: true,
        });
        sendJson(res, 200, toEmployeeSupplyClosingPayload(payload, context));
        return;
      }

      if (action === "deliver") {
        const body = await readJson(req).catch(() => null);
        if (body?.confirmIrreversible !== true) {
          sendJson(res, 400, {
            error: "Подтвердите необратимую передачу поставки в доставку",
          });
          return;
        }
        const existingState = await getSupplyTrbxState(supplyId);
        const boxCount = Math.max(
          Number(existingState?.summary?.boxCount || 0),
          Array.isArray(existingState?.boxes) ? existingState.boxes.length : 0
        );
        if (boxCount < 1) {
          sendJson(res, 409, { error: "Сначала создайте грузоместо" });
          return;
        }
        if (existingState?.summary?.done !== true) {
          await transferSupplyToDelivery(context.store, supplyId);
        }
        const payload = await syncSupplyTrbxState(supplyId, context.settings?.supply_name || null, {
          loadMissingPdf: true,
        });
        sendJson(res, 200, toEmployeeSupplyClosingPayload(payload, context));
        return;
      }

      if (action === "barcode") {
        const syncedState = await syncSupplyTrbxState(supplyId, context.settings?.supply_name || null, {
          loadMissingPdf: true,
        });
        if (syncedState?.summary?.done !== true) {
          sendJson(res, 409, { error: "Поставка ещё не передана в доставку" });
          return;
        }
        if (!syncedState?.summary?.supplyQrPdfUrl) {
          const freshSettings = (await getSupplySettings(supplyId)) || context.settings;
          const prefix =
            freshSettings?.labels_s3_prefix ||
            buildLabelsPrefix(supplyId, freshSettings?.supply_name || supplyId);
          if (!freshSettings?.labels_s3_prefix) {
            await dbQuery(
              "update supply_settings set labels_s3_prefix = $2, updated_at = now() where supply_id = $1",
              [supplyId, prefix]
            );
          }
          await ensureSupplyBarcodePdf(context.store, supplyId, prefix, freshSettings, { force: true });
        }
        const payload = await getSupplyTrbxState(supplyId);
        sendJson(res, 200, toEmployeeSupplyClosingPayload(payload, context));
        return;
      }

      if (action === "complete") {
        const payload = await syncSupplyTrbxState(supplyId, context.settings?.supply_name || null, {
          loadMissingPdf: true,
        });
        const boxes = Array.isArray(payload?.boxes) ? payload.boxes : [];
        if (payload?.summary?.done !== true) {
          sendJson(res, 409, { error: "Сначала передайте поставку в доставку" });
          return;
        }
        if (!boxes.length || boxes.some((box) => !box?.pdfUrl)) {
          sendJson(res, 409, { error: "QR-код грузоместа ещё не готов" });
          return;
        }
        if (!payload?.summary?.supplyQrPdfUrl) {
          sendJson(res, 409, { error: "QR-код поставки ещё не готов" });
          return;
        }
        const completed = await dbQuery(
          `update supply_settings
           set employee_close_completed_at = coalesce(employee_close_completed_at, now()),
               employee_close_completed_by = coalesce(employee_close_completed_by, $2),
               updated_at = now()
           where supply_id = $1
           returning employee_close_completed_at, employee_close_completed_by`,
          [supplyId, auth.id]
        );
        const completedRow = completed.rows?.[0] || {};
        context.settings.employee_close_completed_at = completedRow.employee_close_completed_at || null;
        context.settings.employee_close_completed_by = completedRow.employee_close_completed_by || auth.id;
        notifySupplyUpdate(supplyId, true);
        sendJson(res, 200, {
          ok: true,
          ...toEmployeeSupplyClosingPayload(payload, context),
        });
        return;
      }

      const payload = await syncSupplyTrbxState(supplyId, context.settings?.supply_name || null, {
        loadMissingPdf: true,
      });
      sendJson(res, 200, toEmployeeSupplyClosingPayload(payload, context));
      return;
    }

    const employeeSessionMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/session$/);
    if (employeeSessionMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeSessionMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const workflow = context.workflow || buildEmployeeWorkflowState(context.settings, context.session, context.activeSession, context.assignmentPart);
      ensureEmployeeOrderScanIndexWarm(supplyId, auth, context.settings);
      sendJson(res, 200, {
        supply: {
          id: supplyId,
          name: context.settings?.supply_name || null,
          storeId: context.settings?.store_id || null,
          storeName: context.settings?.store_name || null,
          locationCode: normalizeLocationCode(context.settings?.location_code),
          skladWarehouseId: Number(context.settings?.sklad_warehouse_id || 0) || null,
          skladWarehouseName: context.settings?.sklad_warehouse_name || null,
          workflowMode: workflow.workflowMode,
           workflowStage: workflow.workflowStage,
           productPickingStartedAt: context.settings?.product_picking_started_at || null,
           allowEmployeeClose: context.settings?.allow_employee_close === true,
           employeeCloseCompletedAt: context.settings?.employee_close_completed_at || null,
           total: context.counts.total,
          collected: context.counts.collected,
          remaining: context.counts.remaining,
        },
        session: toEmployeeSessionPayload(context.session),
        assignmentPart: context.assignmentPart,
        workflow: {
          workflowMode: workflow.workflowMode,
          workflowStage: workflow.workflowStage,
          currentStage: workflow.currentStage,
          sessionRole: workflow.sessionRole,
          splitAssignment: workflow.splitAssignment,
          handoffStatus: workflow.handoffStatus,
          tableNo: workflow.tableNo,
          allowNoScan: workflow.allowNoScan,
           desktopPrint: workflow.desktopPrint,
           allowEmployeeClose: workflow.allowEmployeeClose,
          productPickingRequired: workflow.productPickingRequired,
          productPickingActive: workflow.productPickingActive,
          canStartPicking: workflow.canStartPicking,
          canFinishPicking: workflow.canFinishPicking,
          canConfirmTablePlacement: workflow.canConfirmTablePlacement,
          canStartPacking: workflow.canStartPacking,
          isWaitingAdmin: workflow.isWaitingAdmin,
          isWaitingForPicker: workflow.isWaitingForPicker,
          isWaitingForTable: workflow.isWaitingForTable,
        },
        canStart: Boolean(workflow.canStartPicking || workflow.canStartPacking),
        canWork: workflow.canWork,
        activeSession: context.activeSession ? toEmployeeSessionPayload(context.activeSession) : null,
      });
      return;
    }

    const employeeStartMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/start$/);
    if (employeeStartMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeStartMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const workflow = context.workflow || buildEmployeeWorkflowState(context.settings, context.session, context.activeSession, context.assignmentPart);
      const starter =
        workflow.productPickingRequired && workflow.pickingStatus !== PICKING_STATUS_FINISHED
          ? startEmployeeProductPicking
          : startEmployeeSupplySession;
      const { session, pickItems = [], error } = await starter(supplyId, auth.id);
      if (error) {
        sendJson(res, 409, { error });
        return;
      }
      ensureEmployeeOrderScanIndexWarm(supplyId, auth, context.settings);
      const activeSession = await getEmployeeActiveSession(auth.id);
      const nextAssignmentPart = await getSupplyAssignmentPartForSession(session);
      const nextWorkflow = buildEmployeeWorkflowState(context.settings, session, activeSession, nextAssignmentPart);
      const pendingPickItems =
        nextWorkflow.currentStage === "picking"
          ? pickItems.filter((row) => Number(row?.picked_qty || 0) < Number(row?.required_qty || 0))
          : [];
      if (nextWorkflow.currentStage === "picking") {
        void ensureEmployeePickScanIndexWarm(session, pendingPickItems, getSkladWarehouseOptionsFromSettings(context.settings));
        await warmWbImages(pendingPickItems);
      }
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, {
        session: toEmployeeSessionPayload(session),
        assignmentPart: nextAssignmentPart,
        workflow: {
          workflowMode: nextWorkflow.workflowMode,
          workflowStage: nextWorkflow.workflowStage,
          currentStage: nextWorkflow.currentStage,
          sessionRole: nextWorkflow.sessionRole,
          splitAssignment: nextWorkflow.splitAssignment,
          handoffStatus: nextWorkflow.handoffStatus,
          tableNo: nextWorkflow.tableNo,
          allowNoScan: nextWorkflow.allowNoScan,
          desktopPrint: nextWorkflow.desktopPrint,
          productPickingRequired: nextWorkflow.productPickingRequired,
          productPickingActive: nextWorkflow.productPickingActive,
          canStartPicking: nextWorkflow.canStartPicking,
          canFinishPicking: nextWorkflow.canFinishPicking,
          canConfirmTablePlacement: nextWorkflow.canConfirmTablePlacement,
          canStartPacking: nextWorkflow.canStartPacking,
          isWaitingAdmin: nextWorkflow.isWaitingAdmin,
          isWaitingForPicker: nextWorkflow.isWaitingForPicker,
          isWaitingForTable: nextWorkflow.isWaitingForTable,
        },
        pickItems:
          nextWorkflow.currentStage === "picking"
            ? pendingPickItems.map(toEmployeePickItemPayload)
            : [],
      });
      return;
    }

    const employeeTableConfirmMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/table-confirm$/);
    if (employeeTableConfirmMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeTableConfirmMatch[1]);
      const { session, part, error } = await confirmEmployeeSupplyTablePlacement(supplyId, auth.id);
      if (error) {
        sendJson(res, 409, { error });
        return;
      }
      const settings = await getSupplySettings(supplyId);
      const activeSession = await getEmployeeActiveSession(auth.id);
      const workflow = buildEmployeeWorkflowState(settings, session, activeSession, part);
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, {
        session: toEmployeeSessionPayload(session),
        assignmentPart: part,
        workflow: {
          workflowMode: workflow.workflowMode,
          workflowStage: workflow.workflowStage,
          currentStage: workflow.currentStage,
          sessionRole: workflow.sessionRole,
          splitAssignment: workflow.splitAssignment,
          handoffStatus: workflow.handoffStatus,
          tableNo: workflow.tableNo,
          allowNoScan: workflow.allowNoScan,
          desktopPrint: workflow.desktopPrint,
          productPickingRequired: workflow.productPickingRequired,
          productPickingActive: workflow.productPickingActive,
          canStartPicking: workflow.canStartPicking,
          canFinishPicking: workflow.canFinishPicking,
          canConfirmTablePlacement: workflow.canConfirmTablePlacement,
          canStartPacking: workflow.canStartPacking,
          isWaitingAdmin: workflow.isWaitingAdmin,
          isWaitingForPicker: workflow.isWaitingForPicker,
          isWaitingForTable: workflow.isWaitingForTable,
        },
      });
      return;
    }

    const employeePickItemsMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/pick-items$/);
    if (employeePickItemsMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeePickItemsMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      if (!context.workflow?.productPickingRequired) {
        sendJson(res, 409, { error: "Для этой поставки сборка товара не активирована" });
        return;
      }
      if (context.workflow?.isWaitingAdmin) {
        sendJson(res, 409, { error: "Администратор ещё не запустил сборку товара" });
        return;
      }
      if (
        normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
        !context.session?.picking_started_at ||
        context.session?.picking_finished_at
      ) {
        sendJson(res, 409, { error: "Сначала нажмите «Старт»" });
        return;
      }
      const rows = await ensureEmployeeProductPickItems(context.session);
      const refreshedSession = (await refreshEmployeePickingSessionSummary(context.session.id)) || context.session;
      const pendingRows = rows.filter((row) => Number(row?.picked_qty || 0) < Number(row?.required_qty || 0));
      const rowsWithCells = await enrichEmployeePickItemsWithMsCells(pendingRows, context.settings);
      void ensureEmployeePickScanIndexWarm(context.session, rowsWithCells, getSkladWarehouseOptionsFromSettings(context.settings));
      await warmWbImages(rowsWithCells);
      sendJson(res, 200, {
        items: rowsWithCells.map(toEmployeePickItemPayload),
        session: toEmployeeSessionPayload(refreshedSession),
      });
      return;
    }

    const employeePickScanMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/pick-scan$/);
    if (employeePickScanMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const pickScanTraceStartedAt = Date.now();
      let pickScanTraceLastAt = pickScanTraceStartedAt;
      const pickScanTraceSteps = {};
      let pickScanTraceBarcode = "";
      let pickScanTraceSessionId = null;
      const markPickScanTrace = (label) => {
        const now = Date.now();
        pickScanTraceSteps[label] = now - pickScanTraceLastAt;
        pickScanTraceLastAt = now;
      };
      const flushPickScanTrace = (status, extra = {}) => {
        try {
          console.log(
            JSON.stringify({
              tag: "WB_PICK_SCAN_TRACE",
              at: new Date().toISOString(),
              supplyId: decodeURIComponent(employeePickScanMatch[1]),
              userId: auth.id,
              sessionId: pickScanTraceSessionId,
              barcode: pickScanTraceBarcode || null,
              status,
              totalMs: Date.now() - pickScanTraceStartedAt,
              steps: pickScanTraceSteps,
              ...extra,
            })
          );
        } catch {}
      };
      const supplyId = decodeURIComponent(employeePickScanMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      pickScanTraceSessionId = context?.session?.id || null;
      markPickScanTrace("context");
      if (context.error) {
        flushPickScanTrace("context_error", { error: context.error });
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      if (!context.workflow?.productPickingRequired) {
        flushPickScanTrace("product_picking_disabled");
        sendJson(res, 409, { error: "Для этой поставки сборка товара не активирована" });
        return;
      }
      if (context.workflow?.isWaitingAdmin) {
        flushPickScanTrace("waiting_admin");
        sendJson(res, 409, { error: "Администратор ещё не запустил сборку товара" });
        return;
      }
      if (
        normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
        !context.session?.picking_started_at ||
        context.session?.picking_finished_at
      ) {
        flushPickScanTrace("session_not_started");
        sendJson(res, 409, { error: "Сначала нажмите «Старт»" });
        return;
      }
      let pickItems = getCachedEmployeePickItems(context.session.id) || await listEmployeePickItemsForSession(context.session.id);
      if (!pickItems.length) {
        pickItems = await ensureEmployeeProductPickItems(context.session);
      }
      markPickScanTrace("pick_items");
      const body = await readJson(req);
      const inputBarcode = normalizeBarcode(typeof body?.barcode === "string" ? body.barcode : "");
      pickScanTraceBarcode = inputBarcode;
      markPickScanTrace("read_body");
      if (!inputBarcode) {
        flushPickScanTrace("empty_barcode");
        sendJson(res, 400, { error: "Отсканируйте штрихкод товара" });
        return;
      }
      let matchInfo = await findEmployeePickItemMatchesByBarcode(
        { ...context.session, pick_items: pickItems },
        inputBarcode,
        getSkladWarehouseOptionsFromSettings(context.settings)
      );
      let usedForceFreshLookup = false;
      if (!matchInfo.lookupFailed && (matchInfo.matches.length === 0 || matchInfo.ambiguous || matchInfo.multipleScanProducts)) {
        usedForceFreshLookup = true;
        matchInfo = await findEmployeePickItemMatchesByBarcode(
          { ...context.session, pick_items: pickItems },
          inputBarcode,
          { ...getSkladWarehouseOptionsFromSettings(context.settings), forceFresh: true }
        );
      }
      markPickScanTrace("match");
      if (!matchInfo.matches.length) {
        flushPickScanTrace(matchInfo.lookupFailed ? "lookup_failed" : "not_found", {
          forceFresh: usedForceFreshLookup,
          ambiguous: Boolean(matchInfo.ambiguous),
          multipleScanProducts: Boolean(matchInfo.multipleScanProducts),
        });
        if (matchInfo.lookupFailed) {
          sendJson(res, 503, {
            error:
              "Не удалось проверить товар. Повторите сканирование. Если это ваш товар, нажмите «Ошибка» и отправьте админу.",
          });
          return;
        }
        if (matchInfo.multipleScanProducts) {
          sendJson(res, 409, { error: buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts) });
          return;
        }
        sendJson(res, 404, {
          error:
            "Товар не найден в вашей сборке товара. Если это ваш товар, нажмите «Ошибка» и отправьте админу.",
        });
        return;
      }
      if (matchInfo.multipleScanProducts) {
        flushPickScanTrace("multiple_scan_products", { forceFresh: usedForceFreshLookup });
        sendJson(res, 409, { error: buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts) });
        return;
      }
      if (matchInfo.ambiguous) {
        flushPickScanTrace("ambiguous", { forceFresh: usedForceFreshLookup, matchCount: matchInfo.matches.length });
        sendJson(res, 409, {
          error: buildAmbiguousPickWarning(inputBarcode, matchInfo.matches, {
            multipleScanProducts: matchInfo.multipleScanProducts,
            scanProducts: matchInfo.scanProducts,
          }),
        });
        return;
      }
      const item = matchInfo.matches[0];
      const updatedItemResult = await dbQuery(
        `update employee_supply_pick_items
         set picked_qty = least(required_qty, picked_qty + 1),
             updated_at = now()
         where id = $1 and picked_qty < required_qty
         returning *`,
        [item.id]
      );
      const updatedItem = updatedItemResult.rows?.[0] || item;
      markPickScanTrace("update_pick_item");
      if (isStockMethodEnabled(context.settings)) {
        const externalEmployeeId = getAuthExternalEmployeeId(auth);
        if (!externalEmployeeId) {
          await dbQuery(
            `update employee_supply_pick_items
             set picked_qty = greatest(0, picked_qty - 1),
                 updated_at = now()
             where id = $1`,
            [item.id]
          );
          flushPickScanTrace("employee_not_bound", { article: item?.article || null });
          sendJson(res, 409, { error: "Сотрудник не привязан к складской системе" });
          return;
        }
        try {
          const movementHints = getEmployeePickScanMovementHints(context.session.id, item);
          const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0];
          const movementResult = await moveProductFromShelfToEmployee({
            externalEmployeeId,
            warehouseId: getSkladWarehouseOptionsFromSettings(context.settings).warehouseId,
            barcodes: [inputBarcode, item?.barcode],
            resolvedProductIds: movementHints?.productIds || [],
            quantity: 1,
            skipUnavailable: isAllowStockShortage(context.settings),
            preferredShelfId: movementHints?.preferredShelfId || firstCell?.id || null,
            source: "wb_pick_scan",
            notes: `WB: автоперемещение товара сотруднику при сборке товара. Поставка ${supplyId}, товар ${item?.article || "—"} (${item?.product_name || item?.productName || "—"}), отсканирован ${inputBarcode}.`,
            negativeNotes: `WB: автоперемещение товара сотруднику при сборке товара с уходом склада в минус. Поставка ${supplyId}, товар ${item?.article || "—"} (${item?.product_name || item?.productName || "—"}), отсканирован ${inputBarcode}.`,
          });
          await recordStockShortageSkip({ supplyId, stage: "pick_scan", article: item?.article, barcode: inputBarcode || item?.barcode, requestedQuantity: 1, movementResult, userId: auth.id });
          markPickScanTrace("movement");
        } catch (error) {
          await dbQuery(
            `update employee_supply_pick_items
             set picked_qty = greatest(0, picked_qty - 1),
                 updated_at = now()
             where id = $1`,
            [item.id]
          );
          markPickScanTrace("movement_failed");
          flushPickScanTrace("movement_error", {
            article: item?.article || null,
            error: error?.message || String(error),
            forceFresh: usedForceFreshLookup,
          });
          const message = isSkladMovementAccessError(error)
            ? "Не удалось записать перемещение в склад. Повторите сканирование."
            : error?.message || "Не удалось записать перемещение в склад";
          sendJson(res, isSkladMovementAccessError(error) ? 503 : 409, { error: message });
          return;
        }
      }
      const updatedItemWithCells =
        Array.isArray(item?.ms_cells) && item.ms_cells.length
          ? { ...updatedItem, ms_cells: item.ms_cells }
          : await enrichSingleEmployeePickItemWithMsCells(updatedItem);
      syncEmployeePickScanIndexItem(context.session.id, updatedItem);
      const collectedDelta = Math.max(0, Number(updatedItem.picked_qty || 0) - Number(item.picked_qty || 0));
      const refreshedSession =
        (await applyEmployeePickingSessionDelta(context.session.id, 0, collectedDelta)) ||
        (await refreshEmployeePickingSessionSummary(context.session.id)) ||
        context.session;
      markPickScanTrace("finalize");
      notifySupplyUpdate(supplyId, true);
      const liveExternalEmployeeId = getAuthExternalEmployeeId(auth);
      if (collectedDelta > 0 && liveExternalEmployeeId) {
        writeSborkaLiveEventSafe({
          externalEmployeeId: liveExternalEmployeeId,
          eventType: "pick",
          orderId: null,
          productName: updatedItemWithCells?.product_name || updatedItem?.product_name || item?.product_name || null,
          barcode: inputBarcode,
          quantity: collectedDelta,
          marketplace: "wb",
        });
      }
      flushPickScanTrace("ok", {
        article: item?.article || null,
        forceFresh: usedForceFreshLookup,
        collectedDelta,
      });
      await warmWbImages([updatedItemWithCells]);
      sendJson(res, 200, {
        item: toEmployeePickItemPayload(updatedItemWithCells),
        session: toEmployeeSessionPayload(refreshedSession),
        warning: "",
        completed:
          Number(refreshedSession.picking_required_qty || 0) > 0 &&
          Number(refreshedSession.picking_collected_qty || 0) >= Number(refreshedSession.picking_required_qty || 0),
      });
      return;
    }

    const employeePickIssueMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/pick-issue$/);
    if (employeePickIssueMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeePickIssueMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      if (!context.workflow?.productPickingRequired) {
        sendJson(res, 409, { error: "Для этой поставки сборка товара не активирована" });
        return;
      }
      if (context.workflow?.isWaitingAdmin) {
        sendJson(res, 409, { error: "Администратор ещё не запустил сборку товара" });
        return;
      }
      if (
        normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
        !context.session?.picking_started_at ||
        context.session?.picking_finished_at
      ) {
        sendJson(res, 409, { error: "Сначала нажмите «Старт»" });
        return;
      }
      let pickItems = await listEmployeePickItemsForSession(context.session.id);
      if (!pickItems.length) {
        pickItems = await ensureEmployeeProductPickItems(context.session);
      }
      const body = await readJson(req);
      const scannedBarcode = normalizeBarcode(typeof body?.barcode === "string" ? body.barcode : "");
      const originalScannedBarcode = normalizeBarcode(
        typeof body?.originalBarcode === "string" ? body.originalBarcode : ""
      );
      const pickItemId = Number(body?.pickItemId);
      if (!scannedBarcode) {
        sendJson(res, 400, { error: "Отсканируйте штрихкод товара" });
        return;
      }
      if (!originalScannedBarcode) {
        sendJson(res, 400, { error: "Сначала отсканируйте товар в основном поле сборки" });
        return;
      }
      if (originalScannedBarcode !== scannedBarcode) {
        sendJson(res, 409, { error: "Повторный штрихкод не совпадает с первым сканированием" });
        return;
      }
      const pendingItems = pickItems.filter(
        (item) => Number(item.picked_qty || 0) < Number(item.required_qty || 0)
      );
      const selectedItem =
        Number.isFinite(pickItemId) && pickItemId > 0
          ? pendingItems.find((item) => Number(item.id) === pickItemId) || null
          : null;
      if (!selectedItem) {
        sendJson(res, 400, { error: "Выберите товар из списка" });
        return;
      }
      await warmWbImages([selectedItem]);
      const warehouseSnapshot = await resolveIssueWarehouseProductSnapshot({
        article: selectedItem?.article || null,
        barcodes: [selectedItem?.barcode].filter(Boolean),
      });
      const remainingQuantity = Math.max(
        0,
        Number(selectedItem?.required_qty || 0) - Number(selectedItem?.picked_qty || 0)
      );
      const issue = await createOrUpdateEmployeeScanIssue({
        supplyId,
        orderId: null,
        pickItemId: Number(selectedItem.id),
        pickItemKey: selectedItem.item_key || selectedItem.itemKey || null,
        employeeId: auth.id,
        issueStage: SCAN_ISSUE_STAGE_PICK,
        contextNote: buildPickIssueContextNote([selectedItem]),
        orderBarcode: selectedItem?.barcode ? normalizeBarcode(selectedItem.barcode) : null,
        scannedBarcode,
        originalScannedBarcode,
        confirmationScannedBarcode: scannedBarcode,
        article: selectedItem?.article || null,
        marketplaceArticle: selectedItem?.article || null,
        productName: selectedItem?.product_name || selectedItem?.productName || null,
        nmId: selectedItem?.nm_id || selectedItem?.nmId || null,
        ...warehouseSnapshot,
        imageUrl: getWbCachedImage(selectedItem?.nm_id || selectedItem?.nmId || null),
        remainingQuantity,
      });
      notifyScanIssueUpdate();
      sendJson(res, 200, { ok: true, issue: toScanIssuePayload(issue) });
      return;
    }

    const employeePickIssuesMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/pick-issues$/);
    if (employeePickIssuesMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeePickIssuesMatch[1]);
      const result = await dbQuery(
        `select i.*
         from employee_scan_issues i
         where i.supply_id = $1
           and i.employee_id = $2
           and i.issue_stage = $3
         order by i.created_at desc, i.id desc
         limit 50`,
        [supplyId, auth.id, SCAN_ISSUE_STAGE_PICK]
      );
      sendJson(res, 200, { issues: (result.rows || []).map(toScanIssuePayload) });
      return;
    }

    const employeePickFinishMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/pick-finish$/);
    if (employeePickFinishMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeePickFinishMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const { session, error } = await finishEmployeeProductPicking(supplyId, auth.id);
      if (error) {
        sendJson(res, 409, { error });
        return;
      }
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { session: toEmployeeSessionPayload(session) });
      return;
    }

    const employeePickCollectManualMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/pick-items\/(\d+)\/collect-without-scan$/);
    if (employeePickCollectManualMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeePickCollectManualMatch[1]);
      const pickItemId = Number(employeePickCollectManualMatch[2]);
      const context = await getEmployeeSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      if (
        normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
        !context.session?.picking_started_at ||
        context.session?.picking_finished_at
      ) {
        sendJson(res, 409, { error: "Сборка товара ещё не начата" });
        return;
      }
      const item = await getEmployeePickItemById(context.session.id, pickItemId);
      if (!item) {
        sendJson(res, 404, { error: "Строка сборки не найдена" });
        return;
      }
      const allowNoScanMode = context.settings?.allow_no_scan === true;
      await warmWarehouseNames([item]);
      const partialBundleMode =
        context.settings?.partial_safe_scan === true && warehouseIsBundleForRow(item);
      if (item.bypass_without_scan_allowed !== true && !allowNoScanMode && !partialBundleMode) {
        sendJson(res, 409, { error: "Администратор ещё не разрешил собрать этот товар без штрихкода" });
        return;
      }
      const stockMethodEnabled = isStockMethodEnabled(context.settings);
      const externalEmployeeId = stockMethodEnabled ? getAuthExternalEmployeeId(auth) : null;
      if (stockMethodEnabled && !externalEmployeeId) {
        sendJson(res, 409, { error: "Сотрудник не привязан к складской системе" });
        return;
      }
      const updatedItem = await collectEmployeePickItemWithoutScan(pickItemId, {
        requireBypass: !allowNoScanMode && !partialBundleMode,
      });
      if (!updatedItem) {
        sendJson(res, 409, { error: "Товар уже собран" });
        return;
      }
      const collectedDelta = Math.max(0, Number(updatedItem.picked_qty || 0) - Number(item.picked_qty || 0));
      if (stockMethodEnabled && collectedDelta > 0) {
        try {
          const movementHints = getEmployeePickScanMovementHints(context.session.id, item);
          const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0];
          const movementResult = await moveProductFromShelfToEmployee({
            externalEmployeeId,
            warehouseId: getSkladWarehouseOptionsFromSettings(context.settings).warehouseId,
            barcodes: [item?.barcode],
            resolvedProductIds: movementHints?.productIds || [],
            quantity: collectedDelta,
            skipUnavailable: isAllowStockShortage(context.settings),
            preferredShelfId: movementHints?.preferredShelfId || firstCell?.id || null,
            source: "wb_pick_scan",
            notes: `WB: bypass pick movement to employee. Supply ${supplyId}, item ${item?.article || "-"} (${item?.product_name || item?.productName || "-"}) without scan.`,
            negativeNotes: `WB: bypass pick movement to employee with negative shelf stock. Supply ${supplyId}, item ${item?.article || "-"} (${item?.product_name || item?.productName || "-"}) without scan.`,
          });
          await recordStockShortageSkip({ supplyId, stage: "pick_without_scan", article: item?.article, barcode: item?.barcode, requestedQuantity: collectedDelta, movementResult, userId: auth.id });
        } catch (error) {
          await dbQuery(
            `update employee_supply_pick_items
             set picked_qty = $2,
                 updated_at = now()
             where id = $1`,
            [pickItemId, Number(item.picked_qty || 0)]
          );
          const message = isSkladMovementAccessError(error)
            ? "Не удалось записать перемещение в склад. Повторите действие."
            : error?.message || "Не удалось записать перемещение в склад";
          sendJson(res, isSkladMovementAccessError(error) ? 503 : 409, { error: message });
          return;
        }
      }
      syncEmployeePickScanIndexItem(context.session.id, updatedItem);
      const refreshedSession =
        (await applyEmployeePickingSessionDelta(context.session.id, 0, collectedDelta)) ||
        (await refreshEmployeePickingSessionSummary(context.session.id)) ||
        context.session;
      notifySupplyUpdate(supplyId, true);
      const updatedItemWithCells = await enrichSingleEmployeePickItemWithMsCells(updatedItem);
      await warmWbImages([updatedItemWithCells]);
      sendJson(res, 200, {
        item: toEmployeePickItemPayload(updatedItemWithCells),
        session: toEmployeeSessionPayload(refreshedSession),
        completed:
          Number(refreshedSession.picking_required_qty || 0) > 0 &&
          Number(refreshedSession.picking_collected_qty || 0) >= Number(refreshedSession.picking_required_qty || 0),
      });
      return;
    }

    const employeeItemsMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/items$/);
    if (employeeItemsMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeItemsMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: true });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const result = await dbQuery(
        `select article, barcode, product_name, nm_id,
                count(*)::int as count
         from supply_orders
         where supply_id = $1 and collected_at is null and assigned_user_id = $2
         group by article, barcode, product_name, nm_id
         order by count desc`,
        [supplyId, auth.id]
      );
      const itemRows = result.rows || [];
      await warmWarehouseNames(itemRows);
      await warmWbImages(itemRows);
      const items = itemRows.map((row) => ({
        article: row.article,
        barcode: row.barcode,
        productName: warehouseNameForRow(row) || row.product_name,
        nmId: row.nm_id,
        imageUrl: proxifyWbCardImage(getWbCachedImage(row.nm_id)),
        count: Number(row.count || 0),
      }));
      sendJson(res, 200, { items });
      return;
    }

    const employeeOrdersMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/orders$/);
    if (employeeOrdersMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeOrdersMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: true });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const params = [supplyId];
      let where = "supply_id = $1 and collected_at is null";
      params.push(auth.id);
      where += " and assigned_user_id = $2";
      const article = normalizeArticleKey(url?.searchParams?.get("article"));
      const barcode = normalizeText(url?.searchParams?.get("barcode"));
      const nmId = url?.searchParams?.get("nmId");
      if (article) {
        params.push(article);
        where += ` and article = $${params.length}`;
      }
      if (barcode) {
        params.push(barcode);
        where += ` and barcode = $${params.length}`;
      }
      if (nmId) {
        params.push(Number(nmId));
        where += ` and nm_id = $${params.length}`;
      }
      const result = await dbQuery(
        `select wb_order_id, order_created_at, article, barcode, product_name, nm_id,
                quantity, sticker_url, sticker_barcode,
                scan_passed_at, scan_barcode,
                label_scan_passed_at, label_scan_barcode
         from supply_orders
         where ${where}
         order by order_created_at asc nulls last, wb_order_id asc`,
        params
      );
      if ((result.rows || []).some((row) => !row.sticker_url)) {
        void ensureSupplyLabelsHealthy(supplyId, context.settings?.supply_name || null, context.settings).catch(() => {});
      }
      ensureEmployeeOrderScanIndexWarm(supplyId, auth, context.settings);
      const orders = await fillMissingOrderInfo(supplyId, result.rows || [], context.store);
      await warmWarehouseNames(orders);
      await warmWbImages(orders);
      sendJson(res, 200, {
        orders: orders.map(toEmployeeOrderPayload),
      });
      return;
    }

    // Уже СОБРАННЫЕ заказы сотрудника — окно «Собранные заказы» (только просмотр +
    // перепечать этикетки, завершать нельзя). В основной список не попадают.
    const employeeCollectedMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/collected-orders$/);
    if (employeeCollectedMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeCollectedMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: false });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const result = await dbQuery(
        `select wb_order_id, order_created_at, article, barcode, product_name, nm_id,
                quantity, sticker_url, sticker_barcode,
                scan_passed_at, scan_barcode,
                label_scan_passed_at, label_scan_barcode
         from supply_orders
         where supply_id = $1 and collected_at is not null and assigned_user_id = $2
          order by collected_at desc nulls last, wb_order_id asc
         limit 500`,
        [supplyId, auth.id]
      );
      const orders = await fillMissingOrderInfo(supplyId, result.rows || [], context.store);
      await warmWarehouseNames(orders);
      await warmWbImages(orders);
      sendJson(res, 200, { orders: orders.map(toEmployeeOrderPayload) });
      return;
    }

    const employeeLookupMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/scan-order$/);
    if (employeeLookupMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(employeeLookupMatch[1]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: true });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const body = await readJson(req);
      const inputBarcode = normalizeBarcode(typeof body?.barcode === "string" ? body.barcode : "");
      if (!inputBarcode) {
        sendJson(res, 400, { error: "Отсканируйте штрихкод товара" });
        return;
      }

      let lookupResult = await findFirstEmployeeOrderByBarcode(supplyId, auth, context.settings, context.store, inputBarcode);
      if (!lookupResult.lookupFailed && (!lookupResult.order || lookupResult.warning || lookupResult.multipleScanProducts)) {
        lookupResult = await findFirstEmployeeOrderByBarcode(
          supplyId,
          auth,
          context.settings,
          context.store,
          inputBarcode,
          { forceFresh: true }
        );
      }
      const { order, matches, lookupFailed, warning, multipleScanProducts, scanProducts } = lookupResult;
      if (!order) {
        if (lookupFailed) {
          sendJson(res, 503, { error: "Не удалось проверить товар, повторите сканирование" });
          return;
        }
        if (multipleScanProducts) {
          const errorMessage = buildMultipleScanProductsReason(inputBarcode, scanProducts);
          sendJson(res, 409, { error: errorMessage });
          return;
        }
        const errorMessage = "Товар не найден в этой поставке";
        sendJson(res, 404, { error: errorMessage });
        return;
      }
      if (multipleScanProducts) {
        const errorMessage = buildMultipleScanProductsReason(inputBarcode, scanProducts);
        sendJson(res, 409, { error: errorMessage });
        return;
      }
      if (warning) {
        sendJson(res, 409, { error: warning });
        return;
      }

      if (!order.scan_passed_at) {
        await dbQuery(
          `update supply_orders
           set scan_passed_at = now(), scan_passed_by = $3, scan_barcode = $4,
               scan_error = null, updated_at = now()
           where supply_id = $1 and wb_order_id = $2`,
          [supplyId, Number(order.wb_order_id), auth.id, inputBarcode]
        );
        order.scan_passed_at = new Date().toISOString();
        order.scan_barcode = inputBarcode;
        notifySupplyUpdate(supplyId, true);
        const liveExternalEmployeeId = getAuthExternalEmployeeId(auth);
        if (liveExternalEmployeeId) {
          writeSborkaLiveEventSafe({
            externalEmployeeId: liveExternalEmployeeId,
            eventType: "order_start",
            orderId: String(order.wb_order_id),
            productName: order.product_name || null,
            barcode: inputBarcode,
            quantity: 1,
            marketplace: "wb",
          });
        }
      }

      await warmWarehouseNames([order]);
      await warmWbImages([order]);
      sendJson(res, 200, {
        item: {
          productName: warehouseNameForRow(order) || order.product_name || null,
          barcode: order.barcode ?? null,
          article: order.article ?? null,
          entityType: warehouseEntityTypeForRow(order),
          isBundle: warehouseIsBundleForRow(order),
          bundleComponents: warehouseBundleComponentsForRow(order),
          bundleJarCount: warehouseBundleJarCountForRow(order),
          nmId: order.nm_id ?? null,
        },
        order: toEmployeeOrderPayload(order),
        warning: warning || "",
      });
      return;
    }

    const scanMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/orders\/(\d+)\/scan$/);
    if (scanMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(scanMatch[1]);
      const orderId = Number(scanMatch[2]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: true });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const body = await readJson(req);
      const rawBarcode = typeof body?.barcode === "string" ? body.barcode : "";
      const inputBarcode = normalizeBarcode(rawBarcode);
      const liveExternalEmployeeId = getAuthExternalEmployeeId(auth);
      if (!inputBarcode) {
        sendJson(res, 400, { error: "Отсканируйте повторно" });
        return;
      }
      const orderResult = await dbQuery(
        "select article, barcode, product_name, nm_id, scan_passed_at, assigned_user_id from supply_orders where supply_id = $1 and wb_order_id = $2",
        [supplyId, orderId]
      );
      const order = orderResult.rows?.[0] || null;
      if (!order) {
        sendJson(res, 404, { error: "Заказ не найден" });
        return;
      }
      if (order.assigned_user_id && Number(order.assigned_user_id) !== Number(auth.id)) {
        sendJson(res, 403, { error: "FORBIDDEN" });
        return;
      }
      if (order.scan_passed_at) {
        sendJson(res, 200, { ok: true, passed: true });
        return;
      }
      let matchInfo = await listEmployeeOrderMatchesByBarcode(supplyId, auth, context.settings, context.store, inputBarcode);
      if (!matchInfo.lookupFailed && (matchInfo.matches.length === 0 || matchInfo.ambiguous || matchInfo.multipleScanProducts)) {
        matchInfo = await listEmployeeOrderMatchesByBarcode(
          supplyId,
          auth,
          context.settings,
          context.store,
          inputBarcode,
          { forceFresh: true }
        );
      }
      if (matchInfo.matches.length === 0) {
        if (matchInfo.lookupFailed) {
          sendJson(res, 503, { error: "Не удалось проверить товар, повторите сканирование" });
          return;
        }
        if (matchInfo.multipleScanProducts) {
          const errorMessage = buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts);
          sendJson(res, 409, { error: errorMessage });
          return;
        }
        sendJson(res, 400, { error: "Отсканируйте повторно" });
        return;
      }
      if (matchInfo.multipleScanProducts) {
        const errorMessage = buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts);
        sendJson(res, 409, { error: errorMessage });
        return;
      }
      const sameOrderMatched = matchInfo.matches.some((item) => Number(item.wb_order_id) === Number(orderId));
      if (!sameOrderMatched) {
        sendJson(res, 400, { error: "Отсканирован другой товар" });
        return;
      }
      const finalWarning = matchInfo.multipleScanProducts
        ? buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts)
        : matchInfo.ambiguous
        ? buildAmbiguousScanWarning(inputBarcode, matchInfo.matches, {
            multipleScanProducts: matchInfo.multipleScanProducts,
            scanProducts: matchInfo.scanProducts,
          })
        : "";
      if (finalWarning) {
        sendJson(res, 409, { error: finalWarning });
        return;
      }
      await dbQuery(
        `update supply_orders
         set scan_passed_at = now(), scan_passed_by = $3, scan_barcode = $4,
             scan_error = null, updated_at = now()
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId, auth.id, inputBarcode]
      );
      notifySupplyUpdate(supplyId, true);
      if (liveExternalEmployeeId) {
        writeSborkaLiveEventSafe({
          externalEmployeeId: liveExternalEmployeeId,
          eventType: "order_start",
          orderId: String(orderId),
          productName: order.product_name || null,
          barcode: inputBarcode,
          quantity: 1,
          marketplace: "wb",
        });
      }
      sendJson(res, 200, { ok: true, passed: true, warning: finalWarning });
      return;
    }

    const scanIssueMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/orders\/(\d+)\/scan-issue$/);
    if (scanIssueMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(scanIssueMatch[1]);
      const orderId = Number(scanIssueMatch[2]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: true });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const body = await readJson(req);
      const scannedBarcode = normalizeBarcode(typeof body?.barcode === "string" ? body.barcode : "");
      if (!scannedBarcode) {
        sendJson(res, 400, { error: "Отсканируйте штрихкод товара" });
        return;
      }
      const orderResult = await dbQuery(
        `select wb_order_id, article, barcode, product_name, nm_id, assigned_user_id
         from supply_orders
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId]
      );
      const order = orderResult.rows?.[0] || null;
      if (!order) {
        sendJson(res, 404, { error: "Заказ не найден" });
        return;
      }
      if (order.assigned_user_id && Number(order.assigned_user_id) !== Number(auth.id)) {
        sendJson(res, 403, { error: "FORBIDDEN" });
        return;
      }
      const issue = await createOrUpdateEmployeeScanIssue({
        supplyId,
        orderId,
        employeeId: auth.id,
        orderBarcode: normalizeBarcode(order.barcode),
        scannedBarcode,
        article: order.article || null,
        productName: order.product_name || null,
        nmId: order.nm_id || null,
      });
      notifyScanIssueUpdate();
      sendJson(res, 200, { ok: true, issue: toScanIssuePayload(issue) });
      return;
    }

    const completeNoScanMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/orders\/(\d+)\/complete-no-scan$/);
    if (completeNoScanMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(completeNoScanMatch[1]);
      const orderId = Number(completeNoScanMatch[2]);
      const context = await getEmployeeSupplyLabelScanContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const allowNoScanMode = context.settings?.allow_no_scan === true;
      const partialSafeScanMode = context.settings?.partial_safe_scan === true;
      if (!allowNoScanMode && !partialSafeScanMode) {
        sendJson(res, 403, { error: "Для этой поставки не включён режим без сканирования" });
        return;
      }
      const liveExternalEmployeeId = getAuthExternalEmployeeId(auth);
      const orderResult = await dbQuery(
        `select assigned_user_id, label_scan_passed_at, barcode, article, product_name, quantity, scan_barcode
         from supply_orders
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId]
      );
      const order = orderResult.rows?.[0] || null;
      if (!order) {
        sendJson(res, 404, { error: "Заказ не найден" });
        return;
      }
      if (order.assigned_user_id && Number(order.assigned_user_id) !== Number(auth.id)) {
        sendJson(res, 403, { error: "FORBIDDEN" });
        return;
      }
      if (order.label_scan_passed_at) {
        sendJson(res, 200, { ok: true, passed: true });
        return;
      }
      await warmWarehouseNames([order]);
      const containsBundle = warehouseIsBundleForRow(order);
      if (!allowNoScanMode && (!partialSafeScanMode || !containsBundle)) {
        sendJson(res, 403, { error: "Частичный режим разрешает ручное завершение только заказов с комплектом" });
        return;
      }
      const completionMode = allowNoScanMode ? "NO_SCAN_MODE" : "PARTIAL_SAFE_SCAN";
      const collectedVia = allowNoScanMode ? "no_scan" : "partial_safe_scan";
      const collectedAt = new Date();
      await dbQuery(
        `update supply_orders
         set scan_passed_at = coalesce(scan_passed_at, now()), scan_passed_by = coalesce(scan_passed_by, $3), scan_error = null,
             label_scan_passed_at = now(), label_scan_passed_by = $3, label_scan_barcode = $6, label_scan_error = null,
             collected_at = coalesce(collected_at, $4), collected_by = coalesce(collected_by, $3), collected_via = coalesce(collected_via, $7), collected_session_id = coalesce(collected_session_id, $5),
             updated_at = now()
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId, auth.id, collectedAt, context.session?.id || null, completionMode, collectedVia]
      );
      if (isStockMethodEnabled(context.settings)) {
        if (!liveExternalEmployeeId) {
          await dbQuery(
            `update supply_orders
             set label_scan_passed_at = null, label_scan_passed_by = null, label_scan_barcode = null,
                 collected_at = null, collected_by = null, collected_via = null, collected_session_id = null, updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [supplyId, orderId]
          );
          sendJson(res, 409, { error: "Сотрудник не привязан к складской системе" });
          return;
        }
        try {
          const movementResult = await writeOffEmployeeInventory({
            externalEmployeeId: liveExternalEmployeeId,
            warehouseId: getSkladWarehouseOptionsFromSettings(context.settings).warehouseId,
            barcodes: [order.scan_barcode, order.barcode],
            quantity: Math.max(1, Number(order.quantity || 1)),
            skipUnavailable: isAllowStockShortage(context.settings),
            source: "wb_collect",
            notes: `WB: автосписание (режим без сканирования). Поставка ${supplyId}, заказ ${orderId}, товар ${order.article || "—"} (${order.product_name || "—"}).`,
            negativeNotes: `WB: автосписание (режим без сканирования) с уходом сотрудника в минус. Поставка ${supplyId}, заказ ${orderId}, товар ${order.article || "—"} (${order.product_name || "—"}).`,
          });
          await recordStockShortageSkip({ supplyId, orderId, stage: "collect_no_scan", article: order.article, barcode: order.scan_barcode || order.barcode, requestedQuantity: Math.max(1, Number(order.quantity || 1)), movementResult, userId: auth.id });
        } catch (error) {
          await dbQuery(
            `update supply_orders
             set label_scan_passed_at = null, label_scan_passed_by = null, label_scan_barcode = null,
                 collected_at = null, collected_by = null, collected_via = null, collected_session_id = null, updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [supplyId, orderId]
          );
          const message = isSkladMovementAccessError(error)
            ? "Не удалось списать товар со склада сотрудника. Повторите действие."
            : error?.message || "Не удалось списать товар у сотрудника";
          sendJson(res, isSkladMovementAccessError(error) ? 503 : 409, { error: message });
          return;
        }
      }
      let sessionFinished = false;
      try {
        if (context.session?.id) {
          await recordCollectedOrderForSession(context.session.id, collectedAt);
        }
        if (liveExternalEmployeeId) {
          writeSborkaLiveEventSafe({
            externalEmployeeId: liveExternalEmployeeId,
            eventType: "order_complete",
            orderId: String(orderId),
            marketplace: "wb",
          });
        }
        sessionFinished = Boolean(await finalizeEmployeeSupplySessionIfCompleted(supplyId, auth.id));
        notifySupplyUpdate(supplyId, true);
        if (!sessionFinished) {
          notifySupplyUpdate(supplyId);
        }
      } catch (error) {
        console.error("WB complete-no-scan post-process failed", {
          supplyId,
          orderId,
          userId: auth.id,
          error: error?.message || String(error || ""),
        });
        notifySupplyUpdate(supplyId, true);
      }
      sendJson(res, 200, { ok: true, passed: true, sessionFinished });
      return;
    }

    const labelScanMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/orders\/(\d+)\/label-scan$/);
    if (labelScanMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(labelScanMatch[1]);
      const orderId = Number(labelScanMatch[2]);
      const context = await getEmployeeSupplyLabelScanContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const body = await readJson(req);
      const rawBarcode = typeof body?.barcode === "string" ? body.barcode : "";
      const inputBarcode = normalizeBarcode(rawBarcode);
      const liveExternalEmployeeId = getAuthExternalEmployeeId(auth);
      if (!inputBarcode) {
        sendJson(res, 400, { error: "Отсканируйте повторно" });
        return;
      }
      const orderResult = await dbQuery(
        `select assigned_user_id, scan_passed_at, label_scan_passed_at,
                barcode, article, product_name, quantity, scan_barcode
         from supply_orders
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId]
      );
      const order = orderResult.rows?.[0] || null;
      if (!order) {
        sendJson(res, 404, { error: "Заказ не найден" });
        return;
      }
      if (order.assigned_user_id && Number(order.assigned_user_id) !== Number(auth.id)) {
        sendJson(res, 403, { error: "FORBIDDEN" });
        return;
      }
      if (!order.scan_passed_at) {
        sendJson(res, 400, { error: "Сначала пройдите безошибочную сборку" });
        return;
      }
      if (order.label_scan_passed_at) {
        sendJson(res, 200, { ok: true, passed: true });
        return;
      }
      let expected = null;
      try {
        expected = await ensureStickerBarcode(supplyId, orderId);
      } catch {
        sendJson(res, 400, { error: "Отсканируйте повторно" });
        return;
      }
      if (!expected) {
        sendJson(res, 400, { error: "Отсканируйте повторно" });
        return;
      }
      const normalizedExpected = normalizeBarcode(expected);
      if (!normalizedExpected || normalizedExpected !== inputBarcode) {
        const matchedOrder = await findOrderByStickerBarcode({
          supplyId,
          storeId: context.settings?.store_id || null,
          barcode: inputBarcode,
          excludeOrderId: orderId,
          employeeId: auth.id,
        });
        if (matchedOrder) {
          sendJson(res, 409, {
            error: buildWrongLabelScanMessage({
              currentSupplyId: supplyId,
              currentOrderId: orderId,
              matchedOrder,
            }),
            code: "WRONG_LABEL_ORDER",
            currentSupplyId: supplyId,
            currentOrderId: Number(orderId),
            matchedSupplyId: matchedOrder.supply_id || null,
            matchedOrderId: matchedOrder.wb_order_id == null ? null : Number(matchedOrder.wb_order_id),
            matchedArticle: matchedOrder.article || null,
            matchedProductName: matchedOrder.product_name || null,
            matchedScope: matchedOrder.scope || null,
            matchedCollectedAt: matchedOrder.collected_at || null,
          });
          return;
        }
        sendJson(res, 400, { error: "Отсканируйте повторно" });
        return;
      }
      await dbQuery(
        `update supply_orders
         set label_scan_passed_at = now(), label_scan_passed_by = $3, label_scan_barcode = $4,
             label_scan_error = null, updated_at = now()
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId, auth.id, inputBarcode]
      );
      const collectedAt = new Date();
      await dbQuery(
        `update supply_orders
         set collected_at = $4, collected_by = $3, collected_via = 'label_scan', collected_session_id = $5, updated_at = now()
         where supply_id = $1 and wb_order_id = $2 and collected_at is null`,
        [supplyId, orderId, auth.id, collectedAt, context.session?.id || null]
      );
      if (isStockMethodEnabled(context.settings)) {
        if (!liveExternalEmployeeId) {
          await dbQuery(
            `update supply_orders
             set label_scan_passed_at = null,
                 label_scan_passed_by = null,
                 label_scan_barcode = null,
                 collected_at = null,
                 collected_by = null,
                 collected_via = null,
                 collected_session_id = null,
                 updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [supplyId, orderId]
          );
          sendJson(res, 409, { error: "Сотрудник не привязан к складской системе" });
          return;
        }
        try {
          const movementResult = await writeOffEmployeeInventory({
            externalEmployeeId: liveExternalEmployeeId,
            warehouseId: getSkladWarehouseOptionsFromSettings(context.settings).warehouseId,
            barcodes: [order.scan_barcode, order.barcode],
            quantity: Math.max(1, Number(order.quantity || 1)),
            skipUnavailable: isAllowStockShortage(context.settings),
            source: "wb_collect",
            notes: `WB: автосписание товара у сотрудника после сборки заказа. Поставка ${supplyId}, заказ ${orderId}, товар ${order.article || "—"} (${order.product_name || "—"}).`,
            negativeNotes: `WB: автосписание товара у сотрудника после сборки заказа с уходом сотрудника в минус. Поставка ${supplyId}, заказ ${orderId}, товар ${order.article || "—"} (${order.product_name || "—"}).`,
          });
          await recordStockShortageSkip({ supplyId, orderId, stage: "label_scan", article: order.article, barcode: order.scan_barcode || order.barcode, requestedQuantity: Math.max(1, Number(order.quantity || 1)), movementResult, userId: auth.id });
        } catch (error) {
          await dbQuery(
            `update supply_orders
             set label_scan_passed_at = null,
                 label_scan_passed_by = null,
                 label_scan_barcode = null,
                 collected_at = null,
                 collected_by = null,
                 collected_via = null,
                 collected_session_id = null,
                 updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [supplyId, orderId]
          );
          const message = isSkladMovementAccessError(error)
            ? "Не удалось списать товар со склада сотрудника. Повторите сканирование."
            : error?.message || "Не удалось списать товар у сотрудника";
          sendJson(res, isSkladMovementAccessError(error) ? 503 : 409, { error: message });
          return;
        }
      }
      let sessionFinished = false;
      try {
        if (context.session?.id) {
          await recordCollectedOrderForSession(context.session.id, collectedAt);
        }
        if (liveExternalEmployeeId) {
          writeSborkaLiveEventSafe({
            externalEmployeeId: liveExternalEmployeeId,
            eventType: "order_complete",
            orderId: String(orderId),
            marketplace: "wb",
          });
        }
        sessionFinished = Boolean(await finalizeEmployeeSupplySessionIfCompleted(supplyId, auth.id));
        notifySupplyUpdate(supplyId, true);
        if (!sessionFinished) {
          notifySupplyUpdate(supplyId);
        }
      } catch (error) {
        console.error("WB label-scan post-process failed", {
          supplyId,
          orderId,
          userId: auth.id,
          error: error?.message || String(error || ""),
        });
        notifySupplyUpdate(supplyId, true);
      }
      sendJson(res, 200, { ok: true, passed: true, sessionFinished });
      return;
    }

    const collectMatch = pathname.match(/^\/api\/employee\/supplies\/([^/]+)\/orders\/(\d+)\/collect$/);
    if (collectMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensurePackerAccess(res, auth)) return;
      const supplyId = decodeURIComponent(collectMatch[1]);
      const orderId = Number(collectMatch[2]);
      const context = await getEmployeeSupplyContext(supplyId, auth, { requireStartedSession: true });
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const check = await dbQuery(
        `select assigned_user_id, scan_passed_at, label_scan_passed_at, collected_at,
                barcode, article, product_name, quantity, scan_barcode
         from supply_orders
         where supply_id = $1 and wb_order_id = $2`,
        [supplyId, orderId]
      );
      const assignedId = check.rows?.[0]?.assigned_user_id;
      if (assignedId && Number(assignedId) !== Number(auth.id)) {
        sendJson(res, 403, { error: "FORBIDDEN" });
        return;
      }
      if (!check.rows?.[0]?.scan_passed_at) {
        sendJson(res, 400, { error: "Безошибочная сборка не пройдена" });
        return;
      }
      if (!check.rows?.[0]?.label_scan_passed_at) {
        sendJson(res, 400, { error: "Скан этикетки не пройден" });
        return;
      }
      if (check.rows?.[0]?.collected_at) {
        sendJson(res, 200, { ok: true, sessionFinished: false });
        return;
      }
      const liveExternalEmployeeId = getAuthExternalEmployeeId(auth);
      const collectedAt = new Date();
      await dbQuery(
        `update supply_orders
         set collected_at = $4, collected_by = $3, collected_via = 'label',
             collected_session_id = $5, updated_at = now()
         where supply_id = $1 and wb_order_id = $2 and collected_at is null`,
        [supplyId, orderId, auth.id, collectedAt, context.session?.id || null]
      );
      if (isStockMethodEnabled(context.settings)) {
        if (!liveExternalEmployeeId) {
          await dbQuery(
            `update supply_orders
             set collected_at = null,
                 collected_by = null,
                 collected_via = null,
                 collected_session_id = null,
                 updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [supplyId, orderId]
          );
          sendJson(res, 409, { error: "Сотрудник не привязан к складской системе" });
          return;
        }
        try {
          const movementResult = await writeOffEmployeeInventory({
            externalEmployeeId: liveExternalEmployeeId,
            warehouseId: getSkladWarehouseOptionsFromSettings(context.settings).warehouseId,
            barcodes: [check.rows?.[0]?.scan_barcode, check.rows?.[0]?.barcode],
            quantity: Math.max(1, Number(check.rows?.[0]?.quantity || 1)),
            skipUnavailable: isAllowStockShortage(context.settings),
            source: "wb_collect",
            notes: `WB: автосписание товара у сотрудника после сборки заказа. Поставка ${supplyId}, заказ ${orderId}, товар ${check.rows?.[0]?.article || "—"} (${check.rows?.[0]?.product_name || "—"}).`,
            negativeNotes: `WB: автосписание товара у сотрудника после сборки заказа с уходом сотрудника в минус. Поставка ${supplyId}, заказ ${orderId}, товар ${check.rows?.[0]?.article || "—"} (${check.rows?.[0]?.product_name || "—"}).`,
          });
          await recordStockShortageSkip({ supplyId, orderId, stage: "collect", article: check.rows?.[0]?.article, barcode: check.rows?.[0]?.scan_barcode || check.rows?.[0]?.barcode, requestedQuantity: Math.max(1, Number(check.rows?.[0]?.quantity || 1)), movementResult, userId: auth.id });
        } catch (error) {
          await dbQuery(
            `update supply_orders
             set collected_at = null,
                 collected_by = null,
                 collected_via = null,
                 collected_session_id = null,
                 updated_at = now()
             where supply_id = $1 and wb_order_id = $2`,
            [supplyId, orderId]
          );
          const message = isSkladMovementAccessError(error)
            ? "Не удалось списать товар со склада сотрудника. Повторите действие."
            : error?.message || "Не удалось списать товар у сотрудника";
          sendJson(res, isSkladMovementAccessError(error) ? 503 : 409, { error: message });
          return;
        }
      }
      if (context.session?.id) {
        await recordCollectedOrderForSession(context.session.id, collectedAt);
      }
      if (liveExternalEmployeeId) {
        writeSborkaLiveEventSafe({
          externalEmployeeId: liveExternalEmployeeId,
          eventType: "order_complete",
          orderId: String(orderId),
          marketplace: "wb",
        });
      }
      const finishedSession = await finalizeEmployeeSupplySessionIfCompleted(supplyId, auth.id);
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { ok: true, sessionFinished: Boolean(finishedSession) });
      return;
    }

    if (pathname === "/api/employee/collector/supplies" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensureCollectorAccess(res, auth)) return;
      const employeeLocation = await getConfiguredEmployeeLocation(auth.id);
      if (employeeLocation.error) {
        sendJson(res, employeeLocation.errorStatus || 409, { error: employeeLocation.error });
        return;
      }
      const settingsResult = await dbQuery(
        `select supply_id, supply_name, access_mode, store_id, store_name,
                location_code, sklad_warehouse_id, sklad_warehouse_name,
                employee_workflow_mode
         from supply_settings
         where access_mode = 'selected_split'
           and location_code = $1`,
        [employeeLocation.locationCode]
      );
      const allSettings = settingsResult.rows || [];
      if (!allSettings.length) {
        sendJson(res, 200, { supplies: [] });
        return;
      }
      const storeIds = new Set();
      for (const setting of allSettings) {
        const storeId = resolveSettingsStoreId(setting);
        if (storeId) storeIds.add(storeId);
      }
      const activeByStore = new Map();
      await Promise.all(
        Array.from(storeIds).map(async (storeId) => {
          activeByStore.set(storeId, await getActiveSupplyIdSet(storeId));
        })
      );
      const supplies = [];
      for (const setting of allSettings) {
        if (!isCollectorAllowedForSupply(setting)) {
          continue;
        }
        const storeKey = resolveSettingsStoreId(setting);
        const activeSet = storeKey ? activeByStore.get(storeKey) : null;
        if (activeSet && !activeSet.has(setting.supply_id)) {
          continue;
        }
        const latestSession = await getLatestCollectorSupplySession(setting.supply_id, auth.id);
        if (latestSession?.status === "finished") {
          continue;
        }
        const totalsResult = await dbQuery(
          `select count(*) filter (where assigned_user_id is not null and collected_at is null)::int as remaining_orders,
                  coalesce(sum(greatest(coalesce(quantity, 1), 1)) filter (where assigned_user_id is not null and collected_at is null), 0)::int as remaining_items,
                  count(distinct assigned_user_id) filter (where assigned_user_id is not null and collected_at is null)::int as employees
           from supply_orders
           where supply_id = $1`,
          [setting.supply_id]
        );
        const totals = totalsResult.rows?.[0] || {};
        const remainingOrders = Number(totals.remaining_orders || 0);
        const remainingItems = Number(totals.remaining_items || 0);
        const openSession =
          latestSession && latestSession.status === "started" && !latestSession.finished_at ? latestSession : null;
        if (remainingOrders < 1 && !openSession) {
          continue;
        }
        supplies.push({
          id: setting.supply_id,
          name: setting.supply_name || null,
          storeId: setting.store_id || null,
          storeName: setting.store_name || null,
          locationCode: normalizeLocationCode(setting.location_code),
          skladWarehouseId: Number(setting.sklad_warehouse_id || 0) || null,
          skladWarehouseName: setting.sklad_warehouse_name || null,
          remainingOrders,
          remainingItems,
          employees: Number(totals.employees || 0),
          session: toCollectorSessionPayload(openSession),
          canExport: remainingOrders > 0,
          canFinish: Boolean(openSession),
        });
      }
      sendJson(res, 200, { supplies });
      return;
    }

    const collectorExportMatch = pathname.match(/^\/api\/employee\/collector\/supplies\/([^/]+)\/export$/);
    if (collectorExportMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensureCollectorAccess(res, auth)) return;
      const supplyId = decodeURIComponent(collectorExportMatch[1]);
      const context = await getCollectorSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const { session, error } = await ensureCollectorSupplySession(supplyId, auth.id);
      if (error) {
        sendJson(res, 409, { error });
        return;
      }
      const itemsResult = await dbQuery(
        `select product_name, barcode, article, nm_id,
                coalesce(sum(greatest(coalesce(quantity, 1), 1)), 0)::int as count
         from supply_orders
         where supply_id = $1
           and assigned_user_id is not null
           and collected_at is null
         group by product_name, barcode, article, nm_id
         order by count desc, product_name asc`,
        [supplyId]
      );
      const items = (itemsResult.rows || []).map((row) => ({
        productName: row.product_name || null,
        barcode: row.barcode || null,
        article: row.article || null,
        nmId: row.nm_id != null ? Number(row.nm_id) : null,
        count: Number(row.count || 0),
      }));
      sendJson(res, 200, {
        supply: {
          id: supplyId,
          name: context.settings?.supply_name || null,
          storeId: context.settings?.store_id || null,
          storeName: context.settings?.store_name || null,
        },
        session: toCollectorSessionPayload(session),
        items,
      });
      return;
    }

    const collectorExportEmployeesMatch = pathname.match(/^\/api\/employee\/collector\/supplies\/([^/]+)\/export\/employees$/);
    if (collectorExportEmployeesMatch && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensureCollectorAccess(res, auth)) return;
      const supplyId = decodeURIComponent(collectorExportEmployeesMatch[1]);
      const context = await getCollectorSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const { session, error } = await ensureCollectorSupplySession(supplyId, auth.id);
      if (error) {
        sendJson(res, 409, { error });
        return;
      }
      const employeesResult = await dbQuery(
        `select u.id as user_id, u.surname, u.name,
                count(*)::int as order_count,
                coalesce(sum(greatest(coalesce(o.quantity, 1), 1)), 0)::int as item_count
         from supply_orders o
         join users u on u.id = o.assigned_user_id
         where o.supply_id = $1
           and o.assigned_user_id is not null
           and o.collected_at is null
         group by u.id, u.surname, u.name
         order by u.surname asc, u.name asc, u.id asc`,
        [supplyId]
      );
      const itemsResult = await dbQuery(
        `select o.assigned_user_id as user_id, o.product_name, o.barcode, o.article, o.nm_id,
                coalesce(sum(greatest(coalesce(o.quantity, 1), 1)), 0)::int as count
         from supply_orders o
         where o.supply_id = $1
           and o.assigned_user_id is not null
           and o.collected_at is null
         group by o.assigned_user_id, o.product_name, o.barcode, o.article, o.nm_id
         order by o.assigned_user_id asc, count desc, o.product_name asc`,
        [supplyId]
      );
      const itemsByUser = new Map();
      for (const row of itemsResult.rows || []) {
        const userId = Number(row.user_id || 0);
        if (!userId) continue;
        if (!itemsByUser.has(userId)) itemsByUser.set(userId, []);
        itemsByUser.get(userId).push({
          productName: row.product_name || null,
          barcode: row.barcode || null,
          article: row.article || null,
          nmId: row.nm_id != null ? Number(row.nm_id) : null,
          count: Number(row.count || 0),
        });
      }
      const employees = (employeesResult.rows || []).map((row) => {
        const userId = Number(row.user_id || 0);
        return {
          userId,
          surname: row.surname || "",
          name: row.name || "",
          orderCount: Number(row.order_count || 0),
          itemCount: Number(row.item_count || 0),
          items: itemsByUser.get(userId) || [],
        };
      });
      sendJson(res, 200, {
        supply: {
          id: supplyId,
          name: context.settings?.supply_name || null,
          storeId: context.settings?.store_id || null,
          storeName: context.settings?.store_name || null,
        },
        session: toCollectorSessionPayload(session),
        employees,
      });
      return;
    }

    const collectorFinishMatch = pathname.match(/^\/api\/employee\/collector\/supplies\/([^/]+)\/finish$/);
    if (collectorFinishMatch && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_EMPLOYEE]);
      if (!auth) return;
      if (!ensureCollectorAccess(res, auth)) return;
      const supplyId = decodeURIComponent(collectorFinishMatch[1]);
      const context = await getCollectorSupplyContext(supplyId, auth);
      if (context.error) {
        sendJson(res, context.errorStatus || 400, { error: context.error });
        return;
      }
      const result = await finishCollectorSupplySession(supplyId, auth.id);
      if (result.error) {
        sendJson(res, 409, { error: result.error });
        return;
      }
      notifySupplyUpdate(supplyId, true);
      sendJson(res, 200, { ok: true, session: toCollectorSessionPayload(result.session) });
      return;
    }

    if (pathname === "/api/new-orders") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const store = getStoreFromRequest(req);
      const orders = await getNewOrders(store?.id);
      sendJson(res, 200, { updatedAt: new Date().toISOString(), orders });
      return;
    }

    // City → WB marketplace warehouse mapping (from superadmin's global settings).
    // Lets the create-supply modal auto-filter new orders by the city's WB warehouse
    // (mirror of Ozon's /location-warehouse-map). Без него счётчик «будет выбрано»
    // считал заказы по ВСЕМ WB-складам, а не только складу выбранного города.
    if (pathname === "/api/location-warehouse-map") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const locationCode = String(url?.searchParams?.get("location") || "").trim().toLowerCase();
      if (!locationCode) { sendJson(res, 400, { error: "Не указан город" }); return; }
      const store = getStoreFromRequest(req);
      const storeId = normalizeStoreId(store?.id) || "";
      const result = await dbQuery(
        `select marketplace_warehouse_id, marketplace_warehouse_name, sklad_warehouse_id, sklad_warehouse_name
         from site_location_warehouse_settings
         where active = true and location_code = $1
           and marketplace in ('all', 'wb')
           and coalesce(marketplace_warehouse_id, '') <> ''
           and (coalesce(store_id, '') = '' or store_id = $2)`,
        [locationCode, storeId]
      );
      sendJson(res, 200, {
        locationCode,
        storeId,
        warehouses: (result.rows || []).map((row) => ({
          marketplaceWarehouseId: String(row.marketplace_warehouse_id),
          marketplaceWarehouseName: row.marketplace_warehouse_name || null,
          skladWarehouseId: row.sklad_warehouse_id == null ? null : Number(row.sklad_warehouse_id),
          skladWarehouseName: row.sklad_warehouse_name || null,
        })),
      });
      return;
    }

    if (pathname === "/api/supplies") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN, ROLE_SUPER]);
      if (!auth) return;
      const store = getStoreFromRequest(req);
      const data = await getSupplies(store?.id);
      sendJson(res, 200, {
        updatedAt: new Date().toISOString(),
        supplies: Array.isArray(data?.supplies) ? data.supplies : [],
        deliveringSupplies: Array.isArray(data?.deliveringSupplies) ? data.deliveringSupplies : [],
      });
      return;
    }

    if (pathname === "/api/city-article-stock" && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const body = await readJson(req);
      const items = Array.isArray(body?.items) ? body.items.slice(0, 400) : [];
      // Склад ГРАсклад по городу поставки (как на OZ) — иначе для любого города показывали остатки Ижевска.
      // Прямой запрос (без условия marketplace_warehouse_id: у Москвы он непустой, а из пикера мы его не шлём).
      const cityStore = getStoreFromRequest(req);
      const cityStoreId = normalizeStoreId(cityStore?.id) || "";
      const cityLocationCode = normalizeLocationCode(body?.location);
      let stockWh = { warehouseId: SKLAD_FBS_WAREHOUSE_ID, warehouseName: FBS_STOCK_WAREHOUSE_NAME };
      if (cityLocationCode) {
        try {
          const whRes = await dbQuery(
            `select sklad_warehouse_id, sklad_warehouse_name
             from site_location_warehouse_settings
             where active = true and location_code = $1
               and marketplace in ('all', 'wb')
               and (coalesce(store_id, '') = '' or store_id = $2)
               and coalesce(sklad_warehouse_id, 0) > 0
             order by (case when marketplace = 'wb' then 2 else 0 end) + (case when coalesce(store_id, '') = $2 and $2 <> '' then 1 else 0 end) desc, id desc
             limit 1`,
            [cityLocationCode, cityStoreId]
          );
          const whRow = whRes.rows?.[0];
          if (whRow?.sklad_warehouse_id) stockWh = { warehouseId: Number(whRow.sklad_warehouse_id), warehouseName: whRow.sklad_warehouse_name || FBS_STOCK_WAREHOUSE_NAME };
        } catch { /* fallback на дефолтный склад */ }
      }
      if (!items.length) {
        sendJson(res, 200, { warehouseName: stockWh.warehouseName, items: {}, stocks: {} });
        return;
      }
      const out = {};
      // Матч по БАРКОДУ (как скан/сборка): переиспользуем getSkladFbsStockStatusByBarcode.
      await mapWithConcurrency(items, 4, async (it) => {
        const article = String(it?.article || "").trim().toUpperCase();
        if (!article) return;
        const barcode = String(it?.barcode || it?.marketplaceBarcode || "").replace(/\s+/g, "");
        if (!barcode) { out[article] = { status: "no_barcode", stock: 0 }; return; }
        try {
          const ms = await getSkladFbsStockStatusByBarcode(barcode, {
            warehouseId: stockWh.warehouseId,
            storeName: stockWh.warehouseName,
          });
          if (ms?.status === "ok") out[article] = { status: "ok", stock: Number(ms.stock ?? 0) };
          else if (ms?.status === "multiple") out[article] = { status: "duplicate", stock: 0, duplicates: (ms.matches || []).map((m) => ({ article: m.article, name: m.name })) };
          else out[article] = { status: "no_barcode", stock: 0 };
        } catch { out[article] = { status: "no_barcode", stock: 0 }; }
      });
      sendJson(res, 200, { warehouseName: stockWh.warehouseName, items: out, stocks: {} });
      return;
    }

    if (pathname === "/api/supplies/create" && req.method === "POST") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const store = getStoreFromRequest(req);
      const body = await readJson(req);
      // Имя WB-поставки ВСЕГДА генерируется автоматически (МСК {день}/{N}); присланное имя игнорируем.
      const name = await generateWbAutoSupplyName();
      const locationCode = normalizeLocationCode(body?.locationCode);
      const allowStockShortage = body?.allowStockShortage === true;
      const orders = Array.isArray(body?.orders) ? body.orders : [];
      const excludeArticles = Array.isArray(body?.excludeArticles)
        ? body.excludeArticles
        : typeof body?.excludeArticles === "string"
          ? String(body.excludeArticles).split(/[,\n;]+/)
          : [];
      if (!name || name.length > 128) {
        sendJson(res, 400, { error: "Неверное имя поставки" });
        return;
      }
      if (!locationCode) {
        sendJson(res, 400, { error: "Выберите город поставки" });
        return;
      }
      const requestedOrderIds = Array.from(
        new Set(orders.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))
      );
      const excludeMatcher = buildExcludedArticleMatcher(excludeArticles);
      let orderIds = requestedOrderIds;
      let excludedByServerCount = 0;
      let freshOrdersForCreate = [];
      if (excludeMatcher.exact.size || excludeMatcher.families.size) {
        const freshOrders = await getNewOrders(store?.id);
        freshOrdersForCreate = Array.isArray(freshOrders) ? freshOrders : [];
        const orderById = new Map(
          freshOrdersForCreate
            .map((order) => [Number(order?.id), order])
            .filter(([id]) => Number.isFinite(id) && id > 0)
        );
        orderIds = requestedOrderIds.filter((id) => !orderMatchesExcludedArticle(orderById.get(id), excludeMatcher));
        excludedByServerCount = Math.max(0, requestedOrderIds.length - orderIds.length);
        if (requestedOrderIds.length > 0 && orderIds.length < 1) {
          sendJson(res, 400, { error: "После исключения артикулов подходящих заказов не осталось" });
          return;
        }
      }
      if (!freshOrdersForCreate.length) {
        freshOrdersForCreate = await getNewOrders(store?.id).catch(() => []);
      }
      const maxAllowed = Number.isFinite(MAX_CREATE_COUNT) && MAX_CREATE_COUNT > 0 ? MAX_CREATE_COUNT : null;
      if (orderIds.length < 1 || (maxAllowed && orderIds.length > maxAllowed)) {
        const suffix = maxAllowed ? ` до ${maxAllowed}` : "";
        sendJson(res, 400, { error: `Нужно выбрать от 1${suffix} заказов` });
        return;
      }
      const orderByIdForWarehouse = new Map(
        (Array.isArray(freshOrdersForCreate) ? freshOrdersForCreate : [])
          .map((order) => [Number(order?.id), order])
          .filter(([id]) => Number.isFinite(id) && id > 0)
      );
      const marketplaceWarehouseIds = Array.from(
        new Set(
          orderIds
            // warehouseId приходит числом — normalizeText(number) вернёт '', поэтому явно приводим к строке
            .map((id) => {
              const wh = orderByIdForWarehouse.get(Number(id))?.warehouseId;
              return wh === null || wh === undefined ? "" : String(wh).trim();
            })
            .filter(Boolean)
        )
      );
      if (marketplaceWarehouseIds.length > 1) {
        sendJson(res, 400, { error: "WB-поставка должна быть внутри одного склада WB" });
        return;
      }
      let skladWarehouse = null;
      try {
        skladWarehouse = await resolveSkladWarehouseForLocation({
          locationCode,
          marketplace: "wb",
          storeId: store?.id || null,
          marketplaceWarehouseId: marketplaceWarehouseIds[0] || null,
        });
      } catch (error) {
        sendJson(res, error?.statusCode || 409, { error: error?.message || "Не настроен склад" });
        return;
      }
      const requestKey = buildCreateRequestKey(store?.id, [`location:${locationCode}`, `shortage:${allowStockShortage}`, ...orderIds]);
      const cachedCreate = getRecentCreateRequest(recentCreateSupplyRequests, requestKey);
      const result = cachedCreate
        ? await cachedCreate
        : await rememberRecentCreateRequest(
            recentCreateSupplyRequests,
            requestKey,
            createSupply(store, name, orderIds, {
              locationCode,
              skladWarehouseId: skladWarehouse.skladWarehouseId,
              skladWarehouseName: skladWarehouse.skladWarehouseName,
              allowStockShortage,
              orderObjects: orderByIdForWarehouse,
            })
          );
      // Тумблер «Сборка в десктоп-приложении» из модалки создания — как на OZ.
      if (body?.desktopPrint === true && result?.supplyId) {
        await dbQuery("update supply_settings set desktop_print = true, updated_at = now() where supply_id = $1", [result.supplyId]).catch(() => {});
      }
      notifySupplyUpdate(result?.supplyId || null, true);
      sendJson(res, 200, {
        ...result,
        locationCode,
        skladWarehouseId: skladWarehouse?.skladWarehouseId || null,
        skladWarehouseName: skladWarehouse?.skladWarehouseName || null,
        excludedByServerCount,
        excludedArticles: Array.from(excludeMatcher.exact),
      });
      return;
    }

    if (pathname === "/api/supplies/history" && req.method === "GET") {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const store = getStoreFromRequest(req);
      const fromRaw = normalizeText(url.searchParams.get("from"));
      const toRaw = normalizeText(url.searchParams.get("to"));
      const dateRe = /^\d{4}-\d{2}-\d{2}$/;
      const params = [store?.id || null];
      const whereParts = ["ss.store_id = $1"];
      if (dateRe.test(fromRaw)) {
        params.push(fromRaw);
        whereParts.push(`timezone('Europe/Moscow', coalesce(ss.wb_supply_created_at, ss.created_at))::date >= $${params.length}::date`);
      }
      if (dateRe.test(toRaw)) {
        params.push(toRaw);
        whereParts.push(`timezone('Europe/Moscow', coalesce(ss.wb_supply_created_at, ss.created_at))::date <= $${params.length}::date`);
      }
      const listResult = await dbQuery(
        `select ss.supply_id as id, ss.supply_name as name, ss.store_id, ss.store_name, ss.access_mode,
                coalesce(ss.wb_supply_created_at, ss.created_at) as created_at, ss.wb_supply_done,
                count(o.*)::int as total,
                count(o.*) filter (where o.collected_at is not null)::int as collected
         from supply_settings ss
         left join supply_orders o on o.supply_id = ss.supply_id
         where ${whereParts.join(" and ")}
         group by ss.supply_id, ss.supply_name, ss.store_id, ss.store_name, ss.access_mode, ss.wb_supply_created_at, ss.created_at, ss.wb_supply_done
         order by coalesce(ss.wb_supply_created_at, ss.created_at) desc
         limit 500`,
        params
      );
      const rows = listResult.rows || [];
      const ids = rows.map((r) => r.id);
      const collectorsMap = new Map();
      if (ids.length) {
        const cr = await dbQuery(
          `select o.supply_id, o.collected_by,
                  coalesce(nullif(trim(concat_ws(' ', u.surname, u.name)), ''), 'ID '||o.collected_by) as full_name,
                  count(*)::int as cnt,
                  count(*) filter (where coalesce(o.label_scan_barcode, '') = 'NO_SCAN_MODE')::int as noscan
           from supply_orders o
           left join users u on u.id = o.collected_by
           where o.supply_id = any($1::text[]) and o.collected_at is not null
           group by o.supply_id, o.collected_by, u.surname, u.name`,
          [ids]
        );
        for (const row of cr.rows || []) {
          const arr = collectorsMap.get(row.supply_id) || [];
          const cnt = Number(row.cnt || 0);
          const noscan = Number(row.noscan || 0);
          arr.push({ userId: row.collected_by, name: row.full_name, count: cnt, scanCount: cnt - noscan, noScanCount: noscan });
          collectorsMap.set(row.supply_id, arr);
        }
      }
      sendJson(res, 200, {
        assemblies: rows.map((r) => ({
          id: r.id,
          name: r.name,
          storeId: r.store_id,
          storeName: r.store_name,
          accessMode: r.access_mode,
          createdAt: r.created_at,
          total: Number(r.total || 0),
          collected: Number(r.collected || 0),
          sent: r.wb_supply_done ? Number(r.collected || 0) : 0,
          collectors: (collectorsMap.get(r.id) || []).sort((a, b) => b.count - a.count),
        })),
      });
      return;
    }

    const match = pathname.match(/^\/api\/supplies\/([^/]+)\/orders$/);
    if (match) {
      const auth = await requireAuth(req, res, [ROLE_ADMIN]);
      if (!auth) return;
      const supplyId = decodeURIComponent(match[1]);
      const settings = await getSupplySettings(supplyId);
      const orders = await getSupplyOrders(supplyId, settings?.supply_name || null);
      sendJson(res, 200, { updatedAt: new Date().toISOString(), orders });
      return;
    }

    sendJson(res, 404, { error: "Not found" });
  } catch (err) {
    sendJson(res, 500, { error: err.message || "API error" });
  }
}

function isAllowedWbImageTarget(target) {
  const host = target?.hostname?.toLowerCase() || "";
  return target?.protocol === "https:" && (
    host === "wbbasket.ru" || host.endsWith(".wbbasket.ru") ||
    host.endsWith(".wb.ru") || host.endsWith(".wbstatic.net") || host.endsWith(".wbcontent.net")
  );
}

function remoteImageContentType(target, fallback = "image/webp") {
  const pathname = String(target?.pathname || "").toLowerCase();
  if (pathname.endsWith(".png")) return "image/png";
  if (pathname.endsWith(".jpg") || pathname.endsWith(".jpeg")) return "image/jpeg";
  if (pathname.endsWith(".gif")) return "image/gif";
  if (pathname.endsWith(".svg")) return "image/svg+xml";
  return fallback;
}

async function loadCachedRemoteImage(target, isAllowedTarget, fallbackContentType) {
  const sourceUrl = target.toString();
  const cacheKey = crypto.createHash("sha256").update(sourceUrl).digest("hex");
  const cachePath = path.join(PRODUCT_IMAGE_CACHE_DIR, cacheKey);
  try {
    const stat = await fs.promises.stat(cachePath);
    if (stat.isFile() && stat.size > 0) {
      return {
        buffer: await fs.promises.readFile(cachePath),
        contentType: remoteImageContentType(target, fallbackContentType),
      };
    }
  } catch {}

  if (remoteImageJobs.has(cacheKey)) return remoteImageJobs.get(cacheKey);
  const job = (async () => {
    let lastError = null;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        const upstream = await fetch(sourceUrl, {
          redirect: "follow",
          headers: { Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8" },
          signal: AbortSignal.timeout(12000),
        });
        const finalTarget = new URL(upstream.url || sourceUrl);
        if (!isAllowedTarget(finalTarget) || !upstream.ok) {
          throw new Error(`Image upstream returned ${upstream.status}`);
        }
        const contentType = upstream.headers.get("content-type") || remoteImageContentType(finalTarget, fallbackContentType);
        if (!contentType.toLowerCase().startsWith("image/")) {
          throw new Error("Image upstream returned non-image content");
        }
        const buffer = Buffer.from(await upstream.arrayBuffer());
        if (!buffer.length || buffer.length > 20 * 1024 * 1024) {
          throw new Error("Image upstream returned invalid payload");
        }
        const tempPath = `${cachePath}.${process.pid}.${Date.now()}.tmp`;
        await fs.promises.writeFile(tempPath, buffer);
        await fs.promises.rename(tempPath, cachePath).catch(async () => {
          await fs.promises.unlink(tempPath).catch(() => {});
        });
        return { buffer, contentType };
      } catch (error) {
        lastError = error;
        if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, 250 * attempt));
      }
    }
    throw lastError || new Error("Image upstream unavailable");
  })().finally(() => remoteImageJobs.delete(cacheKey));
  remoteImageJobs.set(cacheKey, job);
  return job;
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    const pathname = decodeURIComponent(url.pathname);

        let apiPath = null;
    const siteApiPrefix = `/${ROOT_SITE}/api`;
    if (pathname === siteApiPrefix || pathname === `${siteApiPrefix}/`) {
      apiPath = "/api";
    } else if (pathname.startsWith(`${siteApiPrefix}/`)) {
      apiPath = pathname.slice(`/${ROOT_SITE}`.length);
    } else if (pathname.startsWith("/api/")) {
      apiPath = pathname;
    }

    if (apiPath) {
      await handleApi(req, res, apiPath, url);
      return;
    }

    const debugPaths = new Set([`/${ROOT_SITE}/test`, `/${ROOT_SITE}/test/`, "/test", "/test/"]);
    if (debugPaths.has(pathname)) {
      await handleDebugPage(req, res, url);
      return;
    }

    if (pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    // Прокси картинок карточек WB (CDN wbbasket) через свой origin — до отдачи локальных файлов.
    if (pathname === `/${ROOT_SITE}/files/_img`) {
      try {
        const raw = url.searchParams.get("u") || "";
        let target;
        try { target = new URL(raw); } catch { send(res, 400, "bad"); return; }
        if (!isAllowedWbImageTarget(target)) { send(res, 400, "bad"); return; }
        const image = await loadCachedRemoteImage(target, isAllowedWbImageTarget, "image/webp");
        res.writeHead(200, {
          "Content-Type": image.contentType,
          "Cache-Control": "public, max-age=604800, immutable",
        });
        res.end(image.buffer);
      } catch { send(res, 502, "bad"); }
      return;
    }

    const { site, subpath } = resolveSite(pathname);
    const localFilesPrefix = `/${ROOT_SITE}/files/`;
    if (pathname.startsWith(localFilesPrefix)) {
      const relLocalPath = pathname.slice(localFilesPrefix.length);
      const localFilePath = safeJoin(LOCAL_FILES_DIR, relLocalPath);
      if (!localFilePath || !fs.existsSync(localFilePath) || !fs.statSync(localFilePath).isFile()) {
        send(res, 404, "Not found");
        return;
      }
      streamFile(req, res, localFilePath);
      return;
    }

    if (!siteExists(site)) {
      if ((pathname === "/" || pathname === "") && DEFAULT_SITE && siteExists(DEFAULT_SITE)) {
        res.writeHead(302, { Location: `/${DEFAULT_SITE}/` });
        res.end();
        return;
      }
      send(res, 404, "Site not found");
      return;
    }

    const siteRoot = siteDist(site);
    const rel = subpath === "/" ? "index.html" : subpath.replace(/^\/+/, "");
    let filePath = safeJoin(siteRoot, rel);
    if (!filePath) {
      send(res, 400, "Bad request");
      return;
    }

    let stat = null;
    try {
      stat = fs.statSync(filePath);
    } catch {}

    if (stat && stat.isDirectory()) {
      const indexPath = path.join(filePath, "index.html");
      if (fs.existsSync(indexPath)) {
        streamFile(req, res, indexPath);
        return;
      }
      send(res, 404, "Not found");
      return;
    }

    if (stat && stat.isFile()) {
      streamFile(req, res, filePath);
      return;
    }

    if (!hasExt(subpath)) {
      const indexPath = siteIndex(site);
      if (fs.existsSync(indexPath)) {
        streamFile(req, res, indexPath);
        return;
      }
    }

    send(res, 404, "Not found");
  } catch (err) {
    send(res, 500, "Server error");
  }
});

server.listen(PORT, HOST, () => {
  console.log(`Multi-site server running on http://${HOST}:${PORT}`);
  startNameBackfill();
});


