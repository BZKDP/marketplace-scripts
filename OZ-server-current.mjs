import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import compression from "compression";
import express from "express";
import bwipjs from "bwip-js";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import fontkit from "@pdf-lib/fontkit";
import { Pool } from "pg";
import {
  CACHE_MS,
  COOKIE_NAME,
  COOKIE_PATH,
  COOKIE_SECURE,
  DB_HOST,
  DB_NAME,
  DB_PASSWORD,
  DB_PORT,
  DB_SSL,
  DB_USER,
  HOST,
  LABEL_BATCH_SIZE,
  LABELS_BATCH_DELAY_MS,
  LABELS_RETRY_DELAY_MS,
  MAX_CREATE_COUNT,
  MS_FBS_STOCK_WAREHOUSE_NAME,
  SKLAD_FBS_WAREHOUSE_ID,
  OZON_API_BASE,
  OZON_API_KEY,
  OZON_CLIENT_ID,
  OZON_API_TIMEOUT_MS,
  OZON_API_FAST_TIMEOUT_MS,
  OZON_LIST_DAYS,
  OZON_NEW_LIST_DAYS,
  OZON_PAGE_LIMIT,
  OZON_STORE_1_API_KEY,
  OZON_STORE_1_CLIENT_ID,
  OZON_STORE_1_ID,
  OZON_STORE_1_NAME,
  OZON_STORE_2_API_KEY,
  OZON_STORE_2_CLIENT_ID,
  OZON_STORE_2_ID,
  OZON_STORE_2_NAME,
  PORT,
  PROJECT_ROOT,
  ROLE_ADMIN,
  ROLE_EMPLOYEE,
  ROLE_SUPER,
  ROOT_SITE,
  SESSION_TTL_MS,
} from "./server/config.mjs";
import { buildS3Key, buildS3Url, getLocalFilesDir, isLocalStorageKey, resolveLocalFilePath, uploadPdfToS3 } from "./server/services/s3.mjs";
import {
  getExternalUserById,
  isExternalAuthConfigured,
  listActiveExternalUsers,
  verifyExternalUserPassword,
} from "./server/services/external-auth.mjs";
import {
  getMsBarcodesByArticle,
  getMsProductStatusByArticle,
  getMsProductStatusByBarcode,
  getMsStockStatusByBarcode,
  getMsStockStatusByArticle,
  getMsStockStatusesByBarcodes,
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
  writeOffEmployeeInventoryBatch,
  resolveWarehouseProductMeta,
} from "./server/services/sklad-movements.mjs";
import {
  getSkladFbsStockStatusByBarcode,
  getSkladFbsStockStatusByArticle,
  assertSkladDbAvailable,
  clearSkladFbsCaches,
  isSkladDbAccessError,
  skladRawQuery,
} from "./server/services/sklad-fbs.mjs";
import { normalizeArticleKey, normalizeBarcode, normalizeStoreId, normalizeText } from "./server/utils.mjs";

process.on("unhandledRejection", (reason) => {
  console.error("[oz] unhandledRejection", reason);
});

process.on("uncaughtException", (error) => {
  console.error("[oz] uncaughtException", error);
  setTimeout(() => process.exit(1), 50);
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIST_DIR = path.join(PROJECT_ROOT, "dist");
const INDEX_FILE = path.join(DIST_DIR, "index.html");
const BASE_PATH = `/${ROOT_SITE}`;
const API_ROOT = `${BASE_PATH}/api`;
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
const CARDS_CACHE_FILE = path.join(PROJECT_ROOT, "var", "cards-cache.json");
const storeList = [];
const storeMap = new Map();
const perfMetrics = new Map();
const PERF_SAMPLE_LIMIT = 300;
const SCAN_LOOKUP_CACHE_MS = 20 * 1000;

function writeSborkaLiveEventSafe(options = {}) {
  void writeSborkaLiveEvent(options).catch((error) => {
    try {
      console.warn(
        JSON.stringify({
          tag: "OZ_SBORKA_LIVE_EVENT_ERROR",
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
const SCAN_LOOKUP_CACHE_MAX = 2000;
const SCAN_BARCODE_ALIAS_CACHE_MS = 5 * 60 * 1000;
const ACCEPTED_PICK_BARCODES_CACHE_MS = 5 * 60 * 1000;
const ACCEPTED_PICK_BARCODES_CACHE_MAX = 5000;
const EMPLOYEE_PICK_SCAN_INDEX_CACHE_MS = 2 * 60 * 1000;
const EMPLOYEE_PICK_SCAN_INDEX_MAX = 300;
const EMPLOYEE_PICK_SCAN_INDEX_CONCURRENCY = 8;
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
const scanLookupCache = new Map();
const acceptedPickBarcodesCache = new Map();
const employeePickScanIndexCache = new Map();
const authSessionCache = new Map();
const USER_AUTH_SOURCE_LOCAL = "local";
const USER_AUTH_SOURCE_EXTERNAL = "external";
const USER_AUTH_SOURCE_LEGACY = "legacy";
const LOCATION_IZHEVSK = "izhevsk";
const LOCATION_MOSCOW = "moscow";
const LOCATION_CODES = new Set([LOCATION_IZHEVSK, LOCATION_MOSCOW]);

function elapsedMs(startHr) {
  return Number(process.hrtime.bigint() - startHr) / 1e6;
}

function percentile(sortedValues, ratio) {
  if (!sortedValues.length) return 0;
  const idx = Math.min(sortedValues.length - 1, Math.max(0, Math.ceil(sortedValues.length * ratio) - 1));
  return sortedValues[idx];
}

function recordPerf(metric, valueMs) {
  const name = normalizeText(metric);
  const ms = Number(valueMs);
  if (!name || !Number.isFinite(ms) || ms < 0) return;
  const state =
    perfMetrics.get(name) || {
      count: 0,
      totalMs: 0,
      minMs: Number.POSITIVE_INFINITY,
      maxMs: 0,
      samples: [],
    };
  state.count += 1;
  state.totalMs += ms;
  state.minMs = Math.min(state.minMs, ms);
  state.maxMs = Math.max(state.maxMs, ms);
  state.samples.push(ms);
  if (state.samples.length > PERF_SAMPLE_LIMIT) {
    state.samples.splice(0, state.samples.length - PERF_SAMPLE_LIMIT);
  }
  perfMetrics.set(name, state);
}

function getPerfSnapshot() {
  const out = [];
  for (const [name, state] of perfMetrics.entries()) {
    const sorted = [...state.samples].sort((a, b) => a - b);
    out.push({
      metric: name,
      count: Number(state.count || 0),
      avgMs: Number(((state.totalMs || 0) / Math.max(1, Number(state.count || 0))).toFixed(2)),
      minMs: Number((Number.isFinite(state.minMs) ? state.minMs : 0).toFixed(2)),
      maxMs: Number((Number(state.maxMs || 0)).toFixed(2)),
      p50Ms: Number(percentile(sorted, 0.5).toFixed(2)),
      p95Ms: Number(percentile(sorted, 0.95).toFixed(2)),
      samples: sorted.length,
    });
  }
  out.sort((a, b) => a.metric.localeCompare(b.metric));
  return out;
}

function clearPerfSnapshot() {
  perfMetrics.clear();
}

function getCachedAcceptedPickBarcodes(cacheKey, force = false) {
  if (!cacheKey || force) return null;
  const cached = acceptedPickBarcodesCache.get(cacheKey);
  if (!cached) return null;
  if (Date.now() - Number(cached.ts || 0) >= ACCEPTED_PICK_BARCODES_CACHE_MS) {
    acceptedPickBarcodesCache.delete(cacheKey);
    return null;
  }
  return Array.isArray(cached.barcodes) ? [...cached.barcodes] : [];
}

function setCachedAcceptedPickBarcodes(cacheKey, barcodes = []) {
  if (!cacheKey) return;
  if (acceptedPickBarcodesCache.has(cacheKey)) {
    acceptedPickBarcodesCache.delete(cacheKey);
  }
  while (acceptedPickBarcodesCache.size >= ACCEPTED_PICK_BARCODES_CACHE_MAX) {
    const oldestKey = acceptedPickBarcodesCache.keys().next().value;
    if (!oldestKey) break;
    acceptedPickBarcodesCache.delete(oldestKey);
  }
  acceptedPickBarcodesCache.set(cacheKey, {
    ts: Date.now(),
    barcodes: uniqueTextList(barcodes),
  });
}

function buildAcceptedPickBarcodesCacheKey({ article = null, storeId = null, sku = null, barcode = null } = {}) {
  const articleKey = normalizeArticleKey(article);
  const storeKey = normalizeStoreId(storeId) || "";
  const skuKey = normalizeBarcode(sku);
  const barcodeKey = normalizeBarcode(barcode);
  if (articleKey) {
    return `article:${articleKey}|store:${storeKey}|sku:${skuKey || ""}`;
  }
  if (barcodeKey) {
    return `barcode:${barcodeKey}|store:${storeKey}|sku:${skuKey || ""}`;
  }
  return "";
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

async function buildOzonPickItemMovementHints(item, options = {}) {
  const warehouseOptions = getSkladWarehouseOptionsFromAssembly(options);
  const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0] || null;
  const articleKey = normalizeArticleKey(item?.article);
  const skuValue = Number.isFinite(Number(item?.sku)) ? Number(item.sku) : null;
  let productIds = extractMsMatchProductIds(options?.status, item?.article);
  if (!productIds.length && articleKey) {
    try {
      const status = await getMsStockStatusForOzonOffer(articleKey, null, {
        sku: skuValue,
        warehouseId: warehouseOptions.warehouseId,
        storeName: warehouseOptions.storeName,
        force: options?.force === true,
      });
      productIds = extractMsMatchProductIds(status, item?.article);
    } catch {}
  }
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
  acceptedPickBarcodesCache.clear();
  employeePickScanIndexCache.clear();
  scanLookupCache.clear();
  barcodeArticleResolutionCache.clear();
  clearMsCaches();
  clearSkladFbsCaches();
}

function isPendingEmployeePickItem(row) {
  return Number(row?.picked_qty || row?.pickedQty || 0) < Number(row?.required_qty || row?.requiredQty || 0);
}

function getEmployeePickScanIndexMatches(sessionOrSessionId, inputBarcodes = []) {
  const barcodes = uniqueTextList(Array.isArray(inputBarcodes) ? inputBarcodes : [inputBarcodes]);
  if (!barcodes.length) return null;
  const cacheKey = getEmployeePickScanIndexKey(sessionOrSessionId);
  const cached = getCachedEmployeePickScanIndexEntry(cacheKey);
  if (!cached || cached.loading || !cached.acceptedMatches) {
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

function normalizeEmployeePosition(value) {
  const normalized = normalizeText(value).toLowerCase();
  if (!normalized) return null;
  if (normalized === EMPLOYEE_POSITION_COLLECTOR) return EMPLOYEE_POSITION_COLLECTOR;
  return normalized === EMPLOYEE_POSITION_PACKER ? EMPLOYEE_POSITION_PACKER : null;
}

function normalizeEmployeeWorkflowStage(value) {
  const normalized = normalizeText(value);
  if (!normalized) return EMPLOYEE_WORKFLOW_ORDERS_ONLY;
  return EMPLOYEE_WORKFLOW_STAGES.has(normalized) ? normalized : EMPLOYEE_WORKFLOW_ORDERS_ONLY;
}

function normalizeEmployeeWorkflowMode(value) {
  const normalized = normalizeText(value);
  if (!normalized) return EMPLOYEE_WORKFLOW_MODE_LEGACY;
  return EMPLOYEE_WORKFLOW_MODES.has(normalized) ? normalized : EMPLOYEE_WORKFLOW_MODE_LEGACY;
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

function isStockMethodEnabled(assembly) {
  return assembly?.stock_method_enabled === true || assembly?.stock_method_enabled === "t";
}

function isAllowStockShortage(assembly) {
  return assembly?.allow_stock_shortage === true || assembly?.allow_stock_shortage === "t";
}

async function recordStockShortageSkip({ assemblyId, postingNumber = null, stage, article = null, barcode = null, requestedQuantity, movementResult, userId = null }) {
  const skippedQuantity = Math.max(0, Number(movementResult?.skippedQuantity || 0));
  if (!skippedQuantity) return;
  const processedQuantity = Math.max(0, Number(requestedQuantity || 0) - skippedQuantity);
  try {
    await dbQuery(
      `insert into ozon_stock_shortage_skips (
         assembly_id, posting_number, stage, article, barcode,
         requested_quantity, processed_quantity, skipped_quantity, user_id, created_at
       ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())`,
      [
        normalizeText(assemblyId),
        normalizeText(postingNumber) || null,
        normalizeText(stage),
        normalizeArticleKey(article) || null,
        normalizeBarcode(barcode) || null,
        Math.max(1, Number(requestedQuantity || 1)),
        processedQuantity,
        skippedQuantity,
        Number(userId || 0) || null,
      ]
    );
  } catch (error) {
    console.error("[ozon-stock-shortage] failed to persist skipped movement", error);
  }
}

async function writeOffOzonOrderInventory({
  assembly,
  assemblyId,
  postingNumber,
  storeId,
  orderItems,
  externalEmployeeId,
  userId,
  noScan = false,
}) {
  const preparedItems = [];
  for (const item of orderItems) {
    const marketplaceBarcodes = await getOfferBarcodesWithFallback(item.offer_id, [storeId || null], {
      sku: item.sku,
      preferMarketplaceOnly: true,
    });
    const skuBarcodes = buildOzonMarketplaceBarcodeCandidates(item.sku);
    const itemDescription = `${item.offer_id || "—"} (${item.name || "—"})`;
    preparedItems.push({
      item,
      primaryBarcode: marketplaceBarcodes[0] || skuBarcodes[0],
      movement: {
        barcodes: [...marketplaceBarcodes, ...skuBarcodes],
        quantity: Math.max(1, Number(item.quantity || 1)),
        notes: noScan
          ? `Ozon: автосписание (режим без сканирования). Сборка ${assemblyId}, заказ ${postingNumber}, товар ${itemDescription}.`
          : `Ozon: автосписание товара у сотрудника после сборки заказа. Сборка ${assemblyId}, заказ ${postingNumber}, товар ${itemDescription}.`,
        negativeNotes: noScan
          ? `Ozon: автосписание (режим без сканирования) с уходом сотрудника в минус. Сборка ${assemblyId}, заказ ${postingNumber}, товар ${itemDescription}.`
          : `Ozon: автосписание товара у сотрудника после сборки заказа с уходом сотрудника в минус. Сборка ${assemblyId}, заказ ${postingNumber}, товар ${itemDescription}.`,
      },
    });
  }

  const movementResults = await writeOffEmployeeInventoryBatch({
    externalEmployeeId,
    warehouseId: getSkladWarehouseOptionsFromAssembly(assembly).warehouseId,
    skipUnavailable: isAllowStockShortage(assembly),
    source: "oz_collect",
    items: preparedItems.map((entry) => entry.movement),
  });
  for (let index = 0; index < preparedItems.length; index += 1) {
    const entry = preparedItems[index];
    await recordStockShortageSkip({
      assemblyId,
      postingNumber,
      stage: noScan ? "order_complete_no_scan" : "order_complete",
      article: entry.item.offer_id,
      barcode: entry.primaryBarcode,
      requestedQuantity: entry.item.quantity,
      movementResult: movementResults[index],
      userId,
    });
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

function normalizeScanIssueStage(value) {
  const normalized = normalizeText(value);
  if (!normalized) return SCAN_ISSUE_STAGE_ORDER;
  return SCAN_ISSUE_STAGES.has(normalized) ? normalized : SCAN_ISSUE_STAGE_ORDER;
}

function assemblyScanIssueStageLabel(value) {
  return normalizeScanIssueStage(value) === SCAN_ISSUE_STAGE_PICK ? "Сборка товара" : "Сборка заказов";
}

function isCollectorAllowedForAssembly(assembly) {
  return normalizeEmployeeWorkflowMode(assembly?.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING;
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

function registerStore(store) {
  const id = normalizeStoreId(store?.id);
  const clientId = normalizeText(store?.clientId);
  const apiKey = normalizeText(store?.apiKey);
  if (!id || !clientId || !apiKey) return;
  const normalized = {
    id,
    name: normalizeText(store?.name) || id,
    clientId,
    apiKey,
  };
  storeList.push(normalized);
  storeMap.set(id, normalized);
}

registerStore({
  id: OZON_STORE_1_ID,
  name: OZON_STORE_1_NAME,
  clientId: OZON_STORE_1_CLIENT_ID,
  apiKey: OZON_STORE_1_API_KEY,
});
registerStore({
  id: OZON_STORE_2_ID,
  name: OZON_STORE_2_NAME,
  clientId: OZON_STORE_2_CLIENT_ID,
  apiKey: OZON_STORE_2_API_KEY,
});

if (!storeList.length && OZON_CLIENT_ID && OZON_API_KEY) {
  registerStore({
    id: "irina",
    name: "ИП Ирина",
    clientId: OZON_CLIENT_ID,
    apiKey: OZON_API_KEY,
  });
}

const DEFAULT_STORE = storeList[0] || null;

function resolveStore(storeId) {
  const normalized = normalizeStoreId(storeId);
  if (!normalized) return DEFAULT_STORE;
  return storeMap.get(normalized) || DEFAULT_STORE;
}

function resolveStoreStrict(storeId) {
  const normalized = normalizeStoreId(storeId);
  if (!normalized) return null;
  return storeMap.get(normalized) || null;
}

function getRequiredStoreFromRequest(req) {
  const store = resolveStoreStrict(req.headers["x-store-id"]);
  if (!store) {
    const err = new Error("Магазин не настроен");
    err.status = 400;
    throw err;
  }
  return store;
}

async function getAdminAssemblyInStore(assemblyId, storeId) {
  const assembly = await getAssembly(assemblyId);
  if (!assembly) return null;
  const normalizedAssemblyStoreId = normalizeStoreId(assembly.store_id);
  const normalizedStoreId = normalizeStoreId(storeId);
  if (!normalizedAssemblyStoreId || !normalizedStoreId || normalizedAssemblyStoreId !== normalizedStoreId) {
    return null;
  }
  return assembly;
}

function getSyncAwaitingDeliverAssemblyId(storeId) {
  const store = resolveStore(storeId);
  return `ozon-sync-awaiting-deliver-${store?.id || "default"}`;
}

const app = express();
app.set("trust proxy", true);
app.use(
  compression({
    filter: (req, res) => {
      // SSE нельзя сжимать: gzip буферизует события и они не доходят до клиента
      if (String(req.path || "").endsWith("/events")) return false;
      return compression.filter(req, res);
    },
  })
);
app.use(express.json({ limit: "3mb" }));

if (!storeList.length) {
  console.warn("OZON credentials are empty. Ozon API calls will fail.");
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
    application_name: "oz-main",
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
  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      return await run();
    } catch (error) {
      lastError = error;
      if (!isRetryablePgConnectionError(error)) {
        throw error;
      }
      console.warn(
        `[db-pool] retrying ${label} after transient error (attempt ${attempt + 1}/3):`,
        error?.message || error
      );
      await closePool();
      await new Promise((resolve) => setTimeout(resolve, 150 * (attempt + 1)));
    }
  }
  throw lastError;
}

async function dbQuery(text, params = []) {
  return retryDbOperation(async () => {
    const activePool = await getPool();
    return activePool.query(text, params);
  }, "query");
}

async function ensureSchema() {
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

  await dbQuery(
    `create table if not exists ozon_assemblies (
      id text primary key,
      name text not null,
      created_by bigint references users(id) on delete set null,
      access_mode text not null default 'hidden',
      labels_status text not null default 'idle',
      labels_total integer not null default 0,
      labels_loaded integer not null default 0,
      labels_error text,
      labels_started_at timestamptz,
      labels_finished_at timestamptz,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      check (access_mode in ('hidden', 'all', 'selected', 'selected_split'))
    )`
  );

  await dbQuery(
    `create table if not exists ozon_assembly_access_users (
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      created_at timestamptz not null default now(),
      primary key (assembly_id, user_id)
    )`
  );

  await dbQuery(
    `create table if not exists ozon_assembly_orders (
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
      posting_number text not null,
      order_created_at timestamptz,
      ozon_status text,
      label_barcode text,
      label_s3_key text,
      label_loaded_at timestamptz,
      label_error text,
      assigned_user_id bigint references users(id) on delete set null,
      assigned_at timestamptz,
      scan_passed_at timestamptz,
      scan_passed_by bigint references users(id) on delete set null,
      scan_barcode text,
      label_scan_passed_at timestamptz,
      label_scan_passed_by bigint references users(id) on delete set null,
      label_scan_barcode text,
      collected_at timestamptz,
      collected_by bigint references users(id) on delete set null,
      sent_to_delivering_at timestamptz,
      sent_error text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (assembly_id, posting_number),
      unique (posting_number)
    )`
  );

  await dbQuery(
    `create table if not exists ozon_assembly_order_items (
      assembly_id text not null,
      posting_number text not null,
      line_no integer not null,
      offer_id text,
      sku bigint,
      name text,
      quantity integer not null default 1,
      scanned_qty integer not null default 0,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (assembly_id, posting_number, line_no),
      foreign key (assembly_id, posting_number)
        references ozon_assembly_orders(assembly_id, posting_number)
        on delete cascade
    )`
  );

  await dbQuery(
    `create table if not exists ozon_employee_assembly_sessions (
      id bigserial primary key,
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      assigned_by bigint references users(id) on delete set null,
      assignment_mode text not null default 'split',
      status text not null default 'assigned',
      assigned_at timestamptz not null default now(),
      started_at timestamptz,
      finished_at timestamptz,
      assigned_order_count integer not null default 0,
      collected_order_count integer not null default 0,
      total_seconds integer not null default 0,
      first_collected_at timestamptz,
      last_collected_at timestamptz,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      check (assignment_mode in ('split', 'redistribute', 'manual')),
      check (status in ('assigned', 'started', 'finished', 'reassigned', 'cancelled'))
    )`
  );

  await dbQuery(
    `create table if not exists ozon_collector_assembly_sessions (
      id bigserial primary key,
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      status text not null default 'started',
      started_at timestamptz not null default now(),
      finished_at timestamptz,
      total_seconds integer not null default 0,
      order_count integer not null default 0,
      item_count integer not null default 0,
      assembly_name text,
      store_id text,
      store_name text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      check (status in ('started', 'finished'))
    )`
  );

  await dbQuery(
    `create table if not exists ozon_employee_scan_issues (
      id bigserial primary key,
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
      posting_number text not null,
      employee_id bigint not null references users(id) on delete cascade,
      line_nos integer[] not null default '{}',
      item_articles text[] not null default '{}',
      item_names text[] not null default '{}',
      product_barcodes text[] not null default '{}',
      error_message text,
      scanned_barcode text,
      status text not null default 'open',
      resolved_at timestamptz,
      resolved_by bigint references users(id) on delete set null,
      fixed_at timestamptz,
      fixed_by bigint references users(id) on delete set null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      foreign key (assembly_id, posting_number)
        references ozon_assembly_orders(assembly_id, posting_number)
        on delete cascade,
      check (status in ('open', 'resolved', 'fixed'))
    )`
  );

  await dbQuery(
    `alter table ozon_assembly_orders
       add column if not exists collected_session_id bigint`
  );
  await dbQuery(
    `alter table ozon_assembly_orders
       add constraint ozon_assembly_orders_collected_session_id_fkey
       foreign key (collected_session_id)
       references ozon_employee_assembly_sessions(id)
       on delete set null`
  ).catch(() => {});

  await dbQuery("alter table ozon_assemblies add column if not exists store_id text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists store_name text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists warehouse_id bigint", []);
  await dbQuery("alter table ozon_assemblies add column if not exists warehouse_name text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists location_code text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists sklad_warehouse_id integer", []);
  await dbQuery("alter table ozon_assemblies add column if not exists sklad_warehouse_name text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists stock_method_enabled boolean", []);
  await dbQuery("update ozon_assemblies set stock_method_enabled = true where stock_method_enabled is null", []);
  await dbQuery("alter table ozon_assemblies alter column stock_method_enabled set default true", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies alter column stock_method_enabled set not null", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists allow_no_scan boolean", []);
  await dbQuery("alter table ozon_assemblies alter column allow_no_scan set default false", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists partial_safe_scan boolean", []);
  await dbQuery("update ozon_assemblies set partial_safe_scan = false where partial_safe_scan is null", []);
  await dbQuery("alter table ozon_assemblies alter column partial_safe_scan set default false", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies alter column partial_safe_scan set not null", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists allow_stock_shortage boolean", []);
  await dbQuery("update ozon_assemblies set allow_stock_shortage = false where allow_stock_shortage is null", []);
  await dbQuery("alter table ozon_assemblies alter column allow_stock_shortage set default false", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies alter column allow_stock_shortage set not null", []).catch(() => {});
  await dbQuery(
    `create table if not exists ozon_stock_shortage_skips (
       id bigserial primary key,
       assembly_id text not null references ozon_assemblies(id) on delete cascade,
       posting_number text,
       stage text not null,
       article text,
       barcode text,
       requested_quantity integer not null,
       processed_quantity integer not null default 0,
       skipped_quantity integer not null,
       user_id bigint references users(id) on delete set null,
       created_at timestamptz not null default now()
     )`,
    []
  );
  await dbQuery(
    "create index if not exists idx_ozon_stock_shortage_skips_assembly on ozon_stock_shortage_skips(assembly_id, created_at desc)",
    []
  );
  await dbQuery("alter table ozon_assemblies add column if not exists desktop_print boolean", []);
  await dbQuery("alter table ozon_assemblies alter column desktop_print set default false", []).catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists employee_workflow_mode text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists employee_workflow_stage text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists product_picking_started_at timestamptz", []);
  await dbQuery("alter table ozon_assemblies add column if not exists product_picking_started_by bigint", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_code text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_label_s3_key text", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_label_generated_at timestamptz", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_prepared_at timestamptz", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_confirmed_at timestamptz", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_confirmed_by bigint", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_postings_count integer not null default 0", []);
  await dbQuery("alter table ozon_assemblies add column if not exists shipment_posting_numbers text[] not null default '{}'", []);
  await dbQuery(
    `alter table ozon_assemblies
       add constraint ozon_assemblies_shipment_confirmed_by_fkey
       foreign key (shipment_confirmed_by)
       references users(id)
       on delete set null`
  ).catch(() => {});
  await dbQuery(
    `alter table ozon_assemblies
       add constraint ozon_assemblies_product_picking_started_by_fkey
       foreign key (product_picking_started_by)
       references users(id)
       on delete set null`
  ).catch(() => {});
  await dbQuery(
    `update ozon_assemblies
     set employee_workflow_mode = $1
     where employee_workflow_mode is null or employee_workflow_mode not in ('legacy', 'picking')`,
    [EMPLOYEE_WORKFLOW_MODE_PICKING]
  );
  await dbQuery(
    `update ozon_assemblies
     set employee_workflow_stage = case
       when coalesce(access_mode, 'hidden') = 'selected_split' then $1
       else $2
     end
     where employee_workflow_stage is null or employee_workflow_stage not in ('orders_only', 'pick_pending', 'pick_active')`,
    [EMPLOYEE_WORKFLOW_PICK_PENDING, EMPLOYEE_WORKFLOW_ORDERS_ONLY]
  );
  await dbQuery(
    "alter table ozon_assemblies alter column employee_workflow_mode set default 'picking'",
    []
  ).catch(() => {});
  await dbQuery(
    "alter table ozon_assemblies alter column employee_workflow_mode set not null",
    []
  ).catch(() => {});
  await dbQuery(
    "alter table ozon_assemblies alter column employee_workflow_stage set default 'orders_only'",
    []
  ).catch(() => {});
  await dbQuery(
    "alter table ozon_assemblies alter column employee_workflow_stage set not null",
    []
  ).catch(() => {});
  await dbQuery("alter table ozon_assemblies drop constraint if exists ozon_assemblies_employee_workflow_mode_check", []);
  await dbQuery(
    "alter table ozon_assemblies add constraint ozon_assemblies_employee_workflow_mode_check check (employee_workflow_mode in ('legacy', 'picking'))",
    []
  ).catch(() => {});
  await dbQuery("alter table ozon_assemblies drop constraint if exists ozon_assemblies_employee_workflow_stage_check", []);
  await dbQuery(
    "alter table ozon_assemblies add constraint ozon_assemblies_employee_workflow_stage_check check (employee_workflow_stage in ('orders_only', 'pick_pending', 'pick_active'))",
    []
  ).catch(() => {});
  await dbQuery("update ozon_assemblies set shipment_posting_numbers = '{}' where shipment_posting_numbers is null", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists store_id text", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists store_name text", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists warehouse_id bigint", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists warehouse_name text", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists delivery_method_id bigint", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists delivery_method_name text", []);
  await dbQuery(
    `create table if not exists ozon_assembly_assignment_parts (
      id bigserial primary key,
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
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
      updated_at timestamptz not null default now()
    )`,
    []
  );
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists role_mode text", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists picker_user_id bigint references users(id) on delete set null", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists packer_user_id bigint references users(id) on delete cascade", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists table_no integer", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists handoff_status text", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists picker_started_at timestamptz", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists picker_finished_at timestamptz", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists placed_on_table_at timestamptz", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists transfer_started_at timestamptz", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists transferred_at timestamptz", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists created_by bigint references users(id) on delete set null", []);
  await dbQuery("alter table ozon_assembly_assignment_parts add column if not exists updated_by bigint references users(id) on delete set null", []);
  await dbQuery(
    `update ozon_assembly_assignment_parts
     set role_mode = case
       when coalesce(picker_user_id, 0) > 0 and coalesce(picker_user_id, 0) <> coalesce(packer_user_id, 0)
         then 'split'
       else 'pick_pack'
     end
     where role_mode is null or role_mode not in ('pick_pack', 'split')`,
    []
  );
  await dbQuery(
    `update ozon_assembly_assignment_parts
     set handoff_status = case
       when role_mode = 'split' then 'waiting_pick'
       else 'not_required'
     end
     where handoff_status is null or handoff_status not in ('not_required', 'waiting_pick', 'ready', 'transferred')`,
    []
  );
  await dbQuery("alter table ozon_assembly_assignment_parts alter column role_mode set default 'pick_pack'", []).catch(() => {});
  await dbQuery("alter table ozon_assembly_assignment_parts alter column handoff_status set default 'not_required'", []).catch(() => {});
  await dbQuery("alter table ozon_assembly_assignment_parts alter column role_mode set not null", []).catch(() => {});
  await dbQuery("alter table ozon_assembly_assignment_parts alter column handoff_status set not null", []).catch(() => {});
  await dbQuery("alter table ozon_assembly_assignment_parts drop constraint if exists ozon_assembly_assignment_parts_role_mode_check", []);
  await dbQuery(
    "alter table ozon_assembly_assignment_parts add constraint ozon_assembly_assignment_parts_role_mode_check check (role_mode in ('pick_pack', 'split'))",
    []
  ).catch(() => {});
  await dbQuery("alter table ozon_assembly_assignment_parts drop constraint if exists ozon_assembly_assignment_parts_handoff_status_check", []);
  await dbQuery(
    "alter table ozon_assembly_assignment_parts add constraint ozon_assembly_assignment_parts_handoff_status_check check (handoff_status in ('not_required', 'waiting_pick', 'ready', 'transferred'))",
    []
  ).catch(() => {});
  await dbQuery("alter table ozon_assembly_assignment_parts drop constraint if exists ozon_assembly_assignment_parts_table_no_check", []);
  await dbQuery(
    "alter table ozon_assembly_assignment_parts add constraint ozon_assembly_assignment_parts_table_no_check check (table_no is null or table_no in (1, 2, 3))",
    []
  ).catch(() => {});
  await dbQuery("create index if not exists idx_ozon_assignment_parts_assembly on ozon_assembly_assignment_parts(assembly_id, part_no)", []);
  await dbQuery("create index if not exists idx_ozon_assignment_parts_packer on ozon_assembly_assignment_parts(packer_user_id, created_at desc)", []);
  await dbQuery("create index if not exists idx_ozon_assignment_parts_picker on ozon_assembly_assignment_parts(picker_user_id, created_at desc)", []);
  await dbQuery("alter table ozon_assembly_orders add column if not exists assignment_part_id bigint", []);
  await dbQuery("alter table ozon_employee_assembly_sessions add column if not exists picking_status text", []);
  await dbQuery("alter table ozon_employee_assembly_sessions add column if not exists picking_started_at timestamptz", []);
  await dbQuery("alter table ozon_employee_assembly_sessions add column if not exists picking_finished_at timestamptz", []);
  await dbQuery("alter table ozon_employee_assembly_sessions add column if not exists assignment_part_id bigint", []);
  await dbQuery("alter table ozon_employee_assembly_sessions add column if not exists session_role text", []);
  await dbQuery("alter table ozon_employee_assembly_sessions add column if not exists table_no integer", []);
  await dbQuery(
    "alter table ozon_employee_assembly_sessions add column if not exists picking_total_seconds integer not null default 0",
    []
  );
  await dbQuery(
    "alter table ozon_employee_assembly_sessions add column if not exists picking_required_qty integer not null default 0",
    []
  );
  await dbQuery(
    "alter table ozon_employee_assembly_sessions add column if not exists picking_collected_qty integer not null default 0",
    []
  );
  await dbQuery(
    "update ozon_employee_assembly_sessions set picking_status = $1 where picking_status is null or picking_status not in ('not_required', 'pending', 'started', 'finished', 'cancelled')",
    [PICKING_STATUS_NOT_REQUIRED]
  );
  await dbQuery(
    "alter table ozon_employee_assembly_sessions alter column picking_status set default 'not_required'",
    []
  ).catch(() => {});
  await dbQuery(
    "alter table ozon_employee_assembly_sessions alter column picking_status set not null",
    []
  ).catch(() => {});
  await dbQuery(
    "update ozon_employee_assembly_sessions set session_role = 'pick_pack' where session_role is null or session_role not in ('pick', 'pack', 'pick_pack')",
    []
  );
  await dbQuery("alter table ozon_employee_assembly_sessions alter column session_role set default 'pick_pack'", []).catch(() => {});
  await dbQuery("alter table ozon_employee_assembly_sessions alter column session_role set not null", []).catch(() => {});
  await dbQuery("alter table ozon_employee_assembly_sessions drop constraint if exists ozon_employee_assembly_sessions_session_role_check", []);
  await dbQuery(
    "alter table ozon_employee_assembly_sessions add constraint ozon_employee_assembly_sessions_session_role_check check (session_role in ('pick', 'pack', 'pick_pack'))",
    []
  ).catch(() => {});
  await dbQuery("alter table ozon_employee_assembly_sessions drop constraint if exists ozon_employee_assembly_sessions_table_no_check", []);
  await dbQuery(
    "alter table ozon_employee_assembly_sessions add constraint ozon_employee_assembly_sessions_table_no_check check (table_no is null or table_no in (1, 2, 3))",
    []
  ).catch(() => {});
  await dbQuery(
    "alter table ozon_employee_assembly_sessions drop constraint if exists ozon_employee_assembly_sessions_picking_status_check",
    []
  );
  await dbQuery(
    "alter table ozon_employee_assembly_sessions add constraint ozon_employee_assembly_sessions_picking_status_check check (picking_status in ('not_required', 'pending', 'started', 'finished', 'cancelled'))",
    []
  ).catch(() => {});
  await dbQuery(
    `create table if not exists ozon_employee_assembly_pick_items (
      id bigserial primary key,
      session_id bigint not null references ozon_employee_assembly_sessions(id) on delete cascade,
      assembly_id text not null references ozon_assemblies(id) on delete cascade,
      user_id bigint not null references users(id) on delete cascade,
      item_key text not null,
      article text,
      barcode text,
      product_name text,
      sku bigint,
      required_qty integer not null default 0,
      picked_qty integer not null default 0,
      bypass_without_scan_allowed boolean not null default false,
      bypass_authorized_at timestamptz,
      bypass_authorized_by bigint references users(id) on delete set null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )`
  );
  await dbQuery(
    "create unique index if not exists idx_ozon_employee_assembly_pick_items_unique on ozon_employee_assembly_pick_items(session_id, item_key)",
    []
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_assembly_pick_items_session on ozon_employee_assembly_pick_items(session_id, created_at asc, id asc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_assembly_pick_items_assembly_user on ozon_employee_assembly_pick_items(assembly_id, user_id, updated_at desc)",
    []
  );
  await dbQuery(
    `do $$
     begin
       if not exists (
         select 1
         from information_schema.table_constraints
         where constraint_name = 'fk_ozon_assembly_orders_assignment_part'
           and table_name = 'ozon_assembly_orders'
       ) then
         alter table ozon_assembly_orders
         add constraint fk_ozon_assembly_orders_assignment_part
         foreign key (assignment_part_id) references ozon_assembly_assignment_parts(id) on delete set null;
       end if;
     end $$;`,
    []
  );
  await dbQuery(
    `do $$
     begin
       if not exists (
         select 1
         from information_schema.table_constraints
         where constraint_name = 'fk_ozon_employee_assembly_sessions_assignment_part'
           and table_name = 'ozon_employee_assembly_sessions'
       ) then
         alter table ozon_employee_assembly_sessions
         add constraint fk_ozon_employee_assembly_sessions_assignment_part
         foreign key (assignment_part_id) references ozon_assembly_assignment_parts(id) on delete set null;
       end if;
     end $$;`,
    []
  );
  await dbQuery(
    `create table if not exists ozon_scan_barcode_aliases (
      id bigserial primary key,
      store_id text not null default '',
      marketplace_barcode text not null,
      scanned_barcode text not null,
      created_by bigint references users(id) on delete set null,
      updated_by bigint references users(id) on delete set null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )`
  );
  await dbQuery("alter table ozon_employee_scan_issues alter column posting_number drop not null", []).catch(() => {});
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists issue_stage text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists pick_item_id bigint", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists pick_item_key text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists item_names text[]", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists error_message text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists context_note text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists original_scanned_barcode text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists confirmation_scanned_barcode text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists marketplace_article text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists warehouse_product_id integer", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists warehouse_article text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists warehouse_product_name text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists mapping_barcode text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists image_url text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists remaining_quantity integer", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists resolution_type text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists resolution_note text", []);
  await dbQuery("alter table ozon_employee_scan_issues add column if not exists resolved_quantity integer", []);
  await dbQuery(
    "update ozon_employee_scan_issues set issue_stage = $1 where issue_stage is null or issue_stage not in ('order_scan', 'pick_scan')",
    [SCAN_ISSUE_STAGE_ORDER]
  );
  await dbQuery(
    "alter table ozon_employee_scan_issues alter column issue_stage set default 'order_scan'",
    []
  ).catch(() => {});
  await dbQuery(
    "alter table ozon_employee_scan_issues alter column issue_stage set not null",
    []
  ).catch(() => {});
  await dbQuery("alter table ozon_employee_scan_issues drop constraint if exists ozon_employee_scan_issues_issue_stage_check", []);
  await dbQuery(
    "alter table ozon_employee_scan_issues add constraint ozon_employee_scan_issues_issue_stage_check check (issue_stage in ('order_scan', 'pick_scan'))",
    []
  ).catch(() => {});

  if (DEFAULT_STORE) {
    await dbQuery(
      "update ozon_assemblies set store_id = $1, store_name = coalesce(store_name, $2) where store_id is null",
      [DEFAULT_STORE.id, DEFAULT_STORE.name]
    );
    await dbQuery(
      "update ozon_assembly_orders set store_id = $1, store_name = coalesce(store_name, $2) where store_id is null",
      [DEFAULT_STORE.id, DEFAULT_STORE.name]
    );
  }
  for (const store of storeList) {
    await dbQuery(
      "update ozon_assemblies set store_name = $2 where store_id = $1 and coalesce(store_name, '') <> $2",
      [store.id, store.name]
    );
    await dbQuery(
      "update ozon_assembly_orders set store_name = $2 where store_id = $1 and coalesce(store_name, '') <> $2",
      [store.id, store.name]
    );
  }

  if (storeList.length > 1) {
    // Legacy single-store sync assembly should not exist in multi-store mode.
    await dbQuery("delete from ozon_assemblies where id = 'ozon-sync-awaiting-deliver'");
  }

  await dbQuery("create index if not exists idx_ozon_assemblies_created_at on ozon_assemblies(created_at desc)");
  await dbQuery("create index if not exists idx_ozon_assemblies_store on ozon_assemblies(store_id, created_at desc)");
  await dbQuery(
    "create index if not exists idx_ozon_assembly_orders_assembly on ozon_assembly_orders(assembly_id)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_assembly_orders_assigned on ozon_assembly_orders(assigned_user_id)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_assembly_orders_assignment_part on ozon_assembly_orders(assignment_part_id)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_assembly_orders_collected on ozon_assembly_orders(collected_at)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_assembly_items_article on ozon_assembly_order_items(offer_id)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_assembly_items_assembly on ozon_assembly_order_items(assembly_id)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_assembly_sessions_assembly_user on ozon_employee_assembly_sessions(assembly_id, user_id, assigned_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_assembly_sessions_user_started on ozon_employee_assembly_sessions(user_id, started_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_assembly_sessions_assignment_part on ozon_employee_assembly_sessions(assignment_part_id)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_assembly_sessions_user_picking on ozon_employee_assembly_sessions(user_id, picking_started_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_collector_assembly_sessions_user_started on ozon_collector_assembly_sessions(user_id, started_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_collector_assembly_sessions_assembly_user on ozon_collector_assembly_sessions(assembly_id, user_id, started_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_scan_issues_open on ozon_employee_scan_issues(status, created_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_scan_issues_assembly_posting on ozon_employee_scan_issues(assembly_id, posting_number, status)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_employee_scan_issues_pick_item on ozon_employee_scan_issues(assembly_id, pick_item_id, status)"
  );
  await dbQuery(
    "create unique index if not exists idx_ozon_scan_barcode_aliases_unique on ozon_scan_barcode_aliases(store_id, marketplace_barcode, scanned_barcode)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_scan_barcode_aliases_scan on ozon_scan_barcode_aliases(scanned_barcode, store_id, updated_at desc)"
  );
  await dbQuery(
    "create index if not exists idx_ozon_scan_barcode_aliases_marketplace on ozon_scan_barcode_aliases(marketplace_barcode, store_id, updated_at desc)"
  );
}

ensureSchema().catch((err) => {
  console.error("Schema initialization failed:", err?.message || err);
});

function parseCookies(raw) {
  const out = {};
  if (!raw) return out;
  for (const part of String(raw).split(";")) {
    const idx = part.indexOf("=");
    if (idx < 1) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    out[key] = decodeURIComponent(value);
  }
  return out;
}

function serializeCookie(name, value, options = {}) {
  const parts = [`${name}=${encodeURIComponent(value)}`];
  if (options.maxAge !== undefined) parts.push(`Max-Age=${Math.max(0, Number(options.maxAge) || 0)}`);
  if (options.path) parts.push(`Path=${options.path}`);
  if (options.httpOnly) parts.push("HttpOnly");
  if (options.secure) parts.push("Secure");
  if (options.sameSite) parts.push(`SameSite=${options.sameSite}`);
  return parts.join("; ");
}

function getUnifiedSessionCookiePaths() {
  return Array.from(new Set([COOKIE_PATH, "/sklad", "/WB", "/OZ"].filter(Boolean)));
}

function setSessionCookie(res, token, expiresAt) {
  const maxAgeSec = Math.max(1, Math.floor((new Date(expiresAt).getTime() - Date.now()) / 1000));
  const cookies = getUnifiedSessionCookiePaths().map((cookiePath) =>
    serializeCookie(COOKIE_NAME, token, {
      maxAge: maxAgeSec,
      path: cookiePath,
      httpOnly: true,
      sameSite: "Lax",
      secure: COOKIE_SECURE,
    })
  );
  res.setHeader("Set-Cookie", cookies);
}

function clearSessionCookie(res) {
  const cookies = getUnifiedSessionCookiePaths().map((cookiePath) =>
    serializeCookie(COOKIE_NAME, "", {
      maxAge: 0,
      path: cookiePath,
      httpOnly: true,
      sameSite: "Lax",
      secure: COOKIE_SECURE,
    })
  );
  res.setHeader("Set-Cookie", cookies);
}

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
  if (derived.length !== stored.length) return false;
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

function getSkladWarehouseOptionsFromAssembly(row = {}) {
  const warehouseId = Number(row?.sklad_warehouse_id || 0) || SKLAD_FBS_WAREHOUSE_ID || 1;
  const warehouseName = normalizeText(row?.sklad_warehouse_name) || MS_FBS_STOCK_WAREHOUSE_NAME;
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
  marketplace = "oz",
  storeId = null,
  marketplaceWarehouseId = null,
} = {}) {
  const normalizedLocation = normalizeLocationCode(locationCode);
  if (!normalizedLocation) {
    const error = new Error("Выберите город");
    error.statusCode = 400;
    throw error;
  }
  const normalizedMarketplace = marketplace === "wb" ? "wb" : "oz";
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
    skladWarehouseName: normalizeText(row.sklad_warehouse_name) || MS_FBS_STOCK_WAREHOUSE_NAME,
  };
}

async function findUserById(userId) {
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
    [userId]
  );
  return result.rows?.[0] || null;
}

async function findManagedUserById(userId) {
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
    [userId, USER_AUTH_SOURCE_EXTERNAL]
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

async function deleteSession(token) {
  if (!token) return;
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

async function getAuth(req) {
  const internalAuth = getInternalProxyAuth(req);
  if (internalAuth) return internalAuth;
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[COOKIE_NAME];
  if (!token) return null;
  const cachedUser = getCachedAuthSession(token);
  if (cachedUser) return cachedUser;

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
  const row = result.rows?.[0];
  if (!row) return null;
  if (!canUseUserOnCurrentSite({ ...row, id: Number(row.user_id) })) {
    await deleteSession(token);
    return null;
  }

  const rowExpiresAt = new Date(row.expires_at);
  const rowExpiresAtMs = rowExpiresAt.getTime();
  if (!Number.isFinite(rowExpiresAtMs) || rowExpiresAtMs <= Date.now()) {
    await deleteSession(token);
    return null;
  }

  let effectiveExpires = rowExpiresAt;
  if (rowExpiresAtMs - Date.now() <= AUTH_SESSION_REFRESH_THRESHOLD_MS) {
    const nextExpires = new Date(Date.now() + SESSION_TTL_MS);
    await dbQuery("update sessions set expires_at = $1, updated_at = now() where token = $2", [
      nextExpires,
      token,
    ]);
    req.authRefreshedCookie = { token, expires: nextExpires };
    effectiveExpires = nextExpires;
  }

  const user = toPublicUser({ ...row, id: Number(row.user_id) });
  setCachedAuthSession(token, user, effectiveExpires);
  return user;
}

function requireAuth(roles = []) {
  return async (req, res, next) => {
    try {
      const auth = await getAuth(req);
      if (!auth) {
        return res.status(401).json({ error: "AUTH_REQUIRED" });
      }
      const allowedRoles = new Set(Array.isArray(roles) ? roles : []);
      if (allowedRoles.has(ROLE_ADMIN)) {
        allowedRoles.add(ROLE_SUPER);
      }
      if (allowedRoles.size > 0 && !allowedRoles.has(auth.role)) {
        return res.status(403).json({ error: "FORBIDDEN" });
      }
      req.auth = auth;
      if (req.authRefreshedCookie) {
        setSessionCookie(res, req.authRefreshedCookie.token, req.authRefreshedCookie.expires);
      }
      return next();
    } catch (err) {
      return next(err);
    }
  };
}

function ensurePackerAccess(req, res) {
  if (isPackerUser(req.auth)) return true;
  res.status(403).json({ error: "FORBIDDEN" });
  return false;
}

function ensureCollectorAccess(req, res) {
  if (isCollectorUser(req.auth)) return true;
  res.status(403).json({ error: "FORBIDDEN" });
  return false;
}

function getAuthExternalEmployeeId(auth) {
  const value = Number(auth?.externalEmployeeId || 0);
  return Number.isInteger(value) && value > 0 ? value : null;
}

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function asyncTimedHandler(metric, fn) {
  return asyncHandler(async (req, res, next) => {
    const started = process.hrtime.bigint();
    try {
      return await fn(req, res, next);
    } finally {
      recordPerf(metric, elapsedMs(started));
    }
  });
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function mapWithConcurrency(items, concurrency, worker, hooks = {}) {
  const list = Array.isArray(items) ? items : [];
  const total = list.length;
  if (total === 0) return [];
  const limit = Math.max(1, Number(concurrency || 1));
  const onStart = typeof hooks?.onStart === "function" ? hooks.onStart : null;
  const onProgress = typeof hooks?.onProgress === "function" ? hooks.onProgress : null;
  const out = new Array(total);
  let cursor = 0;
  let done = 0;

  const runner = async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= total) return;
      const item = list[index];
      if (onStart) {
        onStart({ index, item, done, total });
      }
      out[index] = await worker(item, index);
      done += 1;
      if (onProgress) {
        onProgress({ index, item, done, total });
      }
    }
  };

  await Promise.all(Array.from({ length: Math.min(limit, total) }, () => runner()));
  return out;
}

function getCardsCacheFile(storeId) {
  const normalized = normalizeStoreId(storeId);
  if (!normalized) return CARDS_CACHE_FILE;
  const parsed = path.parse(CARDS_CACHE_FILE);
  return path.join(parsed.dir, `${parsed.name}-${normalized}${parsed.ext}`);
}

async function ozonPost(endpoint, body, { raw = false, storeId = null, timeoutMs = OZON_API_TIMEOUT_MS } = {}) {
  const store = resolveStore(storeId);
  if (!store) {
    throw new Error("Ozon магазин не настроен");
  }
  let response;
  const timeout = Math.max(500, Number(timeoutMs || OZON_API_TIMEOUT_MS));
  const started = process.hrtime.bigint();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    response = await fetch(`${OZON_API_BASE}${endpoint}`, {
      method: "POST",
      headers: {
        "Client-Id": store.clientId,
        "Api-Key": store.apiKey,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(body || {}),
      signal: controller.signal,
    });
  } catch (err) {
    if (err?.name === "AbortError") {
      recordPerf("ext.ozon.timeout", timeout);
      throw new Error(`Ozon API timeout after ${timeout}ms`);
    }
    const msg = String(err?.message || "fetch failed");
    throw new Error(`Ошибка сети при запросе в Ozon API: ${msg}`);
  } finally {
    clearTimeout(timer);
    recordPerf("ext.ozon.request", elapsedMs(started));
  }

  if (!response.ok) {
    const text = await response.text();
    const err = new Error(`Ozon API ${response.status}: ${text.slice(0, 400)}`);
    err.status = response.status;
    err.responseText = text;
    throw err;
  }
  if (raw) return response;

  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}

function makeOzonRange(days = OZON_LIST_DAYS) {
  const to = new Date();
  to.setHours(23, 59, 59, 999);
  const since = new Date(to);
  since.setDate(since.getDate() - Math.max(1, Number(days) || OZON_LIST_DAYS));
  since.setHours(0, 0, 0, 0);
  return { since: since.toISOString(), to: to.toISOString() };
}

function mapOzonPosting(rawPosting) {
  const rawProducts = Array.isArray(rawPosting?.products) ? rawPosting.products : [];
  const rawDeliveryMethod = rawPosting?.delivery_method || rawPosting?.deliveryMethod || {};
  const rawAnalyticsData = rawPosting?.analytics_data || rawPosting?.analyticsData || {};
  const rawLegalInfo = rawPosting?.legal_info || rawPosting?.legalInfo || {};
  const deliveryMethodId = normalizeWarehouseId(rawDeliveryMethod?.id);
  const deliveryMethodName = normalizeText(rawDeliveryMethod?.name || "") || null;
  const warehouseId = normalizeWarehouseId(
    rawDeliveryMethod?.warehouse_id ?? rawDeliveryMethod?.warehouseId ?? rawPosting?.warehouse_id ?? rawPosting?.warehouseId
  );
  const warehouseName =
    normalizeText(rawDeliveryMethod?.warehouse || rawDeliveryMethod?.warehouse_name || rawPosting?.warehouse_name || "") || null;
  const legalCompanyName = normalizeText(rawLegalInfo?.company_name || rawLegalInfo?.companyName || "") || null;
  const legalInn = normalizeText(rawLegalInfo?.inn || "") || null;
  const legalKpp = normalizeText(rawLegalInfo?.kpp || "") || null;
  const analyticsIsLegal = rawAnalyticsData?.is_legal;
  const isLegal = analyticsIsLegal === true || (!Number.isNaN(Number(analyticsIsLegal)) && Number(analyticsIsLegal) === 1)
    || Boolean(legalCompanyName || legalInn || legalKpp);
  const products = rawProducts.map((item) => {
    const quantity = Math.max(1, Number(item?.quantity || 1));
    const skuRaw = item?.sku ?? item?.product_id ?? item?.productId ?? null;
    const skuNum = Number(skuRaw);
    const barcodes = uniqueTextList([
      ...(Array.isArray(item?.barcodes) ? item.barcodes : []),
      item?.barcode,
      item?.bar_code,
    ]);
    const sku = Number.isFinite(skuNum) && skuNum > 0 ? skuNum : null;
    return {
      offerId: normalizeArticleKey(item?.offer_id || item?.offerId || item?.article || null),
      name: normalizeText(item?.name || ""),
      quantity,
      sku,
      barcodes,
      marketplaceBarcode: pickPreferredOzonMarketplaceBarcode(barcodes, sku) || "",
    };
  });

  return {
    postingNumber: normalizeText(rawPosting?.posting_number || rawPosting?.postingNumber),
    status: normalizeText(rawPosting?.status || ""),
    createdAt:
      rawPosting?.created_at ||
      rawPosting?.in_process_at ||
      rawPosting?.shipment_date ||
      rawPosting?.order_date ||
      null,
    shipmentDate: rawPosting?.shipment_date || null,
    labelBarcode:
      normalizeText(rawPosting?.barcodes?.lower_barcode || rawPosting?.barcodes?.upper_barcode || "") || null,
    deliveryMethodId,
    deliveryMethodName,
    warehouseId,
    warehouseName,
    tplIntegrationType: normalizeText(rawPosting?.tpl_integration_type || rawPosting?.tplIntegrationType || "") || null,
    isLegal,
    customerType: isLegal ? "business" : "individual",
    legalCompanyName,
    legalInn,
    legalKpp,
    products,
  };
}

async function fetchFbsPostingsByStatus(status, offset = 0, limit = OZON_PAGE_LIMIT, options = {}) {
  const days = Number.isFinite(Number(options?.days)) ? Number(options.days) : OZON_LIST_DAYS;
  const sortDir = options?.sortDir === "DESC" ? "DESC" : "ASC";
  const includeBarcodes = options?.includeBarcodes === true;
  const range = makeOzonRange(days);
  return ozonPost("/v3/posting/fbs/list", {
    dir: sortDir,
    filter: {
      since: range.since,
      to: range.to,
      status,
    },
    limit,
    offset,
    with: {
      barcodes: includeBarcodes,
      analytics_data: true,
    },
  }, { storeId: options?.storeId, timeoutMs: options?.timeoutMs || OZON_API_TIMEOUT_MS });
}

async function fetchAllFbsPostingsByStatus(status, options = {}) {
  const postings = [];
  let offset = 0;
  const limit = Number.isFinite(Number(options?.limit))
    ? Math.max(1, Number(options.limit))
    : OZON_PAGE_LIMIT;
  const maxPages = Number.isFinite(Number(options?.maxPages))
    ? Math.max(1, Number(options.maxPages))
    : Number.POSITIVE_INFINITY;
  let page = 0;
  while (true) {
    if (page >= maxPages) break;
    const data = await fetchFbsPostingsByStatus(status, offset, limit, options);
    const rows = Array.isArray(data?.result?.postings) ? data.result.postings : [];
    for (const row of rows) {
      const mapped = mapOzonPosting(row);
      if (mapped.postingNumber) postings.push(mapped);
    }
    page += 1;
    const hasNext = data?.result?.has_next === true;
    if (!hasNext || rows.length === 0) break;
    offset += limit;
  }
  return postings;
}

async function fetchPostingDetails(postingNumber, storeId = null, options = {}) {
  const data = await ozonPost("/v3/posting/fbs/get", {
    posting_number: postingNumber,
    with: {
      barcodes: true,
      analytics_data: true,
    },
  }, { storeId, timeoutMs: options?.timeoutMs || OZON_API_TIMEOUT_MS });
  return mapOzonPosting(data?.result || {});
}

function isOzonPostingInactiveForAssembly(statusRaw) {
  const status = normalizeText(statusRaw || "");
  if (!status) return false;
  if (status === "delivering" || status === "delivered") return true;
  if (status.includes("cancel")) return true;
  if (status === "not_accepted" || status === "returned_to_seller") return true;
  return false;
}

async function shipPostingToAwaitingDeliver(postingNumber, products, storeId = null) {
  const packedProducts = [];
  for (const product of products || []) {
    const quantity = Math.max(1, Number(product?.quantity || 1));
    const productId = Number(product?.sku);
    if (!Number.isFinite(productId) || productId <= 0) continue;
    packedProducts.push({
      product_id: productId,
      quantity,
    });
  }
  if (!packedProducts.length) {
    throw new Error("Не удалось сформировать package для Ozon (нет product_id). ");
  }
  return ozonPost("/v4/posting/fbs/ship", {
    posting_number: postingNumber,
    packages: [{ products: packedProducts }],
    with: { additional_data: true },
  }, { storeId });
}

async function getLabelPdfBuffer(postingNumber, storeId = null) {
  const response = await ozonPost(
    "/v2/posting/fbs/package-label",
    {
      posting_number: [postingNumber],
    },
    { raw: true, storeId }
  );
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    const text = await response.text();
    throw new Error(`Ozon не вернул PDF этикетки: ${text.slice(0, 300)}`);
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  if (!buffer.length) {
    throw new Error("Ozon вернул пустую этикетку");
  }
  return buffer;
}

async function movePostingsToDelivering(postingNumbers, storeId = null) {
  if (!postingNumbers.length) return;
  await ozonPost("/v2/fbs/posting/delivering", {
    posting_number: postingNumbers,
  }, { storeId });
}

async function createFbsAct(storeId = null, options = {}) {
  const requestedContainers = Number(options?.containersCount);
  const containersCount = Number.isFinite(requestedContainers)
    ? Math.max(1, Math.floor(requestedContainers))
    : 1;
  const data = await ozonPost(
    "/v2/posting/fbs/act/create",
    {
      containers_count: containersCount,
    },
    { storeId }
  );
  const actId = Number(data?.result?.id || 0);
  if (!Number.isFinite(actId) || actId < 1) {
    throw new Error("Ozon не вернул ID акта отгрузки");
  }
  return actId;
}

async function checkFbsActStatus(actId, storeId = null) {
  return ozonPost(
    "/v2/posting/fbs/act/check-status",
    {
      id: Number(actId),
    },
    { storeId }
  );
}

async function getFbsActPostings(actId, storeId = null) {
  const data = await ozonPost(
    "/v2/posting/fbs/act/get-postings",
    {
      id: Number(actId),
    },
    { storeId }
  );
  return normalizePostingNumbers((Array.isArray(data?.result) ? data.result : []).map((row) => row?.posting_number));
}

async function getFbsActPdfBuffer(actId, storeId = null) {
  const response = await ozonPost(
    "/v2/posting/fbs/act/get-pdf",
    {
      id: Number(actId),
    },
    { raw: true, storeId }
  );
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    const text = await response.text();
    throw new Error(`Ozon не вернул PDF акта: ${text.slice(0, 300)}`);
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  if (!buffer.length) {
    throw new Error("Ozon вернул пустой PDF акта");
  }
  return buffer;
}

async function getFbsActBarcodePngBuffer(actId, storeId = null) {
  const response = await ozonPost(
    "/v2/posting/fbs/act/get-barcode",
    {
      id: Number(actId),
    },
    { raw: true, storeId }
  );
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    const text = await response.text();
    throw new Error(`Ozon не вернул PNG штрихкода акта: ${text.slice(0, 300)}`);
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  if (!buffer.length) {
    throw new Error("Ozon вернул пустой PNG штрихкода акта");
  }
  return buffer;
}

async function getFbsActContainerLabelsPdfBuffer(actId, storeId = null) {
  const response = await ozonPost(
    "/v2/posting/fbs/act/get-container-labels",
    {
      id: Number(actId),
    },
    { raw: true, storeId }
  );
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    const text = await response.text();
    throw new Error(`Ozon не вернул PDF контейнерных ярлыков: ${text.slice(0, 300)}`);
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  if (!buffer.length) {
    throw new Error("Ozon вернул пустой PDF контейнерных ярлыков");
  }
  return buffer;
}

async function getFbsDigitalActStatus(actId, storeId = null) {
  return ozonPost(
    "/v2/posting/fbs/digital/act/check-status",
    {
      id: Number(actId),
    },
    { storeId }
  );
}

async function getFbsDigitalActPdfBuffer(actId, storeId = null) {
  const response = await ozonPost(
    "/v2/posting/fbs/digital/act/get-pdf",
    {
      id: Number(actId),
    },
    { raw: true, storeId }
  );
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    const text = await response.text();
    throw new Error(`Ozon не вернул PDF digital-акта: ${text.slice(0, 300)}`);
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  if (!buffer.length) {
    throw new Error("Ozon вернул пустой PDF digital-акта");
  }
  return buffer;
}

async function ensureReadyFbsAct(storeId, options = {}) {
  const actId = await createFbsAct(storeId, {
    containersCount: options?.containersCount,
  });
  let statusRaw = "";
  let statusReady = false;
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const statusData = await checkFbsActStatus(actId, storeId);
    statusRaw = normalizeText(statusData?.result?.status || "");
    if (statusRaw === "ready") {
      statusReady = true;
      break;
    }
    if (statusRaw === "error" || statusRaw === "failed") {
      throw new Error("Ozon вернул ошибку формирования акта отгрузки");
    }
    await wait(900);
  }
  if (!statusReady) {
    throw new Error(statusRaw ? `Акт отгрузки ещё формируется (${statusRaw})` : "Акт отгрузки ещё формируется");
  }
  const actPostingNumbers = await getFbsActPostings(actId, storeId);
  const expectedPostingNumbers = normalizePostingNumbers(options?.expectedPostingNumbers);
  if (expectedPostingNumbers.length && actPostingNumbers.length) {
    const actSet = new Set(actPostingNumbers);
    const overlap = expectedPostingNumbers.filter((postingNumber) => actSet.has(postingNumber)).length;
    if (overlap === 0) {
      throw new Error("В текущем акте Ozon нет заказов этой сборки");
    }
  }
  return {
    actId,
    status: statusRaw || "ready",
    postingNumbers: actPostingNumbers,
  };
}

async function buildFbsActPdfForStore(storeId, options = {}) {
  const act = await ensureReadyFbsAct(storeId, options);
  const pdfBuffer = await getFbsActPdfBuffer(act.actId, storeId);
  return {
    actId: act.actId,
    status: act.status,
    postingNumbers: act.postingNumbers,
    pdfBuffer,
  };
}

function buildAssemblyId() {
  return `ozon-${Date.now()}-${crypto.randomBytes(3).toString("hex")}`;
}

function buildAssemblyName(orderCount = 0) {
  // Дата/время в названии — по Москве. Сервер работает в UTC, поэтому getHours()
  // давал время на 3 часа меньше (05:06 вместо 08:06). Берём МСК через Intl.
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: MOSCOW_TZ,
    day: "2-digit", month: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(new Date());
  const get = (t) => String(parts.find((p) => p.type === t)?.value || "").padStart(2, "0");
  const dd = get("day"), mm = get("month"), hh = get("hour"), min = get("minute");
  const countPart = orderCount > 0 ? ` · ${orderCount} заказов` : "";
  return `Сборка Ozon ${dd}.${mm} ${hh}:${min}${countPart}`;
}

const CREATE_REQUEST_TTL_MS = 10 * 60 * 1000;
const CREATE_ASSEMBLY_RECOVERY_WINDOW_MS = 10 * 60 * 1000;
const recentCreateAssemblyRequests = new Map();

function buildCreateRequestKey(storeId, values = []) {
  const normalizedStore = normalizeStoreId(storeId) || "";
  const normalizedValues = Array.from(
    new Set((Array.isArray(values) ? values : [values]).map((value) => normalizeText(value)).filter(Boolean))
  ).sort();
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

async function findReusableAssemblyForCreate({ storeId, warehouseId, name, postingNumbers, allowStockShortage = false, windowMs = CREATE_ASSEMBLY_RECOVERY_WINDOW_MS }) {
  const normalizedStoreId = normalizeStoreId(storeId);
  const normalizedWarehouseId = normalizeWarehouseId(warehouseId);
  const normalizedName = normalizeText(name);
  const normalizedPostingNumbers = normalizePostingNumbers(postingNumbers);
  if (!normalizedStoreId || !normalizedWarehouseId || (!normalizedName && !normalizedPostingNumbers.length)) return null;
  const result = await dbQuery(
    `with candidate_assemblies as (
       select a.id, a.name, a.created_at,
              count(*) filter (where o.posting_number = any($4::text[]))::int as overlap_count
       from ozon_assemblies a
       left join ozon_assembly_orders o on o.assembly_id = a.id
       where a.store_id = $1
         and coalesce(a.warehouse_id, 0) = $2
         and coalesce(a.allow_stock_shortage, false) = $6
         and a.access_mode = 'hidden'
         and a.created_at >= now() - ($5::text || ' milliseconds')::interval
         and (
           lower(btrim(coalesce(a.name, ''))) = lower(btrim($3))
           or o.posting_number = any($4::text[])
         )
         and not exists (select 1 from ozon_assembly_access_users au where au.assembly_id = a.id)
         and not exists (select 1 from ozon_employee_assembly_sessions es where es.assembly_id = a.id)
         and not exists (select 1 from ozon_collector_assembly_sessions cs where cs.assembly_id = a.id)
         and not exists (select 1 from ozon_employee_assembly_pick_items pi where pi.assembly_id = a.id)
         and not exists (select 1 from ozon_employee_scan_issues si where si.assembly_id = a.id)
       group by a.id, a.name, a.created_at
     )
     select id, name, created_at, overlap_count
     from candidate_assemblies
     where overlap_count > 0
        or lower(btrim(coalesce(name, ''))) = lower(btrim($3))
     order by overlap_count desc, created_at desc, id desc
     limit 1`,
    [normalizedStoreId, normalizedWarehouseId, normalizedName, normalizedPostingNumbers, String(windowMs), allowStockShortage === true]
  );
  return result.rows?.[0] || null;
}

async function getAssemblyPostingNumberSet(assemblyId) {
  const normalizedAssemblyId = normalizeText(assemblyId);
  if (!normalizedAssemblyId) return new Set();
  const result = await dbQuery(
    "select posting_number from ozon_assembly_orders where assembly_id = $1",
    [normalizedAssemblyId]
  );
  return new Set((result.rows || []).map((row) => normalizeText(row.posting_number)).filter(Boolean));
}

function buildLabelsPrefix(assemblyId) {
  return `ozon/${assemblyId}`;
}

function formatMoscowDateTime(value = new Date()) {
  try {
    return new Date(value).toLocaleString("ru-RU", { timeZone: MOSCOW_TZ });
  } catch {
    return new Date().toLocaleString("ru-RU", { timeZone: MOSCOW_TZ });
  }
}

function normalizePostingNumbers(values) {
  const out = [];
  for (const value of Array.isArray(values) ? values : []) {
    const normalized = normalizeText(value);
    if (normalized && !out.includes(normalized)) out.push(normalized);
  }
  return out;
}

function buildShipmentCode() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mm = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");
  const random = crypto.randomBytes(2).toString("hex").toUpperCase();
  return `OZON-${y}${m}${d}-${hh}${mm}${ss}-${random}`;
}

function buildShipmentLabelKey(assemblyId, shipmentCode) {
  const safeCode = String(shipmentCode || "")
    .trim()
    .replace(/[^A-Za-z0-9_-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  const suffix = safeCode || `SHIP-${Date.now()}`;
  return `ozon/${assemblyId}/shipment/${suffix}.pdf`;
}

async function createShipmentLabelPdf({ assembly, shipmentCode, postingNumbers }) {
  const pdfDoc = await PDFDocument.create();
  const page = pdfDoc.addPage([595, 420]);
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const barcodePng = await bwipjs.toBuffer({
    bcid: "code128",
    text: shipmentCode,
    scale: 3,
    height: 22,
    includetext: false,
    backgroundcolor: "FFFFFF",
  });
  const barcodeImage = await pdfDoc.embedPng(barcodePng);
  const barcodeDims = barcodeImage.scale(1);
  const barcodeWidth = Math.min(500, barcodeDims.width);
  const barcodeHeight = (barcodeDims.height * barcodeWidth) / barcodeDims.width;

  page.drawText("Ozon · Этикетка отгрузки", {
    x: 40,
    y: 380,
    size: 20,
    font: fontBold,
    color: rgb(0.1, 0.16, 0.25),
  });
  page.drawText(`Сборка: ${normalizeText(assembly?.name) || "-"}`, {
    x: 40,
    y: 350,
    size: 11,
    font,
    color: rgb(0.18, 0.22, 0.3),
  });
  page.drawText(`Магазин: ${normalizeText(assembly?.store_name) || "-"}`, {
    x: 40,
    y: 333,
    size: 11,
    font,
    color: rgb(0.18, 0.22, 0.3),
  });
  page.drawText(`Создано: ${formatMoscowDateTime(new Date())} (${MOSCOW_TZ})`, {
    x: 40,
    y: 316,
    size: 10,
    font,
    color: rgb(0.28, 0.34, 0.44),
  });

  const barcodeX = (595 - barcodeWidth) / 2;
  page.drawImage(barcodeImage, {
    x: barcodeX,
    y: 220,
    width: barcodeWidth,
    height: barcodeHeight,
  });
  page.drawText(shipmentCode, {
    x: 40,
    y: 200,
    size: 18,
    font: fontBold,
    color: rgb(0.05, 0.09, 0.16),
  });
  page.drawText(`Заказов в поставке: ${postingNumbers.length}`, {
    x: 40,
    y: 177,
    size: 12,
    font: fontBold,
    color: rgb(0.1, 0.16, 0.25),
  });

  const maxRows = 8;
  const shown = postingNumbers.slice(0, maxRows);
  let y = 154;
  for (const postingNumber of shown) {
    page.drawText(`• ${postingNumber}`, {
      x: 50,
      y,
      size: 10,
      font,
      color: rgb(0.14, 0.18, 0.25),
    });
    y -= 15;
  }
  if (postingNumbers.length > maxRows) {
    page.drawText(`... и ещё ${postingNumbers.length - maxRows} заказов`, {
      x: 50,
      y: y - 2,
      size: 10,
      font,
      color: rgb(0.34, 0.38, 0.45),
    });
  }

  return await pdfDoc.save();
}

function getOzonProductArticleSet(products = []) {
  const set = new Set();
  for (const product of Array.isArray(products) ? products : []) {
    const article = normalizeArticleKey(product?.offerId || product?.offer_id || product?.article || "");
    if (article) set.add(article);
  }
  return set;
}

function isOzonPostingExclusiveToArticles(products = [], selectedArticles = new Set()) {
  if (!selectedArticles || selectedArticles.size === 0) return true;
  const rows = Array.isArray(products) ? products : [];
  if (!rows.length) return false;
  for (const product of rows) {
    const article = normalizeArticleKey(product?.offerId || product?.offer_id || product?.article || "");
    if (!article || !selectedArticles.has(article)) return false;
  }
  return true;
}

function toPdfAscii(value, fallback = "-") {
  const text = normalizeText(value) || fallback;
  return text
    .replace(/[•·]/g, "-")
    .replace(/[–—]/g, "-")
    .replace(/[«»“”]/g, '"')
    .replace(/[^\x20-\x7E]+/g, "?");
}

function truncatePdfText(value, maxLength = 110) {
  const text = toPdfAscii(value);
  return text.length > maxLength ? `${text.slice(0, Math.max(0, maxLength - 3))}...` : text;
}

function formatOzonMarketplaceBarcodeForPdf(sku) {
  const normalized = String(sku ?? "").trim();
  if (!normalized) return "";
  if (/^OZN/i.test(normalized)) return normalized;
  return /^\d+$/.test(normalized) ? `OZN${normalized}` : normalized;
}

function resolvePdfFontPath(bold = false) {
  const candidates = bold
    ? [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "C:\\Windows\\Fonts\\arialbd.ttf",
      ]
    : [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:\\Windows\\Fonts\\arial.ttf",
      ];
  return candidates.find((candidate) => fs.existsSync(candidate)) || null;
}

async function embedCyrillicPdfFonts(pdfDoc) {
  pdfDoc.registerFontkit(fontkit);
  const regularPath = resolvePdfFontPath(false);
  const boldPath = resolvePdfFontPath(true);
  if (!regularPath || !boldPath) {
    throw new Error("Не найден кириллический шрифт для PDF листа подбора");
  }
  const [regularBytes, boldBytes] = await Promise.all([
    fs.promises.readFile(regularPath),
    fs.promises.readFile(boldPath),
  ]);
  return {
    regular: await pdfDoc.embedFont(regularBytes),
    bold: await pdfDoc.embedFont(boldBytes),
  };
}

function wrapPdfText(text, font, size, maxWidth) {
  const normalized = normalizeText(text) || "-";
  const words = normalized.split(/\s+/).filter(Boolean);
  const lines = [];
  let line = "";
  const pushLongWord = (word) => {
    let rest = word;
    while (rest) {
      let chunk = rest;
      while (chunk.length > 1 && font.widthOfTextAtSize(chunk, size) > maxWidth) {
        chunk = chunk.slice(0, -1);
      }
      lines.push(chunk);
      rest = rest.slice(chunk.length);
    }
  };
  for (const word of words) {
    const next = line ? `${line} ${word}` : word;
    if (font.widthOfTextAtSize(next, size) <= maxWidth) {
      line = next;
    } else {
      if (line) lines.push(line);
      if (font.widthOfTextAtSize(word, size) > maxWidth) {
        pushLongWord(word);
        line = "";
      } else {
        line = word;
      }
    }
  }
  if (line) lines.push(line);
  return lines.length ? lines : ["-"];
}

function truncatePdfTextToWidth(text, font, size, maxWidth) {
  const normalized = normalizeText(text) || "-";
  if (font.widthOfTextAtSize(normalized, size) <= maxWidth) return normalized;
  let shortened = normalized;
  while (shortened.length > 1 && font.widthOfTextAtSize(`${shortened}...`, size) > maxWidth) {
    shortened = shortened.slice(0, -1);
  }
  return `${shortened}...`;
}

function splitOzonPostingHighlight(value) {
  const normalized = normalizeText(value).replace(/\s+/g, "");
  if (!normalized) return { before: "", highlight: "-", after: "" };
  const separatorIndex = normalized.indexOf("-");
  const firstPart = separatorIndex >= 0 ? normalized.slice(0, separatorIndex) : normalized;
  const highlightStart = Math.min(6, firstPart.length);
  const highlightEnd = Math.min(highlightStart + 4, firstPart.length);
  return {
    before: firstPart.slice(0, highlightStart),
    highlight: firstPart.slice(highlightStart, highlightEnd) || firstPart,
    after: `${firstPart.slice(highlightEnd)}${separatorIndex >= 0 ? normalized.slice(separatorIndex) : ""}`,
  };
}

function drawOzonHighlightedPostingNumber(page, value, { x, y, fontBold, size = 12.5 }) {
  const parts = splitOzonPostingHighlight(value);
  let cursorX = x;
  const prefix = "ЗАКАЗ ";
  page.drawText(prefix, { x: cursorX, y, size, font: fontBold, color: rgb(0.07, 0.09, 0.13) });
  cursorX += fontBold.widthOfTextAtSize(prefix, size);
  if (parts.before) {
    page.drawText(parts.before, { x: cursorX, y, size, font: fontBold, color: rgb(0.07, 0.09, 0.13) });
    cursorX += fontBold.widthOfTextAtSize(parts.before, size);
  }
  page.drawRectangle({
    x: cursorX - 1,
    y: y - 2,
    width: fontBold.widthOfTextAtSize(parts.highlight, size) + 2,
    height: size + 4,
    color: rgb(1, 0.93, 0.35),
  });
  page.drawText(parts.highlight, { x: cursorX, y, size, font: fontBold, color: rgb(0.02, 0.05, 0.1) });
  cursorX += fontBold.widthOfTextAtSize(parts.highlight, size);
  if (parts.after) {
    page.drawText(parts.after, { x: cursorX, y, size, font: fontBold, color: rgb(0.07, 0.09, 0.13) });
  }
}

async function loadPdfImageFromUrl(url) {
  const imageUrl = normalizeText(url);
  if (!imageUrl || !/^https?:\/\//i.test(imageUrl)) return null;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const response = await fetch(imageUrl, { signal: controller.signal });
    if (!response.ok) return null;
    const contentType = normalizeText(response.headers.get("content-type")).toLowerCase();
    const bytes = Buffer.from(await response.arrayBuffer());
    return {
      bytes,
      type: contentType.includes("png") || /\.png(?:$|\?)/i.test(imageUrl) ? "png" : "jpg",
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function embedOzonPickingImages(pdfDoc, storeId, articles) {
  const images = new Map();
  const imageUrls = await resolveOzonCardImages(storeId, articles);
  const loaded = await mapWithConcurrency([...imageUrls.entries()], 12, async ([article, imageUrl]) => ({
    article,
    source: await loadPdfImageFromUrl(imageUrl),
  }));
  for (const { article, source } of loaded) {
    if (!source?.bytes?.length) continue;
    try {
      const image = source.type === "png"
        ? await pdfDoc.embedPng(source.bytes)
        : await pdfDoc.embedJpg(source.bytes);
      images.set(normalizeArticleKey(article), image);
    } catch {
      // Картинка не должна блокировать формирование листа подбора.
    }
  }
  return images;
}

async function buildOzonPickingListPdf(assembly) {
  const assemblyId = normalizeText(assembly?.id);
  const result = await dbQuery(
    `select o.posting_number,
            o.order_created_at,
            o.label_barcode,
            o.label_s3_key,
            i.line_no,
            i.offer_id as article,
            i.name,
            i.sku,
            greatest(coalesce(i.quantity, 1), 1)::int as qty
     from ozon_assembly_orders o
     join ozon_assembly_order_items i
       on i.assembly_id = o.assembly_id
      and i.posting_number = o.posting_number
     where o.assembly_id = $1
     order by o.order_created_at asc nulls last, o.posting_number asc, i.line_no asc`,
    [assemblyId]
  );
  const rows = result.rows || [];
  if (!rows.length) return null;

  const orders = [];
  const orderByPosting = new Map();
  for (const row of rows) {
    const postingNumber = normalizeText(row.posting_number);
    if (!orderByPosting.has(postingNumber)) {
      const order = {
        postingNumber,
        createdAt: row.order_created_at,
        labelBarcode: normalizeText(row.label_barcode || ""),
        labelKey: normalizeText(row.label_s3_key || ""),
        items: [],
      };
      orderByPosting.set(postingNumber, order);
      orders.push(order);
    }
    orderByPosting.get(postingNumber).items.push({
      article: normalizeArticleKey(row.article || ""),
      name: normalizeText(row.name || "-"),
      sku: Number(row.sku || 0) > 0 ? Number(row.sku) : null,
      qty: Number(row.qty || 1),
    });
  }

  const incompleteLabels = orders.filter((order) => !order.labelKey);
  if (incompleteLabels.length) {
    const error = new Error(
      `Лист подбора не создан: у ${incompleteLabels.length} из ${orders.length} заказов нет загруженной этикетки. Нажмите «Обновить этикетки» и повторите.`
    );
    error.statusCode = 409;
    throw error;
  }

  const pdfDoc = await PDFDocument.create();
  const { regular: font, bold: fontBold } = await embedCyrillicPdfFonts(pdfDoc);
  const articleImages = await embedOzonPickingImages(
    pdfDoc,
    assembly?.store_id || null,
    rows.map((row) => row.article).filter(Boolean)
  ).catch(() => new Map());
  const pageSize = [595, 842];
  const marginX = 28;
  const contentWidth = pageSize[0] - marginX * 2;
  const totalUnits = rows.reduce((sum, row) => sum + Number(row.qty || 0), 0);
  const footerY = 22;
  const bodyBottom = 42;
  const bodyTop = 746;
  const colors = {
    ink: rgb(0.07, 0.09, 0.13),
    muted: rgb(0.36, 0.39, 0.44),
    line: rgb(0.82, 0.84, 0.87),
    panel: rgb(0.985, 0.987, 0.99),
    panelAlt: rgb(0.97, 0.974, 0.98),
    orange: rgb(0.94, 0.29, 0.12),
    orangeSoft: rgb(1, 0.94, 0.9),
    green: rgb(0.02, 0.42, 0.22),
    greenSoft: rgb(0.9, 0.97, 0.93),
  };
  let page = null;
  let y = 0;
  let pageNo = 0;

  const addPage = () => {
    page = pdfDoc.addPage(pageSize);
    pageNo += 1;
    page.drawRectangle({ x: 0, y: 832, width: pageSize[0], height: 10, color: colors.orange });
    page.drawText("OZON  /  ЛИСТ ПОДБОРА", {
      x: marginX,
      y: 811,
      size: 8.5,
      font: fontBold,
      color: colors.orange,
    });
    page.drawText(
      truncatePdfTextToWidth(normalizeText(assembly?.name || assemblyId), fontBold, 15, 340),
      { x: marginX, y: 786, size: 15, font: fontBold, color: colors.ink }
    );
    const summary = `${orders.length} заказов  /  ${totalUnits} шт.`;
    page.drawText(summary, {
      x: pageSize[0] - marginX - fontBold.widthOfTextAtSize(summary, 10.5),
      y: 788,
      size: 10.5,
      font: fontBold,
      color: colors.green,
    });
    page.drawText(
      `Магазин: ${normalizeText(assembly?.store_name || "-")}  |  Сформирован: ${formatMoscowDateTime(new Date())}`,
      { x: marginX, y: 767, size: 8.5, font, color: colors.muted }
    );
    page.drawLine({
      start: { x: marginX, y: 755 },
      end: { x: pageSize[0] - marginX, y: 755 },
      thickness: 0.8,
      color: colors.line,
    });
    y = bodyTop;
  };

  const ensureSpace = (height = 74) => {
    if (!page || y - height < bodyBottom) addPage();
  };

  addPage();
  orders.forEach((order, index) => {
    const itemHeights = order.items.map((item) => {
      const nameLines = wrapPdfText(item.name, font, 8.7, contentWidth - 205).slice(0, 3);
      return Math.max(68, 40 + nameLines.length * 9);
    });
    const cardHeight = 44 + itemHeights.reduce((sum, height) => sum + height, 0);
    ensureSpace(cardHeight + 5);
    const cardBottom = y - cardHeight;
    const headerBottom = y - 44;

    page.drawRectangle({
      x: marginX,
      y: cardBottom,
      width: contentWidth,
      height: cardHeight,
      borderWidth: 0.8,
      borderColor: colors.line,
      color: index % 2 === 0 ? colors.panel : colors.panelAlt,
    });
    page.drawRectangle({ x: marginX, y: cardBottom, width: 5, height: cardHeight, color: colors.orange });
    page.drawRectangle({
      x: marginX + 5,
      y: headerBottom,
      width: contentWidth - 5,
      height: 44,
      color: colors.orangeSoft,
    });
    page.drawRectangle({
      x: marginX + 15,
      y: y - 32,
      width: 29,
      height: 24,
      color: colors.orange,
    });
    const indexText = String(index + 1);
    page.drawText(indexText, {
      x: marginX + 29.5 - fontBold.widthOfTextAtSize(indexText, 11) / 2,
      y: y - 25,
      size: 11,
      font: fontBold,
      color: rgb(1, 1, 1),
    });
    drawOzonHighlightedPostingNumber(page, order.postingNumber, {
      x: marginX + 54,
      y: y - 20,
      size: 12.5,
      fontBold,
    });
    const totalOrderUnits = order.items.reduce((sum, item) => sum + item.qty, 0);
    const orderSummary = `${order.items.length} поз.  /  ${totalOrderUnits} шт.`;
    page.drawText(orderSummary, {
      x: pageSize[0] - marginX - 14 - fontBold.widthOfTextAtSize(orderSummary, 9),
      y: y - 19,
      size: 9,
      font: fontBold,
      color: colors.green,
    });
    if (order.labelBarcode) {
      page.drawText("ШК ЭТИКЕТКИ", {
        x: marginX + 54,
        y: y - 37,
        size: 7.5,
        font: fontBold,
        color: colors.muted,
      });
      page.drawText(order.labelBarcode, {
        x: marginX + 124,
        y: y - 38,
        size: 11,
        font,
        color: colors.ink,
      });
    } else {
      page.drawText("СВЕРКА ПО НОМЕРУ ЗАКАЗА НА ЭТИКЕТКЕ", {
        x: marginX + 54,
        y: y - 37,
        size: 7.5,
        font: fontBold,
        color: colors.muted,
      });
    }
    y = headerBottom;

    for (let itemIndex = 0; itemIndex < order.items.length; itemIndex += 1) {
      const item = order.items[itemIndex];
      const itemHeight = itemHeights[itemIndex];
      const image = articleImages.get(item.article);
      const itemTop = y;
      page.drawLine({
        start: { x: marginX + 5, y: itemTop },
        end: { x: pageSize[0] - marginX, y: itemTop },
        thickness: 0.5,
        color: colors.line,
      });
      if (image) {
        const scale = Math.min(50 / image.width, 50 / image.height);
        const width = image.width * scale;
        const height = image.height * scale;
        page.drawImage(image, {
          x: marginX + 15 + (50 - width) / 2,
          y: itemTop - itemHeight + 9 + (50 - height) / 2,
          width,
          height,
        });
      } else {
        page.drawRectangle({
          x: marginX + 15,
          y: itemTop - itemHeight + 9,
          width: 50,
          height: 50,
          borderWidth: 0.5,
          borderColor: colors.line,
          color: rgb(0.94, 0.95, 0.96),
        });
      }
      page.drawRectangle({
        x: marginX + 75,
        y: itemTop - 42,
        width: 50,
        height: 27,
        color: colors.greenSoft,
        borderColor: rgb(0.66, 0.84, 0.73),
        borderWidth: 0.6,
      });
      const quantityText = `x${item.qty}`;
      page.drawText(quantityText, {
        x: marginX + 100 - fontBold.widthOfTextAtSize(quantityText, 14) / 2,
        y: itemTop - 35,
        size: 14,
        font: fontBold,
        color: colors.green,
      });
      const itemBarcode = item.sku ? formatOzonMarketplaceBarcodeForPdf(item.sku) : "-";
      page.drawText(item.article || "БЕЗ АРТИКУЛА", {
        x: marginX + 145,
        y: itemTop - 15,
        size: 9.5,
        font: fontBold,
        color: colors.orange,
      });
      const nameLines = wrapPdfText(item.name, font, 8.7, contentWidth - 205).slice(0, 3);
      let textY = itemTop - 28;
      for (const line of nameLines) {
        page.drawText(line, { x: marginX + 145, y: textY, size: 8.7, font, color: colors.ink });
        textY -= 9;
      }
      page.drawText(`ШК ТОВАРА  ${itemBarcode}`, {
        x: marginX + 145,
        y: itemTop - itemHeight + 11,
        size: 7.8,
        font: fontBold,
        color: colors.muted,
      });
      page.drawRectangle({
        x: pageSize[0] - marginX - 27,
        y: itemTop - 27,
        width: 13,
        height: 13,
        borderWidth: 1,
        borderColor: colors.muted,
      });
      y -= itemHeight;
    }
    y = cardBottom - 5;
  });

  const pages = pdfDoc.getPages();
  pages.forEach((p, index) => {
    const footerText = `Страница ${index + 1} / ${pageNo}`;
    p.drawText(truncatePdfTextToWidth(normalizeText(assembly?.name || assemblyId), font, 7.5, 350), {
      x: marginX,
      y: footerY,
      size: 7.5,
      font,
      color: colors.muted,
    });
    p.drawText(footerText, {
      x: pageSize[0] - marginX - font.widthOfTextAtSize(footerText, 8),
      y: footerY,
      size: 8,
      font,
      color: colors.muted,
    });
  });

  return await pdfDoc.save();
}

function getAssemblyShipmentState(assembly, readyToShipCount = 0) {
  const postingNumbers = normalizePostingNumbers(assembly?.shipment_posting_numbers);
  const assemblyId = normalizeText(assembly?.id);
  const storeId = normalizeStoreId(assembly?.store_id) || "";
  const shipmentLabelsQuery = storeId ? `?storeId=${encodeURIComponent(storeId)}` : "";
  const basePath = assemblyId ? `${API_ROOT}/assemblies/${encodeURIComponent(assemblyId)}/shipment` : "";
  return {
    code: normalizeText(assembly?.shipment_code) || null,
    labelUrl: assembly?.shipment_label_s3_key ? buildS3Url(assembly.shipment_label_s3_key) : null,
    labelS3Key: assembly?.shipment_label_s3_key || null,
    ozonLabelsUrl: basePath ? `${basePath}/labels.pdf${shipmentLabelsQuery}` : null,
    docs: basePath
      ? {
          actBarcodeUrl: `${basePath}/barcode.png${shipmentLabelsQuery}`,
        }
      : null,
    labelGeneratedAt: assembly?.shipment_label_generated_at || null,
    preparedAt: assembly?.shipment_prepared_at || null,
    confirmedAt: assembly?.shipment_confirmed_at || null,
    postingsCount: Number(assembly?.shipment_postings_count || postingNumbers.length || 0),
    postingNumbers,
    readyToShipCount: Math.max(0, Number(readyToShipCount || 0)),
    hasDraft: postingNumbers.length > 0,
    isConfirmed: Boolean(assembly?.shipment_confirmed_at),
  };
}

async function listCollectedNotSentPostingNumbers(assemblyId) {
  const result = await dbQuery(
    `select posting_number
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is not null
       and sent_to_delivering_at is null
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
  );
  return normalizePostingNumbers((result.rows || []).map((row) => row.posting_number));
}

async function listAllAssemblyPostingNumbers(assemblyId) {
  const result = await dbQuery(
    `select posting_number
     from ozon_assembly_orders
     where assembly_id = $1
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
  );
  return normalizePostingNumbers((result.rows || []).map((row) => row.posting_number));
}

async function getAssemblyShipmentExpectedPostingNumbers(assemblyId, assembly = null) {
  const draftPostingNumbers = normalizePostingNumbers(assembly?.shipment_posting_numbers);
  if (draftPostingNumbers.length) return draftPostingNumbers;
  const result = await dbQuery(
    `select posting_number
     from ozon_assembly_orders
     where assembly_id = $1
       and (collected_at is not null or sent_to_delivering_at is not null)
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
  );
  return normalizePostingNumbers((result.rows || []).map((row) => row.posting_number));
}

const assemblyOzonSyncState = new Map();
const assemblyBackgroundSyncSchedule = new Map();
const recentAssembliesSyncState = new Map();

async function syncAssemblyOrdersFromOzon(assemblyId, storeId, options = {}) {
  const normalizedAssemblyId = normalizeText(assemblyId);
  const store = resolveStoreStrict(storeId);
  if (!normalizedAssemblyId || !store) return { checked: 0, updated: 0, failed: 0 };

  const force = options?.force === true;
  const maxAgeMs = Math.max(0, Number(options?.maxAgeMs || 20000));
  const cacheKey = `${store.id}:${normalizedAssemblyId}`;
  const now = Date.now();
  const state = assemblyOzonSyncState.get(cacheKey) || { ts: 0, promise: null, last: null };

  if (!force && state.ts && now - state.ts < maxAgeMs) {
    if (state.promise) {
      try {
        await state.promise;
      } catch {}
    }
    return state.last || { checked: 0, updated: 0, failed: 0 };
  }

  if (state.promise) {
    try {
      await state.promise;
    } catch {}
    const refreshed = assemblyOzonSyncState.get(cacheKey);
    if (refreshed && !force && refreshed.ts && Date.now() - refreshed.ts < maxAgeMs) {
      return refreshed.last || { checked: 0, updated: 0, failed: 0 };
    }
  }

  const job = (async () => {
    const pendingResult = await dbQuery(
      `select posting_number, ozon_status, label_barcode, sent_to_delivering_at
       from ozon_assembly_orders
       where assembly_id = $1
       and (
           sent_to_delivering_at is null
           or coalesce(ozon_status, '') in ('awaiting_packaging', 'awaiting_deliver', 'delivering', 'delivered')
         )
       order by updated_at asc nulls last, posting_number asc
       limit 250`,
      [normalizedAssemblyId]
    );

    let checked = 0;
    let updated = 0;
    let failed = 0;
    for (const row of pendingResult.rows || []) {
      const postingNumber = normalizeText(row?.posting_number);
      if (!postingNumber) continue;
      checked += 1;
      try {
        const details = await fetchPostingDetails(postingNumber, store.id);
        const nextStatusRaw = normalizeText(details?.status || row?.ozon_status || "");
        if (!nextStatusRaw) continue;
        const existingStatus = normalizeText(row?.ozon_status || "");
        const nextStatus =
          existingStatus === "awaiting_deliver" && nextStatusRaw === "awaiting_packaging" && !row?.sent_to_delivering_at
            ? existingStatus
            : nextStatusRaw;
        const nextLabelBarcode = normalizeBarcode(details?.labelBarcode || "") || null;
        const existingLabelBarcode = normalizeBarcode(row?.label_barcode || "") || null;
        const becameInactive = isOzonPostingInactiveForAssembly(nextStatus) && !row?.sent_to_delivering_at;
        const hasDelta =
          nextStatus !== existingStatus ||
          (nextLabelBarcode && nextLabelBarcode !== existingLabelBarcode) ||
          becameInactive;
        if (!hasDelta) continue;

        await dbQuery(
          `update ozon_assembly_orders
           set ozon_status = $3,
               label_barcode = coalesce($4, label_barcode),
               assigned_user_id =
                 case
                   when $5 then null
                   else assigned_user_id
                 end,
               assigned_at =
                 case
                   when $5 then null
                   else assigned_at
                 end,
               sent_to_delivering_at =
                 case
                    when sent_to_delivering_at is null and $5 then now()
                   else sent_to_delivering_at
                 end,
               sent_error =
                 case
                   when $5 then null
                   else sent_error
                 end,
                updated_at = now()
           where assembly_id = $1 and posting_number = $2`,
          [normalizedAssemblyId, postingNumber, nextStatus, nextLabelBarcode, becameInactive]
        );
        updated += 1;
      } catch (err) {
        const message = String(err?.message || "");
        const responseText = String(err?.responseText || "");
        const isPostingGone =
          Number(err?.status || 0) === 404 ||
          /POSTING_NOT_FOUND/i.test(message) ||
          /POSTING_NOT_FOUND/i.test(responseText);
        if (isPostingGone) {
          await dbQuery(
            `update ozon_assembly_orders
             set ozon_status = 'posting_not_found',
                 assigned_user_id = null,
                 assigned_at = null,
                 sent_to_delivering_at = coalesce(sent_to_delivering_at, now()),
                 sent_error = null,
                 updated_at = now()
             where assembly_id = $1 and posting_number = $2`,
            [normalizedAssemblyId, postingNumber]
          );
          updated += 1;
          continue;
        }
        failed += 1;
      }
    }
    if (updated > 0) {
      await reconcileAssemblyAccessAfterExternalSync(normalizedAssemblyId);
    }
    return { checked, updated, failed };
  })();

  assemblyOzonSyncState.set(cacheKey, { ...state, promise: job });
  try {
    const result = await job;
    assemblyOzonSyncState.set(cacheKey, { ts: Date.now(), promise: null, last: result });
    return result;
  } catch (err) {
    const fallback = state.last || { checked: 0, updated: 0, failed: 0 };
    assemblyOzonSyncState.set(cacheKey, { ts: Date.now(), promise: null, last: fallback });
    throw err;
  }
}

async function finishEmployeeProductPickingAfterAssemblySync(session) {
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
  if (remainingQty > 0) return refreshed;

  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
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
  const finished = result.rows?.[0] || refreshed;
  if (Number(finished.assignment_part_id || 0) > 0 && normalizeSessionRole(finished.session_role) === SESSION_ROLE_PICK) {
    await dbQuery(
      `update ozon_assembly_assignment_parts
       set picker_finished_at = coalesce(picker_finished_at, now()),
           handoff_status = case
             when handoff_status = $2 then handoff_status
             else $3
           end,
           updated_at = now()
       where id = $1`,
      [Number(finished.assignment_part_id), HANDOFF_STATUS_NOT_REQUIRED, HANDOFF_STATUS_WAITING_PICK]
    );
  }
  return finished;
}

async function closeEmptyEmployeeAssemblySessionAfterSync(session) {
  if (!session?.id || session.finished_at || session.started_at) return session;
  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
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

async function syncOpenEmployeeAssemblySessions(assemblyId) {
  const assembly = await getAssembly(assemblyId);
  const result = await dbQuery(
    `select *
     from ozon_employee_assembly_sessions
     where assembly_id = $1 and finished_at is null
     order by id asc`,
    [assemblyId]
  );
  const openSessions = result.rows || [];
  let reconciledSessions = 0;
  let closedEmptySessions = 0;
  let autoFinishedPickingSessions = 0;
  let autoFinishedPackingSessions = 0;

  for (const row of openSessions) {
    const assignmentPartId = Number(row.assignment_part_id || 0) || null;
    const counts = await getEmployeeAssemblyCounts(
      assemblyId,
      row.user_id,
      assembly?.access_mode || "selected_split",
      assignmentPartId
    );
    const refreshedResult = await dbQuery(
      `update ozon_employee_assembly_sessions
       set assigned_order_count = $2,
           collected_order_count = $3,
           updated_at = now()
       where id = $1
       returning *`,
      [row.id, counts.total, counts.collected]
    );
    let session = refreshedResult.rows?.[0] || row;

    if (
      normalizeSessionRole(session.session_role) !== SESSION_ROLE_PACK &&
      normalizePickingStatus(session.picking_status) !== PICKING_STATUS_NOT_REQUIRED
    ) {
      await syncEmployeeProductPickItemsAfterAssemblySync(session);
      const beforePickingStatus = normalizePickingStatus(session.picking_status);
      session = await finishEmployeeProductPickingAfterAssemblySync(session);
      if (
        beforePickingStatus === PICKING_STATUS_STARTED &&
        normalizePickingStatus(session?.picking_status) === PICKING_STATUS_FINISHED
      ) {
        autoFinishedPickingSessions += 1;
      }
    }

    const finishedPacking = await finalizeEmployeeAssemblySessionIfCompleted(assemblyId, session.user_id);
    if (finishedPacking) autoFinishedPackingSessions += 1;

    const openAfterFinalize = await getEmployeeOpenAssemblySession(assemblyId, session.user_id);
    if (!openAfterFinalize) {
      reconciledSessions += 1;
      continue;
    }
    const nextCounts = await getEmployeeAssemblyCounts(
      assemblyId,
      openAfterFinalize.user_id,
      assembly?.access_mode || "selected_split",
      Number(openAfterFinalize.assignment_part_id || 0) || null
    );
    if (nextCounts.total < 1) {
      const closedSession = await closeEmptyEmployeeAssemblySessionAfterSync(openAfterFinalize);
      if (closedSession?.finished_at) closedEmptySessions += 1;
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

async function syncAssemblyWithAwaitingDeliverFromOzon(assemblyId, storeId) {
  const normalizedAssemblyId = normalizeText(assemblyId);
  const store = resolveStoreStrict(storeId);
  if (!normalizedAssemblyId || !store) {
    throw new Error("Сборка или магазин Ozon не настроены");
  }

  const localResult = await dbQuery(
    `select posting_number, ozon_status, label_barcode, assigned_user_id
     from ozon_assembly_orders
     where assembly_id = $1
       and sent_to_delivering_at is null
     order by posting_number asc`,
    [normalizedAssemblyId]
  );
  const localRows = localResult.rows || [];
  const postings = await fetchAllFbsPostingsByStatus("awaiting_deliver", {
    days: OZON_LIST_DAYS,
    sortDir: "DESC",
    includeBarcodes: false,
    storeId: store.id,
  });
  const awaitingByPosting = new Map(
    (postings || [])
      .map((posting) => [normalizeText(posting?.postingNumber), posting])
      .filter(([postingNumber]) => postingNumber)
  );

  const missingRows = localRows.filter((row) => !awaitingByPosting.has(normalizeText(row.posting_number)));
  const inactiveRows = [];
  let rechecked = 0;
  let recheckedKept = 0;
  let failed = 0;

  await mapWithConcurrency(missingRows, 2, async (row) => {
    const postingNumber = normalizeText(row.posting_number);
    if (!postingNumber) return;
    rechecked += 1;
    try {
      const details = await fetchPostingDetails(postingNumber, store.id);
      const fetchedStatus = normalizeText(details?.status || "");
      const existingStatus = normalizeText(row?.ozon_status || "");
      const shouldKeep =
        fetchedStatus === "awaiting_deliver" ||
        (existingStatus === "awaiting_deliver" && fetchedStatus === "awaiting_packaging");
      if (shouldKeep) {
        recheckedKept += 1;
        await dbQuery(
          `update ozon_assembly_orders
           set ozon_status = 'awaiting_deliver',
               label_barcode = coalesce($3, label_barcode),
               updated_at = now()
           where assembly_id = $1 and posting_number = $2`,
          [normalizedAssemblyId, postingNumber, normalizeBarcode(details?.labelBarcode || "") || null]
        );
        return;
      }
      if (!fetchedStatus) {
        failed += 1;
        return;
      }
      inactiveRows.push({ ...row, postingNumber, status: fetchedStatus });
    } catch (error) {
      const message = String(error?.message || "");
      const responseText = String(error?.responseText || "");
      const postingGone =
        Number(error?.status || 0) === 404 ||
        /POSTING_NOT_FOUND/i.test(message) ||
        /POSTING_NOT_FOUND/i.test(responseText);
      if (postingGone) {
        inactiveRows.push({ ...row, postingNumber, status: "posting_not_found" });
        return;
      }
      failed += 1;
    }
  });

  for (const row of inactiveRows) {
    await dbQuery(
      `update ozon_assembly_orders
       set ozon_status = $3,
           assigned_user_id = null,
           assigned_at = null,
           sent_to_delivering_at = coalesce(sent_to_delivering_at, now()),
           sent_error = null,
           label_error = null,
           updated_at = now()
       where assembly_id = $1 and posting_number = $2`,
      [normalizedAssemblyId, row.postingNumber, row.status]
    );
  }

  const removedPostingNumbers = inactiveRows.map((row) => row.postingNumber);
  if (removedPostingNumbers.length) {
    await dbQuery(
      `delete from ozon_employee_scan_issues
       where assembly_id = $1
         and posting_number = any($2::text[])
         and resolved_at is null`,
      [normalizedAssemblyId, removedPostingNumbers]
    );
  }

  await dbQuery(
    `update ozon_assemblies
     set labels_total = coalesce((
           select count(*)::int
           from ozon_assembly_orders
           where assembly_id = $1 and sent_to_delivering_at is null
         ), 0),
         labels_loaded = coalesce((
           select count(*)::int
           from ozon_assembly_orders
           where assembly_id = $1
             and sent_to_delivering_at is null
             and coalesce(nullif(btrim(label_s3_key), ''), '') <> ''
         ), 0),
         updated_at = now()
     where id = $1`,
    [normalizedAssemblyId]
  );

  const sessionSummary = await syncOpenEmployeeAssemblySessions(normalizedAssemblyId);
  clearEmployeePickWarmCaches();
  assemblyOzonSyncState.delete(`${store.id}:${normalizedAssemblyId}`);
  notifyAssemblyUpdate(normalizedAssemblyId, true);

  const totalAfterResult = await dbQuery(
    `select count(*)::int as total
     from ozon_assembly_orders
     where assembly_id = $1 and sent_to_delivering_at is null`,
    [normalizedAssemblyId]
  );
  const removedAssignedOrdersCount = inactiveRows.filter(
    (row) => Number.isFinite(Number(row.assigned_user_id)) && Number(row.assigned_user_id) > 0
  ).length;
  return {
    assemblyId: normalizedAssemblyId,
    checkedOrders: localRows.length,
    totalAwaitingDeliver: awaitingByPosting.size,
    rechecked,
    recheckedKept,
    removedOrdersCount: inactiveRows.length,
    removedAssignedOrdersCount,
    failedChecks: failed,
    totalOrders: Number(totalAfterResult.rows?.[0]?.total || 0),
    removedPostingNumbers,
    ...sessionSummary,
  };
}

function scheduleAssemblyOrdersSyncBackground(assemblyId, storeId, options = {}) {
  const normalizedAssemblyId = normalizeText(assemblyId);
  const store = resolveStoreStrict(storeId);
  if (!normalizedAssemblyId || !store) return;
  const maxAgeMs = Math.max(0, Number(options?.maxAgeMs || 12000));
  const minScheduleGapMs = Math.max(500, Number(options?.minScheduleGapMs || 2500));
  const key = `${store.id}:${normalizedAssemblyId}`;
  const now = Date.now();
  const last = Number(assemblyBackgroundSyncSchedule.get(key) || 0);
  if (now - last < minScheduleGapMs) return;
  assemblyBackgroundSyncSchedule.set(key, now);
  syncAssemblyOrdersFromOzon(normalizedAssemblyId, store.id, { maxAgeMs })
    .then((result) => {
      if (Number(result?.updated || 0) > 0) {
        notifyAssemblyUpdate(normalizedAssemblyId, true);
      }
    })
    .catch(() => {});
}

async function syncRecentAssembliesFromOzon(storeId) {
  const store = resolveStoreStrict(storeId);
  if (!store) return;
  const result = await dbQuery(
    `select id
     from ozon_assemblies
     where store_id = $1
       and timezone('Europe/Moscow', created_at)::date >= timezone('Europe/Moscow', now())::date - 1
     order by created_at desc
     limit 8`,
    [store.id]
  );
  for (const row of result.rows || []) {
    try {
      await syncAssemblyOrdersFromOzon(row.id, store.id, { maxAgeMs: 30000 });
    } catch {}
  }
}

function scheduleRecentAssembliesSync(storeId, options = {}) {
  const store = resolveStoreStrict(storeId);
  if (!store) return;
  const minAgeMs = Math.max(5000, Number(options?.minAgeMs || 45000));
  const state = recentAssembliesSyncState.get(store.id) || { ts: 0, promise: null };
  if (state.promise) return;
  if (state.ts && Date.now() - state.ts < minAgeMs) return;

  const job = syncRecentAssembliesFromOzon(store.id)
    .catch(() => {})
    .finally(() => {
      recentAssembliesSyncState.set(store.id, { ts: Date.now(), promise: null });
    });

  recentAssembliesSyncState.set(store.id, { ts: state.ts || 0, promise: job });
}

async function sendAssemblyPostingsToDelivering(assemblyId, storeId, postingNumbers) {
  const selectedPostingNumbers = normalizePostingNumbers(postingNumbers);
  if (!selectedPostingNumbers.length) {
    return { sent: [], failed: [] };
  }
  const sent = [];
  const failed = [];
  const chunkSize = 50;
  for (let i = 0; i < selectedPostingNumbers.length; i += chunkSize) {
    const chunk = selectedPostingNumbers.slice(i, i + chunkSize);
    try {
      await movePostingsToDelivering(chunk, storeId || null);
      for (const postingNumber of chunk) sent.push(postingNumber);
    } catch {
      for (const postingNumber of chunk) {
        try {
          await movePostingsToDelivering([postingNumber], storeId || null);
          sent.push(postingNumber);
        } catch (itemErr) {
          failed.push({ postingNumber, error: itemErr?.message || "Не удалось сменить статус" });
        }
      }
    }
  }
  if (sent.length) {
    await dbQuery(
      `update ozon_assembly_orders
       set sent_to_delivering_at = now(),
           ozon_status = 'delivering',
           sent_error = null,
           updated_at = now()
       where assembly_id = $1 and posting_number = any($2::text[])`,
      [assemblyId, sent]
    );
  }
  for (const item of failed) {
    await dbQuery(
      `update ozon_assembly_orders
       set sent_error = $3, updated_at = now()
       where assembly_id = $1 and posting_number = $2`,
      [assemblyId, item.postingNumber, item.error]
    );
  }
  return { sent, failed };
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
  const externalEmployeeId = Number(result.rows?.[0]?.external_employee_id || 0);
  return externalEmployeeId > 0 ? externalEmployeeId : null;
}

function toAssemblyAssignmentPartPayload(row) {
  if (!row) return null;
  const roleMode = normalizeAssignmentPartMode(row.role_mode);
  const pickerUserId = Number(row.picker_user_id || 0) || null;
  const packerUserId = Number(row.packer_user_id || 0) || null;
  const isSplit = roleMode === ASSIGNMENT_PART_MODE_SPLIT && pickerUserId && packerUserId && pickerUserId !== packerUserId;
  return {
    id: Number(row.id),
    assemblyId: row.assembly_id,
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

async function listAssemblyAssignmentParts(assemblyId) {
  const result = await dbQuery(
    `select p.*,
            picker.surname as picker_surname,
            picker.name as picker_name,
            packer.surname as packer_surname,
            packer.name as packer_name
     from ozon_assembly_assignment_parts p
     left join users picker on picker.id = p.picker_user_id
     left join users packer on packer.id = p.packer_user_id
     where p.assembly_id = $1
     order by p.part_no asc, p.id asc`,
    [assemblyId]
  );
  return (result.rows || []).map(toAssemblyAssignmentPartPayload);
}

async function assemblyHasAssignmentParts(assemblyId) {
  const result = await dbQuery(
    "select 1 from ozon_assembly_assignment_parts where assembly_id = $1 limit 1",
    [assemblyId]
  );
  return Boolean(result.rows?.[0]);
}

async function getAssemblyAssignmentPartById(partId) {
  const normalizedPartId = Number(partId || 0);
  if (!Number.isInteger(normalizedPartId) || normalizedPartId <= 0) return null;
  const result = await dbQuery(
    `select p.*,
            picker.surname as picker_surname,
            picker.name as picker_name,
            packer.surname as packer_surname,
            packer.name as packer_name
     from ozon_assembly_assignment_parts p
     left join users picker on picker.id = p.picker_user_id
     left join users packer on packer.id = p.packer_user_id
     where p.id = $1
     limit 1`,
    [normalizedPartId]
  );
  return toAssemblyAssignmentPartPayload(result.rows?.[0] || null);
}

function normalizeAssemblyAssignmentPartsInput(parts, selectedUserIds = []) {
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

async function clearAssemblyAssignmentParts(assemblyId) {
  await dbQuery("update ozon_assembly_orders set assignment_part_id = null where assembly_id = $1", [assemblyId]);
  await dbQuery("delete from ozon_assembly_assignment_parts where assembly_id = $1", [assemblyId]);
}

async function createAssemblyAssignmentParts(assemblyId, parts, adminId = null) {
  const normalizedParts = Array.isArray(parts) ? parts : [];
  if (!normalizedParts.length) return [];
  const values = [];
  const placeholders = normalizedParts.map((part, index) => {
    const base = index * 9;
    values.push(
      assemblyId,
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
    `insert into ozon_assembly_assignment_parts
      (assembly_id, part_no, role_mode, picker_user_id, packer_user_id, table_no, handoff_status, created_by, updated_by, created_at, updated_at)
     values ${placeholders.join(", ")}
     returning *`,
    values
  );
  return (result.rows || []).map((row) => ({
    id: Number(row.id),
    assemblyId: row.assembly_id,
    partNo: Number(row.part_no || 0),
    roleMode: normalizeAssignmentPartMode(row.role_mode),
    pickerUserId: row.picker_user_id != null ? Number(row.picker_user_id) : null,
    packerUserId: Number(row.packer_user_id),
    tableNo: normalizeAssignmentTableNo(row.table_no),
    handoffStatus: normalizeHandoffStatus(row.handoff_status),
  }));
}

async function assignAssemblyOrdersToParts(assemblyId, parts, mode = "split") {
  const normalizedParts = Array.isArray(parts) ? parts : [];
  if (!normalizedParts.length) {
    return { total: 0, assigned: 0, parts: [] };
  }
  if (mode === "redistribute") {
    await dbQuery(
      `update ozon_assembly_orders
       set assigned_user_id = null,
           assignment_part_id = null,
           assigned_at = null,
           updated_at = now()
       where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
      [assemblyId]
    );
  }
  const result = await dbQuery(
    `select posting_number, order_created_at
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is null
       and sent_to_delivering_at is null
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
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
    const slice = orders.slice(offset, offset + count).map((row) => String(row.posting_number || ""));
    offset += count;
    if (slice.length) {
      await dbQuery(
        `update ozon_assembly_orders
         set assigned_user_id = $3,
             assignment_part_id = $4,
             assigned_at = now(),
             updated_at = now()
         where assembly_id = $1 and posting_number = any($2::text[])`,
        [assemblyId, slice, getAssignmentPartEffectivePickerUserId(part), Number(part.id)]
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

async function createAssemblyAssignmentSessionsFromParts(assemblyId, adminId, mode, parts, workflowStage) {
  const normalizedParts = Array.isArray(parts) ? parts : [];
  const values = [];
  const pickingRequired = isEmployeeProductPickingEnabled(workflowStage);
  const pushSession = (session) => {
    const base = values.length;
    values.push(
      assemblyId,
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
    `insert into ozon_employee_assembly_sessions
      (assembly_id, user_id, assigned_by, assignment_mode, assigned_at, assigned_order_count, picking_status, assignment_part_id, session_role, table_no, created_at, updated_at)
     values ${placeholders.join(", ")}
     returning *`,
    values
  );
  return result.rows || [];
}

function uniqueTextList(values) {
  const out = [];
  for (const value of Array.isArray(values) ? values : []) {
    const normalized = normalizeBarcode(value);
    if (normalized && !out.includes(normalized)) out.push(normalized);
  }
  return out;
}

function uniqueNumberList(values) {
  const out = [];
  for (const value of Array.isArray(values) ? values : []) {
    const normalized = Number(value);
    if (Number.isFinite(normalized) && !out.includes(normalized)) out.push(normalized);
  }
  return out;
}

function getCachedScanLookup(barcode) {
  const key = normalizeBarcode(barcode);
  if (!key) return null;
  const cached = scanLookupCache.get(key);
  if (!cached) return null;
  if (Date.now() - Number(cached.ts || 0) >= SCAN_LOOKUP_CACHE_MS) {
    scanLookupCache.delete(key);
    return null;
  }
  return cached;
}

function setCachedScanLookup(barcode, value) {
  const key = normalizeBarcode(barcode);
  if (!key) return;
  if (scanLookupCache.has(key)) {
    scanLookupCache.delete(key);
  }
  while (scanLookupCache.size >= SCAN_LOOKUP_CACHE_MAX) {
    const oldestKey = scanLookupCache.keys().next().value;
    if (!oldestKey) break;
    scanLookupCache.delete(oldestKey);
  }
  scanLookupCache.set(key, {
    ts: Date.now(),
    barcodes: uniqueTextList(value?.barcodes || []),
    found: Boolean(value?.found),
    multiple: Boolean(value?.multiple),
    matches: Array.isArray(value?.matches) ? value.matches : [],
    lookupFailed: Boolean(value?.lookupFailed),
  });
}

function limitWarningLabels(values = [], limit = 6) {
  const filtered = Array.from(new Set((Array.isArray(values) ? values : []).filter(Boolean)));
  if (filtered.length <= limit) return filtered;
  return [...filtered.slice(0, limit), `+ещё ${filtered.length - limit}`];
}

function formatWarehouseScanProductLabel(item) {
  const article = normalizeArticleKey(item?.article || item?.code || null);
  const name = String(item?.name || "").trim();
  if (article && name) return `${article}`;
  return article || name || "";
}

function formatAssemblyMatchLabel(item) {
  const article = normalizeArticleKey(item?.article);
  const name = String(item?.name || "").trim();
  const postingNumber = normalizeText(item?.postingNumber);
  const ozonBarcodes = limitWarningLabels(
    uniqueTextList([
      item?.barcode,
      ...(Array.isArray(item?.sourceBarcodes) ? item.sourceBarcodes : []),
      ...(Array.isArray(item?.acceptedBarcodes) ? item.acceptedBarcodes : []),
    ]).filter((value) => /^OZN/i.test(String(value || "").trim())),
    3
  );
  if (article && ozonBarcodes.length && postingNumber) {
    return `${article} (${ozonBarcodes.join(", ")}; ${postingNumber})`;
  }
  if (article && ozonBarcodes.length) {
    return `${article} (${ozonBarcodes.join(", ")})`;
  }
  if (article && postingNumber) return `${article} (${postingNumber})`;
  return article || postingNumber || name || "";
}

function getAssemblyMatchIdentityKey(item) {
  const article = normalizeArticleKey(item?.article);
  if (article) return `article:${article}`;
  const barcodes = uniqueTextList([...(item?.sourceBarcodes || []), ...(item?.acceptedBarcodes || [])]);
  if (barcodes.length) return `barcodes:${barcodes.join("|")}`;
  const name = String(item?.name || "").trim().toLowerCase();
  if (name) return `name:${name}`;
  const postingNumber = normalizeText(item?.postingNumber);
  const lineNo = Number(item?.lineNo);
  return `${postingNumber || "unknown"}|${Number.isFinite(lineNo) ? lineNo : 0}`;
}

function buildMultipleScanProductsReason(inputBarcode, scanProducts = []) {
  const scanned = normalizeBarcode(inputBarcode);
  const products = limitWarningLabels(
    (Array.isArray(scanProducts) ? scanProducts : []).map((item) => formatWarehouseScanProductLabel(item))
  );
  const suffix = products.length ? `: ${products.join(", ")}` : "";
  return `ШК ${scanned || "—"} есть у нескольких товаров в складской БД${suffix}. Система не может однозначно зачесть этот скан. Нажмите «Ошибка» и отправьте админу.`;
}

function buildAmbiguousAssemblyScanWarning(inputBarcode, matches, options = {}) {
  const scanned = normalizeBarcode(inputBarcode);
  const items = limitWarningLabels(
    (Array.isArray(matches) ? matches : []).map((item) => formatAssemblyMatchLabel(item))
  );
  const parts = [];
  if (options?.multipleScanProducts) {
    parts.push(buildMultipleScanProductsReason(inputBarcode, options?.scanProducts));
  }
  if (items.length) {
    parts.push(`В текущей сборке по этому скану подходят несколько товаров: ${items.join(", ")}.`);
  }
  if (parts.length) return parts.join(" ");
  return `ШК ${scanned || "—"} подходит к нескольким товарам в текущей сборке. Нажмите «Ошибка» и отправьте админу.`;
}

function buildAmbiguousAssemblyPickWarning(inputBarcode, matches, options = {}) {
  return buildAmbiguousAssemblyScanWarning(inputBarcode, matches, options);
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
    const result = {
      barcodes: uniqueTextList([normalizedInput, ...(status?.barcodes || [])]),
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
    const fallback = {
      barcodes: [normalizedInput],
      found: false,
      multiple: false,
      matches: [],
      lookupFailed: true,
    };
    setCachedScanLookup(normalizedInput, fallback);
    return fallback;
  }
}

const cardsCacheSnapshots = new Map();
const liveOfferBarcodeCache = new Map();
const scanBarcodeAliasCache = new Map();

function loadCardsCacheMap(storeId = null) {
  const key = normalizeStoreId(storeId) || DEFAULT_STORE?.id || "default";
  const filePath = getCardsCacheFile(key);
  const snapshot = cardsCacheSnapshots.get(key) || { mtimeMs: 0, map: new Map() };
  try {
    const stat = fs.statSync(filePath);
    if (snapshot.mtimeMs === stat.mtimeMs && snapshot.map.size > 0) {
      return snapshot.map;
    }
    const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const map = new Map();
    for (const item of Array.isArray(raw?.items) ? raw.items : []) {
      const offerId = normalizeArticleKey(item?.vendorCode || item?.offerId || item?.article || null);
      if (!offerId) continue;
      const barcodes = uniqueTextList(item?.barcodes);
      if (!barcodes.length) continue;
      map.set(offerId, barcodes);
    }
    cardsCacheSnapshots.set(key, { mtimeMs: stat.mtimeMs, map });
    return map;
  } catch {
    return snapshot.map;
  }
}

function getOfferBarcodes(offerId, storeId = null) {
  const key = normalizeArticleKey(offerId);
  if (!key) return [];
  return loadCardsCacheMap(storeId).get(key) || [];
}

function getLiveOfferBarcodeCacheKey(offerId, storeId = null) {
  return `${normalizeStoreId(storeId) || "default"}::${normalizeArticleKey(offerId) || ""}`;
}

function getScanBarcodeAliasStoreKey(storeId = null) {
  return normalizeStoreId(storeId) || "";
}

function getScanBarcodeAliasCacheKey(kind, storeId = null, values = []) {
  return `${normalizeText(kind) || "default"}::${getScanBarcodeAliasStoreKey(storeId)}::${uniqueTextList(values)
    .sort((a, b) => a.localeCompare(b))
    .join("|")}`;
}

function getCachedScanBarcodeAliases(cacheKey) {
  const cached = scanBarcodeAliasCache.get(cacheKey);
  if (!cached) return null;
  if (Date.now() - Number(cached.ts || 0) >= SCAN_BARCODE_ALIAS_CACHE_MS) {
    scanBarcodeAliasCache.delete(cacheKey);
    return null;
  }
  return Array.isArray(cached.values) ? cached.values : [];
}

function setCachedScanBarcodeAliases(cacheKey, values) {
  scanBarcodeAliasCache.set(cacheKey, {
    ts: Date.now(),
    values: uniqueTextList(values),
  });
}

async function listSavedScanBarcodeAliasesByMarketplaceBarcodes(marketplaceBarcodes = [], storeId = null) {
  const normalizedBarcodes = uniqueTextList(
    marketplaceBarcodes.filter((value) => /^OZN/i.test(String(value || "").trim()))
  );
  if (!normalizedBarcodes.length) return [];
  const normalizedStoreId = getScanBarcodeAliasStoreKey(storeId);
  const cacheKey = getScanBarcodeAliasCacheKey("marketplace", normalizedStoreId, normalizedBarcodes);
  const cached = getCachedScanBarcodeAliases(cacheKey);
  if (cached) return cached;
  const params = [normalizedBarcodes];
  let where = "where marketplace_barcode = any($1::text[])";
  if (normalizedStoreId) {
    params.push(normalizedStoreId);
    where += " and store_id in ('', $2)";
  }
  const result = await dbQuery(
    `select scanned_barcode
     from ozon_scan_barcode_aliases
     ${where}
     order by updated_at desc, id desc`,
    params
  );
  const values = uniqueTextList((result.rows || []).map((row) => row.scanned_barcode));
  setCachedScanBarcodeAliases(cacheKey, values);
  return values;
}

async function saveOzonScanBarcodeAliases({ marketplaceBarcodes = [], scannedBarcode, storeId = null, adminId = null }) {
  const normalizedScannedBarcode = normalizeBarcode(scannedBarcode);
  const normalizedMarketplaceBarcodes = uniqueTextList(
    marketplaceBarcodes.filter((value) => /^OZN/i.test(String(value || "").trim()))
  );
  if (!normalizedScannedBarcode || !normalizedMarketplaceBarcodes.length) {
    return 0;
  }
  const normalizedStoreId = getScanBarcodeAliasStoreKey(storeId);
  let saved = 0;
  for (const marketplaceBarcode of normalizedMarketplaceBarcodes) {
    await dbQuery(
      `insert into ozon_scan_barcode_aliases
        (store_id, marketplace_barcode, scanned_barcode, created_by, updated_by, created_at, updated_at)
       values ($1, $2, $3, $4, $4, now(), now())
       on conflict (store_id, marketplace_barcode, scanned_barcode)
       do update
         set updated_by = excluded.updated_by,
             updated_at = now()`,
      [normalizedStoreId, marketplaceBarcode, normalizedScannedBarcode, adminId == null ? null : Number(adminId)]
    );
    saved += 1;
  }
  scanBarcodeAliasCache.clear();
  return saved;
}

const barcodeArticleResolutionCache = new Map();

async function resolveKnownBarcodeArticle(barcode, options = {}) {
  const normalizedBarcode = normalizeBarcode(barcode);
  if (!normalizedBarcode) {
    return { article: null, multiple: false };
  }
  const cacheKey = `${normalizedBarcode}|${options?.force === true ? "1" : "0"}`;
  const cached = barcodeArticleResolutionCache.get(cacheKey);
  if (cached) return cached;
  let result = { article: null, multiple: false };
  try {
    const status = await getMsProductStatusByBarcode(normalizedBarcode, options?.force === true ? { force: true } : {});
    if (status?.status === "multiple" || status?.multiple) {
      result = { article: null, multiple: true };
    } else if (status?.status === "ok") {
      const articles = uniqueTextList(
        (Array.isArray(status?.matches) ? status.matches : [])
          .map((entry) => normalizeArticleKey(entry?.article))
          .filter(Boolean)
      );
      result = {
        article: articles.length === 1 ? articles[0] : null,
        multiple: articles.length > 1,
      };
    }
  } catch {}
  barcodeArticleResolutionCache.set(cacheKey, result);
  return result;
}

async function isCompatibleScannedBarcodeForOffer(offerId, scannedBarcode) {
  const targetArticle = normalizeArticleKey(offerId);
  const normalizedScannedBarcode = normalizeBarcode(scannedBarcode);
  if (!targetArticle || !normalizedScannedBarcode) return true;
  const resolved = await resolveKnownBarcodeArticle(normalizedScannedBarcode);
  if (resolved.multiple) return false;
  if (!resolved.article) return true;
  return resolved.article === targetArticle;
}

async function getAliasMismatchReasonForArticles(itemArticles = [], scannedBarcode) {
  const normalizedScannedBarcode = normalizeBarcode(scannedBarcode);
  if (!normalizedScannedBarcode) return "";
  const resolved = await resolveKnownBarcodeArticle(normalizedScannedBarcode);
  if (resolved.multiple) {
    return `ШК ${normalizedScannedBarcode} в складской БД привязан к нескольким товарам, его нельзя сохранить как alias.`;
  }
  if (!resolved.article) return "";
  for (const article of uniqueTextList(itemArticles.map((value) => normalizeArticleKey(value)).filter(Boolean))) {
    if (resolved.article !== article) {
      return `ШК ${normalizedScannedBarcode} в складской БД относится к товару ${resolved.article}, а не к ${article}. Такой alias сохранять нельзя.`;
    }
  }
  return "";
}

async function fetchOfferBarcodesFromOzon(offerId, storeId = null) {
  const key = normalizeArticleKey(offerId);
  if (!key) return [];
  const cacheKey = getLiveOfferBarcodeCacheKey(key, storeId);
  const cached = liveOfferBarcodeCache.get(cacheKey);
  if (cached && Date.now() - Number(cached.ts || 0) < 10 * 60 * 1000) {
    return Array.isArray(cached.barcodes) ? cached.barcodes : [];
  }
  try {
    const data = await ozonPost(
      "/v3/product/info/list",
      { offer_id: [key] },
      { storeId, timeoutMs: OZON_API_FAST_TIMEOUT_MS }
    );
    const items = Array.isArray(data?.items) ? data.items : Array.isArray(data?.result?.items) ? data.result.items : [];
    const product = items.find((item) => normalizeArticleKey(item?.offer_id || item?.offerId || item?.article || null) === key) || items[0] || null;
    const barcodes = uniqueTextList(product?.barcodes || []);
    liveOfferBarcodeCache.set(cacheKey, { ts: Date.now(), barcodes });
    return barcodes;
  } catch {
    liveOfferBarcodeCache.set(cacheKey, { ts: Date.now(), barcodes: [] });
    return [];
  }
}

async function getOfferBarcodesWithFallback(offerId, storeIds = [], options = {}) {
  const key = normalizeArticleKey(offerId);
  if (!key) return [];
  const primaryStoreIds = Array.from(new Set((Array.isArray(storeIds) ? storeIds : [storeIds]).map((value) => normalizeStoreId(value)).filter(Boolean)));
  const allStoreIds = Array.from(new Set((Array.isArray(storeList) ? storeList : []).map((store) => normalizeStoreId(store?.id)).filter(Boolean)));
  const candidateStoreIds = options?.strictStoreScope === true && primaryStoreIds.length
    ? primaryStoreIds
    : Array.from(new Set([...primaryStoreIds, ...allStoreIds]));
  const preferMarketplaceOnly = options?.preferMarketplaceOnly === true;
  const sku = Number.isFinite(Number(options?.sku)) ? Number(options.sku) : null;

  const cachedBarcodes = uniqueTextList(candidateStoreIds.flatMap((storeId) => getOfferBarcodes(key, storeId)));
  const cachedMarketplaceBarcodes = uniqueTextList(
    cachedBarcodes.filter((value) => /^OZN/i.test(String(value || "").trim()))
  );

  const liveBarcodes = [];
  const shouldFetchLive =
    options?.forceLive === true ||
    cachedBarcodes.length === 0 ||
    cachedMarketplaceBarcodes.length === 0;
  if (shouldFetchLive) {
    for (const currentStoreId of candidateStoreIds) {
      const barcodes = await fetchOfferBarcodesFromOzon(key, currentStoreId);
      if (barcodes.length) {
        liveBarcodes.push(...barcodes);
      }
    }
  }
  const liveMarketplaceBarcodes = uniqueTextList(
    liveBarcodes.filter((value) => /^OZN/i.test(String(value || "").trim()))
  );
  const skuMarketplaceBarcodes = buildSkuBarcodeCandidates(sku).filter((value) =>
    /^OZN/i.test(String(value || "").trim())
  );
  const marketplaceBarcodes = uniqueTextList([
    ...liveMarketplaceBarcodes,
    ...cachedMarketplaceBarcodes,
    ...skuMarketplaceBarcodes,
  ]);
  if (preferMarketplaceOnly && marketplaceBarcodes.length) {
    return marketplaceBarcodes;
  }
  return uniqueTextList([
    ...marketplaceBarcodes,
    ...liveBarcodes,
    ...cachedBarcodes,
    ...buildSkuBarcodeCandidates(sku),
  ]);
}

function buildSkuBarcodeCandidates(sku) {
  const normalized = normalizeBarcode(sku);
  if (!normalized) return [];
  const out = /^\d+$/.test(normalized) ? [`OZN${normalized}`, normalized] : [normalized];
  return uniqueTextList(out);
}

function buildOzonMarketplaceBarcodeCandidates(sku) {
  return buildSkuBarcodeCandidates(sku).filter((value) => /^OZN/i.test(String(value || "").trim()));
}

function pickPreferredOzonMarketplaceBarcode(barcodes = [], sku = null) {
  const normalizedBarcodes = uniqueTextList(barcodes);
  const explicitOzonBarcode =
    normalizedBarcodes.find((value) => /^OZN/i.test(String(value || "").trim())) || null;
  if (explicitOzonBarcode) return explicitOzonBarcode;
  const skuCandidates = buildOzonMarketplaceBarcodeCandidates(sku);
  return skuCandidates[0] || normalizedBarcodes[0] || "";
}

async function getAcceptedScanBarcodesForOffer(offerId, storeId = null, options = {}) {
  const article = normalizeArticleKey(offerId);
  const msLookupOptions = options?.force === true ? { force: true } : {};
  const skuCandidates = buildOzonMarketplaceBarcodeCandidates(options?.sku);
  const sourceBarcodes = uniqueTextList([
    ...(await getOfferBarcodesWithFallback(article, [storeId], {
      sku: options?.sku,
      preferMarketplaceOnly: true,
    })),
    ...skuCandidates,
  ]);
  const aliasBarcodesRaw = await listSavedScanBarcodeAliasesByMarketplaceBarcodes(sourceBarcodes, storeId);
  const aliasBarcodes = uniqueTextList(
    (
      await Promise.all(
        aliasBarcodesRaw.map(async (value) =>
          (await isCompatibleScannedBarcodeForOffer(article, value)) ? value : null
        )
      )
    ).filter(Boolean)
  );
  let acceptedBarcodes = [...sourceBarcodes, ...aliasBarcodes];

  for (const sourceBarcode of sourceBarcodes) {
    try {
      const status = await getMsProductStatusByBarcode(sourceBarcode, msLookupOptions);
      acceptedBarcodes.push(...(status?.barcodes || []));
      continue;
    } catch {}
    if (options?.force === true) {
      continue;
    }
    try {
      const fresh = await getMsProductStatusByBarcode(sourceBarcode, { force: true });
      acceptedBarcodes.push(...(fresh?.barcodes || []));
    } catch {}
  }
  try {
    const articleBarcodes = await getMsBarcodesByArticle(article, msLookupOptions);
    acceptedBarcodes.push(...(articleBarcodes || []));
  } catch {}
  return {
    article,
    sourceBarcodes: uniqueTextList(sourceBarcodes),
    acceptedBarcodes: uniqueTextList(acceptedBarcodes),
  };
}

async function getMsStockStatusForOzonOffer(offerId, storeId = null, options = {}) {
  const article = normalizeArticleKey(offerId);
  const sku = Number.isFinite(Number(options?.sku)) ? Number(options.sku) : null;
  const primaryStoreIds = Array.from(
    new Set(
      (Array.isArray(options?.storeIds) ? options.storeIds : [storeId])
        .map((value) => normalizeStoreId(value))
        .filter(Boolean)
    )
  );
  const offerBarcodes = await getOfferBarcodesWithFallback(article, primaryStoreIds, {
    sku,
    preferMarketplaceOnly: true,
  });
  const marketplaceBarcode = pickPreferredOzonMarketplaceBarcode(offerBarcodes, sku);
  const sourceBarcodes = uniqueTextList([
    ...offerBarcodes,
    ...buildOzonMarketplaceBarcodeCandidates(sku),
  ]);

  if (!sourceBarcodes.length) {
    return {
      status: "no_barcode",
      article,
      sku,
      barcode: "",
      marketplaceBarcode,
      sourceBarcodes,
      found: false,
      multiple: false,
      matches: [],
      stock: null,
      slots: [],
      storeName: String(options?.storeName || "").trim(),
      error: "Не удалось определить баркоды Ozon для товара",
    };
  }

  let fallbackStatus = null;
  for (const sourceBarcode of sourceBarcodes) {
    const status = await getMsStockStatusByBarcode(sourceBarcode, {
      storeName: options?.storeName,
      force: options?.force === true,
    });
    const enriched = {
      ...status,
      article,
      sku,
      marketplaceBarcode,
      sourceBarcode,
      sourceBarcodes,
    };
    if (status?.status === "ok") {
      return enriched;
    }
    if (status?.status === "multiple") {
      return enriched;
    }
    if (status?.status === "store_not_found") {
      return enriched;
    }
    if (status?.status !== "not_found" && !fallbackStatus) {
      fallbackStatus = enriched;
    }
  }

  return (
    fallbackStatus || {
      status: "not_found",
      article,
      sku,
      barcode: sourceBarcodes[0] || "",
      marketplaceBarcode,
      sourceBarcode: sourceBarcodes[0] || "",
      sourceBarcodes,
      found: false,
      multiple: false,
      matches: [],
      stock: null,
      slots: [],
      storeName: String(options?.storeName || "").trim(),
      error: "Товар по баркодам Ozon не найден в складской БД",
    }
  );
}

// --- Названия товаров из складской БД (products_s) для сборки товаров/заказов ---
// Показываем складское название, а не маркетплейсное. Матч ПО БАРКОДУ (OZN+sku / row.barcode),
// фолбэк по артикулу. Кэш по токену (штрихкод/артикул) с TTL.
const WAREHOUSE_NAME_TTL_MS = 5 * 60 * 1000;
const warehouseNameCache = new Map(); // UPPER(token) -> { name, at }

function normWhKey(v) {
  return String(v == null ? "" : v).replace(/\s+/g, "").trim().toUpperCase();
}

// Кандидат-токены для строки: сначала ШТРИХКОДЫ (приоритет), затем артикул.
function warehouseTokensForRow(row) {
  const tokens = [];
  if (row.barcode) tokens.push(normWhKey(row.barcode));
  const sku = row.sku != null ? String(row.sku) : "";
  if (/^\d+$/.test(sku)) tokens.push("OZN" + sku); // ozon-баркод = OZN+sku
  if (row.article) tokens.push(normWhKey(row.article));
  if (row.offer_id) tokens.push(normWhKey(row.offer_id));
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

// Прогрев: собирает токены строк и один bulk-запрос по недостающим.
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

function toEmployeeOrderItem(row) {
  return {
    lineNo: Number(row.line_no),
    article: row.offer_id || null,
    sku: row.sku ? Number(row.sku) : null,
    // Складское название (по баркоду) приоритетнее маркетплейсного; фолбэк — имя из заказа.
    name: warehouseNameForRow(row) || row.name || "",
    entityType: warehouseEntityTypeForRow(row),
    isBundle: warehouseIsBundleForRow(row),
    bundleComponents: warehouseBundleComponentsForRow(row),
    bundleJarCount: warehouseBundleJarCountForRow(row),
    quantity: Number(row.quantity || 0),
    scannedQty: Number(row.scanned_qty || 0),
    imageUrl: null,
  };
}

const ozonCardImageCache = new Map(); // storeId:offerId -> { url, ts }
const OZON_CARD_IMAGE_TTL_MS = 24 * 60 * 60 * 1000;
const OZON_CARD_IMAGE_NEGATIVE_TTL_MS = 2 * 60 * 1000;
// Внешние картинки Ozon (ir.ozone.ru) не всегда доступны из webview/сети рабочего ПК,
// хотя сам сайт доступен. Отдаём их через свой origin (тот же хост, тот же http) —
// путь /OZ/files/_img уже маршрутизируется nginx на этот бэкенд.
function proxifyCardImage(url) {
  const u = url == null ? "" : String(url).trim();
  if (!u) return null;
  if (!/^https?:\/\//i.test(u)) return u; // уже относительный/локальный
  return `${BASE_PATH}/files/_img?u=${encodeURIComponent(u)}`;
}
async function resolveOzonCardImages(storeId, offerIds) {
  const now = Date.now();
  const storeCacheKey = normalizeStoreId(storeId) || "default";
  const unique = [...new Set((offerIds || []).map((o) => (o == null ? "" : String(o).trim())).filter(Boolean))];
  const result = new Map();
  const need = [];
  for (const offer of unique) {
    const cacheKey = `${storeCacheKey}:${offer}`;
    const cached = ozonCardImageCache.get(cacheKey);
    const cacheTtl = cached?.url ? OZON_CARD_IMAGE_TTL_MS : OZON_CARD_IMAGE_NEGATIVE_TTL_MS;
    if (cached && now - cached.ts < cacheTtl) {
      result.set(offer, cached.url);
    } else {
      need.push(offer);
    }
  }
  for (let i = 0; i < need.length; i += 100) {
    const batch = need.slice(i, i + 100);
    let items = [];
    let requestFailed = false;
    try {
      const data = await ozonPost("/v3/product/info/list", { offer_id: batch }, { storeId, timeoutMs: 8000 });
      items = Array.isArray(data?.items) ? data.items : Array.isArray(data?.result?.items) ? data.result.items : [];
    } catch (err) {
      requestFailed = true;
      console.warn("Ozon card images lookup failed", {
        storeId: normalizeStoreId(storeId) || null,
        offers: batch.length,
        error: err?.message || String(err || ""),
      });
    }
    if (requestFailed) continue;
    const byOffer = new Map();
    for (const it of items) {
      const key = String(it?.offer_id || "").trim();
      const primary = Array.isArray(it?.primary_image) ? it.primary_image[0] : it?.primary_image;
      const imgs = Array.isArray(it?.images) ? it.images : [];
      const url = (primary && String(primary).trim()) || imgs.find((u) => u && String(u).trim()) || null;
      if (key) byOffer.set(key, url || null);
    }
    for (const offer of batch) {
      const url = byOffer.has(offer) ? byOffer.get(offer) : null;
      ozonCardImageCache.set(`${storeCacheKey}:${offer}`, { url, ts: now });
      result.set(offer, url);
    }
  }
  return result;
}

async function attachOzonItemImages(assemblyId, itemsByPosting) {
  const offerIds = [];
  for (const items of itemsByPosting.values()) for (const it of items) if (it.article) offerIds.push(it.article);
  if (!offerIds.length) return;
  try {
    const storeRow = await dbQuery("select store_id from ozon_assemblies where id = $1", [assemblyId]);
    const storeId = storeRow.rows?.[0]?.store_id || null;
    const imgMap = await resolveOzonCardImages(storeId, offerIds);
    for (const items of itemsByPosting.values()) for (const it of items) if (it.article) it.imageUrl = proxifyCardImage(imgMap.get(it.article) || null);
  } catch (err) {
    /* фото — не критично, тихо пропускаем */
  }
}

function toEmployeeOrderPayload(row, items = []) {
  const totalItems = items.reduce((sum, item) => sum + item.quantity, 0);
  const scannedItems = items.reduce((sum, item) => sum + item.scannedQty, 0);
  return {
    postingNumber: row.posting_number,
    createdAt: row.order_created_at,
    labelBarcode: row.label_barcode || null,
    labelUrl: row.label_s3_key ? buildS3Url(row.label_s3_key) : null,
    scanPassedAt: row.scan_passed_at || null,
    scanBarcode: row.scan_barcode || null,
    labelScanPassedAt: row.label_scan_passed_at || null,
    labelScanBarcode: row.label_scan_barcode || null,
    collectedAt: row.collected_at || null,
    assignedUserId: row.assigned_user_id ? Number(row.assigned_user_id) : null,
    sentToDeliveringAt: row.sent_to_delivering_at || null,
    totalItems,
    scannedItems,
    scanDone: totalItems > 0 && scannedItems >= totalItems,
    items,
  };
}

function toEmployeeSessionPayload(row) {
  if (!row) return null;
  const pickingStatus = normalizePickingStatus(row.picking_status);
  const pickingRequiredQty = Number(row.picking_required_qty || 0);
  const pickingCollectedQty = Number(row.picking_collected_qty || 0);
  const pickingRemainingQty = Math.max(0, pickingRequiredQty - pickingCollectedQty);
  const packingSeconds =
    row.started_at && !row.finished_at
      ? Math.max(0, Math.round((Date.now() - new Date(row.started_at).getTime()) / 1000))
      : Number(row.total_seconds || 0);
  const pickingSeconds =
    pickingStatus === PICKING_STATUS_STARTED && row.picking_started_at && !row.picking_finished_at
      ? Math.max(0, Math.round((Date.now() - new Date(row.picking_started_at).getTime()) / 1000))
      : Number(row.picking_total_seconds || 0);
  const activePhase =
    pickingStatus === PICKING_STATUS_STARTED && !row.picking_finished_at
      ? "picking"
      : row.status === "started" && !row.finished_at
        ? "packing"
        : null;
  return {
    id: Number(row.id),
    assemblyId: row.assembly_id,
    userId: Number(row.user_id),
    assignmentPartId: row.assignment_part_id != null ? Number(row.assignment_part_id) : null,
    sessionRole: normalizeSessionRole(row.session_role),
    tableNo: normalizeAssignmentTableNo(row.table_no),
    assignedBy: row.assigned_by != null ? Number(row.assigned_by) : null,
    assignmentMode: row.assignment_mode || "split",
    status: row.status || "assigned",
    assignedAt: row.assigned_at ?? null,
    startedAt: row.started_at ?? null,
    finishedAt: row.finished_at ?? null,
    assignedOrderCount: Number(row.assigned_order_count || 0),
    collectedOrderCount: Number(row.collected_order_count || 0),
    totalSeconds: packingSeconds,
    packingTotalSeconds: packingSeconds,
    pickingStatus,
    pickingStartedAt: row.picking_started_at ?? null,
    pickingFinishedAt: row.picking_finished_at ?? null,
    pickingTotalSeconds: pickingSeconds,
    pickingRequiredQty,
    pickingCollectedQty,
    pickingRemainingQty,
    activePhase,
    firstCollectedAt: row.first_collected_at ?? null,
    lastCollectedAt: row.last_collected_at ?? null,
  };
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
    assemblyId: row.assembly_id,
    userId: Number(row.user_id),
    itemKey: row.item_key,
    article: row.article || null,
    barcode: row.barcode || null,
    // Складское название (по баркоду) приоритетнее маркетплейсного.
    productName: warehouseNameForRow(row) || row.product_name || null,
    entityType: warehouseEntityTypeForRow(row),
    isBundle: warehouseIsBundleForRow(row),
    bundleComponents: warehouseBundleComponentsForRow(row),
    bundleJarCount: warehouseBundleJarCountForRow(row),
    sku: row.sku != null ? Number(row.sku) : null,
    imageUrl: proxifyCardImage(row.imageUrl),
    requiredQty,
    pickedQty,
    remainingQty: Math.max(0, requiredQty - pickedQty),
    done: requiredQty > 0 && pickedQty >= requiredQty,
    bypassWithoutScanAllowed: row.bypass_without_scan_allowed === true,
    bypassAuthorizedAt: row.bypass_authorized_at ?? null,
    msCells,
    updatedAt: row.updated_at ?? null,
  };
}

async function getEmployeeAssemblyOrder(assemblyId, postingNumber) {
  const orderResult = await dbQuery(
    `select posting_number, order_created_at, label_barcode, label_s3_key,
            scan_passed_at, scan_barcode, label_scan_passed_at, label_scan_barcode,
            collected_at, assigned_user_id, sent_to_delivering_at
     from ozon_assembly_orders
     where assembly_id = $1
       and posting_number = $2
       and collected_at is null
       and sent_to_delivering_at is null
     limit 1`,
    [assemblyId, postingNumber]
  );
  const order = orderResult.rows?.[0];
  if (!order) return null;
  const itemRows = await ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber);
  await warmWarehouseNames(itemRows);
  return toEmployeeOrderPayload(order, itemRows.map(toEmployeeOrderItem));
}

async function listEmployeeAssemblyOrders(assemblyId, userId, accessMode) {
  const orderResult = await dbQuery(
    `select posting_number, order_created_at, label_barcode, label_s3_key,
            scan_passed_at, scan_barcode, label_scan_passed_at, label_scan_barcode,
            collected_at, assigned_user_id, sent_to_delivering_at
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is null
       and sent_to_delivering_at is null
       ${accessMode === "selected_split" ? "and assigned_user_id = $2" : ""}
     order by order_created_at asc nulls last, posting_number asc`,
    accessMode === "selected_split" ? [assemblyId, userId] : [assemblyId]
  );
  const postings = orderResult.rows.map((row) => row.posting_number);
  if (!postings.length) return [];

  const itemResult = await dbQuery(
    `select posting_number, line_no, offer_id, sku, name, quantity, scanned_qty
     from ozon_assembly_order_items
     where assembly_id = $1 and posting_number = any($2::text[])
     order by posting_number asc, line_no asc`,
    [assemblyId, postings]
  );
  await warmWarehouseNames(itemResult.rows || []);
  const itemsByPosting = new Map();
  for (const row of itemResult.rows || []) {
    if (!itemsByPosting.has(row.posting_number)) itemsByPosting.set(row.posting_number, []);
    itemsByPosting.get(row.posting_number).push(toEmployeeOrderItem(row));
  }

  const missingPostings = postings.filter((postingNumber) => !itemsByPosting.has(postingNumber));
  if (missingPostings.length) {
    await mapWithConcurrency(missingPostings, 4, async (postingNumber) => {
      const rows = await ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber);
      if (!rows.length) return;
      await warmWarehouseNames(rows);
      itemsByPosting.set(postingNumber, rows.map(toEmployeeOrderItem));
    });
  }

  await attachOzonItemImages(assemblyId, itemsByPosting);
  return orderResult.rows.map((row) => toEmployeeOrderPayload(row, itemsByPosting.get(row.posting_number) || []));
}

// Уже СОБРАННЫЕ заказы сотрудника — для окна «Собранные заказы» (только просмотр
// + перепечать этикетки, завершать нельзя). Не попадают в основной список сборки.
async function listEmployeeCollectedAssemblyOrders(assemblyId, userId, accessMode) {
  const orderResult = await dbQuery(
    `select posting_number, order_created_at, label_barcode, label_s3_key,
            scan_passed_at, scan_barcode, label_scan_passed_at, label_scan_barcode,
            collected_at, assigned_user_id, sent_to_delivering_at
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is not null
       ${accessMode === "selected_split" ? "and assigned_user_id = $2" : ""}
     order by collected_at desc nulls last, posting_number asc
     limit 500`,
    accessMode === "selected_split" ? [assemblyId, userId] : [assemblyId]
  );
  const postings = orderResult.rows.map((row) => row.posting_number);
  if (!postings.length) return [];
  const itemResult = await dbQuery(
    `select posting_number, line_no, offer_id, sku, name, quantity, scanned_qty
     from ozon_assembly_order_items
     where assembly_id = $1 and posting_number = any($2::text[])
     order by posting_number asc, line_no asc`,
    [assemblyId, postings]
  );
  await warmWarehouseNames(itemResult.rows || []);
  const itemsByPosting = new Map();
  for (const row of itemResult.rows || []) {
    if (!itemsByPosting.has(row.posting_number)) itemsByPosting.set(row.posting_number, []);
    itemsByPosting.get(row.posting_number).push(toEmployeeOrderItem(row));
  }
  return orderResult.rows.map((row) => toEmployeeOrderPayload(row, itemsByPosting.get(row.posting_number) || []));
}

async function matchOpenItemInOrderByBarcode(assemblyId, postingNumber, barcode, storeId = null, options = {}) {
  const started = process.hrtime.bigint();
  try {
    const forceFresh = options?.forceFresh === true;
    const normalizedBarcode = normalizeBarcode(barcode);
    if (!normalizedBarcode) return null;
    const itemRows = await ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber, storeId);
    const openItems = itemRows.filter(
      (row) => Number(row.quantity || 0) - Number(row.scanned_qty || 0) > 0
    );
    const scanLookup = await getScanLookupBarcodes(normalizedBarcode, forceFresh ? { force: true } : {});
    const scanCandidateSet = new Set(
      scanLookup.multiple ? [normalizedBarcode] : uniqueTextList([normalizedBarcode, ...(scanLookup.barcodes || [])])
    );
    const offerCache = new Map();
    const matchedItems = [];
    for (const row of openItems) {
      const cacheKey = `${normalizeArticleKey(row.offer_id) || ""}||${normalizeBarcode(row.sku) || ""}`;
      let matched = offerCache.get(cacheKey);
      if (!matched) {
        matched = await getAcceptedScanBarcodesForOffer(row.offer_id, storeId, {
          sku: row.sku,
          force: forceFresh,
        });
        offerCache.set(cacheKey, matched);
      }
      const candidateSet = new Set(uniqueTextList([...(matched.sourceBarcodes || []), ...(matched.acceptedBarcodes || [])]));
      const intersects = Array.from(scanCandidateSet).some((value) => candidateSet.has(value));
      if (!intersects) {
        continue;
      }
      matchedItems.push({
        lineNo: Number(row.line_no),
        article: normalizeArticleKey(row.offer_id),
        name: row.name || "",
        postingNumber,
        sourceBarcodes: matched.sourceBarcodes,
        acceptedBarcodes: uniqueTextList([...(matched.acceptedBarcodes || []), ...Array.from(scanCandidateSet)]),
      });
    }
    if (!matchedItems.length) return null;
    const uniqueKeys = new Set(matchedItems.map((item) => getAssemblyMatchIdentityKey(item)));
    if (uniqueKeys.size > 1) {
      return {
        ambiguous: true,
        matches: matchedItems,
      };
    }
    return {
      ...matchedItems[0],
      ambiguous: false,
      matches: matchedItems,
    };
  } finally {
    recordPerf("employee.match_open_item", elapsedMs(started));
  }
}

async function findFirstAssemblyOrderByBarcode(assemblyId, auth, barcode, options = {}) {
  const started = process.hrtime.bigint();
  try {
    const forceFresh = options?.forceFresh === true;
    const context = await getEmployeeAssemblyContext(assemblyId, auth, { requireStartedSession: true });
    if (context.error) return context;
    const scanLookup = await getScanLookupBarcodes(barcode, forceFresh ? { force: true } : {});
    const orders = await listEmployeeAssemblyOrders(assemblyId, auth.id, context.assembly.access_mode);
    const candidates = [];
    const candidateProductKeys = new Set();
    for (const order of orders) {
      const matched = await matchOpenItemInOrderByBarcode(
        assemblyId,
        order.postingNumber,
        barcode,
        context.assembly.store_id || null,
        options
      );
      if (!matched) continue;
      if (matched.ambiguous) {
        for (const item of Array.isArray(matched.matches) ? matched.matches : []) {
          candidates.push({ order, matchedItem: item });
          candidateProductKeys.add(getAssemblyMatchIdentityKey(item));
        }
      } else {
        candidates.push({ order, matchedItem: matched });
        candidateProductKeys.add(getAssemblyMatchIdentityKey(matched));
      }
      if (candidateProductKeys.size > 1) break;
    }
    if (!candidates.length) {
      return {
        context,
        order: null,
        matchedItem: null,
        matches: [],
        ambiguous: false,
        warning: "",
        multipleScanProducts: scanLookup.multiple === true,
        scanProducts: Array.isArray(scanLookup.matches) ? scanLookup.matches : [],
      };
    }
    if (candidateProductKeys.size > 1) {
      return {
        context,
        order: null,
        matchedItem: null,
        matches: candidates.map((entry) => entry?.matchedItem).filter(Boolean),
        ambiguous: true,
        multipleScanProducts: scanLookup.multiple === true,
        scanProducts: Array.isArray(scanLookup.matches) ? scanLookup.matches : [],
        warning: buildAmbiguousAssemblyScanWarning(
          barcode,
          candidates.map((entry) => entry?.matchedItem),
          {
            multipleScanProducts: scanLookup.multiple === true,
            scanProducts: Array.isArray(scanLookup.matches) ? scanLookup.matches : [],
          }
        ),
      };
    }
    return {
      context,
      ...candidates[0],
      matches: candidates.map((entry) => entry?.matchedItem).filter(Boolean),
      ambiguous: false,
      warning: "",
      multipleScanProducts: scanLookup.multiple === true,
      scanProducts: Array.isArray(scanLookup.matches) ? scanLookup.matches : [],
    };
  } finally {
    recordPerf("employee.find_order_by_barcode", elapsedMs(started));
  }
}

function toOzonScanIssuePayload(row) {
  const issueStage = normalizeScanIssueStage(row.issue_stage);
  const employeeFullName = [row.employee_surname, row.employee_name].filter(Boolean).join(" ").trim();
  return {
    id: Number(row.id),
    assemblyId: row.assembly_id,
    assemblyName: row.assembly_name || null,
    assemblyStoreId: row.assembly_store_id || null,
    postingNumber: row.posting_number || null,
    employeeId: Number(row.employee_id),
    employeeSurname: row.employee_surname || null,
    employeeName: employeeFullName || row.employee_name || null,
    issueStage,
    issueStageLabel: assemblyScanIssueStageLabel(issueStage),
    errorMessage: row.error_message || null,
    pickItemId: row.pick_item_id == null ? null : Number(row.pick_item_id),
    pickItemKey: row.pick_item_key || null,
    contextNote: row.context_note || null,
    lineNos: uniqueNumberList(row.line_nos),
    itemArticles: Array.isArray(row.item_articles) ? row.item_articles.filter(Boolean) : [],
    itemNames: Array.isArray(row.item_names) ? row.item_names.filter(Boolean) : [],
    productBarcodes: Array.isArray(row.product_barcodes) ? row.product_barcodes.filter(Boolean) : [],
    scannedBarcode: row.scanned_barcode || null,
    originalScannedBarcode: row.original_scanned_barcode || row.scanned_barcode || null,
    confirmationScannedBarcode: row.confirmation_scanned_barcode || null,
    marketplaceArticle: row.marketplace_article || row.item_articles?.[0] || null,
    warehouseProductId: row.warehouse_product_id == null ? null : Number(row.warehouse_product_id),
    warehouseArticle: row.warehouse_article || null,
    warehouseProductName: row.warehouse_product_name || null,
    mappingBarcode: row.mapping_barcode || null,
    imageUrl: proxifyCardImage(row.image_url) || null,
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

async function getEmployeeOpenAssemblySession(assemblyId, userId) {
  const result = await dbQuery(
    `select *
     from ozon_employee_assembly_sessions
     where assembly_id = $1 and user_id = $2 and finished_at is null
     order by assigned_at desc, id desc
     limit 1`,
    [assemblyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getAssemblyAssignmentPartForSession(session) {
  const assignmentPartId = Number(session?.assignment_part_id || 0);
  if (!assignmentPartId) return null;
  return await getAssemblyAssignmentPartById(assignmentPartId);
}

async function getEmployeeStartedAssemblySession(userId) {
  const result = await dbQuery(
    `select *
     from ozon_employee_assembly_sessions
     where user_id = $1 and status = 'started' and finished_at is null
     order by started_at desc, id desc
     limit 1`,
    [userId]
  );
  return result.rows?.[0] || null;
}

async function getEmployeeStartedAssemblySessionForAssembly(assemblyId, userId) {
  const result = await dbQuery(
    `select *
     from ozon_employee_assembly_sessions
     where assembly_id = $1 and user_id = $2 and status = 'started' and finished_at is null
     order by started_at desc, id desc
     limit 1`,
    [assemblyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getEmployeeActiveAssemblySession(userId) {
  const result = await dbQuery(
    `select *,
            case
              when picking_status = 'started' and picking_finished_at is null then 'picking'
              when status = 'started' and finished_at is null then 'packing'
              else null
            end as active_phase
     from ozon_employee_assembly_sessions
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

async function closeOpenEmployeeSessionsForAssembly(assemblyId, status = "reassigned") {
  await dbQuery(
    `update ozon_employee_assembly_sessions
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
     where assembly_id = $1 and finished_at is null`,
    [assemblyId, status]
  );
}

async function createEmployeeAssignmentSessionsForAssembly(assemblyId, adminId, mode, perUser) {
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
    values.push(assemblyId, entry.userId, adminId || null, mode, entry.count);
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, now(), $${base + 5}, now(), now())`;
  });
  const result = await dbQuery(
    `insert into ozon_employee_assembly_sessions
      (assembly_id, user_id, assigned_by, assignment_mode, assigned_at, assigned_order_count, created_at, updated_at)
     values ${placeholders.join(", ")}
     returning *`,
    values
  );
  return result.rows || [];
}

async function ensureEmployeeAssignmentSessionForAssembly(
  assemblyId,
  userId,
  assignedOrderCount,
  assignedBy = null,
  mode = "split"
) {
  const existing = await getEmployeeOpenAssemblySession(assemblyId, userId);
  if (existing) return existing;
  if (!Number.isFinite(Number(assignedOrderCount)) || Number(assignedOrderCount) < 1) {
    return null;
  }
  const result = await dbQuery(
    `insert into ozon_employee_assembly_sessions
      (assembly_id, user_id, assigned_by, assignment_mode, assigned_at, assigned_order_count, created_at, updated_at)
     values ($1, $2, $3, $4, now(), $5, now(), now())
     returning *`,
    [assemblyId, userId, assignedBy, mode, Number(assignedOrderCount)]
  );
  return result.rows?.[0] || null;
}

async function listEmployeePickItemsForSession(sessionId) {
  const result = await dbQuery(
    `select *
     from ozon_employee_assembly_pick_items
     where session_id = $1
     order by (picked_qty >= required_qty and required_qty > 0)::int asc, required_qty desc nulls last, id asc`,
    [sessionId]
  );
  const rows = await repairEmployeePickItemMarketplaceBarcodes(result.rows || []);
  await attachOzonPickItemImages(rows);
  return rows;
}

async function attachOzonPickItemImages(rows) {
  if (!Array.isArray(rows) || !rows.length) return;
  const offerIds = rows.map((r) => r.offer_id || r.article).filter(Boolean);
  if (!offerIds.length) return;
  try {
    const assemblyId = rows[0].assembly_id;
    const storeRow = await dbQuery("select store_id from ozon_assemblies where id = $1", [assemblyId]);
    const storeId = storeRow.rows?.[0]?.store_id || null;
    const imgMap = await resolveOzonCardImages(storeId, offerIds);
    for (const r of rows) {
      const key = r.offer_id || r.article;
      r.imageUrl = key ? imgMap.get(String(key).trim()) || null : null;
    }
  } catch (err) {
    /* фото — не критично */
  }
}

function buildAssemblyPickItemKey(row) {
  return [
    normalizeArticleKey(row?.offer_id || row?.article || ""),
    normalizeBarcode(row?.barcode || ""),
    normalizeBarcode(row?.sku),
    normalizeText(row?.name || row?.product_name || ""),
  ]
    .filter(Boolean)
    .join("::");
}

function buildPickIssueContextNote(items) {
  const pending = (Array.isArray(items) ? items : []).filter(
    (item) => Number(item.picked_qty || item.pickedQty || 0) < Number(item.required_qty || item.requiredQty || 0)
  );
  if (!pending.length) return null;
  const names = pending
    .map((item) => item.article || item.product_name || item.productName || item.barcode || "")
    .filter(Boolean)
    .slice(0, 6);
  if (!names.length) return null;
  return pending.length > names.length
    ? `${names.join(", ")} и ещё ${pending.length - names.length}`
    : names.join(", ");
}

function buildOrderIssueContextNote({ postingNumber = null, itemArticles = [], lineNos = [], matches = [] } = {}) {
  const parts = [];
  if (postingNumber) parts.push(`Заказ ${postingNumber}`);
  const articles = limitWarningLabels(Array.isArray(itemArticles) ? itemArticles.filter(Boolean) : []);
  if (articles.length) parts.push(`Артикулы: ${articles.join(", ")}`);
  const lines = limitWarningLabels((Array.isArray(lineNos) ? lineNos : []).map((value) => String(value || "").trim()));
  if (lines.length) parts.push(`Строки: ${lines.join(", ")}`);
  const matchLabels = limitWarningLabels(
    (Array.isArray(matches) ? matches : []).map((item) => formatAssemblyMatchLabel(item))
  );
  if (matchLabels.length) {
    parts.push(`Подходящие товары в сборке: ${matchLabels.join(", ")}`);
  }
  return parts.join(". ") || null;
}

async function reportAutoAssemblyOrderScanIssue({
  assemblyId,
  postingNumber = null,
  employeeId,
  lineNos = [],
  itemArticles = [],
  itemNames = [],
  productBarcodes = [],
  matches = [],
  scannedBarcode = null,
  errorMessage,
}) {
  try {
    const issue = await createOrUpdateAssemblyScanIssue({
      assemblyId,
      postingNumber,
      employeeId,
      lineNos,
      itemArticles,
      itemNames,
      productBarcodes,
      scannedBarcode,
      issueStage: SCAN_ISSUE_STAGE_ORDER,
      errorMessage: errorMessage || null,
      contextNote: buildOrderIssueContextNote({ postingNumber, itemArticles, lineNos, matches }),
    });
    broadcastSse("scan_issue_update", { at: new Date().toISOString() }, (client) => client.role === ROLE_ADMIN);
    return issue;
  } catch {
    return null;
  }
}

async function refreshEmployeePickingSessionSummary(sessionId) {
  const totalsResult = await dbQuery(
    `select coalesce(sum(required_qty), 0)::int as required_qty,
            coalesce(sum(picked_qty), 0)::int as picked_qty
     from ozon_employee_assembly_pick_items
     where session_id = $1`,
    [sessionId]
  );
  const requiredQty = Number(totalsResult.rows?.[0]?.required_qty || 0);
  const pickedQty = Number(totalsResult.rows?.[0]?.picked_qty || 0);
  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
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
    `update ozon_employee_assembly_sessions
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
     from ozon_employee_assembly_pick_items
     where session_id = $1 and id = $2
     limit 1`,
    [sessionId, itemId]
  );
  const rows = await repairEmployeePickItemMarketplaceBarcodes(result.rows || []);
  return rows[0] || null;
}

async function repairEmployeePickItemMarketplaceBarcodes(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return [];
  const repaired = await mapWithConcurrency(list, 6, async (row) => {
    const article = normalizeArticleKey(row?.article);
    if (!article) return row;
    const currentBarcode = normalizeBarcode(row?.barcode || "");
    if (/^OZN/i.test(String(currentBarcode || "").trim())) return row;
    const marketplaceBarcodes = await getOfferBarcodesWithFallback(article, [], {
      sku: row?.sku,
      preferMarketplaceOnly: true,
    });
    const preferredBarcode = pickPreferredOzonMarketplaceBarcode(marketplaceBarcodes, row?.sku);
    const normalizedPreferred = normalizeBarcode(preferredBarcode);
    if (!normalizedPreferred || normalizedPreferred === currentBarcode) {
      return row;
    }
    await dbQuery(
      `update ozon_employee_assembly_pick_items
       set barcode = $2,
           updated_at = now()
       where id = $1`,
      [Number(row.id), normalizedPreferred]
    );
    return {
      ...row,
      barcode: normalizedPreferred,
    };
  });
  return repaired;
}

async function allowEmployeePickItemWithoutScan(itemId, adminId) {
  const result = await dbQuery(
    `update ozon_employee_assembly_pick_items
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
    `update ozon_employee_assembly_pick_items
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

async function ensureEmployeeProductPickItems(session) {
  if (!session?.id) return [];
  const existing = await listEmployeePickItemsForSession(session.id);
  if (existing.length > 0) {
    await refreshEmployeePickingSessionSummary(session.id);
    return existing;
  }

  const result = await dbQuery(
    `select i.offer_id as article,
            max(o.store_id) as store_id,
            max(i.name) as product_name,
            max(i.sku) as sku,
            coalesce(sum(greatest(coalesce(i.quantity, 1), 1) - least(coalesce(i.scanned_qty, 0), greatest(coalesce(i.quantity, 1), 1))), 0)::int as required_qty
     from ozon_assembly_orders o
     join ozon_assembly_order_items i
       on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
     where o.assembly_id = $1
       and o.assigned_user_id = $2
       and o.collected_at is null
       and o.sent_to_delivering_at is null
     group by i.offer_id
     having coalesce(sum(greatest(coalesce(i.quantity, 1), 1) - least(coalesce(i.scanned_qty, 0), greatest(coalesce(i.quantity, 1), 1))), 0) > 0
     order by max(i.name) asc, i.offer_id asc`,
    [session.assembly_id, session.user_id]
  );
  const rows = result.rows || [];
  if (!rows.length) {
    await refreshEmployeePickingSessionSummary(session.id);
    return [];
  }

  const preparedRows = await mapWithConcurrency(rows, 6, async (row) => {
    const lookupBarcodes = await getOfferBarcodesWithFallback(row.article, [row.store_id || null], {
      sku: row.sku,
      preferMarketplaceOnly: true,
    });
    return {
      ...row,
      lookup_barcodes: lookupBarcodes,
    };
  });

  const values = [];
  const placeholders = [];
  for (let index = 0; index < preparedRows.length; index += 1) {
    const row = preparedRows[index];
    const itemKey = buildAssemblyPickItemKey(row);
    const barcodes = uniqueTextList([
      ...(Array.isArray(row.lookup_barcodes) ? row.lookup_barcodes : []),
      ...buildOzonMarketplaceBarcodeCandidates(row.sku),
    ]);
    const base = index * 9;
    values.push(
      session.id,
      session.assembly_id,
      session.user_id,
      itemKey,
      row.article || null,
      pickPreferredOzonMarketplaceBarcode(barcodes, row.sku) || null,
      row.product_name || null,
      row.sku ?? null,
      Number(row.required_qty || 0)
    );
    placeholders.push(
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, 0, now(), now())`
    );
  }

  await dbQuery(
    `insert into ozon_employee_assembly_pick_items
      (session_id, assembly_id, user_id, item_key, article, barcode, product_name, sku, required_qty, picked_qty, created_at, updated_at)
     values ${placeholders.join(", ")}
     on conflict (session_id, item_key) do update
       set assembly_id = excluded.assembly_id,
           user_id = excluded.user_id,
           article = coalesce(excluded.article, ozon_employee_assembly_pick_items.article),
           barcode = coalesce(excluded.barcode, ozon_employee_assembly_pick_items.barcode),
           product_name = coalesce(excluded.product_name, ozon_employee_assembly_pick_items.product_name),
           sku = coalesce(excluded.sku, ozon_employee_assembly_pick_items.sku),
           required_qty = excluded.required_qty,
           updated_at = now()`,
    values
  );
  await refreshEmployeePickingSessionSummary(session.id);
  return await listEmployeePickItemsForSession(session.id);
}

async function syncEmployeeProductPickItemsAfterAssemblySync(session) {
  if (!session?.id) return [];
  if (normalizeSessionRole(session.session_role) === SESSION_ROLE_PACK) {
    await dbQuery("delete from ozon_employee_assembly_pick_items where session_id = $1", [session.id]);
    await refreshEmployeePickingSessionSummary(session.id);
    return [];
  }

  const assignmentPartId = Number(session.assignment_part_id || 0);
  const result = await dbQuery(
    `select i.offer_id as article,
            max(o.store_id) as store_id,
            max(i.name) as product_name,
            max(i.sku) as sku,
            coalesce(sum(greatest(coalesce(i.quantity, 1), 1) - least(coalesce(i.scanned_qty, 0), greatest(coalesce(i.quantity, 1), 1))), 0)::int as required_qty
     from ozon_assembly_orders o
     join ozon_assembly_order_items i
       on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
     where o.assembly_id = $1
       and o.collected_at is null
       and o.sent_to_delivering_at is null
       ${assignmentPartId > 0 ? "and o.assignment_part_id = $2" : "and o.assigned_user_id = $2"}
     group by i.offer_id
     having coalesce(sum(greatest(coalesce(i.quantity, 1), 1) - least(coalesce(i.scanned_qty, 0), greatest(coalesce(i.quantity, 1), 1))), 0) > 0
     order by max(i.name) asc, i.offer_id asc`,
    assignmentPartId > 0
      ? [session.assembly_id, assignmentPartId]
      : [session.assembly_id, session.user_id]
  );
  const desiredItems = (result.rows || [])
    .map((row) => ({
      ...row,
      itemKey: buildAssemblyPickItemKey(row),
      requiredQty: Math.max(0, Number(row.required_qty || 0)),
    }))
    .filter((row) => row.itemKey && row.requiredQty > 0);
  const existing = await listEmployeePickItemsForSession(session.id);

  if (!desiredItems.length) {
    if (existing.length) {
      await dbQuery("delete from ozon_employee_assembly_pick_items where session_id = $1", [session.id]);
    }
    await refreshEmployeePickingSessionSummary(session.id);
    return [];
  }

  const desiredByKey = new Map(desiredItems.map((item) => [item.itemKey, item]));
  const existingByKey = new Map(existing.map((row) => [String(row.item_key || ""), row]));
  const staleIds = existing
    .filter((row) => !desiredByKey.has(String(row.item_key || "")))
    .map((row) => Number(row.id))
    .filter((id) => Number.isFinite(id) && id > 0);
  if (staleIds.length) {
    await dbQuery(
      "delete from ozon_employee_assembly_pick_items where session_id = $1 and id = any($2::bigint[])",
      [session.id, staleIds]
    );
  }

  for (const item of desiredItems) {
    const existingRow = existingByKey.get(item.itemKey);
    if (existingRow) {
      await dbQuery(
        `update ozon_employee_assembly_pick_items
         set article = $3,
             product_name = $4,
             sku = $5,
             required_qty = $6,
             picked_qty = least(greatest(coalesce(picked_qty, 0), 0), $6),
             updated_at = now()
         where id = $1 and session_id = $2`,
        [
          Number(existingRow.id),
          session.id,
          item.article || null,
          item.product_name || null,
          item.sku ?? null,
          item.requiredQty,
        ]
      );
      continue;
    }

    const lookupBarcodes = await getOfferBarcodesWithFallback(item.article, [item.store_id || null], {
      sku: item.sku,
      preferMarketplaceOnly: true,
    });
    const barcodes = uniqueTextList([
      ...(Array.isArray(lookupBarcodes) ? lookupBarcodes : []),
      ...buildOzonMarketplaceBarcodeCandidates(item.sku),
    ]);
    await dbQuery(
      `insert into ozon_employee_assembly_pick_items
        (session_id, assembly_id, user_id, item_key, article, barcode, product_name, sku, required_qty, picked_qty, created_at, updated_at)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, now(), now())
       on conflict (session_id, item_key) do update
         set article = excluded.article,
             barcode = coalesce(excluded.barcode, ozon_employee_assembly_pick_items.barcode),
             product_name = excluded.product_name,
             sku = excluded.sku,
             required_qty = excluded.required_qty,
             picked_qty = least(ozon_employee_assembly_pick_items.picked_qty, excluded.required_qty),
             updated_at = now()`,
      [
        session.id,
        session.assembly_id,
        session.user_id,
        item.itemKey,
        item.article || null,
        pickPreferredOzonMarketplaceBarcode(barcodes, item.sku) || null,
        item.product_name || null,
        item.sku ?? null,
        item.requiredQty,
      ]
    );
  }

  await refreshEmployeePickingSessionSummary(session.id);
  return await listEmployeePickItemsForSession(session.id);
}

function normalizeEmployeePickCellName(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "")
    .replace(/C/g, "С")
    .replace(/P/g, "П")
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
  const byName = String(left?.product_name || left?.productName || "").localeCompare(
    String(right?.product_name || right?.productName || ""),
    "ru",
    { numeric: true, sensitivity: "base" }
  );
  if (byName !== 0) return byName;
  return Number(left?.id || 0) - Number(right?.id || 0);
}

function sortEmployeePickRows(rows) {
  return [...(Array.isArray(rows) ? rows : [])].sort(compareEmployeePickRows);
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
  const warehouseOptions = getSkladWarehouseOptionsFromAssembly(options);
  const articleKey = normalizeArticleKey(row?.article);
  const barcodeKey = normalizeBarcode(row?.barcode);
  const skuValue = Number.isFinite(Number(row?.sku)) ? Number(row.sku) : null;
  if (!articleKey && !barcodeKey && !skuValue) return [];
  try {
    const status = barcodeKey
      ? await getSkladFbsStockStatusByBarcode(barcodeKey, {
          fallbackArticle: articleKey,
          warehouseId: warehouseOptions.warehouseId,
          storeName: warehouseOptions.storeName,
          force: true,
        })
      : await getSkladFbsStockStatusByArticle(articleKey || String(skuValue || ""), {
          warehouseId: warehouseOptions.warehouseId,
          storeName: warehouseOptions.storeName,
          marketplaceType: "ozon",
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
  const warehouseOptions = getSkladWarehouseOptionsFromAssembly(options);
  const uniqueRequests = [];
  const requestKeys = new Set();
  for (const row of items) {
    const barcodeKey = normalizeBarcode(row?.barcode);
    const requestKey = barcodeKey ? `barcode:${barcodeKey}` : "";
    if (!requestKey || requestKeys.has(requestKey)) continue;
    requestKeys.add(requestKey);
    uniqueRequests.push({
      key: requestKey,
      barcode: barcodeKey,
      fallbackArticle: row?.article || null,
    });
  }
  const statusByKey =
    uniqueRequests.length > 0
      ? await getMsStockStatusesByBarcodes(uniqueRequests, {
          warehouseId: warehouseOptions.warehouseId,
          storeName: warehouseOptions.storeName,
          marketplaceType: "ozon",
        })
      : new Map();
  const cache = new Map();
  const entries = await mapWithConcurrency(
    items,
    3,
    async (row) => {
      const barcodeKey = normalizeBarcode(row?.barcode);
      const cacheKey = barcodeKey ? `barcode:${barcodeKey}` : "";
      if (!cacheKey) {
        return [row?.id || row?.item_key || Math.random(), []];
      }
      if (cache.has(cacheKey)) {
        return [row?.id || row?.item_key || cacheKey, cache.get(cacheKey)];
      }
      try {
        let cells = normalizePickItemMsCells(statusByKey.get(cacheKey));
        if (!cells.length) {
          cells = await loadFallbackPickItemMsCells(row, warehouseOptions);
        }
        cache.set(cacheKey, cells);
        return [row?.id || row?.item_key || cacheKey, cells];
      } catch {
        const fallbackCells = await loadFallbackPickItemMsCells(row, warehouseOptions);
        cache.set(cacheKey, fallbackCells);
        return [row?.id || row?.item_key || cacheKey, fallbackCells];
      }
    }
  );
  const cellsByRowId = new Map(entries);
  return sortEmployeePickRows(
    items.map((row) => ({
      ...row,
      ms_cells: sortEmployeePickCells(cellsByRowId.get(row?.id || row?.item_key) || []),
    }))
  );
}

async function enrichSingleEmployeePickItemWithMsCells(row, options = {}) {
  if (!row) return null;
  const [enriched] = await enrichEmployeePickItemsWithMsCells([row], options);
  return enriched || row;
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

async function resetEmployeePickingForAssembly(assemblyId, pickingStatus = PICKING_STATUS_NOT_REQUIRED) {
  await dbQuery("delete from ozon_employee_assembly_pick_items where assembly_id = $1", [assemblyId]);
  await dbQuery(
    `update ozon_employee_assembly_sessions
     set picking_status = $2,
         picking_started_at = null,
         picking_finished_at = null,
         picking_total_seconds = 0,
         picking_required_qty = 0,
         picking_collected_qty = 0,
         updated_at = now()
     where assembly_id = $1 and finished_at is null`,
    [assemblyId, pickingStatus]
  );
}

async function assemblyHasActiveEmployeeWork(assemblyId) {
  const result = await dbQuery(
    `select 1
     from ozon_employee_assembly_sessions
     where assembly_id = $1
       and finished_at is null
       and (started_at is not null or picking_started_at is not null)
     limit 1`,
    [assemblyId]
  );
  return Boolean(result.rows?.[0]);
}

async function assemblyHasBlockingRedistributeWork(assemblyId) {
  const result = await dbQuery(
    `select 1
     from ozon_employee_assembly_sessions
     where assembly_id = $1
       and (
         (started_at is not null and finished_at is null)
         or (picking_status = $2 and picking_finished_at is null)
       )
     limit 1`,
    [assemblyId, PICKING_STATUS_STARTED]
  );
  return Boolean(result.rows?.[0]);
}

async function assemblyHasStartedPackingWork(assemblyId) {
  const result = await dbQuery(
    `select 1
     from ozon_employee_assembly_sessions
     where assembly_id = $1
       and started_at is not null
     limit 1`,
    [assemblyId]
  );
  return Boolean(result.rows?.[0]);
}

async function captureAssemblyRedistributeCarryover(assemblyId) {
  const parts = await listAssemblyAssignmentParts(assemblyId);
  if (!parts.length) {
    return { pickedItems: [], partStates: new Map() };
  }

  const sessionResult = await dbQuery(
    `select assignment_part_id,
            bool_or(
              session_role in ($2, $3)
              and picking_status = $4
              and picking_finished_at is not null
            ) as picking_finished
     from ozon_employee_assembly_sessions
     where assembly_id = $1
       and assignment_part_id is not null
     group by assignment_part_id`,
    [assemblyId, SESSION_ROLE_PICK, SESSION_ROLE_PICK_PACK, PICKING_STATUS_FINISHED]
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
    return { pickedItems: [], partStates };
  }

  const itemResult = await dbQuery(
    `select o.posting_number,
            o.assignment_part_id,
            i.offer_id as article,
            i.barcode,
            greatest(
              greatest(coalesce(i.quantity, 1), 1) - least(coalesce(i.scanned_qty, 0), greatest(coalesce(i.quantity, 1), 1)),
              0
            )::int as qty
     from ozon_assembly_orders o
     join ozon_assembly_order_items i
       on i.assembly_id = o.assembly_id
      and i.posting_number = o.posting_number
     where o.assembly_id = $1
       and o.collected_at is null
       and o.sent_to_delivering_at is null
       and o.assignment_part_id = any($2)
     order by o.posting_number asc, i.line_no asc`,
    [assemblyId, pickedPartIds]
  );

  const pickedItems = [];
  for (const row of itemResult.rows || []) {
    const qty = Math.max(0, Number(row.qty || 0));
    if (qty < 1) continue;
    const normalizedPartId = Number(row.assignment_part_id || 0);
    const state = partStates.get(normalizedPartId);
    if (!state?.pickedComplete || !state.inventoryUserId) continue;
    pickedItems.push({
      postingNumber: normalizeText(row.posting_number),
      oldPartId: normalizedPartId,
      oldHolderUserId: Number(state.inventoryUserId),
      article: normalizeArticleKey(row.article),
      barcode: normalizeBarcode(row.barcode),
      qty,
    });
  }

  return { pickedItems, partStates };
}

async function applyAssemblyPickedCarryoverToSession(session, items = []) {
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
      `update ozon_employee_assembly_pick_items
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
      `update ozon_employee_assembly_sessions
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
        `update ozon_assembly_assignment_parts
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

async function applyAssemblyRedistributeCarryover(assemblyId, carryover, parts, sessions, assembly) {
  const pickedItems = Array.isArray(carryover?.pickedItems) ? carryover.pickedItems : [];
  if (!pickedItems.length) return;

  const postingNumbers = Array.from(
    new Set(
      pickedItems
        .map((item) => normalizeText(item.postingNumber))
        .filter(Boolean)
    )
  );
  if (!postingNumbers.length) return;

  const reassignedOrdersResult = await dbQuery(
    `select posting_number, assignment_part_id
     from ozon_assembly_orders
     where assembly_id = $1
       and posting_number = any($2)`,
    [assemblyId, postingNumbers]
  );
  const newPartIdByPostingNumber = new Map(
    (reassignedOrdersResult.rows || []).map((row) => [
      normalizeText(row.posting_number),
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
  for (const item of pickedItems) {
    const postingNumber = normalizeText(item.postingNumber);
    const newPartId = Number(newPartIdByPostingNumber.get(postingNumber) || 0);
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

  if (isStockMethodEnabled(assembly) && transferGroups.size > 0) {
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
        skipUnavailable: isAllowStockShortage(assembly),
        source: "ozon_redistribute",
        notes: `Ozon: перенос уже собранного товара после перераспределения. Сборка ${assemblyId}.`,
        negativeNotes: `Ozon: перенос уже собранного товара после перераспределения с уходом источника в минус. Сборка ${assemblyId}.`,
      });
      await recordStockShortageSkip({
        assemblyId,
        stage: "redistribute",
        article: group.article,
        barcode: group.barcode,
        requestedQuantity: group.qty,
        movementResult,
      });
    }
  }

  for (const session of Array.isArray(sessions) ? sessions : []) {
    const sessionId = Number(session?.id || 0);
    if (!sessionId || !carryoverBySessionId.has(sessionId)) continue;
    await applyAssemblyPickedCarryoverToSession(session, carryoverBySessionId.get(sessionId));
  }
}

async function setAssemblyEmployeeWorkflowMode(assemblyId, mode) {
  const normalizedMode = normalizeEmployeeWorkflowMode(mode);
  const result = await dbQuery(
    `update ozon_assemblies
     set employee_workflow_mode = $2,
         updated_at = now()
     where id = $1
     returning *`,
    [assemblyId, normalizedMode]
  );
  return result.rows?.[0] || null;
}

async function setAssemblyEmployeeWorkflowStage(assemblyId, stage, userId = null) {
  const normalizedStage = normalizeEmployeeWorkflowStage(stage);
  const pickingStartedAt = normalizedStage === EMPLOYEE_WORKFLOW_PICK_ACTIVE ? new Date() : null;
  const result = await dbQuery(
    `update ozon_assemblies
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
     where id = $1
     returning *`,
    [assemblyId, normalizedStage, pickingStartedAt, userId]
  );
  return result.rows?.[0] || null;
}

function buildEmployeeWorkflowState(assembly, session, activeSession, assignmentPart = null) {
  const workflowMode = normalizeEmployeeWorkflowMode(assembly?.employee_workflow_mode);
  const workflowStage = normalizeEmployeeWorkflowStage(assembly?.employee_workflow_stage);
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
    allowNoScan: assembly?.allow_no_scan === true,
    partialSafeScan: assembly?.partial_safe_scan === true,
    desktopPrint: assembly?.desktop_print === true,
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

async function ensureEmployeeSessionPickingStatus(assembly, session) {
  if (!session) return session;
  if (normalizeSessionRole(session.session_role) === SESSION_ROLE_PACK) return session;
  if (!isEmployeeProductPickingEnabled(assembly?.employee_workflow_stage)) return session;
  if (normalizePickingStatus(session.picking_status) !== PICKING_STATUS_NOT_REQUIRED) return session;
  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
     set picking_status = $2,
         updated_at = now()
     where id = $1
     returning *`,
    [session.id, PICKING_STATUS_PENDING]
  );
  return result.rows?.[0] || session;
}

async function startEmployeeProductPicking(assemblyId, userId) {
  const openSession = await getEmployeeOpenAssemblySession(assemblyId, userId);
  if (!openSession) {
    return { session: null, pickItems: [], error: "Сборка не назначена сотруднику" };
  }
  if (normalizeSessionRole(openSession.session_role) === SESSION_ROLE_PACK) {
    return { session: null, pickItems: [], error: "Для этого сотрудника доступна только сборка заказов" };
  }
  const assembly = await getAssembly(assemblyId);
  const workflowStage = normalizeEmployeeWorkflowStage(assembly?.employee_workflow_stage);
  if (!isEmployeeProductPickingEnabled(workflowStage)) {
    return { session: openSession, pickItems: [], error: "Для этой сборки сборка товара не требуется" };
  }
  if (!isEmployeeProductPickingActive(workflowStage)) {
    return { session: null, pickItems: [], error: "Администратор ещё не запустил сборку товара" };
  }
  if (normalizePickingStatus(openSession.picking_status) === PICKING_STATUS_FINISHED && openSession.picking_finished_at) {
    return { session: openSession, pickItems: [], error: null };
  }
  if (normalizePickingStatus(openSession.picking_status) === PICKING_STATUS_STARTED && openSession.picking_started_at && !openSession.picking_finished_at) {
    const pickItems = await ensureEmployeeProductPickItems(openSession);
    const refreshed = (await getEmployeeOpenAssemblySession(assemblyId, userId)) || openSession;
    return { session: refreshed, pickItems, error: null };
  }
  // Категоричное правило: нельзя начать сборку товара, пока не загружены ВСЕ этикетки заказов.
  const labelStat = await dbQuery(
    `select count(*)::int as total,
            count(*) filter (where coalesce(label_s3_key, '') <> '')::int as ready
     from ozon_assembly_orders
     where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
    [assemblyId]
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
  // Разрешаем начинать сборку товара по любой назначенной сборке (несколько параллельно).
  const updated = await dbQuery(
    `update ozon_employee_assembly_sessions
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
      `update ozon_assembly_assignment_parts
       set picker_started_at = coalesce(picker_started_at, now()),
           updated_at = now()
       where id = $1`,
      [Number(session.assignment_part_id)]
    );
  }
  const pickItems = await ensureEmployeeProductPickItems(session);
  const refreshed = (await getEmployeeOpenAssemblySession(assemblyId, userId)) || session;
  return { session: refreshed, pickItems, error: null };
}

async function finishEmployeeProductPicking(assemblyId, userId) {
  const session = await getEmployeeOpenAssemblySession(assemblyId, userId);
  if (!session) {
    return { session: null, error: "Сборка не назначена сотруднику" };
  }
  if (normalizeSessionRole(session.session_role) === SESSION_ROLE_PACK) {
    return { session: null, error: "Для этого сотрудника доступна только сборка заказов" };
  }
  if (
    normalizePickingStatus(session.picking_status) !== PICKING_STATUS_STARTED ||
    !session.picking_started_at ||
    session.picking_finished_at
  ) {
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
    `update ozon_employee_assembly_sessions
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
      `update ozon_assembly_assignment_parts
       set picker_finished_at = coalesce(picker_finished_at, now()),
           updated_at = now()
       where id = $1`,
      [Number(finishedSession.assignment_part_id)]
    );
  }
  return { session: finishedSession, error: null };
}

async function startEmployeeAssemblySession(assemblyId, userId) {
  const openSession = await getEmployeeOpenAssemblySession(assemblyId, userId);
  if (!openSession) {
    return { session: null, error: "Сборка не назначена сотруднику" };
  }
  if (openSession.started_at) {
    return { session: openSession, error: null };
  }
  // Разрешаем сотруднику работать несколько назначенных сборок (переключение между ними).
  const assembly = await getAssembly(assemblyId);
  const assignmentPart = await getAssemblyAssignmentPartForSession(openSession);
  if (normalizeSessionRole(openSession.session_role) === SESSION_ROLE_PACK && assignmentPart) {
    const handoffResult = await performAssemblyPartHandoffToPacker(assemblyId, openSession, assignmentPart, assembly);
    if (!handoffResult.ok) {
      return { session: null, error: handoffResult.error || "Не удалось передать товар упаковщику" };
    }
  }
  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
     set status = 'started', started_at = now(), updated_at = now()
     where id = $1
     returning *`,
    [openSession.id]
  );
  return { session: result.rows?.[0] || openSession, error: null };
}

async function confirmEmployeeAssemblyTablePlacement(assemblyId, userId) {
  const session = await getEmployeeOpenAssemblySession(assemblyId, userId);
  if (!session || normalizeSessionRole(session.session_role) !== SESSION_ROLE_PICK || Number(session.assignment_part_id || 0) < 1) {
    return { session: null, part: null, error: "Подтверждение стола недоступно" };
  }
  const part = await getAssemblyAssignmentPartById(session.assignment_part_id);
  if (!isSplitAssignmentPart(part)) {
    return { session, part, error: "Подтверждение стола нужно только для раздельной сборки" };
  }
  if (normalizePickingStatus(session.picking_status) !== PICKING_STATUS_FINISHED || !session.picking_finished_at) {
    return { session, part, error: "Сначала завершите сборку товара" };
  }
  if (normalizeHandoffStatus(part.handoffStatus) === HANDOFF_STATUS_READY) {
    return { session, part, error: null };
  }
  await dbQuery(
    `update ozon_assembly_assignment_parts
     set handoff_status = $2,
         placed_on_table_at = coalesce(placed_on_table_at, now()),
         updated_at = now()
     where id = $1`,
    [Number(part.id), HANDOFF_STATUS_READY]
  );
  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
     set status = 'finished',
         finished_at = coalesce(finished_at, now()),
         updated_at = now()
     where id = $1
     returning *`,
    [Number(session.id)]
  );
  return {
    session: result.rows?.[0] || session,
    part: await getAssemblyAssignmentPartById(part.id),
    error: null,
  };
}

async function performAssemblyPartHandoffToPacker(assemblyId, session, assignmentPart, assembly) {
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
    `update ozon_assembly_assignment_parts
     set transfer_started_at = coalesce(transfer_started_at, now()),
         updated_at = now()
     where id = $1`,
    [Number(assignmentPart.id)]
  );
  if (isStockMethodEnabled(assembly)) {
    const fromExternalEmployeeId = await getUserExternalEmployeeId(assignmentPart.pickerUserId);
    const toExternalEmployeeId = await getUserExternalEmployeeId(session.user_id);
    if (!fromExternalEmployeeId || !toExternalEmployeeId) {
      return { ok: false, error: "Сотрудник не привязан к складской системе" };
    }
    const itemsResult = await dbQuery(
      `select i.offer_id as article,
              i.barcode,
              coalesce(sum(greatest(coalesce(i.quantity, 1), 1)), 0)::int as qty
       from ozon_assembly_orders o
       join ozon_assembly_order_items i
         on i.assembly_id = o.assembly_id
        and i.posting_number = o.posting_number
       where o.assembly_id = $1
         and o.assignment_part_id = $2
         and o.collected_at is null
         and o.sent_to_delivering_at is null
       group by i.offer_id, i.barcode
       order by i.offer_id asc nulls last, i.barcode asc nulls last`,
      [assemblyId, Number(assignmentPart.id)]
    );
    for (const row of itemsResult.rows || []) {
      const movementResult = await transferEmployeeInventory({
        fromExternalEmployeeId,
        toExternalEmployeeId,
        barcodes: [row.barcode],
        article: row.article,
        allowArticleFallback: true,
        quantity: Math.max(1, Number(row.qty || 0)),
        skipUnavailable: isAllowStockShortage(assembly),
        source: "ozon_handoff",
        notes: `Ozon: передача товара между сотрудниками. Сборка ${assemblyId}, часть ${assignmentPart.partNo}, стол ${assignmentPart.tableNo || "-"}.`,
        negativeNotes: `Ozon: передача товара между сотрудниками с уходом источника в минус. Сборка ${assemblyId}, часть ${assignmentPart.partNo}, стол ${assignmentPart.tableNo || "-"}.`,
      });
      await recordStockShortageSkip({
        assemblyId,
        stage: "handoff",
        article: row.article,
        barcode: row.barcode,
        requestedQuantity: row.qty,
        movementResult,
        userId: session.user_id,
      });
    }
    writeSborkaLiveEventSafe({
      externalEmployeeId: toExternalEmployeeId,
      eventType: "handoff",
      orderId: String(assemblyId),
      productName: `Сборка ${assemblyId}`,
      barcode: null,
      quantity: 1,
      marketplace: "oz",
    });
  }
  await dbQuery(
    `update ozon_assembly_orders
     set assigned_user_id = $3,
         assigned_at = now(),
         updated_at = now()
     where assembly_id = $1
       and assignment_part_id = $2
       and collected_at is null
       and sent_to_delivering_at is null`,
    [assemblyId, Number(assignmentPart.id), Number(session.user_id)]
  );
  await dbQuery(
    `update ozon_assembly_assignment_parts
     set handoff_status = $2,
         transferred_at = coalesce(transferred_at, now()),
         updated_at = now()
     where id = $1`,
    [Number(assignmentPart.id), HANDOFF_STATUS_TRANSFERRED]
  );
  return { ok: true, part: await getAssemblyAssignmentPartById(assignmentPart.id) };
}

async function recordCollectedOrderForAssemblySession(sessionId, collectedAt = new Date()) {
  await dbQuery(
    `update ozon_employee_assembly_sessions
     set collected_order_count = collected_order_count + 1,
         first_collected_at = coalesce(first_collected_at, $2),
         last_collected_at = $2,
         updated_at = now()
     where id = $1`,
    [sessionId, collectedAt]
  );
}

async function finalizeEmployeeAssemblySessionIfCompleted(assemblyId, userId) {
  const session = await getEmployeeStartedAssemblySessionForAssembly(assemblyId, userId);
  if (!session) return null;
  const assembly = await getAssembly(assemblyId);
  if (!assembly) return null;
  const remainingResult = await dbQuery(
    `select count(*) as remaining
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is null
       and sent_to_delivering_at is null
       ${Number(session.assignment_part_id || 0) > 0 ? "and assignment_part_id = $2" : assembly.access_mode === "selected_split" ? "and assigned_user_id = $2" : ""}`,
    Number(session.assignment_part_id || 0) > 0
      ? [assemblyId, Number(session.assignment_part_id)]
      : assembly.access_mode === "selected_split"
        ? [assemblyId, userId]
        : [assemblyId]
  );
  const remaining = Number(remainingResult.rows?.[0]?.remaining || 0);
  if (remaining > 0) return null;
  const result = await dbQuery(
    `update ozon_employee_assembly_sessions
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

async function reconcileAssemblyAccessAfterEmployeeCompletion(assemblyId, userId) {
  const assembly = await getAssembly(assemblyId);
  if (!assembly || assembly.access_mode !== "selected_split") {
    return { removedUser: false, hiddenAssembly: false };
  }

  const ownRemainingResult = await dbQuery(
    `select count(*)::int as remaining
     from ozon_assembly_orders
     where assembly_id = $1 and assigned_user_id = $2 and collected_at is null and sent_to_delivering_at is null`,
    [assemblyId, userId]
  );
  const ownRemaining = Number(ownRemainingResult.rows?.[0]?.remaining || 0);
  let removedUser = false;
  if (ownRemaining < 1) {
    await dbQuery(
      "delete from ozon_assembly_access_users where assembly_id = $1 and user_id = $2",
      [assemblyId, userId]
    );
    removedUser = true;
  }

  const assemblyRemainingResult = await dbQuery(
    `select count(*)::int as remaining
     from ozon_assembly_orders
     where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
    [assemblyId]
  );
  const assemblyRemaining = Number(assemblyRemainingResult.rows?.[0]?.remaining || 0);
  if (assemblyRemaining > 0) {
    return { removedUser, hiddenAssembly: false };
  }

  await dbQuery("delete from ozon_assembly_access_users where assembly_id = $1", [assemblyId]);
  await setAssemblyAccessMode(assemblyId, "hidden");
  await closeOpenEmployeeSessionsForAssembly(assemblyId, "finished");
  return { removedUser, hiddenAssembly: true };
}

async function reconcileAssemblyAccessAfterExternalSync(assemblyId) {
  const assembly = await getAssembly(assemblyId);
  if (!assembly || assembly.access_mode !== "selected_split") {
    return { removedUsers: 0, hiddenAssembly: false };
  }

  const removedResult = await dbQuery(
    `delete from ozon_assembly_access_users a
     where a.assembly_id = $1
       and not exists (
         select 1
         from ozon_assembly_orders o
         where o.assembly_id = a.assembly_id
           and o.assigned_user_id = a.user_id
           and o.collected_at is null
           and o.sent_to_delivering_at is null
       )
     returning user_id`,
    [assemblyId]
  );
  const removedUsers = Number(removedResult.rowCount || 0);

  const remainingResult = await dbQuery(
    `select count(*)::int as remaining
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is null
       and sent_to_delivering_at is null`,
    [assemblyId]
  );
  const remaining = Number(remainingResult.rows?.[0]?.remaining || 0);
  if (remaining > 0) {
    return { removedUsers, hiddenAssembly: false };
  }

  await dbQuery("delete from ozon_assembly_access_users where assembly_id = $1", [assemblyId]);
  await setAssemblyAccessMode(assemblyId, "hidden");
  await closeOpenEmployeeSessionsForAssembly(assemblyId, "finished");
  return { removedUsers, hiddenAssembly: true };
}

async function getEmployeeAssemblyCounts(assemblyId, userId, accessMode, assignmentPartId = null) {
  const normalizedPartId = Number(assignmentPartId || 0);
  const result = await dbQuery(
    `select count(*) as total,
             count(*) filter (where collected_at is not null) as collected,
             count(*) filter (where collected_at is null) as remaining
     from ozon_assembly_orders
     where assembly_id = $1
       and sent_to_delivering_at is null
       ${normalizedPartId > 0 ? "and assignment_part_id = $2" : accessMode === "selected_split" ? "and assigned_user_id = $2" : ""}`,
    normalizedPartId > 0 ? [assemblyId, normalizedPartId] : accessMode === "selected_split" ? [assemblyId, userId] : [assemblyId]
  );
  return {
    total: Number(result.rows?.[0]?.total || 0),
    collected: Number(result.rows?.[0]?.collected || 0),
    remaining: Number(result.rows?.[0]?.remaining || 0),
  };
}

async function getEmployeeAssemblyContext(assemblyId, auth, options = {}) {
  const requireStartedSession = options.requireStartedSession === true;
  const check = await requireEmployeeAssemblyAccess(auth, assemblyId);
  if (!check.ok) return { errorStatus: check.status, error: check.error };
  scheduleAssemblyOrdersSyncBackground(assemblyId, check.assembly.store_id || null, { maxAgeMs: 12000 });
  let session = await getEmployeeOpenAssemblySession(assemblyId, auth.id);
  let assignmentPart = await getAssemblyAssignmentPartForSession(session);
  const hasAssignmentParts = !session ? await assemblyHasAssignmentParts(assemblyId) : Number(session.assignment_part_id || 0) > 0;
  let counts = await getEmployeeAssemblyCounts(
    assemblyId,
    auth.id,
    check.assembly.access_mode,
    session?.assignment_part_id != null ? Number(session.assignment_part_id) : null
  );
  if (counts.total < 1 && !session) {
    return { errorStatus: 404, error: "Сборка недоступна" };
  }
  if (!session && counts.total > 0 && !hasAssignmentParts) {
    session = await ensureEmployeeAssignmentSessionForAssembly(assemblyId, auth.id, counts.remaining);
    assignmentPart = await getAssemblyAssignmentPartForSession(session);
    counts = await getEmployeeAssemblyCounts(
      assemblyId,
      auth.id,
      check.assembly.access_mode,
      session?.assignment_part_id != null ? Number(session.assignment_part_id) : null
    );
  }
  if (!session && hasAssignmentParts) {
    return { errorStatus: 404, error: "Сборка недоступна" };
  }
  session = await ensureEmployeeSessionPickingStatus(check.assembly, session);
  const activeSession = await getEmployeeActiveAssemblySession(auth.id);
  assignmentPart = assignmentPart || (await getAssemblyAssignmentPartForSession(session));
  const workflow = buildEmployeeWorkflowState(check.assembly, session, activeSession, assignmentPart);
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
  return {
    assembly: check.assembly,
    counts,
    session,
    activeSession,
    workflow,
    assignmentPart,
    canStart: Boolean(workflow.canStartPicking || workflow.canStartPacking),
    canWork: workflow.canWork,
    hasAnotherActiveSession:
      Boolean(activeSession) && String(activeSession.assembly_id) !== String(assemblyId),
  };
}

async function getEmployeeAssemblyLabelScanContext(assemblyId, auth) {
  const check = await requireEmployeeAssemblyAccess(auth, assemblyId);
  if (!check.ok) return { errorStatus: check.status, error: check.error };
  let session = await getEmployeeStartedAssemblySessionForAssembly(assemblyId, auth.id);
  if (!session) {
    session = await getEmployeeOpenAssemblySession(assemblyId, auth.id);
  }
  session = await ensureEmployeeSessionPickingStatus(check.assembly, session);
  const assignmentPart = await getAssemblyAssignmentPartForSession(session);
  const workflow = buildEmployeeWorkflowState(check.assembly, session, session || null, assignmentPart);
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
  return {
    assembly: check.assembly,
    session,
    workflow,
    assignmentPart,
    canWork: true,
  };
}

async function getAcceptedPickScanBarcodesForItem(item, storeId = null, options = {}) {
  const article = normalizeArticleKey(item?.article);
  const force = options?.force === true;
  if (article) {
    const articleCacheKey = buildAcceptedPickBarcodesCacheKey({
      article,
      storeId,
      sku: item?.sku,
    });
    const articleCached = getCachedAcceptedPickBarcodes(articleCacheKey, force);
    if (articleCached) {
      return articleCached;
    }
    const accepted = await getAcceptedScanBarcodesForOffer(article, storeId, { sku: item?.sku, force });
    const finalBarcodes = uniqueTextList([...(accepted?.acceptedBarcodes || []), ...(accepted?.sourceBarcodes || [])]);
    setCachedAcceptedPickBarcodes(articleCacheKey, finalBarcodes);
    return finalBarcodes;
  }
  const cacheKey = buildAcceptedPickBarcodesCacheKey({
    storeId,
    sku: item?.sku,
    barcode: item?.barcode,
  });
  const cached = getCachedAcceptedPickBarcodes(cacheKey, force);
  if (cached) {
    return cached;
  }
  const sourceBarcodes = uniqueTextList([
    item?.barcode || null,
    ...buildOzonMarketplaceBarcodeCandidates(item?.sku),
  ]);
  const acceptedBarcodes = [...sourceBarcodes];
  for (const sourceBarcode of sourceBarcodes) {
    try {
      const status = await getMsProductStatusByBarcode(sourceBarcode, force ? { force: true } : {});
      acceptedBarcodes.push(...(status?.barcodes || []));
      continue;
    } catch {}
    try {
      const fresh = await getMsProductStatusByBarcode(sourceBarcode, { force: true });
      acceptedBarcodes.push(...(fresh?.barcodes || []));
    } catch {}
  }
  const finalBarcodes = uniqueTextList(acceptedBarcodes);
  setCachedAcceptedPickBarcodes(cacheKey, finalBarcodes);
  return finalBarcodes;
}

async function buildEmployeePickScanIndex(session, sourceItems = null, options = {}) {
  const cacheKey = getEmployeePickScanIndexKey(session);
  const itemsById = new Map();
  const acceptedMatches = new Map();
  const movementHintsByItemId = new Map();
  if (!cacheKey) {
    return { ts: Date.now(), acceptedMatches, itemsById, movementHintsByItemId, loading: null };
  }
  const force = options?.force === true;
  const sourceList = Array.isArray(sourceItems) ? sourceItems : await listEmployeePickItemsForSession(Number(session.id));
  const pendingSourceItems = sourceList.filter((item) => isPendingEmployeePickItem(item));
  const items = await enrichEmployeePickItemsWithMsCells(pendingSourceItems, options);
  if (!items.length) {
    return { ts: Date.now(), acceptedMatches, itemsById, movementHintsByItemId, loading: null };
  }

  const sourceEntries = new Map();
  for (const item of items) {
    const itemId = Number(item?.id || 0);
    if (Number.isFinite(itemId) && itemId > 0) {
      itemsById.set(itemId, item);
    }
    const sourceKey =
      buildAcceptedPickBarcodesCacheKey({
        article: item?.article,
        storeId: session?.store_id || null,
        sku: item?.sku,
        barcode: item?.barcode,
      }) || `item:${itemId || 0}`;
    if (!sourceEntries.has(sourceKey)) {
      sourceEntries.set(sourceKey, { item, accepted: [] });
    }
  }

  await mapWithConcurrency([...sourceEntries.entries()], EMPLOYEE_PICK_SCAN_INDEX_CONCURRENCY, async ([sourceKey, entry]) => {
    const accepted = await getAcceptedPickScanBarcodesForItem(entry.item, session?.store_id || null, { force });
    const movementHints = await buildOzonPickItemMovementHints(entry.item, { ...options, force });
    sourceEntries.set(sourceKey, { ...entry, accepted, movementHints });
    return accepted;
  });

  for (const item of items) {
    const itemId = Number(item?.id || 0);
    const sourceKey =
      buildAcceptedPickBarcodesCacheKey({
        article: item?.article,
        storeId: session?.store_id || null,
        sku: item?.sku,
        barcode: item?.barcode,
      }) || `item:${itemId || 0}`;
    const entry = sourceEntries.get(sourceKey);
    for (const barcode of uniqueTextList(entry?.accepted || [])) {
      if (!acceptedMatches.has(barcode)) {
        acceptedMatches.set(barcode, []);
      }
      acceptedMatches.get(barcode).push(item);
    }
    if (Number.isFinite(itemId) && itemId > 0) {
      movementHintsByItemId.set(itemId, entry?.movementHints || { productIds: [], preferredShelfId: null });
    }
  }

  return {
    ts: Date.now(),
    acceptedMatches,
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
    ? { ts: 0, acceptedMatches: null, itemsById: null, movementHintsByItemId: null, loading: null }
    : cached || { ts: 0, acceptedMatches: null, itemsById: null, movementHintsByItemId: null, loading: null };
  const loading = buildEmployeePickScanIndex(session, sourceItems, { ...options, force })
    .then((built) => {
      const readyEntry = {
        ts: Date.now(),
        acceptedMatches: built?.acceptedMatches || new Map(),
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

async function findEmployeePickItemMatchesByBarcode(session, barcode, options = {}) {
  const forceFresh = options?.forceFresh === true;
  const normalizedBarcode = normalizeBarcode(barcode);
  if (!normalizedBarcode) {
    return { matches: [], lookupFailed: false, ambiguous: false, multipleScanProducts: false, scanProducts: [] };
  }
  const pickItems = Array.isArray(session?.pick_items)
    ? session.pick_items
    : session?.id
      ? await listEmployeePickItemsForSession(session.id)
      : [];
  const pendingItems = (Array.isArray(pickItems) ? pickItems : []).filter(
    (item) => Number(item.picked_qty || item.pickedQty || 0) < Number(item.required_qty || item.requiredQty || 0)
  );
  if (!pendingItems.length) {
    return { matches: [], lookupFailed: false, ambiguous: false, multipleScanProducts: false, scanProducts: [] };
  }
  if (!forceFresh) {
    const warmingEntry = getCachedEmployeePickScanIndexEntry(getEmployeePickScanIndexKey(session));
    if (warmingEntry?.loading) {
      try {
        await warmingEntry.loading;
      } catch {}
    }
  }

  const directIndexMatch = forceFresh ? null : getEmployeePickScanIndexMatches(session, [normalizedBarcode]);
  if (directIndexMatch?.matches?.length) {
    const orderedDirectIndexMatches = await sortEmployeePickMatchRows(directIndexMatch.matches, options);
    return {
      matches: orderedDirectIndexMatches,
      lookupFailed: false,
      ambiguous: directIndexMatch.ambiguous,
      multipleScanProducts: false,
    };
  }

  const directMatches = pendingItems.filter((item) => normalizeBarcode(item?.barcode) === normalizedBarcode);
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

  const scanLookup = await getScanLookupBarcodes(normalizedBarcode, forceFresh ? { force: true } : {});
  let lookupFailed = scanLookup.lookupFailed === true;
  let multipleScanProducts = scanLookup.multiple === true;
  const scanProducts = Array.isArray(scanLookup.matches) ? scanLookup.matches : [];
  const lookupBarcodes = new Set(
    scanLookup.multiple
      ? [normalizedBarcode]
      : uniqueTextList([
          normalizedBarcode,
          ...(scanLookup.barcodes || []),
          ...((Array.isArray(scanLookup.matches) ? scanLookup.matches : []).flatMap((match) => match?.barcodes || [])),
        ])
  );

  const indexedLookupMatch = forceFresh
    ? null
    : getEmployeePickScanIndexMatches(session, [...lookupBarcodes]);
  if (indexedLookupMatch?.matches?.length) {
    const orderedIndexedMatches = await sortEmployeePickMatchRows(indexedLookupMatch.matches, options);
    return {
      matches: orderedIndexedMatches,
      lookupFailed,
      ambiguous: indexedLookupMatch.ambiguous,
      multipleScanProducts,
      scanProducts,
    };
  }

  const fastMatches = pendingItems.filter((item) => {
    const itemBarcode = normalizeBarcode(item?.barcode);
    return itemBarcode && lookupBarcodes.has(itemBarcode);
  });
  if (fastMatches.length) {
    const orderedFastMatches = await sortEmployeePickMatchRows(fastMatches, options);
    const fastKeys = new Set(orderedFastMatches.map((item) => item.item_key || String(item.id)));
    return {
      matches: orderedFastMatches,
      lookupFailed,
      ambiguous: fastKeys.size > 1,
      multipleScanProducts,
      scanProducts,
    };
  }

  const scanCandidateBarcodes = scanLookup.multiple
    ? [normalizedBarcode]
    : uniqueTextList([normalizedBarcode, ...(scanLookup.barcodes || [])]);
  const acceptedCache = new Map();
  const itemChecks = await mapWithConcurrency(
    pendingItems,
    6,
    async (item) => {
      const cacheKey = `${normalizeArticleKey(item?.article) || ""}||${normalizeBarcode(item?.barcode) || ""}||${normalizeBarcode(item?.sku) || ""}`;
      let accepted = acceptedCache.get(cacheKey);
      if (!accepted) {
        accepted = await getAcceptedPickScanBarcodesForItem(item, session?.store_id || null, {
          force: forceFresh,
        });
        acceptedCache.set(cacheKey, accepted);
      }
      return { item, accepted };
    }
  );

  const matches = [];
  const itemKeys = new Set();
  for (const { item, accepted } of itemChecks) {
    const candidateBarcodes = uniqueTextList([item?.barcode, ...(accepted || [])]);
    if (!candidateBarcodes.length) continue;
    const matched = candidateBarcodes.some((value) => scanCandidateBarcodes.includes(value));
    if (!matched) continue;
    matches.push(item);
    itemKeys.add(item.item_key || String(item.id));
  }
  const sorted = await sortEmployeePickMatchRows(matches);
  return {
    matches: sorted,
    lookupFailed,
    ambiguous: itemKeys.size > 1,
    multipleScanProducts,
    scanProducts,
  };
}

async function listOpenAssemblyScanIssues(storeId = null) {
  const store = resolveStoreStrict(storeId);
  const params = [];
  let where = "where i.status = 'open'";
  if (store) {
    params.push(store.id);
    where += " and a.store_id = $1";
  }
  const result = await dbQuery(
    `select i.*,
            u.surname as employee_surname,
            u.name as employee_name,
            a.name as assembly_name,
            a.store_id as assembly_store_id,
            greatest(coalesce(pi.required_qty, 0) - coalesce(pi.picked_qty, 0), 0)::int as live_remaining_quantity
     from ozon_employee_scan_issues i
     join users u on u.id = i.employee_id
     join ozon_assemblies a on a.id = i.assembly_id
     left join ozon_employee_assembly_pick_items pi on pi.id = i.pick_item_id
     ${where}
     order by i.created_at desc, i.id desc`,
    params
  );
  return (result.rows || []).map(toOzonScanIssuePayload);
}

async function getOpenAssemblyScanIssueById(issueId, storeId = null) {
  const store = resolveStoreStrict(storeId);
  const params = [issueId];
  let where = "where i.id = $1 and i.status = 'open'";
  if (store) {
    params.push(store.id);
    where += " and a.store_id = $2";
  }
  const result = await dbQuery(
    `select i.*,
            a.store_id as assembly_store_id
     from ozon_employee_scan_issues i
     join ozon_assemblies a on a.id = i.assembly_id
     ${where}
     limit 1`,
    params
  );
  return result.rows?.[0] || null;
}

async function buildIssueMarketplaceBarcodesForAlias(issue, storeId = null) {
  const issueBarcodes = uniqueTextList(
    (Array.isArray(issue?.product_barcodes) ? issue.product_barcodes : []).filter((value) =>
      /^OZN/i.test(String(value || "").trim())
    )
  );
  const articleBarcodes = [];
  for (const article of Array.isArray(issue?.item_articles) ? issue.item_articles : []) {
    const current = await getOfferBarcodesWithFallback(article, [storeId], {
      preferMarketplaceOnly: true,
      forceLive: issueBarcodes.length === 0,
    });
    articleBarcodes.push(...current);
  }
  return uniqueTextList([...issueBarcodes, ...articleBarcodes]);
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

async function createOrUpdateAssemblyScanIssue({
  assemblyId,
  postingNumber = null,
  employeeId,
  lineNos = [],
  itemArticles = [],
  itemNames = [],
  productBarcodes = [],
  scannedBarcode,
  issueStage = SCAN_ISSUE_STAGE_ORDER,
  pickItemId = null,
  pickItemKey = null,
  errorMessage = null,
  contextNote = null,
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
     from ozon_employee_scan_issues
     where assembly_id = $1
       and posting_number is not distinct from $2
       and pick_item_id is not distinct from $3
       and employee_id = $4
       and issue_stage = $5
       and status = 'open'
     order by created_at desc, id desc
     limit 1`,
    [assemblyId, postingNumber || null, pickItemId, employeeId, normalizedStage]
  );
  const normalizedLineNos = uniqueNumberList(lineNos);
  const normalizedArticles = Array.isArray(itemArticles) ? itemArticles.filter(Boolean) : [];
  const normalizedNames = uniqueTextList(itemNames);
  const normalizedBarcodes = uniqueTextList(productBarcodes);
  if (existing.rows?.[0]?.id) {
    const result = await dbQuery(
      `update ozon_employee_scan_issues
       set line_nos = $2,
           item_articles = $3,
           item_names = $4,
           product_barcodes = $5,
           scanned_barcode = $6,
           pick_item_key = $7,
           error_message = $8,
           context_note = $9,
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
        normalizedLineNos,
        normalizedArticles,
        normalizedNames,
        normalizedBarcodes,
        scannedBarcode || null,
        pickItemKey || null,
        errorMessage || null,
        contextNote || null,
        originalScannedBarcode || scannedBarcode || null,
        confirmationScannedBarcode || scannedBarcode || null,
        marketplaceArticle || normalizedArticles[0] || null,
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
    `insert into ozon_employee_scan_issues
      (assembly_id, posting_number, employee_id, issue_stage, pick_item_id, pick_item_key, error_message, context_note, line_nos, item_articles, item_names, product_barcodes, scanned_barcode,
       original_scanned_barcode, confirmation_scanned_barcode, marketplace_article, warehouse_product_id, warehouse_article, warehouse_product_name, mapping_barcode, image_url, remaining_quantity, created_at, updated_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, now(), now())
     returning *`,
    [
      assemblyId,
      postingNumber || null,
      employeeId,
      normalizedStage,
      pickItemId,
      pickItemKey || null,
      errorMessage || null,
      contextNote || null,
      normalizedLineNos,
      normalizedArticles,
      normalizedNames,
      normalizedBarcodes,
      scannedBarcode || null,
      originalScannedBarcode || scannedBarcode || null,
      confirmationScannedBarcode || scannedBarcode || null,
      marketplaceArticle || normalizedArticles[0] || null,
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

async function buildAssemblyOrderIssueDetails(assemblyId, postingNumber, storeId = null) {
  const itemsResult = await dbQuery(
    `select line_no, offer_id, sku, name
     from ozon_assembly_order_items
     where assembly_id = $1 and posting_number = $2
     order by line_no asc`,
    [assemblyId, postingNumber]
  );
  const lineNos = (itemsResult.rows || []).map((row) => Number(row.line_no)).filter((value) => Number.isFinite(value));
  const itemArticles = [];
  const itemNames = [];
  const productBarcodes = [];
  for (const row of itemsResult.rows || []) {
    const article = normalizeArticleKey(row.offer_id);
    if (article && !itemArticles.includes(article)) itemArticles.push(article);
    const name = String(row.name || "").trim();
    if (name && !itemNames.includes(name)) itemNames.push(name);
    const accepted = await getAcceptedScanBarcodesForOffer(article, storeId, { sku: row.sku });
    productBarcodes.push(...accepted.acceptedBarcodes);
  }
  return {
    lineNos: uniqueNumberList(lineNos),
    itemArticles,
    itemNames,
    productBarcodes: uniqueTextList(productBarcodes),
  };
}

async function resolveAssemblyScanIssue(issueId, adminId, status, options = {}) {
  const result = await dbQuery(
    `update ozon_employee_scan_issues
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

function toCollectorAssemblySessionPayload(row) {
  if (!row) return null;
  const totalSecondsStored = Number(row.total_seconds || 0);
  const startedAt = row.started_at ? new Date(row.started_at).getTime() : NaN;
  const isRunning = row.status === "started" && !row.finished_at && Number.isFinite(startedAt);
  const liveSeconds = isRunning ? Math.max(0, Math.round((Date.now() - startedAt) / 1000)) : 0;
  return {
    id: Number(row.id),
    assemblyId: row.assembly_id,
    status: row.status || "started",
    startedAt: row.started_at || null,
    finishedAt: row.finished_at || null,
    totalSeconds: isRunning ? Math.max(totalSecondsStored, liveSeconds) : totalSecondsStored,
    orderCount: Number(row.order_count || 0),
    itemCount: Number(row.item_count || 0),
    assemblyName: row.assembly_name || null,
    storeId: row.store_id || null,
    storeName: row.store_name || null,
  };
}

async function getCollectorOpenAssemblySession(assemblyId, userId) {
  const result = await dbQuery(
    `select *
     from ozon_collector_assembly_sessions
     where assembly_id = $1 and user_id = $2 and status = 'started' and finished_at is null
     order by started_at desc, id desc
     limit 1`,
    [assemblyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getLatestCollectorAssemblySession(assemblyId, userId) {
  const result = await dbQuery(
    `select *
     from ozon_collector_assembly_sessions
     where assembly_id = $1 and user_id = $2
     order by started_at desc, id desc
     limit 1`,
    [assemblyId, userId]
  );
  return result.rows?.[0] || null;
}

async function getCollectorAssemblyTotals(assemblyId) {
  const result = await dbQuery(
    `with active_orders as (
       select posting_number, assigned_user_id
       from ozon_assembly_orders
       where assembly_id = $1
         and assigned_user_id is not null
         and collected_at is null
         and sent_to_delivering_at is null
     )
     select count(*)::int as orders,
            count(distinct assigned_user_id)::int as employees,
            coalesce((
              select sum(greatest(coalesce(i.quantity, 1), 1))::bigint
              from ozon_assembly_order_items i
              join active_orders ao
                on ao.posting_number = i.posting_number
              where i.assembly_id = $1
            ), 0)::bigint as items
     from active_orders`,
    [assemblyId]
  );
  return {
    orders: Number(result.rows?.[0]?.orders || 0),
    employees: Number(result.rows?.[0]?.employees || 0),
    items: Number(result.rows?.[0]?.items || 0),
  };
}

async function ensureCollectorAssemblySession(assemblyId, userId) {
  const openSession = await getCollectorOpenAssemblySession(assemblyId, userId);
  if (openSession) {
    return { session: openSession, error: null };
  }
  const latest = await getLatestCollectorAssemblySession(assemblyId, userId);
  if (latest?.status === "finished") {
    return { session: null, error: "Сборка уже отмечена как собранная" };
  }
  const totals = await getCollectorAssemblyTotals(assemblyId);
  if (totals.orders < 1) {
    return { session: null, error: "Нет заказов для сборки товаров" };
  }
  const assembly = await getAssembly(assemblyId);
  const result = await dbQuery(
    `insert into ozon_collector_assembly_sessions
      (assembly_id, user_id, status, started_at, order_count, item_count, assembly_name, store_id, store_name, created_at, updated_at)
     values ($1, $2, 'started', now(), $3, $4, $5, $6, $7, now(), now())
     returning *`,
    [
      assemblyId,
      userId,
      totals.orders,
      totals.items,
      assembly?.name || null,
      assembly?.store_id || null,
      assembly?.store_name || null,
    ]
  );
  return { session: result.rows?.[0] || null, error: null };
}

async function finishCollectorAssemblySession(assemblyId, userId) {
  const openSession = await getCollectorOpenAssemblySession(assemblyId, userId);
  if (!openSession) {
    const latest = await getLatestCollectorAssemblySession(assemblyId, userId);
    if (latest?.status === "finished") {
      return { session: latest, error: null, alreadyFinished: true };
    }
    return { session: null, error: "Сессия сборщика не запущена" };
  }
  const result = await dbQuery(
    `update ozon_collector_assembly_sessions
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

async function getCollectorAssemblyContext(assemblyId, auth = null) {
  const employeeLocation = auth?.id ? await getConfiguredEmployeeLocation(auth.id) : null;
  if (employeeLocation?.error) {
    return { errorStatus: employeeLocation.errorStatus || 409, error: employeeLocation.error };
  }
  const assembly = await getAssembly(assemblyId);
  if (!assembly || assembly.access_mode !== "selected_split") {
    return { errorStatus: 404, error: "Сборка недоступна" };
  }
  const assemblyLocation = normalizeLocationCode(assembly.location_code);
  if (!assemblyLocation) {
    return { errorStatus: 409, error: "Сборка не настроена по городу. Обратитесь к администратору." };
  }
  if (employeeLocation?.locationCode && assemblyLocation !== employeeLocation.locationCode) {
    return { errorStatus: 404, error: "Сборка недоступна для вашего города" };
  }
  return { assembly };
}

async function getCollectorAssemblyStats(userId, rangeStart, rangeEnd) {
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
            count(distinct s.assembly_id)::int as assemblies,
            coalesce(sum(s.order_count), 0)::bigint as orders,
            coalesce(sum(s.item_count), 0)::bigint as items,
            coalesce(sum(${durationSql}), 0)::bigint as total_seconds
     from ozon_collector_assembly_sessions s
     where s.user_id = $1
       and s.started_at is not null
       and timezone($2, s.started_at)::date = timezone($2, now())::date`,
    [userId, MOSCOW_TZ]
  );

  const allTimeSummaryPromise = dbQuery(
    `select count(*)::int as sessions,
            count(distinct s.assembly_id)::int as assemblies,
            coalesce(sum(s.order_count), 0)::bigint as orders,
            coalesce(sum(s.item_count), 0)::bigint as items,
            coalesce(sum(${durationSql}), 0)::bigint as total_seconds
     from ozon_collector_assembly_sessions s
     where s.user_id = $1 and s.started_at is not null`,
    [userId]
  );

  const periodSummaryPromise = dbQuery(
    `select count(*)::int as sessions,
            count(distinct s.assembly_id)::int as assemblies,
            coalesce(sum(s.order_count), 0)::bigint as orders,
            coalesce(sum(s.item_count), 0)::bigint as items,
            coalesce(sum(${durationSql}), 0)::bigint as total_seconds
     from ozon_collector_assembly_sessions s
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
       from ozon_collector_assembly_sessions s
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
            s.assembly_id,
            s.assembly_name,
            s.store_id,
            s.store_name,
            s.status,
            s.started_at,
            s.finished_at,
            ${durationSql}::int as total_seconds,
            s.order_count,
            s.item_count
     from ozon_collector_assembly_sessions s
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
      assemblies: Number(row?.assemblies || 0),
      sessions: Number(row?.sessions || 0),
      orders,
      items,
      totalSeconds,
      avgSecondsPerOrder: orders > 0 ? totalSeconds / orders : 0,
      avgSecondsPerItem: items > 0 ? totalSeconds / items : 0,
    };
  };

  return {
    timezone: MOSCOW_TZ,
    stats: {
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
        assemblyId: row.assembly_id,
        assemblyName: row.assembly_name || null,
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
    },
  };
}

function toEmployeeStatsSummary(row) {
  const totalSeconds = Number(row?.total_seconds || 0);
  const packingSeconds = Number(row?.packing_seconds || 0);
  const pickingSeconds = Number(row?.picking_seconds || 0);
  const orders = Number(row?.orders || 0);
  const pickedItems = Number(row?.picked_items || 0);
  return {
    assemblies: Number(row?.assemblies || 0),
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

async function getEmployeeAssemblyStats(userId, rangeStart, rangeEnd) {
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
       from ozon_assembly_orders o
       where o.collected_by = $1
         and o.collected_at is not null
         and timezone($2, o.collected_at)::date = timezone($2, now())::date
     ),
     today_sessions as (
       select count(*) filter (
                where (s.started_at is not null and timezone($2, s.started_at)::date = timezone($2, now())::date)
                   or (s.picking_started_at is not null and timezone($2, s.picking_started_at)::date = timezone($2, now())::date)
              )::int as sessions,
              count(distinct s.assembly_id) filter (
                where (s.started_at is not null and timezone($2, s.started_at)::date = timezone($2, now())::date)
                   or (s.picking_started_at is not null and timezone($2, s.picking_started_at)::date = timezone($2, now())::date)
              )::int as assemblies,
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
       from ozon_employee_assembly_sessions s
       where s.user_id = $1
     )
     select today_sessions.assemblies,
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
    `select count(distinct s.assembly_id) filter (
              where s.started_at is not null or s.picking_started_at is not null
            )::int as assemblies,
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
     from ozon_employee_assembly_sessions s
     where s.user_id = $1`,
    [userId]
  );

  const periodSummaryPromise = dbQuery(
    `with period_orders as (
       select count(*) as orders
       from ozon_assembly_orders o
       where ${orderPeriodWhere}
     ),
     period_sessions as (
       select count(*) filter (
                where (s.started_at is not null and ${packingPeriodSql})
                   or (s.picking_started_at is not null and ${pickingPeriodSql})
              )::int as sessions,
              count(distinct s.assembly_id) filter (
                where (s.started_at is not null and ${packingPeriodSql})
                   or (s.picking_started_at is not null and ${pickingPeriodSql})
              )::int as assemblies,
              coalesce(sum(
                case when s.started_at is not null and ${packingPeriodSql} then ${packingDurationSql} else 0 end
              ), 0)::bigint as packing_seconds,
              coalesce(sum(
                case when s.picking_started_at is not null and ${pickingPeriodSql} then ${pickingDurationSql} else 0 end
              ), 0)::bigint as picking_seconds,
              coalesce(sum(
                case when s.picking_started_at is not null and ${pickingPeriodSql} then s.picking_collected_qty else 0 end
              ), 0)::bigint as picked_items
       from ozon_employee_assembly_sessions s
       where s.user_id = $1
     )
     select period_sessions.assemblies,
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
       from ozon_assembly_orders o
       where o.collected_by = $1
         and o.collected_at is not null
         ${hasRange ? "and o.collected_at >= $3 and o.collected_at < $4" : ""}
       group by 1
     ),
     day_packing as (
       select timezone($2, s.started_at)::date as day,
              coalesce(sum(${packingDurationSql}), 0)::bigint as packing_seconds
       from ozon_employee_assembly_sessions s
       where s.user_id = $1
         and s.started_at is not null
         ${hasRange ? "and s.started_at >= $3 and s.started_at < $4" : ""}
       group by 1
     ),
     day_picking as (
       select timezone($2, s.picking_started_at)::date as day,
              coalesce(sum(${pickingDurationSql}), 0)::bigint as picking_seconds
       from ozon_employee_assembly_sessions s
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
    `select s.id, s.assembly_id, a.name as assembly_name, a.store_name,
            s.assignment_mode, s.status, s.assigned_at, s.started_at, s.finished_at,
            s.assigned_order_count, s.collected_order_count,
            s.picking_status, s.picking_started_at, s.picking_finished_at,
            s.picking_required_qty, s.picking_collected_qty,
            ${packingDurationSql}::bigint as packing_total_seconds,
            ${pickingDurationSql}::bigint as picking_total_seconds,
            s.first_collected_at, s.last_collected_at,
            admin.surname as assigned_by_surname, admin.name as assigned_by_name
     from ozon_employee_assembly_sessions s
     left join ozon_assemblies a on a.id = s.assembly_id
     left join users admin on admin.id = s.assigned_by
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
    timezone: MOSCOW_TZ,
    stats: {
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
          assemblyId: row.assembly_id,
          assemblyName: row.assembly_name || null,
          storeName: row.store_name || null,
          assignedByName: row.assigned_by_surname
            ? `${row.assigned_by_surname} ${row.assigned_by_name || ""}`.trim()
            : null,
          assignmentMode: row.assignment_mode || "split",
          status: row.status || "assigned",
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
        })) || [],
    },
  };
}

async function getAssembly(assemblyId) {
  const result = await dbQuery(
    `select id, name, store_id, store_name, warehouse_id, warehouse_name, access_mode, labels_status, labels_total, labels_loaded, labels_error,
            labels_started_at, labels_finished_at,
            location_code, sklad_warehouse_id, sklad_warehouse_name,
            stock_method_enabled, allow_no_scan, partial_safe_scan, desktop_print, allow_stock_shortage, employee_workflow_mode, employee_workflow_stage,
            product_picking_started_at, product_picking_started_by,
            shipment_code, shipment_label_s3_key, shipment_label_generated_at, shipment_prepared_at,
            shipment_confirmed_at, shipment_confirmed_by, shipment_postings_count, shipment_posting_numbers,
            created_at, updated_at
     from ozon_assemblies
     where id = $1`,
    [assemblyId]
  );
  return result.rows?.[0] || null;
}

async function getAssemblyAccessUserIds(assemblyId) {
  const result = await dbQuery(
    "select user_id from ozon_assembly_access_users where assembly_id = $1 order by user_id",
    [assemblyId]
  );
  return result.rows.map((row) => Number(row.user_id));
}

async function setAssemblyAccessUsers(assemblyId, userIds) {
  await dbQuery("delete from ozon_assembly_access_users where assembly_id = $1", [assemblyId]);
  if (!userIds.length) return;
  const values = [];
  const placeholders = userIds.map((userId, idx) => {
    values.push(userId);
    return `($1, $${idx + 2})`;
  });
  await dbQuery(
    `insert into ozon_assembly_access_users (assembly_id, user_id) values ${placeholders.join(", ")}`,
    [assemblyId, ...values]
  );
}

async function setAssemblyAccessMode(assemblyId, mode) {
  await dbQuery("update ozon_assemblies set access_mode = $2, updated_at = now() where id = $1", [
    assemblyId,
    mode,
  ]);
}

async function setAssemblyStockMethodEnabled(assemblyId, enabled) {
  await dbQuery("update ozon_assemblies set stock_method_enabled = $2, updated_at = now() where id = $1", [
    assemblyId,
    enabled === true,
  ]);
}

async function splitAssemblyOrders(assemblyId, userIds, mode = "split") {
  if (!userIds.length) {
    return { assigned: 0, users: [] };
  }

  if (mode === "redistribute") {
    await dbQuery(
      `update ozon_assembly_orders
       set assigned_user_id = null, assigned_at = null
       where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
      [assemblyId]
    );
  }

  const queueResult = await dbQuery(
    `select posting_number
     from ozon_assembly_orders
     where assembly_id = $1
       and collected_at is null
       and sent_to_delivering_at is null
       ${mode === "split" ? "and assigned_user_id is null" : ""}
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
  );
  const queue = queueResult.rows.map((row) => row.posting_number);
  if (!queue.length) {
    return { assigned: 0, users: [] };
  }

  let cursor = 0;
  const perUser = new Map();
  for (const postingNumber of queue) {
    const userId = userIds[cursor % userIds.length];
    cursor += 1;
    if (!perUser.has(userId)) perUser.set(userId, []);
    perUser.get(userId).push(postingNumber);
  }

  for (const [userId, postings] of perUser.entries()) {
    await dbQuery(
      `update ozon_assembly_orders
       set assigned_user_id = $3, assigned_at = now(), updated_at = now()
       where assembly_id = $1 and posting_number = any($2::text[])`,
      [assemblyId, postings, userId]
    );
  }

  return {
    assigned: queue.length,
    users: Array.from(perUser.entries()).map(([userId, postings]) => ({
      userId: Number(userId),
      count: postings.length,
    })),
  };
}

async function ensureAssemblyExists(assemblyId, name, createdBy, accessMode = "hidden", store = null) {
  const result = await dbQuery(
    `insert into ozon_assemblies (
       id, name, created_by, access_mode, store_id, store_name,
       stock_method_enabled, allow_no_scan, employee_workflow_mode, employee_workflow_stage,
       created_at, updated_at
     )
     values ($1, $2, $3, $4, $5, $6, true, false, $7, $8, now(), now())
     on conflict (id)
     do update set
       name = excluded.name,
       store_id = coalesce(excluded.store_id, ozon_assemblies.store_id),
       store_name = coalesce(excluded.store_name, ozon_assemblies.store_name),
       updated_at = now()
     returning id`,
    [
      assemblyId,
      name,
      createdBy || null,
      accessMode,
      store?.id || null,
      store?.name || null,
      EMPLOYEE_WORKFLOW_MODE_PICKING,
      getEmployeeWorkflowStageForMode(EMPLOYEE_WORKFLOW_MODE_PICKING, accessMode),
    ]
  );
  return result.rows?.[0]?.id || assemblyId;
}

async function upsertAssemblyOrderItems(assemblyId, postingNumber, products, { replace = false } = {}) {
  if (!Array.isArray(products) || !products.length) return;
  if (replace) {
    await dbQuery(
      "delete from ozon_assembly_order_items where assembly_id = $1 and posting_number = $2",
      [assemblyId, postingNumber]
    );
  }

  let lineNo = 1;
  for (const product of products) {
    const quantity = Math.max(1, Number(product?.quantity || 1));
    const offerId = normalizeArticleKey(product?.offerId || product?.offer_id || product?.article || null);
    const skuNum = Number(product?.sku ?? product?.product_id ?? null);
    const sku = Number.isFinite(skuNum) && skuNum > 0 ? skuNum : null;
    const name = normalizeText(product?.name || "");
    await dbQuery(
      `insert into ozon_assembly_order_items (
        assembly_id, posting_number, line_no, offer_id, sku, name, quantity, scanned_qty, created_at, updated_at
      ) values ($1, $2, $3, $4, $5, $6, $7, 0, now(), now())
      on conflict (assembly_id, posting_number, line_no)
      do update set
        offer_id = excluded.offer_id,
        sku = excluded.sku,
        name = excluded.name,
        quantity = excluded.quantity,
        updated_at = now()`,
      [assemblyId, postingNumber, lineNo, offerId, sku, name, quantity]
    );
    lineNo += 1;
  }
}

async function ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber, storeId = null) {
  const normalizedAssemblyId = normalizeText(assemblyId);
  const normalizedPostingNumber = normalizeText(postingNumber);
  if (!normalizedAssemblyId || !normalizedPostingNumber) return [];

  const existingItemsResult = await dbQuery(
    `select line_no, offer_id, sku, name, quantity, scanned_qty
     from ozon_assembly_order_items
     where assembly_id = $1 and posting_number = $2
     order by line_no asc`,
    [normalizedAssemblyId, normalizedPostingNumber]
  );
  if (existingItemsResult.rows.length) {
    return existingItemsResult.rows;
  }

  const orderResult = await dbQuery(
    `select store_id, label_barcode, ozon_status
     from ozon_assembly_orders
     where assembly_id = $1 and posting_number = $2
     limit 1`,
    [normalizedAssemblyId, normalizedPostingNumber]
  );
  const order = orderResult.rows?.[0] || null;
  if (!order) return [];

  const details = await fetchPostingDetails(
    normalizedPostingNumber,
    storeId || order.store_id || null,
    { timeoutMs: OZON_API_FAST_TIMEOUT_MS }
  );
  const products = Array.isArray(details?.products) ? details.products : [];
  if (products.length) {
    await upsertAssemblyOrderItems(normalizedAssemblyId, normalizedPostingNumber, products, { replace: false });
  }
  await dbQuery(
    `update ozon_assembly_orders
     set label_barcode = coalesce($3, label_barcode),
         ozon_status = coalesce($4, ozon_status),
         updated_at = now()
     where assembly_id = $1 and posting_number = $2`,
    [
      normalizedAssemblyId,
      normalizedPostingNumber,
      normalizeBarcode(details?.labelBarcode || "") || null,
      normalizeText(details?.status || "") || null,
    ]
  );
  const refreshedItemsResult = await dbQuery(
    `select line_no, offer_id, sku, name, quantity, scanned_qty
     from ozon_assembly_order_items
     where assembly_id = $1 and posting_number = $2
     order by line_no asc`,
    [normalizedAssemblyId, normalizedPostingNumber]
  );
  return refreshedItemsResult.rows || [];
}

async function upsertPostingAcrossAssemblies(defaultAssemblyId, posting, store = null) {
  const postingNumber = normalizeText(posting?.postingNumber || "");
  if (!postingNumber) return { action: "skipped", postingNumber: null };

  const existingResult = await dbQuery(
    "select assembly_id from ozon_assembly_orders where posting_number = $1",
    [postingNumber]
  );
  const existing = existingResult.rows?.[0] || null;
  const targetAssemblyId = existing?.assembly_id || defaultAssemblyId;
  const createdAt = posting?.createdAt ? new Date(posting.createdAt) : null;
  const labelBarcode = normalizeBarcode(posting?.labelBarcode || "") || null;
  const ozonStatus = normalizeText(posting?.status || "awaiting_deliver") || "awaiting_deliver";

  if (!existing) {
    await dbQuery(
      `insert into ozon_assembly_orders (
        assembly_id, posting_number, order_created_at, ozon_status, label_barcode, store_id, store_name, created_at, updated_at
      ) values ($1, $2, $3, $4, $5, $6, $7, now(), now())`,
      [targetAssemblyId, postingNumber, createdAt, ozonStatus, labelBarcode, store?.id || null, store?.name || null]
    );
    await upsertAssemblyOrderItems(targetAssemblyId, postingNumber, posting?.products || [], { replace: false });
    return { action: "inserted", postingNumber, assemblyId: targetAssemblyId };
  }

  await dbQuery(
    `update ozon_assembly_orders
     set order_created_at = coalesce($2, order_created_at),
         ozon_status = $3,
         label_barcode = coalesce($4, label_barcode),
         store_id = coalesce($5, store_id),
         store_name = coalesce($6, store_name),
         updated_at = now()
     where posting_number = $1`,
    [postingNumber, createdAt, ozonStatus, labelBarcode, store?.id || null, store?.name || null]
  );

  const itemCountResult = await dbQuery(
    "select count(*)::int as count from ozon_assembly_order_items where assembly_id = $1 and posting_number = $2",
    [targetAssemblyId, postingNumber]
  );
  const hasItems = Number(itemCountResult.rows?.[0]?.count || 0) > 0;
  if (!hasItems && Array.isArray(posting?.products) && posting.products.length) {
    await upsertAssemblyOrderItems(targetAssemblyId, postingNumber, posting.products, { replace: false });
  }

  return {
    action: existing.assembly_id === defaultAssemblyId ? "updated" : "updated_existing_other_assembly",
    postingNumber,
    assemblyId: targetAssemblyId,
  };
}

async function syncAwaitingDeliverFromOzon({ loadLabels = false, userId = null, storeId = null } = {}) {
  const store = resolveStore(storeId);
  if (!store) {
    throw new Error("Ozon магазин не настроен");
  }
  const syncAssemblyId = getSyncAwaitingDeliverAssemblyId(store.id);
  await ensureAssemblyExists(
    syncAssemblyId,
    `Синхронизация Ozon · ${store.name} · Ожидают отгрузки`,
    userId,
    "hidden",
    store
  );

  const postingsRaw = await fetchAllFbsPostingsByStatus("awaiting_deliver", {
    days: OZON_LIST_DAYS,
    sortDir: "DESC",
    includeBarcodes: false,
    storeId: store.id,
  });
  const postingMap = new Map();
  for (const posting of postingsRaw || []) {
    const postingNumber = normalizeText(posting?.postingNumber || "");
    if (!postingNumber) continue;
    const existing = postingMap.get(postingNumber);
    if (!existing) {
      postingMap.set(postingNumber, posting);
      continue;
    }
    const existingProducts = Array.isArray(existing?.products) ? existing.products : [];
    const nextProducts = Array.isArray(posting?.products) ? posting.products : [];
    if (!existingProducts.length && nextProducts.length) {
      postingMap.set(postingNumber, posting);
    }
  }
  const postings = Array.from(postingMap.values());

  let inserted = 0;
  let updated = 0;
  let updatedOther = 0;
  let detailsFetched = 0;
  let failed = 0;
  let removed = 0;
  let rechecked = 0;
  let recheckedKept = 0;
  const failedItems = [];

  for (const posting of postings) {
    try {
      let source = posting;
      if (!Array.isArray(source?.products) || source.products.length === 0) {
        source = await fetchPostingDetails(source.postingNumber, store.id);
        detailsFetched += 1;
      }
      const result = await upsertPostingAcrossAssemblies(syncAssemblyId, {
        ...source,
        status: "awaiting_deliver",
      }, store);
      if (result.action === "inserted") inserted += 1;
      else if (result.action === "updated") updated += 1;
      else if (result.action === "updated_existing_other_assembly") updatedOther += 1;
    } catch (err) {
      failed += 1;
      failedItems.push({
        postingNumber: posting?.postingNumber || null,
        error: err?.message || "sync_failed",
      });
    }
  }

  const fetchedSet = new Set(
    postings
      .map((posting) => normalizeText(posting?.postingNumber || ""))
      .filter(Boolean)
  );

  const localResult = await dbQuery(
    "select posting_number from ozon_assembly_orders where assembly_id = $1",
    [syncAssemblyId]
  );
  const toRecheck = [];
  for (const row of localResult.rows || []) {
    const postingNumber = normalizeText(row?.posting_number || "");
    if (!postingNumber) continue;
    if (!fetchedSet.has(postingNumber)) {
      toRecheck.push(postingNumber);
    }
  }

  const removable = [];
  for (const postingNumber of toRecheck) {
    rechecked += 1;
    try {
      const details = await fetchPostingDetails(postingNumber, store.id);
      const status = normalizeText(details?.status || "");
      if (status === "awaiting_deliver") {
        recheckedKept += 1;
        const result = await upsertPostingAcrossAssemblies(syncAssemblyId, {
          ...details,
          status: "awaiting_deliver",
        }, store);
        if (result.action === "updated_existing_other_assembly") {
          updatedOther += 1;
        } else if (result.action === "updated") {
          updated += 1;
        }
      } else {
        removable.push(postingNumber);
      }
    } catch (err) {
      failed += 1;
      failedItems.push({
        postingNumber,
        error: err?.message || "recheck_failed",
      });
    }
  }

  if (removable.length) {
    const removedResult = await dbQuery(
      `delete from ozon_assembly_orders
       where assembly_id = $1
         and posting_number = any($2::text[])`,
      [syncAssemblyId, removable]
    );
    removed = Number(removedResult.rowCount || 0);
  }

  await dbQuery(
    `update ozon_assemblies
     set labels_total = coalesce((select count(*)::int from ozon_assembly_orders where assembly_id = $1), 0),
         labels_loaded = least(
           coalesce(labels_loaded, 0),
           coalesce((select count(*)::int from ozon_assembly_orders where assembly_id = $1), 0)
         ),
         updated_at = now()
     where id = $1`,
    [syncAssemblyId]
  );

  const totalAfterResult = await dbQuery(
    "select count(*)::int as total from ozon_assembly_orders where assembly_id = $1",
    [syncAssemblyId]
  );
  const localTotalAfter = Number(totalAfterResult.rows?.[0]?.total || 0);

  if (loadLabels) {
    startLabelsJob(syncAssemblyId, false).catch(() => {});
  }

  notifyAssemblyUpdate(syncAssemblyId, true);
  return {
    assemblyId: syncAssemblyId,
    totalFromOzon: postings.length,
    storeId: store.id,
    storeName: store.name,
    inserted,
    updated,
    updatedOther,
    removed,
    rechecked,
    recheckedKept,
    localTotalAfter,
    detailsFetched,
    failed,
    failedItems: failedItems.slice(0, 50),
  };
}

async function listAssemblies(storeId = null) {
  const store = resolveStore(storeId);
  const params = [];
  const whereParts = [];
  whereParts.push(
    "timezone('Europe/Moscow', a.created_at)::date >= timezone('Europe/Moscow', now())::date - 1"
  );
  if (store) {
    params.push(store.id);
    whereParts.push(`a.store_id = $${params.length}`);
  }
  if (storeList.length > 1) {
    whereParts.push("a.id <> 'ozon-sync-awaiting-deliver'");
  }
  const where = whereParts.length ? `where ${whereParts.join(" and ")}` : "";
  const result = await dbQuery(
    `select a.id, a.name, a.store_id, a.store_name, a.warehouse_id, a.warehouse_name, a.access_mode, a.labels_status, a.labels_total, a.labels_loaded, a.labels_error,
            a.location_code, a.sklad_warehouse_id, a.sklad_warehouse_name,
            a.labels_started_at, a.labels_finished_at, a.created_at, a.updated_at
     from ozon_assemblies a
     ${where}
     order by a.created_at desc`,
    params
  );

  const ids = (result.rows || []).map((row) => row.id);
  const orderStats = new Map();
  const itemStats = new Map();
  const accessCounts = new Map();
  if (ids.length) {
    const orderStatsResult = await dbQuery(
      `select assembly_id,
              count(*) filter (where sent_to_delivering_at is null)::int as order_count,
              count(*) filter (where collected_at is not null and sent_to_delivering_at is null)::int as collected_count,
              count(*) filter (where sent_to_delivering_at is not null)::int as sent_count,
              count(*) filter (where assigned_user_id is not null and collected_at is null and sent_to_delivering_at is null)::int as assigned_count
       from ozon_assembly_orders
       where assembly_id = any($1::text[])
       group by assembly_id`,
      [ids]
    );
    for (const row of orderStatsResult.rows || []) {
      orderStats.set(row.assembly_id, {
        orderCount: Number(row.order_count || 0),
        collectedCount: Number(row.collected_count || 0),
        sentCount: Number(row.sent_count || 0),
        assignedCount: Number(row.assigned_count || 0),
      });
    }

    const itemStatsResult = await dbQuery(
      `select assembly_id, coalesce(sum(quantity), 0)::int as item_count
       from ozon_assembly_order_items
       where assembly_id = any($1::text[])
       group by assembly_id`,
      [ids]
    );
    for (const row of itemStatsResult.rows || []) {
      itemStats.set(row.assembly_id, Number(row.item_count || 0));
    }

    const accessResult = await dbQuery(
      `select assembly_id, count(*)::int as cnt
       from ozon_assembly_access_users
       where assembly_id = any($1::text[])
       group by assembly_id`,
      [ids]
    );
    for (const row of accessResult.rows || []) {
      accessCounts.set(row.assembly_id, Number(row.cnt || 0));
    }
  }

  return (result.rows || []).map((row) => {
    const stats = orderStats.get(row.id) || {
      orderCount: 0,
      collectedCount: 0,
      sentCount: 0,
      assignedCount: 0,
    };
    const orderCount = Number(stats.orderCount || 0);
    const labelsLoadedRaw = Number(row.labels_loaded || 0);
    const store = resolveStoreStrict(row.store_id);
    return {
      id: row.id,
      name: row.name,
      storeId: row.store_id || null,
      storeName: store?.name || row.store_name || null,
      warehouseId: normalizeWarehouseId(row.warehouse_id),
      warehouseName: row.warehouse_name || null,
      locationCode: normalizeLocationCode(row.location_code),
      skladWarehouseId: Number(row.sklad_warehouse_id || 0) || null,
      skladWarehouseName: row.sklad_warehouse_name || null,
      accessMode: row.access_mode,
      accessUserCount: accessCounts.get(row.id) || 0,
      labelsStatus: row.labels_status,
      labelsTotal: orderCount,
      labelsLoaded: Math.min(labelsLoadedRaw, orderCount),
      labelsError: row.labels_error || null,
      labelsStartedAt: row.labels_started_at || null,
      labelsFinishedAt: row.labels_finished_at || null,
      orderCount,
      itemCount: Number(itemStats.get(row.id) || 0),
      collectedCount: Number(stats.collectedCount || 0),
      sentCount: Number(stats.sentCount || 0),
      assignedCount: Number(stats.assignedCount || 0),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  });
}

async function getAssembliesCached(force = false, storeId = null, options = {}) {
  const store = resolveStore(storeId);
  if (!store) return { assemblies: [], stale: false, warning: null };
  const cache = getAssembliesCache(store.id);
  const maxAgeMs = Math.max(1000, Number(options?.maxAgeMs || 7000));
  const waitMs = Math.max(500, Number(options?.waitMs || 2500));
  const errorBackoffMs = Math.max(5000, Number(options?.errorBackoffMs || 30000));
  const hasData = Array.isArray(cache.data) && cache.data.length > 0;

  if (!force && hasData && Date.now() - cache.ts < maxAgeMs) {
    return { assemblies: cache.data, stale: false, warning: null };
  }

  if (!force && cache.lastErrorAt && Date.now() - cache.lastErrorAt < errorBackoffMs) {
    return {
      assemblies: cache.data,
      stale: true,
      warning: cache.lastError?.message || "Нет ответа от Ozon API, показан локальный кэш",
    };
  }

  const runRefresh = () => {
    if (cache.loading) return cache.loading;
    cache.loading = (async () => {
      const assemblies = await listAssemblies(store.id);
      cache.data = assemblies;
      cache.ts = Date.now();
      cache.lastError = null;
      cache.lastErrorAt = 0;
      return assemblies;
    })()
      .catch((err) => {
        cache.lastError = err;
        cache.lastErrorAt = Date.now();
        throw err;
      })
      .finally(() => {
        cache.loading = null;
      });
    return cache.loading;
  };

  if (!force && hasData) {
    runRefresh().catch(() => {});
    return { assemblies: cache.data, stale: true, warning: null };
  }

  const refreshPromise = runRefresh();
  let timeoutId = null;
  const raceResult = await Promise.race([
    refreshPromise
      .then((assemblies) => ({ ok: true, assemblies }))
      .catch((err) => ({ ok: false, err })),
    new Promise((resolve) => {
      timeoutId = setTimeout(() => resolve({ timeout: true }), waitMs);
    }),
  ]);
  if (timeoutId) clearTimeout(timeoutId);

  if (raceResult?.timeout) {
    return {
      assemblies: cache.data,
      stale: true,
      warning: "Обновляем список в фоне. Показан текущий кэш.",
    };
  }

  if (raceResult?.ok) {
    return { assemblies: raceResult.assemblies, stale: false, warning: null };
  }

  if (Array.isArray(cache.data)) {
    return {
      assemblies: cache.data,
      stale: true,
      warning: raceResult?.err?.message || "Не удалось обновить сборки, показан локальный кэш",
    };
  }

  throw raceResult?.err || new Error("Не удалось загрузить сборки");
}

async function getAssemblyOrders(assemblyId) {
  const orderResult = await dbQuery(
    `select assembly_id, posting_number, order_created_at, ozon_status, label_barcode, label_s3_key, label_loaded_at,
            label_error, warehouse_id, warehouse_name, delivery_method_id, delivery_method_name,
            assigned_user_id, assigned_at, scan_passed_at, scan_barcode, label_scan_passed_at,
            label_scan_barcode, collected_at, sent_to_delivering_at, sent_error
     from ozon_assembly_orders
     where assembly_id = $1 and sent_to_delivering_at is null
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
  );

  const postings = (orderResult.rows || []).map((row) => row.posting_number).filter(Boolean);
  if (postings.length) {
    const existingItemCounts = await dbQuery(
      `select posting_number, count(*)::int as count
       from ozon_assembly_order_items
       where assembly_id = $1 and posting_number = any($2::text[])
       group by posting_number`,
      [assemblyId, postings]
    );
    const countByPosting = new Map(
      (existingItemCounts.rows || []).map((row) => [row.posting_number, Number(row.count || 0)])
    );
    const missingPostings = postings.filter((postingNumber) => !countByPosting.get(postingNumber));
    if (missingPostings.length) {
      await mapWithConcurrency(missingPostings, 4, async (postingNumber) => {
        await ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber);
      });
    }
  }

  const itemResult = await dbQuery(
    `select assembly_id, posting_number, line_no, offer_id, sku, name, quantity, scanned_qty
     from ozon_assembly_order_items
     where assembly_id = $1
     order by posting_number asc, line_no asc`,
    [assemblyId]
  );

  const itemsByPosting = new Map();
  for (const item of itemResult.rows || []) {
    const key = item.posting_number;
    if (!itemsByPosting.has(key)) itemsByPosting.set(key, []);
    itemsByPosting.get(key).push({
      lineNo: Number(item.line_no),
      article: item.offer_id || null,
      sku: item.sku ? Number(item.sku) : null,
      name: item.name || "",
      quantity: Number(item.quantity || 0),
      scannedQty: Number(item.scanned_qty || 0),
    });
  }

  await attachOzonItemImages(assemblyId, itemsByPosting);

  return (orderResult.rows || []).map((row) => {
    const items = itemsByPosting.get(row.posting_number) || [];
    const total = items.reduce((sum, item) => sum + item.quantity, 0);
    const scanned = items.reduce((sum, item) => sum + item.scannedQty, 0);
    const scanDone = total > 0 && scanned >= total;
    return {
      assemblyId: row.assembly_id,
      postingNumber: row.posting_number,
      createdAt: row.order_created_at,
      ozonStatus: row.ozon_status || null,
      labelBarcode: row.label_barcode || null,
      warehouseId: normalizeWarehouseId(row.warehouse_id),
      warehouseName: row.warehouse_name || null,
      deliveryMethodId: normalizeWarehouseId(row.delivery_method_id),
      deliveryMethodName: row.delivery_method_name || null,
      labelS3Key: row.label_s3_key || null,
      labelUrl: row.label_s3_key ? buildS3Url(row.label_s3_key) : null,
      labelLoadedAt: row.label_loaded_at,
      labelError: row.label_error || null,
      assignedUserId: row.assigned_user_id ? Number(row.assigned_user_id) : null,
      assignedAt: row.assigned_at || null,
      scanPassedAt: row.scan_passed_at || null,
      scanBarcode: row.scan_barcode || null,
      labelScanPassedAt: row.label_scan_passed_at || null,
      labelScanBarcode: row.label_scan_barcode || null,
      collectedAt: row.collected_at || null,
      sentToDeliveringAt: row.sent_to_delivering_at || null,
      sentError: row.sent_error || null,
      totalItems: total,
      scannedItems: scanned,
      scanDone,
      items,
    };
  });
}

async function getAssemblyProgress(assemblyId, accessMode) {
  if (accessMode !== "selected_split") {
    return [];
  }
  const employees = await listEmployees();
  const byUser = new Map();
  for (const emp of employees) {
    byUser.set(Number(emp.id), {
      id: Number(emp.id),
      surname: emp.surname,
      name: emp.name,
      total: 0,
      collected: 0,
      remaining: 0,
      pickingTotal: 0,
      pickingCollected: 0,
      pickingRemaining: 0,
    });
  }

  const [ordersResult, pickingResult] = await Promise.all([
    dbQuery(
      `select assigned_user_id as user_id,
              count(*)::int as total,
              count(*) filter (where collected_at is not null)::int as collected
       from ozon_assembly_orders
       where assembly_id = $1 and assigned_user_id is not null and sent_to_delivering_at is null
       group by assigned_user_id`,
      [assemblyId]
    ),
    dbQuery(
      `select user_id,
              coalesce(sum(greatest(picking_required_qty, 0)), 0)::int as picking_total,
              coalesce(sum(least(greatest(picking_collected_qty, 0), greatest(picking_required_qty, 0))), 0)::int as picking_collected
       from ozon_employee_assembly_sessions
       where assembly_id = $1
         and status not in ('reassigned', 'cancelled')
         and session_role in ('pick', 'pick_pack')
       group by user_id`,
      [assemblyId]
    ),
  ]);
  for (const row of ordersResult.rows || []) {
    const userId = Number(row.user_id);
    if (!byUser.has(userId)) continue;
    const entry = byUser.get(userId);
    entry.total = Number(row.total || 0);
    entry.collected = Number(row.collected || 0);
    entry.remaining = Math.max(0, entry.total - entry.collected);
  }
  for (const row of pickingResult.rows || []) {
    const userId = Number(row.user_id);
    if (!byUser.has(userId)) continue;
    const entry = byUser.get(userId);
    entry.pickingTotal = Number(row.picking_total || 0);
    entry.pickingCollected = Number(row.picking_collected || 0);
    entry.pickingRemaining = Math.max(0, entry.pickingTotal - entry.pickingCollected);
  }
  return Array.from(byUser.values())
    .filter((entry) => entry.total > 0 || entry.pickingTotal > 0)
    .sort((a, b) => a.surname.localeCompare(b.surname, "ru-RU"));
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

// The stock-problem report runs 5 heavy aggregations over the (slow, shared)
// sklad DB. The admin tab polls it every 15s and several admins may open it at
// once, so recomputing per request caused REQUEST_TIMEOUT under load. Cache the
// result ~30s, dedupe concurrent builds into one in-flight promise, and serve
// the last good report if a rebuild fails (instead of throwing a timeout).
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

async function getAssemblyLabelDiagnostics(assemblyId) {
  const assembly = await getAssembly(assemblyId);
  if (!assembly) return null;
  const result = await dbQuery(
    `select posting_number, label_s3_key, label_loaded_at, label_error
     from ozon_assembly_orders
     where assembly_id = $1
     order by posting_number asc`,
    [assemblyId]
  );
  const rows = result.rows || [];
  const problemBuckets = new Map();
  let retryableFailedCount = 0;
  const failedOrders = rows
    .filter((row) => normalizeText(row.label_error))
    .map((row) => {
      const error = String(row.label_error || "").trim();
      const classified = classifyLabelError(error);
      const bucket = problemBuckets.get(classified.code) || {
        code: classified.code,
        title: classified.title,
        count: 0,
      };
      bucket.count += 1;
      problemBuckets.set(classified.code, bucket);
      if (["not_ready", "network", "rate_limit"].includes(classified.code)) {
        retryableFailedCount += 1;
      }
      return {
        postingNumber: row.posting_number,
        labelLoadedAt: row.label_loaded_at || null,
        error,
        code: classified.code,
        title: classified.title,
        recommendation: classified.recommendation,
      };
    });
  const loadedCount = rows.filter((row) => normalizeText(row.label_s3_key)).length;
  const failedCount = failedOrders.length;
  const pendingCount = Math.max(0, rows.length - loadedCount - failedCount);
  const topProblem = failedOrders[0] || null;
  return {
    assemblyId,
    jobRunning: labelJobs.has(assemblyId),
    status: assembly.labels_status,
    total: rows.length,
    loadedCount,
    failedCount,
    pendingCount,
    retryableFailedCount,
    summaryError: assembly.labels_error || null,
    recommendation: topProblem?.recommendation || (failedCount > 0
      ? "Повторите загрузку этикеток. Если отдельные заказы продолжают падать, проверьте их вручную в Ozon."
      : pendingCount > 0
      ? "Этикетки ещё не загружены для части заказов. Запустите обновление."
      : "Этикетки загружены."),
    problemBuckets: Array.from(problemBuckets.values()).sort((a, b) => b.count - a.count || a.title.localeCompare(b.title, "ru-RU")),
    failedOrders,
  };
}

function ensureValidAccessMode(mode) {
  return ["hidden", "selected_split"].includes(mode);
}

const sseClients = new Map();
let sseClientSeq = 1;
const sseLastNotify = new Map();
const SSE_PING_MS = 20000;

function sendSse(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload || {})}\n\n`);
}

function broadcastSse(event, payload, filter = null) {
  for (const [id, client] of sseClients) {
    if (filter && !filter(client)) continue;
    try {
      sendSse(client.res, event, payload);
    } catch {
      sseClients.delete(id);
    }
  }
}

function notifyAssemblyUpdate(assemblyId, force = false) {
  for (const cache of assembliesCaches.values()) {
    cache.ts = 0;
  }
  const key = String(assemblyId || "");
  const now = Date.now();
  const last = sseLastNotify.get(key) || 0;
  if (!force && now - last < 700) return;
  sseLastNotify.set(key, now);
  broadcastSse("assembly_update", { assemblyId: key });
}

setInterval(() => {
  for (const [id, client] of sseClients) {
    try {
      sendSse(client.res, "ping", { time: Date.now() });
    } catch {
      sseClients.delete(id);
    }
  }
}, SSE_PING_MS);

const labelJobs = new Map();

function isRetryableLabelError(err) {
  const message = String(err?.message || "").toLowerCase();
  if (!message) return false;
  if (message.includes("not ready") || message.includes("aren't ready")) return true;
  if (message.includes("fetch failed")) return true;
  if (message.includes("network")) return true;
  if (message.includes("timeout")) return true;
  if (message.includes("econn")) return true;
  if (message.includes("socket")) return true;
  if (message.includes("429")) return true;
  if (message.includes(" 500") || message.includes(" 502") || message.includes(" 503") || message.includes(" 504")) {
    return true;
  }
  return false;
}

function isDeliveredLabelForbiddenError(err) {
  const message = String(err?.message || err || "").toLowerCase();
  if (!message) return false;
  return message.includes("label not allowed for delivered postings");
}

function formatShortLabelError(err) {
  const message = String(err?.message || "").trim();
  if (!message) return "Ошибка загрузки этикетки";
  if (message.length > 220) return `${message.slice(0, 220)}...`;
  return message;
}

function classifyLabelError(message) {
  const text = String(message || "").trim();
  const lower = text.toLowerCase();
  if (!text) {
    return {
      code: "unknown",
      title: "Ошибка без деталей",
      recommendation: "Повторите загрузку этикеток. Если ошибка повторится, проверьте ответ Ozon API.",
    };
  }
  if (lower.includes("not ready") || lower.includes("aren't ready")) {
    return {
      code: "not_ready",
      title: "Этикетка ещё не готова в Ozon",
      recommendation: "Подождите 1-2 минуты и нажмите «Повторить этикетки». Обычно это временное состояние Ozon.",
    };
  }
  if (lower.includes("429") || lower.includes("too many requests")) {
    return {
      code: "rate_limit",
      title: "Ozon ограничил частоту запросов",
      recommendation: "Подождите немного и повторите загрузку. Обычно это временный лимит API.",
    };
  }
  if (lower.includes("401") || lower.includes("403") || lower.includes("unauthorized") || lower.includes("forbidden")) {
    return {
      code: "auth",
      title: "Ошибка доступа к Ozon API",
      recommendation: "Проверьте Ozon Client-Id и API Key. Повторная попытка без исправления токенов не поможет.",
    };
  }
  if (lower.includes("fetch failed") || lower.includes("network") || lower.includes("econn") || lower.includes("socket")) {
    return {
      code: "network",
      title: "Сетевая ошибка при загрузке",
      recommendation: "Нажмите «Повторить этикетки». Если ошибка массовая, проверьте соединение с Ozon и S3.",
    };
  }
  if (lower.includes("pdf") || lower.includes("пустую этикетку")) {
    return {
      code: "invalid_pdf",
      title: "Ozon не вернул корректную PDF-этикетку",
      recommendation: "Сделайте повторную попытку. Если ошибка остаётся только у отдельных заказов, проверьте их вручную в кабинете Ozon.",
    };
  }
  if (lower.includes("s3") || lower.includes("upload")) {
    return {
      code: "s3",
      title: "Ошибка загрузки в S3",
      recommendation: "Повторите загрузку. Если ошибка повторяется массово, проверьте доступы и состояние S3.",
    };
  }
  return {
    code: "generic",
    title: "Неизвестная ошибка загрузки",
    recommendation: "Повторите загрузку. Если не поможет, смотрите текст ошибки по заказу и лог backend.",
  };
}

async function loadLabelPdfWithRetry(postingNumber, storeId = null) {
  const maxAttempts = 2;
  const retryDelayMs = Math.min(Math.max(750, Number(LABELS_RETRY_DELAY_MS || 0) || 0), 2500);
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await getLabelPdfBuffer(postingNumber, storeId);
    } catch (err) {
      lastErr = err;
      if (attempt >= maxAttempts || !isRetryableLabelError(err)) {
        break;
      }
      await wait(retryDelayMs);
    }
  }
  const details = formatShortLabelError(lastErr);
  throw new Error(`Не удалось получить этикетку для ${postingNumber}: ${details}`);
}

async function loadStoredOrderLabelPdf(labelS3Key) {
  const key = normalizeText(labelS3Key);
  if (!key) return null;
  if (isLocalStorageKey(key)) {
    const filePath = resolveLocalFilePath(key);
    try {
      return await fs.promises.readFile(filePath);
    } catch {
      return null;
    }
  }
  try {
    const response = await fetch(buildS3Url(key));
    if (!response.ok) return null;
    return Buffer.from(await response.arrayBuffer());
  } catch {
    return null;
  }
}

async function buildStoredOrderLabelsPdf(assemblyId) {
  const result = await dbQuery(
    `select posting_number, label_s3_key
     from ozon_assembly_orders
     where assembly_id = $1
     order by order_created_at asc nulls last, posting_number asc`,
    [assemblyId]
  );
  const rows = result.rows || [];
  const storedRows = rows.filter((row) => normalizeText(row.label_s3_key));
  if (!rows.length) return { buffer: null, count: 0, totalStored: 0, totalOrders: 0 };

  const merged = await PDFDocument.create();
  let count = 0;
  for (const row of storedRows) {
    const bytes = await loadStoredOrderLabelPdf(row.label_s3_key);
    if (!bytes?.length) continue;
    try {
      const src = await PDFDocument.load(bytes, { ignoreEncryption: true });
      const pages = await merged.copyPages(src, src.getPageIndices());
      for (const page of pages) merged.addPage(page);
      count += 1;
    } catch (err) {
      console.warn("OZ stored order label merge failed", {
        assemblyId,
        postingNumber: row.posting_number,
        error: err?.message || err,
      });
    }
  }
  if (!count) return { buffer: null, count: 0, totalStored: storedRows.length, totalOrders: rows.length };
  return {
    buffer: Buffer.from(await merged.save()),
    count,
    totalStored: storedRows.length,
    totalOrders: rows.length,
  };
}

async function updateAssemblyScanStatusIfReady(assemblyId, postingNumber, authId, scanBarcode = null) {
  const pending = await dbQuery(
    `select 1
     from ozon_assembly_order_items
     where assembly_id = $1 and posting_number = $2 and scanned_qty < quantity
     limit 1`,
    [assemblyId, postingNumber]
  );
  if (pending.rows.length > 0) return false;
  await dbQuery(
    `update ozon_assembly_orders
     set scan_passed_at = coalesce(scan_passed_at, now()),
         scan_passed_by = coalesce(scan_passed_by, $3),
         scan_barcode = coalesce($4, scan_barcode),
         updated_at = now()
     where assembly_id = $1 and posting_number = $2`,
    [assemblyId, postingNumber, authId, scanBarcode]
  );
  return true;
}

async function ensureOrderLabelBarcode(assemblyId, postingNumber) {
  const existing = await dbQuery(
    "select label_barcode, store_id from ozon_assembly_orders where assembly_id = $1 and posting_number = $2",
    [assemblyId, postingNumber]
  );
  const barcode = normalizeBarcode(existing.rows?.[0]?.label_barcode || "");
  if (barcode) return barcode;

  const details = await fetchPostingDetails(postingNumber, existing.rows?.[0]?.store_id || null, {
    timeoutMs: OZON_API_FAST_TIMEOUT_MS,
  });
  const fresh = normalizeBarcode(details?.labelBarcode || "");
  if (!fresh) return null;

  await dbQuery(
    "update ozon_assembly_orders set label_barcode = $3, updated_at = now() where assembly_id = $1 and posting_number = $2",
    [assemblyId, postingNumber, fresh]
  );
  return fresh;
}

async function startLabelsJob(assemblyId, force = false) {
  if (labelJobs.has(assemblyId)) return labelJobs.get(assemblyId);
  const job = (async () => {
    let total = 0;
    let loaded = 0;
    try {
      const prefix = buildLabelsPrefix(assemblyId);
      const assembly = await getAssembly(assemblyId);
      const storeId = assembly?.store_id || null;
      if (storeId) {
        try {
          await syncAssemblyOrdersFromOzon(assemblyId, storeId, { force: true, maxAgeMs: 0 });
        } catch (err) {
          console.warn("OZ labels pre-sync failed", {
            assemblyId,
            storeId,
            error: err?.message || err,
          });
        }
      }
      const ordersResult = await dbQuery(
        `select posting_number, label_s3_key
         from ozon_assembly_orders
         where assembly_id = $1
           and sent_to_delivering_at is null
         order by posting_number asc`,
        [assemblyId]
      );
      const rows = ordersResult.rows || [];
      total = rows.length;
      loaded = force ? 0 : rows.filter((row) => normalizeText(row.label_s3_key)).length;
      let recoveredCount = 0;
      let skippedInactiveCount = 0;
      const retryQueue = [];
      const failedByPosting = new Map();

      await dbQuery(
        `update ozon_assemblies
         set labels_status = 'loading',
             labels_total = $2,
             labels_loaded = $3,
             labels_error = null,
             labels_started_at = now(),
             labels_finished_at = null,
             updated_at = now()
         where id = $1`,
        [assemblyId, total, loaded]
      );
      notifyAssemblyUpdate(assemblyId, true);

      const getCurrentAssemblyOrderRow = async (postingNumber) => {
        const result = await dbQuery(
          `select ozon_status, label_s3_key
           from ozon_assembly_orders
           where assembly_id = $1 and posting_number = $2
           limit 1`,
          [assemblyId, postingNumber]
        );
        return result.rows?.[0] || null;
      };

      const processPostingLabel = async (postingNumber) => {
        const currentRow = await getCurrentAssemblyOrderRow(postingNumber);
        const currentStatus = normalizeText(currentRow?.ozon_status || "");
        if (currentStatus === "awaiting_packaging") {
          const formatted = `Этикетка ещё не готова в Ozon для ${postingNumber}: заказ остаётся в awaiting_packaging.`;
          await dbQuery(
            `update ozon_assembly_orders
             set label_error = $3,
                 updated_at = now()
             where assembly_id = $1 and posting_number = $2`,
            [assemblyId, postingNumber, formatted]
          );
          failedByPosting.set(postingNumber, {
            error: formatted,
            code: "not_ready",
          });
          if (!retryQueue.includes(postingNumber)) {
            retryQueue.push(postingNumber);
          }
          return { ok: false, error: formatted, code: "not_ready" };
        }
        try {
          const pdfBuffer = await loadLabelPdfWithRetry(postingNumber, storeId);
          const key = buildS3Key(prefix, postingNumber);
          const storedKey = await uploadPdfToS3(key, pdfBuffer);
          await dbQuery(
            `update ozon_assembly_orders
             set label_s3_key = $3,
                 label_loaded_at = now(),
                 label_error = null,
                 updated_at = now()
             where assembly_id = $1 and posting_number = $2`,
            [assemblyId, postingNumber, storedKey]
          );
          loaded += 1;
          failedByPosting.delete(postingNumber);
          return { ok: true };
        } catch (err) {
          if (isDeliveredLabelForbiddenError(err)) {
            let nextStatus = "delivering";
            try {
              const details = await fetchPostingDetails(postingNumber, storeId, { timeoutMs: Math.max(OZON_API_TIMEOUT_MS, 15000) });
              const fetchedStatus = normalizeText(details?.status || "");
              if (fetchedStatus) nextStatus = fetchedStatus;
            } catch {}
            await dbQuery(
              `update ozon_assembly_orders
               set ozon_status = $3,
                   assigned_user_id = null,
                   assigned_at = null,
                   sent_to_delivering_at = coalesce(sent_to_delivering_at, now()),
                   label_error = null,
                   updated_at = now()
               where assembly_id = $1 and posting_number = $2`,
              [assemblyId, postingNumber, nextStatus]
            );
            failedByPosting.delete(postingNumber);
            skippedInactiveCount += 1;
            return { ok: false, skipped: true, code: "inactive" };
          }
          const formatted = formatShortLabelError(err);
          const classified = classifyLabelError(formatted);
          await dbQuery(
            `update ozon_assembly_orders
             set label_error = $3,
                 updated_at = now()
             where assembly_id = $1 and posting_number = $2`,
            [assemblyId, postingNumber, formatted]
          );
          failedByPosting.set(postingNumber, {
            error: formatted,
            code: classified.code,
          });
          if (["not_ready", "network", "rate_limit"].includes(classified.code)) {
            retryQueue.push(postingNumber);
          }
          return { ok: false, error: formatted, code: classified.code };
        } finally {
          await dbQuery("update ozon_assemblies set labels_loaded = $2, updated_at = now() where id = $1", [
            assemblyId,
            loaded,
          ]);
          notifyAssemblyUpdate(assemblyId);
        }
      };

      for (const row of rows) {
        const postingNumber = row.posting_number;
        if (!force && normalizeText(row.label_s3_key)) continue;
        await processPostingLabel(postingNumber);
        await wait(LABELS_BATCH_DELAY_MS);
      }

      const retryablePostings = Array.from(new Set(retryQueue));
      if (retryablePostings.length > 0) {
      await wait(Math.min(Math.max(LABELS_RETRY_DELAY_MS, 2000), 4000));
        for (const postingNumber of retryablePostings) {
          if (!failedByPosting.has(postingNumber)) continue;
          const retryResult = await processPostingLabel(postingNumber);
          if (retryResult.ok) {
            recoveredCount += 1;
          }
          await wait(LABELS_BATCH_DELAY_MS);
        }
      }

      const failedEntries = Array.from(failedByPosting.values());
      const failedCount = failedEntries.length;
      const hasError = failedCount > 0;
      const lastError = failedEntries[failedEntries.length - 1]?.error || null;
      const status = hasError ? (loaded > 0 ? "partial" : "error") : "ready";
      const recoveredText = recoveredCount > 0 ? ` Автоматически восстановлено: ${recoveredCount}.` : "";
      const skippedInactiveText = skippedInactiveCount > 0 ? ` Исключено как уже закрытые в Ozon: ${skippedInactiveCount}.` : "";
      const summaryError = hasError
        ? `Не загружено этикеток: ${failedCount} из ${total}.${recoveredText}${skippedInactiveText} Нажмите «Повторить», если ошибка осталась.${lastError ? ` ${lastError}` : ""}`.trim()
        : recoveredCount > 0
        ? `Этикетки загружены.${recoveredText}${skippedInactiveText}`.trim()
        : skippedInactiveCount > 0
        ? `Этикетки обновлены. Исключено как уже закрытые в Ozon: ${skippedInactiveCount}.`
        : null;
      await dbQuery(
        `update ozon_assemblies
         set labels_status = $2,
             labels_error = $3,
             labels_finished_at = now(),
             updated_at = now()
         where id = $1`,
        [assemblyId, status, summaryError]
      );
      notifyAssemblyUpdate(assemblyId, true);
    } catch (error) {
      const summaryError = `Ошибка фоновой загрузки этикеток: ${formatShortLabelError(error)}`;
      try {
        await dbQuery(
          `update ozon_assemblies
           set labels_status = $2,
               labels_error = $3,
               labels_total = greatest(labels_total, $4),
               labels_loaded = greatest(labels_loaded, $5),
               labels_finished_at = now(),
               updated_at = now()
           where id = $1`,
          [assemblyId, loaded > 0 ? "partial" : "error", summaryError, total, loaded]
        );
      } catch {}
      notifyAssemblyUpdate(assemblyId, true);
      throw error;
    }
  })();
  labelJobs.set(assemblyId, job);
  job.finally(() => labelJobs.delete(assemblyId));
  return job;
}

const newOrdersCaches = new Map();
const assembliesCaches = new Map();
const activeWarehouseCaches = new Map();
const fbsStockCheckJobs = new Map();

function normalizeDemandQuantity(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, num);
}

function buildOzonFbsDemandItemKey(item) {
  const marketplaceBarcode = getOzonDemandMarketplaceBarcode(item);
  const article = normalizeArticleKey(item?.article) || "";
  const sku = item?.sku != null ? String(item.sku) : "";
  const name = normalizeText(item?.productName || item?.name || "") || "";
  if (marketplaceBarcode) {
    return `barcode:${marketplaceBarcode}`;
  }
  if (article || sku) {
    return `${article}||${sku}`;
  }
  return `name:${name || "unknown"}`;
}

function getOzonDemandMarketplaceBarcode(item) {
  const explicitMarketplaceBarcodes = uniqueTextList([
    ...(Array.isArray(item?.marketplaceBarcodes) ? item.marketplaceBarcodes : []),
    normalizeBarcode(item?.marketplaceBarcode || ""),
  ]);
  return (
    pickPreferredOzonMarketplaceBarcode(explicitMarketplaceBarcodes, null) ||
    pickPreferredOzonMarketplaceBarcode(buildOzonMarketplaceBarcodeCandidates(item?.sku), item?.sku) ||
    ""
  );
}

function aggregateOzonFbsDemandItems({ assemblingItems, newOrders }) {
  const byItem = new Map();

  const ensureEntry = (source) => {
    const key = buildOzonFbsDemandItemKey(source);
    const marketplaceBarcode = getOzonDemandMarketplaceBarcode(source);
    if (!byItem.has(key)) {
      byItem.set(key, {
        key,
        article: normalizeArticleKey(source?.article) || "",
        sku: Number.isFinite(Number(source?.sku)) ? Number(source.sku) : null,
        marketplaceBarcode,
        productName: normalizeText(source?.productName || source?.name || "") || "",
        newQty: 0,
        assemblingQty: 0,
        postingKeys: new Set(),
        byStore: {},
      });
    }
    const entry = byItem.get(key);
    if (!entry.productName) {
      entry.productName = normalizeText(source?.productName || source?.name || "") || "";
    }
    if (!entry.article) {
      entry.article = normalizeArticleKey(source?.article) || "";
    }
    if (entry.sku == null && Number.isFinite(Number(source?.sku))) {
      entry.sku = Number(source.sku);
    }
    if (!entry.marketplaceBarcode) {
      entry.marketplaceBarcode = marketplaceBarcode;
    }
    return entry;
  };

  for (const item of Array.isArray(assemblingItems) ? assemblingItems : []) {
    const quantity = normalizeDemandQuantity(item?.quantity);
    if (quantity <= 0) continue;
    const entry = ensureEntry(item);
    entry.assemblingQty += quantity;
    const storeId = normalizeStoreId(item?.storeId);
    if (storeId) {
      entry.byStore[storeId] = Number(entry.byStore[storeId] || 0) + quantity;
    }
    const postingNumber = normalizeText(item?.postingNumber || "");
    if (postingNumber) {
      entry.postingKeys.add(`${storeId || ""}:${postingNumber}`);
    }
  }

  for (const order of Array.isArray(newOrders) ? newOrders : []) {
    const postingNumber = normalizeText(order?.postingNumber || "");
    const storeId = normalizeStoreId(order?.storeId);
    for (const product of Array.isArray(order?.products) ? order.products : []) {
      const quantity = normalizeDemandQuantity(product?.quantity);
      if (quantity <= 0) continue;
      const entry = ensureEntry({
        article: product?.offerId,
        sku: product?.sku ?? null,
        marketplaceBarcode: product?.marketplaceBarcode || "",
        productName: product?.name || "",
      });
      entry.newQty += quantity;
      if (storeId) {
        entry.byStore[storeId] = Number(entry.byStore[storeId] || 0) + quantity;
      }
      if (postingNumber) {
        entry.postingKeys.add(`${storeId || ""}:${postingNumber}`);
      }
    }
  }

  return Array.from(byItem.values())
    .map((entry) => ({
      key: entry.key,
      article: entry.article,
      sku: entry.sku,
      marketplaceBarcode: entry.marketplaceBarcode || "",
      productName: entry.productName || "—",
      newQty: Number(entry.newQty || 0),
      assemblingQty: Number(entry.assemblingQty || 0),
      requiredQty: Number(entry.newQty || 0) + Number(entry.assemblingQty || 0),
      orderCount: entry.postingKeys.size,
      byStore: entry.byStore || {},
    }))
    .filter((entry) => entry.requiredQty > 0);
}

function buildResolvedOzonFbsRowKey(match, row) {
  const numericId = Number(match?.id);
  if (Number.isInteger(numericId) && numericId > 0) return `id:${numericId}`;
  const article = normalizeArticleKey(match?.article || row?.article) || "";
  const name = normalizeText(match?.name || row?.productName || "") || "";
  return `${article}||${name}`;
}

function mergeOzonStoreQuantities(target, source) {
  for (const [storeId, qty] of Object.entries(source || {})) {
    const normalizedStoreId = String(storeId || "").trim();
    if (!normalizedStoreId) continue;
    target[normalizedStoreId] = Number(target[normalizedStoreId] || 0) + Number(qty || 0);
  }
  return target;
}

function groupResolvedOzonFbsRows(rows = []) {
  const grouped = new Map();
  const passthrough = [];
  const uniqueArticleList = (values = []) =>
    Array.from(
      new Set(
        (Array.isArray(values) ? values : [])
          .map((value) => normalizeArticleKey(value))
          .filter(Boolean)
      )
    );
  const uniqueValueList = (values = []) =>
    Array.from(
      new Set(
        (Array.isArray(values) ? values : [])
          .map((value) => String(value ?? "").trim())
          .filter(Boolean)
      )
    );

  for (const sourceRow of Array.isArray(rows) ? rows : []) {
    const row = {
      ...sourceRow,
      marketplaceBarcodes: uniqueTextList([
        ...(Array.isArray(sourceRow?.marketplaceBarcodes) ? sourceRow.marketplaceBarcodes : []),
        sourceRow?.marketplaceBarcode || "",
      ]),
      articles: uniqueArticleList([...(Array.isArray(sourceRow?.articles) ? sourceRow.articles : []), sourceRow?.article || ""]),
      skus: uniqueValueList([...(Array.isArray(sourceRow?.skus) ? sourceRow.skus : []), sourceRow?.sku != null ? String(sourceRow.sku) : ""]),
    };
    const match = Array.isArray(row.msMatches) && row.msMatches.length === 1 ? row.msMatches[0] : null;
    const isResolvedRow = (row.msStatus === "ok" || row.msStatus === "insufficient") && match;
    if (!isResolvedRow) {
      passthrough.push({
        ...row,
        marketplaceBarcode: row.marketplaceBarcodes[0] || "",
        article: row.articles[0] || "",
        sku: row.skus[0] || row.sku || null,
      });
      continue;
    }

    const resolvedKey = buildResolvedOzonFbsRowKey(match, row);
    if (!grouped.has(resolvedKey)) {
      grouped.set(resolvedKey, {
        ...row,
        key: `resolved:${resolvedKey}`,
        productName: match?.name || row.productName || "—",
        marketplaceBarcodes: uniqueTextList(row.marketplaceBarcodes),
        marketplaceBarcode: row.marketplaceBarcodes[0] || "",
        articles: uniqueArticleList([match?.article || "", ...row.articles]),
        article: normalizeArticleKey(match?.article || row.article) || row.articles[0] || "",
        skus: uniqueValueList(row.skus),
        sku: row.skus[0] || row.sku || null,
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
    entry.marketplaceBarcodes = uniqueTextList([...(entry.marketplaceBarcodes || []), ...(row.marketplaceBarcodes || [])]);
    entry.marketplaceBarcode = entry.marketplaceBarcodes[0] || entry.marketplaceBarcode || "";
    entry.articles = uniqueArticleList([...(entry.articles || []), ...(row.articles || []), match?.article || ""]);
    entry.article = entry.articles[0] || entry.article || "";
    entry.skus = uniqueValueList([...(entry.skus || []), ...(row.skus || [])]);
    entry.sku = entry.skus[0] || entry.sku || null;
    entry.requiredQty += Number(row.requiredQty || 0);
    entry.newQty += Number(row.newQty || 0);
    entry.assemblingQty += Number(row.assemblingQty || 0);
    entry.orderCount += Number(row.orderCount || 0);
    mergeOzonStoreQuantities(entry.byStore, row.byStore);
  }

  const resolvedRows = Array.from(grouped.values()).map((entry) => {
    const stockQty = entry.msStockQty == null ? null : Number(entry.msStockQty || 0);
    const requiredQty = Number(entry.requiredQty || 0);
    const isShortage = stockQty != null && stockQty < requiredQty;
    return {
      ...entry,
      marketplaceBarcode: entry.marketplaceBarcodes[0] || "",
      article: entry.articles[0] || "",
      sku: entry.skus[0] || entry.sku || null,
      msStatus: stockQty == null ? entry.msStatus : isShortage ? "insufficient" : "ok",
      isShortage,
      msError: null,
    };
  });

  return [...resolvedRows, ...passthrough];
}

async function listPendingAssemblyItemsForFbs(storeIds) {
  const normalizedStoreIds = Array.from(
    new Set((Array.isArray(storeIds) ? storeIds : [storeIds]).map((value) => normalizeStoreId(value)).filter(Boolean))
  );
  if (!normalizedStoreIds.length) return [];
  const result = await dbQuery(
    `select o.assembly_id,
            o.posting_number,
            o.store_id,
            i.offer_id as article,
            i.sku,
            i.name as product_name,
            greatest(coalesce(i.quantity, 1) - coalesce(i.scanned_qty, 0), 0)::int as quantity
     from ozon_assembly_orders o
     join ozon_assembly_order_items i
       on i.assembly_id = o.assembly_id
      and i.posting_number = o.posting_number
     where o.store_id = any($1)
       and o.assembly_id not like 'ozon-sync-awaiting-deliver-%'
       and o.collected_at is null
       and o.sent_to_delivering_at is null
       and greatest(coalesce(i.quantity, 1) - coalesce(i.scanned_qty, 0), 0) > 0
     order by o.assembly_id asc, o.posting_number asc, i.line_no asc`,
    [normalizedStoreIds]
  );

  return (result.rows || []).map((row) => ({
    assemblyId: row.assembly_id,
    postingNumber: row.posting_number,
    storeId: normalizeStoreId(row.store_id) || null,
    article: row.article || null,
    sku: row.sku != null ? Number(row.sku) : null,
    productName: row.product_name || null,
    quantity: Number(row.quantity || 0),
  }));
}

async function enrichOzonFbsSourceWithMarketplaceBarcodes(source, storeId = null) {
  const article = normalizeArticleKey(source?.offerId || source?.article || "");
  const sku = Number.isFinite(Number(source?.sku)) ? Number(source.sku) : null;
  const liveBarcodes = article
    ? await getOfferBarcodesWithFallback(article, [storeId], {
        sku,
        preferMarketplaceOnly: true,
        strictStoreScope: true,
      })
    : buildOzonMarketplaceBarcodeCandidates(sku);
  const marketplaceBarcodes = uniqueTextList([
    ...liveBarcodes,
    ...(Array.isArray(source?.marketplaceBarcodes) ? source.marketplaceBarcodes : []),
    ...(Array.isArray(source?.barcodes) ? source.barcodes : []),
    source?.marketplaceBarcode || "",
  ]);
  const marketplaceBarcode = pickPreferredOzonMarketplaceBarcode(marketplaceBarcodes, sku) || "";
  return {
    ...source,
    article: source?.article || article || "",
    offerId: source?.offerId || article || "",
    barcodes: uniqueTextList([...(Array.isArray(source?.barcodes) ? source.barcodes : []), ...liveBarcodes]),
    marketplaceBarcodes,
    marketplaceBarcode,
  };
}

async function enrichOzonFbsNewOrdersWithMarketplaceBarcodes(orders = []) {
  return await mapWithConcurrency(Array.isArray(orders) ? orders : [], 3, async (order) => ({
    ...order,
    products: await mapWithConcurrency(Array.isArray(order?.products) ? order.products : [], 4, async (product) =>
      await enrichOzonFbsSourceWithMarketplaceBarcodes(product, order?.storeId || null)
    ),
  }));
}

async function enrichOzonFbsAssemblyItemsWithMarketplaceBarcodes(items = []) {
  return await mapWithConcurrency(Array.isArray(items) ? items : [], 4, async (item) =>
    await enrichOzonFbsSourceWithMarketplaceBarcodes(item, item?.storeId || null)
  );
}

function listOzonFbsScopeStores(fallbackStoreId = null) {
  const stores = Array.isArray(storeList) ? storeList.filter((store) => store?.id) : [];
  if (stores.length > 0) return stores;
  const fallbackStore = resolveStore(fallbackStoreId);
  return fallbackStore?.id ? [fallbackStore] : [];
}

async function getFbsStockCheckReport(storeId, options = {}) {
  const stores = listOzonFbsScopeStores(storeId);
  const force = options?.force === true;
  const onProgress = typeof options?.onProgress === "function" ? options.onProgress : null;
  if (onProgress) {
    onProgress({
      stage: "load_orders",
      message: "Загружаем новые заказы и активные сборки по всем магазинам",
      done: 0,
      total: stores.length,
      current: null,
    });
  }
  if (!stores.length) {
    return {
      warehouseName: MS_FBS_STOCK_WAREHOUSE_NAME,
      items: [],
      totals: { all: 0, ok: 0, shortage: 0, errors: 0 },
      updatedAt: new Date().toISOString(),
      scopeStores: [],
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
  await assertSkladDbAvailable(MS_FBS_STOCK_WAREHOUSE_NAME, { force });

  const storeLoadResults = await mapWithConcurrency(
    stores,
    2,
    async (store) => {
      const newOrders = await getNewOrdersCached(force, store.id, { maxAgeMs: 1000 });
      return {
        storeId: store.id,
        storeName: store.name || null,
        newOrders: Array.isArray(newOrders) ? newOrders : [],
      };
    },
    {
      onStart: ({ item, done, total }) => {
        if (!onProgress) return;
        onProgress({
          stage: "load_orders",
          message: "Загружаем новые заказы и активные сборки по всем магазинам",
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
          message: "Загружаем новые заказы и активные сборки по всем магазинам",
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
  const enrichedNewOrders = await enrichOzonFbsNewOrdersWithMarketplaceBarcodes(newOrders);
  const assemblingItems = await enrichOzonFbsAssemblyItemsWithMarketplaceBarcodes(
    await listPendingAssemblyItemsForFbs(stores.map((store) => store.id))
  );
  const aggregatedItems = aggregateOzonFbsDemandItems({
    assemblingItems,
    newOrders: Array.isArray(enrichedNewOrders) ? enrichedNewOrders : [],
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
      const marketplaceBarcode = getOzonDemandMarketplaceBarcode(item);
      const base = {
        key: item.key,
        productName: item.productName || "—",
        article: item.article || "",
        sku: item.sku ?? null,
        requiredQty: Number(item.requiredQty || 0),
        byStore: item.byStore || {},
        newQty: Number(item.newQty || 0),
        assemblingQty: Number(item.assemblingQty || 0),
        orderCount: Number(item.orderCount || 0),
        marketplaceBarcode: marketplaceBarcode || "",
        marketplaceBarcodes: marketplaceBarcode ? [marketplaceBarcode] : [],
        msStockQty: null,
        msCells: [],
        msStatus: "ok",
        msError: null,
        msMatches: [],
        isShortage: false,
      };

      if (!base.marketplaceBarcode) {
        return {
          ...base,
          msStatus: "no_barcode",
          msError: "У товара нет ШК Ozon",
        };
      }

      const ms = await getSkladFbsStockStatusByBarcode(base.marketplaceBarcode, {
        warehouseId: 1,
        storeName: MS_FBS_STOCK_WAREHOUSE_NAME,
        marketplaceType: "ozon",
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
          marketplaceBarcode: ms?.marketplaceBarcode || base.marketplaceBarcode || "",
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
          marketplaceBarcode: ms?.marketplaceBarcode || base.marketplaceBarcode || "",
          msStatus: "multiple",
          msError: "По ШК Ozon найдено несколько товаров в складской БД",
          msMatches: matches,
        };
      }

      if (ms?.status === "not_found") {
        return {
          ...base,
          marketplaceBarcode: ms?.marketplaceBarcode || base.marketplaceBarcode || "",
          msStatus: "not_found",
          msError: "Товар по ШК Ozon не найден в складской БД",
        };
      }

      if (ms?.status === "store_not_found") {
        return {
          ...base,
          marketplaceBarcode: ms?.marketplaceBarcode || base.marketplaceBarcode || "",
          msStatus: "store_not_found",
          msError: ms?.error || `Склад "${MS_FBS_STOCK_WAREHOUSE_NAME}" не найден в складской БД`,
          msMatches: matches,
        };
      }

      if (ms?.status === "stock_error") {
        return {
          ...base,
          marketplaceBarcode: ms?.marketplaceBarcode || base.marketplaceBarcode || "",
          msStatus: "stock_error",
          msError: ms?.error || "Не удалось получить остаток по складу",
          msMatches: matches,
        };
      }

      return {
        ...base,
        marketplaceBarcode: ms?.marketplaceBarcode || base.marketplaceBarcode || "",
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
            marketplaceBarcode: item?.marketplaceBarcode || null,
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
            marketplaceBarcode: item?.marketplaceBarcode || null,
            article: item?.article || null,
            requiredQty: Number(item?.requiredQty || 0),
          },
        });
      },
    }
  );
  const groupedRows = groupResolvedOzonFbsRows(rows);

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
    warehouseName: MS_FBS_STOCK_WAREHOUSE_NAME,
    items: groupedRows,
    totals,
    updatedAt: new Date().toISOString(),
    scopeStores: stores.map((store) => ({ id: store.id, name: store.name || store.id })),
  };
}

function getFbsStockJobKey(storeId) {
  return "all_stores";
}

function getEmptyFbsStockReport() {
  return {
    warehouseName: MS_FBS_STOCK_WAREHOUSE_NAME,
    items: [],
    totals: { all: 0, ok: 0, shortage: 0, errors: 0 },
    updatedAt: null,
    scopeStores: [],
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
  state.progress = {
    stage: patch.stage || prev.stage || "idle",
    message: patch.message || prev.message || "",
    done,
    total,
    percent: Number.isFinite(Number(patch.percent)) ? Number(patch.percent) : toFbsPercent(done, total),
    current: patch.current !== undefined ? patch.current : prev.current || null,
    updatedAt: new Date().toISOString(),
  };
}

function buildFbsStockJobPayload(state) {
  const report = state?.report || getEmptyFbsStockReport();
  return {
    warehouseName: report.warehouseName || MS_FBS_STOCK_WAREHOUSE_NAME,
    items: Array.isArray(report.items) ? report.items : [],
    totals: report.totals || { all: 0, ok: 0, shortage: 0, errors: 0 },
    scopeStores: Array.isArray(report.scopeStores) ? report.scopeStores : [],
    updatedAt: state?.updatedAt || report.updatedAt || null,
    running: Boolean(state?.running),
    error: state?.error || null,
    startedAt: state?.startedAt || null,
    finishedAt: state?.finishedAt || null,
    cooldownUntil: null,
    cooldownRemainingMs: 0,
    progress: {
      ...(state?.progress || {}),
      running: Boolean(state?.running),
    },
  };
}

function startFbsStockCheckJob(storeId, options = {}) {
  const state = getFbsStockJobState(storeId);
  if (state.running && state.jobPromise) {
    return state.jobPromise;
  }

  const force = options?.force === true;
  state.running = true;
  state.error = null;
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  setFbsStockProgress(state, {
    stage: "load_orders",
    message: "Загружаем новые заказы и активные сборки",
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
      return state.report;
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
      throw err;
    })
    .finally(() => {
      state.updatedAt = new Date().toISOString();
      state.jobPromise = null;
    });

  return state.jobPromise;
}

function getNewOrdersCache(storeId = null) {
  const key = normalizeStoreId(storeId) || DEFAULT_STORE?.id || "default";
  if (!newOrdersCaches.has(key)) {
    newOrdersCaches.set(key, {
      ts: 0,
      data: [],
      loading: null,
      lastError: null,
      lastErrorAt: 0,
    });
  }
  return newOrdersCaches.get(key);
}

function getActiveWarehouseCache(storeId = null) {
  const key = normalizeStoreId(storeId) || DEFAULT_STORE?.id || "default";
  if (!activeWarehouseCaches.has(key)) {
    activeWarehouseCaches.set(key, {
      ts: 0,
      data: [],
      loading: null,
      lastError: null,
      lastErrorAt: 0,
    });
  }
  return activeWarehouseCaches.get(key);
}

function getAssembliesCache(storeId = null) {
  const key = normalizeStoreId(storeId) || DEFAULT_STORE?.id || "default";
  if (!assembliesCaches.has(key)) {
    assembliesCaches.set(key, {
      ts: 0,
      data: [],
      loading: null,
      lastError: null,
      lastErrorAt: 0,
    });
  }
  return assembliesCaches.get(key);
}

function normalizeUniquePostings(rows) {
  const seen = new Set();
  const unique = [];
  for (const row of rows || []) {
    const postingNumber = normalizeText(row?.postingNumber || "");
    if (!postingNumber || seen.has(postingNumber)) continue;
    seen.add(postingNumber);
    unique.push(row);
  }
  return unique;
}

function normalizeWarehouseId(value) {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? num : null;
}

function buildWarehouseOptionLabel(warehouseName, deliveryMethodNames) {
  const safeWarehouseName = normalizeText(warehouseName || "");
  const methodNames = Array.from(
    new Set((Array.isArray(deliveryMethodNames) ? deliveryMethodNames : []).map((value) => normalizeText(value)).filter(Boolean))
  );
  if (!safeWarehouseName && methodNames.length) return methodNames.join(", ");
  if (!safeWarehouseName) return "Неизвестный склад";
  if (!methodNames.length) return safeWarehouseName;
  const extraNames = methodNames.filter((value) => !value.toLowerCase().includes(safeWarehouseName.toLowerCase()));
  return extraNames.length ? `${safeWarehouseName} · ${extraNames.join(", ")}` : safeWarehouseName;
}

async function fetchActiveWarehouseOptions(storeId = null) {
  // NOTE: Ozon removed /v1/delivery-method/list and /v1/warehouse/list
  // ("obsolete method cannot be used"), which silently emptied this list and
  // broke the warehouse filter + assembly creation. Use /v2/warehouse/list.
  // Output shape is kept identical (deliveryMethod* stay as empty arrays).
  const warehouseData = await ozonPost("/v2/warehouse/list", {}, { storeId, timeoutMs: OZON_API_FAST_TIMEOUT_MS });
  const warehouseRows = Array.isArray(warehouseData?.warehouses)
    ? warehouseData.warehouses
    : Array.isArray(warehouseData?.result)
    ? warehouseData.result
    : [];
  const options = [];
  for (const row of warehouseRows) {
    const warehouseId = normalizeWarehouseId(row?.warehouse_id);
    if (!warehouseId) continue;
    const status = normalizeText(row?.status || "");
    if (status && ["disabled", "blocked", "disabled_due_to_limit", "error"].includes(status.toLowerCase())) continue;
    const warehouseName = normalizeText(row?.name || "") || `Склад ${warehouseId}`;
    options.push({
      warehouseId,
      warehouseName,
      warehouseStatus: status || "ACTIVE",
      isRfbs: row?.is_rfbs === true,
      deliveryMethodIds: [],
      deliveryMethodNames: [],
      label: warehouseName,
    });
  }
  return options.sort((a, b) => {
    const aName = String(a?.warehouseName || a?.label || "");
    const bName = String(b?.warehouseName || b?.label || "");
    return aName.localeCompare(bName, "ru");
  });
}

async function refreshActiveWarehouseCache(cache, storeId) {
  const rows = await fetchActiveWarehouseOptions(storeId);
  cache.data = rows;
  cache.ts = Date.now();
  cache.lastError = null;
  cache.lastErrorAt = 0;
  return rows;
}

// Raw Ozon warehouse list for the global-settings picker (all seller FBS
// warehouses, NOT filtered by active delivery methods). Cached ~60s per store.
const ozWarehouseListCache = new Map(); // storeId -> { ts, data }
const OZ_WH_LIST_CACHE_MS = 60000;

async function fetchOzonWarehouseListForPicker(storeId) {
  try {
    // NOTE: /v1/warehouse/list and /v1/delivery-method/list are obsolete (Ozon
    // returns "obsolete method cannot be used"); the current endpoint is v2.
    const data = await ozonPost("/v2/warehouse/list", {}, { storeId, timeoutMs: OZON_API_FAST_TIMEOUT_MS });
    const rows = Array.isArray(data?.warehouses)
      ? data.warehouses
      : Array.isArray(data?.result)
      ? data.result
      : [];
    return rows
      .map((row) => {
        // warehouse_id can be a 16-digit number: keep it as a string to avoid precision loss.
        const id = String(row?.warehouse_id ?? row?.id ?? "").trim();
        const name = normalizeText(row?.name || "") || (id ? `Склад ${id}` : "склад");
        return { warehouseId: id, warehouseName: name, id, name };
      })
      .filter((w) => w.warehouseId);
  } catch {
    return [];
  }
}

async function getOzonWarehouseListForPickerCached(storeId) {
  const store = resolveStore(storeId);
  if (!store) return [];
  const cached = ozWarehouseListCache.get(store.id);
  if (cached && Date.now() - cached.ts < OZ_WH_LIST_CACHE_MS) return cached.data;
  const data = await fetchOzonWarehouseListForPicker(store.id);
  if (data.length) ozWarehouseListCache.set(store.id, { ts: Date.now(), data });
  return data;
}

async function getActiveWarehouseOptionsCached(force = false, storeId = null, options = {}) {
  const store = resolveStore(storeId);
  if (!store) return [];
  const cache = getActiveWarehouseCache(store.id);
  const maxAgeMs = Math.max(5000, Number(options?.maxAgeMs || 60000));
  const errorBackoffMs = Math.max(5000, Number(options?.errorBackoffMs || 30000));
  const hasCachedData = Array.isArray(cache.data) && cache.data.length > 0;
  if (!force && hasCachedData && Date.now() - cache.ts < maxAgeMs) {
    return cache.data;
  }
  if (!force && cache.lastErrorAt && Date.now() - cache.lastErrorAt < errorBackoffMs) {
    return cache.data;
  }

  const runRefresh = () => {
    if (cache.loading) return cache.loading;
    cache.loading = refreshActiveWarehouseCache(cache, store.id)
      .catch((err) => {
        cache.lastError = err;
        cache.lastErrorAt = Date.now();
        throw err;
      })
      .finally(() => {
        cache.loading = null;
      });
    return cache.loading;
  };

  if (!force && hasCachedData) {
    runRefresh().catch(() => {});
    return cache.data;
  }
  if (cache.loading) {
    return await cache.loading;
  }
  return await runRefresh();
}

async function resolveCreateWarehouseOption(storeId, requestedWarehouseId, postingNumbers, activeWarehouses = []) {
  const fromActive =
    (Array.isArray(activeWarehouses) ? activeWarehouses : []).find(
      (item) => Number(item?.warehouseId || 0) === Number(requestedWarehouseId)
    ) || null;
  if (fromActive) return fromActive;

  const cache = getNewOrdersCache(storeId);
  let cachedOrders = Array.isArray(cache.data) ? cache.data : [];
  if (!cachedOrders.length) {
    try {
      cachedOrders = await getNewOrdersCached(false, storeId, {
        maxAgeMs: 15000,
        errorBackoffMs: 15000,
      });
    } catch {
      cachedOrders = Array.isArray(cache.data) ? cache.data : [];
    }
  }
  if (!cachedOrders.length || !Array.isArray(postingNumbers) || !postingNumbers.length) {
    return null;
  }

  const byPosting = new Map();
  for (const row of cachedOrders) {
    const postingNumber = normalizeText(row?.postingNumber || "");
    if (postingNumber && !byPosting.has(postingNumber)) {
      byPosting.set(postingNumber, row);
    }
  }
  const selectedOrders = postingNumbers.map((postingNumber) => byPosting.get(postingNumber)).filter(Boolean);
  if (selectedOrders.length !== postingNumbers.length) {
    return null;
  }
  if (!selectedOrders.every((row) => Number(row?.warehouseId || 0) === Number(requestedWarehouseId))) {
    return null;
  }

  const warehouseName =
    normalizeText(selectedOrders[0]?.warehouseName || "") ||
    normalizeText(selectedOrders[0]?.deliveryMethodName || "") ||
    `Склад ${requestedWarehouseId}`;
  const deliveryMethodIds = Array.from(
    new Set(selectedOrders.map((row) => normalizeWarehouseId(row?.deliveryMethodId)).filter(Boolean))
  );
  const deliveryMethodNames = Array.from(
    new Set(selectedOrders.map((row) => normalizeText(row?.deliveryMethodName || "")).filter(Boolean))
  );
  return {
    warehouseId: requestedWarehouseId,
    warehouseName,
    warehouseStatus: "ACTIVE",
    isRfbs: true,
    deliveryMethodIds,
    deliveryMethodNames,
    label: buildWarehouseOptionLabel(warehouseName, deliveryMethodNames),
    source: "orders_fallback",
  };
}

async function refreshNewOrdersCache(cache, storeId) {
  const rows = await fetchAllFbsPostingsByStatus("awaiting_packaging", {
    days: OZON_NEW_LIST_DAYS,
    sortDir: "DESC",
    includeBarcodes: true,
    storeId,
    timeoutMs: OZON_API_FAST_TIMEOUT_MS,
  });
  const unique = normalizeUniquePostings(rows);
  try {
    const offerIds = unique.flatMap((order) => (Array.isArray(order?.products) ? order.products : []))
      .map((product) => normalizeArticleKey(product?.offerId || product?.article || ""))
      .filter(Boolean);
    const imageMap = await resolveOzonCardImages(storeId, offerIds);
    for (const order of unique) {
      for (const product of Array.isArray(order?.products) ? order.products : []) {
        const offerId = normalizeArticleKey(product?.offerId || product?.article || "");
        product.imageUrl = proxifyCardImage(imageMap.get(offerId) || null);
      }
    }
  } catch {
    // Фото не блокируют загрузку списка новых заказов.
  }
  cache.data = unique;
  cache.ts = Date.now();
  cache.lastError = null;
  cache.lastErrorAt = 0;
  return unique;
}

async function getNewOrdersCached(force = false, storeId = null, options = {}) {
  const store = resolveStore(storeId);
  if (!store) return [];
  const cache = getNewOrdersCache(store.id);
  const maxAgeMs = Math.max(1000, Number(options?.maxAgeMs || CACHE_MS));
  const errorBackoffMs = Math.max(5000, Number(options?.errorBackoffMs || 30000));
  const hasCachedData = Array.isArray(cache.data) && cache.data.length > 0;
  if (!force && hasCachedData && Date.now() - cache.ts < maxAgeMs) {
    return cache.data;
  }
  if (!force && cache.lastErrorAt && Date.now() - cache.lastErrorAt < errorBackoffMs) {
    return cache.data;
  }

  const runRefresh = () => {
    if (cache.loading) return cache.loading;
    cache.loading = refreshNewOrdersCache(cache, store.id)
      .catch((err) => {
        cache.lastError = err;
        cache.lastErrorAt = Date.now();
        throw err;
      })
      .finally(() => {
        cache.loading = null;
      });
    return cache.loading;
  };

  if (!force && hasCachedData) {
    runRefresh().catch(() => {});
    return cache.data;
  }

  if (cache.loading) {
    try {
      return await cache.loading;
    } catch (err) {
      if (hasCachedData) return cache.data;
      throw err;
    }
  }

  try {
    return await runRefresh();
  } catch (err) {
    if (hasCachedData) return cache.data;
    throw err;
  }
}

async function requireEmployeeAssemblyAccess(auth, assemblyId) {
  const employeeLocation = await getConfiguredEmployeeLocation(auth.id);
  if (employeeLocation.error) {
    return { ok: false, status: employeeLocation.errorStatus || 409, error: employeeLocation.error };
  }
  const assembly = await getAssembly(assemblyId);
  if (!assembly) return { ok: false, status: 404, error: "Сборка не найдена" };
  const assemblyLocation = normalizeLocationCode(assembly.location_code);
  if (!assemblyLocation) {
    return { ok: false, status: 409, error: "Сборка не настроена по городу. Обратитесь к администратору." };
  }
  if (assemblyLocation !== employeeLocation.locationCode) {
    return { ok: false, status: 404, error: "Сборка недоступна для вашего города" };
  }
  if (assembly.access_mode === "hidden") {
    return { ok: false, status: 403, error: "Сборка недоступна" };
  }
  if (assembly.access_mode !== "selected_split") {
    return { ok: false, status: 403, error: "Сборка недоступна" };
  }
  const allowed = await dbQuery(
    "select 1 from ozon_assembly_access_users where assembly_id = $1 and user_id = $2 limit 1",
    [assemblyId, auth.id]
  );
  if (!allowed.rows.length) {
    return { ok: false, status: 403, error: "Сборка недоступна" };
  }
  return { ok: true, assembly };
}
app.get(`${API_ROOT}/health`, (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

app.post(
  `${API_ROOT}/auth/login`,
  asyncHandler(async (req, res) => {
    const login = normalizeText(req.body?.login ?? req.body?.surname);
    const password = typeof req.body?.password === "string" ? req.body.password : "";
    if (!login || !password) {
      return res.status(400).json({ error: "Введите логин и пароль" });
    }

    const localUsers = await findLocalLoginUsersByLogin(login);
    const matchedLocal = localUsers.find((row) => verifyPassword(password, row.password_salt, row.password_hash));
    if (matchedLocal) {
      const session = await createSession(matchedLocal.id);
      setSessionCookie(res, session.token, session.expires);
      return res.json({ user: toPublicUser(matchedLocal) });
    }

    if (!isExternalAuthConfigured()) {
      return res.status(500).json({ error: "Внешняя база авторизации не настроена" });
    }

    const externalUser = await verifyExternalUserPassword(login, password);
    if (!externalUser) {
      return res.status(401).json({ error: "Неверные данные для входа" });
    }

    const siteUser =
      (await findSiteUserByExternalUserId(externalUser.id)) ||
      (await findSiteUserByExternalLogin(login));
    if (!siteUser) {
      return res.status(403).json({ error: "Пользователь не добавлен" });
    }

    const syncedUser = await syncLocalExternalUser(siteUser.id, externalUser);
    const session = await createSession(syncedUser?.id || siteUser.id);
    setSessionCookie(res, session.token, session.expires);
    return res.json({ user: toPublicUser(syncedUser || siteUser) });
  })
);

app.post(
  `${API_ROOT}/auth/logout`,
  asyncHandler(async (req, res) => {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = cookies[COOKIE_NAME];
    if (token) await deleteSession(token);
    clearSessionCookie(res);
    res.json({ ok: true });
  })
);

app.get(
  `${API_ROOT}/auth/me`,
  asyncHandler(async (req, res) => {
    const user = await getAuth(req);
    if (req.authRefreshedCookie) {
      setSessionCookie(res, req.authRefreshedCookie.token, req.authRefreshedCookie.expires);
    }
    res.json({ user: user || null });
  })
);

app.get(
  `${API_ROOT}/events`,
  requireAuth([ROLE_SUPER, ROLE_ADMIN, ROLE_EMPLOYEE]),
  asyncHandler(async (req, res) => {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    const id = sseClientSeq++;
    sseClients.set(id, { id, res, userId: req.auth.id, role: req.auth.role });
    sendSse(res, "hello", { time: new Date().toISOString() });
    req.on("close", () => {
      sseClients.delete(id);
    });
  })
);

app.get(
  `${API_ROOT}/settings/perf`,
  requireAuth([ROLE_SUPER, ROLE_ADMIN]),
  asyncTimedHandler("api.settings.perf", async (req, res) => {
    res.json({
      generatedAt: new Date().toISOString(),
      metrics: getPerfSnapshot(),
    });
  })
);

app.post(
  `${API_ROOT}/settings/perf/reset`,
  requireAuth([ROLE_SUPER, ROLE_ADMIN]),
  asyncTimedHandler("api.settings.perf_reset", async (req, res) => {
    clearPerfSnapshot();
    res.json({ ok: true });
  })
);

app.get(
  `${API_ROOT}/users/directory`,
  requireAuth([ROLE_SUPER]),
  asyncHandler(async (req, res) => {
    if (!isExternalAuthConfigured()) {
      return res.status(500).json({ error: "Внешняя база авторизации не настроена" });
    }
    const users = await listDirectoryUsersForCurrentSite();
    res.json({ users });
  })
);

app.get(
  `${API_ROOT}/users`,
  requireAuth([ROLE_SUPER, ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    let roleFilter = null;
    if (req.auth.role === ROLE_ADMIN) {
      roleFilter = ROLE_EMPLOYEE;
    } else {
      const requested = req.query?.role;
      if ([ROLE_SUPER, ROLE_ADMIN, ROLE_EMPLOYEE].includes(requested)) roleFilter = requested;
    }
    const users = await listManageableUsers(roleFilter);
    res.json({ users: users.map(toPublicUser) });
  })
);

app.post(
  `${API_ROOT}/users`,
  requireAuth([ROLE_SUPER]),
  asyncHandler(async (req, res) => {
    if (!isExternalAuthConfigured()) {
      return res.status(500).json({ error: "Внешняя база авторизации не настроена" });
    }
    const externalUserId = Number(req.body?.externalUserId);
    let role = req.body?.role;
    if (req.auth.role === ROLE_ADMIN) role = ROLE_EMPLOYEE;

    if (!Number.isFinite(externalUserId) || externalUserId < 1) {
      return res.status(400).json({ error: "Выберите сотрудника из общей базы" });
    }
    if (![ROLE_ADMIN, ROLE_EMPLOYEE].includes(role)) {
      return res.status(400).json({ error: "Некорректная роль" });
    }

    const externalUser = await getExternalUserById(externalUserId);
    if (!externalUser?.employeeId) {
      return res.status(400).json({ error: "У сотрудника нет учётной записи для входа" });
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
      return res.status(409).json({ error: "Этот сотрудник уже добавлен" });
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
    res.json({ user: toPublicUser(result.rows[0]) });
  })
);

app.patch(
  `${API_ROOT}/users/:userId`,
  requireAuth([ROLE_SUPER]),
  asyncHandler(async (req, res) => {
    const userId = Number(req.params.userId);
    const target = await findManagedUserById(userId);
    if (!target) return res.status(404).json({ error: "Пользователь не найден" });
    if (req.auth.role === ROLE_ADMIN && target.role !== ROLE_EMPLOYEE) {
      return res.status(403).json({ error: "FORBIDDEN" });
    }
    let role = req.body?.role ?? target.role;
    if (req.auth.role === ROLE_ADMIN) role = ROLE_EMPLOYEE;
    if (![ROLE_ADMIN, ROLE_EMPLOYEE].includes(role)) {
      return res.status(400).json({ error: "Некорректная роль" });
    }
    const externalUser = await getExternalUserById(target.external_user_id);
    if (!externalUser?.employeeId) {
      return res.status(400).json({ error: "Сотрудник больше недоступен во внешней базе" });
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
    res.json({ user: toPublicUser(result.rows[0]) });
  })
);

app.delete(
  `${API_ROOT}/users/:userId`,
  requireAuth([ROLE_SUPER]),
  asyncHandler(async (req, res) => {
    const userId = Number(req.params.userId);
    const target = await findManagedUserById(userId);
    if (!target) return res.status(404).json({ error: "Пользователь не найден" });
    if (req.auth.id === target.id) return res.status(400).json({ error: "Нельзя удалить себя" });
    if (req.auth.role === ROLE_ADMIN && target.role !== ROLE_EMPLOYEE) {
      return res.status(403).json({ error: "FORBIDDEN" });
    }
    await dbQuery("delete from users where id = $1", [target.id]);
    res.json({ ok: true });
  })
);

app.get(
  `${API_ROOT}/users/:userId/stats`,
  requireAuth([ROLE_SUPER, ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const userId = Number(req.params.userId);
    const user = await findManagedUserById(userId);
    if (!user || user.role !== ROLE_EMPLOYEE) {
      return res.status(404).json({ error: "Сотрудник не найден" });
    }
    let rangeStart = null;
    let rangeEnd = null;
    const from = normalizeText(req.query?.from);
    const to = normalizeText(req.query?.to);
    if (from && to) {
      const parseStatsBoundary = (value, boundary) => {
        const normalized = normalizeText(value);
        if (!normalized) return null;
        if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
          if (boundary === "start") return new Date(`${normalized}T00:00:00+03:00`);
          if (boundary === "end") return new Date(`${normalized}T23:59:59.999+03:00`);
        }
        return new Date(normalized);
      };
      const fromDate = parseStatsBoundary(from, "start");
      const toDate = parseStatsBoundary(to, "end");
      if (!Number.isNaN(fromDate.getTime()) && !Number.isNaN(toDate.getTime()) && toDate >= fromDate) {
        rangeStart = fromDate.toISOString();
        rangeEnd = /^\d{4}-\d{2}-\d{2}$/.test(String(to || "").trim())
          ? new Date(toDate.getTime() + 1).toISOString()
          : toDate.toISOString();
      }
    }
    const stats = getEmployeePositionForUser(user) === EMPLOYEE_POSITION_COLLECTOR
      ? await getCollectorAssemblyStats(userId, rangeStart, rangeEnd)
      : await getEmployeeAssemblyStats(userId, rangeStart, rangeEnd);
    res.json(stats);
  })
);

app.get(
  `${API_ROOT}/settings/scan-issues`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const issues = await listOpenAssemblyScanIssues(store.id);
    // Diagnose each issue by looking the scanned barcode up in the sklad DB
    // (matching is by barcode): not found / duplicate / different product.
    try {
      const barcodes = Array.from(
        new Set(issues.map((i) => normalizeText(String(i.scannedBarcode || i.scanned_barcode || "")).replace(/\s+/g, "")).filter(Boolean))
      );
      if (barcodes.length) {
        const found = await skladRawQuery(
          `select c.cand, p.id, upper(p.article) as article, p.name
           from unnest($1::text[]) as c(cand)
           join products_s p on (
             regexp_replace(coalesce(p.production_barcode, ''), '\\s+', '', 'g') = c.cand
             or exists (
               select 1 from regexp_split_to_table(coalesce(p.barcode_list, ''), ';') as v
               where regexp_replace(btrim(v), '\\s+', '', 'g') = c.cand
             )
             or coalesce(p.marketplace_barcodes_json, '[]'::jsonb) @> jsonb_build_array(jsonb_build_object('value', c.cand))
           )`,
          [barcodes]
        );
        const byCand = new Map();
        for (const row of found.rows || []) {
          const list = byCand.get(row.cand) || [];
          if (!list.some((p) => p.id === Number(row.id))) list.push({ id: Number(row.id), article: row.article, name: row.name || "" });
          byCand.set(row.cand, list);
        }
        for (const issue of issues) {
          const scanned = normalizeText(String(issue.scannedBarcode || issue.scanned_barcode || "")).replace(/\s+/g, "");
          const stage = normalizeScanIssueStage(issue.issueStage || issue.issue_stage);
          if (!scanned) { issue.diagnosis = { type: "no_scan" }; continue; }
          const matches = byCand.get(scanned) || [];
          const noteText = String(issue.contextNote || issue.context_note || "").toLowerCase();
          if ((stage !== SCAN_ISSUE_STAGE_PICK && stage !== SCAN_ISSUE_STAGE_ORDER) || noteText.includes("этикетк")) {
            issue.diagnosis = { type: "label_mismatch" };
          } else if (!matches.length) {
            issue.diagnosis = { type: "not_found" };
          } else if (matches.length > 1) {
            issue.diagnosis = { type: "duplicate", products: matches.map((p) => ({ article: p.article, name: p.name })) };
          } else {
            const expected = (Array.isArray(issue.itemArticles) ? issue.itemArticles : []).map((a) => String(a || "").toUpperCase());
            issue.diagnosis = expected.length && !expected.includes(matches[0].article)
              ? { type: "wrong_product", product: { article: matches[0].article, name: matches[0].name } }
              : { type: "known_product", product: { article: matches[0].article, name: matches[0].name } };
          }
        }
      }
    } catch (error) {
      console.error("[oz] scan-issues diagnosis failed:", error?.message || error);
    }
    res.json({ issues, total: issues.length });
  })
);

async function completeOzonPickIssueItem(issue, adminId, confirmedQuantity) {
  const pickItemId = Number(issue?.pick_item_id || 0);
  if (!pickItemId) throw new Error("Для этой ошибки не выбрана строка сборки товара");
  const itemResult = await dbQuery("select * from ozon_employee_assembly_pick_items where id = $1 limit 1", [
    pickItemId,
  ]);
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

  const sessionResult = await dbQuery("select * from ozon_employee_assembly_sessions where id = $1 limit 1", [
    item.session_id,
  ]);
  const assemblyResult = await dbQuery("select * from ozon_assemblies where id = $1 limit 1", [item.assembly_id]);
  const session = sessionResult.rows?.[0] || null;
  const assembly = assemblyResult.rows?.[0] || null;
  if (!session || !assembly) throw new Error("Сессия сборки товара не найдена");

  const stockMethodEnabled = isStockMethodEnabled(assembly);
  const externalEmployeeId = stockMethodEnabled ? await getUserExternalEmployeeId(issue.employee_id) : null;
  if (stockMethodEnabled && !externalEmployeeId) {
    throw new Error("Сотрудник не привязан к складской системе");
  }

  const updatedItem = await collectEmployeePickItemWithoutScan(pickItemId, { requireBypass: false });
  if (!updatedItem) return { item, resolvedQuantity: 0 };
  if (stockMethodEnabled) {
    try {
      const movementHints = await buildOzonPickItemMovementHints(item, { ...assembly, force: true });
      const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0];
      const movementResult = await moveProductFromShelfToEmployee({
        externalEmployeeId,
        warehouseId: getSkladWarehouseOptionsFromAssembly(assembly).warehouseId,
        barcodes: [item?.barcode, issue?.mapping_barcode].filter(Boolean),
        resolvedProductIds: movementHints?.productIds || [],
        quantity: remainingQuantity,
        skipUnavailable: isAllowStockShortage(assembly),
        preferredShelfId: movementHints?.preferredShelfId || firstCell?.id || null,
        source: "oz_pick_admin_fix",
        notes: `Ozon: admin fixed product picking. Assembly ${item.assembly_id}, item ${item.article || "-"}.`,
        negativeNotes: `Ozon: admin fixed product picking with stock shortage. Assembly ${item.assembly_id}, item ${item.article || "-"}.`,
      });
      await recordStockShortageSkip({
        assemblyId: item.assembly_id,
        stage: "pick_admin_fix",
        article: item.article,
        barcode: item.barcode,
        requestedQuantity: remainingQuantity,
        movementResult,
        userId: adminId,
      });
    } catch (error) {
      await dbQuery("update ozon_employee_assembly_pick_items set picked_qty = $2, updated_at = now() where id = $1", [
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

async function handleAdminScanIssueAction(req, res, action) {
  const store = getRequiredStoreFromRequest(req);
  const issueId = Number(req.params.issueId);
  const issue = await getOpenAssemblyScanIssueById(issueId, store.id);
  if (!issue) {
    return res.status(404).json({ error: "Ошибка не найдена" });
  }
  clearEmployeePickWarmCaches();

  if (action === "auto-fix") {
    if (normalizeScanIssueStage(issue.issue_stage) !== SCAN_ISSUE_STAGE_PICK) {
      return res.status(409).json({ error: "Автоматическая привязка доступна только для сборки товара" });
    }
    if (!issue.warehouse_product_id) {
      return res.status(409).json({ error: "Складская карточка товара не определена автоматически" });
    }
    const barcode = normalizeBarcode(issue.confirmation_scanned_barcode || issue.scanned_barcode || "");
    if (!barcode) return res.status(409).json({ error: "В ошибке нет подтверждённого штрихкода" });
    let result;
    try {
      result = await addWarehouseProductBarcode({ productId: issue.warehouse_product_id, barcode });
    } catch (error) {
      if (error?.isWarehouseBarcodeConflict) {
        return res.status(409).json({ error: error.message });
      }
      throw error;
    }
    clearEmployeePickWarmCaches();
    await resolveAssemblyScanIssue(issueId, req.auth.id, "fixed", {
      resolutionType: "auto_barcode",
      resolutionNote: result.added ? `Штрихкод ${barcode} добавлен в складскую карточку` : `Штрихкод ${barcode} уже был в складской карточке`,
      resolvedQuantity: 0,
    });
    broadcastSse(
      "scan_issue_fixed",
      { assemblyId: issue.assembly_id, issueStage: SCAN_ISSUE_STAGE_PICK, pickItemId: issue.pick_item_id },
      (client) => client.role === ROLE_EMPLOYEE || client.role === ROLE_ADMIN
    );
  } else if (action === "fix") {
    if (normalizeScanIssueStage(issue.issue_stage) === SCAN_ISSUE_STAGE_PICK) {
      let fixResult;
      try {
        fixResult = await completeOzonPickIssueItem(issue, req.auth.id, req.body?.confirmedQuantity);
      } catch (error) {
        if (error?.fixConfirmationRequired) {
          return res.status(409).json({
            error: error.message,
            code: "FIX_CONFIRM_REQUIRED",
            remainingQuantity: error.remainingQuantity,
          });
        }
        throw error;
      }
      await resolveAssemblyScanIssue(issueId, req.auth.id, "fixed", {
        resolutionType: "fixed",
        resolutionNote: `Администратор завершил остаток товара: ${fixResult.resolvedQuantity} шт.`,
        resolvedQuantity: fixResult.resolvedQuantity,
      });
      broadcastSse(
        "scan_issue_fixed",
        {
          assemblyId: issue.assembly_id,
          postingNumber: null,
          issueStage: SCAN_ISSUE_STAGE_PICK,
          pickItemId: issue.pick_item_id,
          scanBarcode: issue.scanned_barcode || null,
        },
        (client) => client.role === ROLE_EMPLOYEE || client.role === ROLE_ADMIN
      );
    } else {
      await dbQuery(
        `update ozon_assembly_order_items
         set scanned_qty = quantity,
              updated_at = now()
         where assembly_id = $1 and posting_number = $2 and scanned_qty < quantity`,
        [issue.assembly_id, issue.posting_number]
      );
      await updateAssemblyScanStatusIfReady(issue.assembly_id, issue.posting_number, req.auth.id, issue.scanned_barcode || null);
      await resolveAssemblyScanIssue(issueId, req.auth.id, "fixed", { resolutionType: "fixed" });
      broadcastSse(
        "scan_issue_fixed",
        {
          assemblyId: issue.assembly_id,
          postingNumber: issue.posting_number,
          issueStage: SCAN_ISSUE_STAGE_ORDER,
          scanBarcode: issue.scanned_barcode || null,
        },
        (client) => client.role === ROLE_EMPLOYEE || client.role === ROLE_ADMIN
      );
    }
  } else {
    await resolveAssemblyScanIssue(issueId, req.auth.id, "resolved", { resolutionType: "resolved" });
  }

  broadcastSse("scan_issue_update", { at: new Date().toISOString() }, (client) => client.role === ROLE_ADMIN);
  notifyAssemblyUpdate(issue.assembly_id, true);
  return res.json({ ok: true });
}

app.post(
  `${API_ROOT}/settings/scan-issues/:issueId/resolve`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => handleAdminScanIssueAction(req, res, "resolve"))
);

app.post(
  `${API_ROOT}/settings/scan-issues/:issueId/fix`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => handleAdminScanIssueAction(req, res, "fix"))
);

app.get(
  `${API_ROOT}/stores`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (_req, res) => {
    // Attach live Ozon warehouses per store (cached ~60s) so superadmin can
    // pick a marketplace warehouse in global settings, not just a store.
    const stores = await Promise.all(
      storeList.map(async (store) => {
        let warehouses = [];
        try {
          warehouses = await getOzonWarehouseListForPickerCached(store.id);
        } catch {
          warehouses = [];
        }
        return { id: store.id, name: store.name, warehouses: Array.isArray(warehouses) ? warehouses : [] };
      })
    );
    res.json({
      stores,
      defaultStoreId: DEFAULT_STORE?.id || null,
    });
  })
);

// City → Ozon marketplace warehouses mapping (from superadmin's global settings).
// Lets the create-assembly modal pick the Ozon warehouse automatically by city.
app.get(
  `${API_ROOT}/location-warehouse-map`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const locationCode = normalizeText(req.query?.location || "").toLowerCase();
    if (!locationCode) return res.status(400).json({ error: "Не указан город" });
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    const storeId = normalizeStoreId(store?.id) || "";
    const result = await dbQuery(
      `select marketplace_warehouse_id, marketplace_warehouse_name, sklad_warehouse_id, sklad_warehouse_name
       from site_location_warehouse_settings
       where active = true and location_code = $1
         and marketplace in ('all', 'oz')
         and coalesce(marketplace_warehouse_id, '') <> ''
         and (coalesce(store_id, '') = '' or store_id = $2)`,
      [locationCode, storeId]
    );
    res.json({
      locationCode,
      storeId,
      warehouses: (result.rows || []).map((row) => ({
        marketplaceWarehouseId: String(row.marketplace_warehouse_id),
        marketplaceWarehouseName: row.marketplace_warehouse_name || null,
        skladWarehouseId: row.sklad_warehouse_id == null ? null : Number(row.sklad_warehouse_id),
        skladWarehouseName: row.sklad_warehouse_name || null,
      })),
    });
  })
);

// Per-article stock on the city's internal (GRAsklad) warehouse — feeds the
// «exclude articles» picker in the create-assembly modal.
app.post(
  `${API_ROOT}/city-article-stock`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const locationCode = normalizeText(req.body?.location || "").toLowerCase();
    const articles = Array.from(
      new Set(
        (Array.isArray(req.body?.articles) ? req.body.articles : [])
          .map((value) => normalizeText(value).toUpperCase())
          .filter(Boolean)
      )
    ).slice(0, 400);
    if (!locationCode) return res.status(400).json({ error: "Не указан город" });
    if (!articles.length) return res.json({ warehouseId: null, warehouseName: null, stocks: {} });
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    const marketplaceWarehouseId = normalizeText(req.body?.marketplaceWarehouseId || "") || null;
    let warehouse;
    try {
      warehouse = await resolveSkladWarehouseForLocation({
        locationCode,
        marketplace: "oz",
        storeId: store?.id || null,
        marketplaceWarehouseId,
      });
    } catch (error) {
      // Precise mappings require a matching Ozon warehouse id; retry without it
      // so a store-level or city-level mapping can still resolve the stock.
      try {
        warehouse = await resolveSkladWarehouseForLocation({ locationCode, marketplace: "oz", storeId: store?.id || null });
      } catch (error2) {
        return res.status(error2?.statusCode || 409).json({ error: error2?.message || "Склад города не настроен" });
      }
    }
    // Matching is done BY BARCODE, never by article: the sklad card's barcode
    // field holds both the jar's production barcode and the marketplace barcode,
    // exactly like the pick stage and the error-proof scan do. A barcode mapped
    // to several products is a real data problem — surface it as "duplicate".
    const rawItems = Array.isArray(req.body?.items) ? req.body.items : [];
    const pickerItems = rawItems.length
      ? rawItems
      : articles.map((article) => ({ article })); // legacy fallback (old cached frontend)
    const candidatesByArticle = new Map();
    for (const item of pickerItems.slice(0, 400)) {
      const article = normalizeText(item?.article || "").toUpperCase();
      if (!article) continue;
      const candidates = [];
      const push = (value) => {
        const normalized = normalizeText(String(value ?? "")).replace(/\s+/g, "");
        if (normalized && !candidates.includes(normalized)) candidates.push(normalized);
      };
      push(item?.marketplaceBarcode);
      (Array.isArray(item?.barcodes) ? item.barcodes : []).forEach(push);
      if (item?.sku != null && String(item.sku).trim() !== "") {
        push(`OZN${String(item.sku).trim()}`);
        push(item.sku);
      }
      candidatesByArticle.set(article, candidates);
    }
    const allCandidates = Array.from(new Set([...candidatesByArticle.values()].flat()));
    const matchesByCandidate = new Map();
    const productIds = new Set();
    if (allCandidates.length) {
      const found = await skladRawQuery(
        `select c.cand, p.id, upper(p.article) as article, p.name
         from unnest($1::text[]) as c(cand)
         join products_s p on (
           regexp_replace(coalesce(p.production_barcode, ''), '\\s+', '', 'g') = c.cand
           or exists (
             select 1 from regexp_split_to_table(coalesce(p.barcode_list, ''), ';') as v
             where regexp_replace(btrim(v), '\\s+', '', 'g') = c.cand
           )
           or coalesce(p.marketplace_barcodes_json, '[]'::jsonb) @> jsonb_build_array(jsonb_build_object('value', c.cand))
         )`,
        [allCandidates]
      );
      for (const row of found.rows || []) {
        const list = matchesByCandidate.get(row.cand) || [];
        if (!list.some((p) => p.id === Number(row.id))) {
          list.push({ id: Number(row.id), article: row.article, name: row.name || "" });
        }
        matchesByCandidate.set(row.cand, list);
        productIds.add(Number(row.id));
      }
    }
    const stockByProduct = new Map();
    if (productIds.size) {
      const stockRes = await skladRawQuery(
        `select x.product_id, coalesce(sum(x.qty), 0)::int as stock
         from (
           select si.product_id, si.quantity as qty
           from shelf_items_s si
           join shelves_s s on s.id = si.shelf_id
           join racks_s r on r.id = s.rack_id
           where r.warehouse_id = $1 and si.product_id = any($2::int[])
           union all
           select sbi.product_id, sbi.quantity
           from shelf_box_items_s sbi
           join shelf_boxes_s sb on sb.id = sbi.shelf_box_id
           join shelves_s s2 on s2.id = sb.shelf_id
           join racks_s r2 on r2.id = s2.rack_id
           where r2.warehouse_id = $1 and sbi.product_id = any($2::int[])
         ) x
         group by x.product_id`,
        [warehouse.skladWarehouseId, [...productIds]]
      );
      for (const row of stockRes.rows || []) stockByProduct.set(Number(row.product_id), Number(row.stock || 0));
    }
    const items = {};
    for (const [article, candidates] of candidatesByArticle.entries()) {
      const matched = [];
      for (const candidate of candidates) {
        for (const product of matchesByCandidate.get(candidate) || []) {
          if (!matched.some((p) => p.id === product.id)) matched.push(product);
        }
      }
      if (!matched.length) {
        items[article] = { status: "no_barcode", stock: 0 };
      } else if (matched.length > 1) {
        items[article] = {
          status: "duplicate",
          stock: 0,
          duplicates: matched.map((p) => ({ article: p.article, name: p.name })),
        };
      } else {
        items[article] = {
          status: "ok",
          stock: stockByProduct.get(matched[0].id) || 0,
          matchedArticle: matched[0].article,
        };
      }
    }
    res.json({
      warehouseId: warehouse.skladWarehouseId,
      warehouseName: warehouse.skladWarehouseName,
      items,
      // legacy field for old cached frontends
      stocks: Object.fromEntries(Object.entries(items).map(([k, v]) => [k, v.stock])),
    });
  })
);

app.get(
  `${API_ROOT}/new-orders`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const force = String(req.query?.force || "") === "1";
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    if (!store) return res.status(400).json({ error: "Магазин не настроен" });
    const cache = getNewOrdersCache(store.id);
    const warehouseCache = getActiveWarehouseCache(store.id);
    let warehouses = [];
    try {
      warehouses = await getActiveWarehouseOptionsCached(force, store.id);
    } catch {
      warehouses = Array.isArray(warehouseCache.data) ? warehouseCache.data : [];
    }
    try {
      const orders = await getNewOrdersCached(force, store.id);
      return res.json({
        updatedAt: new Date().toISOString(),
        orders,
        warehouses,
        storeId: store.id,
        storeName: store.name,
      });
    } catch (err) {
      const cachedOrders = Array.isArray(cache.data) ? cache.data : [];
      return res.json({
        updatedAt: new Date().toISOString(),
        orders: cachedOrders,
        warehouses,
        storeId: store.id,
        storeName: store.name,
        stale: true,
        warning: err?.message || "Нет ответа от Ozon API, показан локальный кэш",
      });
    }
  })
);

app.get(
  `${API_ROOT}/fbs-stock-check`,
  requireAuth([ROLE_ADMIN]),
  asyncTimedHandler("api.fbs_stock_check", async (req, res) => {
    const requestedStoreId = normalizeStoreId(req.headers["x-store-id"]);
    const force = String(req.query?.refresh || "") === "1";
    const state = getFbsStockJobState(requestedStoreId);
    if (force) {
      startFbsStockCheckJob(requestedStoreId, { force: true }).catch(() => {});
    } else if (!state.report && !state.running) {
      startFbsStockCheckJob(requestedStoreId, { force: false }).catch(() => {});
    }
    res.json(buildFbsStockJobPayload(getFbsStockJobState(requestedStoreId)));
  })
);

app.get(
  `${API_ROOT}/stock-problems`,
  requireAuth([ROLE_ADMIN]),
  asyncTimedHandler("api.stock_problems", async (req, res) => {
    try {
      const report = await getStockProblemReportCached(String(req.query?.force || "") === "1");
      res.json(report);
    } catch (error) {
      const message = isSkladMovementAccessError(error)
        ? "Не удалось получить отчёт по проблемам остатков"
        : error?.message || "Не удалось получить отчёт по проблемам остатков";
      res.status(isSkladMovementAccessError(error) ? 503 : 500).json({ error: message });
    }
  })
);

app.get(
  `${API_ROOT}/assemblies`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const force = String(req.query?.force || "") === "1";
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    if (!store) return res.status(400).json({ error: "Магазин не настроен" });
    const result = await getAssembliesCached(force, store.id, {
      maxAgeMs: 7000,
      waitMs: 2500,
      errorBackoffMs: 30000,
    });
    res.json({
      updatedAt: new Date().toISOString(),
      assemblies: result.assemblies,
      storeId: store.id,
      storeName: store.name,
      stale: Boolean(result.stale),
      warning: result.warning || null,
    });
  })
);

app.get(
  `${API_ROOT}/assemblies/history`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    if (!store) return res.status(400).json({ error: "Магазин не настроен" });
    const fromRaw = normalizeText(req.query?.from);
    const toRaw = normalizeText(req.query?.to);
    const dateRe = /^\d{4}-\d{2}-\d{2}$/;
    const params = [store.id];
    const whereParts = ["a.store_id = $1", "a.id not like 'ozon-sync-awaiting-deliver%'"];
    if (dateRe.test(fromRaw)) {
      params.push(fromRaw);
      whereParts.push(`timezone('Europe/Moscow', a.created_at)::date >= $${params.length}::date`);
    }
    if (dateRe.test(toRaw)) {
      params.push(toRaw);
      whereParts.push(`timezone('Europe/Moscow', a.created_at)::date <= $${params.length}::date`);
    }
    const listResult = await dbQuery(
      `select a.id, a.name, a.store_id, a.store_name, a.access_mode, a.created_at,
              count(o.*)::int as total,
              count(o.*) filter (where o.collected_at is not null)::int as collected,
              count(o.*) filter (where o.sent_to_delivering_at is not null)::int as sent
       from ozon_assemblies a
       left join ozon_assembly_orders o on o.assembly_id = a.id
       where ${whereParts.join(" and ")}
       group by a.id, a.name, a.store_id, a.store_name, a.access_mode, a.created_at
       order by a.created_at desc
       limit 500`,
      params
    );
    const rows = listResult.rows || [];
    const ids = rows.map((r) => r.id);
    const collectorsMap = new Map();
    if (ids.length) {
      const cr = await dbQuery(
        `select o.assembly_id, o.collected_by,
                coalesce(nullif(trim(concat_ws(' ', u.surname, u.name)), ''), 'ID '||o.collected_by) as full_name,
                count(*)::int as cnt,
                count(*) filter (where coalesce(o.label_scan_barcode, '') = 'NO_SCAN_MODE')::int as noscan
         from ozon_assembly_orders o
         left join users u on u.id = o.collected_by
         where o.assembly_id = any($1::text[]) and o.collected_at is not null
         group by o.assembly_id, o.collected_by, u.surname, u.name`,
        [ids]
      );
      for (const row of cr.rows || []) {
        const arr = collectorsMap.get(row.assembly_id) || [];
        const cnt = Number(row.cnt || 0);
        const noscan = Number(row.noscan || 0);
        arr.push({ userId: row.collected_by, name: row.full_name, count: cnt, scanCount: cnt - noscan, noScanCount: noscan });
        collectorsMap.set(row.assembly_id, arr);
      }
    }
    res.json({
      assemblies: rows.map((r) => ({
        id: r.id,
        name: r.name,
        storeId: r.store_id,
        storeName: r.store_name,
        accessMode: r.access_mode,
        createdAt: r.created_at,
        total: Number(r.total || 0),
        collected: Number(r.collected || 0),
        sent: Number(r.sent || 0),
        collectors: (collectorsMap.get(r.id) || []).sort((a, b) => b.count - a.count),
      })),
    });
  })
);

app.post(
  `${API_ROOT}/assemblies/sync-awaiting-deliver`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const loadLabels = req.body?.loadLabels !== false;
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    if (!store) return res.status(400).json({ error: "Магазин не настроен" });
    const result = await syncAwaitingDeliverFromOzon({ loadLabels, userId: req.auth.id, storeId: store.id });
    res.json({
      ok: true,
      ...result,
    });
  })
);

app.post(
  `${API_ROOT}/assemblies/create`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = resolveStoreStrict(req.headers["x-store-id"]);
    if (!store) return res.status(400).json({ error: "Магазин не настроен" });
    const requestedName = normalizeText(req.body?.name);
    const requestedWarehouseId = normalizeWarehouseId(req.body?.warehouseId);
    const locationCode = normalizeLocationCode(req.body?.locationCode);
    const allowStockShortage = req.body?.allowStockShortage === true;
    const articleSelectionMode = normalizeText(req.body?.selectionMode) === "articles";
    const selectedArticleSet = new Set(
      (articleSelectionMode && Array.isArray(req.body?.selectedArticles) ? req.body.selectedArticles : [])
        .map((value) => normalizeArticleKey(value))
        .filter(Boolean)
    );
    const postingNumbersRaw = Array.isArray(req.body?.postingNumbers) ? req.body.postingNumbers : [];
    const postingNumbers = Array.from(
      new Set(postingNumbersRaw.map((item) => normalizeText(item)).filter(Boolean))
    );
    if (requestedName && requestedName.length > 120) {
      return res.status(400).json({ error: "Слишком длинное название сборки" });
    }
    if (!postingNumbers.length) {
      return res.status(400).json({ error: "Нужно выбрать хотя бы один заказ" });
    }
    if (articleSelectionMode && selectedArticleSet.size === 0) {
      return res.status(400).json({ error: "Выберите хотя бы один артикул для режима «Только выбранные»" });
    }
    if (!requestedWarehouseId) {
      return res.status(400).json({ error: "Выберите активный склад Ozon" });
    }
    if (!locationCode) {
      return res.status(400).json({ error: "Выберите город сборки" });
    }
    if (MAX_CREATE_COUNT > 0 && postingNumbers.length > MAX_CREATE_COUNT) {
      return res.status(400).json({ error: `Можно выбрать не более ${MAX_CREATE_COUNT} заказов` });
    }

    const activeWarehouses = await getActiveWarehouseOptionsCached(false, store.id, { maxAgeMs: 15000 });
    const selectedWarehouse = await resolveCreateWarehouseOption(
      store.id,
      requestedWarehouseId,
      postingNumbers,
      activeWarehouses
    );
    if (!selectedWarehouse) {
      return res.status(400).json({ error: "Выбранный склад недоступен или больше не активен" });
    }
    let skladWarehouse = null;
    try {
      skladWarehouse = await resolveSkladWarehouseForLocation({
        locationCode,
        marketplace: "oz",
        storeId: store.id,
        marketplaceWarehouseId: String(requestedWarehouseId),
      });
    } catch (error) {
      return res.status(error?.statusCode || 409).json({ error: error?.message || "Не настроен склад" });
    }

    const assemblyName = requestedName || buildAssemblyName(postingNumbers.length);
    const requestKey = buildCreateRequestKey(store.id, [
      `location:${locationCode}`,
      `warehouse:${requestedWarehouseId}`,
      `allowStockShortage:${allowStockShortage}`,
      `selectionMode:${articleSelectionMode ? "articles" : "count"}`,
      ...Array.from(selectedArticleSet).sort().map((article) => `article:${article}`),
      ...postingNumbers,
    ]);
    const cachedCreate = getRecentCreateRequest(recentCreateAssemblyRequests, requestKey);
    if (cachedCreate) {
      return res.json(await cachedCreate);
    }

    const createPromise = (async () => {
      const reusableAssembly = await findReusableAssemblyForCreate({
        storeId: store.id,
        warehouseId: requestedWarehouseId,
        name: assemblyName,
        postingNumbers,
        allowStockShortage,
      });
      const assemblyId = normalizeText(reusableAssembly?.id) || buildAssemblyId();
      const reuseExisting = Boolean(reusableAssembly?.id);
      if (!reuseExisting) {
        await dbQuery(
          `insert into ozon_assemblies (
             id, name, created_by, access_mode, store_id, store_name,
             warehouse_id, warehouse_name,
             location_code, sklad_warehouse_id, sklad_warehouse_name,
             stock_method_enabled, allow_no_scan, desktop_print, allow_stock_shortage, employee_workflow_mode, employee_workflow_stage,
             created_at, updated_at
           ) values ($1, $2, $3, 'hidden', $4, $5, $6, $7, $8, $9, $10, true, false, $13, $14, $11, $12, now(), now())`,
          [
            assemblyId,
            assemblyName,
            req.auth.id,
            store.id,
            store.name,
            requestedWarehouseId,
            selectedWarehouse.warehouseName,
            locationCode,
            skladWarehouse.skladWarehouseId,
            skladWarehouse.skladWarehouseName,
            EMPLOYEE_WORKFLOW_MODE_PICKING,
            getEmployeeWorkflowStageForMode(EMPLOYEE_WORKFLOW_MODE_PICKING, "hidden"),
            req.body?.desktopPrint === true,
            allowStockShortage,
          ]
        );
      } else {
        await dbQuery(
          `update ozon_assemblies
           set location_code = coalesce(location_code, $2),
               sklad_warehouse_id = coalesce(sklad_warehouse_id, $3),
               sklad_warehouse_name = coalesce(sklad_warehouse_name, $4),
               updated_at = now()
           where id = $1`,
          [assemblyId, locationCode, skladWarehouse.skladWarehouseId, skladWarehouse.skladWarehouseName]
        );
      }
      const existingPostingNumbers = reuseExisting ? await getAssemblyPostingNumberSet(assemblyId) : new Set();

      // === ПРОВЕРКА ОСТАТКОВ ДО ОТГРУЗКИ ===
      // Заказ не должен попасть в сборку, если товара нет на складе (или его меньше нужного).
      // Проверяем ДО необратимой отгрузки на Ozon; при нехватке — ничего не создаём/не отгружаем.
      const detailsByPosting = new Map();
      const demand = new Map(); // UPPER(barcode) -> { barcode, requiredQty, productName, article, postings:Set }
      for (const postingNumber of postingNumbers) {
        if (existingPostingNumbers.has(postingNumber)) continue;
        let preDetails;
        try { preDetails = await fetchPostingDetails(postingNumber, store.id); }
        catch { continue; } // проблему заказа обработает основной цикл ниже
        if (!isOzonPostingExclusiveToArticles(preDetails.products || [], selectedArticleSet)) continue;
        detailsByPosting.set(postingNumber, preDetails);
        for (const product of preDetails.products || []) {
          const candidates = buildOzonMarketplaceBarcodeCandidates(product.sku);
          const barcode = pickPreferredOzonMarketplaceBarcode(candidates, product.sku) || candidates[0] || null;
          if (!barcode) continue;
          const key = String(barcode).toUpperCase();
          const cur = demand.get(key) || {
            barcode,
            requiredQty: 0,
            productName: normalizeText(product.name || ""),
            article: normalizeArticleKey(product.offerId),
            postings: new Set(),
          };
          cur.requiredQty += Math.max(1, Number(product.quantity || 1));
          cur.postings.add(postingNumber);
          demand.set(key, cur);
        }
      }
      if (demand.size && !allowStockShortage) {
        const shortages = [];
        await mapWithConcurrency([...demand.values()], 3, async (d) => {
          try {
            const ms = await getSkladFbsStockStatusByBarcode(d.barcode, {
              warehouseId: skladWarehouse.skladWarehouseId,
              storeName: skladWarehouse.skladWarehouseName,
              marketplaceType: "ozon",
            });
            if (ms?.status === "ok") {
              const stock = Number(ms.stock ?? 0);
              if (stock < d.requiredQty) shortages.push({ ...d, stock });
            } else if (ms?.status === "not_found") {
              shortages.push({ ...d, stock: 0 });
            }
            // multiple / store_not_found / ошибки чтения — не блокируем (нехватку не подтвердить)
          } catch { /* инфра-ошибка склада — не блокируем создание */ }
        });
        if (shortages.length) {
          if (!reuseExisting) await dbQuery("delete from ozon_assemblies where id = $1", [assemblyId]);
          const lines = shortages
            .sort((a, b) => String(a.article || "").localeCompare(String(b.article || "")))
            .map((s) => `${s.article || "—"} «${s.productName || "—"}»: на складе ${s.stock}, нужно ${s.requiredQty}`)
            .join("; ");
          const error = new Error(
            `Недостаточно товара на складе — эти позиции нельзя собрать. Занесите остатки на склад и создайте сборку заново. ${lines}`
          );
          error.statusCode = 409;
          throw error;
        }
      }

      const added = [];
      const failed = [];
      for (const postingNumber of postingNumbers) {
        if (existingPostingNumbers.has(postingNumber)) {
          added.push(postingNumber);
          continue;
        }
        try {
          const details = detailsByPosting.get(postingNumber) || await fetchPostingDetails(postingNumber, store.id);
          if (!details.postingNumber) throw new Error("Заказ не найден в Ozon");
          if (!isOzonPostingExclusiveToArticles(details.products || [], selectedArticleSet)) {
            throw new Error("В заказе есть товары вне выбранных артикулов");
          }
          if (details.status !== "awaiting_packaging") {
            throw new Error(`Статус заказа в Ozon: ${details.status || "unknown"}`);
          }
          if (Number(details.warehouseId || 0) !== Number(requestedWarehouseId)) {
            throw new Error(
              `Заказ относится к другому складу: ${details.warehouseName || details.deliveryMethodName || details.warehouseId || "неизвестно"}`
            );
          }
          await shipPostingToAwaitingDeliver(postingNumber, details.products, store.id);

          await dbQuery(
            `insert into ozon_assembly_orders (
              assembly_id, posting_number, order_created_at, ozon_status, label_barcode, store_id, store_name,
              warehouse_id, warehouse_name, delivery_method_id, delivery_method_name, created_at, updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now())`,
            [
              assemblyId,
              postingNumber,
              details.createdAt ? new Date(details.createdAt) : null,
              "awaiting_deliver",
              normalizeBarcode(details.labelBarcode || "") || null,
              store.id,
              store.name,
              requestedWarehouseId,
              details.warehouseName || selectedWarehouse.warehouseName || null,
              details.deliveryMethodId || null,
              details.deliveryMethodName || null,
            ]
          );

          const rows = details.products || [];
          let lineNo = 1;
          for (const product of rows) {
            await dbQuery(
              `insert into ozon_assembly_order_items (
                assembly_id, posting_number, line_no, offer_id, sku, name, quantity, scanned_qty, created_at, updated_at
              ) values ($1, $2, $3, $4, $5, $6, $7, 0, now(), now())`,
              [
                assemblyId,
                postingNumber,
                lineNo++,
                normalizeArticleKey(product.offerId),
                Number.isFinite(Number(product.sku)) ? Number(product.sku) : null,
                normalizeText(product.name || ""),
                Math.max(1, Number(product.quantity || 1)),
              ]
            );
          }

          added.push(postingNumber);
          existingPostingNumbers.add(postingNumber);
        } catch (err) {
          failed.push({
            postingNumber,
            error: err?.message || "Не удалось добавить заказ",
          });
        }
      }

      if (!added.length) {
        if (!reuseExisting) {
          await dbQuery("delete from ozon_assemblies where id = $1", [assemblyId]);
        }
        const error = new Error("Не удалось создать сборку");
        error.statusCode = 400;
        error.details = failed;
        throw error;
      }

      getNewOrdersCache(store.id).ts = 0;
      startLabelsJob(assemblyId, true).catch(() => {});
      notifyAssemblyUpdate(assemblyId, true);
      return {
        assemblyId,
        assemblyName,
        storeId: store.id,
        storeName: store.name,
        warehouseId: requestedWarehouseId,
        warehouseName: selectedWarehouse.warehouseName || null,
        locationCode,
        skladWarehouseId: skladWarehouse.skladWarehouseId,
        skladWarehouseName: skladWarehouse.skladWarehouseName,
        allowStockShortage,
        addedCount: added.length,
        failedCount: failed.length,
        added,
        failed,
        reusedAssembly: reuseExisting,
      };
    })();

    try {
      const result = await rememberRecentCreateRequest(recentCreateAssemblyRequests, requestKey, createPromise);
      return res.json(result);
    } catch (err) {
      if (err?.statusCode === 400) {
        return res.status(400).json({ error: err.message || "Не удалось создать сборку", failed: err.details || [] });
      }
      throw err;
    }
  })
);
app.get(
  `${API_ROOT}/assemblies/:assemblyId/orders`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const skipSync = String(req.query?.skipSync || "") === "1";
    if (!skipSync) {
      scheduleAssemblyOrdersSyncBackground(assemblyId, store.id, { maxAgeMs: 12000 });
    }
    const orders = await getAssemblyOrders(assemblyId);
    res.json({ updatedAt: new Date().toISOString(), orders });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/sync-awaiting-deliver`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const result = await syncAssemblyWithAwaitingDeliverFromOzon(assemblyId, store.id);
    res.json({ ok: true, ...result });
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/settings`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const exists = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!exists) return res.status(404).json({ error: "Сборка не найдена" });
    const skipSync = String(req.query?.skipSync || "") === "1";
    if (!skipSync) {
      scheduleAssemblyOrdersSyncBackground(assemblyId, store.id, { maxAgeMs: 12000 });
    }
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const totalResult = await dbQuery(
      `select count(*)::int as total,
              count(*) filter (where collected_at is not null)::int as collected,
              count(*) filter (where collected_at is not null and sent_to_delivering_at is null)::int as ready_to_ship
       from ozon_assembly_orders
       where assembly_id = $1
         and sent_to_delivering_at is null`,
      [assemblyId]
    );
    const total = Number(totalResult.rows?.[0]?.total || 0);
    const collected = Number(totalResult.rows?.[0]?.collected || 0);
    const readyToShip = Number(totalResult.rows?.[0]?.ready_to_ship || 0);
    const assemblyLocation = normalizeLocationCode(assembly.location_code);
    const employees = (await listEmployees())
      .filter((row) => !assemblyLocation || normalizeLocationCode(row.location_code) === assemblyLocation)
      .map(toEmployeeUser);
    const employeeIds = new Set(employees.map((user) => Number(user.id)));
    const accessUserIds = (await getAssemblyAccessUserIds(assemblyId))
      .filter((userId) => !assemblyLocation || employeeIds.has(Number(userId)));
    const progress = await getAssemblyProgress(assemblyId, assembly.access_mode);
    const assignmentParts = await listAssemblyAssignmentParts(assemblyId);

    res.json({
      settings: {
        assemblyId: assembly.id,
        assemblyName: assembly.name,
        storeId: assembly.store_id || null,
        storeName: resolveStoreStrict(assembly.store_id)?.name || assembly.store_name || null,
        locationCode: normalizeLocationCode(assembly.location_code),
        skladWarehouseId: Number(assembly.sklad_warehouse_id || 0) || null,
        skladWarehouseName: assembly.sklad_warehouse_name || null,
        accessMode: assembly.access_mode,
        stockMethodEnabled: isStockMethodEnabled(assembly),
        allowStockShortage: isAllowStockShortage(assembly),
        allowNoScan: assembly.allow_no_scan === true,
        partialSafeScan: assembly.partial_safe_scan === true,
        desktopPrint: assembly.desktop_print === true,
        employeeWorkflowMode: normalizeEmployeeWorkflowMode(assembly.employee_workflow_mode),
        employeeWorkflowStage: normalizeEmployeeWorkflowStage(assembly.employee_workflow_stage),
        productPickingStartedAt: assembly.product_picking_started_at || null,
        productPickingStartedBy:
          assembly.product_picking_started_by != null ? Number(assembly.product_picking_started_by) : null,
        labelsStatus: assembly.labels_status,
        labelsTotal: total,
        labelsLoaded: Math.min(Number(assembly.labels_loaded || 0), total),
        labelsError: assembly.labels_error || null,
        labelsStartedAt: assembly.labels_started_at,
        labelsFinishedAt: assembly.labels_finished_at,
        shipment: getAssemblyShipmentState(assembly, readyToShip),
        assignmentParts,
      },
      totals: {
        total,
        collected,
        remaining: Math.max(0, total - collected),
        readyToShip: Math.max(0, readyToShip),
      },
      accessUserIds,
      employees,
      progress,
    });
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/labels/diagnostics`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const diagnostics = await getAssemblyLabelDiagnostics(assemblyId);
    if (!diagnostics) return res.status(404).json({ error: "Сборка не найдена" });
    res.json({ diagnostics });
  })
);

app.patch(
  `${API_ROOT}/assemblies/:assemblyId/settings`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const hasAccessMode = typeof req.body?.accessMode === "string";
    const mode = hasAccessMode ? normalizeText(req.body?.accessMode) : normalizeText(assembly.access_mode);
    if (hasAccessMode && !ensureValidAccessMode(mode)) return res.status(400).json({ error: "Некорректный режим доступа" });
    const workflowModeInput =
      typeof req.body?.employeeWorkflowMode === "string"
        ? req.body.employeeWorkflowMode
        : typeof req.body?.workflowMode === "string"
          ? req.body.workflowMode
          : null;
    const hasWorkflowMode = workflowModeInput !== null;
    if (hasWorkflowMode && !EMPLOYEE_WORKFLOW_MODES.has(normalizeText(workflowModeInput))) {
      return res.status(400).json({ error: "Некорректный режим сборки" });
    }
    const workflowMode = hasWorkflowMode
      ? normalizeEmployeeWorkflowMode(workflowModeInput)
      : normalizeEmployeeWorkflowMode(assembly.employee_workflow_mode);
    const hasStockMethod = typeof req.body?.stockMethodEnabled === "boolean";
    const stockMethodEnabled = hasStockMethod
      ? req.body.stockMethodEnabled === true
      : isStockMethodEnabled(assembly);
    const hasAllowNoScan = typeof req.body?.allowNoScan === "boolean";
    const hasPartialSafeScan = typeof req.body?.partialSafeScan === "boolean";
    const hasDesktopPrint = typeof req.body?.desktopPrint === "boolean";
    if (req.body?.allowNoScan === true && req.body?.partialSafeScan === true) {
      return res.status(400).json({ error: "Полный и частичный режимы без сканирования нельзя включить одновременно" });
    }
    if (!hasAccessMode && !hasWorkflowMode && !hasStockMethod && !hasAllowNoScan && !hasPartialSafeScan && !hasDesktopPrint) {
      return res.status(400).json({ error: "Нет изменений" });
    }
    if (hasAllowNoScan) {
      await dbQuery("update ozon_assemblies set allow_no_scan = $2, partial_safe_scan = case when $2 then false else partial_safe_scan end, updated_at = now() where id = $1", [
        assemblyId,
        req.body.allowNoScan === true,
      ]);
      assembly.allow_no_scan = req.body.allowNoScan === true;
      if (assembly.allow_no_scan) assembly.partial_safe_scan = false;
      if (!hasAccessMode && !hasWorkflowMode && !hasStockMethod && !hasPartialSafeScan && !hasDesktopPrint) {
        notifyAssemblyUpdate(assemblyId, true);
        return res.json({
          ok: true,
          allowNoScan: assembly.allow_no_scan,
          partialSafeScan: assembly.partial_safe_scan === true,
        });
      }
    }
    if (hasPartialSafeScan) {
      await dbQuery("update ozon_assemblies set partial_safe_scan = $2, allow_no_scan = case when $2 then false else allow_no_scan end, updated_at = now() where id = $1", [
        assemblyId,
        req.body.partialSafeScan === true,
      ]);
      assembly.partial_safe_scan = req.body.partialSafeScan === true;
      if (assembly.partial_safe_scan) assembly.allow_no_scan = false;
      if (!hasAccessMode && !hasWorkflowMode && !hasStockMethod && !hasAllowNoScan && !hasDesktopPrint) {
        notifyAssemblyUpdate(assemblyId, true);
        return res.json({
          ok: true,
          allowNoScan: assembly.allow_no_scan === true,
          partialSafeScan: assembly.partial_safe_scan,
        });
      }
    }
    if (typeof req.body?.desktopPrint === "boolean") {
      await dbQuery("update ozon_assemblies set desktop_print = $2, updated_at = now() where id = $1", [
        assemblyId,
        req.body.desktopPrint === true,
      ]);
      assembly.desktop_print = req.body.desktopPrint === true;
      if (!hasAccessMode && !hasWorkflowMode && !hasStockMethod && !hasAllowNoScan && !hasPartialSafeScan) {
        notifyAssemblyUpdate(assemblyId, true);
        return res.json({ ok: true, desktopPrint: assembly.desktop_print });
      }
    }
    const workflowModeChanged =
      hasWorkflowMode &&
      normalizeEmployeeWorkflowMode(assembly.employee_workflow_mode) !== workflowMode;
    const stockMethodChanged = hasStockMethod && isStockMethodEnabled(assembly) !== stockMethodEnabled;
    if ((workflowModeChanged || stockMethodChanged) && (await assemblyHasActiveEmployeeWork(assemblyId))) {
      return res.status(409).json({ error: "Нельзя менять метод, пока сотрудник уже работает со сборкой" });
    }
    if (hasAccessMode) {
      await setAssemblyAccessMode(assemblyId, mode);
    }
    if (mode === "hidden") {
      await dbQuery("delete from ozon_assembly_access_users where assembly_id = $1", [assemblyId]);
      await closeOpenEmployeeSessionsForAssembly(assemblyId, "cancelled");
      await clearAssemblyAssignmentParts(assemblyId);
      await dbQuery(
        `update ozon_assembly_orders
         set assigned_user_id = null, assignment_part_id = null, assigned_at = null, updated_at = now()
         where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
        [assemblyId]
      );
      await setAssemblyEmployeeWorkflowStage(assemblyId, EMPLOYEE_WORKFLOW_ORDERS_ONLY);
      await resetEmployeePickingForAssembly(assemblyId, PICKING_STATUS_NOT_REQUIRED);
    } else if (mode !== "selected_split") {
      await closeOpenEmployeeSessionsForAssembly(assemblyId, "cancelled");
      await dbQuery(
        `update ozon_assembly_orders
         set assigned_user_id = null, assigned_at = null, updated_at = now()
         where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
        [assemblyId]
      );
    }
    if (hasWorkflowMode) {
      await setAssemblyEmployeeWorkflowMode(assemblyId, workflowMode);
      const nextStage = getEmployeeWorkflowStageForMode(workflowMode, mode);
      await setAssemblyEmployeeWorkflowStage(assemblyId, nextStage);
      await resetEmployeePickingForAssembly(
        assemblyId,
        nextStage === EMPLOYEE_WORKFLOW_PICK_PENDING ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED
      );
    }
    if (hasStockMethod) {
      await setAssemblyStockMethodEnabled(assemblyId, stockMethodEnabled);
    }
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ ok: true });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/access`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const userIds = Array.isArray(req.body?.userIds)
      ? req.body.userIds.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)
      : [];
    const assemblyLocation = normalizeLocationCode(assembly.location_code);
    if (!assemblyLocation) {
      return res.status(409).json({ error: "Сначала укажите город сборки" });
    }
    if (userIds.length) {
      const configuredUsers = await dbQuery(
        `select id
         from users
         where id = any($1::bigint[])
           and role = $2
           and location_code = $3`,
        [userIds, ROLE_EMPLOYEE, assemblyLocation]
      );
      const configuredIds = new Set((configuredUsers.rows || []).map((row) => Number(row.id)));
      const invalidIds = userIds.filter((id) => !configuredIds.has(Number(id)));
      if (invalidIds.length) {
        return res.status(400).json({ error: "В сборке можно выбирать только сотрудников того же города" });
      }
    }
    await setAssemblyAccessUsers(assemblyId, userIds);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ ok: true });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/split`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const body = req.body || {};
    const userIds = await getAssemblyAccessUserIds(assemblyId);
    if (!userIds.length) return res.status(400).json({ error: "Не выбраны сотрудники" });
    if (await assemblyHasStartedPackingWork(assemblyId)) {
      return res.status(409).json({ error: "Нельзя перераспределять сборку, если упаковщик уже начал сборку заказов" });
    }
    if (await assemblyHasBlockingRedistributeWork(assemblyId)) {
      return res.status(409).json({ error: "Нельзя перераспределять сборку, пока сотрудник ещё собирает товар" });
    }
    const requestedParts =
      Array.isArray(body?.parts) && body.parts.length
        ? normalizeAssemblyAssignmentPartsInput(body.parts, userIds)
        : [...userIds].map((userId, index) => ({
            partNo: index + 1,
            roleMode: ASSIGNMENT_PART_MODE_PICK_PACK,
            pickerUserId: null,
            packerUserId: Number(userId),
            tableNo: null,
          }));
    const settings = await getAssembly(assemblyId);
    const hasSplitRoles = requestedParts.some(
      (part) => normalizeAssignmentPartMode(part?.roleMode) === ASSIGNMENT_PART_MODE_SPLIT
    );
    if (
      hasSplitRoles &&
      normalizeEmployeeWorkflowMode(settings?.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING
    ) {
      return res.status(400).json({ error: "Для раздельной сборки включите «Метод сборки»" });
    }
    if (hasSplitRoles && !isStockMethodEnabled(settings)) {
      return res.status(400).json({ error: "Для раздельной сборки включите «Складской метод»" });
    }
    await setAssemblyAccessMode(assemblyId, "selected_split");
    await closeOpenEmployeeSessionsForAssembly(assemblyId, "reassigned");
    await clearAssemblyAssignmentParts(assemblyId);
    const parts = await createAssemblyAssignmentParts(assemblyId, requestedParts, req.auth.id);
    const result = await assignAssemblyOrdersToParts(assemblyId, parts, "split");
    const nextStage = getEmployeeWorkflowStageForMode(settings?.employee_workflow_mode, "selected_split");
    await createAssemblyAssignmentSessionsFromParts(assemblyId, req.auth.id, "split", result.parts, nextStage);
    await setAssemblyEmployeeWorkflowStage(assemblyId, nextStage);
    await resetEmployeePickingForAssembly(
      assemblyId,
      nextStage === EMPLOYEE_WORKFLOW_PICK_PENDING ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED
    );
    notifyAssemblyUpdate(assemblyId, true);
    res.json({
      total: result.total,
      assigned: result.assigned,
      parts: result.parts,
    });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/redistribute`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const body = req.body || {};
    const userIds = await getAssemblyAccessUserIds(assemblyId);
    if (!userIds.length) return res.status(400).json({ error: "Не выбраны сотрудники" });
    if (await assemblyHasStartedPackingWork(assemblyId)) {
      return res.status(409).json({ error: "Нельзя перераспределять сборку, если упаковщик уже начал сборку заказов" });
    }
    if (await assemblyHasBlockingRedistributeWork(assemblyId)) {
      return res.status(409).json({ error: "Нельзя перераспределять сборку, пока сотрудник ещё собирает товар" });
    }
    const carryover = await captureAssemblyRedistributeCarryover(assemblyId);
    const requestedParts =
      Array.isArray(body?.parts) && body.parts.length
        ? normalizeAssemblyAssignmentPartsInput(body.parts, userIds)
        : [...userIds].map((userId, index) => ({
            partNo: index + 1,
            roleMode: ASSIGNMENT_PART_MODE_PICK_PACK,
            pickerUserId: null,
            packerUserId: Number(userId),
            tableNo: null,
          }));
    const settings = await getAssembly(assemblyId);
    const hasSplitRoles = requestedParts.some(
      (part) => normalizeAssignmentPartMode(part?.roleMode) === ASSIGNMENT_PART_MODE_SPLIT
    );
    if (
      hasSplitRoles &&
      normalizeEmployeeWorkflowMode(settings?.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING
    ) {
      return res.status(400).json({ error: "Для раздельной сборки включите «Метод сборки»" });
    }
    if (hasSplitRoles && !isStockMethodEnabled(settings)) {
      return res.status(400).json({ error: "Для раздельной сборки включите «Складской метод»" });
    }
    await setAssemblyAccessMode(assemblyId, "selected_split");
    await closeOpenEmployeeSessionsForAssembly(assemblyId, "reassigned");
    await clearAssemblyAssignmentParts(assemblyId);
    const parts = await createAssemblyAssignmentParts(assemblyId, requestedParts, req.auth.id);
    const result = await assignAssemblyOrdersToParts(assemblyId, parts, "redistribute");
    const nextStage = getEmployeeWorkflowStageForMode(settings?.employee_workflow_mode, "selected_split");
    const createdSessions = await createAssemblyAssignmentSessionsFromParts(
      assemblyId,
      req.auth.id,
      "redistribute",
      result.parts,
      nextStage
    );
    await setAssemblyEmployeeWorkflowStage(assemblyId, nextStage);
    await resetEmployeePickingForAssembly(
      assemblyId,
      nextStage === EMPLOYEE_WORKFLOW_PICK_PENDING ? PICKING_STATUS_PENDING : PICKING_STATUS_NOT_REQUIRED
    );
    await applyAssemblyRedistributeCarryover(assemblyId, carryover, result.parts, createdSessions, settings);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({
      total: result.total,
      assigned: result.assigned,
      parts: result.parts,
    });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/product-picking/start`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const accessUserIds = await getAssemblyAccessUserIds(assemblyId);
    if (!accessUserIds.length) {
      return res.status(400).json({ error: "Сначала распределите сборку между сотрудниками" });
    }
    if (normalizeText(assembly.access_mode) !== "selected_split") {
      return res.status(400).json({ error: "Сборка должна быть доступна выбранным сотрудникам" });
    }
    if (normalizeEmployeeWorkflowMode(assembly.employee_workflow_mode) !== EMPLOYEE_WORKFLOW_MODE_PICKING) {
      return res.status(400).json({ error: "Для этой сборки включён старый метод" });
    }
    await setAssemblyEmployeeWorkflowStage(assemblyId, EMPLOYEE_WORKFLOW_PICK_ACTIVE, req.auth.id);
    await resetEmployeePickingForAssembly(assemblyId, PICKING_STATUS_PENDING);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ ok: true });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/reset-access`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    await setAssemblyAccessMode(assemblyId, "hidden");
    await closeOpenEmployeeSessionsForAssembly(assemblyId, "cancelled");
    await setAssemblyEmployeeWorkflowStage(assemblyId, EMPLOYEE_WORKFLOW_ORDERS_ONLY);
    await resetEmployeePickingForAssembly(assemblyId, PICKING_STATUS_NOT_REQUIRED);
    await dbQuery("delete from ozon_assembly_access_users where assembly_id = $1", [assemblyId]);
    await clearAssemblyAssignmentParts(assemblyId);
    await dbQuery(
      `update ozon_assembly_orders
       set assigned_user_id = null, assignment_part_id = null, assigned_at = null, updated_at = now()
       where assembly_id = $1 and collected_at is null and sent_to_delivering_at is null`,
      [assemblyId]
    );
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ ok: true });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/labels`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });
    const force = req.body?.force === true;
    startLabelsJob(assemblyId, force).catch(() => {});
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ ok: true });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/shipment/prepare`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const postingNumbers = await listCollectedNotSentPostingNumbers(assemblyId);
    if (!postingNumbers.length) {
      return res.status(400).json({ error: "Нет собранных заказов для формирования отгрузки" });
    }

    const shipmentCode = buildShipmentCode();
    const labelPdfBytes = await createShipmentLabelPdf({
      assembly,
      shipmentCode,
      postingNumbers,
    });
    const labelKey = buildShipmentLabelKey(assemblyId, shipmentCode);
    const storedLabelKey = await uploadPdfToS3(labelKey, labelPdfBytes);

    await dbQuery(
      `update ozon_assemblies
       set shipment_code = $2,
           shipment_label_s3_key = $3,
           shipment_label_generated_at = now(),
           shipment_prepared_at = now(),
           shipment_confirmed_at = null,
           shipment_confirmed_by = null,
           shipment_postings_count = $4,
           shipment_posting_numbers = $5::text[],
           updated_at = now()
       where id = $1`,
      [assemblyId, shipmentCode, storedLabelKey, postingNumbers.length, postingNumbers]
    );

    const updatedAssembly = await getAssembly(assemblyId);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({
      ok: true,
      shipment: getAssemblyShipmentState(updatedAssembly, postingNumbers.length),
    });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/shipment/confirm`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const requestedPostingNumbers = Array.isArray(req.body?.postingNumbers)
      ? normalizePostingNumbers(req.body.postingNumbers)
      : [];
    const draftPostingNumbers = normalizePostingNumbers(assembly.shipment_posting_numbers);
    let postingNumbers = requestedPostingNumbers.length
      ? requestedPostingNumbers
      : draftPostingNumbers.length
        ? draftPostingNumbers
        : await listCollectedNotSentPostingNumbers(assemblyId);

    const pending = new Set(await listCollectedNotSentPostingNumbers(assemblyId));
    postingNumbers = postingNumbers.filter((postingNumber) => pending.has(postingNumber));
    if (!postingNumbers.length) {
      return res.status(400).json({ error: "Нет заказов для подтверждения отгрузки" });
    }

    const result = await sendAssemblyPostingsToDelivering(assemblyId, assembly.store_id || null, postingNumbers);
    if (result.sent.length) {
      await dbQuery(
        `update ozon_assemblies
         set shipment_confirmed_at = now(),
             shipment_confirmed_by = $2,
             updated_at = now()
         where id = $1`,
        [assemblyId, req.auth.id]
      );
    }

    const remainingToShip = await listCollectedNotSentPostingNumbers(assemblyId);
    const updatedAssembly = await getAssembly(assemblyId);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({
      sentCount: result.sent.length,
      failedCount: result.failed.length,
      sent: result.sent,
      failed: result.failed,
      shipment: getAssemblyShipmentState(updatedAssembly, remainingToShip.length),
    });
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/labels.pdf`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const expectedPostingNumbers = await getAssemblyShipmentExpectedPostingNumbers(assemblyId, assembly);

    const act = await buildFbsActPdfForStore(store.id, {
      expectedPostingNumbers,
    });

    if (act.postingNumbers.length) {
      await dbQuery(
        `update ozon_assemblies
         set shipment_posting_numbers = $2::text[],
             shipment_postings_count = $3,
             shipment_prepared_at = coalesce(shipment_prepared_at, now()),
             updated_at = now()
         where id = $1`,
        [assemblyId, act.postingNumbers, act.postingNumbers.length]
      );
    }

    const filename = `ozon-act-${assemblyId}-${act.actId}.pdf`;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename=\"${filename}\"`);
    res.send(act.pdfBuffer);
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/barcode.png`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const expectedPostingNumbers = await getAssemblyShipmentExpectedPostingNumbers(assemblyId, assembly);
    const act = await ensureReadyFbsAct(store.id, { expectedPostingNumbers });
    const barcodeBuffer = await getFbsActBarcodePngBuffer(act.actId, store.id);

    const filename = `ozon-act-barcode-${assemblyId}-${act.actId}.png`;
    res.setHeader("Content-Type", "image/png");
    res.setHeader("Content-Disposition", `inline; filename=\"${filename}\"`);
    res.send(barcodeBuffer);
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/order-labels.pdf`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const labels = await buildStoredOrderLabelsPdf(assemblyId);
    if (labels.totalStored < labels.totalOrders) {
      return res.status(409).json({
        error: `Этикетки загружены не полностью: ${labels.totalStored} из ${labels.totalOrders}. Нажмите «Обновить этикетки» и повторите печать.`,
      });
    }
    if (labels.count < labels.totalOrders) {
      return res.status(500).json({
        error: `Не удалось прочитать все сохраненные этикетки: ${labels.count} из ${labels.totalOrders}. Нажмите «Обновить этикетки» и повторите печать.`,
      });
    }
    if (!labels.buffer?.length) {
      return res.status(400).json({
        error: labels.totalStored > 0
          ? "Сохраненные этикетки не удалось прочитать. Нажмите «Обновить этикетки» и повторите печать."
          : "Этикетки заказов еще не загружены. Нажмите «Обновить этикетки» и дождитесь загрузки.",
      });
    }
    const filename = `ozon-order-labels-${assemblyId}.pdf`;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename=\"${filename}\"`);
    res.setHeader("X-Ozon-Labels-Merged", String(labels.count));
    res.send(labels.buffer);
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/picking-list.pdf`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const buffer = await buildOzonPickingListPdf(assembly);
    if (!buffer?.length) {
      return res.status(400).json({ error: "Нет товаров для листа подбора" });
    }

    const filename = `ozon-picking-list-${assemblyId}.pdf`;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename=\"${filename}\"`);
    res.send(Buffer.from(buffer));
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/container-labels.pdf`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const expectedPostingNumbers = await getAssemblyShipmentExpectedPostingNumbers(assemblyId, assembly);
    const act = await ensureReadyFbsAct(store.id, { expectedPostingNumbers });
    const containerBuffer = await getFbsActContainerLabelsPdfBuffer(act.actId, store.id);
    const filename = `ozon-container-labels-${assemblyId}-${act.actId}.pdf`;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename=\"${filename}\"`);
    res.send(containerBuffer);
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/digital-act.pdf`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const expectedPostingNumbers = await getAssemblyShipmentExpectedPostingNumbers(assemblyId, assembly);
    const act = await ensureReadyFbsAct(store.id, { expectedPostingNumbers });
    const digitalPdf = await getFbsDigitalActPdfBuffer(act.actId, store.id);
    const filename = `ozon-digital-act-${assemblyId}-${act.actId}.pdf`;
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename=\"${filename}\"`);
    res.send(digitalPdf);
  })
);

app.get(
  `${API_ROOT}/assemblies/:assemblyId/shipment/digital-status`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const storeFromQuery = resolveStoreStrict(req.query?.storeId);
    const store = storeFromQuery || getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    const expectedPostingNumbers = await getAssemblyShipmentExpectedPostingNumbers(assemblyId, assembly);
    const act = await ensureReadyFbsAct(store.id, { expectedPostingNumbers });
    const digitalStatus = await getFbsDigitalActStatus(act.actId, store.id);
    res.json({
      actId: act.actId,
      status: digitalStatus?.status || null,
      raw: digitalStatus || null,
    });
  })
);

app.post(
  `${API_ROOT}/assemblies/:assemblyId/send`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => {
    const store = getRequiredStoreFromRequest(req);
    const assemblyId = normalizeText(req.params.assemblyId);
    const assembly = await getAdminAssemblyInStore(assemblyId, store.id);
    if (!assembly) return res.status(404).json({ error: "Сборка не найдена" });

    let postingNumbers = [];
    if (Array.isArray(req.body?.postingNumbers) && req.body.postingNumbers.length) {
      postingNumbers = Array.from(
        new Set(req.body.postingNumbers.map((value) => normalizeText(value)).filter(Boolean))
      );
    } else {
      const result = await dbQuery(
        `select posting_number
         from ozon_assembly_orders
         where assembly_id = $1 and collected_at is not null and sent_to_delivering_at is null`,
        [assemblyId]
      );
      postingNumbers = result.rows.map((row) => row.posting_number);
    }
    if (!postingNumbers.length) {
      return res.status(400).json({ error: "Нет заказов для передачи в статус отгружены" });
    }

    const result = await sendAssemblyPostingsToDelivering(assemblyId, assembly.store_id || null, postingNumbers);
    if (result.sent.length) {
      await dbQuery(
        `update ozon_assemblies
         set shipment_confirmed_at = coalesce(shipment_confirmed_at, now()),
             shipment_confirmed_by = coalesce(shipment_confirmed_by, $2),
             updated_at = now()
         where id = $1`,
        [assemblyId, req.auth.id]
      );
    }

    notifyAssemblyUpdate(assemblyId, true);
    res.json({
      sentCount: result.sent.length,
      failedCount: result.failed.length,
      sent: result.sent,
      failed: result.failed,
    });
  })
);

app.get(
  `${API_ROOT}/employee/assemblies`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.assemblies", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const employeeLocation = await getConfiguredEmployeeLocation(req.auth.id);
    if (employeeLocation.error) {
      return res.status(employeeLocation.errorStatus || 409).json({ error: employeeLocation.error });
    }
    const assemblyResult = await dbQuery(
      `select id, name, store_id, store_name, access_mode, labels_status, labels_loaded, labels_total,
              location_code, sklad_warehouse_id, sklad_warehouse_name,
              employee_workflow_mode, employee_workflow_stage, allow_no_scan, partial_safe_scan, desktop_print
       from ozon_assemblies a
       where access_mode = 'selected_split'
         and location_code = $1
         and (
           timezone('Europe/Moscow', created_at)::date >= timezone('Europe/Moscow', now())::date - 1
           or exists (
             select 1
             from ozon_employee_assembly_sessions es
             where es.assembly_id = a.id
               and es.user_id = $2
               and es.finished_at is null
           )
         )
       order by created_at desc`,
      [employeeLocation.locationCode, req.auth.id]
    );
    const rows = assemblyResult.rows || [];
    if (!rows.length) return res.json({ assemblies: [] });

    const allowedResult = await dbQuery(
      "select assembly_id from ozon_assembly_access_users where user_id = $1",
      [req.auth.id]
    );
    const allowedSet = new Set(allowedResult.rows.map((row) => row.assembly_id));
    const activeSession = await getEmployeeActiveAssemblySession(req.auth.id);

    const out = [];
    for (const row of rows) {
      const mode = row.access_mode;
      if (!allowedSet.has(row.id)) continue;
      scheduleAssemblyOrdersSyncBackground(row.id, row.store_id || null, { maxAgeMs: 12000 });
      let session = await getEmployeeOpenAssemblySession(row.id, req.auth.id);
      const assignmentPart = await getAssemblyAssignmentPartForSession(session);
      const hasAssignmentParts = !session ? await assemblyHasAssignmentParts(row.id) : Number(session.assignment_part_id || 0) > 0;
      const countResult = await dbQuery(
        `select count(*)::int as total,
                count(*) filter (where collected_at is null)::int as remaining,
                count(*) filter (where collected_at is not null)::int as collected
         from ozon_assembly_orders
         where assembly_id = $1
           and sent_to_delivering_at is null
           ${
             Number(session?.assignment_part_id || 0) > 0
               ? "and assignment_part_id = $2"
               : "and assigned_user_id = $2"
           }`,
        [row.id, Number(session?.assignment_part_id || 0) > 0 ? Number(session.assignment_part_id) : req.auth.id]
      );
      const total = Number(countResult.rows?.[0]?.total || 0);
      const remaining = Number(countResult.rows?.[0]?.remaining || 0);
      const collected = Number(countResult.rows?.[0]?.collected || 0);
      if (total < 1 && !(normalizeSessionRole(session?.session_role) === SESSION_ROLE_PACK && assignmentPart)) continue;
      if (!session && remaining > 0 && !hasAssignmentParts) {
        session = await ensureEmployeeAssignmentSessionForAssembly(row.id, req.auth.id, remaining);
      }
      if (!session && hasAssignmentParts) continue;
      session = await ensureEmployeeSessionPickingStatus(row, session);
      const nextAssignmentPart = assignmentPart || (await getAssemblyAssignmentPartForSession(session));
      const workflow = buildEmployeeWorkflowState(row, session, activeSession, nextAssignmentPart);
      out.push({
        id: row.id,
        name: row.name,
        storeId: row.store_id || null,
        storeName: resolveStoreStrict(row.store_id)?.name || row.store_name || null,
        locationCode: normalizeLocationCode(row.location_code),
        skladWarehouseId: Number(row.sklad_warehouse_id || 0) || null,
        skladWarehouseName: row.sklad_warehouse_name || null,
        accessMode: mode,
        labelsStatus: row.labels_status,
        labelsLoaded: Number(row.labels_loaded || 0),
        labelsTotal: Number(row.labels_total || 0),
        workflowMode: workflow.workflowMode,
        workflowStage: workflow.workflowStage,
        currentStage: workflow.currentStage,
        sessionRole: workflow.sessionRole,
        tableNo: workflow.tableNo,
        handoffStatus: workflow.handoffStatus,
        productPickingRequired: workflow.productPickingRequired,
        productPickingActive: workflow.productPickingActive,
        allowNoScan: workflow.allowNoScan,
        total,
        remaining,
        collected,
        session: toEmployeeSessionPayload(session),
        assignmentPart: nextAssignmentPart,
        canStart: Boolean(workflow.canStartPicking || workflow.canStartPacking),
        canStartPicking: workflow.canStartPicking,
        canFinishPicking: workflow.canFinishPicking,
        canConfirmTablePlacement: workflow.canConfirmTablePlacement,
        canStartPacking: workflow.canStartPacking,
        canWork: workflow.canWork,
        isWaitingAdmin: workflow.isWaitingAdmin,
        isWaitingForPicker: workflow.isWaitingForPicker,
        isWaitingForTable: workflow.isWaitingForTable,
        hasAnotherActiveSession: Boolean(activeSession) && String(activeSession.assembly_id) !== String(row.id),
      });
    }
    res.json({ assemblies: out });
  })
);

app.get(
  `${API_ROOT}/employee/assemblies/:assemblyId/session`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.session", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) {
      return res.status(context.errorStatus || 400).json({ error: context.error });
    }
    const assembly = context.assembly;
    const workflow = context.workflow || buildEmployeeWorkflowState(assembly, context.session, context.activeSession, context.assignmentPart);
    res.json({
      assembly: {
        id: assembly.id,
        name: assembly.name,
        storeId: assembly.store_id || null,
        storeName: resolveStoreStrict(assembly.store_id)?.name || assembly.store_name || null,
        locationCode: normalizeLocationCode(assembly.location_code),
        skladWarehouseId: Number(assembly.sklad_warehouse_id || 0) || null,
        skladWarehouseName: assembly.sklad_warehouse_name || null,
        accessMode: assembly.access_mode,
        workflowMode: workflow.workflowMode,
        workflowStage: workflow.workflowStage,
        productPickingStartedAt: assembly.product_picking_started_at || null,
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
        productPickingRequired: workflow.productPickingRequired,
        productPickingActive: workflow.productPickingActive,
        allowNoScan: workflow.allowNoScan,
        canStartPicking: workflow.canStartPicking,
        canFinishPicking: workflow.canFinishPicking,
        canConfirmTablePlacement: workflow.canConfirmTablePlacement,
        canStartPacking: workflow.canStartPacking,
        isWaitingAdmin: workflow.isWaitingAdmin,
        isWaitingForPicker: workflow.isWaitingForPicker,
        isWaitingForTable: workflow.isWaitingForTable,
      },
      activeSession: context.activeSession ? toEmployeeSessionPayload(context.activeSession) : null,
      canStart: Boolean(workflow.canStartPicking || workflow.canStartPacking),
      canWork: workflow.canWork,
      hasAnotherActiveSession: Boolean(context.activeSession) && String(context.activeSession.assembly_id) !== String(assemblyId),
    });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/start`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.start", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) {
      return res.status(context.errorStatus || 400).json({ error: context.error });
    }
    const workflow = context.workflow || buildEmployeeWorkflowState(context.assembly, context.session, context.activeSession, context.assignmentPart);
    const starter =
      workflow.productPickingRequired && workflow.pickingStatus !== PICKING_STATUS_FINISHED
        ? startEmployeeProductPicking
        : startEmployeeAssemblySession;
    const { session, pickItems = [], error } = await starter(assemblyId, req.auth.id);
    if (error) return res.status(409).json({ error });
    const activeSession = await getEmployeeActiveAssemblySession(req.auth.id);
    const nextAssignmentPart = await getAssemblyAssignmentPartForSession(session);
    const nextWorkflow = buildEmployeeWorkflowState(context.assembly, session, activeSession, nextAssignmentPart);
    const pendingPickItems =
      nextWorkflow.currentStage === "picking"
        ? pickItems.filter((row) => Number(row?.picked_qty || 0) < Number(row?.required_qty || 0))
        : [];
    if (nextWorkflow.currentStage === "picking") {
      void ensureEmployeePickScanIndexWarm(
        { ...session, store_id: context.assembly?.store_id || session?.store_id || null },
        pendingPickItems,
        getSkladWarehouseOptionsFromAssembly(context.assembly)
      );
    }
    const enrichedPickItems =
      nextWorkflow.currentStage === "picking" ? await enrichEmployeePickItemsWithMsCells(pendingPickItems, context.assembly) : [];
    await warmWarehouseNames(enrichedPickItems);
    await attachOzonPickItemImages(enrichedPickItems);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({
      ok: true,
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
        productPickingRequired: nextWorkflow.productPickingRequired,
        productPickingActive: nextWorkflow.productPickingActive,
        allowNoScan: nextWorkflow.allowNoScan,
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
          ? enrichedPickItems.map(toEmployeePickItemPayload)
          : [],
    });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/table-confirm`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.table_confirm", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const { session, part, error } = await confirmEmployeeAssemblyTablePlacement(assemblyId, req.auth.id);
    if (error) return res.status(409).json({ error });
    const assembly = await getAssembly(assemblyId);
    const activeSession = await getEmployeeActiveAssemblySession(req.auth.id);
    const workflow = buildEmployeeWorkflowState(assembly, session, activeSession, part);
    notifyAssemblyUpdate(assemblyId, true);
    res.json({
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
        productPickingRequired: workflow.productPickingRequired,
        productPickingActive: workflow.productPickingActive,
        allowNoScan: workflow.allowNoScan,
        canStartPicking: workflow.canStartPicking,
        canFinishPicking: workflow.canFinishPicking,
        canConfirmTablePlacement: workflow.canConfirmTablePlacement,
        canStartPacking: workflow.canStartPacking,
        isWaitingAdmin: workflow.isWaitingAdmin,
        isWaitingForPicker: workflow.isWaitingForPicker,
        isWaitingForTable: workflow.isWaitingForTable,
      },
    });
  })
);

app.get(
  `${API_ROOT}/employee/assemblies/:assemblyId/pick-items`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.pick_items", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });
    if (!context.workflow?.productPickingRequired) {
      return res.status(409).json({ error: "Для этой сборки сборка товара не активирована" });
    }
    if (context.workflow?.isWaitingAdmin) {
      return res.status(409).json({ error: "Администратор ещё не запустил сборку товара" });
    }
    if (
      normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
      !context.session?.picking_started_at ||
      context.session?.picking_finished_at
    ) {
      return res.status(409).json({ error: "Сначала нажмите «Старт»" });
    }
    const rows = await ensureEmployeeProductPickItems(context.session);
    const refreshedSession = (await refreshEmployeePickingSessionSummary(context.session.id)) || context.session;
    const pendingRows = rows.filter((row) => Number(row?.picked_qty || 0) < Number(row?.required_qty || 0));
    const rowsWithCells = await enrichEmployeePickItemsWithMsCells(pendingRows, context.assembly);
    void ensureEmployeePickScanIndexWarm(
      { ...context.session, store_id: context.assembly?.store_id || context.session?.store_id || null },
      rowsWithCells,
      getSkladWarehouseOptionsFromAssembly(context.assembly)
    );
    await warmWarehouseNames(rowsWithCells);
    await attachOzonPickItemImages(rowsWithCells);
    res.json({
      items: rowsWithCells.map(toEmployeePickItemPayload),
      session: toEmployeeSessionPayload(refreshedSession),
    });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/pick-scan`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.pick_scan", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });
    if (!context.workflow?.productPickingRequired) {
      return res.status(409).json({ error: "Для этой сборки сборка товара не активирована" });
    }
    if (context.workflow?.isWaitingAdmin) {
      return res.status(409).json({ error: "Администратор ещё не запустил сборку товара" });
    }
    if (
      normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
      !context.session?.picking_started_at ||
      context.session?.picking_finished_at
    ) {
      return res.status(409).json({ error: "Сначала нажмите «Старт»" });
    }
    let pickItems = getCachedEmployeePickItems(context.session.id) || await listEmployeePickItemsForSession(context.session.id);
    if (!pickItems.length) {
      pickItems = await ensureEmployeeProductPickItems(context.session);
    }
    const body = req.body || {};
    const inputBarcode = normalizeBarcode(typeof body?.barcode === "string" ? body.barcode : "");
    if (!inputBarcode) {
      return res.status(400).json({ error: "Отсканируйте штрихкод товара" });
    }
    const pickScanSession = {
      ...context.session,
      store_id: context.assembly?.store_id || context.session?.store_id || null,
      pick_items: pickItems,
    };
    let matchInfo = await findEmployeePickItemMatchesByBarcode(
      pickScanSession,
      inputBarcode,
      getSkladWarehouseOptionsFromAssembly(context.assembly)
    );
    if (!matchInfo.lookupFailed && (matchInfo.matches.length === 0 || matchInfo.ambiguous || matchInfo.multipleScanProducts)) {
      matchInfo = await findEmployeePickItemMatchesByBarcode(
        pickScanSession,
        inputBarcode,
        { ...getSkladWarehouseOptionsFromAssembly(context.assembly), forceFresh: true }
      );
    }
    if (!matchInfo.matches.length) {
      if (matchInfo.lookupFailed) {
        return res.status(503).json({
          error: "Не удалось проверить товар. Повторите сканирование. Если это ваш товар, нажмите «Ошибка» и отправьте админу.",
        });
      }
      if (matchInfo.multipleScanProducts) {
        return res.status(409).json({
          error: buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts),
        });
      }
      return res.status(404).json({
        error: "Товар не найден в вашей сборке товара. Если это ваш товар, нажмите «Ошибка» и отправьте админу.",
      });
    }
    if (matchInfo.multipleScanProducts) {
      return res.status(409).json({
        error: buildMultipleScanProductsReason(inputBarcode, matchInfo.scanProducts),
      });
    }
    if (matchInfo.ambiguous) {
      return res.status(409).json({
        error: buildAmbiguousAssemblyPickWarning(inputBarcode, matchInfo.matches, {
          multipleScanProducts: matchInfo.multipleScanProducts,
          scanProducts: matchInfo.scanProducts,
        }),
      });
    }
    const item = matchInfo.matches[0];
    const updatedItemResult = await dbQuery(
      `update ozon_employee_assembly_pick_items
       set picked_qty = least(required_qty, picked_qty + 1),
           updated_at = now()
       where id = $1 and picked_qty < required_qty
       returning *`,
      [item.id]
    );
    const updatedItem = updatedItemResult.rows?.[0] || item;
    if (isStockMethodEnabled(context.assembly)) {
      const externalEmployeeId = getAuthExternalEmployeeId(req.auth);
      if (!externalEmployeeId) {
        await dbQuery(
          `update ozon_employee_assembly_pick_items
           set picked_qty = greatest(0, picked_qty - 1),
               updated_at = now()
           where id = $1`,
          [item.id]
        );
        return res.status(409).json({ error: "Сотрудник не привязан к складской системе" });
      }
      try {
        const movementHints = getEmployeePickScanMovementHints(context.session.id, item);
        const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0];
        const movementResult = await moveProductFromShelfToEmployee({
          externalEmployeeId,
          warehouseId: getSkladWarehouseOptionsFromAssembly(context.assembly).warehouseId,
          barcodes: [inputBarcode, item?.barcode],
          resolvedProductIds: movementHints?.productIds || [],
          quantity: 1,
          skipUnavailable: isAllowStockShortage(context.assembly),
          preferredShelfId: movementHints?.preferredShelfId || firstCell?.id || null,
          source: "oz_pick_scan",
          notes: `Ozon: автоперемещение товара сотруднику при сборке товара. Сборка ${assemblyId}, товар ${item?.article || "—"} (${item?.product_name || item?.productName || "—"}), отсканирован ${inputBarcode}.`,
          negativeNotes: `Ozon: автоперемещение товара сотруднику при сборке товара с уходом склада в минус. Сборка ${assemblyId}, товар ${item?.article || "—"} (${item?.product_name || item?.productName || "—"}), отсканирован ${inputBarcode}.`,
        });
        await recordStockShortageSkip({
          assemblyId,
          stage: "pick_scan",
          article: item?.article,
          barcode: inputBarcode || item?.barcode,
          requestedQuantity: 1,
          movementResult,
          userId: req.auth.id,
        });
      } catch (error) {
        await dbQuery(
          `update ozon_employee_assembly_pick_items
           set picked_qty = greatest(0, picked_qty - 1),
               updated_at = now()
           where id = $1`,
          [item.id]
        );
        const message = isSkladMovementAccessError(error)
          ? "Не удалось записать перемещение в склад. Повторите сканирование."
          : error?.message || "Не удалось записать перемещение в склад";
        return res.status(isSkladMovementAccessError(error) ? 503 : 409).json({ error: message });
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
    notifyAssemblyUpdate(assemblyId, true);
    const liveExternalEmployeeId = getAuthExternalEmployeeId(req.auth);
    if (collectedDelta > 0 && liveExternalEmployeeId) {
      writeSborkaLiveEventSafe({
        externalEmployeeId: liveExternalEmployeeId,
        eventType: "pick",
        orderId: null,
        productName: updatedItemWithCells?.product_name || updatedItem?.product_name || item?.product_name || null,
        barcode: inputBarcode,
        quantity: collectedDelta,
        marketplace: "ozon",
      });
    }
    await attachOzonPickItemImages([updatedItemWithCells]);
    res.json({
      item: toEmployeePickItemPayload(updatedItemWithCells),
      session: toEmployeeSessionPayload(refreshedSession),
      warning: "",
      completed:
        Number(refreshedSession.picking_required_qty || 0) > 0 &&
        Number(refreshedSession.picking_collected_qty || 0) >= Number(refreshedSession.picking_required_qty || 0),
    });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/pick-issue`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncHandler(async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });
    if (!context.workflow?.productPickingRequired) {
      return res.status(409).json({ error: "Для этой сборки сборка товара не активирована" });
    }
    if (context.workflow?.isWaitingAdmin) {
      return res.status(409).json({ error: "Администратор ещё не запустил сборку товара" });
    }
    if (
      normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
      !context.session?.picking_started_at ||
      context.session?.picking_finished_at
    ) {
      return res.status(409).json({ error: "Сначала нажмите «Старт»" });
    }
    let pickItems = await listEmployeePickItemsForSession(context.session.id);
    if (!pickItems.length) {
      pickItems = await ensureEmployeeProductPickItems(context.session);
    }
    const body = req.body || {};
    const scannedBarcode = normalizeBarcode(typeof body?.barcode === "string" ? body.barcode : "");
    const originalScannedBarcode = normalizeBarcode(
      typeof body?.originalBarcode === "string" ? body.originalBarcode : ""
    );
    const pickItemId = Number(body?.pickItemId);
    if (!scannedBarcode) return res.status(400).json({ error: "Отсканируйте штрихкод товара" });
    if (!originalScannedBarcode) {
      return res.status(400).json({ error: "Сначала отсканируйте товар в основном поле сборки" });
    }
    if (originalScannedBarcode !== scannedBarcode) {
      return res.status(409).json({ error: "Повторный штрихкод не совпадает с первым сканированием" });
    }
    const pendingItems = pickItems.filter((item) => Number(item.picked_qty || 0) < Number(item.required_qty || 0));
    const selectedItem =
      Number.isFinite(pickItemId) && pickItemId > 0
        ? pendingItems.find((item) => Number(item.id) === pickItemId) || null
        : null;
    if (!selectedItem) {
      return res.status(400).json({ error: "Выберите товар из списка" });
    }
    const issueProductBarcodes = uniqueTextList([
      selectedItem?.barcode ? normalizeBarcode(selectedItem.barcode) : null,
      ...(await getAcceptedPickScanBarcodesForItem(selectedItem, context.assembly.store_id || null)),
    ]);
    await attachOzonPickItemImages([selectedItem]);
    const warehouseSnapshot = await resolveIssueWarehouseProductSnapshot({
      article: selectedItem?.article || null,
      barcodes: issueProductBarcodes,
    });
    const remainingQuantity = Math.max(
      0,
      Number(selectedItem?.required_qty || 0) - Number(selectedItem?.picked_qty || 0)
    );
    const issue = await createOrUpdateAssemblyScanIssue({
      assemblyId,
      postingNumber: null,
      employeeId: req.auth.id,
      lineNos: [],
      itemArticles: selectedItem?.article ? [selectedItem.article] : [],
      itemNames: [selectedItem?.product_name || selectedItem?.productName || ""].filter(Boolean),
      productBarcodes: issueProductBarcodes,
      scannedBarcode,
      originalScannedBarcode,
      confirmationScannedBarcode: scannedBarcode,
      marketplaceArticle: selectedItem?.article || null,
      ...warehouseSnapshot,
      imageUrl: selectedItem?.imageUrl || null,
      remainingQuantity,
      issueStage: SCAN_ISSUE_STAGE_PICK,
      pickItemId: Number(selectedItem.id),
      pickItemKey: selectedItem.item_key || selectedItem.itemKey || null,
      contextNote: buildPickIssueContextNote([selectedItem]),
    });
    broadcastSse("scan_issue_update", { at: new Date().toISOString() }, (client) => client.role === ROLE_ADMIN);
    res.json({ ok: true, issue: toOzonScanIssuePayload(issue) });
  })
);

app.post(
  `${API_ROOT}/settings/scan-issues/:issueId/auto-fix`,
  requireAuth([ROLE_ADMIN]),
  asyncHandler(async (req, res) => handleAdminScanIssueAction(req, res, "auto-fix"))
);

app.get(
  `${API_ROOT}/employee/assemblies/:assemblyId/pick-issues`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncHandler(async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const access = await requireEmployeeAssemblyAccess(req.auth, assemblyId);
    if (!access.ok) return res.status(access.status).json({ error: access.error });
    const result = await dbQuery(
      `select i.*
       from ozon_employee_scan_issues i
       where i.assembly_id = $1
         and i.employee_id = $2
         and i.issue_stage = $3
       order by i.created_at desc, i.id desc
       limit 50`,
      [assemblyId, req.auth.id, SCAN_ISSUE_STAGE_PICK]
    );
    res.json({ issues: (result.rows || []).map(toOzonScanIssuePayload) });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/pick-finish`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.pick_finish", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });
    const { session, error } = await finishEmployeeProductPicking(assemblyId, req.auth.id);
    if (error) return res.status(409).json({ error });
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ session: toEmployeeSessionPayload(session) });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/pick-items/:pickItemId/collect-without-scan`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.pick_collect_without_scan", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const pickItemId = Number(req.params.pickItemId);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });
    if (
      normalizePickingStatus(context.session?.picking_status) !== PICKING_STATUS_STARTED ||
      !context.session?.picking_started_at ||
      context.session?.picking_finished_at
    ) {
      return res.status(409).json({ error: "Сборка товара ещё не начата" });
    }
    const item = await getEmployeePickItemById(context.session.id, pickItemId);
    if (!item) return res.status(404).json({ error: "Строка сборки не найдена" });
    await warmWarehouseNames([item]);
    const allowNoScanMode = context.assembly?.allow_no_scan === true;
    const partialBundleMode =
      context.assembly?.partial_safe_scan === true && warehouseIsBundleForRow(item);
    if (item.bypass_without_scan_allowed !== true && !allowNoScanMode && !partialBundleMode) {
      return res.status(409).json({ error: "Администратор ещё не разрешил собрать этот товар без штрихкода" });
    }
    const stockMethodEnabled = isStockMethodEnabled(context.assembly);
    const externalEmployeeId = stockMethodEnabled ? getAuthExternalEmployeeId(req.auth) : null;
    if (stockMethodEnabled && !externalEmployeeId) {
      return res.status(409).json({ error: "Сотрудник не привязан к складской системе" });
    }
    const updatedItem = await collectEmployeePickItemWithoutScan(pickItemId, {
      requireBypass: !allowNoScanMode && !partialBundleMode,
    });
    if (!updatedItem) return res.status(409).json({ error: "Товар уже собран" });
    const collectedDelta = Math.max(0, Number(updatedItem.picked_qty || 0) - Number(item.picked_qty || 0));
    if (stockMethodEnabled && collectedDelta > 0) {
      try {
        const movementHints = getEmployeePickScanMovementHints(context.session.id, item);
        const firstCell = sortEmployeePickCells(item?.ms_cells || [])[0];
        const movementResult = await moveProductFromShelfToEmployee({
          externalEmployeeId,
          warehouseId: getSkladWarehouseOptionsFromAssembly(context.assembly).warehouseId,
          barcodes: [item?.barcode],
          resolvedProductIds: movementHints?.productIds || [],
          quantity: collectedDelta,
          skipUnavailable: isAllowStockShortage(context.assembly),
          preferredShelfId: movementHints?.preferredShelfId || firstCell?.id || null,
          source: "oz_pick_scan",
          notes: `Ozon: bypass pick movement to employee. Assembly ${assemblyId}, item ${item?.article || "-"} (${item?.product_name || item?.productName || "-"}) without scan.`,
          negativeNotes: `Ozon: bypass pick movement to employee with negative shelf stock. Assembly ${assemblyId}, item ${item?.article || "-"} (${item?.product_name || item?.productName || "-"}) without scan.`,
        });
        await recordStockShortageSkip({
          assemblyId,
          stage: "pick_without_scan",
          article: item?.article,
          barcode: item?.barcode,
          requestedQuantity: collectedDelta,
          movementResult,
          userId: req.auth.id,
        });
      } catch (error) {
        await dbQuery(
          `update ozon_employee_assembly_pick_items
           set picked_qty = $2,
               updated_at = now()
           where id = $1`,
          [pickItemId, Number(item.picked_qty || 0)]
        );
        const message = isSkladMovementAccessError(error)
          ? "Не удалось записать перемещение в склад. Повторите действие."
          : error?.message || "Не удалось записать перемещение в склад";
        return res.status(isSkladMovementAccessError(error) ? 503 : 409).json({ error: message });
      }
    }
    syncEmployeePickScanIndexItem(context.session.id, updatedItem);
    const refreshedSession =
      (await applyEmployeePickingSessionDelta(context.session.id, 0, collectedDelta)) ||
      (await refreshEmployeePickingSessionSummary(context.session.id)) ||
      context.session;
    notifyAssemblyUpdate(assemblyId, true);
    const updatedItemWithCells = await enrichSingleEmployeePickItemWithMsCells(updatedItem);
    await attachOzonPickItemImages([updatedItemWithCells]);
    res.json({
      item: toEmployeePickItemPayload(updatedItemWithCells),
      session: toEmployeeSessionPayload(refreshedSession),
      completed:
        Number(refreshedSession.picking_required_qty || 0) > 0 &&
        Number(refreshedSession.picking_collected_qty || 0) >= Number(refreshedSession.picking_required_qty || 0),
    });
  })
);
app.get(
  `${API_ROOT}/employee/assemblies/:assemblyId/orders`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.orders", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const check = await requireEmployeeAssemblyAccess(req.auth, assemblyId);
    if (!check.ok) return res.status(check.status).json({ error: check.error });
    scheduleAssemblyOrdersSyncBackground(assemblyId, check.assembly.store_id || null, { maxAgeMs: 12000 });

    const mode = check.assembly.access_mode;
    const orders = await listEmployeeAssemblyOrders(assemblyId, req.auth.id, mode);
    res.json({ orders });
  })
);

app.get(
  `${API_ROOT}/employee/assemblies/:assemblyId/collected-orders`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.collected-orders", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const check = await requireEmployeeAssemblyAccess(req.auth, assemblyId);
    if (!check.ok) return res.status(check.status).json({ error: check.error });
    const mode = check.assembly.access_mode;
    const orders = await listEmployeeCollectedAssemblyOrders(assemblyId, req.auth.id, mode);
    res.json({ orders });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/orders/:postingNumber/scan`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.scan", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const postingNumber = normalizeText(req.params.postingNumber);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth, { requireStartedSession: true });
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });

    const mode = context.assembly.access_mode;
    const orderResult = await dbQuery(
      `select posting_number
       from ozon_assembly_orders
       where assembly_id = $1 and posting_number = $2 and collected_at is null and sent_to_delivering_at is null
          ${mode === "selected_split" ? "and assigned_user_id = $3" : ""}
       limit 1`,
      mode === "selected_split" ? [assemblyId, postingNumber, req.auth.id] : [assemblyId, postingNumber]
    );
    if (!orderResult.rows.length) return res.status(404).json({ error: "Заказ недоступен" });

    const barcode = normalizeBarcode(req.body?.barcode || "");
    if (!barcode) return res.status(400).json({ error: "Отсканируйте повторно" });
    const scanLookup = await getScanLookupBarcodes(barcode);
    if (scanLookup.multiple) {
      const errorMessage = buildMultipleScanProductsReason(barcode, scanLookup.matches);
      return res.status(409).json({
        error: errorMessage,
      });
    }

    let matched = await matchOpenItemInOrderByBarcode(
      assemblyId,
      postingNumber,
      barcode,
      context.assembly.store_id || null
    );
    if (!matched) {
      matched = await matchOpenItemInOrderByBarcode(
        assemblyId,
        postingNumber,
        barcode,
        context.assembly.store_id || null,
        { forceFresh: true }
      );
    }
    if (!matched) {
      const scanLookup = await getScanLookupBarcodes(barcode);
      if (scanLookup.multiple) {
        const errorMessage = buildMultipleScanProductsReason(barcode, scanLookup.matches);
        return res.status(409).json({
          error: errorMessage,
        });
      }
      return res.status(400).json({ error: "Штрихкод не относится к товарам этого заказа" });
    }
    if (matched.ambiguous) {
      const errorMessage = buildAmbiguousAssemblyScanWarning(barcode, matched.matches);
      return res.status(409).json({
        error: errorMessage,
      });
    }
    const hadScannedBeforeResult = await dbQuery(
      `select 1
       from ozon_assembly_order_items
       where assembly_id = $1 and posting_number = $2 and scanned_qty > 0
       limit 1`,
      [assemblyId, postingNumber]
    );
    const isFirstOrderScan = !hadScannedBeforeResult.rows.length;

    const updateResult = await dbQuery(
      `with target as (
         select line_no
         from ozon_assembly_order_items
         where assembly_id = $1 and posting_number = $2 and offer_id = $3 and scanned_qty < quantity
         order by line_no asc
         limit 1
       )
       update ozon_assembly_order_items i
       set scanned_qty = i.scanned_qty + 1,
           updated_at = now()
       from target t
       where i.assembly_id = $1 and i.posting_number = $2 and i.line_no = t.line_no
       returning i.line_no`,
      [assemblyId, postingNumber, matched.article]
    );
    if (!updateResult.rows.length) {
      return res.status(400).json({ error: "Количество по этому артикулу уже закрыто" });
    }

    await updateAssemblyScanStatusIfReady(assemblyId, postingNumber, req.auth.id, barcode);
    const updatedOrder = await getEmployeeAssemblyOrder(assemblyId, postingNumber);
    notifyAssemblyUpdate(assemblyId, true);
    const liveExternalEmployeeId = getAuthExternalEmployeeId(req.auth);
    if (isFirstOrderScan && liveExternalEmployeeId) {
      writeSborkaLiveEventSafe({
        externalEmployeeId: liveExternalEmployeeId,
        eventType: "order_start",
        orderId: postingNumber,
        productName: matched.name || null,
        barcode,
        quantity: 1,
        marketplace: "ozon",
      });
    }
    res.json({
      ok: true,
      matchedArticle: matched.article,
      matchedLineNo: Number(matched.lineNo),
      order: updatedOrder,
    });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/scan-order`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.scan_order", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const barcode = normalizeBarcode(req.body?.barcode || "");
    if (!barcode) return res.status(400).json({ error: "Отсканируйте повторно" });

    const context = await getEmployeeAssemblyContext(assemblyId, req.auth, { requireStartedSession: true });
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });

    let found = await findFirstAssemblyOrderByBarcode(assemblyId, req.auth, barcode);
    if (!found.lookupFailed && (!found.order || found.ambiguous || found.multipleScanProducts)) {
      found = await findFirstAssemblyOrderByBarcode(assemblyId, req.auth, barcode, { forceFresh: true });
    }
    if (found.multipleScanProducts) {
      const errorMessage = buildMultipleScanProductsReason(barcode, found.scanProducts);
      return res.status(409).json({ error: errorMessage });
    }
    if (found.ambiguous) {
      const errorMessage = found.warning || buildAmbiguousAssemblyScanWarning(barcode, []);
      return res.status(409).json({ error: errorMessage });
    }
    if (!found.order || !found.matchedItem) {
      return res.status(404).json({ error: "Товар не найден в этой сборке" });
    }
    const hadScannedBeforeResult = await dbQuery(
      `select 1
       from ozon_assembly_order_items
       where assembly_id = $1 and posting_number = $2 and scanned_qty > 0
       limit 1`,
      [assemblyId, found.order.postingNumber]
    );
    const isFirstOrderScan = !hadScannedBeforeResult.rows.length;

    const updateResult = await dbQuery(
      `with target as (
         select line_no
         from ozon_assembly_order_items
         where assembly_id = $1 and posting_number = $2 and line_no = $3 and scanned_qty < quantity
         limit 1
       )
       update ozon_assembly_order_items i
       set scanned_qty = i.scanned_qty + 1,
           updated_at = now()
       from target t
       where i.assembly_id = $1 and i.posting_number = $2 and i.line_no = t.line_no
       returning i.line_no`,
      [assemblyId, found.order.postingNumber, found.matchedItem.lineNo]
    );
    if (!updateResult.rows.length) {
      return res.status(400).json({ error: "Количество по этому товару уже закрыто" });
    }

    await updateAssemblyScanStatusIfReady(assemblyId, found.order.postingNumber, req.auth.id, barcode);
    const orders = await listEmployeeAssemblyOrders(assemblyId, req.auth.id, found.context.assembly.access_mode);
    const order = orders.find((item) => item.postingNumber === found.order.postingNumber) || null;
    notifyAssemblyUpdate(assemblyId, true);
    const liveExternalEmployeeId = getAuthExternalEmployeeId(req.auth);
    if (isFirstOrderScan && liveExternalEmployeeId) {
      writeSborkaLiveEventSafe({
        externalEmployeeId: liveExternalEmployeeId,
        eventType: "order_start",
        orderId: found.order.postingNumber,
        productName: found.matchedItem?.name || null,
        barcode,
        quantity: 1,
        marketplace: "ozon",
      });
    }
    res.json({
      ok: true,
      order,
      matchedItem: {
        lineNo: found.matchedItem.lineNo,
        article: found.matchedItem.article,
        name: found.matchedItem.name,
      },
    });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/orders/:postingNumber/label-scan`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.label_scan", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const postingNumber = normalizeText(req.params.postingNumber);
    const context = await getEmployeeAssemblyLabelScanContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });

    const mode = context.assembly.access_mode;
    const orderResult = await dbQuery(
      `select posting_number, label_scan_passed_at, store_id
       from ozon_assembly_orders
       where assembly_id = $1 and posting_number = $2 and collected_at is null and sent_to_delivering_at is null
          ${mode === "selected_split" ? "and assigned_user_id = $3" : ""}
       limit 1`,
      mode === "selected_split" ? [assemblyId, postingNumber, req.auth.id] : [assemblyId, postingNumber]
    );
    const order = orderResult.rows?.[0];
    if (!order) return res.status(404).json({ error: "Заказ недоступен" });
    if (order.label_scan_passed_at) return res.json({ ok: true, passed: true });

    const remaining = await dbQuery(
      `select 1
       from ozon_assembly_order_items
       where assembly_id = $1 and posting_number = $2 and scanned_qty < quantity
       limit 1`,
      [assemblyId, postingNumber]
    );
    if (remaining.rows.length > 0) {
      return res.status(400).json({ error: "Сначала завершите сканирование всех товаров" });
    }

    const scannedBarcode = normalizeBarcode(req.body?.barcode || "");
    if (!scannedBarcode) return res.status(400).json({ error: "Отсканируйте повторно" });

    const expectedBarcode = normalizeBarcode((await ensureOrderLabelBarcode(assemblyId, postingNumber)) || "");
    if (!expectedBarcode) {
      return res.status(400).json({ error: "Не удалось получить штрихкод этикетки из Ozon" });
    }
    if (expectedBarcode !== scannedBarcode) {
      return res.status(400).json({ error: "Неверный штрихкод этикетки" });
    }
    const externalEmployeeId = getAuthExternalEmployeeId(req.auth);
    if (isStockMethodEnabled(context.assembly) && !externalEmployeeId) {
      return res.status(409).json({ error: "Сотрудник не привязан к складской системе" });
    }
    const orderItems = await ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber, order.store_id || null);

    await dbQuery(
      `update ozon_assembly_orders
       set label_scan_passed_at = now(),
           label_scan_passed_by = $3,
           label_scan_barcode = $4,
           collected_at = now(),
           collected_by = $3,
           collected_session_id = $5,
           updated_at = now()
       where assembly_id = $1 and posting_number = $2`,
      [assemblyId, postingNumber, req.auth.id, scannedBarcode, context.session ? Number(context.session.id) : null]
    );
    if (isStockMethodEnabled(context.assembly)) {
      try {
        await writeOffOzonOrderInventory({
          assembly: context.assembly,
          assemblyId,
          postingNumber,
          storeId: order.store_id,
          orderItems,
          externalEmployeeId,
          userId: req.auth.id,
        });
      } catch (error) {
        await dbQuery(
          `update ozon_assembly_orders
           set label_scan_passed_at = null,
               label_scan_passed_by = null,
               label_scan_barcode = null,
               collected_at = null,
               collected_by = null,
               collected_session_id = null,
               updated_at = now()
           where assembly_id = $1 and posting_number = $2`,
          [assemblyId, postingNumber]
        );
        const message = isSkladMovementAccessError(error)
          ? "Не удалось списать товар со склада сотрудника. Повторите сканирование."
          : error?.message || "Не удалось списать товар у сотрудника";
        return res.status(isSkladMovementAccessError(error) ? 503 : 409).json({ error: message });
      }
    }
    let sessionFinished = false;
    try {
      if (context.session?.id) {
        await recordCollectedOrderForAssemblySession(context.session.id, new Date());
      }
      if (externalEmployeeId) {
        writeSborkaLiveEventSafe({
          externalEmployeeId,
          eventType: "order_complete",
          orderId: postingNumber,
          marketplace: "ozon",
        });
      }
      sessionFinished = Boolean(await finalizeEmployeeAssemblySessionIfCompleted(assemblyId, req.auth.id));
      if (sessionFinished) {
        await reconcileAssemblyAccessAfterEmployeeCompletion(assemblyId, req.auth.id);
      }
      notifyAssemblyUpdate(assemblyId, true);
    } catch (error) {
      console.error("OZ label-scan post-process failed", {
        assemblyId,
        postingNumber,
        userId: req.auth.id,
        error: error?.message || String(error || ""),
      });
      notifyAssemblyUpdate(assemblyId, true);
    }
    res.json({ ok: true, passed: true, sessionFinished });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/orders/:postingNumber/complete-no-scan`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.complete_no_scan", async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const postingNumber = normalizeText(req.params.postingNumber);
    const context = await getEmployeeAssemblyLabelScanContext(assemblyId, req.auth);
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });
    const allowNoScanMode = context.assembly?.allow_no_scan === true;
    const partialSafeScanMode = context.assembly?.partial_safe_scan === true;
    if (!allowNoScanMode && !partialSafeScanMode) {
      return res.status(403).json({ error: "Для этой сборки не включён режим без сканирования" });
    }

    const mode = context.assembly.access_mode;
    const orderResult = await dbQuery(
      `select posting_number, label_scan_passed_at, store_id
       from ozon_assembly_orders
       where assembly_id = $1 and posting_number = $2 and collected_at is null and sent_to_delivering_at is null
          ${mode === "selected_split" ? "and assigned_user_id = $3" : ""}
       limit 1`,
      mode === "selected_split" ? [assemblyId, postingNumber, req.auth.id] : [assemblyId, postingNumber]
    );
    const order = orderResult.rows?.[0];
    if (!order) return res.status(404).json({ error: "Заказ недоступен" });
    if (order.label_scan_passed_at) return res.json({ ok: true, passed: true });

    const externalEmployeeId = getAuthExternalEmployeeId(req.auth);
    if (isStockMethodEnabled(context.assembly) && !externalEmployeeId) {
      return res.status(409).json({ error: "Сотрудник не привязан к складской системе" });
    }
    const orderItems = await ensureAssemblyOrderItemsLoaded(assemblyId, postingNumber, order.store_id || null);
    await warmWarehouseNames(orderItems);
    const containsBundle = orderItems.some((item) => warehouseIsBundleForRow(item));
    if (!allowNoScanMode && (!partialSafeScanMode || !containsBundle)) {
      return res.status(403).json({ error: "Частичный режим разрешает ручное завершение только заказов с комплектом" });
    }
    const completionMode = allowNoScanMode ? "NO_SCAN_MODE" : "PARTIAL_SAFE_SCAN";

    await dbQuery(
      `update ozon_assembly_order_items
       set scanned_qty = quantity, updated_at = now()
       where assembly_id = $1 and posting_number = $2 and scanned_qty < quantity`,
      [assemblyId, postingNumber]
    );
    await dbQuery(
      `update ozon_assembly_orders
       set label_scan_passed_at = now(),
           label_scan_passed_by = $3,
           label_scan_barcode = $5,
           collected_at = now(),
           collected_by = $3,
           collected_session_id = $4,
           updated_at = now()
       where assembly_id = $1 and posting_number = $2`,
       [assemblyId, postingNumber, req.auth.id, context.session ? Number(context.session.id) : null, completionMode]
    );
    if (isStockMethodEnabled(context.assembly)) {
      try {
        await writeOffOzonOrderInventory({
          assembly: context.assembly,
          assemblyId,
          postingNumber,
          storeId: order.store_id,
          orderItems,
          externalEmployeeId,
          userId: req.auth.id,
          noScan: true,
        });
      } catch (error) {
        await dbQuery(
          `update ozon_assembly_orders
           set label_scan_passed_at = null,
               label_scan_passed_by = null,
               label_scan_barcode = null,
               collected_at = null,
               collected_by = null,
               collected_session_id = null,
               updated_at = now()
           where assembly_id = $1 and posting_number = $2`,
          [assemblyId, postingNumber]
        );
        const message = isSkladMovementAccessError(error)
          ? "Не удалось списать товар со склада сотрудника. Повторите действие."
          : error?.message || "Не удалось списать товар у сотрудника";
        return res.status(isSkladMovementAccessError(error) ? 503 : 409).json({ error: message });
      }
    }
    let sessionFinished = false;
    try {
      if (context.session?.id) {
        await recordCollectedOrderForAssemblySession(context.session.id, new Date());
      }
      if (externalEmployeeId) {
        writeSborkaLiveEventSafe({
          externalEmployeeId,
          eventType: "order_complete",
          orderId: postingNumber,
          marketplace: "ozon",
        });
      }
      sessionFinished = Boolean(await finalizeEmployeeAssemblySessionIfCompleted(assemblyId, req.auth.id));
      if (sessionFinished) {
        await reconcileAssemblyAccessAfterEmployeeCompletion(assemblyId, req.auth.id);
      }
      notifyAssemblyUpdate(assemblyId, true);
    } catch (error) {
      console.error("OZ complete-no-scan post-process failed", {
        assemblyId,
        postingNumber,
        userId: req.auth.id,
        error: error?.message || String(error || ""),
      });
      notifyAssemblyUpdate(assemblyId, true);
    }
    res.json({ ok: true, passed: true, sessionFinished });
  })
);

app.post(
  `${API_ROOT}/employee/assemblies/:assemblyId/orders/:postingNumber/scan-issue`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncHandler(async (req, res) => {
    if (!ensurePackerAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const postingNumber = normalizeText(req.params.postingNumber);
    const context = await getEmployeeAssemblyContext(assemblyId, req.auth, { requireStartedSession: true });
    if (context.error) return res.status(context.errorStatus || 400).json({ error: context.error });

    const mode = context.assembly.access_mode;
    const orderResult = await dbQuery(
      `select posting_number
       from ozon_assembly_orders
       where assembly_id = $1 and posting_number = $2 and collected_at is null and sent_to_delivering_at is null
          ${mode === "selected_split" ? "and assigned_user_id = $3" : ""}
       limit 1`,
      mode === "selected_split" ? [assemblyId, postingNumber, req.auth.id] : [assemblyId, postingNumber]
    );
    if (!orderResult.rows.length) return res.status(404).json({ error: "Заказ недоступен" });

    const scannedBarcode = normalizeBarcode(req.body?.barcode || "");
    if (!scannedBarcode) return res.status(400).json({ error: "Отсканируйте товар повторно" });

    const isLabelStage = normalizeText(req.body?.stage) === "label";
    const selectedLineNos = uniqueNumberList(req.body?.lineNos);
    if (!selectedLineNos.length && !isLabelStage) {
      return res.status(400).json({ error: "Выберите проблемный товар" });
    }
    if (isLabelStage) {
      const expectedLabelBarcode = normalizeBarcode(
        (await ensureOrderLabelBarcode(assemblyId, postingNumber).catch(() => "")) || ""
      );
      await createOrUpdateAssemblyScanIssue({
        assemblyId,
        postingNumber,
        employeeId: req.auth.id,
        lineNos: [],
        itemArticles: [],
        itemNames: [],
        productBarcodes: expectedLabelBarcode ? [expectedLabelBarcode] : [],
        scannedBarcode,
        contextNote: "Скан этикетки: штрихкод не совпал с этикеткой заказа",
      });
      broadcastSse("scan_issue_update", { at: new Date().toISOString() }, (client) => client.role === ROLE_ADMIN);
      return res.json({ ok: true });
    }

    const itemsResult = await dbQuery(
      `select line_no, offer_id, sku, name
       from ozon_assembly_order_items
       where assembly_id = $1 and posting_number = $2 and line_no = any($3::int[])
       order by line_no asc`,
      [assemblyId, postingNumber, selectedLineNos]
    );
    if (!itemsResult.rows.length) {
      return res.status(400).json({ error: "Не удалось найти выбранные товары" });
    }

    const itemArticles = [];
    const itemNames = [];
    const productBarcodes = [];
    for (const row of itemsResult.rows) {
      const article = normalizeArticleKey(row.offer_id);
      if (article && !itemArticles.includes(article)) itemArticles.push(article);
      const name = String(row.name || "").trim();
      if (name && !itemNames.includes(name)) itemNames.push(name);
      const accepted = await getAcceptedScanBarcodesForOffer(article, context.assembly.store_id || null, {
        sku: row.sku,
      });
      productBarcodes.push(...accepted.acceptedBarcodes);
    }

    await createOrUpdateAssemblyScanIssue({
      assemblyId,
      postingNumber,
      employeeId: req.auth.id,
      lineNos: itemsResult.rows.map((row) => Number(row.line_no)),
      itemArticles,
      itemNames,
      productBarcodes,
      scannedBarcode,
    });

    broadcastSse("scan_issue_update", { at: new Date().toISOString() }, (client) => client.role === ROLE_ADMIN);
    res.json({ ok: true });
  })
);

app.get(
  `${API_ROOT}/employee/collector/assemblies`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.collector.assemblies", async (req, res) => {
    if (!ensureCollectorAccess(req, res)) return;
    const employeeLocation = await getConfiguredEmployeeLocation(req.auth.id);
    if (employeeLocation.error) {
      return res.status(employeeLocation.errorStatus || 409).json({ error: employeeLocation.error });
    }
    const assemblyResult = await dbQuery(
      `select id, name, store_id, store_name, access_mode,
              location_code, sklad_warehouse_id, sklad_warehouse_name
       from ozon_assemblies
       where access_mode = 'selected_split'
         and location_code = $1
         and timezone('Europe/Moscow', created_at)::date >= timezone('Europe/Moscow', now())::date - 1
       order by created_at desc`,
      [employeeLocation.locationCode]
    );
    const rows = assemblyResult.rows || [];
    if (!rows.length) {
      return res.json({ assemblies: [] });
    }
    const assemblies = [];
    for (const row of rows) {
      if (!isCollectorAllowedForAssembly(row)) {
        continue;
      }
      scheduleAssemblyOrdersSyncBackground(row.id, row.store_id || null, { maxAgeMs: 12000 });
      const latestSession = await getLatestCollectorAssemblySession(row.id, req.auth.id);
      if (latestSession?.status === "finished") {
        continue;
      }
      const totals = await getCollectorAssemblyTotals(row.id);
      const openSession =
        latestSession && latestSession.status === "started" && !latestSession.finished_at ? latestSession : null;
      if (totals.orders < 1 && !openSession) {
        continue;
      }
      assemblies.push({
        id: row.id,
        name: row.name || null,
        storeId: row.store_id || null,
        storeName: resolveStoreStrict(row.store_id)?.name || row.store_name || null,
        locationCode: normalizeLocationCode(row.location_code),
        skladWarehouseId: Number(row.sklad_warehouse_id || 0) || null,
        skladWarehouseName: row.sklad_warehouse_name || null,
        remainingOrders: totals.orders,
        remainingItems: totals.items,
        employees: totals.employees,
        session: toCollectorAssemblySessionPayload(openSession),
        canExport: totals.orders > 0,
        canFinish: Boolean(openSession),
      });
    }
    res.json({ assemblies });
  })
);

app.get(
  `${API_ROOT}/employee/collector/assemblies/:assemblyId/export`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.collector.export", async (req, res) => {
    if (!ensureCollectorAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getCollectorAssemblyContext(assemblyId, req.auth);
    if (context.error) {
      return res.status(context.errorStatus || 400).json({ error: context.error });
    }
    if (!isCollectorAllowedForAssembly(context.assembly)) {
      return res.status(409).json({ error: "Для этой сборки сборщик отключён: товар собирают упаковщики" });
    }
    const { session, error } = await ensureCollectorAssemblySession(assemblyId, req.auth.id);
    if (error) {
      return res.status(409).json({ error });
    }
    const itemsResult = await dbQuery(
      `select i.name as product_name,
              i.offer_id as article,
              i.sku,
              coalesce(sum(greatest(coalesce(i.quantity, 1), 1)), 0)::int as count
       from ozon_assembly_orders o
       join ozon_assembly_order_items i
         on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
       where o.assembly_id = $1
         and o.assigned_user_id is not null
         and o.collected_at is null
         and o.sent_to_delivering_at is null
       group by i.name, i.offer_id, i.sku
       order by count desc, i.name asc`,
      [assemblyId]
    );
    const items = (itemsResult.rows || []).map((row) => ({
      productName: row.product_name || null,
      article: row.article || null,
      sku: Number.isFinite(Number(row.sku)) ? Number(row.sku) : null,
      count: Number(row.count || 0),
    }));
    res.json({
      assembly: {
        id: context.assembly.id,
        name: context.assembly.name || null,
        storeId: context.assembly.store_id || null,
        storeName: resolveStoreStrict(context.assembly.store_id)?.name || context.assembly.store_name || null,
      },
      session: toCollectorAssemblySessionPayload(session),
      items,
    });
  })
);

app.get(
  `${API_ROOT}/employee/collector/assemblies/:assemblyId/export/employees`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.collector.export_employees", async (req, res) => {
    if (!ensureCollectorAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getCollectorAssemblyContext(assemblyId, req.auth);
    if (context.error) {
      return res.status(context.errorStatus || 400).json({ error: context.error });
    }
    if (!isCollectorAllowedForAssembly(context.assembly)) {
      return res.status(409).json({ error: "Для этой сборки сборщик отключён: товар собирают упаковщики" });
    }
    const { session, error } = await ensureCollectorAssemblySession(assemblyId, req.auth.id);
    if (error) {
      return res.status(409).json({ error });
    }
    const employeesResult = await dbQuery(
      `select u.id as user_id,
              u.surname,
              u.name,
              count(distinct o.posting_number)::int as order_count,
              coalesce(sum(greatest(coalesce(i.quantity, 1), 1)), 0)::int as item_count
       from ozon_assembly_orders o
       join users u on u.id = o.assigned_user_id
       left join ozon_assembly_order_items i
         on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
       where o.assembly_id = $1
         and o.assigned_user_id is not null
         and o.collected_at is null
         and o.sent_to_delivering_at is null
       group by u.id, u.surname, u.name
       order by u.surname asc, u.name asc, u.id asc`,
      [assemblyId]
    );
    const itemsResult = await dbQuery(
      `select o.assigned_user_id as user_id,
              i.name as product_name,
              i.offer_id as article,
              i.sku,
              coalesce(sum(greatest(coalesce(i.quantity, 1), 1)), 0)::int as count
       from ozon_assembly_orders o
       join ozon_assembly_order_items i
         on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
       where o.assembly_id = $1
         and o.assigned_user_id is not null
         and o.collected_at is null
         and o.sent_to_delivering_at is null
       group by o.assigned_user_id, i.name, i.offer_id, i.sku
       order by o.assigned_user_id asc, count desc, i.name asc`,
      [assemblyId]
    );
    const itemsByUser = new Map();
    for (const row of itemsResult.rows || []) {
      const userId = Number(row.user_id || 0);
      if (!userId) continue;
      if (!itemsByUser.has(userId)) itemsByUser.set(userId, []);
      itemsByUser.get(userId).push({
        productName: row.product_name || null,
        article: row.article || null,
        sku: Number.isFinite(Number(row.sku)) ? Number(row.sku) : null,
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
    res.json({
      assembly: {
        id: context.assembly.id,
        name: context.assembly.name || null,
        storeId: context.assembly.store_id || null,
        storeName: resolveStoreStrict(context.assembly.store_id)?.name || context.assembly.store_name || null,
      },
      session: toCollectorAssemblySessionPayload(session),
      employees,
    });
  })
);

app.post(
  `${API_ROOT}/employee/collector/assemblies/:assemblyId/finish`,
  requireAuth([ROLE_EMPLOYEE]),
  asyncTimedHandler("api.employee.collector.finish", async (req, res) => {
    if (!ensureCollectorAccess(req, res)) return;
    const assemblyId = normalizeText(req.params.assemblyId);
    const context = await getCollectorAssemblyContext(assemblyId, req.auth);
    if (context.error) {
      return res.status(context.errorStatus || 400).json({ error: context.error });
    }
    if (!isCollectorAllowedForAssembly(context.assembly)) {
      return res.status(409).json({ error: "Для этой сборки сборщик отключён: товар собирают упаковщики" });
    }
    const result = await finishCollectorAssemblySession(assemblyId, req.auth.id);
    if (result.error) {
      return res.status(409).json({ error: result.error });
    }
    notifyAssemblyUpdate(assemblyId, true);
    res.json({ ok: true, session: toCollectorAssemblySessionPayload(result.session) });
  })
);

app.use(API_ROOT, (_req, res) => {
  res.status(404).json({ error: "Not found" });
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, site: ROOT_SITE });
});

app.get("/", (_req, res) => {
  res.redirect(302, `${BASE_PATH}/`);
});

if (fs.existsSync(DIST_DIR)) {
  app.use(
    BASE_PATH,
    express.static(DIST_DIR, {
      index: false,
      fallthrough: true,
      maxAge: "1h",
      setHeaders(res, filePath) {
        if (path.basename(filePath).toLowerCase() === "index.html") {
          res.setHeader("Cache-Control", "no-store");
        }
      },
    })
  );
}

const LOCAL_FILES_DIR = getLocalFilesDir();
try {
  fs.mkdirSync(LOCAL_FILES_DIR, { recursive: true });
} catch {}
const PRODUCT_IMAGE_CACHE_DIR = path.join(LOCAL_FILES_DIR, "_product-images");
const remoteImageJobs = new Map();
try {
  fs.mkdirSync(PRODUCT_IMAGE_CACHE_DIR, { recursive: true });
} catch {}

function isAllowedOzonImageTarget(target) {
  const host = target?.hostname?.toLowerCase() || "";
  return target?.protocol === "https:" && (
    host === "ozone.ru" || host.endsWith(".ozone.ru") ||
    host === "ozon.ru" || host.endsWith(".ozon.ru")
  );
}

function remoteImageContentType(target, fallback = "image/jpeg") {
  const pathname = String(target?.pathname || "").toLowerCase();
  if (pathname.endsWith(".png")) return "image/png";
  if (pathname.endsWith(".webp")) return "image/webp";
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
// Прокси картинок карточек товара (Ozon CDN) через свой origin — до статики /files.
app.get(`${BASE_PATH}/files/_img`, async (req, res) => {
  try {
    const raw = String(req.query.u || "");
    let target;
    try { target = new URL(raw); } catch { return res.status(400).end(); }
    if (!isAllowedOzonImageTarget(target)) return res.status(400).end();
    const image = await loadCachedRemoteImage(target, isAllowedOzonImageTarget, "image/jpeg");
    res.setHeader("Content-Type", image.contentType);
    res.setHeader("Cache-Control", "public, max-age=604800, immutable");
    res.end(image.buffer);
  } catch {
    res.status(502).end();
  }
});
app.use(
  `${BASE_PATH}/files`,
  express.static(LOCAL_FILES_DIR, {
    fallthrough: false,
    index: false,
    maxAge: "1h",
    setHeaders(res) {
      res.setHeader("Cache-Control", "public, max-age=3600");
    },
  })
);

const baseRegex = new RegExp(`^${BASE_PATH.replace(/\//g, "\\/")}(?:\\/.*)?$`);

app.get(baseRegex, (_req, res) => {
  if (!fs.existsSync(INDEX_FILE)) {
    return res.status(404).type("text/plain; charset=utf-8").send("Build not found");
  }
  res.setHeader("Cache-Control", "no-store");
  return res.sendFile(INDEX_FILE);
});

app.use((err, _req, res, _next) => {
  const message = err?.message || "Server error";
  const rawStatus = Number(err?.statusCode || err?.status);
  const status = rawStatus >= 400 && rawStatus < 600 ? rawStatus : 500;
  if (status >= 500) {
    console.error(err);
  }
  if (res.headersSent) return;
  res.status(status).json({ error: message });
});

app.listen(PORT, HOST, () => {
  console.log(`Ozon site server running on http://${HOST}:${PORT}${BASE_PATH}`);
});
