import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";

const ROOT = "/opt/grasklad-sborka";
const APP = `${ROOT}/apps/OZ`;
const SOURCE = "/tmp/warehouse-link-20260907-1010.json";
const EXCLUDED = new Set(["GRA-08410_3", "GRA-07850_4", "GRA-07750_3"]);
const VERIFIED_ALIASES = new Map([
  ["GRA-00271_1", 63],
  ["GRA-08081_1", 19],
  ["GRA-04461_1", 69],
  ["GRA-00351_1", 176],
]);
const APPLY = process.argv.includes("--apply");
const backupArg = process.argv.find((value) => value.startsWith("--backup-dir="));
const backupDir = backupArg ? backupArg.slice("--backup-dir=".length) : "";

function loadEnv(content) {
  for (const pair of content.split(/\0|\r?\n/)) {
    const line = pair.replace(/^\uFEFF/, "").trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index < 1) continue;
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    if (!process.env[key]) process.env[key] = value;
  }
}

loadEnv(fs.readFileSync(`${ROOT}/.env`, "utf8"));
for (const entry of fs.readdirSync("/proc")) {
  if (!/^\d+$/.test(entry)) continue;
  try {
    const cwd = fs.readlinkSync(`/proc/${entry}/cwd`);
    const cmdline = fs.readFileSync(`/proc/${entry}/cmdline`, "utf8");
    if (!cwd.endsWith("/apps/OZ") || !cmdline.includes("server.mjs")) continue;
    loadEnv(fs.readFileSync(`/proc/${entry}/environ`, "utf8"));
    break;
  } catch {}
}

const config = await import(`${APP}/server/config.mjs`);
const { addWarehouseProductBarcode, resolveWarehouseProductMeta } = await import(`${APP}/server/services/sklad-movements.mjs`);
const { skladRawQuery } = await import(`${APP}/server/services/sklad-fbs.mjs`);
const require = createRequire(import.meta.url);
const { Pool } = require(`${APP}/node_modules/pg`);

const rows = JSON.parse(fs.readFileSync(SOURCE, "utf8"));
const requested = rows.filter((row) => !EXCLUDED.has(String(row["Арт. продавца"] || "").trim().toUpperCase()));
const excluded = rows.filter((row) => EXCLUDED.has(String(row["Арт. продавца"] || "").trim().toUpperCase()));
if (requested.length !== 15 || excluded.length !== 3) {
  throw new Error(`Unexpected file contents: requested=${requested.length}, excluded=${excluded.length}`);
}

const stores = [
  { id: config.OZON_STORE_1_ID, name: config.OZON_STORE_1_NAME, clientId: config.OZON_STORE_1_CLIENT_ID, apiKey: config.OZON_STORE_1_API_KEY },
  { id: config.OZON_STORE_2_ID, name: config.OZON_STORE_2_NAME, clientId: config.OZON_STORE_2_CLIENT_ID, apiKey: config.OZON_STORE_2_API_KEY },
].filter((store) => store.id && store.clientId && store.apiKey);
const store = stores.find((entry) => /евген/i.test(String(entry.name || "")) || String(entry.id).toLowerCase() === "evgeny");
if (!store) throw new Error("Ozon Evgeny store is not configured");

function norm(value) {
  return String(value || "").replace(/\s+/g, "").trim().toUpperCase();
}

function baseArticle(value) {
  return norm(value).replace(/(?:_\d+)+$/g, "");
}

function unique(values) {
  return [...new Set(values.map(norm).filter(Boolean))];
}

function allBarcodes(product) {
  const values = [product.production_barcode, ...String(product.barcode_list || "").split(";")];
  for (const item of Array.isArray(product.marketplace_barcodes_json) ? product.marketplace_barcodes_json : []) values.push(item?.value);
  return unique(values);
}

const response = await fetch(`${String(config.OZON_API_BASE || "https://api-seller.ozon.ru").replace(/\/$/, "")}/v3/product/info/list`, {
  method: "POST",
  headers: { "Client-Id": String(store.clientId), "Api-Key": String(store.apiKey), "Content-Type": "application/json" },
  body: JSON.stringify({ offer_id: requested.map((row) => String(row["Арт. продавца"] || "").trim()) }),
});
if (!response.ok) throw new Error(`Ozon product info HTTP ${response.status}: ${(await response.text()).replace(/\s+/g, " ").slice(0, 500)}`);
const responseData = await response.json();
const apiItems = responseData?.items || responseData?.result?.items || [];
const byOffer = new Map(apiItems.map((item) => [norm(item.offer_id || item.offerId), item]));

const products = (await skladRawQuery(
  `select id, article, code, name, entity_type, archived, production_barcode, barcode_list, marketplace_barcodes_json
     from products_s where coalesce(archived, false) = false order by id`,
)).rows || [];
const owners = new Map();
for (const product of products) {
  for (const barcode of allBarcodes(product)) {
    if (!owners.has(barcode)) owners.set(barcode, []);
    owners.get(barcode).push(Number(product.id));
  }
}

const audit = requested.map((row, index) => {
  const article = norm(row["Арт. продавца"]);
  const marketplaceId = String(row["ID МП"] ?? "").replace(/\.0$/, "").trim();
  const expectedBarcode = /^\d+$/.test(marketplaceId) && Number(marketplaceId) > 0 ? `OZN${marketplaceId}` : "";
  const apiItem = byOffer.get(article) || null;
  const apiOzonBarcodes = unique(Array.isArray(apiItem?.barcodes) ? apiItem.barcodes : []).filter((value) => /^OZN\d+$/i.test(value));
  const barcode = apiOzonBarcodes.includes(expectedBarcode)
    ? expectedBarcode
    : apiOzonBarcodes.length === 1
      ? apiOzonBarcodes[0]
      : "";
  const exactCandidates = products.filter((product) => [norm(product.article), norm(product.code)].includes(article));
  let candidates = exactCandidates.length
    ? exactCandidates
    : products.filter((product) => [norm(product.article), norm(product.code)].includes(baseArticle(article)));
  const verifiedAliasProductId = VERIFIED_ALIASES.get(article) || null;
  if (!candidates.length && verifiedAliasProductId) {
    candidates = products.filter((product) => Number(product.id) === verifiedAliasProductId);
  }
  const barcodeOwners = barcode ? owners.get(barcode) || [] : [];
  let status = "safe";
  let reason = verifiedAliasProductId ? "api_barcode_and_verified_article_alias" : "api_barcode_and_unique_article";
  if (!apiItem) {
    status = "blocked";
    reason = "not_returned_by_ozon_api";
  } else if (!barcode) {
    status = "blocked";
    reason = apiOzonBarcodes.length > 1 ? "multiple_ozon_barcodes" : "no_ozon_barcode_from_api";
  } else if (candidates.length !== 1) {
    status = "blocked";
    reason = candidates.length ? "ambiguous_warehouse_article" : "warehouse_article_not_found";
  } else if (barcodeOwners.length && !barcodeOwners.includes(Number(candidates[0].id))) {
    status = "blocked";
    reason = "barcode_owned_by_other_product";
  } else if (barcodeOwners.includes(Number(candidates[0].id))) {
    status = "already";
    reason = "already_linked";
  }
  return {
    row: index + 2,
    article,
    marketplaceId,
    marketplaceName: row["Название"] || "",
    apiOfferId: apiItem?.offer_id || apiItem?.offerId || null,
    apiProductId: apiItem?.id || null,
    apiOzonBarcodes,
    barcode,
    status,
    reason,
    ownerIds: barcodeOwners,
    candidate: candidates.length === 1 ? { id: Number(candidates[0].id), name: candidates[0].name, entityType: candidates[0].entity_type } : null,
    candidateIds: candidates.map((product) => Number(product.id)),
  };
});

const safe = audit.filter((row) => row.status === "safe");
const already = audit.filter((row) => row.status === "already");
const blocked = audit.filter((row) => row.status === "blocked");
const report = {
  mode: APPLY ? "apply" : "dry-run",
  sourceRows: rows.length,
  requested: requested.length,
  excluded: excluded.map((row) => norm(row["Арт. продавца"])),
  apiReturned: apiItems.length,
  safe: safe.length,
  already: already.length,
  blocked: blocked.length,
  audit,
};

if (!APPLY) {
  console.log(JSON.stringify(report, null, 2));
  process.exit(0);
}
if (!backupDir) throw new Error("--backup-dir is required in apply mode");
if (blocked.length) throw new Error(`Refusing apply: ${blocked.length} rows are blocked`);
const links = [...safe, ...already];
const productIds = [...new Set(links.map((row) => row.candidate.id))];
const before = products.filter((product) => productIds.includes(Number(product.id)));
fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });
fs.writeFileSync(path.join(backupDir, "products-before.json"), JSON.stringify(before, null, 2), { mode: 0o600 });
fs.writeFileSync(path.join(backupDir, "links.json"), JSON.stringify(links, null, 2), { mode: 0o600 });

const added = [];
for (const row of safe) {
  const result = await addWarehouseProductBarcode({ productId: row.candidate.id, barcode: row.barcode });
  added.push({ article: row.article, barcode: row.barcode, productId: row.candidate.id, added: result.added });
}
const resolved = await resolveWarehouseProductMeta(links.map((row) => row.barcode));
const failures = links.filter((row) => Number(resolved.get(norm(row.barcode))?.id || 0) !== row.candidate.id);
if (failures.length) throw new Error(`Post-apply verification failed for ${failures.length} rows`);
console.log(JSON.stringify({
  ...report,
  backupDir,
  added: added.filter((row) => row.added).length,
  alreadyPresent: already.length + added.filter((row) => !row.added).length,
  verified: links.length,
  changes: added,
}, null, 2));
