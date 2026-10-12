import fs from "node:fs";
import pg from "/opt/grasklad-sborka/apps/WB/node_modules/pg/lib/index.js";

for (const rawLine of fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").split(/\r?\n/)) {
  const line = rawLine.replace(/^\uFEFF/, "").trim();
  if (!line || line.startsWith("#")) continue;
  const separator = line.indexOf("=");
  if (separator < 1) continue;
  const key = line.slice(0, separator).trim();
  let value = line.slice(separator + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[key] ??= value;
}

const source = JSON.parse(fs.readFileSync("/tmp/warehouse-link-20260905-0914.json", "utf8"));
const { Pool } = pg;
const db = new Pool({
  host: process.env.SKLAD_DB_HOST,
  port: Number(process.env.SKLAD_DB_PORT || 5432),
  user: process.env.SKLAD_DB_USER,
  password: process.env.SKLAD_DB_PASSWORD,
  database: process.env.SKLAD_DB_NAME,
  ssl: String(process.env.SKLAD_DB_SSL || "disable").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
  max: 1,
});

function norm(value) {
  return String(value || "").trim().toUpperCase();
}

function baseArticle(value) {
  return norm(value).replace(/(?:_\d+)+$/g, "");
}

function allBarcodes(product) {
  const values = [product.production_barcode, ...String(product.barcode_list || "").split(";")];
  for (const entry of Array.isArray(product.marketplace_barcodes_json) ? product.marketplace_barcodes_json : []) values.push(entry?.value);
  return [...new Set(values.map(norm).filter(Boolean))];
}

function marketplaceToken(row) {
  if (norm(row["Платформа"]) === "WB") return norm(row["Баркод МП"]);
  const sku = String(row["ID МП"] ?? "").replace(/\.0$/, "").trim();
  return /^\d+$/.test(sku) && Number(sku) > 0 ? `OZN${sku}` : "";
}

function titleTokens(value) {
  const stop = new Set(["graflab", "grafLab", "капсул", "капсулы", "мг", "шт", "банка", "банки", "набор", "для", "и", "с", "в", "по"]);
  return new Set(String(value || "").toLowerCase().replace(/ё/g, "е").replace(/[^a-zа-я0-9]+/g, " ").trim().split(/\s+/).filter((word) => word.length > 2 && !stop.has(word)));
}

function titleScore(left, right) {
  const a = titleTokens(left);
  const b = titleTokens(right);
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const word of a) if (b.has(word)) intersection += 1;
  return intersection / Math.min(a.size, b.size);
}

const productsResult = await db.query(`
  select id, article, code, name, entity_type, archived, production_barcode,
         barcode_list, marketplace_barcodes_json
    from products_s
   where coalesce(archived, false) = false
   order by id
`);
const componentsResult = await db.query(`
  select bc.bundle_id, bc.quantity, c.id component_id, c.article, c.name
    from bundle_components_s bc
    join products_s c on c.id = bc.component_id
   order by bc.bundle_id, c.id
`);
await db.end();

const products = productsResult.rows;
const components = new Map();
for (const row of componentsResult.rows) {
  const key = Number(row.bundle_id);
  if (!components.has(key)) components.set(key, []);
  components.get(key).push({ id: Number(row.component_id), article: row.article, name: row.name, quantity: Number(row.quantity) });
}
const barcodeOwners = new Map();
for (const product of products) {
  for (const barcode of allBarcodes(product)) {
    if (!barcodeOwners.has(barcode)) barcodeOwners.set(barcode, []);
    barcodeOwners.get(barcode).push(Number(product.id));
  }
}

const audit = source.map((row, index) => {
  const sellerArticle = norm(row["Арт. продавца"]);
  const base = baseArticle(sellerArticle);
  const token = marketplaceToken(row);
  const candidates = products.filter((product) => [norm(product.article), norm(product.code)].includes(sellerArticle));
  const baseCandidates = candidates.length ? candidates : products.filter((product) => [norm(product.article), norm(product.code)].includes(base));
  const owners = token ? (barcodeOwners.get(token) || []) : [];
  let status = "safe";
  let reason = "unique_article";
  if (!token) {
    status = "blocked";
    reason = "no_marketplace_barcode";
  } else if (baseCandidates.length !== 1) {
    status = "blocked";
    reason = baseCandidates.length ? "ambiguous_article" : "warehouse_article_not_found";
  } else if (owners.length && !owners.includes(Number(baseCandidates[0].id))) {
    status = "blocked";
    reason = "barcode_owned_by_other_product";
  } else if (owners.includes(Number(baseCandidates[0].id))) {
    status = "already";
    reason = "already_linked";
  }
  return {
    row: index + 2,
    platform: String(row["Платформа"] || ""),
    cabinet: String(row["Кабинет"] || ""),
    marketplaceId: String(row["ID МП"] ?? ""),
    marketplaceArticle: String(row["Арт. продавца"] || ""),
    marketplaceName: String(row["Название"] || ""),
    token,
    status,
    reason,
    owners,
    candidates: baseCandidates.map((product) => ({
      id: Number(product.id), article: product.article || product.code, name: product.name,
      entityType: product.entity_type, score: titleScore(row["Название"], product.name),
      components: components.get(Number(product.id)) || [],
    })),
    fuzzyCandidates: baseCandidates.length ? [] : products
      .map((product) => ({
        id: Number(product.id), article: product.article || product.code, name: product.name,
        entityType: product.entity_type, score: titleScore(row["Название"], product.name),
        components: components.get(Number(product.id)) || [],
      }))
      .filter((product) => product.score > 0)
      .sort((left, right) => right.score - left.score || left.id - right.id)
      .slice(0, 6),
  };
});

const counts = Object.fromEntries([...new Set(audit.map((row) => row.status))].map((status) => [status, audit.filter((row) => row.status === status).length]));
console.log(JSON.stringify({ counts, audit }, null, 2));
