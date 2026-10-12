import fs from "node:fs";
import path from "node:path";
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

const audit = JSON.parse(fs.readFileSync("/tmp/audit-warehouse-link-20260905-result.json", "utf8")).audit;
const manualOverrides = new Map([
  ["OZN5699041234", 84],
  ["OZN5699113354", 35],
  ["OZN5699138974", 70],
  ["OZN5699824148", 171],
  ["OZN5700052239", 11],
  ["OZN5700947951", 38],
  ["OZN5700947812", 3],
  ["OZN5701365188", 147],
  ["OZN5700948719", 80],
  ["OZN5700948249", 22],
  ["OZN5701294510", 32],
]);

const links = audit
  .filter((row) => row.status === "safe" || manualOverrides.has(row.token))
  .map((row) => ({
    row: row.row,
    platform: row.platform,
    cabinet: row.cabinet,
    marketplaceArticle: row.marketplaceArticle,
    marketplaceName: row.marketplaceName,
    barcode: row.token,
    productId: manualOverrides.get(row.token) || Number(row.candidates[0]?.id),
  }));

if (links.length !== 90) throw new Error(`Ожидалось 90 подтверждённых связок, получено ${links.length}`);
if (new Set(links.map((link) => link.barcode)).size !== links.length) throw new Error("В списке есть повторяющиеся маркетплейсные штрихкоды");
if (links.some((link) => !link.barcode || !Number.isInteger(link.productId) || link.productId < 1)) throw new Error("В списке есть неполная связка");

const { Pool } = pg;
const sklad = new Pool({
  host: process.env.SKLAD_DB_HOST,
  port: Number(process.env.SKLAD_DB_PORT || 5432),
  user: process.env.SKLAD_DB_USER,
  password: process.env.SKLAD_DB_PASSWORD,
  database: process.env.SKLAD_DB_NAME,
  ssl: String(process.env.SKLAD_DB_SSL || "disable").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
  max: 1,
});

const ids = [...new Set(links.map((link) => link.productId))];
const snapshot = await sklad.query(
  `select id, article, code, name, entity_type, archived, production_barcode,
          barcode_list, marketplace_barcodes_json
     from products_s
    where id = any($1::int[])
    order by id`,
  [ids]
);
if (snapshot.rowCount !== ids.length) throw new Error(`Не найдены складские карточки: ожидалось ${ids.length}, найдено ${snapshot.rowCount}`);
if (snapshot.rows.some((row) => row.archived === true)) throw new Error("Одна из складских карточек находится в архиве");

const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
const backupDir = path.join("/opt/grasklad-sborka/backups", `${stamp}-warehouse-link-20260905-0914`);
fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });
fs.writeFileSync(path.join(backupDir, "products-before.json"), JSON.stringify(snapshot.rows, null, 2), { mode: 0o600 });
fs.writeFileSync(path.join(backupDir, "links.json"), JSON.stringify(links, null, 2), { mode: 0o600 });

const { addWarehouseProductBarcode, resolveWarehouseProductMeta } = await import(
  "/opt/grasklad-sborka/apps/WB/server/services/sklad-movements.mjs"
);
const results = [];
for (const link of links) {
  const result = await addWarehouseProductBarcode({ productId: link.productId, barcode: link.barcode });
  results.push({ ...link, added: result.added, warehouseArticle: result.article, warehouseName: result.name });
}

const resolved = await resolveWarehouseProductMeta(links.map((link) => link.barcode));
const verification = links.map((link) => ({
  barcode: link.barcode,
  expectedProductId: link.productId,
  actualProductId: Number(resolved.get(link.barcode.toUpperCase())?.id || 0),
}));
const failed = verification.filter((row) => row.actualProductId !== row.expectedProductId);
if (failed.length) throw new Error(`Проверка не прошла для ${failed.length} связок: ${JSON.stringify(failed)}`);

const blocked = audit.filter((row) => !links.some((link) => link.row === row.row));
console.log(JSON.stringify({
  backupDir,
  requested: audit.length,
  linked: links.length,
  added: results.filter((row) => row.added).length,
  alreadyPresent: results.filter((row) => !row.added).length,
  verified: verification.length,
  blocked: blocked.map((row) => ({
    row: row.row,
    cabinet: row.cabinet,
    marketplaceArticle: row.marketplaceArticle,
    marketplaceName: row.marketplaceName,
    reason: row.reason,
  })),
}, null, 2));

await sklad.end();
