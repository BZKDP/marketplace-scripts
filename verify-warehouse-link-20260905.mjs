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

const backupDir = "/opt/grasklad-sborka/backups/20260905T062250Z-warehouse-link-20260905-0914";
const links = JSON.parse(fs.readFileSync(`${backupDir}/links.json`, "utf8"));
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

const rows = await db.query(`
  select id, article, code, name, production_barcode, barcode_list, marketplace_barcodes_json
    from products_s
   where coalesce(archived, false) = false
   order by id
`);
await db.end();

function norm(value) {
  return String(value || "").replace(/\s+/g, "").trim().toUpperCase();
}

const owners = new Map();
for (const product of rows.rows) {
  const values = [product.production_barcode, ...String(product.barcode_list || "").split(";")];
  for (const entry of Array.isArray(product.marketplace_barcodes_json) ? product.marketplace_barcodes_json : []) values.push(entry?.value);
  for (const barcode of new Set(values.map(norm).filter(Boolean))) {
    if (!owners.has(barcode)) owners.set(barcode, []);
    owners.get(barcode).push(Number(product.id));
  }
}

const verification = links.map((link) => ({
  row: link.row,
  barcode: link.barcode,
  expectedProductId: Number(link.productId),
  ownerIds: owners.get(norm(link.barcode)) || [],
}));
const failures = verification.filter((row) => row.ownerIds.length !== 1 || row.ownerIds[0] !== row.expectedProductId);
const [wbModule, ozModule] = await Promise.all([
  import("/opt/grasklad-sborka/apps/WB/server/services/sklad-movements.mjs"),
  import("/opt/grasklad-sborka/apps/OZ/server/services/sklad-movements.mjs"),
]);
const tokens = links.map((link) => link.barcode);
const [wbResolved, ozResolved] = await Promise.all([
  wbModule.resolveWarehouseProductMeta(tokens),
  ozModule.resolveWarehouseProductMeta(tokens),
]);
const wbMissing = links.filter((link) => !wbResolved.has(norm(link.barcode))).map((link) => link.barcode);
const ozMismatches = links.filter((link) => Number(ozResolved.get(norm(link.barcode))?.id || 0) !== Number(link.productId)).map((link) => link.barcode);
console.log(JSON.stringify({
  backupDir,
  expected: links.length,
  verified: verification.length - failures.length,
  failures,
  wbResolverVerified: links.length - wbMissing.length,
  wbMissing,
  ozonAndYandexResolverVerified: links.length - ozMismatches.length,
  ozMismatches,
}, null, 2));
if (failures.length || wbMissing.length || ozMismatches.length) process.exitCode = 2;
