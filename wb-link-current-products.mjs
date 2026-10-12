import fs from "node:fs";
import path from "node:path";
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

const links = [
  { productId: 2, expectedArticle: "GRA-08710", marketplaceArticle: "GRA-08710_6", barcode: "3279281480741" },
  { productId: 74, expectedArticle: "GRA-09400", marketplaceArticle: "GRA-09400_6", barcode: "7527841042752" },
  { productId: 154, expectedArticle: "GRA-08660", marketplaceArticle: "GRA-08660_7", barcode: "1571919054068" },
  { productId: 125, expectedArticle: "GRA-08570", marketplaceArticle: "GRA-08570_7", barcode: "9007523362067" },
  { productId: 64, expectedArticle: "GRA-08900", marketplaceArticle: "GRA-08902_2", barcode: "2512467987841" },
  { productId: 173, expectedArticle: "GRA-08730", marketplaceArticle: "GRA-08730_4", barcode: "7684134161894" },
  { productId: 51, expectedArticle: "GRA-04420", marketplaceArticle: "GRA-04420_9", barcode: "3692791871036" },
  { productId: 93, expectedArticle: "GRA-09000", marketplaceArticle: "GRA-09000_4", barcode: "9996410997515" },
];

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

try {
  const ids = links.map((link) => link.productId);
  const snapshot = await sklad.query(
    `select id, article, code, name, entity_type, archived, production_barcode,
            barcode_list, marketplace_barcodes_json
       from products_s
      where id = any($1::int[])
      order by id`,
    [ids]
  );
  const byId = new Map(snapshot.rows.map((row) => [Number(row.id), row]));
  for (const link of links) {
    const product = byId.get(link.productId);
    if (!product || product.archived === true) throw new Error(`Складской товар ${link.productId} недоступен`);
    const article = String(product.article || product.code || "").trim().toUpperCase();
    if (article !== link.expectedArticle) throw new Error(`Товар ${link.productId}: ожидался ${link.expectedArticle}, найден ${article}`);
  }

  const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  const backupDir = path.join("/opt/grasklad-sborka/backups", `${stamp}-wb-current-barcode-links`);
  fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(backupDir, "products-before.json"), JSON.stringify({ links, products: snapshot.rows }, null, 2), { mode: 0o600 });

  const { addWarehouseProductBarcode, resolveWarehouseProductMeta } = await import(
    "/opt/grasklad-sborka/apps/WB/server/services/sklad-movements.mjs"
  );
  const results = [];
  for (const link of links) results.push(await addWarehouseProductBarcode(link));
  const verified = await resolveWarehouseProductMeta(links.map((link) => link.barcode));
  const verification = links.map((link) => ({
    ...link,
    matched: verified.has(link.barcode.toUpperCase()),
    warehouseName: verified.get(link.barcode.toUpperCase())?.name || null,
  }));
  if (verification.some((item) => !item.matched)) throw new Error("После записи не все штрихкоды находятся в складской системе");
  console.log(JSON.stringify({ backupDir, results, verification }, null, 2));
} finally {
  await sklad.end();
}
