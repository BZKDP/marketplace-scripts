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

const requested = [
  { article: "GRA-09400_6", barcode: "7527841042752", words: ["пиколинат", "хром"] },
  { article: "GRA-08660_7", barcode: "1571919054068", words: ["янтарная", "кислота"] },
  { article: "GRA-08570_7", barcode: "9007523362067", words: ["гинкго", "готу"] },
  { article: "GRA-08902_2", barcode: "2512467987841", words: ["куркумин"] },
  { article: "GRA-08730_4", barcode: "7684134161894", words: ["псиллиум"] },
  { article: "GRA-04420_9", barcode: "3692791871036", words: ["тирозин"] },
  { article: "GRA-09000_4", barcode: "9996410997515", words: ["холин"] },
];

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

try {
  const results = [];
  for (const item of requested) {
    const base = item.article.replace(/(?:_\d+)+$/g, "");
    const candidates = await db.query(
      `select id, article, code, name, entity_type, archived,
              production_barcode, barcode_list, marketplace_barcodes_json
         from products_s
        where coalesce(archived, false) = false
          and (
            upper(coalesce(article, '')) like upper($1)
            or upper(coalesce(code, '')) like upper($1)
            or (${item.words.map((_, index) => `lower(coalesce(name, '')) like lower($${index + 2})`).join(" and ")})
          )
        order by
          case when upper(coalesce(article, '')) = upper($${item.words.length + 2}) then 0
               when upper(coalesce(code, '')) = upper($${item.words.length + 2}) then 1
               when upper(coalesce(article, '')) like upper($1) then 2 else 3 end,
          id
        limit 30`,
      [`${base}%`, ...item.words.map((word) => `%${word}%`), base]
    );
    results.push({ ...item, base, candidates: candidates.rows });
  }
  console.log(JSON.stringify(results, null, 2));
} finally {
  await db.end();
}
