import fs from "node:fs";

const ROOT = "/opt/grasklad-sborka";
const APP = `${ROOT}/apps/OZ`;

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

const { skladRawQuery } = await import(`${APP}/server/services/sklad-fbs.mjs`);
const products = (await skladRawQuery(
  `select id, article, code, name, entity_type, production_barcode, barcode_list, marketplace_barcodes_json
     from products_s
    where coalesce(archived, false) = false
      and (name ilike any($1::text[]) or coalesce(article, '') ilike any($1::text[]) or coalesce(code, '') ilike any($1::text[]))
    order by entity_type, name, id`,
  [["%лизин%", "%lysine%", "%аргинин%", "%бенфотиамин%", "%липоевая%", "%берберин%", "%холин%"]],
)).rows || [];
const ids = products.map((row) => Number(row.id));
const components = ids.length
  ? (await skladRawQuery(
      `select bc.bundle_id, bc.quantity, c.id as component_id, c.article, c.code, c.name
         from bundle_components_s bc
         join products_s c on c.id = bc.component_id
        where bc.bundle_id = any($1::bigint[])
        order by bc.bundle_id, c.id`,
      [ids],
    )).rows || []
  : [];
const byBundle = new Map();
for (const row of components) {
  const key = Number(row.bundle_id);
  if (!byBundle.has(key)) byBundle.set(key, []);
  byBundle.get(key).push({ id: Number(row.component_id), article: row.article || row.code || null, name: row.name, quantity: Number(row.quantity) });
}
console.log(JSON.stringify(products.map((row) => ({
  id: Number(row.id),
  article: row.article || row.code || null,
  name: row.name,
  entityType: row.entity_type,
  productionBarcode: row.production_barcode || null,
  marketplaceBarcodes: Array.isArray(row.marketplace_barcodes_json) ? row.marketplace_barcodes_json : [],
  components: byBundle.get(Number(row.id)) || [],
})), null, 2));
