import fs from "node:fs";

const raw = fs.readFileSync("/opt/grasklad-sborka/.env", "utf8").replace(/^\uFEFF/, "");
for (const sourceLine of raw.split(/\r?\n/)) {
  const line = sourceLine.trim();
  if (!line || line.startsWith("#")) continue;
  const index = line.indexOf("=");
  if (index < 1) continue;
  let value = line.slice(index + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
  process.env[line.slice(0, index).trim()] = value;
}

const headers = {
  "Client-Id": process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID,
  "Api-Key": process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY,
  "Content-Type": "application/json",
};
const response = await fetch("https://api-seller.ozon.ru/v1/description-category/tree", {
  method: "POST",
  headers,
  body: JSON.stringify({ language: "DEFAULT" }),
});
if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
const data = await response.json();
const wantedDescriptionId = "200000896";
const wantedTypeIds = new Set(["115949376", "971300815", "971300823", "971300824", "971300829", "971300834", "971300837", "971300839"]);
const found = [];
function walk(nodes, path = []) {
  for (const node of Array.isArray(nodes) ? nodes : []) {
    const name = String(node.category_name || node.name || "").trim();
    const nextPath = name ? [...path, name] : path;
    const types = Array.isArray(node.type) ? node.type : Array.isArray(node.types) ? node.types : [];
    const selected = types.filter((type) => wantedTypeIds.has(String(type.type_id ?? type.id)));
    if (String(node.description_category_id ?? node.category_id) === wantedDescriptionId || selected.length) {
      found.push({
        descriptionCategoryId: node.description_category_id ?? node.category_id ?? null,
        path: nextPath,
        types: selected.length ? selected : types,
      });
    }
    walk(node.children, nextPath);
  }
}
walk(data?.result || data?.items || []);
console.log(JSON.stringify(found, null, 2));
