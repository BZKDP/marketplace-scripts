import fs from "node:fs";

function env(path) {
  const out = {};
  for (const source of fs.readFileSync(path, "utf8").replace(/^\uFEFF/, "").split(/\r?\n/)) {
    const match = source.match(/^\s*([^#=\s]+)\s*=\s*(.*)\s*$/);
    if (!match) continue;
    let value = match[2];
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    out[match[1]] = value;
  }
  return out;
}

const token = env("/opt/grasklad-sborka/.env").WB_API_TOKEN_2;
async function get(url) {
  const response = await fetch(url, { headers: { Authorization: token, Accept: "application/json" } });
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status}: ${text}`);
  return JSON.parse(text);
}

const [bad, complex] = await Promise.all([
  get("https://content-api.wildberries.ru/content/v2/object/charcs/1524?locale=ru"),
  get("https://content-api.wildberries.ru/content/v2/object/charcs/4378?locale=ru"),
]);
const simplify = (data) => (data?.data || []).map(({ charcID, subjectID, subjectName, name, required, charcType, maxCount }) => ({ charcID, subjectID, subjectName, name, required, charcType, maxCount }));
console.log(JSON.stringify({ bad: simplify(bad), complex: simplify(complex) }, null, 2));
