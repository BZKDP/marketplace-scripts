import fs from "node:fs";

const envPath = process.argv[2] || "/opt/grasklad-sborka/.env";
const cutoff = new Date(process.argv[3] || "2026-09-07T10:00:00.000Z");

function parseEnv(path) {
  const out = {};
  const raw = fs.readFileSync(path, "utf8").replace(/^\uFEFF/, "");
  for (const sourceLine of raw.split(/\r?\n/)) {
    const line = sourceLine.trim();
    if (!line || line.startsWith("#")) continue;
    const match = sourceLine.match(/^\s*([^=\s]+)\s*=\s*(.*)\s*$/);
    if (!match) continue;
    let value = match[2];
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    out[match[1]] = value;
  }
  return out;
}

const env = parseEnv(envPath);
const token = env.WB_API_TOKEN_2;
if (!token) throw new Error("WB_API_TOKEN_2 is missing");

async function jsonRequest(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: token,
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${url}: ${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : null;
}

async function fetchAllCards() {
  const cards = [];
  let next = {};
  for (let page = 0; page < 100; page += 1) {
    const cursor = { limit: 100, ...next };
    const body = { settings: { cursor, sort: { ascending: true }, filter: { withPhoto: -1 } } };
    const data = await jsonRequest("https://content-api.wildberries.ru/content/v2/get/cards/list", {
      method: "POST",
      body: JSON.stringify(body),
    });
    const batch = Array.isArray(data?.cards) ? data.cards : [];
    cards.push(...batch);
    if (batch.length < 100) break;
    const updatedAt = data?.cursor?.updatedAt;
    const nmID = data?.cursor?.nmID;
    if (!updatedAt || !nmID || (next.updatedAt === updatedAt && next.nmID === nmID)) break;
    next = { updatedAt, nmID };
    await new Promise((resolve) => setTimeout(resolve, 150));
  }
  return cards;
}

async function fetchBlocked() {
  const data = await jsonRequest(
    "https://seller-analytics-api.wildberries.ru/api/v1/analytics/banned-products/blocked?sort=nmId&order=asc"
  );
  return Array.isArray(data?.report) ? data.report : [];
}

const [cards, blocked] = await Promise.all([fetchAllCards(), fetchBlocked()]);
const blockedByNm = new Map(blocked.map((row) => [Number(row.nmId ?? row.nmID), row]));
const recentlyChanged = cards.filter((card) => {
  const updatedAt = new Date(card.updatedAt);
  return Number.isFinite(updatedAt.getTime()) && updatedAt >= cutoff;
});
const candidates = recentlyChanged.filter((card) => {
  const isBad = Number(card.subjectID) === 1524 || String(card.subjectName || "").trim().toLowerCase() === "бады";
  return isBad && blockedByNm.has(Number(card.nmID));
});

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const outputPath = `/opt/grasklad-sborka/backups/wb-evgeny-category-candidates-${stamp}.json`;
fs.writeFileSync(outputPath, JSON.stringify({ createdAt: new Date().toISOString(), cutoff: cutoff.toISOString(), cardsCount: cards.length, blockedCount: blocked.length, recentlyChangedCount: recentlyChanged.length, candidates }, null, 2));

console.log(JSON.stringify({
  cutoff: cutoff.toISOString(),
  cardsCount: cards.length,
  blockedCount: blocked.length,
  recentlyChangedCount: recentlyChanged.length,
  candidateCount: candidates.length,
  outputPath,
  candidates: candidates.map((card) => ({
    nmID: card.nmID,
    vendorCode: card.vendorCode,
    title: card.title,
    subjectID: card.subjectID,
    subjectName: card.subjectName,
    updatedAt: card.updatedAt,
    reason: blockedByNm.get(Number(card.nmID))?.reason || null,
  })),
}, null, 2));
