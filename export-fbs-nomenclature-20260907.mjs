import fs from "node:fs";

const input = "./data/active-fbs-nomenclature-20260907.json";
const output = "./FBS_nomenclature_Ozon_WB_2026-09-07.csv";
const report = JSON.parse(fs.readFileSync(input, "utf8"));

const rows = report.nomenclature
  .filter((item) => item.article !== "GRA-04" && String(item.name || "").trim().toLowerCase() !== "удалить")
  .map((item, index) => ({
    number: index + 1,
    article: item.article,
    name: item.name,
    target: 50,
    marketplaces: item.marketplaces.join(", "),
    stores: item.stores.join("; "),
    maxUnits: item.maxUnitsInOneMarketplaceCard,
    activeCards: item.marketplaceCards,
  }));

const quote = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
const lines = [
  ["№", "Артикул склада", "Физическое наименование", "Подготовить, шт", "Маркетплейсы", "Рабочие кабинеты", "Макс. штук в одной карточке", "Активных карточек"],
  ...rows.map((row) => [row.number, row.article, row.name, row.target, row.marketplaces, row.stores, row.maxUnits, row.activeCards]),
].map((row) => row.map(quote).join(";"));

fs.writeFileSync(output, `\uFEFF${lines.join("\r\n")}\r\n`, "utf8");
console.log(JSON.stringify({ output, rows: rows.length, invalid: rows.filter((row) => !row.article || !row.name || row.target !== 50).length }, null, 2));
