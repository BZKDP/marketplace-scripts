import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const requireFromSborka = createRequire(path.join(rootDir, "apps", "sborka", "package.json"));
export const { Pool } = requireFromSborka("pg");

export function loadEnv(filePath = path.join(rootDir, ".env")) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Missing .env file at ${filePath}`);
  }
  const content = fs.readFileSync(filePath, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const idx = line.indexOf("=");
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

export function dbConfig(prefix = "") {
  const key = (name) => (prefix ? `${prefix}_${name}` : name);
  return {
    host: process.env[key("DB_HOST")] || process.env[`${prefix}_HOST`] || "",
    port: Number(process.env[key("DB_PORT")] || process.env[`${prefix}_PORT`] || 5432),
    database: process.env[key("DB_NAME")] || process.env[`${prefix}_NAME`] || "",
    user: process.env[key("DB_USER")] || process.env[`${prefix}_USER`] || "",
    password: process.env[key("DB_PASSWORD")] || process.env[`${prefix}_PASSWORD`] || "",
    sslMode: String(process.env[key("DB_SSL")] || process.env[`${prefix}_SSL`] || "disable").toLowerCase(),
  };
}

export function mainDbConfig() {
  return dbConfig("");
}

export function authDbConfig() {
  return dbConfig("AUTH");
}

export function skladDbConfig() {
  return dbConfig("SKLAD");
}

function poolFor(config, ssl) {
  return new Pool({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    max: 1,
    idleTimeoutMillis: 1000,
    connectionTimeoutMillis: 8000,
    ssl: ssl ? { rejectUnauthorized: false } : undefined,
  });
}

async function testPool(config, ssl) {
  const pool = poolFor(config, ssl);
  try {
    await pool.query("select 1");
    return pool;
  } catch (error) {
    await pool.end().catch(() => {});
    throw error;
  }
}

export async function connectAuto(config) {
  if (config.sslMode === "disable") return await testPool(config, false);
  if (config.sslMode === "require") return await testPool(config, true);
  try {
    return await testPool(config, true);
  } catch {
    return await testPool(config, false);
  }
}

export function pgSslMode(sslMode) {
  if (sslMode === "disable") return "disable";
  if (sslMode === "require") return "require";
  return "prefer";
}

export function publicConfig(config) {
  return {
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    sslMode: config.sslMode,
  };
}
