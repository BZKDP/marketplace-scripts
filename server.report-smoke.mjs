import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import express from "express";
import compression from "compression";
import { Pool } from "pg";
import bcrypt from "bcryptjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
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
    if (!process.env[key]) process.env[key] = value;
  }
}

loadEnv(path.join(__dirname, ".env"));

const PORT = Number(process.env.SBORKA_PORT || process.env.PORT || 3008);
const HOST = process.env.HOST || "0.0.0.0";
const DB_HOST = process.env.DB_HOST || "<SERVER_HOST>";
const DB_PORT = Number(process.env.DB_PORT || 5432);
const DB_USER = process.env.DB_USER || "gen_user";
const DB_PASSWORD = process.env.DB_PASSWORD || "";
const DB_NAME = process.env.DB_NAME || "default_db";
const DB_SSL = (process.env.DB_SSL || "auto").toLowerCase();
const DB_POOL_MAX = Math.max(1, Number(process.env.DB_POOL_MAX || 6));
const AUTH_DB_HOST = process.env.AUTH_DB_HOST || "";
const AUTH_DB_PORT = Number(process.env.AUTH_DB_PORT || 5432);
const AUTH_DB_USER = process.env.AUTH_DB_USER || "";
const AUTH_DB_PASSWORD = process.env.AUTH_DB_PASSWORD || "";
const AUTH_DB_NAME = process.env.AUTH_DB_NAME || "";
const AUTH_DB_SSL = (process.env.AUTH_DB_SSL || "disable").toLowerCase();
const AUTH_DB_POOL_MAX = Math.max(1, Number(process.env.AUTH_DB_POOL_MAX || 2));
const SKLAD_DB_HOST = process.env.SKLAD_DB_HOST || AUTH_DB_HOST || "";
const SKLAD_DB_PORT = Number(process.env.SKLAD_DB_PORT || AUTH_DB_PORT || 5432);
const SKLAD_DB_USER = process.env.SKLAD_DB_USER || AUTH_DB_USER || "";
const SKLAD_DB_PASSWORD = process.env.SKLAD_DB_PASSWORD || AUTH_DB_PASSWORD || "";
const SKLAD_DB_NAME = process.env.SKLAD_DB_NAME || AUTH_DB_NAME || "";
const SKLAD_DB_SSL = (process.env.SKLAD_DB_SSL || AUTH_DB_SSL || "disable").toLowerCase();
const SKLAD_DB_POOL_MAX = Math.max(1, Number(process.env.SKLAD_DB_POOL_MAX || AUTH_DB_POOL_MAX || 2));
const DB_POOL_CONNECTION_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.DB_POOL_CONNECTION_TIMEOUT_MS || 8000) || 8000
);
const SKLAD_FBS_WAREHOUSE_ID = Math.max(1, Number(process.env.SKLAD_FBS_WAREHOUSE_ID || 1));
const COOKIE_NAME = process.env.COOKIE_NAME || "sid";
const COOKIE_PATH = process.env.COOKIE_PATH || "/sborka";
const COOKIE_SECURE = process.env.COOKIE_SECURE === "1";
const SESSION_TTL_MS = Number(process.env.SESSION_TTL_MS || 604800000);
const INTERNAL_PROXY_TOKEN = (process.env.INTERNAL_PROXY_TOKEN || "");
const OZ_BACKEND = (process.env.OZ_BACKEND || "http://127.0.0.1:3006").replace(/\/$/, "");
const WB_BACKEND = (process.env.WB_BACKEND || "http://127.0.0.1:3007").replace(/\/$/, "");
const PROXY_TIMEOUT_MS = Math.max(3000, Number(process.env.PROXY_TIMEOUT_MS || 30000));
const CREATE_PROXY_TIMEOUT_MS = Math.max(
  PROXY_TIMEOUT_MS,
  Number(process.env.CREATE_PROXY_TIMEOUT_MS || 180000) || 180000
);

const BASE = "/sborka";
const API_ROOT = `${BASE}/api`;
const OZ_BASE = `${BASE}/oz`;
const WB_BASE = `${BASE}/wb`;
const OZ_API_ROOT = `${OZ_BASE}/api`;
const WB_API_ROOT = `${WB_BASE}/api`;
const ROLE_SUPER = "super_admin";
const ROLE_ADMIN = "admin";
const ROLE_EMPLOYEE = "employee";
const ROLE_ARCHIVED = "archived";
const AUTH_SOURCE_LOCAL = "local";
const AUTH_SOURCE_EXTERNAL = "external";
const APPLICATION_LOG_RETENTION_DAYS = Math.max(
  30,
  Number(process.env.APPLICATION_LOG_RETENTION_DAYS || 180) || 180
);
const APPLICATION_LOG_MAX_MESSAGE = 4000;
const APPLICATION_LOG_MAX_STACK = 12000;
const APPLICATION_LOG_MAX_CONTEXT = 32000;
// Статусы во внешней базе (employees_d), при которых сотрудник считается действующим
// и может входить/работать: штатный (active) и стажёр (internship). fired/rejected — нет.
const EMPLOYEE_ACTIVE_STATUSES = ["active", "internship"];
const LOCATION_IZHEVSK = "izhevsk";
const LOCATION_MOSCOW = "moscow";
const LOCATION_CODES = new Set([LOCATION_IZHEVSK, LOCATION_MOSCOW]);
let bootstrapSuperAdminEnsured = false;

const DIST_DIR = path.join(__dirname, "dist");
const SHELL_INDEX = path.join(DIST_DIR, "index.html");
const OZ_DIST_DIR = path.join(DIST_DIR, "oz");
const WB_DIST_DIR = path.join(DIST_DIR, "wb");
const OZ_INDEX = path.join(OZ_DIST_DIR, "index.html");
const WB_INDEX = path.join(WB_DIST_DIR, "index.html");

function isRetryablePgConnectionError(error) {
  const code = String(error?.code || "").trim().toUpperCase();
  const message = String(error?.message || "").toLowerCase();
  return (
    code === "ECONNRESET" ||
    code === "53300" ||
    code === "57P05" ||
    code === "57P01" ||
    code === "57P02" ||
    code === "57P03" ||
    message.includes("remaining connection slots are reserved") ||
    message.includes("idle-session timeout") ||
    message.includes("timeout exceeded when trying to connect") ||
    message.includes("connection terminated unexpectedly") ||
    message.includes("terminating connection") ||
    message.includes("server closed the connection unexpectedly") ||
    message.includes("read econnreset") ||
    message.includes("socket hang up")
  );
}

const POOL_CLIENT_ERROR_HANDLER = Symbol("poolClientErrorHandler");

function attachPoolClientErrorHandler(client, poolName) {
  if (!client || client[POOL_CLIENT_ERROR_HANDLER]) return client;
  const handler = (error) => {
    console.error(`[${poolName}] pooled client error:`, error);
  };
  client.on("error", handler);
  client[POOL_CLIENT_ERROR_HANDLER] = handler;
  return client;
}

function makeAutoPool(host, port, user, password, database, sslMode, maxConnections, poolName) {
  let pool = null;
  let promise = null;

  const mk = (ssl) => {
    const nextPool = new Pool({
      host,
      port,
      user,
      password,
      database,
      application_name: poolName,
      max: maxConnections,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: DB_POOL_CONNECTION_TIMEOUT_MS,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000,
      ssl: ssl ? { rejectUnauthorized: false } : undefined,
    });
    nextPool.on("error", (error) => {
      console.error(`[${poolName}] unexpected idle client error:`, error);
    });
    return nextPool;
  };

  async function closePool(currentPool = null) {
    const existingPool = currentPool || pool;
    if (existingPool && existingPool === pool) {
      pool = null;
    }
    promise = null;
    try {
      if (existingPool) await existingPool.end();
    } catch {}
  }

  async function testPool(ssl) {
    const nextPool = mk(ssl);
    try {
      await nextPool.query("select 1 as ok");
      return nextPool;
    } catch (error) {
      try {
        await nextPool.end();
      } catch {}
      throw error;
    }
  }

  async function getPool() {
    if (pool) return pool;
    if (promise) return promise;
    promise = (async () => {
      try {
        let nextPool;
        if (sslMode === "disable") {
          nextPool = await testPool(false);
        } else if (sslMode === "require") {
          nextPool = await testPool(true);
        } else {
          try {
            nextPool = await testPool(true);
          } catch {
            nextPool = await testPool(false);
          }
        }
        pool = nextPool;
        return nextPool;
      } catch (error) {
        throw error;
      } finally {
        promise = null;
      }
    })();
    return promise;
  }

  async function retryOperation(run, label) {
    let lastError = null;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        return await run();
      } catch (error) {
        lastError = error;
        if (!isRetryablePgConnectionError(error)) {
          throw error;
        }
        console.warn(
          `[${poolName}] retrying ${label} after transient error (attempt ${attempt + 1}/3):`,
          error?.message || error
        );
        await closePool();
        await new Promise((resolve) => setTimeout(resolve, 150 * (attempt + 1)));
      }
    }
    throw lastError;
  }

  return {
    async query(text, params = []) {
      return retryOperation(async () => {
        const activePool = await getPool();
        return activePool.query(text, params);
      }, "query");
    },
    async reset() {
      await closePool();
    },
    async connect() {
      return retryOperation(async () => {
        const activePool = await getPool();
        return attachPoolClientErrorHandler(await activePool.connect(), poolName);
      }, "connect");
    },
  };
}

const mainDb = makeAutoPool(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_SSL, DB_POOL_MAX, "sborka-main");
const authDb = makeAutoPool(
  AUTH_DB_HOST,
  AUTH_DB_PORT,
  AUTH_DB_USER,
  AUTH_DB_PASSWORD,
  AUTH_DB_NAME,
  AUTH_DB_SSL,
  AUTH_DB_POOL_MAX,
  "sborka-auth"
);
const skladDb = makeAutoPool(
  SKLAD_DB_HOST,
  SKLAD_DB_PORT,
  SKLAD_DB_USER,
  SKLAD_DB_PASSWORD,
  SKLAD_DB_NAME,
  SKLAD_DB_SSL,
  SKLAD_DB_POOL_MAX,
  "sborka-sklad"
);

async function dbQuery(text, params = []) {
  return mainDb.query(text, params);
}

async function authQuery(text, params = []) {
  return authDb.query(text, params);
}

async function skladQuery(text, params = []) {
  return skladDb.query(text, params);
}

const EARNINGS_TZ = "Europe/Moscow";
const EARNINGS_SETTING_PICK_RATE_LEGACY = "reward_rate_per_pick";
const EARNINGS_SETTING_PICK_RATE = "reward_rate_per_pick_item";
const EARNINGS_SETTING_ORDER_RATE = "reward_rate_per_order_collect";
// "1"/"0": whether employees SEE the balance widget (earnings always accrue regardless)
const EARNINGS_SETTING_SHOW_BALANCE = "show_employee_balance";
const EARNINGS_DEFAULT_PICK_RATE = 8.5;
const EARNINGS_DEFAULT_ORDER_RATE = 0;
const EARNINGS_EVENT_PICK_SCAN = "pick_scan";
const EARNINGS_EVENT_ORDER_COLLECT = "order_collect";
const EARNINGS_EVENT_MANUAL_ADJUSTMENT = "manual_adjustment";
const ROLE_SET_ADMIN = new Set(["admin", "super_admin"]);
let earningsSchemaPromise = null;
let skladEarningsSchemaPromise = null;
let pendingSkladSyncPromise = null;

const SKLAD_EVENT_EXTERNAL_PICK = "external_order_pick";
const SKLAD_EVENT_EXTERNAL_ORDER_COLLECT = "external_order_collect";
const SKLAD_SOURCE_SBORKA = "sborka-site";
const SKLAD_SOURCE_SBORKA_ADMIN = "sborka-admin";

function roundMoney(value) {
  return Number(Number(value || 0).toFixed(2));
}

function roundSkladMoney(value) {
  return Number(Number(value || 0).toFixed(3));
}

function parseMoneyValue(value, fallback = 0) {
  const parsed = Number.parseFloat(String(value ?? "").replace(",", "."));
  return Number.isFinite(parsed) ? roundMoney(parsed) : roundMoney(fallback);
}

function truncateText(value, maxLength = 1000) {
  const normalized = String(value ?? "");
  return normalized.length > maxLength ? normalized.slice(0, maxLength) : normalized;
}

function toJsonValue(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch {
    return JSON.stringify(null);
  }
}

function toNullableText(value) {
  const normalized = normalizeText(value);
  return normalized || null;
}

function toNullableInteger(value) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function toIsoDate(value) {
  const normalized = normalizeText(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : null;
}

function getMskDateSql(columnName) {
  return `timezone('${EARNINGS_TZ}', ${columnName})::date`;
}

async function ensureEarningsSchema() {
  if (!earningsSchemaPromise) {
    earningsSchemaPromise = (async () => {
      await dbQuery(
        `create table if not exists employee_coin_settings (
           key text primary key,
           value_text text not null,
           updated_at timestamptz not null default now(),
           updated_by bigint references users(id) on delete set null
         )`
      );
      await dbQuery(
        `create table if not exists employee_coin_balances (
           user_id bigint primary key references users(id) on delete cascade,
           balance numeric(14,2) not null default 0,
           updated_at timestamptz not null default now()
         )`
      );
      await dbQuery(
        `create table if not exists employee_coin_events (
           id bigserial primary key,
           user_id bigint not null references users(id) on delete cascade,
           external_employee_id integer,
           event_type text not null,
           source text not null default 'sborka-site',
           marketplace text,
           store_id text,
           store_name text,
           entity_type text,
           entity_id text,
           entity_name text,
           session_id bigint,
           item_id bigint,
           item_key text,
           order_ref text,
           product_name text,
           article text,
           marketplace_code_kind text,
           marketplace_code text,
           scanned_code text,
           reward_units numeric(14,2) not null default 0,
           rate_per_unit numeric(14,2) not null default 0,
           amount_delta numeric(14,2) not null default 0,
           balance_before numeric(14,2) not null default 0,
           balance_after numeric(14,2) not null default 0,
           notes text,
           source_task_id text,
           dedupe_key text unique,
           external_sync_status text not null default 'pending',
           external_sync_attempts integer not null default 0,
           external_sync_last_error text,
           external_synced_at timestamptz,
           created_at timestamptz not null default now(),
           updated_at timestamptz not null default now()
         )`
      );
      await dbQuery(
        "alter table employee_coin_events drop constraint if exists employee_coin_events_event_type_check"
      );
      await dbQuery(
        `alter table employee_coin_events
         add constraint employee_coin_events_event_type_check
         check (event_type in ('pick_scan', 'order_collect', 'manual_adjustment'))`
      );
      await dbQuery(
        "create index if not exists idx_employee_coin_events_user_created on employee_coin_events(user_id, created_at desc)"
      );
      await dbQuery(
        "create index if not exists idx_employee_coin_events_marketplace_created on employee_coin_events(marketplace, created_at desc)"
      );
      await dbQuery(
        "create index if not exists idx_employee_coin_events_store_created on employee_coin_events(store_id, created_at desc)"
      );
      await dbQuery(
        `insert into employee_coin_settings (key, value_text)
         values ($1, $2)
         on conflict (key) do nothing`,
        [EARNINGS_SETTING_PICK_RATE_LEGACY, String(EARNINGS_DEFAULT_PICK_RATE)]
      );
      await dbQuery(
        `insert into employee_coin_settings (key, value_text)
         select $1, coalesce((select value_text from employee_coin_settings where key = $2 limit 1), $3)
         where not exists (select 1 from employee_coin_settings where key = $1)`,
        [EARNINGS_SETTING_PICK_RATE, EARNINGS_SETTING_PICK_RATE_LEGACY, String(EARNINGS_DEFAULT_PICK_RATE)]
      );
      await dbQuery(
        `insert into employee_coin_settings (key, value_text)
         values ($1, $2)
         on conflict (key) do nothing`,
        [EARNINGS_SETTING_ORDER_RATE, String(EARNINGS_DEFAULT_ORDER_RATE)]
      );
    })().catch((error) => {
      earningsSchemaPromise = null;
      throw error;
    });
  }
  return earningsSchemaPromise;
}

async function ensureSkladEarningsSchema() {
  if (!SKLAD_DB_HOST || !SKLAD_DB_USER || !SKLAD_DB_NAME) return false;
  if (!skladEarningsSchemaPromise) {
    skladEarningsSchemaPromise = (async () => {
      await skladQuery(
        `alter table employees_s
         add column if not exists gra_balance numeric(18,3) not null default 0`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source varchar(40) not null default 'warehouse'`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_task_id text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_marketplace varchar(20)`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_store_id text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_store_name text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_entity_type varchar(30)`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_entity_id text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_entity_name text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_session_id bigint`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_item_id bigint`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_item_key text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_order_ref text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_product_name text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_article text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_marketplace_code_kind text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_marketplace_code text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_scanned_code text`
      );
      await skladQuery(
        `alter table employee_earnings_s
         add column if not exists source_payload jsonb`
      );
      await skladQuery(
        "alter table employee_earnings_s drop constraint if exists employee_earnings_s_event_type_check"
      );
      await skladQuery(
        `alter table employee_earnings_s
         add constraint employee_earnings_s_event_type_check
         check (event_type in ('inventory_scan', 'manual_adjustment', 'external_order_pick', 'external_order_collect'))`
      );
      await skladQuery(
        `create index if not exists idx_employee_earnings_source_created
         on employee_earnings_s(source, created_at desc)`
      );
      await skladQuery(
        `create index if not exists idx_employee_earnings_source_marketplace
         on employee_earnings_s(source_marketplace, created_at desc)`
      );
      await skladQuery(
        `create unique index if not exists idx_employee_earnings_source_task_unique
         on employee_earnings_s(source, source_task_id)
         where source_task_id is not null`
      );
      return true;
    })().catch((error) => {
      skladEarningsSchemaPromise = null;
      throw error;
    });
  }
  return skladEarningsSchemaPromise;
}

async function getCoinRates(client = null) {
  await ensureEarningsSchema();
  const executor = client || { query: dbQuery };
  const result = await executor.query(
    `select key, value_text
     from employee_coin_settings
     where key = any($1::text[])`,
    [[EARNINGS_SETTING_PICK_RATE_LEGACY, EARNINGS_SETTING_PICK_RATE, EARNINGS_SETTING_ORDER_RATE, EARNINGS_SETTING_SHOW_BALANCE]]
  );
  const values = Object.fromEntries((result.rows || []).map((row) => [row.key, row.value_text]));
  const pickItemRatePerUnit = parseMoneyValue(
    values[EARNINGS_SETTING_PICK_RATE] ?? values[EARNINGS_SETTING_PICK_RATE_LEGACY],
    EARNINGS_DEFAULT_PICK_RATE
  );
  const orderCollectRatePerUnit = parseMoneyValue(
    values[EARNINGS_SETTING_ORDER_RATE],
    EARNINGS_DEFAULT_ORDER_RATE
  );
  return {
    pickItemRatePerUnit,
    orderCollectRatePerUnit,
    ratePerUnit: pickItemRatePerUnit,
    showEmployeeBalance: values[EARNINGS_SETTING_SHOW_BALANCE] !== "0",
  };
}

async function getCoinRate(client = null) {
  const rates = await getCoinRates(client);
  return rates.pickItemRatePerUnit;
}

async function setCoinRates(
  { pickItemRatePerUnit = undefined, orderCollectRatePerUnit = undefined, showEmployeeBalance = undefined } = {},
  updatedBy = null
) {
  await ensureEarningsSchema();
  const current = await getCoinRates();
  const nextPickRate = Number.isFinite(Number(pickItemRatePerUnit))
    ? parseMoneyValue(pickItemRatePerUnit, EARNINGS_DEFAULT_PICK_RATE)
    : current.pickItemRatePerUnit;
  const nextOrderRate = Number.isFinite(Number(orderCollectRatePerUnit))
    ? parseMoneyValue(orderCollectRatePerUnit, EARNINGS_DEFAULT_ORDER_RATE)
    : current.orderCollectRatePerUnit;
  await dbQuery(
    `insert into employee_coin_settings (key, value_text, updated_at, updated_by)
     values
       ($1, $2, now(), $4),
       ($3, $5, now(), $4)
     on conflict (key) do update
     set value_text = excluded.value_text,
         updated_at = excluded.updated_at,
         updated_by = excluded.updated_by`,
    [
      EARNINGS_SETTING_PICK_RATE,
      String(nextPickRate),
      EARNINGS_SETTING_ORDER_RATE,
      updatedBy || null,
      String(nextOrderRate),
    ]
  );
  if (typeof showEmployeeBalance === "boolean") {
    await dbQuery(
      `insert into employee_coin_settings (key, value_text, updated_at, updated_by)
       values ($1, $2, now(), $3)
       on conflict (key) do update
       set value_text = excluded.value_text, updated_at = excluded.updated_at, updated_by = excluded.updated_by`,
      [EARNINGS_SETTING_SHOW_BALANCE, showEmployeeBalance ? "1" : "0", updatedBy || null]
    );
  }
  return {
    pickItemRatePerUnit: nextPickRate,
    orderCollectRatePerUnit: nextOrderRate,
    ratePerUnit: nextPickRate,
    showEmployeeBalance:
      typeof showEmployeeBalance === "boolean" ? showEmployeeBalance : current.showEmployeeBalance !== false,
  };
}

async function setCoinRate(ratePerUnit, updatedBy = null) {
  const rates = await setCoinRates({ pickItemRatePerUnit: ratePerUnit }, updatedBy);
  return rates.pickItemRatePerUnit;
}

async function getEmployeeCoinSummary(userId) {
  await ensureEarningsSchema();
  const result = await dbQuery(
    `with base as (
       select $1::bigint as user_id
     ),
     balance as (
       select coalesce((select b.balance from employee_coin_balances b where b.user_id = base.user_id), 0)::numeric(14,2) as balance
       from base
     ),
     aggregates as (
       select
         coalesce(sum(case when ${getMskDateSql("created_at")} = ${getMskDateSql("now()")} then amount_delta else 0 end), 0)::numeric(14,2) as today_amount,
         coalesce(sum(case when ${getMskDateSql("created_at")} = ${getMskDateSql("now()")} then reward_units else 0 end), 0)::numeric(14,2) as today_units,
         max(created_at) as last_earned_at
       from employee_coin_events
       where user_id = $1
     )
     select
       balance.balance,
       aggregates.today_amount,
       aggregates.today_units,
       aggregates.last_earned_at
     from balance, aggregates`,
    [userId]
  );
  const row = result.rows?.[0] || {};
  const rates = await getCoinRates();
  return {
    balance: roundMoney(row.balance || 0),
    todayAmount: roundMoney(row.today_amount || 0),
    todayUnits: roundMoney(row.today_units || 0),
    ratePerUnit: rates.pickItemRatePerUnit,
    pickItemRatePerUnit: rates.pickItemRatePerUnit,
    orderCollectRatePerUnit: rates.orderCollectRatePerUnit,
    lastEarnedAt: row.last_earned_at || null,
  };
}

function toPublicCoinEvent(row) {
  if (!row) return null;
  return {
    id: Number(row.id),
    userId: Number(row.user_id),
    externalEmployeeId: row.external_employee_id != null ? Number(row.external_employee_id) : null,
    eventType: row.event_type,
    source: row.source || "sborka-site",
    marketplace: row.marketplace || null,
    storeId: row.store_id || null,
    storeName: row.store_name || null,
    entityType: row.entity_type || null,
    entityId: row.entity_id || null,
    entityName: row.entity_name || null,
    sessionId: row.session_id != null ? Number(row.session_id) : null,
    itemId: row.item_id != null ? Number(row.item_id) : null,
    itemKey: row.item_key || null,
    orderRef: row.order_ref || null,
    productName: row.product_name || null,
    article: row.article || null,
    marketplaceCodeKind: row.marketplace_code_kind || null,
    marketplaceCode: row.marketplace_code || null,
    scannedCode: row.scanned_code || null,
    rewardUnits: roundMoney(row.reward_units || 0),
    ratePerUnit: roundMoney(row.rate_per_unit || 0),
    amountDelta: roundMoney(row.amount_delta || 0),
    balanceBefore: roundMoney(row.balance_before || 0),
    balanceAfter: roundMoney(row.balance_after || 0),
    notes: row.notes || null,
    sourceTaskId: row.source_task_id || null,
    createdAt: row.created_at || null,
  };
}

async function setCoinEventSyncState(eventId, status, errorText = null) {
  if (!eventId) return;
  await dbQuery(
    `update employee_coin_events
     set external_sync_status = $2,
         external_sync_attempts = coalesce(external_sync_attempts, 0) + 1,
         external_sync_last_error = $3,
         external_synced_at = case when $2 = 'synced' then now() else external_synced_at end,
         updated_at = now()
     where id = $1`,
    [eventId, status, errorText ? truncateText(errorText, 1000) : null]
  );
}

function getSkladEventType(localEventType) {
  if (localEventType === EARNINGS_EVENT_MANUAL_ADJUSTMENT) return "manual_adjustment";
  if (localEventType === EARNINGS_EVENT_ORDER_COLLECT) return SKLAD_EVENT_EXTERNAL_ORDER_COLLECT;
  return SKLAD_EVENT_EXTERNAL_PICK;
}

function getSkladEventSource(localEventType) {
  return localEventType === EARNINGS_EVENT_MANUAL_ADJUSTMENT ? SKLAD_SOURCE_SBORKA_ADMIN : SKLAD_SOURCE_SBORKA;
}

function getSkladSourceTaskId(event) {
  return toNullableText(event?.sourceTaskId) || `sborka:event:${event?.id || crypto.randomUUID()}`;
}

function buildSkladEventNotes(event) {
  if (event?.notes) return truncateText(event.notes, 1000);
  if (event?.eventType === EARNINGS_EVENT_MANUAL_ADJUSTMENT) return "Корректировка баланса из sborka";
  if (event?.eventType === EARNINGS_EVENT_ORDER_COLLECT) {
    if (event?.marketplace === "wb") {
      return truncateText(`Сборка заказа в поставке ${event.entityName || event.entityId || "WB"}`, 1000);
    }
    if (event?.marketplace === "oz") {
      return truncateText(`Сборка заказа в сборке ${event.entityName || event.entityId || "Ozon"}`, 1000);
    }
    return "Начисление за сборку заказа из sborka";
  }
  if (event?.marketplace === "wb") {
    return truncateText(`Сборка товара в поставке ${event.entityName || event.entityId || "WB"}`, 1000);
  }
  if (event?.marketplace === "oz") {
    return truncateText(`Сборка товара в сборке ${event.entityName || event.entityId || "Ozon"}`, 1000);
  }
  return "Начисление из sborka";
}

function buildSkladEventPayload(event, remoteEmployeeId, remoteProductId) {
  return {
    source: getSkladEventSource(event?.eventType),
    localEventId: Number(event?.id || 0) || null,
    localUserId: Number(event?.userId || 0) || null,
    localEventType: event?.eventType || null,
    remoteEventType: getSkladEventType(event?.eventType),
    remoteEmployeeId: remoteEmployeeId || null,
    remoteProductId: remoteProductId || null,
    externalEmployeeId: event?.externalEmployeeId || null,
    marketplace: event?.marketplace || null,
    storeId: event?.storeId || null,
    storeName: event?.storeName || null,
    entityType: event?.entityType || null,
    entityId: event?.entityId || null,
    entityName: event?.entityName || null,
    sessionId: event?.sessionId || null,
    itemId: event?.itemId || null,
    itemKey: event?.itemKey || null,
    orderRef: event?.orderRef || null,
    productName: event?.productName || null,
    article: event?.article || null,
    marketplaceCodeKind: event?.marketplaceCodeKind || null,
    marketplaceCode: event?.marketplaceCode || null,
    scannedCode: event?.scannedCode || null,
    rewardUnits: roundSkladMoney(event?.rewardUnits || 0),
    ratePerUnit: roundSkladMoney(event?.ratePerUnit || 0),
    amountDelta: roundSkladMoney(event?.amountDelta || 0),
    balanceBefore: roundSkladMoney(event?.balanceBefore || 0),
    balanceAfter: roundSkladMoney(event?.balanceAfter || 0),
    notes: event?.notes || null,
    sourceTaskId: getSkladSourceTaskId(event),
    createdAt: event?.createdAt || null,
  };
}

async function syncCoinEventToSklad(event) {
  if (!event?.id) return null;
  if (!SKLAD_DB_HOST || !SKLAD_DB_USER || !SKLAD_DB_NAME) {
    await setCoinEventSyncState(event.id, "error", "SKLAD_DB_NOT_CONFIGURED");
    return null;
  }
  if (!event.externalEmployeeId) {
    await setCoinEventSyncState(event.id, "error", "SKLAD_EMPLOYEE_EXTERNAL_ID_MISSING");
    return null;
  }

  await ensureSkladEarningsSchema();
  const source = getSkladEventSource(event.eventType);
  const sourceTaskId = getSkladSourceTaskId(event);
  const client = await skladDb.connect();
  try {
    await client.query("begin");
    const employeeResult = await client.query(
      `select id, coalesce(gra_balance, 0) as gra_balance
       from employees_s
       where external_employee_id = $1
       limit 1
       for update`,
      [Number(event.externalEmployeeId)]
    );
    const employee = employeeResult.rows?.[0] || null;
    if (!employee) {
      throw new Error(`SKLAD_EMPLOYEE_NOT_FOUND:${event.externalEmployeeId}`);
    }

    const existingResult = await client.query(
      `select id, balance_after
       from employee_earnings_s
       where source = $1
         and source_task_id = $2
       limit 1`,
      [source, sourceTaskId]
    );
    if (existingResult.rows?.length) {
      await client.query("commit");
      await setCoinEventSyncState(event.id, "synced", null);
      return {
        duplicate: true,
        remoteEventId: Number(existingResult.rows[0].id),
      };
    }

    let remoteProductId = null;
    if (event.article) {
      const productResult = await client.query(
        `select id
         from products_s
         where code = $1
         limit 1`,
        [event.article]
      );
      remoteProductId = productResult.rows?.[0]?.id ? Number(productResult.rows[0].id) : null;
    }

    const balanceBefore = roundSkladMoney(employee.gra_balance || 0);
    const amountDelta = roundSkladMoney(event.amountDelta || 0);
    const balanceAfter = roundSkladMoney(balanceBefore + amountDelta);
    const payload = buildSkladEventPayload(event, Number(employee.id), remoteProductId);
    const insertResult = await client.query(
      `insert into employee_earnings_s (
         employee_id, product_id, event_type, reward_units, rate_per_unit, amount_delta,
         balance_before, balance_after, notes, created_by_user_id, created_at,
         source, source_task_id, source_marketplace, source_store_id, source_store_name,
         source_entity_type, source_entity_id, source_entity_name, source_session_id,
         source_item_id, source_item_key, source_order_ref, source_product_name,
         source_article, source_marketplace_code_kind, source_marketplace_code,
         source_scanned_code, source_payload
       )
       values (
         $1, $2, $3, $4, $5, $6,
         $7, $8, $9, $10, coalesce($11::timestamptz, now()),
         $12, $13, $14, $15, $16,
         $17, $18, $19, $20,
         $21, $22, $23, $24,
         $25, $26, $27,
         $28, $29::jsonb
       )
       returning id`,
      [
        Number(employee.id),
        remoteProductId,
        getSkladEventType(event.eventType),
        roundSkladMoney(event.rewardUnits || 0),
        roundSkladMoney(event.ratePerUnit || 0),
        amountDelta,
        balanceBefore,
        balanceAfter,
        buildSkladEventNotes(event),
        null,
        event.createdAt || null,
        source,
        sourceTaskId,
        event.marketplace || null,
        event.storeId || null,
        event.storeName || null,
        event.entityType || null,
        event.entityId || null,
        event.entityName || null,
        event.sessionId || null,
        event.itemId || null,
        event.itemKey || null,
        event.orderRef || null,
        event.productName || null,
        event.article || null,
        event.marketplaceCodeKind || null,
        event.marketplaceCode || null,
        event.scannedCode || null,
        toJsonValue(payload),
      ]
    );

    await client.query(
      `update employees_s
       set gra_balance = $1,
           updated_at = now()
       where id = $2`,
      [balanceAfter, Number(employee.id)]
    );

    await client.query("commit");
    await setCoinEventSyncState(event.id, "synced", null);
    return {
      duplicate: false,
      remoteEventId: Number(insertResult.rows?.[0]?.id || 0) || null,
      remoteEmployeeId: Number(employee.id),
      remoteProductId,
    };
  } catch (error) {
    try {
      await client.query("rollback");
    } catch {}
    await setCoinEventSyncState(event.id, "error", error?.message || String(error));
    throw error;
  } finally {
    client.release();
  }
}

async function syncPendingCoinEvents(limit = 50) {
  if (pendingSkladSyncPromise) return pendingSkladSyncPromise;
  pendingSkladSyncPromise = (async () => {
    if (!SKLAD_DB_HOST || !SKLAD_DB_USER || !SKLAD_DB_NAME) return { processed: 0 };
    await ensureEarningsSchema();
    const result = await dbQuery(
      `select *
       from employee_coin_events
       where external_sync_status in ('pending', 'error')
       order by created_at asc, id asc
       limit $1`,
      [Math.max(1, Number(limit || 50))]
    );
    let processed = 0;
    for (const row of result.rows || []) {
      try {
        await syncCoinEventToSklad(toPublicCoinEvent(row));
      } catch (error) {
        console.error("[sborka] sklad sync failed:", error?.message || error);
      }
      processed += 1;
    }
    return { processed };
  })().finally(() => {
    pendingSkladSyncPromise = null;
  });
  return pendingSkladSyncPromise;
}

async function insertCoinEvent({
  userId,
  externalEmployeeId = null,
  eventType,
  source = "sborka-site",
  marketplace = null,
  storeId = null,
  storeName = null,
  entityType = null,
  entityId = null,
  entityName = null,
  sessionId = null,
  itemId = null,
  itemKey = null,
  orderRef = null,
  productName = null,
  article = null,
  marketplaceCodeKind = null,
  marketplaceCode = null,
  scannedCode = null,
  rewardUnits = 0,
  ratePerUnit = 0,
  amountDelta = null,
  notes = null,
  sourceTaskId = null,
  dedupeKey = null,
}) {
  await ensureEarningsSchema();
  const client = await mainDb.connect();
  try {
    await client.query("begin");
    const balanceRow = await client.query(
      "select balance from employee_coin_balances where user_id = $1 for update",
      [userId]
    );
    const balanceBefore = roundMoney(balanceRow.rows?.[0]?.balance || 0);
    const resolvedAmountDelta =
      amountDelta == null
        ? roundMoney(parseMoneyValue(rewardUnits, 0) * parseMoneyValue(ratePerUnit, 0))
        : parseMoneyValue(amountDelta, 0);
    const balanceAfter = roundMoney(balanceBefore + resolvedAmountDelta);
    const insertResult = await client.query(
      `insert into employee_coin_events (
         user_id, external_employee_id, event_type, source, marketplace, store_id, store_name,
         entity_type, entity_id, entity_name, session_id, item_id, item_key, order_ref,
         product_name, article, marketplace_code_kind, marketplace_code, scanned_code,
         reward_units, rate_per_unit, amount_delta, balance_before, balance_after,
         notes, source_task_id, dedupe_key, created_at, updated_at
       )
       values (
         $1, $2, $3, $4, $5, $6, $7,
         $8, $9, $10, $11, $12, $13, $14,
         $15, $16, $17, $18, $19,
         $20, $21, $22, $23, $24,
         $25, $26, $27, now(), now()
       )
       on conflict (dedupe_key) do nothing
       returning *`,
      [
        userId,
        externalEmployeeId,
        eventType,
        source,
        marketplace,
        storeId,
        storeName,
        entityType,
        entityId,
        entityName,
        sessionId,
        itemId,
        itemKey,
        orderRef,
        productName,
        article,
        marketplaceCodeKind,
        marketplaceCode,
        scannedCode,
        roundMoney(rewardUnits),
        roundMoney(ratePerUnit),
        resolvedAmountDelta,
        balanceBefore,
        balanceAfter,
        notes,
        sourceTaskId,
        dedupeKey,
      ]
    );

    let insertedEvent = insertResult.rows?.[0] || null;
    if (insertedEvent) {
      await client.query(
        `insert into employee_coin_balances (user_id, balance, updated_at)
         values ($1, $2, now())
         on conflict (user_id) do update
         set balance = excluded.balance,
             updated_at = excluded.updated_at`,
        [userId, balanceAfter]
      );
    } else if (dedupeKey) {
      const existingResult = await client.query(
        "select * from employee_coin_events where dedupe_key = $1 limit 1",
        [dedupeKey]
      );
      insertedEvent = existingResult.rows?.[0] || null;
    }

    await client.query("commit");
    const publicEvent = toPublicCoinEvent(insertedEvent);
    if (publicEvent) {
      try {
        await syncCoinEventToSklad(publicEvent);
      } catch (error) {
        console.error("[sborka] immediate sklad sync failed:", error?.message || error);
      }
    }
    const summary = await getEmployeeCoinSummary(userId);
    return {
      event: publicEvent,
      summary,
      duplicate: !Boolean(insertResult.rows?.[0]),
    };
  } catch (error) {
    try {
      await client.query("rollback");
    } catch {}
    throw error;
  } finally {
    client.release();
  }
}

async function getWbRewardContext(supplyId) {
  const result = await dbQuery(
    `select supply_name, store_id, store_name
     from analytics_employee_order_fact
     where supply_id = $1
     order by coalesce(assigned_at, order_created_at) desc nulls last
     limit 1`,
    [supplyId]
  );
  return {
    entityType: "supply",
    entityId: supplyId,
    entityName: result.rows?.[0]?.supply_name || supplyId,
    storeId: result.rows?.[0]?.store_id || null,
    storeName: result.rows?.[0]?.store_name || null,
  };
}

async function getWbOrderRewardContext(supplyId, orderId) {
  const [context, orderResult] = await Promise.all([
    getWbRewardContext(supplyId),
    dbQuery(
      `select wb_order_id, article, product_name
       from supply_orders
       where supply_id = $1 and wb_order_id = $2
       limit 1`,
      [supplyId, Number(orderId)]
    ),
  ]);
  const row = orderResult.rows?.[0] || null;
  return {
    ...context,
    orderRef: row?.wb_order_id != null ? String(row.wb_order_id) : String(orderId),
    article: row?.article || null,
    productName: row?.product_name || `Заказ ${orderId}`,
  };
}

async function getOzonRewardContext(assemblyId) {
  const result = await dbQuery(
    `select id, name, store_id, store_name
     from ozon_assemblies
     where id = $1
     limit 1`,
    [assemblyId]
  );
  const row = result.rows?.[0] || null;
  return {
    entityType: "assembly",
    entityId: assemblyId,
    entityName: row?.name || assemblyId,
    storeId: row?.store_id || null,
    storeName: row?.store_name || null,
  };
}

async function getOzonOrderRewardContext(assemblyId, postingNumber) {
  const [context, itemResult] = await Promise.all([
    getOzonRewardContext(assemblyId),
    dbQuery(
      `select
         count(*)::int as item_count,
         max(offer_id) filter (where nullif(btrim(offer_id), '') is not null) as article,
         max(name) filter (where nullif(btrim(name), '') is not null) as product_name
       from ozon_assembly_order_items
       where assembly_id = $1 and posting_number = $2`,
      [assemblyId, postingNumber]
    ),
  ]);
  const row = itemResult.rows?.[0] || {};
  const itemCount = Number(row.item_count || 0);
  const baseName = row.product_name || "";
  return {
    ...context,
    orderRef: postingNumber,
    article: row.article || null,
    productName:
      itemCount > 1 && baseName
        ? `${baseName} +${itemCount - 1}`
        : baseName || `Заказ ${postingNumber}`,
  };
}

async function awardPickScanReward({ marketplace, entityId, sessionUser, responseData, scannedCode }) {
  const item = responseData?.item || null;
  const session = responseData?.session || null;
  if (!item?.id || !sessionUser?.id) {
    return { reward: null, summary: await getEmployeeCoinSummary(sessionUser?.id || 0) };
  }
  const pickedQty = Number(item.pickedQty || 0);
  if (!Number.isFinite(pickedQty) || pickedQty < 1) {
    return { reward: null, summary: await getEmployeeCoinSummary(sessionUser.id) };
  }
  const ratePerUnit = (await getCoinRates()).pickItemRatePerUnit;
  if (!(ratePerUnit > 0)) {
    return { reward: null, summary: await getEmployeeCoinSummary(sessionUser.id) };
  }
  const context =
    marketplace === "wb" ? await getWbRewardContext(entityId) : await getOzonRewardContext(entityId);
  return insertCoinEvent({
    userId: Number(sessionUser.id),
    externalEmployeeId: sessionUser.externalEmployeeId ? Number(sessionUser.externalEmployeeId) : null,
    eventType: EARNINGS_EVENT_PICK_SCAN,
    source: "sborka-site",
    marketplace,
    storeId: context.storeId || null,
    storeName: context.storeName || null,
    entityType: context.entityType,
    entityId: context.entityId,
    entityName: context.entityName,
    sessionId: Number(session?.id || item.sessionId || 0) || null,
    itemId: Number(item.id),
    itemKey: item.itemKey || null,
    productName: item.productName || null,
    article: item.article || null,
    marketplaceCodeKind: marketplace === "wb" ? "wb_barcode" : "ozon_barcode",
    marketplaceCode: item.barcode || null,
    scannedCode: toNullableText(scannedCode) || toNullableText(item.barcode),
    rewardUnits: 1,
    ratePerUnit,
    notes:
      marketplace === "wb"
        ? `Сборка товара в поставке ${context.entityName || entityId}`
        : `Сборка товара в сборке ${context.entityName || entityId}`,
    sourceTaskId: `${marketplace}:${context.entityType}:${entityId}:${item.id}:${pickedQty}`,
    dedupeKey: `${marketplace}:pick:${session?.id || item.sessionId || 0}:${item.id}:${pickedQty}`,
  });
}

async function awardOrderCollectReward({
  marketplace,
  entityId,
  orderRef,
  sessionUser,
  scannedCode = null,
}) {
  const normalizedOrderRef = toNullableText(orderRef);
  if (!sessionUser?.id || !normalizedOrderRef) {
    return { reward: null, summary: await getEmployeeCoinSummary(sessionUser?.id || 0) };
  }
  const ratePerUnit = (await getCoinRates()).orderCollectRatePerUnit;
  if (!(ratePerUnit > 0)) {
    return { reward: null, summary: await getEmployeeCoinSummary(sessionUser.id) };
  }
  const context =
    marketplace === "wb"
      ? await getWbOrderRewardContext(entityId, normalizedOrderRef)
      : await getOzonOrderRewardContext(entityId, normalizedOrderRef);
  return insertCoinEvent({
    userId: Number(sessionUser.id),
    externalEmployeeId: sessionUser.externalEmployeeId ? Number(sessionUser.externalEmployeeId) : null,
    eventType: EARNINGS_EVENT_ORDER_COLLECT,
    source: "sborka-site",
    marketplace,
    storeId: context.storeId || null,
    storeName: context.storeName || null,
    entityType: context.entityType,
    entityId: context.entityId,
    entityName: context.entityName,
    sessionId: null,
    itemId: null,
    itemKey: null,
    orderRef: context.orderRef,
    productName: context.productName || null,
    article: context.article || null,
    marketplaceCodeKind: marketplace === "wb" ? "wb_order" : "ozon_posting",
    marketplaceCode: context.orderRef,
    scannedCode: toNullableText(scannedCode) || null,
    rewardUnits: 1,
    ratePerUnit,
    notes:
      marketplace === "wb"
        ? `Сборка заказа в поставке ${context.entityName || entityId}`
        : `Сборка заказа в сборке ${context.entityName || entityId}`,
    sourceTaskId: `${marketplace}:${context.entityType}:${entityId}:order:${context.orderRef}`,
    dedupeKey: `${marketplace}:collect:${context.entityType}:${entityId}:${context.orderRef}`,
  });
}

function queueOrderCollectReward(params) {
  setImmediate(async () => {
    try {
      await awardOrderCollectReward(params);
    } catch (error) {
      console.error("[earnings] queued order collect reward failed:", error?.message || error);
    }
  });
}

function getEarningsFilters(query) {
  return {
    from: toIsoDate(query?.from),
    to: toIsoDate(query?.to),
    marketplace: ["oz", "wb"].includes(normalizeText(query?.marketplace)) ? normalizeText(query?.marketplace) : null,
    storeId: toNullableText(query?.storeId),
    userId: toNullableInteger(query?.userId),
    search: toNullableText(query?.search),
  };
}

function getEarningsWhere(filters, params) {
  const clauses = [];
  if (filters.from) {
    params.push(filters.from);
    clauses.push(`e.created_at >= (($${params.length}::date)::timestamp at time zone '${EARNINGS_TZ}')`);
  }
  if (filters.to) {
    params.push(filters.to);
    clauses.push(`e.created_at < ((($${params.length}::date + 1)::timestamp) at time zone '${EARNINGS_TZ}')`);
  }
  if (filters.marketplace) {
    params.push(filters.marketplace);
    clauses.push(`e.marketplace = $${params.length}`);
  }
  if (filters.storeId) {
    params.push(filters.storeId);
    clauses.push(`e.store_id = $${params.length}`);
  }
  if (filters.userId) {
    params.push(filters.userId);
    clauses.push(`e.user_id = $${params.length}`);
  }
  if (filters.search) {
    params.push(`%${filters.search.toLowerCase()}%`);
    clauses.push(`(
      lower(coalesce(e.product_name, '')) like $${params.length}
      or lower(coalesce(e.article, '')) like $${params.length}
      or lower(coalesce(e.store_name, '')) like $${params.length}
      or lower(coalesce(e.entity_name, '')) like $${params.length}
      or lower(coalesce(e.order_ref, '')) like $${params.length}
      or lower(coalesce(e.marketplace_code, '')) like $${params.length}
      or lower(coalesce(e.scanned_code, '')) like $${params.length}
      or lower(coalesce(u.surname, '') || ' ' || coalesce(u.name, '') || ' ' || coalesce(u.external_full_name, '')) like $${params.length}
    )`);
  }
  return clauses.length ? `where ${clauses.join(" and ")}` : "";
}

async function getEarningsDashboard(filters) {
  await ensureEarningsSchema();
  const params = [];
  const whereSql = getEarningsWhere(filters, params);
  const hasEventScopedFilters = Boolean(filters.from || filters.to || filters.marketplace || filters.storeId || filters.search);

  const summaryResult = await dbQuery(
    `with filtered as (
       select e.*, u.surname, u.name, u.external_full_name
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
     ),
     balance_rows as (
       select user_id, balance
       from employee_coin_balances
       where coalesce(balance, 0) > 0
     ),
      marketplace_totals as (
        select marketplace, coalesce(sum(amount_delta), 0)::numeric(14,2) as amount
        from filtered
        group by marketplace
      )
      select
        coalesce((select sum(amount_delta) from filtered), 0)::numeric(14,2) as total_amount,
        coalesce((select sum(reward_units) from filtered), 0)::numeric(14,2) as total_units,
        coalesce((select count(*) from filtered), 0)::int as total_events,
        coalesce((select sum(amount_delta) from filtered where event_type = 'pick_scan'), 0)::numeric(14,2) as pick_amount,
        coalesce((select sum(reward_units) from filtered where event_type = 'pick_scan'), 0)::numeric(14,2) as pick_units,
        coalesce((select count(*) from filtered where event_type = 'pick_scan'), 0)::int as pick_events,
        coalesce((select sum(amount_delta) from filtered where event_type = 'order_collect'), 0)::numeric(14,2) as order_amount,
        coalesce((select sum(reward_units) from filtered where event_type = 'order_collect'), 0)::numeric(14,2) as order_units,
        coalesce((select count(*) from filtered where event_type = 'order_collect'), 0)::int as order_events,
        coalesce((select sum(amount_delta) from filtered where event_type = 'manual_adjustment'), 0)::numeric(14,2) as manual_amount,
        coalesce((select count(*) from filtered where event_type = 'manual_adjustment'), 0)::int as manual_events,
        coalesce((select count(distinct user_id) from filtered), 0)::int as active_employees,
        coalesce((select sum(balance) from balance_rows), 0)::numeric(14,2) as total_balance,
        coalesce((select count(*) from balance_rows), 0)::int as balance_holders,
        coalesce((select amount from marketplace_totals where marketplace = 'oz'), 0)::numeric(14,2) as oz_amount,
        coalesce((select amount from marketplace_totals where marketplace = 'wb'), 0)::numeric(14,2) as wb_amount`,
    params
  );

  const marketplaceResult = await dbQuery(
    `with filtered as (
       select e.*, u.surname, u.name, u.external_full_name
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
     )
     select
       marketplace,
       coalesce(sum(amount_delta), 0)::numeric(14,2) as amount,
       coalesce(sum(reward_units), 0)::numeric(14,2) as units,
       count(*)::int as event_count,
       count(distinct user_id)::int as active_employees,
       max(created_at) as last_event_at
     from filtered
     group by marketplace
     order by case marketplace when 'wb' then 0 when 'oz' then 1 else 2 end, marketplace`,
    params
  );

  const storeStatsResult = await dbQuery(
    `with filtered as (
       select e.*, u.surname, u.name, u.external_full_name
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
     )
     select
       marketplace,
       store_id,
       coalesce(max(store_name), max(store_id), '—') as store_name,
       coalesce(sum(amount_delta), 0)::numeric(14,2) as amount,
       coalesce(sum(reward_units), 0)::numeric(14,2) as units,
       count(*)::int as event_count,
       count(distinct user_id)::int as active_employees,
       max(created_at) as last_event_at
     from filtered
     where store_id is not null or store_name is not null
     group by marketplace, store_id
     order by amount desc, store_name asc nulls last, store_id asc nulls last`,
    params
  );

  const employeeParams = [...params];
  const employeeWhere = [`u.role = 'employee'`, `coalesce(b.balance, 0) > 0`];
  if (filters.userId) {
    employeeParams.push(filters.userId);
    employeeWhere.push(`u.id = $${employeeParams.length}`);
  }
  if (hasEventScopedFilters && !filters.userId) {
    employeeWhere.push(`exists (select 1 from filtered fx where fx.user_id = u.id)`);
  }

  const employeeResult = await dbQuery(
    `with filtered as (
       select e.*, u.surname, u.name, u.external_full_name, u.position
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
     )
      select
        u.id as user_id,
        u.surname,
        u.name,
        u.external_full_name,
        u.position,
        coalesce(b.balance, 0)::numeric(14,2) as balance,
        coalesce(sum(filtered.amount_delta), 0)::numeric(14,2) as period_amount,
        coalesce(sum(filtered.reward_units), 0)::numeric(14,2) as period_units,
        coalesce(sum(case when filtered.event_type = 'pick_scan' then filtered.amount_delta else 0 end), 0)::numeric(14,2) as item_amount,
        coalesce(sum(case when filtered.event_type = 'pick_scan' then filtered.reward_units else 0 end), 0)::numeric(14,2) as item_units,
        coalesce(sum(case when filtered.event_type = 'order_collect' then filtered.amount_delta else 0 end), 0)::numeric(14,2) as order_amount,
        coalesce(sum(case when filtered.event_type = 'order_collect' then filtered.reward_units else 0 end), 0)::numeric(14,2) as order_units,
        count(filtered.id)::int as event_count,
        max(filtered.created_at) as last_earned_at
     from users u
     join employee_coin_balances b on b.user_id = u.id
     left join filtered on filtered.user_id = u.id
     where ${employeeWhere.join(" and ")}
     group by u.id, u.surname, u.name, u.external_full_name, u.position, b.balance
     order by balance desc, period_amount desc, last_earned_at desc nulls last, u.surname asc, u.name asc`
    ,
    employeeParams
  );

  const eventsResult = await dbQuery(
    `select
       e.*,
       u.surname,
       u.name,
       u.external_full_name
     from employee_coin_events e
     join users u on u.id = e.user_id
     ${whereSql}
     order by e.created_at desc
     limit 250`,
    params
  );

  const storesResult = await dbQuery(
    `select distinct marketplace, store_id, store_name
     from employee_coin_events
     where store_id is not null or store_name is not null
     order by marketplace nulls last, store_name nulls last, store_id nulls last`
  );

  const rates = await getCoinRates();
  const summary = summaryResult.rows?.[0] || {};
  return {
    settings: rates,
    summary: {
      totalAmount: roundMoney(summary.total_amount || 0),
      totalUnits: roundMoney(summary.total_units || 0),
      totalEvents: Number(summary.total_events || 0),
      pickAmount: roundMoney(summary.pick_amount || 0),
      pickUnits: roundMoney(summary.pick_units || 0),
      pickEvents: Number(summary.pick_events || 0),
      orderAmount: roundMoney(summary.order_amount || 0),
      orderUnits: roundMoney(summary.order_units || 0),
      orderEvents: Number(summary.order_events || 0),
      manualAmount: roundMoney(summary.manual_amount || 0),
      manualEvents: Number(summary.manual_events || 0),
      activeEmployees: Number(summary.active_employees || 0),
      totalBalance: roundMoney(summary.total_balance || 0),
      balanceHolders: Number(summary.balance_holders || 0),
      ozAmount: roundMoney(summary.oz_amount || 0),
      wbAmount: roundMoney(summary.wb_amount || 0),
    },
    marketplaces: (marketplaceResult.rows || []).map((row) => ({
      marketplace: row.marketplace || null,
      amount: roundMoney(row.amount || 0),
      units: roundMoney(row.units || 0),
      eventCount: Number(row.event_count || 0),
      activeEmployees: Number(row.active_employees || 0),
      lastEventAt: row.last_event_at || null,
    })),
    storeStats: (storeStatsResult.rows || []).map((row) => ({
      marketplace: row.marketplace || null,
      storeId: row.store_id || null,
      storeName: row.store_name || row.store_id || "—",
      amount: roundMoney(row.amount || 0),
      units: roundMoney(row.units || 0),
      eventCount: Number(row.event_count || 0),
      activeEmployees: Number(row.active_employees || 0),
      lastEventAt: row.last_event_at || null,
    })),
    employees: (employeeResult.rows || []).map((row) => ({
      userId: Number(row.user_id),
      surname: row.surname || "",
      name: row.name || "",
      fullName: normalizeText(row.external_full_name) || normalizeText(`${row.surname || ""} ${row.name || ""}`) || "—",
      position: row.position || null,
      balance: roundMoney(row.balance || 0),
      periodAmount: roundMoney(row.period_amount || 0),
      periodUnits: roundMoney(row.period_units || 0),
      itemAmount: roundMoney(row.item_amount || 0),
      itemUnits: roundMoney(row.item_units || 0),
      orderAmount: roundMoney(row.order_amount || 0),
      orderUnits: roundMoney(row.order_units || 0),
      eventCount: Number(row.event_count || 0),
      lastEarnedAt: row.last_earned_at || null,
    })),
    events: (eventsResult.rows || []).map((row) => ({
      ...toPublicCoinEvent(row),
      employeeName:
        normalizeText(row.external_full_name) || normalizeText(`${row.surname || ""} ${row.name || ""}`) || "—",
    })),
    stores: (storesResult.rows || []).map((row) => ({
      marketplace: row.marketplace || null,
      storeId: row.store_id || null,
      storeName: row.store_name || row.store_id || "—",
    })),
  };
}

async function getEmployeeEarningsDetail(userId, filters = {}) {
  await ensureEarningsSchema();
  const employeeResult = await dbQuery(
    `select
       u.id as user_id,
       u.surname,
       u.name,
       u.external_full_name,
       u.position,
       coalesce(b.balance, 0)::numeric(14,2) as balance
     from users u
     left join employee_coin_balances b on b.user_id = u.id
     where u.id = $1 and u.role = 'employee'
     limit 1`,
    [userId]
  );
  const employeeRow = employeeResult.rows?.[0] || null;
  if (!employeeRow) return null;

  const scopedFilters = { ...filters, userId };
  const params = [];
  const whereSql = getEarningsWhere(scopedFilters, params);

  const summaryResult = await dbQuery(
    `with filtered as (
       select e.*
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
      )
      select
        coalesce(sum(case when event_type = 'pick_scan' then amount_delta else 0 end), 0)::numeric(14,2) as item_amount,
        coalesce(sum(case when event_type = 'pick_scan' then reward_units else 0 end), 0)::numeric(14,2) as item_units,
        count(*) filter (where event_type = 'pick_scan')::int as item_events,
        coalesce(sum(case when event_type = 'order_collect' then amount_delta else 0 end), 0)::numeric(14,2) as order_amount,
        coalesce(sum(case when event_type = 'order_collect' then reward_units else 0 end), 0)::numeric(14,2) as order_units,
        count(*) filter (where event_type = 'order_collect')::int as order_events,
        coalesce(sum(case when event_type = 'manual_adjustment' then amount_delta else 0 end), 0)::numeric(14,2) as manual_amount,
        count(*) filter (where event_type = 'manual_adjustment')::int as manual_events,
        max(created_at) as last_earned_at
     from filtered`,
    params
  );

  const marketplaceResult = await dbQuery(
    `with filtered as (
       select e.*
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
     )
     select
       marketplace,
       coalesce(sum(amount_delta), 0)::numeric(14,2) as amount,
       coalesce(sum(reward_units), 0)::numeric(14,2) as units,
       count(*)::int as event_count,
       max(created_at) as last_event_at
     from filtered
     where event_type <> 'manual_adjustment'
     group by marketplace
     order by case marketplace when 'wb' then 0 when 'oz' then 1 else 2 end, marketplace`,
    params
  );

  const entityResult = await dbQuery(
    `with filtered as (
       select e.*
       from employee_coin_events e
       join users u on u.id = e.user_id
       ${whereSql}
     )
     select
       marketplace,
       entity_type,
       entity_id,
       coalesce(max(entity_name), max(entity_id), '—') as entity_name,
       store_id,
       coalesce(max(store_name), max(store_id), '—') as store_name,
       coalesce(sum(amount_delta), 0)::numeric(14,2) as amount,
       coalesce(sum(reward_units), 0)::numeric(14,2) as units,
       count(*)::int as event_count,
       max(created_at) as last_event_at
     from filtered
     where event_type <> 'manual_adjustment'
       and (entity_id is not null or entity_name is not null)
     group by marketplace, entity_type, entity_id, store_id
     order by
       case marketplace when 'wb' then 0 when 'oz' then 1 else 2 end,
       amount desc,
       last_event_at desc nulls last,
       entity_name asc nulls last,
       entity_id asc nulls last`,
    params
  );

  const summary = summaryResult.rows?.[0] || {};
  return {
    employee: {
      userId: Number(employeeRow.user_id),
      surname: employeeRow.surname || "",
      name: employeeRow.name || "",
      fullName:
        normalizeText(employeeRow.external_full_name) ||
        normalizeText(`${employeeRow.surname || ""} ${employeeRow.name || ""}`) ||
        "—",
      position: employeeRow.position || null,
      balance: roundMoney(employeeRow.balance || 0),
    },
      summary: {
        balance: roundMoney(employeeRow.balance || 0),
        itemAmount: roundMoney(summary.item_amount || 0),
        itemUnits: roundMoney(summary.item_units || 0),
        itemEvents: Number(summary.item_events || 0),
        orderAmount: roundMoney(summary.order_amount || 0),
        orderUnits: roundMoney(summary.order_units || 0),
        orderEvents: Number(summary.order_events || 0),
        manualAmount: roundMoney(summary.manual_amount || 0),
        manualEvents: Number(summary.manual_events || 0),
        lastEarnedAt: summary.last_earned_at || null,
    },
    marketplaces: (marketplaceResult.rows || []).map((row) => ({
      marketplace: row.marketplace || null,
      amount: roundMoney(row.amount || 0),
      units: roundMoney(row.units || 0),
      eventCount: Number(row.event_count || 0),
      lastEventAt: row.last_event_at || null,
    })),
    entities: (entityResult.rows || []).map((row) => ({
      marketplace: row.marketplace || null,
      entityType: row.entity_type || null,
      entityId: row.entity_id || null,
      entityName: row.entity_name || row.entity_id || "—",
      storeId: row.store_id || null,
      storeName: row.store_name || row.store_id || "—",
      amount: roundMoney(row.amount || 0),
      units: roundMoney(row.units || 0),
      eventCount: Number(row.event_count || 0),
      lastEventAt: row.last_event_at || null,
    })),
  };
}

function requireAdminUser(req, res) {
  if (!ROLE_SET_ADMIN.has(req.sessionUser?.role)) {
    res.status(403).json({ error: "ADMIN_REQUIRED" });
    return false;
  }
  return true;
}

function requireSuperUser(req, res) {
  if (req.sessionUser?.role !== ROLE_SUPER) {
    res.status(403).json({ error: "SUPERADMIN_REQUIRED" });
    return false;
  }
  return true;
}

function normalizeLocationCode(value) {
  const normalized = normalizeText(value).toLowerCase();
  return LOCATION_CODES.has(normalized) ? normalized : null;
}

function normalizeManagedRole(value, fallback = ROLE_EMPLOYEE) {
  const normalized = normalizeText(value).toLowerCase();
  if (normalized === ROLE_ADMIN || normalized === ROLE_EMPLOYEE) return normalized;
  return fallback;
}

function createLocalPasswordHash(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return { salt, hash };
}

function createPlaceholderPasswordHash() {
  return createLocalPasswordHash(crypto.randomBytes(32).toString("hex"));
}

// The DDL below is idempotent but costs many round-trips to the remote main DB.
// Running it on EVERY settings call made simple saves feel slow — run once per process.
let ensureGlobalSettingsSchemaPromise = null;
function ensureGlobalSettingsSchema() {
  if (!ensureGlobalSettingsSchemaPromise) {
    ensureGlobalSettingsSchemaPromise = ensureGlobalSettingsSchemaOnce().catch((error) => {
      ensureGlobalSettingsSchemaPromise = null; // retry next call if it failed
      throw error;
    });
  }
  return ensureGlobalSettingsSchemaPromise;
}

async function ensureGlobalSettingsSchemaOnce() {
  await dbQuery(
    `create table if not exists site_locations (
       location_code text primary key,
       label text not null,
       sort_order integer not null default 100,
       active boolean not null default true,
       created_at timestamptz not null default now(),
       updated_at timestamptz not null default now()
     )`
  );
  await dbQuery(
    `insert into site_locations (location_code, label, sort_order)
     values ('izhevsk', 'Ижевск', 10), ('moscow', 'Москва', 20)
     on conflict (location_code) do update
     set label = excluded.label,
         sort_order = excluded.sort_order,
         active = true,
         updated_at = now()`
  );
  await dbQuery(
    `create table if not exists site_location_warehouse_settings (
       id bigserial primary key,
       location_code text not null references site_locations(location_code),
       marketplace text not null check (marketplace in ('all', 'wb', 'oz')),
       store_id text,
       marketplace_warehouse_id text,
       marketplace_warehouse_name text,
       sklad_warehouse_id integer,
       sklad_warehouse_name text,
       active boolean not null default true,
       created_at timestamptz not null default now(),
       updated_at timestamptz not null default now(),
       updated_by bigint references users(id) on delete set null
     )`
  );
  await dbQuery(
    `create unique index if not exists idx_site_location_warehouse_settings_unique
     on site_location_warehouse_settings (
       location_code,
       marketplace,
       coalesce(store_id, ''),
       coalesce(marketplace_warehouse_id, '')
     )`
  );
  await dbQuery("alter table users add column if not exists location_code text references site_locations(location_code)");
  await dbQuery("alter table users add column if not exists location_configured_at timestamptz");
  await dbQuery("alter table users add column if not exists location_configured_by bigint references users(id) on delete set null");
  await dbQuery("alter table supply_settings add column if not exists location_code text references site_locations(location_code)").catch(() => {});
  await dbQuery("alter table supply_settings add column if not exists sklad_warehouse_id integer").catch(() => {});
  await dbQuery("alter table supply_settings add column if not exists sklad_warehouse_name text").catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists location_code text references site_locations(location_code)").catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists sklad_warehouse_id integer").catch(() => {});
  await dbQuery("alter table ozon_assemblies add column if not exists sklad_warehouse_name text").catch(() => {});
  await dbQuery(
    `create table if not exists site_settings_audit (
       id bigserial primary key,
       actor_user_id bigint references users(id) on delete set null,
       action text not null,
       entity_type text not null,
       entity_id text,
       before_json jsonb,
       after_json jsonb,
       created_at timestamptz not null default now()
     )`
  );
  await ensureBootstrapSuperAdmin();
}

async function ensureBootstrapSuperAdmin() {
  if (bootstrapSuperAdminEnsured) return;
  const login = normalizeText(process.env.SBORKA_BOOTSTRAP_SUPERADMIN_LOGIN || process.env.BOOTSTRAP_SUPERADMIN_LOGIN);
  const password = String(process.env.SBORKA_BOOTSTRAP_SUPERADMIN_PASSWORD || process.env.BOOTSTRAP_SUPERADMIN_PASSWORD || "");
  if (!login || !password) {
    bootstrapSuperAdminEnsured = true;
    return;
  }
  if (password.length < 8) {
    console.warn("[auth] bootstrap super_admin skipped: password must be at least 8 characters");
    bootstrapSuperAdminEnsured = true;
    return;
  }

  const displayName = normalizeText(process.env.SBORKA_BOOTSTRAP_SUPERADMIN_NAME || process.env.BOOTSTRAP_SUPERADMIN_NAME || "");
  const publicName = displayName && displayName.toLowerCase() !== login.toLowerCase() ? displayName : "";
  const resetPassword = process.env.SBORKA_BOOTSTRAP_SUPERADMIN_RESET === "1" || process.env.BOOTSTRAP_SUPERADMIN_RESET === "1";
  const existing = await dbQuery(
    `select id, password_hash
     from users
     where role = $1 and coalesce(auth_source, $2) = $2 and lower(surname) = lower($3)
     order by id
     limit 1`,
    [ROLE_SUPER, AUTH_SOURCE_LOCAL, login]
  );
  const row = existing.rows?.[0];
  if (row?.id) {
    if (resetPassword) {
      const nextPassword = createLocalPasswordHash(password);
      await dbQuery(
        `update users
         set password_hash = $1,
             password_salt = $2,
             name = $3,
             position = null,
             site_code = null,
             auth_source = $4,
             updated_at = now()
         where id = $5`,
        [nextPassword.hash, nextPassword.salt, publicName, AUTH_SOURCE_LOCAL, Number(row.id)]
      );
      await dbQuery("delete from sessions where user_id = $1", [Number(row.id)]).catch(() => {});
      localSuperUserCache.delete(login.toLowerCase());
      console.log(`[auth] bootstrap super_admin password reset for ${login}`);
    }
    bootstrapSuperAdminEnsured = true;
    return;
  }

  const nextPassword = createLocalPasswordHash(password);
  await dbQuery(
    `insert into users (
       surname, name, role, position, password_hash, password_salt,
       created_at, updated_at, site_code, auth_source
     )
     values ($1, $2, $3, null, $4, $5, now(), now(), null, $6)`,
    [login, publicName, ROLE_SUPER, nextPassword.hash, nextPassword.salt, AUTH_SOURCE_LOCAL]
  );
  localSuperUserCache.delete(login.toLowerCase());
  bootstrapSuperAdminEnsured = true;
  console.log(`[auth] bootstrap super_admin created for ${login}`);
}

async function writeSettingsAudit(actorUserId, action, entityType, entityId, beforeValue, afterValue) {
  try {
    await dbQuery(
      `insert into site_settings_audit (actor_user_id, action, entity_type, entity_id, before_json, after_json)
       values ($1, $2, $3, $4, $5::jsonb, $6::jsonb)`,
      [
        actorUserId ? Number(actorUserId) : null,
        truncateText(action, 120),
        truncateText(entityType, 120),
        entityId == null ? null : truncateText(String(entityId), 240),
        JSON.stringify(beforeValue ?? null),
        JSON.stringify(afterValue ?? null),
      ]
    );
  } catch (error) {
    console.warn("[global-settings] audit write failed:", error?.message || error);
  }
}

function toGlobalUserPayload(row, externalStatusById = new Map()) {
  const publicUser = toPublicUser(row);
  const externalUserId = row.external_user_id ? Number(row.external_user_id) : null;
  const externalStatus = externalUserId ? externalStatusById.get(externalUserId) || null : null;
  const locationCode = normalizeLocationCode(row.location_code);
  return {
    ...publicUser,
    role: row.role,
    authSource: row.auth_source || null,
    siteCode: row.site_code || null,
    externalUserId,
    externalEmployeeId: row.external_employee_id ? Number(row.external_employee_id) : null,
    externalLogin: row.external_login || null,
    externalFullName: row.external_full_name || null,
    externalStatus,
    archived: row.role === ROLE_ARCHIVED,
    locationCode,
    locationConfiguredAt: row.location_configured_at || null,
    locationConfiguredBy: row.location_configured_by ? Number(row.location_configured_by) : null,
    needsSetup: row.role === ROLE_EMPLOYEE && !locationCode,
    balance: row.balance == null ? null : roundMoney(row.balance),
    lastEarnedAt: row.last_earned_at || null,
  };
}

async function getExternalStatuses(externalUserIds) {
  const ids = [...new Set((externalUserIds || []).map((value) => Number(value)).filter((value) => Number.isInteger(value) && value > 0))];
  const out = new Map();
  if (!ids.length || !isExternalAuthConfigured()) return out;
  const result = await authQuery(
    `select u.id, coalesce(e.status, '') as status
     from users_d u
     left join employees_d e on e.id = u.employee_id
     where u.id = any($1::int[])`,
    [ids]
  );
  for (const row of result.rows || []) {
    out.set(Number(row.id), normalizeText(row.status) || null);
  }
  return out;
}

async function listGlobalUsers() {
  await ensureGlobalSettingsSchema();
  const result = await dbQuery(
    `select u.id,
            u.surname,
            u.name,
            u.role,
            u.position,
            u.created_at,
            u.site_code,
            u.auth_source,
            u.external_user_id,
            u.external_employee_id,
            u.external_login,
            u.external_full_name,
            u.location_code,
            u.location_configured_at,
            u.location_configured_by,
            b.balance,
            max(e.created_at) as last_earned_at
     from users u
     left join employee_coin_balances b on b.user_id = u.id
     left join employee_coin_events e on e.user_id = u.id
     where u.role in ($1, $2, $3, $4)
     group by u.id, b.balance
     order by
       case u.role when $1 then 0 when $2 then 1 when $3 then 2 else 3 end,
       coalesce(nullif(u.external_full_name, ''), concat_ws(' ', u.surname, u.name), u.external_login)`,
    [ROLE_SUPER, ROLE_ADMIN, ROLE_EMPLOYEE, ROLE_ARCHIVED]
  );
  const statuses = await getExternalStatuses(result.rows.map((row) => row.external_user_id));
  return result.rows.map((row) => toGlobalUserPayload(row, statuses));
}

async function listExternalDirectoryUsers() {
  if (!isExternalAuthConfigured()) return [];
  const existingResult = await dbQuery(
    `select external_user_id
     from users
     where auth_source = $1 and external_user_id is not null and role <> $2`,
    [AUTH_SOURCE_EXTERNAL, ROLE_ARCHIVED]
  );
  const existingIds = new Set(existingResult.rows.map((row) => Number(row.external_user_id)));
  const result = await authQuery(
    `select u.id,
            u.login,
            coalesce(nullif(u.full_name, ''), nullif(e.full_name, ''), u.login) as full_name,
            u.employee_id,
            coalesce(e.status, '') as status
     from users_d u
     join employees_d e on e.id = u.employee_id
     where coalesce(e.status, '') = 'active'
     order by coalesce(nullif(u.full_name, ''), nullif(e.full_name, ''), u.login)`,
    []
  );
  return (result.rows || []).map((row) => ({
    id: Number(row.id),
    login: row.login || "",
    fullName: row.full_name || row.login || "",
    employeeId: row.employee_id ? Number(row.employee_id) : null,
    status: row.status || "",
    alreadyAdded: existingIds.has(Number(row.id)),
  }));
}

async function getExternalUserById(externalUserId) {
  if (!isExternalAuthConfigured()) return null;
  const result = await authQuery(
    `select u.id,
            u.login,
            coalesce(nullif(u.full_name, ''), nullif(e.full_name, ''), u.login) as full_name,
            u.employee_id,
            coalesce(e.status, '') as status
     from users_d u
     join employees_d e on e.id = u.employee_id
     where u.id = $1 and coalesce(e.status, '') = 'active'
     limit 1`,
    [Number(externalUserId)]
  );
  const row = result.rows?.[0];
  if (!row) return null;
  return {
    id: Number(row.id),
    login: normalizeText(row.login),
    fullName: normalizeText(row.full_name) || normalizeText(row.login),
    employeeId: row.employee_id ? Number(row.employee_id) : null,
    status: row.status || "",
  };
}

// Каталог сотрудников для desktop-админки отпечатков: активные + стажёры (internship).
// В отличие от listExternalDirectoryUsers (только active), сюда попадают и стажёры,
// чтобы суперадмин мог завести им отпечаток до первого входа на сайт.
const DESKTOP_DIRECTORY_STATUSES = EMPLOYEE_ACTIVE_STATUSES;

async function listDesktopDirectory() {
  if (!isExternalAuthConfigured()) return [];
  const existingResult = await dbQuery(
    `select external_user_id
     from users
     where auth_source = $1 and external_user_id is not null and role <> $2`,
    [AUTH_SOURCE_EXTERNAL, ROLE_ARCHIVED]
  );
  const existingIds = new Set(existingResult.rows.map((row) => Number(row.external_user_id)));
  const result = await authQuery(
    `select u.id,
            u.login,
            coalesce(nullif(u.full_name, ''), nullif(e.full_name, ''), u.login) as full_name,
            u.employee_id,
            coalesce(e.status, '') as status
     from users_d u
     join employees_d e on e.id = u.employee_id
     where coalesce(e.status, '') = any($1::text[])
     order by coalesce(nullif(u.full_name, ''), nullif(e.full_name, ''), u.login)`,
    [DESKTOP_DIRECTORY_STATUSES]
  );
  return (result.rows || []).map((row) => ({
    externalUserId: Number(row.id),
    login: row.login || "",
    fullName: row.full_name || row.login || "",
    employeeId: row.employee_id ? Number(row.employee_id) : null,
    status: row.status || "",
    isIntern: normalizeText(row.status) === "internship",
    alreadyAdded: existingIds.has(Number(row.id)),
  }));
}

// Внешний сотрудник по id, включая стажёров — для desktop-провижининга.
async function getDesktopExternalUserById(externalUserId) {
  if (!isExternalAuthConfigured()) return null;
  const result = await authQuery(
    `select u.id,
            u.login,
            coalesce(nullif(u.full_name, ''), nullif(e.full_name, ''), u.login) as full_name,
            u.employee_id,
            coalesce(e.status, '') as status
     from users_d u
     join employees_d e on e.id = u.employee_id
     where u.id = $1 and coalesce(e.status, '') = any($2::text[])
     limit 1`,
    [Number(externalUserId), DESKTOP_DIRECTORY_STATUSES]
  );
  const row = result.rows?.[0];
  if (!row) return null;
  return {
    id: Number(row.id),
    login: normalizeText(row.login),
    fullName: normalizeText(row.full_name) || normalizeText(row.login),
    employeeId: row.employee_id ? Number(row.employee_id) : null,
    status: row.status || "",
  };
}

// Завести/привязать сотрудника в СКЛАДСКОЙ системе (bd2: employees_s + users_s),
// чтобы скан/сборка не падали с «Сотрудник не найден в складской БД или не привязан к users_s».
// employees_s связывается по external_employee_id (= employees_d.id), users_s — по employee_id (= employees_s.id).
async function ensureWarehouseUser(externalEmployeeId, fullName, login) {
  if (!isExternalAuthConfigured()) return { ok: false, reason: "no-auth-db" };
  const empId = Number(externalEmployeeId);
  if (!Number.isInteger(empId) || empId <= 0) return { ok: false, reason: "no-emp-id" };
  try {
    // 1) employees_s по external_employee_id (обычно уже есть — склад синхронит справочник)
    let es = await skladQuery(
      "select id from employees_s where external_employee_id = $1 order by id limit 1",
      [empId]
    );
    let employeesSId = es.rows?.[0]?.id ? Number(es.rows[0].id) : null;
    if (!employeesSId) {
      const ins = await skladQuery(
        `insert into employees_s (full_name, position, active, external_employee_id, gra_balance)
         values ($1, $2, true, $3, 0) returning id`,
        [normalizeText(fullName) || normalizeText(login) || `emp ${empId}`, "Упаковщик", empId]
      );
      employeesSId = Number(ins.rows[0].id);
    }
    // 2) активный users_s по employee_id
    const us = await skladQuery(
      "select id from users_s where employee_id = $1 and coalesce(active, true) = true order by id limit 1",
      [employeesSId]
    );
    if (us.rows?.[0]?.id) {
      return { ok: true, employeesSId, usersSId: Number(us.rows[0].id), created: false };
    }
    // данные для связи из users_d (логин как username, users_d_id, password_hash)
    const ud = await skladQuery(
      "select id, login, password_hash from users_d where employee_id = $1 order by id limit 1",
      [empId]
    );
    const udRow = ud.rows?.[0] || null;
    const usersDId = udRow?.id ? Number(udRow.id) : null;
    const username = normalizeText(udRow?.login) || `emp_${empId}`;
    const passwordHash = udRow?.password_hash || "__desktop_no_login__";
    // role_id для роли employee (как у существующих складских сотрудников)
    const roleRes = await skladQuery(
      "select role_id from users_s where lower(role) = 'employee' and role_id is not null order by id desc limit 1",
      []
    );
    const roleId = Number(roleRes.rows?.[0]?.role_id || 3) || 3;
    const insU = await skladQuery(
      `insert into users_s (username, password_hash, role, employee_id, active, role_id, users_d_id)
       values ($1, $2, 'employee', $3, true, $4, $5)
       on conflict do nothing
       returning id`,
      [username, passwordHash, employeesSId, roleId, usersDId]
    );
    if (insU.rows?.[0]?.id) {
      return { ok: true, employeesSId, usersSId: Number(insU.rows[0].id), created: true };
    }
    // конфликт по username — перепроверим, вдруг уже появился users_s для этого сотрудника
    const again = await skladQuery(
      "select id from users_s where employee_id = $1 order by id limit 1",
      [employeesSId]
    );
    return again.rows?.[0]?.id
      ? { ok: true, employeesSId, usersSId: Number(again.rows[0].id), created: false }
      : { ok: false, reason: "username-taken", employeesSId };
  } catch (e) {
    return { ok: false, reason: "error", error: String(e?.message || e) };
  }
}

// Завести локального сотрудника (role=employee) из общей базы для desktop-отпечатка
// + привязать его к складской системе. Возвращает { userId, created, alreadyExisted, warehouse }.
async function provisionDesktopEmployee(externalUserId) {
  await ensureGlobalSettingsSchema();
  const externalUser = await getDesktopExternalUserById(externalUserId);
  if (!externalUser?.employeeId) {
    const error = new Error("Сотрудник не активен или у него нет учётной записи для входа");
    error.statusCode = 400;
    throw error;
  }
  // Привязка к складской системе нужна в любом случае (даже если локальный юзер уже есть)
  const warehouse = await ensureWarehouseUser(externalUser.employeeId, externalUser.fullName, externalUser.login);

  const existing = await dbQuery(
    `select id, role
     from users
     where auth_source = $1 and external_user_id = $2
     limit 1`,
    [AUTH_SOURCE_EXTERNAL, externalUser.id]
  );
  if (existing.rows?.[0]?.id && existing.rows[0].role !== ROLE_ARCHIVED) {
    return { userId: Number(existing.rows[0].id), created: false, alreadyExisted: true, warehouse };
  }
  const profile = splitDisplayName(externalUser.fullName, externalUser.login);
  const placeholder = createPlaceholderPasswordHash();
  const params = [
    profile.surname || externalUser.login,
    profile.name || "",
    ROLE_EMPLOYEE,
    "packer",
    placeholder.hash,
    placeholder.salt,
    null,
    AUTH_SOURCE_EXTERNAL,
    externalUser.id,
    externalUser.employeeId,
    externalUser.login,
    externalUser.fullName,
  ];
  const result = existing.rows?.[0]?.role === ROLE_ARCHIVED
    ? await dbQuery(
        `update users
         set surname = $1, name = $2, role = $3, position = $4,
             password_hash = $5, password_salt = $6, site_code = $7,
             auth_source = $8, external_user_id = $9, external_employee_id = $10,
             external_login = $11, external_full_name = $12, updated_at = now()
         where id = $13
         returning id`,
        [...params, existing.rows[0].id]
      )
    : await dbQuery(
        `insert into users (
           surname, name, role, position, password_hash, password_salt,
           created_at, updated_at, site_code, auth_source, external_user_id,
           external_employee_id, external_login, external_full_name
         )
         values ($1, $2, $3, $4, $5, $6, now(), now(), $7, $8, $9, $10, $11, $12)
         returning id`,
        params
      );
  return { userId: Number(result.rows[0].id), created: true, alreadyExisted: false, warehouse };
}

async function listLocations() {
  await ensureGlobalSettingsSchema();
  const result = await dbQuery(
    `select location_code, label, sort_order, active
     from site_locations
     where active is true
     order by sort_order, label`
  );
  return result.rows.map((row) => ({
    code: row.location_code,
    label: row.label,
    sortOrder: Number(row.sort_order || 100),
    active: row.active === true,
  }));
}

async function listSkladWarehouses() {
  const result = await skladQuery(
    `select id, name, warehouse_type, active, sort_order
     from warehouses_s
     where active is true
     order by coalesce(sort_order, 100000), name, id`
  );
  return (result.rows || []).map((row) => ({
    id: Number(row.id),
    name: row.name || "",
    type: row.warehouse_type || null,
    active: row.active === true,
    sortOrder: row.sort_order == null ? null : Number(row.sort_order),
  }));
}

async function listLocationWarehouseSettings() {
  await ensureGlobalSettingsSchema();
  const result = await dbQuery(
    `select id,
            location_code,
            marketplace,
            store_id,
            marketplace_warehouse_id,
            marketplace_warehouse_name,
            sklad_warehouse_id,
            sklad_warehouse_name,
            active,
            updated_at,
            updated_by
     from site_location_warehouse_settings
     where active is true
     order by location_code, marketplace, store_id nulls first, marketplace_warehouse_name nulls first, id`
  );
  return (result.rows || []).map((row) => ({
    id: Number(row.id),
    locationCode: row.location_code,
    marketplace: row.marketplace,
    storeId: row.store_id || null,
    marketplaceWarehouseId: row.marketplace_warehouse_id || null,
    marketplaceWarehouseName: row.marketplace_warehouse_name || null,
    skladWarehouseId: row.sklad_warehouse_id == null ? null : Number(row.sklad_warehouse_id),
    skladWarehouseName: row.sklad_warehouse_name || null,
    active: row.active === true,
    updatedAt: row.updated_at || null,
    updatedBy: row.updated_by ? Number(row.updated_by) : null,
  }));
}

async function fetchMarketplaceOptions(req) {
  const fetchOne = async (key, targetBase, targetPath) => {
    try {
      const result = await proxyJsonRequestWithMethod("GET", req, targetBase, targetPath);
      return result.ok ? result.data : { error: result.data?.error || `HTTP_${result.status}` };
    } catch (error) {
      return { error: error?.message || "BACKEND_UNAVAILABLE" };
    }
  };
  const [oz, wb] = await Promise.all([
    // no force=1: let the backends serve their 60s warehouse caches
    fetchOne("oz", OZ_BACKEND, "/OZ/api/stores"),
    fetchOne("wb", WB_BACKEND, "/WB/api/stores"),
  ]);
  return { oz, wb };
}

// Slow settings sources (bd2 directory, sklad DB, live WB/Ozon APIs) are cached
// for 60s with a stale fallback and a hard timeout, so opening/saving settings
// never waits 10+ seconds on a flaky network — worst case it serves the last
// good snapshot while a background refresh would catch up on the next call.
const SETTINGS_SOURCE_TTL_MS = 60000;
const settingsSourceCache = new Map(); // key -> { ts, data }
async function cachedSettingsSource(key, fn, timeoutMs = 6000) {
  const cached = settingsSourceCache.get(key);
  if (cached && Date.now() - cached.ts < SETTINGS_SOURCE_TTL_MS) return cached.data;
  try {
    const data = await Promise.race([
      fn(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("SOURCE_TIMEOUT")), timeoutMs).unref?.()),
    ]);
    settingsSourceCache.set(key, { ts: Date.now(), data });
    return data;
  } catch (error) {
    if (cached) return cached.data; // stale beats waiting
    throw error;
  }
}

async function getGlobalSettingsPayload(req) {
  const [locations, users, directory, skladWarehouses, warehouseSettings, marketplaceOptions] = await Promise.all([
    listLocations(),
    listGlobalUsers(),
    cachedSettingsSource("directory", () => listExternalDirectoryUsers()).catch(() => []),
    cachedSettingsSource("sklad", () => listSkladWarehouses()).catch((error) => ({ error: error?.message || "SKLAD_DB_UNAVAILABLE" })),
    listLocationWarehouseSettings(),
    cachedSettingsSource("marketplace", () => fetchMarketplaceOptions(req)).catch(() => ({ oz: null, wb: null })),
  ]);
  return {
    locations,
    users,
    directory,
    skladWarehouses,
    warehouseSettings,
    marketplaceOptions,
  };
}

async function findGlobalUserById(userId) {
  await ensureGlobalSettingsSchema();
  const result = await dbQuery(
    `select u.id,
            u.surname,
            u.name,
            u.role,
            u.position,
            u.created_at,
            u.site_code,
            u.auth_source,
            u.external_user_id,
            u.external_employee_id,
            u.external_login,
            u.external_full_name,
            u.location_code,
            u.location_configured_at,
            u.location_configured_by
     from users u
     where u.id = $1
     limit 1`,
    [Number(userId)]
  );
  return result.rows?.[0] || null;
}

async function createGlobalUser(body, actorUserId) {
  await ensureGlobalSettingsSchema();
  const externalUserId = Number(body?.externalUserId);
  if (!Number.isInteger(externalUserId) || externalUserId < 1) {
    const error = new Error("Выберите сотрудника из общей базы");
    error.statusCode = 400;
    throw error;
  }
  const role = normalizeManagedRole(body?.role, ROLE_EMPLOYEE);
  const locationCode = role === ROLE_EMPLOYEE ? normalizeLocationCode(body?.locationCode) : null;
  const externalUser = await getExternalUserById(externalUserId);
  if (!externalUser?.employeeId) {
    const error = new Error("Сотрудник не активен или у него нет учётной записи для входа");
    error.statusCode = 400;
    throw error;
  }
  const existing = await dbQuery(
    `select id, role
     from users
     where auth_source = $1 and external_user_id = $2
     limit 1`,
    [AUTH_SOURCE_EXTERNAL, externalUser.id]
  );
  if (existing.rows?.[0]?.id && existing.rows[0].role !== ROLE_ARCHIVED) {
    const error = new Error("Этот сотрудник уже добавлен");
    error.statusCode = 409;
    throw error;
  }
  const profile = splitDisplayName(externalUser.fullName, externalUser.login);
  const placeholder = createPlaceholderPasswordHash();
  const params = [
    profile.surname || externalUser.login,
    profile.name || "",
    role,
    role === ROLE_EMPLOYEE ? "packer" : null,
    placeholder.hash,
    placeholder.salt,
    null,
    AUTH_SOURCE_EXTERNAL,
    externalUser.id,
    externalUser.employeeId,
    externalUser.login,
    externalUser.fullName,
    locationCode,
    locationCode ? actorUserId : null,
  ];
  const result = existing.rows?.[0]?.role === ROLE_ARCHIVED
    ? await dbQuery(
        `update users
         set surname = $1,
             name = $2,
             role = $3,
             position = $4,
             password_hash = $5,
             password_salt = $6,
             site_code = $7,
             auth_source = $8,
             external_user_id = $9,
             external_employee_id = $10,
             external_login = $11,
             external_full_name = $12,
             location_code = $13,
             location_configured_at = case when $13::text is null then null else now() end,
             location_configured_by = $14,
             updated_at = now()
         where id = $15
         returning *`,
        [...params, existing.rows[0].id]
      )
    : await dbQuery(
        `insert into users (
           surname, name, role, position, password_hash, password_salt,
           created_at, updated_at, site_code, auth_source, external_user_id,
           external_employee_id, external_login, external_full_name,
           location_code, location_configured_at, location_configured_by
         )
         values ($1, $2, $3, $4, $5, $6, now(), now(), $7, $8, $9, $10, $11, $12,
                 $13, case when $13::text is null then null else now() end, $14)
         returning *`,
        params
      );
  const user = result.rows[0];
  await writeSettingsAudit(actorUserId, "create_user", "user", user.id, null, user);
  return toGlobalUserPayload(user);
}

async function updateGlobalUser(userId, body, actorUserId) {
  await ensureGlobalSettingsSchema();
  const target = await findGlobalUserById(userId);
  if (!target) {
    const error = new Error("Пользователь не найден");
    error.statusCode = 404;
    throw error;
  }
  if (target.role === ROLE_SUPER && Number(target.id) === Number(actorUserId) && body?.role && body.role !== ROLE_SUPER) {
    const error = new Error("Нельзя снять права Superadmin с самого себя");
    error.statusCode = 400;
    throw error;
  }
  const role = target.role === ROLE_SUPER ? ROLE_SUPER : normalizeManagedRole(body?.role ?? target.role, target.role);
  const locationCode = role === ROLE_EMPLOYEE ? normalizeLocationCode(body?.locationCode) : null;
  const result = await dbQuery(
    `update users
     set role = $1,
         position = case when $1 = $2 then coalesce(nullif(position, ''), 'packer') else null end,
         location_code = $3,
         location_configured_at = case
           when $1 = $2 and $3::text is not null then now()
           else null
         end,
         location_configured_by = case
           when $1 = $2 and $3::text is not null then $4::bigint
           else null
         end,
         updated_at = now()
     where id = $5
     returning *`,
    [role, ROLE_EMPLOYEE, locationCode, actorUserId ? Number(actorUserId) : null, Number(userId)]
  );
  const user = result.rows[0];
  await writeSettingsAudit(actorUserId, "update_user", "user", user.id, target, user);
  return toGlobalUserPayload(user);
}

async function archiveGlobalUser(userId, actorUserId) {
  await ensureGlobalSettingsSchema();
  const target = await findGlobalUserById(userId);
  if (!target) {
    const error = new Error("Пользователь не найден");
    error.statusCode = 404;
    throw error;
  }
  if (Number(target.id) === Number(actorUserId)) {
    const error = new Error("Нельзя удалить себя");
    error.statusCode = 400;
    throw error;
  }
  if (target.role === ROLE_SUPER) {
    const error = new Error("Superadmin не удаляется из этого экрана");
    error.statusCode = 400;
    throw error;
  }
  const result = await dbQuery(
    `update users
     set role = $1,
         position = null,
         location_code = null,
         location_configured_at = null,
         location_configured_by = null,
         updated_at = now()
     where id = $2
     returning *`,
    [ROLE_ARCHIVED, Number(userId)]
  );
  await dbQuery("delete from sessions where user_id = $1", [Number(userId)]).catch(() => {});
  const user = result.rows[0];
  await writeSettingsAudit(actorUserId, "archive_user", "user", user.id, target, user);
  return toGlobalUserPayload(user);
}

async function upsertWarehouseSetting(body, actorUserId) {
  await ensureGlobalSettingsSchema();
  const locationCode = normalizeLocationCode(body?.locationCode);
  const marketplace = normalizeText(body?.marketplace || "all").toLowerCase();
  if (!locationCode) {
    const error = new Error("Выберите локацию");
    error.statusCode = 400;
    throw error;
  }
  if (!["all", "wb", "oz"].includes(marketplace)) {
    const error = new Error("Некорректный маркетплейс");
    error.statusCode = 400;
    throw error;
  }
  const skladWarehouseId = Number(body?.skladWarehouseId);
  if (!Number.isInteger(skladWarehouseId) || skladWarehouseId < 1) {
    const error = new Error("Выберите внутренний склад остатков");
    error.statusCode = 400;
    throw error;
  }
  const warehouse = (await listSkladWarehouses()).find((item) => Number(item.id) === skladWarehouseId);
  if (!warehouse) {
    const error = new Error("Внутренний склад не найден или не активен");
    error.statusCode = 400;
    throw error;
  }
  const storeId = toNullableText(body?.storeId);
  const marketplaceWarehouseId = toNullableText(body?.marketplaceWarehouseId);
  const marketplaceWarehouseName = toNullableText(body?.marketplaceWarehouseName);
  const existing = await dbQuery(
    `select *
     from site_location_warehouse_settings
     where location_code = $1
       and marketplace = $2
       and coalesce(store_id, '') = coalesce($3::text, '')
       and coalesce(marketplace_warehouse_id, '') = coalesce($4::text, '')
     limit 1`,
    [locationCode, marketplace, storeId, marketplaceWarehouseId]
  );
  const before = existing.rows?.[0] || null;
  const result = before
    ? await dbQuery(
        `update site_location_warehouse_settings
         set marketplace_warehouse_name = $1,
             sklad_warehouse_id = $2,
             sklad_warehouse_name = $3,
             active = true,
             updated_at = now(),
             updated_by = $4
         where id = $5
         returning *`,
        [marketplaceWarehouseName, warehouse.id, warehouse.name, actorUserId ? Number(actorUserId) : null, before.id]
      )
    : await dbQuery(
        `insert into site_location_warehouse_settings (
           location_code, marketplace, store_id, marketplace_warehouse_id,
           marketplace_warehouse_name, sklad_warehouse_id, sklad_warehouse_name,
           updated_by
         )
         values ($1, $2, $3, $4, $5, $6, $7, $8)
         returning *`,
        [
          locationCode,
          marketplace,
          storeId,
          marketplaceWarehouseId,
          marketplaceWarehouseName,
          warehouse.id,
          warehouse.name,
          actorUserId ? Number(actorUserId) : null,
        ]
      );
  const setting = result.rows[0];
  await writeSettingsAudit(actorUserId, "upsert_warehouse_setting", "warehouse_setting", setting.id, before, setting);
  return (await listLocationWarehouseSettings()).find((item) => Number(item.id) === Number(setting.id));
}

async function archiveWarehouseSetting(settingId, actorUserId) {
  await ensureGlobalSettingsSchema();
  const beforeResult = await dbQuery("select * from site_location_warehouse_settings where id = $1 limit 1", [Number(settingId)]);
  const before = beforeResult.rows?.[0] || null;
  if (!before) {
    const error = new Error("Настройка не найдена");
    error.statusCode = 404;
    throw error;
  }
  const result = await dbQuery(
    `update site_location_warehouse_settings
     set active = false,
         updated_at = now(),
         updated_by = $1
     where id = $2
     returning *`,
    [actorUserId ? Number(actorUserId) : null, Number(settingId)]
  );
  await writeSettingsAudit(actorUserId, "archive_warehouse_setting", "warehouse_setting", settingId, before, result.rows[0]);
  return { ok: true };
}

async function proxyJsonRequestWithMethod(method, req, targetBase, targetPath, body = undefined) {
  const baseUrl = new URL(targetBase);
  const targetUrl = new URL(targetPath.includes("?") ? targetPath : `${targetPath}${getQuerySuffix(req)}`, baseUrl);
  const headers = buildProxyHeaders(req, targetUrl);
  if (body !== undefined) headers["content-type"] = "application/json";
  const response = await fetch(targetUrl, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return {
    ok: response.ok,
    status: response.status,
    data,
    text,
    contentType: response.headers.get("content-type") || "application/json; charset=utf-8",
  };
}

async function proxyJsonRequest(req, targetBase, targetPath, body = undefined) {
  return proxyJsonRequestWithMethod(req.method, req, targetBase, targetPath, body);
}

function sendProxyJsonResult(res, result) {
  res.status(result.status);
  res.setHeader("Content-Type", result.contentType);
  if (typeof result.data === "string") {
    res.send(result.data);
    return;
  }
  res.send(result.data ?? {});
}

function normalizeWbSupplyQrPayload(payload) {
  if (!payload || typeof payload !== "object") return payload;
  const source = payload;
  const summarySource = source.summary && typeof source.summary === "object" ? source.summary : {};
  const boxes = Array.isArray(source.boxes) ? source.boxes : [];
  const summaryCount = Number(summarySource.boxCount || 0);
  const boxCount = Number.isFinite(summaryCount) ? Math.max(summaryCount, boxes.length) : boxes.length;
  const hasBoxes = boxCount > 0;
  return {
    ...source,
    summary: {
      ...summarySource,
      boxCount,
      canCreateBoxes: hasBoxes ? false : summarySource.canCreateBoxes !== false,
      singleBoxMode: true,
    },
    requestedAmount: 1,
    enforcedAmount: 1,
  };
}

function normalizeText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeArticleKey(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  const str = String(value).trim();
  return str ? str : null;
}

function splitDisplayName(fullName, fallbackLogin = "") {
  const source = normalizeText(fullName) || normalizeText(fallbackLogin);
  if (!source) return { surname: "", name: "", fullName: "" };
  const parts = source.split(/\s+/).filter(Boolean);
  if (parts.length === 1) return { surname: parts[0], name: "", fullName: source };
  return { surname: parts[0], name: parts.slice(1).join(" "), fullName: source };
}

function getUserDisplayProfile(row) {
  const login = normalizeText(row?.external_login) || normalizeText(row?.surname);
  const rawFullName =
    normalizeText(row?.external_full_name) ||
    normalizeText([row?.surname, row?.name].filter(Boolean).join(" ")) ||
    login;
  const split = splitDisplayName(rawFullName, login);
  return { login, fullName: split.fullName, surname: split.surname, name: split.name };
}

function getDisplayUserName(row) {
  return (
    normalizeText(row?.fullName) ||
    normalizeText(row?.external_full_name) ||
    normalizeText([row?.surname, row?.name].filter(Boolean).join(" ")) ||
    normalizeText(row?.login) ||
    "—"
  );
}

function toPublicUser(row) {
  const profile = getUserDisplayProfile(row);
  return {
    id: Number(row.id),
    surname: profile.surname,
    name: profile.name,
    fullName: profile.fullName,
    login: profile.login,
    role: row.role,
    position: row.role === "employee" ? normalizeText(row.position).toLowerCase() || "packer" : null,
    locationCode: normalizeLocationCode(row.location_code),
    externalEmployeeId: row.external_employee_id ? Number(row.external_employee_id) : null,
    createdAt: row.created_at,
  };
}

function parseCookies(raw) {
  const out = {};
  if (!raw) return out;
  for (const part of String(raw).split(";")) {
    const idx = part.indexOf("=");
    if (idx < 1) continue;
    out[part.slice(0, idx).trim()] = decodeURIComponent(part.slice(idx + 1).trim());
  }
  return out;
}

function serializeCookie(name, value, options = {}) {
  const parts = [`${name}=${encodeURIComponent(value)}`];
  if (options.maxAge !== undefined) parts.push(`Max-Age=${Math.max(0, Number(options.maxAge) || 0)}`);
  if (options.path) parts.push(`Path=${options.path}`);
  if (options.httpOnly) parts.push("HttpOnly");
  if (options.secure) parts.push("Secure");
  if (options.sameSite) parts.push(`SameSite=${options.sameSite}`);
  return parts.join("; ");
}

function setSessionCookie(res, token, expiresAt) {
  const maxAge = Math.max(1, Math.floor((new Date(expiresAt).getTime() - Date.now()) / 1000));
  res.setHeader(
    "Set-Cookie",
    serializeCookie(COOKIE_NAME, token, {
      maxAge,
      path: COOKIE_PATH,
      httpOnly: true,
      sameSite: "Lax",
      secure: COOKIE_SECURE,
    })
  );
}

function clearSessionCookie(res) {
  res.setHeader(
    "Set-Cookie",
    serializeCookie(COOKIE_NAME, "", {
      maxAge: 0,
      path: COOKIE_PATH,
      httpOnly: true,
      sameSite: "Lax",
      secure: COOKIE_SECURE,
    })
  );
}

const USER_COLS = `id, surname, name, role, position, password_hash, password_salt,
  created_at, site_code, auth_source, location_code, location_configured_at,
  location_configured_by, external_user_id, external_employee_id,
  external_login, external_full_name`;
const AUTH_SESSION_CACHE_MS = Math.max(
  60 * 1000,
  Number(process.env.AUTH_SESSION_CACHE_MS || 5 * 60 * 1000) || 5 * 60 * 1000
);
const AUTH_SESSION_REFRESH_THRESHOLD_MS = Math.min(30 * 60 * 1000, Math.max(2 * 60 * 1000, Math.floor(SESSION_TTL_MS / 4)));
const authSessionCache = new Map();
const SUPER_USER_CACHE_MS = Math.max(60 * 1000, Number(process.env.SUPER_USER_CACHE_MS || 10 * 60 * 1000) || 10 * 60 * 1000);
const localSuperUserCache = new Map();

// Стабильный отпечаток пароля: sha256 от текущего хеша (scrypt-hex для local/legacy,
// bcrypt для external). Если хеш меняется (смена пароля) — отпечаток перестаёт совпадать,
// и все старые сессии становятся недействительными.
function authFingerprint(hash) {
  if (!hash) return null;
  return crypto.createHash("sha256").update(String(hash)).digest("hex");
}

async function createSession(userId, authFp = null) {
  const token = crypto.randomBytes(32).toString("hex");
  const expires = new Date(Date.now() + SESSION_TTL_MS);
  await dbQuery(
    "insert into sessions (token, user_id, expires_at, created_at, updated_at, auth_fp) values ($1, $2, $3, now(), now(), $4)",
    [token, userId, expires, authFp]
  );
  return { token, expires };
}

// Текущий отпечаток пароля пользователя из источника правды + признак активности.
// Возвращает { fingerprint, active }. active=false → пользователь уволен/удалён.
async function getCurrentAuthState(row) {
  try {
    if (row.auth_source === AUTH_SOURCE_EXTERNAL) {
      const login = normalizeText(row.external_login);
      if (!login) return { fingerprint: null, active: true };
      const r = await authQuery(
        `select u.password_hash, e.status
           from users_d u
           join employees_d e on e.id = u.employee_id
          where lower(u.login) = lower($1)
          limit 1`,
        [login]
      );
      const ext = r.rows?.[0];
      if (!ext) return { fingerprint: null, active: false };
      return { fingerprint: authFingerprint(ext.password_hash), active: EMPLOYEE_ACTIVE_STATUSES.includes(ext.status || "") };
    }
    // local / legacy — источник правды в основной БД users
    const r = await dbQuery("select password_hash from users where id = $1 limit 1", [Number(row.user_id)]);
    const loc = r.rows?.[0];
    if (!loc) return { fingerprint: null, active: false };
    return { fingerprint: authFingerprint(loc.password_hash), active: true };
  } catch {
    // при сбое БД авторизации не разлогиниваем (fail-open по доступности), но и не продлеваем
    return { fingerprint: undefined, active: true };
  }
}

async function deleteSession(token) {
  if (!token) return;
  authSessionCache.delete(token);
  await dbQuery("delete from sessions where token = $1", [token]);
}

function getCachedSessionUser(token) {
  if (!token) return null;
  const cached = authSessionCache.get(token);
  if (!cached) return null;
  const now = Date.now();
  if (cached.cacheUntilMs <= now || cached.sessionExpiresAtMs <= now) {
    authSessionCache.delete(token);
    return null;
  }
  return cached.payload;
}

function setCachedSessionUser(token, payload) {
  if (!token || !payload?.user || !payload?.expires) return;
  const expiresAtMs = new Date(payload.expires).getTime();
  if (!Number.isFinite(expiresAtMs)) return;
  const now = Date.now();
  authSessionCache.set(token, {
    payload,
    sessionExpiresAtMs: expiresAtMs,
    cacheUntilMs: Math.min(expiresAtMs, now + AUTH_SESSION_CACHE_MS),
  });
}

async function getSessionUser(token) {
  if (!token) return null;
  const cached = getCachedSessionUser(token);
  if (cached) return cached;
  const result = await dbQuery(
    `select s.token, s.expires_at, s.auth_fp,
       u.id as user_id, u.surname, u.name, u.role, u.position,
       u.site_code, u.auth_source, u.external_user_id,
       u.external_employee_id, u.external_login, u.external_full_name,
       u.location_code, u.location_configured_at, u.location_configured_by
     from sessions s
     join users u on u.id = s.user_id
     where s.token = $1 and s.expires_at > now()`,
    [token]
  );
  const row = result.rows?.[0];
  if (!row) return null;

  // Перепроверка актуальности: уволенный сотрудник или сменённый пароль -> сессия недействительна.
  const state = await getCurrentAuthState(row);
  if (state.active === false) {
    await deleteSession(token);
    return null;
  }
  // Сверяем отпечаток пароля только если он сохранён в сессии и удалось получить текущий.
  if (row.auth_fp && state.fingerprint && row.auth_fp !== state.fingerprint) {
    await deleteSession(token);
    return null;
  }

  const rowExpires = new Date(row.expires_at);
  let effectiveExpires = rowExpires;
  if (rowExpires.getTime() - Date.now() <= AUTH_SESSION_REFRESH_THRESHOLD_MS) {
    const nextExpires = new Date(Date.now() + SESSION_TTL_MS);
    await dbQuery("update sessions set expires_at = $1, updated_at = now() where token = $2", [nextExpires, token]);
    effectiveExpires = nextExpires;
  }
  const payload = {
    user: toPublicUser({ ...row, id: Number(row.user_id) }),
    token,
    expires: effectiveExpires,
  };
  setCachedSessionUser(token, payload);
  return payload;
}

let ensureApplicationLogSchemaPromise = null;

function ensureApplicationLogSchema() {
  if (!ensureApplicationLogSchemaPromise) {
    ensureApplicationLogSchemaPromise = ensureApplicationLogSchemaOnce().catch((error) => {
      ensureApplicationLogSchemaPromise = null;
      throw error;
    });
  }
  return ensureApplicationLogSchemaPromise;
}

async function ensureApplicationLogSchemaOnce() {
  await dbQuery(
    `create table if not exists application_error_logs (
      id bigserial primary key,
      event_id text,
      source text not null,
      severity text not null default 'error',
      marketplace text,
      user_id bigint,
      user_name text,
      user_role text,
      request_method text,
      request_path text,
      http_status integer,
      duration_ms integer,
      message text,
      stack text,
      context jsonb not null default '{}'::jsonb,
      user_agent text,
      client_ip text,
      client_created_at timestamptz,
      created_at timestamptz not null default now()
    )`
  );
  await dbQuery(
    "create index if not exists idx_application_error_logs_created on application_error_logs(created_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_application_error_logs_user_created on application_error_logs(user_id, created_at desc)",
    []
  );
  await dbQuery(
    "create index if not exists idx_application_error_logs_event on application_error_logs(event_id) where event_id is not null",
    []
  );
  await dbQuery(
    `delete from application_error_logs
     where created_at < now() - ($1::text || ' days')::interval`,
    [APPLICATION_LOG_RETENTION_DAYS]
  );
}

function limitApplicationLogText(value, maxLength) {
  const text = String(value ?? "").trim();
  return text ? text.slice(0, maxLength) : null;
}

function sanitizeApplicationLogContext(value) {
  const seen = new WeakSet();
  let json = "{}";
  try {
    json = JSON.stringify(value && typeof value === "object" ? value : {}, (key, child) => {
      if (/password|authorization|cookie|session|secret|token|template/i.test(String(key || ""))) {
        return "[REDACTED]";
      }
      if (typeof child === "string") return child.slice(0, 4000);
      if (child && typeof child === "object") {
        if (seen.has(child)) return "[CIRCULAR]";
        seen.add(child);
      }
      return child;
    });
  } catch {
    json = JSON.stringify({ serializationError: true });
  }
  if (json.length > APPLICATION_LOG_MAX_CONTEXT) {
    json = JSON.stringify({ truncated: true, preview: json.slice(0, APPLICATION_LOG_MAX_CONTEXT) });
  }
  try {
    return JSON.parse(json);
  } catch {
    return { serializationError: true };
  }
}

function marketplaceFromRequestPath(requestPath) {
  const pathValue = String(requestPath || "").toLowerCase();
  if (/\/(?:api\/)?oz(?:\/|$)/.test(pathValue)) return "oz";
  if (/\/(?:api\/)?wb(?:\/|$)/.test(pathValue)) return "wb";
  if (/\/(?:api\/)?ym(?:\/|$)/.test(pathValue)) return "ym";
  return null;
}

function normalizeApplicationEventId(value) {
  const eventId = String(value || "").trim();
  if (/^[a-zA-Z0-9._:-]{6,120}$/.test(eventId)) return eventId;
  return crypto.randomUUID();
}

async function recordApplicationLog(entry = {}) {
  await ensureApplicationLogSchema();
  const context = sanitizeApplicationLogContext(entry.context);
  await dbQuery(
    `insert into application_error_logs (
       event_id, source, severity, marketplace,
       user_id, user_name, user_role,
       request_method, request_path, http_status, duration_ms,
       message, stack, context, user_agent, client_ip, client_created_at, created_at
     ) values (
       $1, $2, $3, $4,
       $5, $6, $7,
       $8, $9, $10, $11,
       $12, $13, $14::jsonb, $15, $16, $17, now()
     )`,
    [
      normalizeApplicationEventId(entry.eventId),
      limitApplicationLogText(entry.source, 80) || "unknown",
      limitApplicationLogText(entry.severity, 30) || "error",
      limitApplicationLogText(entry.marketplace, 20),
      Number(entry.userId || 0) > 0 ? Number(entry.userId) : null,
      limitApplicationLogText(entry.userName, 300),
      limitApplicationLogText(entry.userRole, 50),
      limitApplicationLogText(entry.requestMethod, 20),
      limitApplicationLogText(entry.requestPath, 2000),
      Number.isFinite(Number(entry.httpStatus)) ? Number(entry.httpStatus) : null,
      Number.isFinite(Number(entry.durationMs)) ? Math.max(0, Math.round(Number(entry.durationMs))) : null,
      limitApplicationLogText(entry.message, APPLICATION_LOG_MAX_MESSAGE),
      limitApplicationLogText(entry.stack, APPLICATION_LOG_MAX_STACK),
      JSON.stringify(context),
      limitApplicationLogText(entry.userAgent, 1000),
      limitApplicationLogText(entry.clientIp, 120),
      entry.clientCreatedAt && !Number.isNaN(new Date(entry.clientCreatedAt).getTime())
        ? new Date(entry.clientCreatedAt)
        : null,
    ]
  );
}

async function findLocalSuperUsersByLogin(login) {
  const normalized = normalizeText(login);
  if (!normalized) return [];
  const cached = localSuperUserCache.get(normalized);
  if (cached && cached.expiresAtMs > Date.now()) {
    return cached.rows;
  }
  const result = await dbQuery(
    `select ${USER_COLS}
     from users
     where role = $1 and coalesce(auth_source, $2) = $2 and lower(surname) = lower($3)
     order by id`,
    [ROLE_SUPER, AUTH_SOURCE_LOCAL, normalized]
  );
  const rows = result.rows || [];
  localSuperUserCache.set(normalized, {
    rows,
    expiresAtMs: Date.now() + SUPER_USER_CACHE_MS,
  });
  return rows;
}

async function findLocalLoginUsersByLogin(login) {
  const normalized = normalizeText(login);
  if (!normalized) return [];
  const result = await dbQuery(
    `select ${USER_COLS}
     from users
     where role = any($1::text[])
       and coalesce(auth_source, $2) = $2
       and (lower(surname) = lower($3) or lower(coalesce(external_login, '')) = lower($3))
     order by id`,
    [[ROLE_SUPER, ROLE_ADMIN, ROLE_EMPLOYEE], AUTH_SOURCE_LOCAL, normalized]
  );
  return result.rows || [];
}

async function findSiteUserByExternalLogin(login) {
  const normalized = normalizeText(login);
  if (!normalized) return null;
  const result = await dbQuery(
    `select ${USER_COLS}
     from users
     where auth_source = $1 and lower(external_login) = lower($2)
     order by id desc
     limit 1`,
    [AUTH_SOURCE_EXTERNAL, normalized]
  );
  return result.rows?.[0] || null;
}

function verifyPassword(password, salt, hash) {
  const derived = crypto.scryptSync(password, salt, 64);
  const stored = Buffer.from(hash, "hex");
  if (derived.length !== stored.length) return false;
  return crypto.timingSafeEqual(derived, stored);
}

function isExternalAuthConfigured() {
  return Boolean(AUTH_DB_HOST && AUTH_DB_USER && AUTH_DB_NAME);
}

async function verifyExternalUserPassword(login, password) {
  const normalized = normalizeText(login);
  if (!normalized || typeof password !== "string" || !password) return null;
  const result = await authQuery(
    `select u.id, u.login, u.full_name, u.password_hash,
            u.employee_id, e.full_name as employee_full_name, e.status
     from users_d u
     join employees_d e on e.id = u.employee_id
     where lower(u.login) = lower($1) and coalesce(e.status, '') = 'active'
     limit 1`,
    [normalized]
  );
  const row = result.rows?.[0];
  if (!row?.password_hash) return null;
  const ok = await bcrypt.compare(password, row.password_hash);
  if (!ok) return null;
  const fullName = normalizeText(row.full_name) || normalizeText(row.employee_full_name) || normalizeText(row.login);
  return {
    id: Number(row.id),
    login: normalizeText(row.login),
    fullName,
    employeeId: Number(row.employee_id || 0) || null,
    passwordHash: row.password_hash,
  };
}

async function syncLocalExternalUser(localUserId, externalUser) {
  if (!localUserId || !externalUser) {
    const result = await dbQuery(`select ${USER_COLS} from users where id = $1`, [localUserId]);
    return result.rows?.[0] || null;
  }
  const profile = splitDisplayName(externalUser.fullName, externalUser.login);
  const result = await dbQuery(
    `update users
     set surname = $1,
         name = $2,
         external_login = $3,
         external_full_name = $4,
         external_employee_id = $5,
         updated_at = now()
     where id = $6
     returning ${USER_COLS}`,
    [profile.surname || externalUser.login, profile.name || "", externalUser.login, externalUser.fullName, externalUser.employeeId, localUserId]
  );
  return result.rows?.[0] || null;
}

const app = express();
app.disable("x-powered-by");
app.use(
  compression({
    filter: (req, res) => {
      // SSE-потоки нельзя сжимать: gzip буферизует события и они не доходят до клиента
      if (String(req.path || "").endsWith("/events")) return false;
      return compression.filter(req, res);
    },
  })
);

// Lightweight request logger: record slow (>2s) or failed (>=400) requests to
// stdout (captured into logs/app.log). Normal fast traffic stays quiet so the
// log is readable. This is the first place to look when the UI feels slow or
// shows an error like REQUEST_TIMEOUT.
app.use((req, res, next) => {
  const startedAt = Date.now();
  const requestId = normalizeApplicationEventId(req.headers["x-client-request-id"]);
  req.requestId = requestId;
  res.setHeader("X-Request-Id", requestId);
  res.on("finish", () => {
    const ms = Date.now() - startedAt;
    if (res.statusCode >= 400 || ms >= 2000) {
      const user = req.sessionUser || null;
      const userLabel = user ? `${user.id}:${user.login || user.fullName || user.surname || user.role}` : "anonymous";
      console.warn(
        `[req:${requestId}] ${req.method} ${req.originalUrl} -> ${res.statusCode} ${ms}ms user=${userLabel}`
      );
      void recordApplicationLog({
        eventId: requestId,
        source: "server_request",
        severity: res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warning" : "slow",
        marketplace: marketplaceFromRequestPath(req.originalUrl),
        userId: user?.id || null,
        userName: user?.fullName || [user?.surname, user?.name].filter(Boolean).join(" ") || user?.login || null,
        userRole: user?.role || null,
        requestMethod: req.method,
        requestPath: req.originalUrl,
        httpStatus: res.statusCode,
        durationMs: ms,
        message:
          res.statusCode >= 500
            ? `Server request failed with HTTP ${res.statusCode}`
            : res.statusCode >= 400
              ? `Request rejected with HTTP ${res.statusCode}`
              : `Slow request: ${ms}ms`,
        context: { contentType: res.getHeader("content-type") || null },
        userAgent: req.headers["user-agent"] || null,
        clientIp: req.headers["x-forwarded-for"] || req.socket?.remoteAddress || null,
      }).catch((error) => {
        console.error(`[req:${requestId}] failed to persist request log:`, error?.message || error);
      });
    }
  });
  next();
});

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

const authRouter = express.Router();
authRouter.use(express.json());

authRouter.post(
  "/login",
  asyncHandler(async (req, res) => {
    const login = normalizeText(req.body?.login ?? req.body?.surname);
    const password = typeof req.body?.password === "string" ? req.body.password : "";
    if (!login || !password) {
      return res.status(400).json({ error: "Введите логин и пароль" });
    }

    const localUsers = await findLocalLoginUsersByLogin(login);
    const matchedLocal = localUsers.find((row) => verifyPassword(password, row.password_salt, row.password_hash));
    if (matchedLocal) {
      const session = await createSession(matchedLocal.id, authFingerprint(matchedLocal.password_hash));
      setCachedSessionUser(session.token, {
        user: toPublicUser(matchedLocal),
        token: session.token,
        expires: session.expires,
      });
      setSessionCookie(res, session.token, session.expires);
      return res.json({ user: toPublicUser(matchedLocal) });
    }

    if (!isExternalAuthConfigured()) {
      return res.status(500).json({ error: "Внешняя база авторизации не настроена" });
    }

    let siteUser = await findSiteUserByExternalLogin(login);
    if (!siteUser) {
      const externalUser = await verifyExternalUserPassword(login, password);
      if (externalUser) {
        const fallback = await dbQuery(
          `select *
           from users
           where auth_source = $1 and external_user_id = $2
           order by id desc
           limit 1`,
          [AUTH_SOURCE_EXTERNAL, externalUser.id]
        );
        if (fallback.rows?.[0]) {
          await dbQuery("update users set external_login = $1 where id = $2", [login, fallback.rows[0].id]);
          siteUser = { ...fallback.rows[0], external_login: login };
          const syncedUser = await syncLocalExternalUser(siteUser.id, externalUser);
          const session = await createSession(syncedUser?.id || siteUser.id, authFingerprint(externalUser.passwordHash));
          setCachedSessionUser(session.token, {
            user: toPublicUser(syncedUser || siteUser),
            token: session.token,
            expires: session.expires,
          });
          setSessionCookie(res, session.token, session.expires);
          return res.json({ user: toPublicUser(syncedUser || siteUser) });
        }
      }
      return res.status(403).json({ error: "Пользователь не добавлен" });
    }

    const externalUser = await verifyExternalUserPassword(login, password);
    if (!externalUser) {
      return res.status(401).json({ error: "Неверные данные для входа" });
    }

    const syncedUser = await syncLocalExternalUser(siteUser.id, externalUser);
    const session = await createSession(syncedUser?.id || siteUser.id, authFingerprint(externalUser.passwordHash));
    setCachedSessionUser(session.token, {
      user: toPublicUser(syncedUser || siteUser),
      token: session.token,
      expires: session.expires,
    });
    setSessionCookie(res, session.token, session.expires);
    return res.json({ user: toPublicUser(syncedUser || siteUser) });
  })
);

authRouter.get(
  "/me",
  asyncHandler(async (req, res) => {
    const cookies = parseCookies(req.headers.cookie || "");
    const auth = await getSessionUser(cookies[COOKIE_NAME]);
    if (auth) setSessionCookie(res, auth.token, auth.expires);
    return res.json({ user: auth?.user || null });
  })
);

authRouter.post(
  "/logout",
  asyncHandler(async (req, res) => {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = cookies[COOKIE_NAME];
    if (token) await deleteSession(token);
    clearSessionCookie(res);
    return res.json({ ok: true });
  })
);

app.use(`${API_ROOT}/auth`, authRouter);
app.use(`${OZ_API_ROOT}/auth`, authRouter);
app.use(`${WB_API_ROOT}/auth`, authRouter);

// ================= Desktop-приложение (kiosk-обёртка): вход по отпечатку =================
const DESKTOP_SECRET = process.env.DESKTOP_SECRET || "grasklad-desktop-2026-secret";

let ensureDesktopSchemaPromise = null;
function ensureDesktopSchema() {
  if (!ensureDesktopSchemaPromise) {
    ensureDesktopSchemaPromise = (async () => {
      await dbQuery(
        `create table if not exists desktop_fingerprints (
           user_id bigint primary key,
           finger_id integer unique,
           template_b64 text,
           enabled boolean not null default true,
           created_at timestamptz not null default now(),
           updated_at timestamptz not null default now()
         )`
      );
    })().catch((e) => {
      ensureDesktopSchemaPromise = null;
      throw e;
    });
  }
  return ensureDesktopSchemaPromise;
}

const desktopRouter = express.Router();
desktopRouter.use(express.json({ limit: "1mb" }));
// защита секретом (совпадает с приложением: env DESKTOP_SECRET)
desktopRouter.use((req, res, next) => {
  const secret = req.headers["x-desktop-secret"] || "";
  if (secret !== DESKTOP_SECRET) return res.status(403).json({ error: "forbidden" });
  next();
});

// Проверка суперадмина для входа в админку оболочки (сессию не создаём)
desktopRouter.post(
  "/admin-auth",
  asyncHandler(async (req, res) => {
    const login = normalizeText(req.body?.login);
    const password = typeof req.body?.password === "string" ? req.body.password : "";
    if (!login || !password) return res.status(400).json({ error: "Введите логин и пароль" });
    const superUsers = await findLocalSuperUsersByLogin(login);
    const matched = superUsers.find((row) => verifyPassword(password, row.password_salt, row.password_hash));
    if (!matched) return res.status(401).json({ error: "Неверные данные или не суперадмин" });
    return res.json({ ok: true, role: "super_admin", userId: Number(matched.id) });
  })
);

// Список сотрудников + статус отпечатка
desktopRouter.get(
  "/roster",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const result = await dbQuery(
      `select ${USER_COLS.split(",").map((c) => "u." + c.trim()).join(", ")},
              f.finger_id, f.template_b64, f.enabled as fp_enabled
       from users u
       left join desktop_fingerprints f on f.user_id = u.id
       where u.role = 'employee'
       order by lower(u.surname), lower(u.name)`
    );
    const employees = (result.rows || []).map((row) => {
      const pub = toPublicUser(row);
      return {
        userId: pub.id,
        name: pub.name,
        surname: pub.surname,
        locationCode: pub.locationCode,
        fingerId: row.finger_id != null ? Number(row.finger_id) : null,
        templateB64: row.template_b64 || null,
        fpEnabled: row.fp_enabled === true && row.finger_id != null,
      };
    });
    return res.json({ employees });
  })
);

// Каталог общей базы (активные + стажёры) для добавления в отпечатки.
// q — необязательный поиск по ФИО/логину.
desktopRouter.get(
  "/directory",
  asyncHandler(async (req, res) => {
    const q = normalizeText(req.query?.q || "").toLowerCase();
    let list = await listDesktopDirectory();
    if (q) {
      list = list.filter((x) => `${x.fullName} ${x.login}`.toLowerCase().includes(q));
    }
    return res.json({ employees: list });
  })
);

// Добавить сотрудника из общей базы в локальную (чтобы завести отпечаток).
desktopRouter.post(
  "/add-employee",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const externalUserId = Number(req.body?.externalUserId);
    if (!Number.isInteger(externalUserId) || externalUserId < 1) {
      return res.status(400).json({ error: "Выберите сотрудника из общей базы" });
    }
    const out = await provisionDesktopEmployee(externalUserId);
    return res.json({ ok: true, ...out });
  })
);

// Следующий свободный ID пальца в модуле (1..100)
desktopRouter.get(
  "/next-finger-id",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const r = await dbQuery("select finger_id from desktop_fingerprints where finger_id is not null order by finger_id");
    const used = new Set((r.rows || []).map((x) => Number(x.finger_id)));
    let id = 1;
    while (id <= 100 && used.has(id)) id++;
    if (id > 100) return res.status(409).json({ error: "Достигнут лимит отпечатков (100)" });
    return res.json({ fingerId: id });
  })
);

// Сохранить/обновить отпечаток сотрудника
desktopRouter.post(
  "/enroll",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const userId = Number(req.body?.userId);
    const fingerId = Number(req.body?.fingerId);
    const templateB64 = typeof req.body?.templateB64 === "string" ? req.body.templateB64 : null;
    if (!Number.isFinite(userId) || userId < 1) return res.status(400).json({ error: "userId" });
    if (!Number.isFinite(fingerId) || fingerId < 1 || fingerId > 100) return res.status(400).json({ error: "fingerId" });
    await dbQuery(
      `insert into desktop_fingerprints (user_id, finger_id, template_b64, enabled, updated_at)
       values ($1, $2, $3, true, now())
       on conflict (user_id) do update set
         finger_id = excluded.finger_id,
         template_b64 = coalesce(excluded.template_b64, desktop_fingerprints.template_b64),
         enabled = true,
         updated_at = now()`,
      [userId, fingerId, templateB64]
    );
    return res.json({ ok: true, fingerId });
  })
);

// Удалить отпечаток сотрудника
desktopRouter.post(
  "/delete-fingerprint",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const userId = Number(req.body?.userId);
    if (!Number.isFinite(userId) || userId < 1) return res.status(400).json({ error: "userId" });
    await dbQuery("delete from desktop_fingerprints where user_id = $1", [userId]);
    return res.json({ ok: true });
  })
);

// Включить/выключить вход по отпечатку
desktopRouter.post(
  "/set-fp-enabled",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const userId = Number(req.body?.userId);
    const enabled = req.body?.enabled === true;
    if (!Number.isFinite(userId) || userId < 1) return res.status(400).json({ error: "userId" });
    await dbQuery(
      `insert into desktop_fingerprints (user_id, enabled, updated_at)
       values ($1, $2, now())
       on conflict (user_id) do update set enabled = excluded.enabled, updated_at = now()`,
      [userId, enabled]
    );
    return res.json({ ok: true });
  })
);

// Вход по отпечатку: создать сессию сайта для сотрудника, вернуть cookie (приложение проставит в webview)
desktopRouter.post(
  "/login",
  asyncHandler(async (req, res) => {
    await ensureDesktopSchema();
    const userId = Number(req.body?.userId);
    if (!Number.isFinite(userId) || userId < 1) return res.status(400).json({ error: "userId" });
    // сотрудник должен иметь включённый вход по отпечатку
    const fp = await dbQuery("select enabled, finger_id from desktop_fingerprints where user_id = $1", [userId]);
    if (!fp.rows?.[0] || fp.rows[0].enabled !== true || fp.rows[0].finger_id == null) {
      return res.status(403).json({ error: "Вход по отпечатку не настроен" });
    }
    const ures = await dbQuery(`select ${USER_COLS} from users where id = $1 and role = 'employee' limit 1`, [userId]);
    const row = ures.rows?.[0];
    if (!row) return res.status(404).json({ error: "Сотрудник не найден" });
    const authState = await getCurrentAuthState(row);
    if (authState.active === false) return res.status(403).json({ error: "Сотрудник неактивен" });
    const session = await createSession(userId, authState.fingerprint || null);
    setCachedSessionUser(session.token, { user: toPublicUser(row), token: session.token, expires: session.expires });
    return res.json({
      ok: true,
      user: toPublicUser(row),
      cookies: [{ name: COOKIE_NAME, value: session.token, path: COOKIE_PATH, httpOnly: true }],
    });
  })
);

app.use(`${API_ROOT}/desktop`, desktopRouter);

async function requireProxyAuth(req, res, next) {
  try {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = cookies[COOKIE_NAME];
    if (!token) return res.status(401).json({ error: "AUTH_REQUIRED" });
    const auth = await getSessionUser(token);
    if (!auth) return res.status(401).json({ error: "AUTH_REQUIRED" });
    setSessionCookie(res, auth.token, auth.expires);
    req.sessionToken = auth.token;
    req.sessionUser = auth.user;
    next();
  } catch (error) {
    next(error);
  }
}

async function getOptionalRequestSession(req) {
  if (req.sessionUser) return { user: req.sessionUser };
  const cookies = parseCookies(req.headers.cookie || "");
  const token = cookies[COOKIE_NAME];
  if (!token) return null;
  try {
    return await getSessionUser(token);
  } catch {
    return null;
  }
}

app.post(
  `${API_ROOT}/client-errors`,
  express.json({ limit: "64kb" }),
  asyncHandler(async (req, res) => {
    const session = await getOptionalRequestSession(req);
    const user = session?.user || null;
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const request = body.request && typeof body.request === "object" ? body.request : {};
    const eventId = normalizeApplicationEventId(body.eventId || req.requestId);
    await recordApplicationLog({
      eventId,
      source: body.source || "client",
      severity: body.severity || "error",
      marketplace: body.marketplace || marketplaceFromRequestPath(request.url || req.headers.referer || ""),
      userId: user?.id || null,
      userName: user?.fullName || [user?.surname, user?.name].filter(Boolean).join(" ") || user?.login || null,
      userRole: user?.role || null,
      requestMethod: request.method || null,
      requestPath: request.url || req.headers.referer || null,
      httpStatus: request.status,
      durationMs: request.durationMs,
      message: body.message || "Client error",
      stack: body.stack || null,
      context: {
        ...(body.context && typeof body.context === "object" ? body.context : {}),
        requestBody: request.body ?? null,
        responseBody: request.responseBody ?? null,
        pageUrl: body.pageUrl || null,
        viewport: body.viewport || null,
        online: body.online,
      },
      userAgent: req.headers["user-agent"] || null,
      clientIp: req.headers["x-forwarded-for"] || req.socket?.remoteAddress || null,
      clientCreatedAt: body.clientCreatedAt || null,
    });
    res.status(202).json({ ok: true, eventId });
  })
);

app.get(
  `${API_ROOT}/client-errors`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (![ROLE_ADMIN, ROLE_SUPER].includes(req.sessionUser?.role)) {
      return res.status(403).json({ error: "FORBIDDEN" });
    }
    const params = [];
    const where = [];
    const marketplace = normalizeText(req.query?.marketplace).toLowerCase();
    const source = normalizeText(req.query?.source);
    const eventId = normalizeText(req.query?.eventId);
    const userId = Number(req.query?.userId || 0);
    if (marketplace) {
      params.push(marketplace);
      where.push(`marketplace = $${params.length}`);
    }
    if (source) {
      params.push(source);
      where.push(`source = $${params.length}`);
    }
    if (eventId) {
      params.push(eventId);
      where.push(`event_id = $${params.length}`);
    }
    if (Number.isInteger(userId) && userId > 0) {
      params.push(userId);
      where.push(`user_id = $${params.length}`);
    }
    const limit = Math.min(500, Math.max(1, Number(req.query?.limit || 100) || 100));
    params.push(limit);
    const result = await dbQuery(
      `select id, event_id, source, severity, marketplace,
              user_id, user_name, user_role,
              request_method, request_path, http_status, duration_ms,
              message, stack, context, user_agent, client_ip,
              client_created_at, created_at
       from application_error_logs
       ${where.length ? `where ${where.join(" and ")}` : ""}
       order by created_at desc, id desc
       limit $${params.length}`,
      params
    );
    res.json({ logs: result.rows || [] });
  })
);

app.get(
  `${API_ROOT}/reports/hourly`,
  asyncHandler(async (req, res) => {
    const timezone = "Europe/Moscow";
    const requestedHours = Number.parseInt(String(req.query?.hours || "24"), 10);
    const hours = Math.min(168, Math.max(1, Number.isFinite(requestedHours) ? requestedHours : 24));
    const result = await dbQuery(
      `with bounds as (
         select (
           date_trunc('hour', timezone($1, now()))
           - make_interval(hours => greatest($2::int - 1, 0))
         ) at time zone $1 as from_at
       ),
       oz_item_totals as (
         select assembly_id, posting_number,
                coalesce(sum(greatest(coalesce(quantity, 0), 0)), 0)::int as items
         from ozon_assembly_order_items
         group by assembly_id, posting_number
       ),
       oz_ranked as (
         select o.collected_at as completed_at,
                'oz'::text as marketplace,
                coalesce(nullif(o.store_id, ''), nullif(a.store_id, ''), 'ozon') as store_id,
                coalesce(nullif(o.store_name, ''), nullif(a.store_name, ''), 'Ozon') as store_name,
                o.posting_number::text as order_key,
                greatest(coalesce(i.items, 0), 1)::int as items,
                row_number() over (
                  partition by coalesce(nullif(o.store_id, ''), nullif(a.store_id, ''), 'ozon'), o.posting_number
                  order by o.collected_at desc, o.assembly_id desc
                ) as row_num
         from ozon_assembly_orders o
         join ozon_assemblies a on a.id = o.assembly_id
         left join oz_item_totals i on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
         cross join bounds b
         where o.collected_at is not null and o.collected_at >= b.from_at
       ),
       wb_ranked as (
         select o.collected_at as completed_at,
                'wb'::text as marketplace,
                coalesce(nullif(s.store_id, ''), 'wb') as store_id,
                coalesce(nullif(s.store_name, ''), 'Wildberries') as store_name,
                o.wb_order_id::text as order_key,
                greatest(coalesce(o.quantity, 0), 1)::int as items,
                row_number() over (
                  partition by coalesce(nullif(s.store_id, ''), 'wb'), o.wb_order_id
                  order by o.collected_at desc, o.supply_id desc
                ) as row_num
         from supply_orders o
         join supply_settings s on s.supply_id = o.supply_id
         cross join bounds b
         where o.collected_at is not null and o.collected_at >= b.from_at
       ),
       ym_item_totals as (
         select order_id,
                coalesce(sum(greatest(coalesce(quantity, 0), 0)), 0)::int as items
         from ym_order_items
         group by order_id
       ),
       facts as (
         select completed_at, marketplace, store_id, store_name, order_key, items
         from oz_ranked where row_num = 1
         union all
         select completed_at, marketplace, store_id, store_name, order_key, items
         from wb_ranked where row_num = 1
         union all
         select o.ready_to_ship_at as completed_at,
                'ym'::text as marketplace,
                a.campaign_id::text as store_id,
                case when a.campaign_id = 148851177 then 'GraphLab 2'
                     else 'Кабинет ' || a.campaign_id::text end as store_name,
                o.order_id::text as order_key,
                greatest(coalesce(i.items, 0), 1)::int as items
         from ym_orders o
         join ym_assemblies a on a.id = o.assembly_id
         left join ym_item_totals i on i.order_id = o.order_id
         cross join bounds b
         where o.ready_to_ship_at is not null and o.ready_to_ship_at >= b.from_at
       )
       select marketplace, store_id, store_name,
              to_char(date_trunc('hour', timezone($1, completed_at)), 'YYYY-MM-DD"T"HH24:00:00') as hour,
              count(*)::int as orders,
              coalesce(sum(items), 0)::int as items,
              count(*) filter (where completed_at >= now() - interval '1 hour')::int as last_60_orders,
              coalesce(sum(items) filter (where completed_at >= now() - interval '1 hour'), 0)::int as last_60_items
       from facts
       group by marketplace, store_id, store_name, date_trunc('hour', timezone($1, completed_at))
       order by hour desc, marketplace, store_name`,
      [timezone, hours]
    );

    const rows = (result.rows || []).map((row) => ({
      marketplace: row.marketplace,
      storeId: row.store_id,
      storeName: row.store_name,
      hour: row.hour,
      orders: Number(row.orders || 0),
      items: Number(row.items || 0),
      last60Orders: Number(row.last_60_orders || 0),
      last60Items: Number(row.last_60_items || 0),
    }));
    const marketplaceKeys = ["oz", "wb", "ym"];
    const emptyBreakdown = () => Object.fromEntries(
      marketplaceKeys.map((key) => [key, { orders: 0, items: 0 }])
    );
    const hourBuckets = new Map();
    const currentLocalHour = new Date(new Date().toLocaleString("en-US", { timeZone: timezone }));
    currentLocalHour.setMinutes(0, 0, 0);
    for (let offset = 0; offset < hours; offset += 1) {
      const localHour = new Date(currentLocalHour.getTime() - offset * 60 * 60 * 1000);
      const key = [
        localHour.getFullYear(),
        String(localHour.getMonth() + 1).padStart(2, "0"),
        String(localHour.getDate()).padStart(2, "0"),
      ].join("-") + `T${String(localHour.getHours()).padStart(2, "0")}:00:00`;
      hourBuckets.set(key, { hour: key, orders: 0, items: 0, marketplaces: emptyBreakdown() });
    }

    const totals = { orders: 0, items: 0, marketplaces: emptyBreakdown() };
    const last60 = { orders: 0, items: 0, marketplaces: emptyBreakdown() };
    const stores = new Map();
    for (const row of rows) {
      const bucket = hourBuckets.get(row.hour);
      if (bucket && bucket.marketplaces[row.marketplace]) {
        bucket.orders += row.orders;
        bucket.items += row.items;
        bucket.marketplaces[row.marketplace].orders += row.orders;
        bucket.marketplaces[row.marketplace].items += row.items;
      }
      if (totals.marketplaces[row.marketplace]) {
        totals.orders += row.orders;
        totals.items += row.items;
        totals.marketplaces[row.marketplace].orders += row.orders;
        totals.marketplaces[row.marketplace].items += row.items;
        last60.orders += row.last60Orders;
        last60.items += row.last60Items;
        last60.marketplaces[row.marketplace].orders += row.last60Orders;
        last60.marketplaces[row.marketplace].items += row.last60Items;
      }
      const storeKey = `${row.marketplace}:${row.storeId}`;
      const store = stores.get(storeKey) || {
        marketplace: row.marketplace,
        storeId: row.storeId,
        storeName: row.storeName,
        orders: 0,
        items: 0,
        last60Orders: 0,
        last60Items: 0,
      };
      store.orders += row.orders;
      store.items += row.items;
      store.last60Orders += row.last60Orders;
      store.last60Items += row.last60Items;
      stores.set(storeKey, store);
    }

    res.json({
      timezone,
      hoursRequested: hours,
      generatedAt: new Date().toISOString(),
      hours: Array.from(hourBuckets.values()),
      totals,
      last60,
      stores: Array.from(stores.values()).sort((left, right) =>
        left.marketplace.localeCompare(right.marketplace) || left.storeName.localeCompare(right.storeName, "ru")
      ),
    });
  })
);

function getQuerySuffix(req) {
  const idx = req.url.indexOf("?");
  return idx === -1 ? "" : req.url.slice(idx);
}

function buildProxyHeaders(req, targetUrl) {
  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    if (key === "cookie" || key === "host" || key === "connection" || key === "content-length") continue;
    headers[key] = value;
  }
  headers.cookie = `${COOKIE_NAME}=${req.sessionToken}`;
  headers.host = targetUrl.host;
  headers["accept-encoding"] = "identity";
  if (req.sessionUser) {
    headers["x-internal-proxy-token"] = INTERNAL_PROXY_TOKEN;
    headers["x-internal-user"] = Buffer.from(JSON.stringify(req.sessionUser), "utf8").toString("base64url");
  }
  return headers;
}

const URL_REWRITE_RULES = [
  [/https?:\/\/[^/]+\/OZ\/api(?=\/|$)/gi, OZ_API_ROOT],
  [/https?:\/\/[^/]+\/WB\/api(?=\/|$)/gi, WB_API_ROOT],
  [/https?:\/\/[^/]+\/OZ(?=\/(?!files(?:\/|$))|$)/gi, OZ_BASE],
  [/https?:\/\/[^/]+\/WB(?=\/(?!files(?:\/|$))|$)/gi, WB_BASE],
  [/\/OZ\/api(?=\/|$)/g, OZ_API_ROOT],
  [/\/WB\/api(?=\/|$)/g, WB_API_ROOT],
  [/\/OZ(?=\/(?!files(?:\/|$))|$)/g, OZ_BASE],
  [/\/WB(?=\/(?!files(?:\/|$))|$)/g, WB_BASE],
];

function rewriteProxyString(value) {
  if (typeof value !== "string" || !value) return value;
  let next = value;
  for (const [pattern, replacement] of URL_REWRITE_RULES) {
    next = next.replace(pattern, replacement);
  }
  return next;
}

function rewriteProxyPayload(value) {
  if (typeof value === "string") return rewriteProxyString(value);
  if (Array.isArray(value)) return value.map(rewriteProxyPayload);
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [key, child] of Object.entries(value)) {
    out[key] = rewriteProxyPayload(child);
  }
  return out;
}

function getStockMarketplaceMeta(marketplace) {
  const key = String(marketplace || "").trim().toLowerCase() === "wb" ? "wb" : "oz";
  if (key === "wb") {
    return {
      key,
      pickSource: "wb_pick_scan",
      collectSource: "wb_collect",
      label: "WB",
      marketplaceLabel: "Wildberries",
      entityType: "supply",
      entityLabel: "Поставка",
    };
  }
  return {
    key: "oz",
    pickSource: "oz_pick_scan",
    collectSource: "oz_collect",
    label: "OZON",
    marketplaceLabel: "Ozon",
    entityType: "assembly",
    entityLabel: "Сборка",
  };
}

function formatEntityDisplay(entityLabel, entityId) {
  const id = normalizeText(entityId);
  if (!id) return "";
  return `${entityLabel} ${id}`;
}

function parseStockProblemContext(source, notes = "") {
  const sourceText = normalizeText(source).toLowerCase();
  const marketplace = sourceText.startsWith("wb_") ? "wb" : sourceText.startsWith("oz_") ? "oz" : "";
  const meta = getStockMarketplaceMeta(marketplace);
  const text = normalizeText(notes);
  const entityMatch =
    marketplace === "wb"
      ? text.match(/Поставка\s+([^,.;]+)/iu)
      : marketplace === "oz"
      ? text.match(/Сборка\s+([^,.;]+)/iu)
      : null;
  const orderMatch = text.match(/заказ\s+([^,.;]+)/iu);
  const scannedMatch = text.match(/отсканирован\s+([^,.;]+)/iu);
  const entityId = normalizeText(entityMatch?.[1]);
  const orderId = normalizeText(orderMatch?.[1]);
  const scannedBarcode = normalizeText(scannedMatch?.[1]);
  return {
    marketplace,
    marketplaceLabel: marketplace ? meta.marketplaceLabel : "",
    entityType: marketplace ? meta.entityType : "",
    entityLabel: marketplace ? meta.entityLabel : "",
    entityId: entityId || "",
    entityDisplay: entityId ? formatEntityDisplay(meta.entityLabel, entityId) : "",
    orderId: orderId || "",
    scannedBarcode: scannedBarcode || "",
  };
}

function buildTupleValues(items, keys, startIndex = 1) {
  const values = [];
  const params = [];
  let nextIndex = startIndex;
  for (const item of items) {
    const tuple = [];
    for (const key of keys) {
      tuple.push(`$${nextIndex}`);
      params.push(item[key]);
      nextIndex += 1;
    }
    values.push(`(${tuple.join(", ")})`);
  }
  return { values: values.join(", "), params };
}

function dedupePairs(items, keys) {
  const seen = new Set();
  const result = [];
  for (const item of items) {
    const signature = keys.map((key) => String(item?.[key] ?? "")).join("|");
    if (seen.has(signature)) continue;
    seen.add(signature);
    result.push(item);
  }
  return result;
}

function groupRowsByKey(rows, getKey) {
  const grouped = new Map();
  for (const row of rows || []) {
    const key = getKey(row);
    const bucket = grouped.get(key);
    if (bucket) bucket.push(row);
    else grouped.set(key, [row]);
  }
  return grouped;
}

function parseNumeric(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num : 0;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sortByCreatedAtAsc(left, right) {
  const leftTime = left?.createdAt ? new Date(left.createdAt).getTime() : 0;
  const rightTime = right?.createdAt ? new Date(right.createdAt).getTime() : 0;
  if (leftTime !== rightTime) return leftTime - rightTime;
  return Number(left?.id || 0) - Number(right?.id || 0);
}

function sortByCreatedAtDesc(left, right) {
  return sortByCreatedAtAsc(right, left);
}

function consumeOutstandingLots(queue, quantity) {
  let remaining = Math.max(0, parseNumeric(quantity));
  while (remaining > 0 && queue.length > 0) {
    const lot = queue[0];
    const available = Math.max(0, parseNumeric(lot.remainingQty));
    if (available <= 0) {
      queue.shift();
      continue;
    }
    const taken = Math.min(available, remaining);
    lot.remainingQty = available - taken;
    remaining -= taken;
    if (lot.remainingQty <= 0.000001) queue.shift();
  }
  return remaining;
}

function resolveOutstandingLots(events, getSignedQuantity) {
  const positiveLots = [];
  const negativeLots = [];
  for (const event of [...(events || [])].sort(sortByCreatedAtAsc)) {
    const signedQty = parseNumeric(getSignedQuantity(event));
    if (!signedQty) continue;
    if (signedQty > 0) {
      let remaining = consumeOutstandingLots(negativeLots, signedQty);
      if (remaining > 0) positiveLots.push({ ...event, remainingQty: remaining });
      continue;
    }
    let remaining = consumeOutstandingLots(positiveLots, Math.abs(signedQty));
    if (remaining > 0) negativeLots.push({ ...event, remainingQty: remaining });
  }
  return {
    positiveLots: positiveLots.filter((row) => parseNumeric(row.remainingQty) > 0),
    negativeLots: negativeLots.filter((row) => parseNumeric(row.remainingQty) > 0),
  };
}

function aggregatePositiveContexts(lots, requiredSource) {
  const aggregated = new Map();
  for (const lot of lots || []) {
    if (requiredSource && lot.source !== requiredSource) continue;
    const quantity = parseNumeric(lot.remainingQty);
    if (quantity <= 0) continue;
    const key = [
      normalizeText(lot.entityId),
      normalizeText(lot.orderId),
      normalizeText(lot.shelfName),
      normalizeText(lot.scannedBarcode),
    ].join("|");
    const current = aggregated.get(key) || {
      marketplace: lot.marketplace || "",
      marketplaceLabel: lot.marketplaceLabel || "",
      entityType: lot.entityType || "",
      entityLabel: lot.entityLabel || "",
      entityId: lot.entityId || "",
      entityDisplay: lot.entityDisplay || "",
      orderId: lot.orderId || "",
      shelfName: lot.shelfName || "",
      scannedBarcode: lot.scannedBarcode || "",
      quantity: 0,
      createdAt: lot.createdAt || null,
    };
    current.quantity += quantity;
    if (
      lot.createdAt &&
      (!current.createdAt || new Date(lot.createdAt).getTime() > new Date(current.createdAt).getTime())
    ) {
      current.createdAt = lot.createdAt;
    }
    aggregated.set(key, current);
  }
  return Array.from(aggregated.values()).sort(sortByCreatedAtDesc);
}

async function getExpectedWbEmployeeInventoryByArticle() {
  const result = await dbQuery(
    `select u.external_employee_id,
            o.article,
            coalesce(sum(greatest(coalesce(o.quantity, 1), 1)), 0)::int as qty
     from supply_orders o
     join supply_settings ss on ss.supply_id = o.supply_id
     join users u on u.id = o.assigned_user_id
     where ss.stock_method_enabled = true
       and o.assigned_user_id is not null
       and o.collected_at is null
       and u.external_employee_id is not null
       and coalesce(nullif(btrim(o.article), ''), '') <> ''
     group by u.external_employee_id, o.article`,
    []
  );
  const expected = new Map();
  for (const row of result.rows || []) {
    const employeeId = Number(row.external_employee_id || 0);
    const article = normalizeArticleKey(row.article);
    const qty = parseNumeric(row.qty);
    if (!employeeId || !article || qty <= 0) continue;
    expected.set(`${employeeId}|${article}`, qty);
  }
  return expected;
}

async function getExpectedOzonEmployeeInventoryByArticle() {
  const result = await dbQuery(
    `select u.external_employee_id,
            i.offer_id as article,
            coalesce(sum(greatest(coalesce(i.quantity, 1), 1)), 0)::int as qty
     from ozon_assembly_orders o
     join ozon_assemblies a on a.id = o.assembly_id
     join ozon_assembly_order_items i on i.assembly_id = o.assembly_id and i.posting_number = o.posting_number
     join users u on u.id = o.assigned_user_id
     where a.stock_method_enabled = true
       and o.assigned_user_id is not null
       and o.collected_at is null
       and o.sent_to_delivering_at is null
       and u.external_employee_id is not null
       and coalesce(nullif(btrim(i.offer_id), ''), '') <> ''
     group by u.external_employee_id, i.offer_id`,
    []
  );
  const expected = new Map();
  for (const row of result.rows || []) {
    const employeeId = Number(row.external_employee_id || 0);
    const article = normalizeArticleKey(row.article);
    const qty = parseNumeric(row.qty);
    if (!employeeId || !article || qty <= 0) continue;
    expected.set(`${employeeId}|${article}`, qty);
  }
  return expected;
}

async function getExpectedEmployeeInventoryByArticle() {
  const [wbExpected, ozExpected] = await Promise.all([
    getExpectedWbEmployeeInventoryByArticle(),
    getExpectedOzonEmployeeInventoryByArticle(),
  ]);
  const combined = new Map();
  for (const source of [wbExpected, ozExpected]) {
    for (const [key, qty] of source.entries()) {
      combined.set(key, parseNumeric(combined.get(key)) + parseNumeric(qty));
    }
  }
  return combined;
}

async function loadCurrentNegativeShelfRows() {
  const result = await skladQuery(
    `select si.shelf_id,
            si.product_id,
            si.quantity,
            coalesce(nullif(btrim(s.code), ''), nullif(btrim(s.name), ''), s.id::text) as shelf_name,
            p.name as product_name,
            p.article
     from shelf_items_s si
     join shelves_s s on s.id = si.shelf_id
     join racks_s r on r.id = s.rack_id
     join products_s p on p.id = si.product_id
     where r.warehouse_id = $1 and si.quantity < 0
     order by p.article, p.name, shelf_name`,
    [SKLAD_FBS_WAREHOUSE_ID]
  );
  return (result.rows || []).map((row) => ({
    shelfId: Number(row.shelf_id),
    productId: Number(row.product_id),
    quantity: parseNumeric(row.quantity),
    shelfName: row.shelf_name || "",
    productName: row.product_name || "",
    article: row.article || "",
  }));
}

async function loadShelfMovementHistory(pairs) {
  const uniquePairs = dedupePairs(
    (pairs || []).filter((item) => Number(item?.shelfId) > 0 && Number(item?.productId) > 0),
    ["shelfId", "productId"]
  );
  if (!uniquePairs.length) return [];
  const tuple = buildTupleValues(uniquePairs, ["shelfId", "productId"], 2);
  const result = await skladQuery(
    `with target_pairs(shelf_id, product_id) as (
       values ${tuple.values}
     )
     select sm.id,
            sm.shelf_id,
            sm.product_id,
            sm.operation_type,
            sm.quantity_before,
            sm.quantity_after,
            sm.quantity_delta,
            sm.user_id,
            sm.notes,
            sm.source,
            sm.created_at,
            p.name as product_name,
            p.article,
            coalesce(nullif(btrim(s.code), ''), nullif(btrim(s.name), ''), s.id::text) as shelf_name,
            link.to_employee_id,
            coalesce(e.full_name, u.username, '') as employee_name
     from shelf_movements_s sm
     join target_pairs tp on tp.shelf_id::int = sm.shelf_id and tp.product_id::int = sm.product_id
     join shelves_s s on s.id = sm.shelf_id
     join racks_s r on r.id = s.rack_id
     join products_s p on p.id = sm.product_id
     left join lateral (
       select m.id, m.to_employee_id
       from movements_s m
       where m.product_id = sm.product_id
         and m.from_shelf_id = sm.shelf_id
         and m.source = sm.source
         and m.performed_by = sm.user_id
         and m.notes = sm.notes
         and abs(extract(epoch from (m.created_at - sm.created_at))) < 2
       order by abs(extract(epoch from (m.created_at - sm.created_at))) asc, m.id desc
       limit 1
     ) link on true
     left join employees_s e on e.id = link.to_employee_id
     left join users_s u on u.id = sm.user_id
     where r.warehouse_id = $1
     order by sm.shelf_id, sm.product_id, sm.created_at asc, sm.id asc`,
    [SKLAD_FBS_WAREHOUSE_ID, ...tuple.params]
  );
  return (result.rows || []).map((row) => {
    const context = parseStockProblemContext(row.source, row.notes);
    return {
      id: Number(row.id),
      shelfId: Number(row.shelf_id),
      productId: Number(row.product_id),
      operationType: row.operation_type || "",
      quantityBefore: parseNumeric(row.quantity_before),
      quantityAfter: parseNumeric(row.quantity_after),
      quantityDelta: parseNumeric(row.quantity_delta),
      notes: row.notes || "",
      source: row.source || "",
      createdAt: row.created_at || null,
      productName: row.product_name || "",
      article: row.article || "",
      shelfName: row.shelf_name || "",
      employeeId: Number(row.to_employee_id || 0) || null,
      employeeName: row.employee_name || "",
      ...context,
    };
  });
}

async function loadEmployeeInventoryRows(mode) {
  const quantityClause = mode === "positive" ? "> 0" : "< 0";
  const result = await skladQuery(
    `select i.employee_id,
            e.external_employee_id,
            i.product_id,
            i.quantity,
            e.full_name as employee_name,
            p.name as product_name,
            p.article
     from employee_inventory_s i
     join employees_s e on e.id = i.employee_id
     join products_s p on p.id = i.product_id
     where i.quantity ${quantityClause}
     order by p.article, p.name, e.full_name`,
    []
  );
  return (result.rows || []).map((row) => ({
    employeeId: Number(row.employee_id),
    externalEmployeeId: Number(row.external_employee_id || 0) || null,
    productId: Number(row.product_id),
    quantity: parseNumeric(row.quantity),
    employeeName: row.employee_name || "",
    productName: row.product_name || "",
    article: row.article || "",
  }));
}

async function loadEmployeeMovementHistory(pairs) {
  const uniquePairs = dedupePairs(
    (pairs || []).filter((item) => Number(item?.employeeId) > 0 && Number(item?.productId) > 0),
    ["employeeId", "productId"]
  );
  if (!uniquePairs.length) return [];
  const tuple = buildTupleValues(uniquePairs, ["employeeId", "productId"], 1);
  const result = await skladQuery(
    `with target_pairs(employee_id, product_id) as (
       values ${tuple.values}
     )
     select m.id,
            m.product_id,
            m.quantity,
            m.source,
            m.notes,
            m.created_at,
            coalesce(m.to_employee_id, m.from_employee_id) as employee_id,
            p.name as product_name,
            p.article,
            e.full_name as employee_name,
            coalesce(nullif(btrim(s.code), ''), nullif(btrim(s.name), ''), s.id::text) as shelf_name
     from movements_s m
     join target_pairs tp
       on tp.employee_id::int = coalesce(m.to_employee_id, m.from_employee_id)
      and tp.product_id::int = m.product_id
     join products_s p on p.id = m.product_id
     join employees_s e on e.id = coalesce(m.to_employee_id, m.from_employee_id)
     left join shelves_s s on s.id = m.from_shelf_id
     where m.source in ('oz_pick_scan', 'wb_pick_scan', 'oz_collect', 'wb_collect')
     order by employee_id, m.product_id, m.created_at asc, m.id asc`,
    tuple.params
  );
  return (result.rows || []).map((row) => {
    const context = parseStockProblemContext(row.source, row.notes);
    return {
      id: Number(row.id),
      employeeId: Number(row.employee_id),
      productId: Number(row.product_id),
      quantity: parseNumeric(row.quantity),
      source: row.source || "",
      notes: row.notes || "",
      createdAt: row.created_at || null,
      productName: row.product_name || "",
      article: row.article || "",
      employeeName: row.employee_name || "",
      shelfName: row.shelf_name || "",
      ...context,
    };
  });
}

async function buildEnrichedStockProblemReport(marketplace) {
  const meta = getStockMarketplaceMeta(marketplace);
  const [expectedMap, currentNegativeShelves, currentNegativeEmployees, currentPositiveEmployees] = await Promise.all([
    getExpectedEmployeeInventoryByArticle(),
    loadCurrentNegativeShelfRows(),
    loadEmployeeInventoryRows("negative"),
    loadEmployeeInventoryRows("positive"),
  ]);

  const leftoverEmployeesBase = currentPositiveEmployees
    .map((row) => {
      const article = normalizeArticleKey(row.article);
      const expectedQty =
        row.externalEmployeeId && article ? parseNumeric(expectedMap.get(`${row.externalEmployeeId}|${article}`)) : 0;
      const actualQty = parseNumeric(row.quantity);
      const leftoverQty = actualQty - expectedQty;
      if (leftoverQty <= 0) return null;
      return {
        ...row,
        actualQty,
        expectedQty,
        quantity: leftoverQty,
      };
    })
    .filter(Boolean);

  const [shelfHistoryRows, employeeHistoryRows] = await Promise.all([
    loadShelfMovementHistory(currentNegativeShelves),
    loadEmployeeMovementHistory([
      ...currentNegativeEmployees.map((row) => ({ employeeId: row.employeeId, productId: row.productId })),
      ...leftoverEmployeesBase.map((row) => ({ employeeId: row.employeeId, productId: row.productId })),
    ]),
  ]);

  const shelfHistoryByKey = groupRowsByKey(shelfHistoryRows, (row) => `${row.shelfId}|${row.productId}`);
  const employeeHistoryByKey = groupRowsByKey(employeeHistoryRows, (row) => `${row.employeeId}|${row.productId}`);

  const negativeShelves = [];
  for (const snapshot of currentNegativeShelves) {
    const history = shelfHistoryByKey.get(`${snapshot.shelfId}|${snapshot.productId}`) || [];
    const resolved = resolveOutstandingLots(history, (row) => parseNumeric(row.quantityDelta));
    const unresolved = resolved.negativeLots.filter((row) => row.source === meta.pickSource);
    for (const row of unresolved) {
      negativeShelves.push({
        problemId: `shelf-${row.id}`,
        shelfId: snapshot.shelfId,
        shelfName: snapshot.shelfName,
        productId: snapshot.productId,
        productName: snapshot.productName,
        article: snapshot.article,
        currentQuantity: snapshot.quantity,
        quantity: parseNumeric(row.remainingQty),
        quantityAfter: parseNumeric(row.quantityAfter),
        createdAt: row.createdAt || null,
        employeeId: row.employeeId,
        employeeName: row.employeeName || "",
        marketplace: row.marketplace || meta.key,
        marketplaceLabel: row.marketplaceLabel || meta.marketplaceLabel,
        entityType: row.entityType || meta.entityType,
        entityLabel: row.entityLabel || meta.entityLabel,
        entityId: row.entityId || "",
        entityDisplay: row.entityDisplay || "",
        orderId: row.orderId || "",
        scannedBarcode: row.scannedBarcode || "",
      });
    }
  }

  const negativeEmployees = [];
  for (const snapshot of currentNegativeEmployees) {
    const history = employeeHistoryByKey.get(`${snapshot.employeeId}|${snapshot.productId}`) || [];
    const resolved = resolveOutstandingLots(history, (row) =>
      row.source === "oz_pick_scan" || row.source === "wb_pick_scan"
        ? parseNumeric(row.quantity)
        : row.source === "oz_collect" || row.source === "wb_collect"
        ? -parseNumeric(row.quantity)
        : 0
    );
    const unresolved = resolved.negativeLots.filter((row) => row.source === meta.collectSource);
    for (const row of unresolved) {
      negativeEmployees.push({
        problemId: `employee-negative-${row.id}`,
        employeeId: snapshot.employeeId,
        employeeName: snapshot.employeeName,
        productId: snapshot.productId,
        productName: snapshot.productName,
        article: snapshot.article,
        currentQuantity: -Math.abs(snapshot.quantity),
        quantity: parseNumeric(row.remainingQty),
        createdAt: row.createdAt || null,
        marketplace: row.marketplace || meta.key,
        marketplaceLabel: row.marketplaceLabel || meta.marketplaceLabel,
        entityType: row.entityType || meta.entityType,
        entityLabel: row.entityLabel || meta.entityLabel,
        entityId: row.entityId || "",
        entityDisplay: row.entityDisplay || "",
        orderId: row.orderId || "",
        shelfName: row.shelfName || "",
      });
    }
  }

  const leftoverEmployees = [];
  for (const snapshot of leftoverEmployeesBase) {
    const history = employeeHistoryByKey.get(`${snapshot.employeeId}|${snapshot.productId}`) || [];
    const resolved = resolveOutstandingLots(history, (row) =>
      row.source === "oz_pick_scan" || row.source === "wb_pick_scan"
        ? parseNumeric(row.quantity)
        : row.source === "oz_collect" || row.source === "wb_collect"
        ? -parseNumeric(row.quantity)
        : 0
    );
    const contexts = aggregatePositiveContexts(resolved.positiveLots, meta.pickSource);
    const marketQuantity = contexts.reduce((sum, row) => sum + parseNumeric(row.quantity), 0);
    if (marketQuantity <= 0) continue;
    leftoverEmployees.push({
      employeeId: snapshot.employeeId,
      employeeName: snapshot.employeeName,
      productId: snapshot.productId,
      productName: snapshot.productName,
      article: snapshot.article,
      actualQty: snapshot.actualQty,
      expectedQty: snapshot.expectedQty,
      quantity: snapshot.quantity,
      marketQuantity,
      contexts,
    });
  }

  negativeShelves.sort(sortByCreatedAtDesc);
  negativeEmployees.sort(sortByCreatedAtDesc);
  leftoverEmployees.sort((left, right) => {
    const articleCompare = String(left.article || "").localeCompare(String(right.article || ""), "ru", {
      numeric: true,
      sensitivity: "base",
    });
    if (articleCompare) return articleCompare;
    const nameCompare = String(left.productName || "").localeCompare(String(right.productName || ""), "ru", {
      numeric: true,
      sensitivity: "base",
    });
    if (nameCompare) return nameCompare;
    return String(left.employeeName || "").localeCompare(String(right.employeeName || ""), "ru", {
      numeric: true,
      sensitivity: "base",
    });
  });

  return {
    warehouseId: SKLAD_FBS_WAREHOUSE_ID,
    generatedAt: new Date().toISOString(),
    negativeShelves,
    negativeEmployees,
    leftoverEmployees,
  };
}

async function buildBasicStockProblemReport(marketplace) {
  const meta = getStockMarketplaceMeta(marketplace);
  const [expectedMap, currentNegativeShelves, currentNegativeEmployees, currentPositiveEmployees] = await Promise.all([
    getExpectedEmployeeInventoryByArticle(),
    loadCurrentNegativeShelfRows(),
    loadEmployeeInventoryRows("negative"),
    loadEmployeeInventoryRows("positive"),
  ]);

  const leftoverEmployees = currentPositiveEmployees
    .map((row) => {
      const article = normalizeArticleKey(row.article);
      const expectedQty =
        row.externalEmployeeId && article ? parseNumeric(expectedMap.get(`${row.externalEmployeeId}|${article}`)) : 0;
      const actualQty = parseNumeric(row.quantity);
      const leftoverQty = actualQty - expectedQty;
      if (leftoverQty <= 0) return null;
      return {
        employeeId: row.employeeId,
        employeeName: row.employeeName,
        productId: row.productId,
        productName: row.productName,
        article: row.article,
        actualQty,
        expectedQty,
        quantity: leftoverQty,
        marketQuantity: 0,
        contexts: [],
      };
    })
    .filter(Boolean);

  return {
    warehouseId: SKLAD_FBS_WAREHOUSE_ID,
    generatedAt: new Date().toISOString(),
    degraded: true,
    negativeShelves: currentNegativeShelves.map((row) => ({
      problemId: `basic-shelf-${row.shelfId}-${row.productId}`,
      shelfId: row.shelfId,
      shelfName: row.shelfName,
      productId: row.productId,
      productName: row.productName,
      article: row.article,
      currentQuantity: row.quantity,
      quantity: Math.abs(row.quantity),
      quantityAfter: row.quantity,
      createdAt: null,
      employeeId: null,
      employeeName: "",
      marketplace: meta.key,
      marketplaceLabel: meta.marketplaceLabel,
      entityType: meta.entityType,
      entityLabel: meta.entityLabel,
      entityId: "",
      entityDisplay: "",
      orderId: "",
      scannedBarcode: "",
    })),
    negativeEmployees: currentNegativeEmployees.map((row) => ({
      problemId: `basic-employee-${row.employeeId}-${row.productId}`,
      employeeId: row.employeeId,
      employeeName: row.employeeName,
      productId: row.productId,
      productName: row.productName,
      article: row.article,
      currentQuantity: row.quantity,
      quantity: Math.abs(row.quantity),
      createdAt: null,
      marketplace: meta.key,
      marketplaceLabel: meta.marketplaceLabel,
      entityType: meta.entityType,
      entityLabel: meta.entityLabel,
      entityId: "",
      entityDisplay: "",
      orderId: "",
      shelfName: "",
    })),
    leftoverEmployees,
  };
}

const stockProblemReportCache = {
  oz: null,
  wb: null,
};
// TTL + in-flight dedup so the heavy (~13s) report is not rebuilt on every 15s
// poll or by every concurrent admin. Cache holds last good report; on stale we
// rebuild once and everyone shares that single build.
const stockProblemReportMeta = {
  oz: { ts: 0, loading: null },
  wb: { ts: 0, loading: null },
};
const STOCK_PROBLEM_TTL_MS = 30000;

function isTransientStockProblemError(error) {
  const code = String(error?.code || "");
  if (["40P01", "40001", "55P03", "57014"].includes(code)) return true;
  const message = String(error?.message || "").toLowerCase();
  return (
    message.includes("deadlock detected") ||
    message.includes("timeout exceeded") ||
    message.includes("connection terminated due to connection timeout")
  );
}

async function buildStableStockProblemReportUncached(key) {
  let lastError = null;
  for (const waitMs of [0, 150, 350]) {
    if (waitMs > 0) await delay(waitMs);
    try {
      const report = await buildEnrichedStockProblemReport(key);
      stockProblemReportCache[key] = report;
      stockProblemReportMeta[key].ts = Date.now();
      return report;
    } catch (error) {
      lastError = error;
      if (!isTransientStockProblemError(error)) break;
    }
  }

  if (stockProblemReportCache[key]) {
    console.warn(`[stock-problems] ${key} using cached report after error:`, lastError?.message || lastError);
    return {
      ...stockProblemReportCache[key],
      generatedAt: stockProblemReportCache[key]?.generatedAt || new Date().toISOString(),
      cached: true,
    };
  }

  console.warn(`[stock-problems] ${key} using degraded report after error:`, lastError?.message || lastError);
  return buildBasicStockProblemReport(key);
}

async function getStableStockProblemReport(marketplace, force = false) {
  const key = String(marketplace || "").trim().toLowerCase() === "wb" ? "wb" : "oz";
  const meta = stockProblemReportMeta[key];
  const now = Date.now();
  // Fresh cache hit: return instantly without touching the slow sklad DB.
  if (!force && stockProblemReportCache[key] && now - meta.ts < STOCK_PROBLEM_TTL_MS) {
    return stockProblemReportCache[key];
  }
  // Rebuild in progress: share it (dedup concurrent admins / polls).
  if (meta.loading) return meta.loading;
  meta.loading = buildStableStockProblemReportUncached(key).finally(() => {
    meta.loading = null;
  });
  return meta.loading;
}

function proxyRequest(req, res, targetBase, targetPath) {
  const targetUrl = new URL(targetBase);
  const fullPath = targetPath.includes("?") ? targetPath : `${targetPath}${getQuerySuffix(req)}`;
  const isLongRunningPost =
    String(req.method || "").toUpperCase() === "POST" &&
    (
      /\/OZ\/api\/assemblies\/create(?:\?|$)/.test(fullPath) ||
      /\/WB\/api\/supplies\/create(?:\?|$)/.test(fullPath) ||
      /\/OZ\/api\/assemblies\/[^/?]+\/sync-awaiting-deliver(?:\?|$)/.test(fullPath)
    );
  const timeoutMs =
    isLongRunningPost
      ? CREATE_PROXY_TIMEOUT_MS
      : PROXY_TIMEOUT_MS;
  let completed = false;

  const proxyReq = http.request(
    {
      hostname: targetUrl.hostname,
      port: Number(targetUrl.port) || 80,
      path: fullPath,
      method: req.method,
      headers: buildProxyHeaders(req, targetUrl),
    },
    (proxyRes) => {
      const headers = { ...proxyRes.headers };
      delete headers["set-cookie"];
      if (headers.location) {
        headers.location = rewriteProxyString(String(headers.location));
      }

      const contentType = String(headers["content-type"] || "");
      const shouldRewriteJson = req.method !== "HEAD" && /\bapplication\/json\b/i.test(contentType);
      if (!shouldRewriteJson) {
        completed = true;
        res.writeHead(proxyRes.statusCode || 502, headers);
        proxyRes.pipe(res, { end: true });
        return;
      }

      delete headers["content-length"];
      delete headers["transfer-encoding"];
      const chunks = [];
      proxyRes.on("data", (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      proxyRes.on("end", () => {
        completed = true;
        let body = Buffer.concat(chunks).toString("utf8");
        try {
          const payload = JSON.parse(body);
          body = JSON.stringify(rewriteProxyPayload(payload));
        } catch {
          body = rewriteProxyString(body);
        }
        headers["content-length"] = Buffer.byteLength(body);
        res.writeHead(proxyRes.statusCode || 502, headers);
        res.end(body);
      });
    }
  );

  proxyReq.setTimeout(timeoutMs, () => {
    if (completed) return;
    console.error(`[proxy] ${req.method} ${fullPath} timeout after ${timeoutMs}ms`);
    proxyReq.destroy(new Error("PROXY_TIMEOUT"));
  });

  proxyReq.on("error", (error) => {
    console.error(`[proxy] ${req.method} ${fullPath} error:`, error.message);
    if (!res.headersSent) {
      const status = String(error?.message || "").includes("PROXY_TIMEOUT") ? 504 : 502;
      res.status(status).json({ error: status === 504 ? "PROXY_TIMEOUT" : "PROXY_ERROR" });
    }
  });

  if (req.method === "GET" || req.method === "HEAD") {
    proxyReq.end();
    return;
  }
  req.pipe(proxyReq, { end: true });
}

function proxySSE(req, res, targetBase, targetPath) {
  const targetUrl = new URL(targetBase);
  const fullPath = targetPath.includes("?") ? targetPath : `${targetPath}${getQuerySuffix(req)}`;
  let connected = false;
  const headers = buildProxyHeaders(req, targetUrl);
  headers.accept = "text/event-stream";
  headers["cache-control"] = "no-cache";
  const proxyReq = http.request(
    {
      hostname: targetUrl.hostname,
      port: Number(targetUrl.port) || 80,
      path: fullPath,
      method: "GET",
      headers,
    },
    (proxyRes) => {
      connected = true;
      proxyReq.setTimeout(0);
      if (proxyRes.socket) proxyRes.socket.setTimeout(0);
      res.writeHead(proxyRes.statusCode || 200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      });
      proxyRes.pipe(res, { end: true });
    }
  );

  proxyReq.setTimeout(PROXY_TIMEOUT_MS, () => {
    if (connected) return;
    console.error(`[sse-proxy] ${fullPath} timeout after ${PROXY_TIMEOUT_MS}ms`);
    proxyReq.destroy(new Error("SSE_PROXY_TIMEOUT"));
  });

  proxyReq.on("error", (error) => {
    console.error(`[sse-proxy] ${fullPath} error:`, error.message);
    if (!res.headersSent) res.status(502).json({ error: "SSE_PROXY_ERROR" });
  });

  req.on("close", () => proxyReq.destroy());
  proxyReq.end();
}

app.get(
  `${API_ROOT}/earnings/me`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    const summary = await getEmployeeCoinSummary(Number(req.sessionUser.id));
    if (summary) {
      const rates = await getCoinRates();
      summary.showEmployeeBalance = rates.showEmployeeBalance !== false;
    }
    res.json({ summary });
  })
);

app.get(
  `${API_ROOT}/earnings/dashboard`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireAdminUser(req, res)) return;
    const dashboard = await getEarningsDashboard(getEarningsFilters(req.query));
    res.json(dashboard);
  })
);

app.get(
  `${API_ROOT}/earnings/employees/:userId`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireAdminUser(req, res)) return;
    const userId = Number(req.params.userId);
    if (!Number.isFinite(userId) || userId < 1) {
      return res.status(400).json({ error: "Некорректный сотрудник" });
    }
    const detail = await getEmployeeEarningsDetail(userId, getEarningsFilters(req.query));
    if (!detail) {
      return res.status(404).json({ error: "Сотрудник не найден" });
    }
    res.json(detail);
  })
);

app.patch(
  `${API_ROOT}/earnings/settings`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    if (!requireAdminUser(req, res)) return;
    const rawPickRate = req.body?.pickItemRatePerUnit ?? req.body?.ratePerUnit;
    const rawOrderRate = req.body?.orderCollectRatePerUnit;
    const hasShowBalance = typeof req.body?.showEmployeeBalance === "boolean";
    // Visibility-only update (the «показывать заработок» checkbox): no rate validation needed.
    if (hasShowBalance && rawPickRate == null && rawOrderRate == null) {
      const settings = await setCoinRates(
        { showEmployeeBalance: req.body.showEmployeeBalance },
        Number(req.sessionUser.id)
      );
      return res.json({ settings });
    }
    const nextPickRate = parseMoneyValue(rawPickRate, NaN);
    const nextOrderRate =
      rawOrderRate == null || rawOrderRate === ""
        ? NaN
        : parseMoneyValue(rawOrderRate, NaN);
    if (!Number.isFinite(nextPickRate) || nextPickRate < 0) {
      return res.status(400).json({ error: "Укажите корректную ставку за сборку товара" });
    }
    if (!Number.isNaN(nextOrderRate) && (!Number.isFinite(nextOrderRate) || nextOrderRate < 0)) {
      return res.status(400).json({ error: "Укажите корректную ставку за сборку заказа" });
    }
    const settings = await setCoinRates(
      {
        pickItemRatePerUnit: nextPickRate,
        orderCollectRatePerUnit: Number.isNaN(nextOrderRate) ? undefined : nextOrderRate,
      },
      Number(req.sessionUser.id)
    );
    res.json({ settings });
  })
);

app.post(
  `${API_ROOT}/earnings/employees/:userId/adjust`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    if (!requireAdminUser(req, res)) return;
    const userId = Number(req.params.userId);
    if (!Number.isFinite(userId) || userId < 1) {
      return res.status(400).json({ error: "Некорректный сотрудник" });
    }
    const balance = parseMoneyValue(req.body?.balance, NaN);
    if (!Number.isFinite(balance) || balance < 0) {
      return res.status(400).json({ error: "Укажите корректный баланс" });
    }
    const userResult = await dbQuery(`select ${USER_COLS} from users where id = $1 and role = 'employee' limit 1`, [userId]);
    const targetUser = userResult.rows?.[0] || null;
    if (!targetUser) {
      return res.status(404).json({ error: "Сотрудник не найден" });
    }
    const currentSummary = await getEmployeeCoinSummary(userId);
    const amountDelta = roundMoney(balance - currentSummary.balance);
    const result =
      amountDelta === 0
        ? { event: null, summary: { ...currentSummary, balance } }
        : await insertCoinEvent({
            userId,
            externalEmployeeId: targetUser.external_employee_id ? Number(targetUser.external_employee_id) : null,
            eventType: EARNINGS_EVENT_MANUAL_ADJUSTMENT,
            rewardUnits: 0,
            ratePerUnit: 0,
            amountDelta,
            notes: toNullableText(req.body?.notes) || `Корректировка баланса администратором ${getDisplayUserName(req.sessionUser)}`,
            sourceTaskId: null,
            dedupeKey: null,
          });
    res.json({
      event: result.event,
      summary: result.summary,
    });
  })
);

app.get(
  `${API_ROOT}/global/settings`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    res.json(await getGlobalSettingsPayload(req));
  })
);

app.get(
  `${API_ROOT}/global/users`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    res.json({ users: await listGlobalUsers() });
  })
);

app.get(
  `${API_ROOT}/global/users/directory`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    res.json({ users: await listExternalDirectoryUsers() });
  })
);

app.post(
  `${API_ROOT}/global/users`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    try {
      const user = await createGlobalUser(req.body || {}, Number(req.sessionUser.id));
      res.json({ user });
    } catch (error) {
      res.status(error.statusCode || 500).json({ error: error?.message || "Не удалось добавить пользователя" });
    }
  })
);

app.patch(
  `${API_ROOT}/global/users/:userId`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    try {
      const user = await updateGlobalUser(Number(req.params.userId), req.body || {}, Number(req.sessionUser.id));
      res.json({ user });
    } catch (error) {
      res.status(error.statusCode || 500).json({ error: error?.message || "Не удалось обновить пользователя" });
    }
  })
);

app.delete(
  `${API_ROOT}/global/users/:userId`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    try {
      const user = await archiveGlobalUser(Number(req.params.userId), Number(req.sessionUser.id));
      res.json({ user });
    } catch (error) {
      res.status(error.statusCode || 500).json({ error: error?.message || "Не удалось удалить пользователя" });
    }
  })
);

app.get(
  `${API_ROOT}/global/warehouses`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    const [locations, skladWarehouses, warehouseSettings, marketplaceOptions] = await Promise.all([
      listLocations(),
      listSkladWarehouses(),
      listLocationWarehouseSettings(),
      fetchMarketplaceOptions(req),
    ]);
    res.json({ locations, skladWarehouses, warehouseSettings, marketplaceOptions });
  })
);

app.post(
  `${API_ROOT}/global/warehouse-settings`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    try {
      const setting = await upsertWarehouseSetting(req.body || {}, Number(req.sessionUser.id));
      res.json({ setting });
    } catch (error) {
      res.status(error.statusCode || 500).json({ error: error?.message || "Не удалось сохранить настройку склада" });
    }
  })
);

app.delete(
  `${API_ROOT}/global/warehouse-settings/:settingId`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    if (!requireSuperUser(req, res)) return;
    try {
      res.json(await archiveWarehouseSetting(Number(req.params.settingId), Number(req.sessionUser.id)));
    } catch (error) {
      res.status(error.statusCode || 500).json({ error: error?.message || "Не удалось удалить настройку склада" });
    }
  })
);

app.post(
  `${API_ROOT}/oz/employee/assemblies/:assemblyId/pick-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      OZ_BACKEND,
      `/OZ/api/employee/assemblies/${encodeURIComponent(req.params.assemblyId)}/pick-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    const reward = await awardPickScanReward({
      marketplace: "oz",
      entityId: normalizeText(req.params.assemblyId),
      sessionUser: req.sessionUser,
      responseData: result.data || {},
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: reward.event,
      employeeCoin: reward.summary,
    });
  })
);

app.post(
  `${OZ_API_ROOT}/employee/assemblies/:assemblyId/pick-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      OZ_BACKEND,
      `/OZ/api/employee/assemblies/${encodeURIComponent(req.params.assemblyId)}/pick-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    const reward = await awardPickScanReward({
      marketplace: "oz",
      entityId: normalizeText(req.params.assemblyId),
      sessionUser: req.sessionUser,
      responseData: result.data || {},
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: reward.event,
      employeeCoin: reward.summary,
    });
  })
);

app.post(
  `${API_ROOT}/wb/employee/supplies/:supplyId/pick-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      WB_BACKEND,
      `/WB/api/employee/supplies/${encodeURIComponent(req.params.supplyId)}/pick-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    const reward = await awardPickScanReward({
      marketplace: "wb",
      entityId: normalizeText(req.params.supplyId),
      sessionUser: req.sessionUser,
      responseData: result.data || {},
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: reward.event,
      employeeCoin: reward.summary,
    });
  })
);

app.post(
  `${WB_API_ROOT}/employee/supplies/:supplyId/pick-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      WB_BACKEND,
      `/WB/api/employee/supplies/${encodeURIComponent(req.params.supplyId)}/pick-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    const reward = await awardPickScanReward({
      marketplace: "wb",
      entityId: normalizeText(req.params.supplyId),
      sessionUser: req.sessionUser,
      responseData: result.data || {},
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: reward.event,
      employeeCoin: reward.summary,
    });
  })
);

app.post(
  `${API_ROOT}/oz/employee/assemblies/:assemblyId/orders/:postingNumber/label-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      OZ_BACKEND,
      `/OZ/api/employee/assemblies/${encodeURIComponent(req.params.assemblyId)}/orders/${encodeURIComponent(req.params.postingNumber)}/label-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    queueOrderCollectReward({
      marketplace: "oz",
      entityId: normalizeText(req.params.assemblyId),
      orderRef: normalizeText(req.params.postingNumber),
      sessionUser: req.sessionUser,
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: null,
      employeeCoin: null,
    });
  })
);

app.post(
  `${OZ_API_ROOT}/employee/assemblies/:assemblyId/orders/:postingNumber/label-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      OZ_BACKEND,
      `/OZ/api/employee/assemblies/${encodeURIComponent(req.params.assemblyId)}/orders/${encodeURIComponent(req.params.postingNumber)}/label-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    queueOrderCollectReward({
      marketplace: "oz",
      entityId: normalizeText(req.params.assemblyId),
      orderRef: normalizeText(req.params.postingNumber),
      sessionUser: req.sessionUser,
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: null,
      employeeCoin: null,
    });
  })
);

app.post(
  `${API_ROOT}/wb/employee/supplies/:supplyId/orders/:orderId/label-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      WB_BACKEND,
      `/WB/api/employee/supplies/${encodeURIComponent(req.params.supplyId)}/orders/${encodeURIComponent(req.params.orderId)}/label-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    queueOrderCollectReward({
      marketplace: "wb",
      entityId: normalizeText(req.params.supplyId),
      orderRef: normalizeText(req.params.orderId),
      sessionUser: req.sessionUser,
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: null,
      employeeCoin: null,
    });
  })
);

app.post(
  `${WB_API_ROOT}/employee/supplies/:supplyId/orders/:orderId/label-scan`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      WB_BACKEND,
      `/WB/api/employee/supplies/${encodeURIComponent(req.params.supplyId)}/orders/${encodeURIComponent(req.params.orderId)}/label-scan`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    queueOrderCollectReward({
      marketplace: "wb",
      entityId: normalizeText(req.params.supplyId),
      orderRef: normalizeText(req.params.orderId),
      sessionUser: req.sessionUser,
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: null,
      employeeCoin: null,
    });
  })
);

app.post(
  `${API_ROOT}/wb/employee/supplies/:supplyId/orders/:orderId/collect`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      WB_BACKEND,
      `/WB/api/employee/supplies/${encodeURIComponent(req.params.supplyId)}/orders/${encodeURIComponent(req.params.orderId)}/collect`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    queueOrderCollectReward({
      marketplace: "wb",
      entityId: normalizeText(req.params.supplyId),
      orderRef: normalizeText(req.params.orderId),
      sessionUser: req.sessionUser,
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: null,
      employeeCoin: null,
    });
  })
);

app.post(
  `${WB_API_ROOT}/employee/supplies/:supplyId/orders/:orderId/collect`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    const result = await proxyJsonRequest(
      req,
      WB_BACKEND,
      `/WB/api/employee/supplies/${encodeURIComponent(req.params.supplyId)}/orders/${encodeURIComponent(req.params.orderId)}/collect`,
      req.body || {}
    );
    if (!result.ok) return sendProxyJsonResult(res, result);
    queueOrderCollectReward({
      marketplace: "wb",
      entityId: normalizeText(req.params.supplyId),
      orderRef: normalizeText(req.params.orderId),
      sessionUser: req.sessionUser,
      scannedCode: req.body?.barcode,
    });
    res.status(result.status).json({
      ...(typeof result.data === "object" && result.data ? result.data : {}),
      reward: null,
      employeeCoin: null,
    });
  })
);

app.get(`${API_ROOT}/oz/events`, requireProxyAuth, (req, res) => {
  proxySSE(req, res, OZ_BACKEND, "/OZ/api/events");
});

app.get(`${API_ROOT}/wb/events`, requireProxyAuth, (req, res) => {
  proxySSE(req, res, WB_BACKEND, "/WB/api/events");
});

app.get(`${OZ_API_ROOT}/events`, requireProxyAuth, (req, res) => {
  proxySSE(req, res, OZ_BACKEND, "/OZ/api/events");
});

app.get(`${WB_API_ROOT}/events`, requireProxyAuth, (req, res) => {
  proxySSE(req, res, WB_BACKEND, "/WB/api/events");
});

app.get(
  `${API_ROOT}/oz/stock-problems`,
  requireProxyAuth,
  asyncHandler(async (_req, res) => {
    const report = await getStableStockProblemReport("oz");
    res.json(report);
  })
);

app.get(
  `${API_ROOT}/wb/stock-problems`,
  requireProxyAuth,
  asyncHandler(async (_req, res) => {
    const report = await getStableStockProblemReport("wb");
    res.json(report);
  })
);

app.get(
  `${OZ_API_ROOT}/stock-problems`,
  requireProxyAuth,
  asyncHandler(async (_req, res) => {
    const report = await getStableStockProblemReport("oz");
    res.json(report);
  })
);

app.get(
  `${WB_API_ROOT}/stock-problems`,
  requireProxyAuth,
  asyncHandler(async (_req, res) => {
    const report = await getStableStockProblemReport("wb");
    res.json(report);
  })
);

async function loadWbSupplyQrState(req, supplyId) {
  const result = await proxyJsonRequestWithMethod(
    "GET",
    req,
    WB_BACKEND,
    `/WB/api/supplies/${encodeURIComponent(supplyId)}/trbx`
  );
  if (!result.ok) return result;
  return {
    ...result,
    data: normalizeWbSupplyQrPayload(result.data),
  };
}

async function handleWbSupplyQrCreate(req, res) {
  const supplyId = normalizeText(req.params.supplyId);
  if (!supplyId) {
    return res.status(400).json({ error: "SUPPLY_ID_REQUIRED" });
  }

  const current = await loadWbSupplyQrState(req, supplyId);
  if (!current.ok) return sendProxyJsonResult(res, current);

  const currentBoxes = Array.isArray(current.data?.boxes) ? current.data.boxes : [];
  const currentBoxCount = Math.max(Number(current.data?.summary?.boxCount || 0), currentBoxes.length);
  if (currentBoxCount > 0) {
    return res.status(200).json(current.data);
  }

  const created = await proxyJsonRequestWithMethod(
    "POST",
    req,
    WB_BACKEND,
    `/WB/api/supplies/${encodeURIComponent(supplyId)}/trbx`,
    { amount: 1 }
  );
  if (!created.ok) return sendProxyJsonResult(res, created);

  return res.status(created.status).json(normalizeWbSupplyQrPayload(created.data));
}

app.get(
  `${API_ROOT}/wb/supplies/:supplyId/trbx`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    const result = await loadWbSupplyQrState(req, req.params.supplyId);
    if (!result.ok) return sendProxyJsonResult(res, result);
    res.status(result.status).json(result.data);
  })
);

app.get(
  `${WB_API_ROOT}/supplies/:supplyId/trbx`,
  requireProxyAuth,
  asyncHandler(async (req, res) => {
    const result = await loadWbSupplyQrState(req, req.params.supplyId);
    if (!result.ok) return sendProxyJsonResult(res, result);
    res.status(result.status).json(result.data);
  })
);

app.post(
  `${API_ROOT}/wb/supplies/:supplyId/trbx`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    await handleWbSupplyQrCreate(req, res);
  })
);

app.post(
  `${WB_API_ROOT}/supplies/:supplyId/trbx`,
  requireProxyAuth,
  express.json(),
  asyncHandler(async (req, res) => {
    await handleWbSupplyQrCreate(req, res);
  })
);

app.use(`${API_ROOT}/oz`, requireProxyAuth, (req, res) => {
  proxyRequest(req, res, OZ_BACKEND, `/OZ/api${req.url}`);
});

app.use(`${API_ROOT}/wb`, requireProxyAuth, (req, res) => {
  proxyRequest(req, res, WB_BACKEND, `/WB/api${req.url}`);
});

app.use(OZ_API_ROOT, requireProxyAuth, (req, res) => {
  proxyRequest(req, res, OZ_BACKEND, `/OZ/api${req.url}`);
});

app.use(WB_API_ROOT, requireProxyAuth, (req, res) => {
  proxyRequest(req, res, WB_BACKEND, `/WB/api${req.url}`);
});

function sendIndex(res, indexFile) {
  if (!fs.existsSync(indexFile)) {
    return res.status(503).send("Build files not found");
  }
  res.setHeader("Cache-Control", "no-store");
  return res.sendFile(indexFile);
}

// Static assets (js/css/fonts/hashed vite bundles) are versioned via ?v= or
// content-hash, so cache them for a year. HTML shells stay no-store so new
// deploys (with bumped ?v=) are always picked up immediately.
const STATIC_CACHE_OPTS = {
  index: false,
  setHeaders: (res, filePath) => {
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-store");
    } else {
      res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    }
  },
};

app.use((req, res, next) => {
  if (req.method !== "GET") return next();
  const pathname = req.path || "";
  const isOldShellRoute =
    pathname === OZ_BASE ||
    pathname.startsWith(`${OZ_BASE}/`) ||
    pathname === WB_BASE ||
    pathname.startsWith(`${WB_BASE}/`);
  if (!isOldShellRoute) return next();
  if (
    pathname === OZ_API_ROOT ||
    pathname.startsWith(`${OZ_API_ROOT}/`) ||
    pathname === WB_API_ROOT ||
    pathname.startsWith(`${WB_API_ROOT}/`) ||
    pathname.startsWith(`${OZ_BASE}/assets/`) ||
    pathname.startsWith(`${WB_BASE}/assets/`)
  ) {
    return next();
  }
  return sendIndex(res, SHELL_INDEX);
});

function mountSpa(basePath, staticDir, indexFile) {
  app.use(basePath, express.static(staticDir, STATIC_CACHE_OPTS));

  app.use(basePath, (req, res, next) => {
    if (req.method !== "GET" || req.path.startsWith("/api/")) return next();
    return sendIndex(res, indexFile);
  });
}

mountSpa(OZ_BASE, OZ_DIST_DIR, OZ_INDEX);
mountSpa(WB_BASE, WB_DIST_DIR, WB_INDEX);

app.get(`${BASE}/health`, (_req, res) => {
  res.json({ ok: true, service: "sborka", at: new Date().toISOString() });
});

app.use(BASE, express.static(DIST_DIR, STATIC_CACHE_OPTS));

app.use(BASE, (req, res, next) => {
  if (req.method !== "GET" || req.path.startsWith("/api/")) return next();
  if (req.path === "/oz" || req.path.startsWith("/oz/") || req.path === "/wb" || req.path.startsWith("/wb/")) {
    return next();
  }
  return sendIndex(res, SHELL_INDEX);
});

app.use((err, req, res, _next) => {
  const requestId = req.requestId || normalizeApplicationEventId(null);
  console.error(`[server:${requestId}] error:`, err);
  void recordApplicationLog({
    eventId: requestId,
    source: "server_exception",
    severity: "error",
    marketplace: marketplaceFromRequestPath(req.originalUrl),
    userId: req.sessionUser?.id || null,
    userName:
      req.sessionUser?.fullName ||
      [req.sessionUser?.surname, req.sessionUser?.name].filter(Boolean).join(" ") ||
      req.sessionUser?.login ||
      null,
    userRole: req.sessionUser?.role || null,
    requestMethod: req.method,
    requestPath: req.originalUrl,
    httpStatus: 500,
    message: err?.message || "Unhandled server error",
    stack: err?.stack || null,
    context: { code: err?.code || null },
    userAgent: req.headers["user-agent"] || null,
    clientIp: req.headers["x-forwarded-for"] || req.socket?.remoteAddress || null,
  }).catch((logError) => {
    console.error(`[server:${requestId}] failed to persist exception:`, logError?.message || logError);
  });
  if (!res.headersSent) res.status(500).json({ error: "INTERNAL_ERROR" });
});

app.listen(PORT, HOST, () => {
  console.log(`[sborka] listening on http://${HOST}:${PORT}${BASE}`);
  console.log(`[sborka] OZ backend: ${OZ_BACKEND}`);
  console.log(`[sborka] WB backend: ${WB_BACKEND}`);
  ensureEarningsSchema()
    .then(() => console.log("[sborka] employee earnings schema ready"))
    .catch((error) => console.error("[sborka] employee earnings schema init failed:", error?.message || error));
  ensureGlobalSettingsSchema()
    .then(() => console.log("[sborka] global settings schema ready"))
    .catch((error) => console.error("[sborka] global settings schema init failed:", error?.message || error));
  ensureApplicationLogSchema()
    .then(() => console.log("[sborka] application error log schema ready"))
    .catch((error) => console.error("[sborka] application error log schema init failed:", error?.message || error));
  ensureSkladEarningsSchema()
    .then((ready) => {
      if (!ready) return;
      console.log("[sborka] sklad earnings schema ready");
      return syncPendingCoinEvents(100)
        .then((result) => console.log(`[sborka] sklad sync bootstrap done: ${result?.processed || 0}`))
        .catch((error) => console.error("[sborka] sklad sync bootstrap failed:", error?.message || error));
    })
    .catch((error) => console.error("[sborka] sklad earnings schema init failed:", error?.message || error));
  setInterval(() => {
    syncPendingCoinEvents(100).catch((error) => {
      console.error("[sborka] scheduled sklad sync failed:", error?.message || error);
    });
  }, 60000).unref();
});
