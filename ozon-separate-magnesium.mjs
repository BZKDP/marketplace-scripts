import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import pg from "/opt/grasklad-sborka/apps/OZ/node_modules/pg/lib/index.js";

function loadEnv(file) {
  for (const rawLine of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const line = rawLine.trim().replace(/^\uFEFF/, "");
    if (!line || line.startsWith("#")) continue;
    const separator = line.indexOf("=");
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    let value = line.slice(separator + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    process.env[key] ??= value;
  }
}

loadEnv("/opt/grasklad-sborka/.env");

const stores = [
  {
    id: process.env.OZON_STORE_1_ID || "irina",
    name: process.env.OZON_STORE_1_NAME || "ИП Ирина",
    clientId: process.env.OZON_STORE_1_CLIENT_ID || process.env.OZON_CLIENT_ID,
    apiKey: process.env.OZON_STORE_1_API_KEY || process.env.OZON_API_KEY,
    warehouseId: "1020005026543450",
    warehouseName: "Мск",
  },
  {
    id: process.env.OZON_STORE_2_ID || "evgeny",
    name: process.env.OZON_STORE_2_NAME || "ИП Евгений",
    clientId: process.env.OZON_STORE_2_CLIENT_ID,
    apiKey: process.env.OZON_STORE_2_API_KEY,
    warehouseId: "1020005010664520",
    warehouseName: "Москва",
  },
].filter((store) => store.clientId && store.apiKey);

async function listAwaiting(store) {
  const now = new Date();
  const since = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000);
  const rows = [];
  let offset = 0;
  while (true) {
    const response = await fetch("https://api-seller.ozon.ru/v3/posting/fbs/list", {
      method: "POST",
      headers: { "Client-Id": store.clientId, "Api-Key": store.apiKey, "Content-Type": "application/json" },
      body: JSON.stringify({
        dir: "ASC",
        filter: { since: since.toISOString(), to: now.toISOString(), status: "awaiting_deliver" },
        limit: 1000,
        offset,
        with: { analytics_data: true, barcodes: false },
      }),
    });
    if (!response.ok) throw new Error(`${store.name}: Ozon HTTP ${response.status}`);
    const payload = await response.json();
    const page = Array.isArray(payload?.result?.postings) ? payload.result.postings : [];
    rows.push(...page);
    if (payload?.result?.has_next !== true || page.length === 0) break;
    offset += 1000;
  }
  return [...new Map(rows.map((row) => [String(row.posting_number || "").trim(), row]).filter(([number]) => number)).values()];
}

function isMagnesiumItem(item) {
  const article = String(item?.offer_id || "").trim().toUpperCase();
  const name = String(item?.name || "").trim().toLowerCase();
  return article === "GRA-01503"
    || /(магний|magnesium).*(b6|в6|б6)/i.test(name)
    || /(b6|в6|б6).*(магний|magnesium)/i.test(name);
}

function moscowParts() {
  return Object.fromEntries(
    new Intl.DateTimeFormat("en-GB", {
      timeZone: "Europe/Moscow",
      day: "2-digit",
      month: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).formatToParts(new Date()).filter((part) => part.type !== "literal").map((part) => [part.type, part.value])
  );
}

async function insertObject(client, table, row) {
  const columns = Object.keys(row);
  await client.query(
    `insert into ${table} (${columns.map((column) => `"${column}"`).join(", ")}) values (${columns.map((_, index) => `$${index + 1}`).join(", ")})`,
    columns.map((column) => row[column])
  );
}

const liveByStore = new Map();
for (const store of stores) liveByStore.set(store.id, await listAwaiting(store));
const allLiveNumbers = stores.flatMap((store) => liveByStore.get(store.id).map((row) => String(row.posting_number).trim()));

const { Pool } = pg;
const db = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  ssl: String(process.env.DB_SSL || "auto").toLowerCase() === "disable" ? undefined : { rejectUnauthorized: false },
  max: 1,
});

const client = await db.connect();
let backupDir = null;
try {
  await client.query("begin");
  const ordersBefore = await client.query(
    `select * from ozon_assembly_orders where posting_number = any($1::text[]) order by store_id, posting_number for update`,
    [allLiveNumbers]
  );
  const itemsBefore = await client.query(
    `select * from ozon_assembly_order_items where posting_number = any($1::text[]) order by assembly_id, posting_number, line_no`,
    [allLiveNumbers]
  );
  const itemsByPosting = new Map();
  for (const item of itemsBefore.rows) {
    const list = itemsByPosting.get(item.posting_number) || [];
    list.push(item);
    itemsByPosting.set(item.posting_number, list);
  }

  const selectedByStore = new Map();
  for (const store of stores) {
    const liveNumbers = new Set(liveByStore.get(store.id).map((row) => String(row.posting_number).trim()));
    const selected = ordersBefore.rows.filter(
      (order) => order.store_id === store.id
        && liveNumbers.has(order.posting_number)
        && order.sent_to_delivering_at == null
        && order.collected_at == null
        && (itemsByPosting.get(order.posting_number) || []).some(isMagnesiumItem)
    );
    selectedByStore.set(store.id, selected);
  }

  const selectedOrders = stores.flatMap((store) => selectedByStore.get(store.id));
  const selectedNumbers = selectedOrders.map((order) => order.posting_number);
  if (!selectedNumbers.length) throw new Error("Не найдено актуальных заказов с Магнием B6");
  const sourceAssemblyIds = [...new Set(selectedOrders.map((order) => order.assembly_id))];
  const assembliesBefore = await client.query(
    `select * from ozon_assemblies where id = any($1::text[]) order by id for update`,
    [sourceAssemblyIds]
  );
  const issuesBefore = await client.query(
    `select * from ozon_employee_scan_issues where posting_number = any($1::text[]) order by id`,
    [selectedNumbers]
  );
  const skipsBefore = await client.query(
    `select * from ozon_stock_shortage_skips where posting_number = any($1::text[]) order by id`,
    [selectedNumbers]
  );

  const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  backupDir = path.join("/opt/grasklad-sborka/backups", `${stamp}-ozon-separate-magnesium`);
  fs.mkdirSync(backupDir, { recursive: true, mode: 0o700 });
  const backup = {
    createdAt: new Date().toISOString(),
    assemblies: assembliesBefore.rows,
    orders: selectedOrders,
    items: itemsBefore.rows.filter((item) => selectedNumbers.includes(item.posting_number)),
    scanIssues: issuesBefore.rows,
    stockShortageSkips: skipsBefore.rows,
  };
  const backupJson = JSON.stringify(backup, null, 2);
  fs.writeFileSync(path.join(backupDir, "before.json"), backupJson, { mode: 0o600 });
  fs.writeFileSync(path.join(backupDir, "before.sha256"), `${crypto.createHash("sha256").update(backupJson).digest("hex")}  before.json\n`, { mode: 0o600 });

  await client.query(`delete from ozon_assembly_orders where posting_number = any($1::text[])`, [selectedNumbers]);

  const created = [];
  const date = moscowParts();
  for (const store of stores) {
    const selected = selectedByStore.get(store.id);
    if (!selected.length) continue;
    const selectedSet = new Set(selected.map((order) => order.posting_number));
    const newId = `ozon-hold-magnesium-${store.id}-${Date.now()}-${crypto.randomBytes(3).toString("hex")}`;
    const name = `Отложено Ozon ${date.day}.${date.month} · Магний B6 · ${selected.length} заказов`;
    await client.query(
      `insert into ozon_assemblies (
         id, name, created_by, access_mode, store_id, store_name,
         warehouse_id, warehouse_name, location_code, sklad_warehouse_id, sklad_warehouse_name,
         labels_status, labels_total, labels_loaded,
         employee_workflow_mode, employee_workflow_stage, stock_method_enabled,
         allow_no_scan, desktop_print, allow_stock_shortage, partial_safe_scan,
         created_at, updated_at
       ) values (
         $1, $2, 2, 'hidden', $3, $4,
         $5, $6, 'moscow', 38, 'Москва 2',
         'loading', $7, 0,
         'picking', 'orders_only', true,
         false, true, true, false,
         now(), now()
       )`,
      [newId, name, store.id, store.name, store.warehouseId, store.warehouseName, selected.length]
    );

    for (const source of selected) {
      await insertObject(client, "ozon_assembly_orders", {
        ...source,
        assembly_id: newId,
        assigned_user_id: null,
        assigned_at: null,
        scan_passed_at: null,
        scan_passed_by: null,
        scan_barcode: null,
        label_scan_passed_at: null,
        label_scan_passed_by: null,
        label_scan_barcode: null,
        collected_at: null,
        collected_by: null,
        sent_to_delivering_at: null,
        sent_error: null,
        collected_session_id: null,
        assignment_part_id: null,
        updated_at: new Date(),
      });
    }
    const selectedItems = itemsBefore.rows.filter((item) => selectedSet.has(item.posting_number));
    for (const source of selectedItems) {
      await insertObject(client, "ozon_assembly_order_items", { ...source, assembly_id: newId, scanned_qty: 0, updated_at: new Date() });
    }
    const labelsLoaded = selected.filter((order) => order.label_s3_key).length;
    await client.query(
      `update ozon_assemblies
          set labels_loaded = $2,
              labels_status = case when $2 = labels_total then 'ready' else 'pending' end,
              labels_finished_at = case when $2 = labels_total then now() else null end,
              updated_at = now()
        where id = $1`,
      [newId, labelsLoaded]
    );
    created.push({
      id: newId,
      name,
      storeId: store.id,
      orders: selected.length,
      orderUnits: selectedItems.reduce((sum, item) => sum + Number(item.quantity || 0), 0),
      magnesiumUnits: selectedItems.filter(isMagnesiumItem).reduce((sum, item) => sum + Number(item.quantity || 0), 0),
      labelsLoaded,
    });
  }

  for (const assemblyId of sourceAssemblyIds) {
    const countResult = await client.query(`select count(*)::int as count from ozon_assembly_orders where assembly_id = $1`, [assemblyId]);
    const remaining = Number(countResult.rows[0]?.count || 0);
    await client.query(
      `update ozon_assemblies
          set name = regexp_replace(name, ' · [0-9]+ заказов.*$', '') || ' · ' || $2::text || ' заказов · без Магния B6',
              labels_total = $2::int,
              labels_loaded = (select count(*)::int from ozon_assembly_orders where assembly_id = $1 and label_s3_key is not null),
              updated_at = now()
        where id = $1`,
      [assemblyId, remaining]
    );
  }

  await client.query("commit");
  fs.writeFileSync(path.join(backupDir, "created.json"), JSON.stringify(created, null, 2), { mode: 0o600 });
  console.log(JSON.stringify({ backupDir, created }, null, 2));
} catch (error) {
  await client.query("rollback").catch(() => {});
  throw error;
} finally {
  client.release();
  await db.end();
}
