import { connectAuto, loadEnv, mainDbConfig, skladDbConfig } from "./db-common.mjs";

const norm = (value) => String(value || "").replace(/\s+/g, "").trim().toUpperCase();

loadEnv();
const main = await connectAuto(mainDbConfig());
const sklad = await connectAuto(skladDbConfig());

try {
  const suppliesResult = await main.query(
    `select ss.supply_id, ss.supply_name, ss.store_name, ss.employee_workflow_stage,
            ss.access_mode, ss.allow_no_scan, ss.partial_safe_scan, ss.allow_stock_shortage,
            ss.wb_supply_done, ss.created_at,
            count(so.wb_order_id)::int as orders,
            count(so.wb_order_id) filter (where so.collected_at is null)::int as open_orders,
            coalesce(sum(greatest(coalesce(so.quantity, 1), 1)) filter (where so.collected_at is null), 0)::int as open_units
       from supply_settings ss
       join supply_orders so on so.supply_id = ss.supply_id
      where coalesce(ss.wb_supply_done, false) = false
        and ss.created_at >= now() - interval '14 days'
        and (ss.access_mode = 'selected_split' or exists (
          select 1 from employee_supply_sessions es
           where es.supply_id = ss.supply_id and es.finished_at is null
        ))
      group by ss.supply_id
     having count(so.wb_order_id) filter (where so.collected_at is null) > 0
      order by ss.created_at desc`,
    []
  );
  const rowsResult = await main.query(
    `select so.supply_id, so.article, so.barcode, max(so.product_name) as product_name,
            max(so.nm_id)::bigint as nm_id,
            sum(greatest(coalesce(so.quantity, 1), 1))::int as quantity
       from supply_orders so
       join supply_settings ss on ss.supply_id = so.supply_id
      where coalesce(ss.wb_supply_done, false) = false
        and ss.created_at >= now() - interval '14 days'
        and (ss.access_mode = 'selected_split' or exists (
          select 1 from employee_supply_sessions es
           where es.supply_id = ss.supply_id and es.finished_at is null
        ))
        and so.collected_at is null
      group by so.supply_id, so.article, so.barcode
      order by so.supply_id, so.article`,
    []
  );
  const productsResult = await sklad.query(
    `select id, article, code, name, entity_type, production_barcode, barcode_list, marketplace_barcodes_json
       from products_s
      where coalesce(archived, false) = false`,
    []
  );
  const products = productsResult.rows || [];
  const byToken = new Map();
  const byBarcode = new Map();
  const remember = (map, key, product) => {
    if (!key) return;
    if (!map.has(key)) map.set(key, new Map());
    map.get(key).set(Number(product.id), product);
  };
  for (const product of products) {
    for (const token of [product.article, product.code]) {
      const key = norm(token);
      if (!key) continue;
      remember(byToken, key, product);
    }
    const barcodes = [product.production_barcode, ...String(product.barcode_list || '').split(';')];
    const mp = Array.isArray(product.marketplace_barcodes_json) ? product.marketplace_barcodes_json : [];
    for (const entry of mp) barcodes.push(entry?.value);
    for (const value of barcodes) {
      const key = norm(value);
      if (!key) continue;
      remember(byBarcode, key, product);
    }
  }

  const issues = [];
  for (const row of rowsResult.rows || []) {
    const articleMatches = [...(byToken.get(norm(row.article))?.values() || [])];
    const barcodeMatches = [...(byBarcode.get(norm(row.barcode))?.values() || [])];
    const exactSame = articleMatches.length === 1 && barcodeMatches.some((item) => Number(item.id) === Number(articleMatches[0].id));
    if (exactSame || (barcodeMatches.length === 1 && articleMatches.length === 0)) continue;
    issues.push({
      supplyId: row.supply_id,
      article: row.article,
      barcode: row.barcode,
      productName: row.product_name,
      nmId: row.nm_id,
      quantity: Number(row.quantity || 0),
      articleMatches: articleMatches.map((item) => ({ id: item.id, article: item.article, name: item.name, entityType: item.entity_type })),
      barcodeMatches: barcodeMatches.map((item) => ({ id: item.id, article: item.article, name: item.name, entityType: item.entity_type })),
      fixable: articleMatches.length === 1 && barcodeMatches.length === 0,
    });
  }
  const picksResult = await main.query(
    `select pi.supply_id, pi.id, pi.session_id, pi.user_id, pi.article, pi.barcode,
            pi.product_name, pi.required_qty, pi.picked_qty, pi.bypass_without_scan_allowed
       from employee_supply_pick_items pi
       join supply_settings ss on ss.supply_id = pi.supply_id
      where coalesce(ss.wb_supply_done, false) = false
        and ss.created_at >= now() - interval '14 days'
        and ss.access_mode = 'selected_split'
        and pi.picked_qty < pi.required_qty
      order by pi.supply_id, pi.user_id, pi.required_qty desc, pi.article`,
    []
  );
  const recentFailedItem = await main.query(
    `select pi.*, es.picking_status, es.picking_started_at, es.picking_finished_at,
            ss.stock_method_enabled, ss.allow_no_scan, ss.allow_stock_shortage
       from employee_supply_pick_items pi
       join employee_supply_sessions es on es.id = pi.session_id
       join supply_settings ss on ss.supply_id = pi.supply_id
      where pi.id = 23411`,
    []
  );
  const scanIssues = await main.query(
    `select i.id, i.supply_id, i.issue_stage, i.marketplace_article, i.warehouse_product_id,
            i.warehouse_article, i.warehouse_product_name, i.original_scanned_barcode,
            i.confirmation_scanned_barcode, i.status, i.created_at
       from employee_scan_issues i
       join supply_settings ss on ss.supply_id = i.supply_id
      where ss.created_at >= now() - interval '14 days'
        and i.created_at >= now() - interval '2 days'
      order by i.created_at desc
      limit 100`,
    []
  );
  console.log(JSON.stringify({ supplies: suppliesResult.rows, issueCount: issues.length, issues, remainingPicks: picksResult.rows, recentFailedItem: recentFailedItem.rows, scanIssues: scanIssues.rows }, null, 2));
} finally {
  await Promise.allSettled([main.end(), sklad.end()]);
}
