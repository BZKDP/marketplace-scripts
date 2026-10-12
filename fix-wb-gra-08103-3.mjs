import { loadEnv, mainDbConfig, skladDbConfig, connectAuto } from "/opt/grasklad-sborka/scripts/db-common.mjs";
import { addWarehouseProductBarcode } from "/opt/grasklad-sborka/apps/WB/server/services/sklad-movements.mjs";

loadEnv("/opt/grasklad-sborka/.env");

const article = "GRA-08103_3";
const APPLY = process.argv.includes("--apply");

function norm(value) {
  return String(value ?? "").trim().toUpperCase();
}

function words(value) {
  return new Set(
    String(value || "")
      .toLowerCase()
      .replace(/[^0-9a-zа-яё]+/giu, " ")
      .split(/\s+/)
      .filter((x) => x.length >= 3)
  );
}

function similarity(a, b) {
  const aw = words(a);
  const bw = words(b);
  if (!aw.size || !bw.size) return 0;
  let hit = 0;
  for (const w of aw) if (bw.has(w)) hit += 1;
  return hit / Math.max(aw.size, bw.size);
}

const main = await connectAuto(mainDbConfig());
const sklad = await connectAuto(skladDbConfig());

try {
  const orders = await main.query(
    `select so.supply_id, ss.store_name, ss.employee_workflow_stage,
            so.wb_order_id, so.article, so.barcode, so.product_name, so.nm_id, so.quantity,
            so.scan_passed_at, so.label_scan_passed_at
       from supply_orders so
       left join supply_settings ss on ss.supply_id = so.supply_id
      where upper(coalesce(so.article,'')) = upper($1)
      order by so.synced_at desc nulls last, so.order_created_at desc nulls last
      limit 100`,
    [article]
  );

  const pickItems = await main.query(
    `select i.id, i.supply_id, i.session_id, i.article, i.barcode, i.product_name, i.nm_id,
            i.required_qty, i.picked_qty, i.bypass_without_scan_allowed, s.user_id, s.picking_status
       from employee_supply_pick_items i
       left join employee_supply_sessions s on s.id = i.session_id
      where upper(coalesce(i.article,'')) = upper($1)
      order by i.updated_at desc nulls last
      limit 100`,
    [article]
  );

  const exactProducts = await sklad.query(
    `select id, article, code, name, production_barcode, barcode_list, marketplace_barcodes_json, archived
       from products_s
      where upper(coalesce(article,'')) = upper($1)
         or upper(coalesce(code,'')) = upper($1)
      order by archived asc, id asc`,
    [article]
  );

  const rows = [...orders.rows, ...pickItems.rows];
  const barcodes = [...new Set(rows.map((r) => String(r.barcode || "").trim()).filter(Boolean))];
  const names = [...new Set(rows.map((r) => String(r.product_name || "").trim()).filter(Boolean))];

  let target = exactProducts.rows.find((r) => !r.archived) || null;
  let targetSource = target ? "exact_article" : null;

  if (!target && names.length) {
    const candidateRows = await sklad.query(
      `select id, article, code, name, production_barcode, barcode_list, marketplace_barcodes_json, archived
         from products_s
        where coalesce(archived, false) = false
        order by id`,
      []
    );
    const scored = candidateRows.rows
      .map((p) => ({ p, score: Math.max(...names.map((n) => similarity(n, p.name))) }))
      .filter((x) => x.score >= 0.75)
      .sort((a, b) => b.score - a.score);
    if (scored.length === 1 || (scored[0] && scored[0].score > (scored[1]?.score || 0) + 0.15)) {
      target = scored[0].p;
      targetSource = `name_similarity_${scored[0].score.toFixed(3)}`;
    }
  }

  const result = {
    apply: APPLY,
    article,
    orderRows: orders.rows.length,
    pickRows: pickItems.rows.length,
    barcodes,
    names,
    exactProducts: exactProducts.rows.map((p) => ({
      id: p.id,
      article: p.article,
      code: p.code,
      name: p.name,
      archived: p.archived,
      barcode_list: p.barcode_list,
      marketplace_barcodes_json: p.marketplace_barcodes_json,
    })),
    target: target
      ? {
          id: target.id,
          article: target.article,
          code: target.code,
          name: target.name,
          archived: target.archived,
          barcode_list: target.barcode_list,
          marketplace_barcodes_json: target.marketplace_barcodes_json,
          source: targetSource,
        }
      : null,
    applied: [],
    errors: [],
  };

  if (APPLY) {
    if (!target) throw new Error("No unique warehouse product target");
    if (barcodes.length !== 1) throw new Error(`Expected exactly one WB barcode, got ${barcodes.length}: ${barcodes.join(", ")}`);
    const before = await sklad.query("select * from products_s where id = $1", [target.id]);
    result.before = before.rows[0] || null;
    for (const barcode of barcodes) {
      try {
        result.applied.push(await addWarehouseProductBarcode({ productId: target.id, barcode }));
      } catch (error) {
        result.errors.push({ barcode, message: error.message });
      }
    }
    const after = await sklad.query("select id, article, code, name, barcode_list, marketplace_barcodes_json from products_s where id = $1", [target.id]);
    result.after = after.rows[0] || null;
  }

  console.log(JSON.stringify(result, null, 2));
} finally {
  await main.end().catch(() => {});
  await sklad.end().catch(() => {});
}
