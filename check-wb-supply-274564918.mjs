import { loadEnv, connectAuto, mainDbConfig } from "/opt/grasklad-sborka/scripts/db-common.mjs";

loadEnv("/opt/grasklad-sborka/.env");
const pool = await connectAuto(mainDbConfig());
const supplyId = "WB-GI-274564918";
try {
  const orders = await pool.query(
    `select count(*)::int as total,
            count(*) filter (where collected_at is not null)::int as collected,
            count(*) filter (where collected_at is null)::int as open,
            count(*) filter (where collected_at is null and assigned_user_id is not null)::int as open_assigned
       from supply_orders where supply_id = $1`,
    [supplyId]
  );
  const picks = await pool.query(
    `select coalesce(sum(required_qty), 0)::int as required,
            coalesce(sum(picked_qty), 0)::int as picked,
            count(*) filter (where picked_qty < required_qty)::int as incomplete_rows
       from employee_supply_pick_items where supply_id = $1`,
    [supplyId]
  );
  const settings = await pool.query(
    `select * from supply_settings where supply_id = $1`,
    [supplyId]
  );
  console.log(JSON.stringify({ orders: orders.rows[0], picks: picks.rows[0], settings: settings.rows[0] || null }, null, 2));
} finally {
  await pool.end();
}
