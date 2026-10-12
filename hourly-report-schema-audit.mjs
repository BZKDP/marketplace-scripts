import { loadEnv, mainDbConfig, connectAuto } from "/opt/grasklad-sborka/scripts/db-common.mjs";

loadEnv("/opt/grasklad-sborka/.env");
const pool = await connectAuto(mainDbConfig());
try {
  const tables = [
    "ozon_assemblies",
    "ozon_assembly_orders",
    "ozon_assembly_order_items",
    "supply_settings",
    "supply_orders",
    "ym_assemblies",
    "ym_orders",
    "ym_order_items",
  ];
  const result = await pool.query(
    `select table_name, column_name, data_type
       from information_schema.columns
      where table_schema = 'public' and table_name = any($1::text[])
      order by table_name, ordinal_position`,
    [tables]
  );
  for (const table of tables) {
    const columns = result.rows
      .filter((row) => row.table_name === table)
      .map((row) => `${row.column_name}:${row.data_type}`);
    console.log(`${table} => ${columns.join(", ")}`);
  }
} finally {
  await pool.end();
}
