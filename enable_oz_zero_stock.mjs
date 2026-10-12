import fs from "node:fs";
import { loadEnv, mainDbConfig, connectAuto } from "/opt/grasklad-sborka/scripts/db-common.mjs";

const assemblyId = "ozon-1788780004984-evgeny-86c001";
loadEnv("/opt/grasklad-sborka/.env");
const pool = await connectAuto(mainDbConfig());
const client = await pool.connect();
try {
  await client.query("begin");
  const beforeResult = await client.query(
    `select id, name, store_id, allow_stock_shortage, updated_at
       from ozon_assemblies
      where id = $1
      for update`,
    [assemblyId]
  );
  if (beforeResult.rowCount !== 1) throw new Error(`Assembly not found: ${assemblyId}`);
  const before = beforeResult.rows[0];
  const backupPath = `/opt/grasklad-sborka/backups/${assemblyId}-allow-stock-shortage-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
  fs.writeFileSync(backupPath, JSON.stringify({ createdAt: new Date().toISOString(), before }, null, 2));
  const afterResult = await client.query(
    `update ozon_assemblies
        set allow_stock_shortage = true,
            updated_at = now()
      where id = $1
      returning id, name, store_id, allow_stock_shortage, updated_at`,
    [assemblyId]
  );
  await client.query("commit");
  console.log(JSON.stringify({ backupPath, before, after: afterResult.rows[0] }, null, 2));
} catch (error) {
  await client.query("rollback").catch(() => {});
  throw error;
} finally {
  client.release();
  await pool.end();
}
