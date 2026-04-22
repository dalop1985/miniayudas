require("dotenv").config();

const { getPool } = require("../db");

async function main() {
  const table = process.argv[2];
  if (!table) {
    process.stdout.write("Uso: node src/tools/db_introspect.js <TABLE_NAME>\n");
    process.exit(2);
  }

  const pool = await getPool();
  const r = await pool
    .request()
    .input("t", table)
    .query(
      "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@t ORDER BY ORDINAL_POSITION"
    );

  process.stdout.write(`TABLE ${table} cols ${r.recordset.length}\n`);
  for (const c of r.recordset) {
    process.stdout.write(`${c.COLUMN_NAME}:${c.DATA_TYPE}\n`);
  }
}

main().catch((e) => {
  process.stderr.write(String(e && e.message ? e.message : e) + "\n");
  process.exit(1);
});

