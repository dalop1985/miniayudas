require('dotenv').config();
const { getPool } = require('../db');

(async () => {
  try {
    const pool = await getPool();
    const table = 'ALPREDIOVALUOCATASTRAL';
    const r = await pool.request().query(`
      SELECT COLUMN_NAME, DATA_TYPE 
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME='${table}' 
      ORDER BY COLUMN_NAME
    `);
    console.log(`Table: ${table}`);
    r.recordset.forEach(c => console.log(`${c.COLUMN_NAME}: ${c.DATA_TYPE}`));
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
