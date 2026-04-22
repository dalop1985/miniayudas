require('dotenv').config();
const { getPool } = require('../db');

(async () => {
  try {
    const pool = await getPool();
    const table = 'AlPredio';
    const r = await pool.request().query(`
      SELECT COLUMN_NAME, DATA_TYPE 
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME='${table}' 
        AND (COLUMN_NAME LIKE '%Terreno%' 
          OR COLUMN_NAME LIKE '%Construccion%' 
          OR COLUMN_NAME LIKE '%Area%' 
          OR COLUMN_NAME LIKE '%Valor%' 
          OR COLUMN_NAME LIKE '%Catastral%') 
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
