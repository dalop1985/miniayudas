"use strict";

const sql = require("mssql");

let poolPromise;

function parseExtraParams(raw) {
  const params = {};
  if (!raw) return params;
  for (const item of String(raw).split(";")) {
    const chunk = item.trim();
    if (!chunk) continue;
    const [key, ...rest] = chunk.split("=");
    params[key.trim().toLowerCase()] = rest.join("=").trim();
  }
  return params;
}

function isIpAddress(value) {
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(String(value || ""));
}

function boolish(value, defaultValue) {
  if (value === undefined || value === null || String(value).trim() === "") return !!defaultValue;
  return ["true", "1", "yes", "y", "si", "sí"].includes(String(value).trim().toLowerCase());
}

function envInt(name, fallback) {
  const raw = process.env[name];
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function getDbConfig() {
  const engine = String(process.env.DB_ENGINE || "mssql").trim().toLowerCase();
  if (engine !== "mssql") {
    throw new Error(`DB_ENGINE no soportado: ${engine}`);
  }

  const host = String(process.env.DB_HOST || "").trim();
  const name = String(process.env.DB_NAME || "").trim();
  const user = String(process.env.DB_USER || "").trim();
  const password = String(process.env.DB_PASSWORD || "");
  const port = envInt("DB_PORT", 1433);
  const extraParams = String(process.env.DB_EXTRA_PARAMS || "");
  const trustServerCertificateDefault = boolish(process.env.DB_TRUST_SERVER_CERT, true);
  const tlsServerName = String(process.env.DB_TLS_SERVER_NAME || "").trim();

  if (!host) throw new Error("Falta DB_HOST");
  if (!name) throw new Error("Falta DB_NAME");
  if (!user) throw new Error("Falta DB_USER");
  if (!password) throw new Error("Falta DB_PASSWORD");

  const extra = parseExtraParams(extraParams);
  const encryptInExtra = extra.encrypt;
  const trustInExtra = extra.trustservercertificate;

  const encrypt =
    encryptInExtra !== undefined
      ? ["yes", "true", "1"].includes(String(encryptInExtra).toLowerCase())
      : true;
  const trustServerCertificate =
    trustInExtra !== undefined
      ? ["yes", "true", "1"].includes(String(trustInExtra).toLowerCase())
      : trustServerCertificateDefault;

  const options = {
    encrypt,
    trustServerCertificate,
  };

  if (tlsServerName) {
    options.serverName = tlsServerName;
    options.cryptoCredentialsDetails = { servername: tlsServerName };
  } else if (encrypt && isIpAddress(host)) {
    const fallbackServerName = "sqlserver";
    options.serverName = fallbackServerName;
    options.cryptoCredentialsDetails = { servername: fallbackServerName };
  }

  return {
    server: host,
    user,
    password,
    database: name,
    port,
    options,
    requestTimeout: 60000,
    connectionTimeout: 15000,
    pool: {
      max: 15,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  };
}

async function getPool() {
  if (!poolPromise) {
    poolPromise = new sql.ConnectionPool(getDbConfig())
      .connect()
      .catch((error) => {
        poolPromise = undefined;
        throw error;
      });
  }
  return poolPromise;
}

async function closePool() {
  if (!poolPromise) return;
  try {
    const pool = await poolPromise;
    await pool.close();
  } catch (e) {
    /* swallow */
  } finally {
    poolPromise = undefined;
  }
}

module.exports = { sql, getDbConfig, getPool, closePool };
