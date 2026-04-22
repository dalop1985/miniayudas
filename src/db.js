const sql = require("mssql");

let poolPromise;

function normalizeBool(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  return ["true", "1", "yes", "y"].includes(String(value).toLowerCase());
}

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

function getDbConfig() {
  const engine = (process.env.DB_ENGINE || "mssql").toLowerCase();
  if (engine !== "mssql") {
    throw new Error(`DB_ENGINE no soportado: ${process.env.DB_ENGINE}`);
  }

  const server = process.env.DB_HOST || process.env.DB_SERVER;
  const database = process.env.DB_NAME || process.env.DB_DATABASE || "Tulum";
  const user = process.env.DB_USER;
  const password = process.env.DB_PASSWORD;
  const port = process.env.DB_PORT ? Number(process.env.DB_PORT) : undefined;
  const extra = parseExtraParams(process.env.DB_EXTRA_PARAMS);

  if (!server) throw new Error("Falta DB_HOST o DB_SERVER");
  if (!user) throw new Error("Falta DB_USER");
  if (!password) throw new Error("Falta DB_PASSWORD");

  const encrypt = normalizeBool(extra.encrypt, normalizeBool(process.env.DB_ENCRYPT, true));
  const trustServerCertificate = normalizeBool(
    extra.trustservercertificate,
    normalizeBool(process.env.DB_TRUST_SERVER_CERT, true)
  );
  const tlsServerName = process.env.DB_TLS_SERVER_NAME || "";

  const options = {
    encrypt,
    trustServerCertificate
  };

  if (tlsServerName) {
    options.serverName = tlsServerName;
    options.cryptoCredentialsDetails = { servername: tlsServerName };
  } else if (encrypt && isIpAddress(server)) {
    const fallbackServerName = "sqlserver";
    options.serverName = fallbackServerName;
    options.cryptoCredentialsDetails = { servername: fallbackServerName };
  }

  return {
    server,
    user,
    password,
    database,
    port,
    options,
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000
    }
  };
}

async function getPool() {
  if (!poolPromise) {
    poolPromise = new sql.ConnectionPool(getDbConfig()).connect().catch((error) => {
      poolPromise = undefined;
      throw error;
    });
  }
  return poolPromise;
}

module.exports = { sql, getDbConfig, getPool };
