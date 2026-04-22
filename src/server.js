require("dotenv").config();

const express = require("express");
const path = require("path");
const { sql, getDbConfig, getPool } = require("./db");

const app = express();

app.use(express.json({ limit: "256kb" }));
app.use(
  "/vendor/react",
  express.static(path.join(__dirname, "..", "node_modules", "react", "umd"))
);
app.use(
  "/vendor/react-dom",
  express.static(path.join(__dirname, "..", "node_modules", "react-dom", "umd"))
);
app.use(
  "/vendor/babel",
  express.static(path.join(__dirname, "..", "node_modules", "@babel", "standalone"))
);
app.use(express.static(path.join(__dirname, "..", "public")));

function parseIntStrict(value, label) {
  const n = Number(value);
  if (!Number.isInteger(n)) throw new Error(`${label} inválido`);
  return n;
}

function requireAdmin(req) {
  const expected = process.env.ADMIN_KEY;
  if (!expected) {
    const err = new Error("ADMIN_KEY no configurada en el servidor");
    err.status = 503;
    throw err;
  }
  const provided = req.get("x-admin-key");
  if (!provided || provided !== expected) {
    const err = new Error("No autorizado");
    err.status = 401;
    throw err;
  }
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/test-connection", async (_req, res) => {
  try {
    const cfg = getDbConfig();
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT
        DB_NAME() AS databaseName,
        @@SERVERNAME AS serverName,
        SUSER_SNAME() AS loginName,
        GETDATE() AS serverTime
    `);

    res.json({
      ok: true,
      message: "Conexion exitosa",
      target: {
        server: cfg.server,
        port: cfg.port,
        database: cfg.database,
        encrypt: !!cfg.options?.encrypt,
        trustServerCertificate: !!cfg.options?.trustServerCertificate,
        serverName: cfg.options?.serverName || null
      },
      connection: result.recordset[0]
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: e.message
    });
  }
});

app.get("/api/fuentes", async (req, res) => {
  try {
    const solicitudId = parseIntStrict(req.query.solicitudId, "solicitudId");
    const ano = parseIntStrict(req.query.ano, "ano");
    const grupoTramiteId = req.query.grupoTramiteId
      ? parseIntStrict(req.query.grupoTramiteId, "grupoTramiteId")
      : 42;
    const cveFteMT = (req.query.cveFteMT || "MTULUM").toString();

    const pool = await getPool();
    const request = pool.request();
    request.input("GrupoTramiteId", sql.Int, grupoTramiteId);
    request.input("SolicitudId", sql.Int, solicitudId);
    request.input("Ano", sql.Int, ano);
    request.input("CveFteMT", sql.VarChar(32), cveFteMT);

    const result = await request.query(`
      SELECT
        GrupoTramiteId,
        SolicitudId,
        SolicitudDetalleEjericicio,
        SolicitudDetalleFteIngId,
        CveFteMT,
        SolicitudDetalleImporteFijo
      FROM TLSOLICITUDFUENTESINGRESO
      WHERE GrupoTramiteId = @GrupoTramiteId
        AND SolicitudId = @SolicitudId
        AND SolicitudDetalleEjericicio = @Ano
        AND CveFteMT = @CveFteMT
      ORDER BY SolicitudDetalleFteIngId ASC
    `);

    res.json({
      ok: true,
      filtros: { grupoTramiteId, solicitudId, ano, cveFteMT },
      count: result.recordset.length,
      rows: result.recordset
    });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.get("/api/reportes/prediales/sabana", async (req, res) => {
  try {
    const cveFteMT = (req.query.cveFteMT || "MTULUM").toString();
    const q = req.query.q ? req.query.q.toString().trim() : "";
    const limitRaw = req.query.limit ? Number(req.query.limit) : 200;
    const offsetRaw = req.query.offset ? Number(req.query.offset) : 0;

    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.trunc(limitRaw))) : 200;
    const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.trunc(offsetRaw)) : 0;

    const fromAlta = req.query.fromAlta ? new Date(req.query.fromAlta.toString()) : null;
    const toAlta = req.query.toAlta ? new Date(req.query.toAlta.toString()) : null;

    const pool = await getPool();
    const request = pool.request();
    request.input("CveFteMT", sql.VarChar(32), cveFteMT);
    request.input("Q", sql.VarChar(200), q ? q : null);
    request.input("FromAlta", sql.DateTime, fromAlta && !Number.isNaN(fromAlta.getTime()) ? fromAlta : null);
    request.input("ToAlta", sql.DateTime, toAlta && !Number.isNaN(toAlta.getTime()) ? toAlta : null);
    request.input("Limit", sql.Int, limit + 1);
    request.input("Offset", sql.Int, offset);

    const result = await request.query(`
      SELECT
        p.PredioId,
        p.PredioCveCatastral,
        p.PredioClavePredial,
        p.PredioStatus,
        p.PredioAltaFecha,
        p.ZonaId,
        p.PredioCalle,
        p.PredioNumExt,
        p.PredioNumInt,
        p.PredioColoniaId,
        p.PredioCodigoPostal,
        p.PredioArea,
        p.PredioTotalTerreno,
        p.PredioTotalConstruccion,
        p.PredioValorM2,
        p.PredioTerrenoImporte,
        p.PredioConstruccionImporte,
        p.PredioCatastralImporte,
        p.PredioFiscalImporte,
        p.PredioActualAdeudoImporte,
        p.PredioFechaUltimoPago,
        p.PredioUltimoEjericicioPagado,
        p.PredioUltimoPeriodoPagado,
        per.NombreCompletoPersona AS PropietarioNombre,
        per.RFCPersona AS PropietarioRFC,
        per.CURPPersona AS PropietarioCURP,
        terr.TerrenoAreaSum,
        terr.TerrenoImporteSum,
        cons.ConstruccionAreaSum,
        cons.NumConstrucciones,
        val.PredioValuoCatastralEjercicio,
        val.PredioValuoCatastralFecha,
        val.PredioValuoCatastralImporte,
        recibo.ReciboEjercicio,
        recibo.ReciboPeriodo,
        recibo.ReciboSerie,
        recibo.ReciboFolio,
        recibo.ReciboImporte,
        mov.UltMovFecha,
        mov.UltMovReferenciaId,
        mov.UltMovObservaciones
      FROM AlPredio p
      LEFT JOIN XiPersonas per
        ON per.CveFteMT = p.CveFteMT
       AND per.CvePersona = p.CvePersona
      OUTER APPLY (
        SELECT
          SUM(t.PredioTerrenoArea) AS TerrenoAreaSum,
          SUM(t.PredioTerrenoArea * t.PredioTerrenoUnitarioImporte) AS TerrenoImporteSum
        FROM ALPREDIOTERRENOS t
        WHERE t.CveFteMT = p.CveFteMT
          AND t.PredioId = p.PredioId
      ) terr
      OUTER APPLY (
        SELECT
          SUM(c.PredioConstruccionArea) AS ConstruccionAreaSum,
          COUNT(1) AS NumConstrucciones
        FROM ALPREDIOCONSTRUCCIONES c
        WHERE c.CveFteMT = p.CveFteMT
          AND c.PredioId = p.PredioId
      ) cons
      OUTER APPLY (
        SELECT TOP 1
          v.PredioValuoCatastralEjercicio,
          v.PredioValuoCatastralFecha,
          v.PredioValuoCatastralImporte
        FROM ALPREDIOVALUOCATASTRAL v
        WHERE v.CveFteMT = p.CveFteMT
          AND v.PredioId = p.PredioId
        ORDER BY v.PredioValuoCatastralFecha DESC, v.PredioValuoCatastralEjercicio DESC
      ) val
      OUTER APPLY (
        SELECT TOP 1
          r.PredioEdoCuentaEjercicio AS ReciboEjercicio,
          r.PredioEdoCuentaPeriodo AS ReciboPeriodo,
          r.PredioEdoCuentaReciboSerie AS ReciboSerie,
          r.PredioEdoCuentaReciboFolio AS ReciboFolio,
          r.PredioEdoCuentaReciboImporte AS ReciboImporte
        FROM ALPREDIOEDOCUENTARECIBO r
        WHERE r.CveFteMT = p.CveFteMT
          AND r.PredioId = p.PredioId
        ORDER BY r.PredioEdoCuentaEjercicio DESC, r.PredioEdoCuentaPeriodo DESC, r.PredioEdoCuentaReciboId DESC
      ) recibo
      OUTER APPLY (
        SELECT TOP 1
          m.PredioEdoCuentaMovFecha AS UltMovFecha,
          m.PredioEdoCuentaMovReferenciaId AS UltMovReferenciaId,
          m.PredioEdoCuentaMovObservaciones AS UltMovObservaciones
        FROM ALPREDIOEDOCUENTAMOV m
        WHERE m.CveFteMT = p.CveFteMT
          AND m.PredioId = p.PredioId
        ORDER BY m.PredioEdoCuentaMovFecha DESC, m.PredioEdoCuentaEjercicio DESC, m.PredioEdoCuentaPeriodo DESC, m.PredioEdoCuentaMovId DESC
      ) mov
      WHERE p.CveFteMT = @CveFteMT
        AND (@FromAlta IS NULL OR p.PredioAltaFecha >= @FromAlta)
        AND (@ToAlta IS NULL OR p.PredioAltaFecha < DATEADD(DAY, 1, @ToAlta))
        AND (
          @Q IS NULL OR
          p.PredioCveCatastral LIKE '%' + @Q + '%' OR
          p.PredioClavePredial LIKE '%' + @Q + '%' OR
          per.NombreCompletoPersona LIKE '%' + @Q + '%'
        )
      ORDER BY p.PredioId ASC
      OFFSET @Offset ROWS
      FETCH NEXT @Limit ROWS ONLY;
    `);

    const data = result.recordset || [];
    const hasMore = data.length > limit;
    const rows = hasMore ? data.slice(0, limit) : data;

    res.json({
      ok: true,
      filtros: { cveFteMT, q, fromAlta: req.query.fromAlta || null, toAlta: req.query.toAlta || null, limit, offset },
      count: rows.length,
      hasMore,
      nextOffset: hasMore ? offset + limit : null,
      rows
    });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) {
    const t = value.getTime();
    if (!Number.isNaN(t)) return value.toISOString();
  }
  const s = String(value);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

app.get("/api/reportes/prediales/sabana.csv", async (req, res) => {
  try {
    const cveFteMT = (req.query.cveFteMT || "MTULUM").toString();
    const q = req.query.q ? req.query.q.toString().trim() : "";
    const fromAlta = req.query.fromAlta ? new Date(req.query.fromAlta.toString()) : null;
    const toAlta = req.query.toAlta ? new Date(req.query.toAlta.toString()) : null;
    const maxRowsRaw = req.query.maxRows ? Number(req.query.maxRows) : 50000;
    const maxRows = Number.isFinite(maxRowsRaw) ? Math.max(1, Math.min(200000, Math.trunc(maxRowsRaw))) : 50000;

    const columns = [
      "PredioId",
      "PredioCveCatastral",
      "PredioClavePredial",
      "PredioStatus",
      "PredioAltaFecha",
      "ZonaId",
      "PredioCalle",
      "PredioNumExt",
      "PredioNumInt",
      "PredioColoniaId",
      "PredioCodigoPostal",
      "PredioArea",
      "PredioTotalTerreno",
      "PredioTotalConstruccion",
      "PredioValorM2",
      "PredioTerrenoImporte",
      "PredioConstruccionImporte",
      "PredioCatastralImporte",
      "PredioFiscalImporte",
      "PredioActualAdeudoImporte",
      "PredioFechaUltimoPago",
      "PredioUltimoEjericicioPagado",
      "PredioUltimoPeriodoPagado",
      "PropietarioNombre",
      "PropietarioRFC",
      "PropietarioCURP",
      "TerrenoAreaSum",
      "TerrenoImporteSum",
      "ConstruccionAreaSum",
      "NumConstrucciones",
      "PredioValuoCatastralEjercicio",
      "PredioValuoCatastralFecha",
      "PredioValuoCatastralImporte",
      "ReciboEjercicio",
      "ReciboPeriodo",
      "ReciboSerie",
      "ReciboFolio",
      "ReciboImporte",
      "UltMovFecha",
      "UltMovReferenciaId",
      "UltMovObservaciones"
    ];

    res.setHeader("content-type", "text/csv; charset=utf-8");
    res.setHeader("content-disposition", `attachment; filename="prediales_sabana.csv"`);
    res.write(`${columns.join(",")}\n`);

    const pool = await getPool();
    const request = pool.request();
    request.stream = true;
    request.input("CveFteMT", sql.VarChar(32), cveFteMT);
    request.input("Q", sql.VarChar(200), q ? q : null);
    request.input("FromAlta", sql.DateTime, fromAlta && !Number.isNaN(fromAlta.getTime()) ? fromAlta : null);
    request.input("ToAlta", sql.DateTime, toAlta && !Number.isNaN(toAlta.getTime()) ? toAlta : null);
    request.input("MaxRows", sql.Int, maxRows);

    request.on("row", (row) => {
      const line = columns.map((c) => csvEscape(row[c])).join(",");
      res.write(`${line}\n`);
    });

    request.on("error", (err) => {
      res.destroy(err);
    });

    request.on("done", () => {
      res.end();
    });

    request.query(`
      SELECT TOP (@MaxRows)
        p.PredioId,
        p.PredioCveCatastral,
        p.PredioClavePredial,
        p.PredioStatus,
        p.PredioAltaFecha,
        p.ZonaId,
        p.PredioCalle,
        p.PredioNumExt,
        p.PredioNumInt,
        p.PredioColoniaId,
        p.PredioCodigoPostal,
        p.PredioArea,
        p.PredioTotalTerreno,
        p.PredioTotalConstruccion,
        p.PredioValorM2,
        p.PredioTerrenoImporte,
        p.PredioConstruccionImporte,
        p.PredioCatastralImporte,
        p.PredioFiscalImporte,
        p.PredioActualAdeudoImporte,
        p.PredioFechaUltimoPago,
        p.PredioUltimoEjericicioPagado,
        p.PredioUltimoPeriodoPagado,
        per.NombreCompletoPersona AS PropietarioNombre,
        per.RFCPersona AS PropietarioRFC,
        per.CURPPersona AS PropietarioCURP,
        terr.TerrenoAreaSum,
        terr.TerrenoImporteSum,
        cons.ConstruccionAreaSum,
        cons.NumConstrucciones,
        val.PredioValuoCatastralEjercicio,
        val.PredioValuoCatastralFecha,
        val.PredioValuoCatastralImporte,
        recibo.ReciboEjercicio,
        recibo.ReciboPeriodo,
        recibo.ReciboSerie,
        recibo.ReciboFolio,
        recibo.ReciboImporte,
        mov.UltMovFecha,
        mov.UltMovReferenciaId,
        mov.UltMovObservaciones
      FROM AlPredio p
      LEFT JOIN XiPersonas per
        ON per.CveFteMT = p.CveFteMT
       AND per.CvePersona = p.CvePersona
      OUTER APPLY (
        SELECT
          SUM(t.PredioTerrenoArea) AS TerrenoAreaSum,
          SUM(t.PredioTerrenoArea * t.PredioTerrenoUnitarioImporte) AS TerrenoImporteSum
        FROM ALPREDIOTERRENOS t
        WHERE t.CveFteMT = p.CveFteMT
          AND t.PredioId = p.PredioId
      ) terr
      OUTER APPLY (
        SELECT
          SUM(c.PredioConstruccionArea) AS ConstruccionAreaSum,
          COUNT(1) AS NumConstrucciones
        FROM ALPREDIOCONSTRUCCIONES c
        WHERE c.CveFteMT = p.CveFteMT
          AND c.PredioId = p.PredioId
      ) cons
      OUTER APPLY (
        SELECT TOP 1
          v.PredioValuoCatastralEjercicio,
          v.PredioValuoCatastralFecha,
          v.PredioValuoCatastralImporte
        FROM ALPREDIOVALUOCATASTRAL v
        WHERE v.CveFteMT = p.CveFteMT
          AND v.PredioId = p.PredioId
        ORDER BY v.PredioValuoCatastralFecha DESC, v.PredioValuoCatastralEjercicio DESC
      ) val
      OUTER APPLY (
        SELECT TOP 1
          r.PredioEdoCuentaEjercicio AS ReciboEjercicio,
          r.PredioEdoCuentaPeriodo AS ReciboPeriodo,
          r.PredioEdoCuentaReciboSerie AS ReciboSerie,
          r.PredioEdoCuentaReciboFolio AS ReciboFolio,
          r.PredioEdoCuentaReciboImporte AS ReciboImporte
        FROM ALPREDIOEDOCUENTARECIBO r
        WHERE r.CveFteMT = p.CveFteMT
          AND r.PredioId = p.PredioId
        ORDER BY r.PredioEdoCuentaEjercicio DESC, r.PredioEdoCuentaPeriodo DESC, r.PredioEdoCuentaReciboId DESC
      ) recibo
      OUTER APPLY (
        SELECT TOP 1
          m.PredioEdoCuentaMovFecha AS UltMovFecha,
          m.PredioEdoCuentaMovReferenciaId AS UltMovReferenciaId,
          m.PredioEdoCuentaMovObservaciones AS UltMovObservaciones
        FROM ALPREDIOEDOCUENTAMOV m
        WHERE m.CveFteMT = p.CveFteMT
          AND m.PredioId = p.PredioId
        ORDER BY m.PredioEdoCuentaMovFecha DESC, m.PredioEdoCuentaEjercicio DESC, m.PredioEdoCuentaPeriodo DESC, m.PredioEdoCuentaMovId DESC
      ) mov
      WHERE p.CveFteMT = @CveFteMT
        AND (@FromAlta IS NULL OR p.PredioAltaFecha >= @FromAlta)
        AND (@ToAlta IS NULL OR p.PredioAltaFecha < DATEADD(DAY, 1, @ToAlta))
        AND (
          @Q IS NULL OR
          p.PredioCveCatastral LIKE '%' + @Q + '%' OR
          p.PredioClavePredial LIKE '%' + @Q + '%' OR
          per.NombreCompletoPersona LIKE '%' + @Q + '%'
        )
      ORDER BY p.PredioId ASC;
    `);
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.get("/api/reportes/prediales/sabana-pagos", async (req, res) => {
  try {
    const cveFteMT = (req.query.cveFteMT || "MTULUM").toString();
    let claveCatastral = req.query.claveCatastral ? req.query.claveCatastral.toString().trim() : "";
    let claveCatastralFrom = req.query.claveCatastralFrom ? req.query.claveCatastralFrom.toString().trim() : "";
    let claveCatastralTo = req.query.claveCatastralTo ? req.query.claveCatastralTo.toString().trim() : "";
    let predioId = req.query.predioId ? Number(req.query.predioId) : null;
    const ejercicio = req.query.ejercicio ? Number(req.query.ejercicio) : null;
    const todos = ["true", "1", "yes", "y", "si", "sí"].includes(String(req.query.todos || "").toLowerCase());
    let pagoFrom = req.query.pagoFrom ? new Date(req.query.pagoFrom.toString()) : null;
    let pagoTo = req.query.pagoTo ? new Date(req.query.pagoTo.toString()) : null;

    if (ejercicio && Number.isFinite(ejercicio)) {
      pagoFrom = new Date(ejercicio, 0, 1);
      pagoTo = new Date(ejercicio, 11, 31);
    }
    if (todos) {
      claveCatastral = "";
      claveCatastralFrom = "";
      claveCatastralTo = "";
      predioId = null;
    }

    const limitRaw = req.query.limit ? Number(req.query.limit) : 200;
    const offsetRaw = req.query.offset ? Number(req.query.offset) : 0;
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(1000, Math.trunc(limitRaw))) : 200;
    const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.trunc(offsetRaw)) : 0;

    const pool = await getPool();
    const request = pool.request();
    request.input("CveFteMT", sql.VarChar(32), cveFteMT);
    request.input("ClaveCatastral", sql.VarChar(64), claveCatastral || null);
    request.input("ClaveCatastralFrom", sql.VarChar(64), claveCatastralFrom || null);
    request.input("ClaveCatastralTo", sql.VarChar(64), claveCatastralTo || null);
    request.input("PredioId", sql.Decimal(18, 0), Number.isFinite(predioId) ? predioId : null);
    request.input("PagoFrom", sql.DateTime, (pagoFrom && !isNaN(pagoFrom.getTime())) ? pagoFrom : null);
    request.input("PagoTo", sql.DateTime, (pagoTo && !isNaN(pagoTo.getTime())) ? pagoTo : null);
    request.input("Limit", sql.Int, limit + 1);
    request.input("Offset", sql.Int, offset);

    const query = `
      SELECT
        CAST(sp.PredioId AS int) AS Clave,
        '''' + COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), ''), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT), '') AS [Clave Catastral],
        RTRIM(COALESCE(per.NombreCompletoPersona COLLATE DATABASE_DEFAULT, '')) AS Propietario,
        RTRIM(COALESCE(per.RazonSocialPersona COLLATE DATABASE_DEFAULT, p.PredioContribuyenteNombre COLLATE DATABASE_DEFAULT, '')) AS [Razon social del contribuyente],
        RTRIM(COALESCE(p.PredioQroDireccion COLLATE DATABASE_DEFAULT, '')) AS Direccion,
        RTRIM(COALESCE(p.PredioNombreColonia COLLATE DATABASE_DEFAULT, '')) AS Colonia,
        RTRIM(p.PredioTipo) COLLATE DATABASE_DEFAULT AS [Tipo de Predio],
        CAST(COALESCE(p.PredioTerrenoImporte, 0) AS decimal(18,2)) AS [Valor del terreno],
        CAST(COALESCE(p.PredioTotalConstruccion, 0) AS decimal(18,2)) AS [Área construida],
        CAST(COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor de construcción],
        CAST(COALESCE(p.PredioTerrenoImporte, 0) + COALESCE(p.PredioTotalConstruccion, 0) + COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor catastral],
        RTRIM(COALESCE(cal.CalificativoPropietarioNombre COLLATE DATABASE_DEFAULT, '')) AS Calificativo,
        RTRIM(COALESCE(ef.EstadoFisicoNombre COLLATE DATABASE_DEFAULT, '')) AS [Estado fisico],
        CONCAT(sp.SPagoPredialInicialEjercicio, '- ', sp.SPagoPredialInicialPeriodo) AS [Periodo inicial],
        CONCAT(sp.SPagoPredialFinalEjercicio, '- ', sp.SPagoPredialFinalPeriodo) AS [Periodo final],
        RTRIM(sp.SPagoPredialSerie COLLATE DATABASE_DEFAULT) + ' ' + CAST(sp.SPagoPredialFolio AS varchar(20)) AS Recibo,
        sp.SPagoPredialPagoFecha AS [Fecha de Pago],
        calc.Impuesto AS [Impuesto Corriente y Anticipado],
        calc.RezagoAnt AS [Rezago años anteriores],
        calc.Rezago AS Rezago,
        calc.Adicional AS Adicional,
        calc.Actualizacion AS Actualizacion,
        calc.Recargos AS Recargos,
        CAST(0 AS decimal(18,2)) AS Requerimiento,
        CAST(0 AS decimal(18,2)) AS Embargo,
        CAST(0 AS decimal(18,2)) AS Multa,
        calc.Descuentos AS Descuentos,
        calc.TotalDetalle AS Total
      FROM ALSPAGOPREDIAL sp
      LEFT JOIN AlPredio p ON p.CveFteMT = sp.CveFteMT AND p.PredioId = sp.PredioId
      LEFT JOIN XiPersonas per ON per.CveFteMT = p.CveFteMT AND per.CvePersona = p.CvePersona
      LEFT JOIN ALCALIFICATIVOPROPIETARIO cal ON cal.CveFteMT = p.CveFteMT AND cal.CalificativoPropietarioId = p.CalificativoPropietarioId
      LEFT JOIN AlEstadoFisico ef ON ef.CveFteMT = p.CveFteMT AND ef.EstadoFisicoId = p.EstadoFisicoId
      OUTER APPLY (
        SELECT UPPER(RTRIM(CONVERT(varchar(50), p.PredioTipo))) AS PredioTipoNorm
      ) tipo
      OUTER APPLY (
        SELECT
          SUM(COALESCE(rd.ImpIniRec, 0)) AS SumAll,
          SUM(CASE WHEN rd.CveFteIng = 1201010101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010101,
          SUM(CASE WHEN rd.CveFteIng = 1201010102 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010102,
          SUM(CASE WHEN rd.CveFteIng = 1201010103 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010103,
          SUM(CASE WHEN rd.CveFteIng = 1201010105 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010105,
          SUM(CASE WHEN rd.CveFteIng = 1201020101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201020101,
          SUM(CASE WHEN rd.CveFteIng = 1201020102 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201020102,
          SUM(CASE WHEN rd.CveFteIng = 1201020104 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201020104,
          SUM(CASE WHEN rd.CveFteIng = 1201030101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201030101,
          SUM(CASE WHEN rd.CveFteIng = 1201030102 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201030102,
          SUM(CASE WHEN rd.CveFteIng = 1201030103 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201030103,
          SUM(CASE WHEN rd.CveFteIng = 1701010101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1701010101,
          SUM(CASE WHEN rd.CveFteIng IN (1201040110, 1201040108, 1201040104, 1201040101, 1201040106, 1201040107, 1201040105, 1701010102) THEN rd.ImpIniRec ELSE 0 END) AS SumDescuentos
        FROM COQRECIBODETALLE rd
        WHERE rd.CveFteMT = sp.CveFteMT AND rd.CveSerFol = sp.SPagoPredialSerie AND rd.CveFolio = sp.SPagoPredialFolio
      ) det
      OUTER APPLY (
        SELECT
          CAST(
            CASE
              WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201010103, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201010102, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201010101, 0)
              ELSE 0
            END
          AS decimal(18,2)) AS Impuesto,
          CAST(
            CASE
              WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201030101, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201030102, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201030103, 0)
              ELSE 0
            END
          AS decimal(18,2)) AS RezagoAnt,
          CAST(
            CASE
              WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201020101, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201020102, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201020104, 0)
              ELSE 0
            END
          AS decimal(18,2)) AS Rezago,
          CAST(COALESCE(det.Sum1201010105, 0) AS decimal(18,2)) AS Actualizacion,
          CAST(COALESCE(det.Sum1701010101, 0) AS decimal(18,2)) AS Recargos,
          CAST(ABS(COALESCE(det.SumDescuentos, 0)) AS decimal(18,2)) AS Descuentos,
          CAST(COALESCE(det.SumAll, 0) AS decimal(18,2)) AS TotalDetalle,
          CAST(
            COALESCE(det.SumAll, 0) -
            (
              (
                CASE
                  WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201010103, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201010102, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201010101, 0)
                  ELSE 0
                END
              ) +
              (
                CASE
                  WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201030101, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201030102, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201030103, 0)
                  ELSE 0
                END
              ) +
              (
                CASE
                  WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201020101, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201020102, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201020104, 0)
                  ELSE 0
                END
              ) +
              COALESCE(det.Sum1201010105, 0) +
              COALESCE(det.Sum1701010101, 0) -
              ABS(COALESCE(det.SumDescuentos, 0))
            )
          AS decimal(18,2)) AS Adicional
      ) calc
      WHERE sp.CveFteMT = @CveFteMT
        AND (@PredioId IS NULL OR sp.PredioId = @PredioId)
        AND (@PagoFrom IS NULL OR sp.SPagoPredialPagoFecha >= @PagoFrom)
        AND (@PagoTo IS NULL OR sp.SPagoPredialPagoFecha < DATEADD(DAY, 1, @PagoTo))
        AND (
          @ClaveCatastral IS NULL OR
          COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT)) =
            (@ClaveCatastral COLLATE DATABASE_DEFAULT)
        )
        AND (
          @ClaveCatastralFrom IS NULL OR
          COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT)) >=
            (@ClaveCatastralFrom COLLATE DATABASE_DEFAULT)
        )
        AND (
          @ClaveCatastralTo IS NULL OR
          COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT)) <=
            (@ClaveCatastralTo COLLATE DATABASE_DEFAULT)
        )
      ORDER BY sp.SPagoPredialPagoFecha DESC, sp.SPagoPredialId DESC
      OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY
    `;

    const result = await request.query(query);
    const data = result.recordset || [];
    const hasMore = data.length > limit;
    const rows = hasMore ? data.slice(0, limit) : data;
    const nextOffset = hasMore ? offset + limit : null;

    res.json({
      ok: true,
      filtros: {
        cveFteMT,
        todos,
        claveCatastral,
        claveCatastralFrom,
        claveCatastralTo,
        predioId: Number.isFinite(predioId) ? predioId : null,
        ejercicio: ejercicio && Number.isFinite(ejercicio) ? ejercicio : null,
        pagoFrom: pagoFrom && !Number.isNaN(pagoFrom.getTime()) ? pagoFrom.toISOString() : null,
        pagoTo: pagoTo && !Number.isNaN(pagoTo.getTime()) ? pagoTo.toISOString() : null,
        limit,
        offset
      },
      count: rows.length,
      hasMore,
      nextOffset,
      rows
    });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.get("/api/reportes/prediales/sabana-pagos.csv", async (req, res) => {
  try {
    const cveFteMT = (req.query.cveFteMT || "MTULUM").toString();
    let claveCatastral = req.query.claveCatastral ? req.query.claveCatastral.toString().trim() : "";
    let claveCatastralFrom = req.query.claveCatastralFrom
      ? req.query.claveCatastralFrom.toString().trim()
      : "";
    let claveCatastralTo = req.query.claveCatastralTo ? req.query.claveCatastralTo.toString().trim() : "";
    let predioId = req.query.predioId ? Number(req.query.predioId) : null;
    const ejercicio = req.query.ejercicio ? Number(req.query.ejercicio) : null;
    const todos = ["true", "1", "yes", "y", "si", "sí"].includes(String(req.query.todos || "").toLowerCase());
    let pagoFrom = req.query.pagoFrom ? new Date(req.query.pagoFrom.toString()) : null;
    let pagoTo = req.query.pagoTo ? new Date(req.query.pagoTo.toString()) : null;

    if (ejercicio && Number.isFinite(ejercicio)) {
      pagoFrom = new Date(ejercicio, 0, 1);
      pagoTo = new Date(ejercicio, 11, 31);
    }
    if (todos) {
      claveCatastral = "";
      claveCatastralFrom = "";
      claveCatastralTo = "";
      predioId = null;
    }
    const maxRowsRaw = req.query.maxRows ? Number(req.query.maxRows) : 50000;
    const maxRows = Number.isFinite(maxRowsRaw) ? Math.max(1, Math.min(200000, Math.trunc(maxRowsRaw))) : 50000;

    const columns = [
      "Clave",
      "Clave Catastral",
      "Propietario",
      "Razon social del contribuyente",
      "Direccion",
      "Colonia",
      "Tipo de Predio",
      "Valor del terreno",
      "Área construida",
      "Valor de construcción",
      "Valor catastral",
      "Calificativo",
      "Estado fisico",
      "Periodo inicial",
      "Periodo final",
      "Recibo",
      "Fecha de Pago",
      "Impuesto Corriente y Anticipado",
      "Rezago años anteriores",
      "Rezago",
      "Adicional",
      "Actualizacion",
      "Recargos",
      "Requerimiento",
      "Embargo",
      "Multa",
      "Descuentos",
      "Total"
    ];

    res.setHeader("content-type", "text/csv; charset=utf-8");
    res.setHeader("content-disposition", `attachment; filename="prediales_sabana.csv"`);
    res.write(`${columns.join(",")}\n`);

    const pool = await getPool();
    const request = pool.request();
    request.stream = true;
    request.input("CveFteMT", sql.VarChar(32), cveFteMT);
    request.input("ClaveCatastral", sql.VarChar(64), claveCatastral ? claveCatastral : null);
    request.input("ClaveCatastralFrom", sql.VarChar(64), claveCatastralFrom ? claveCatastralFrom : null);
    request.input("ClaveCatastralTo", sql.VarChar(64), claveCatastralTo ? claveCatastralTo : null);
    request.input("PredioId", sql.Decimal(18, 0), Number.isFinite(predioId) ? predioId : null);
    request.input("PagoFrom", sql.DateTime, pagoFrom && !Number.isNaN(pagoFrom.getTime()) ? pagoFrom : null);
    request.input("PagoTo", sql.DateTime, pagoTo && !Number.isNaN(pagoTo.getTime()) ? pagoTo : null);
    request.input("MaxRows", sql.Int, maxRows);

    request.on("row", (row) => {
      const line = columns.map((c) => csvEscape(row[c])).join(",");
      res.write(`${line}\n`);
    });

    request.on("error", (err) => {
      res.destroy(err);
    });

    request.on("done", () => {
      res.end();
    });

    request.query(`
      SELECT TOP (@MaxRows)
        CAST(sp.PredioId AS int) AS Clave,
        '''' + COALESCE(
          NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT),
          RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT),
          '' COLLATE DATABASE_DEFAULT
        ) AS [Clave Catastral],
        RTRIM(
          COALESCE(
            NULLIF(per.NombreCompletoPersona COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            '' COLLATE DATABASE_DEFAULT
          )
        ) AS Propietario,
        RTRIM(
          COALESCE(
            NULLIF(per.RazonSocialPersona COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            NULLIF(p.PredioContribuyenteNombre COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            '' COLLATE DATABASE_DEFAULT
          )
        ) AS [Razon social del contribuyente],
        RTRIM(
          COALESCE(
            NULLIF(p.PredioQroDireccion COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            NULLIF(p.PredioDireccionPropietario COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            NULLIF(
              CONCAT(
                RTRIM(COALESCE(p.PredioCalle COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT)),
                ' ' COLLATE DATABASE_DEFAULT,
                RTRIM(COALESCE(p.PredioNumExt COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT)),
                ' ' COLLATE DATABASE_DEFAULT,
                RTRIM(COALESCE(p.PredioNumInt COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT))
              ),
              '' COLLATE DATABASE_DEFAULT
            ),
            '' COLLATE DATABASE_DEFAULT
          )
        ) AS Direccion,
        RTRIM(
          COALESCE(
            NULLIF(p.PredioNombreColonia COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            NULLIF(p.PredioContribuyenteColoniaNombre COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            NULLIF(p.PredioNotificacionColoniaNom COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            NULLIF(p.PredioQroColoniaNombre COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT),
            '' COLLATE DATABASE_DEFAULT
          )
        ) AS Colonia,
        RTRIM(CONVERT(varchar(20), p.PredioTipo)) COLLATE DATABASE_DEFAULT AS [Tipo de Predio],
        CAST(COALESCE(p.PredioTerrenoImporte, 0) AS decimal(18,2)) AS [Valor del terreno],
        CAST(COALESCE(p.PredioTotalConstruccion, 0) AS decimal(18,2)) AS [Área construida],
        CAST(COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor de construcción],
        CAST(
          COALESCE(p.PredioTerrenoImporte, 0) +
          COALESCE(p.PredioTotalConstruccion, 0) +
          COALESCE(p.PredioConstruccionImporte, 0)
        AS decimal(18,2)) AS [Valor catastral],
        RTRIM(COALESCE(cal.CalificativoPropietarioNombre COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT)) AS Calificativo,
        RTRIM(
          COALESCE(
            ef.EstadoFisicoNombre COLLATE DATABASE_DEFAULT,
            ef.EstadoFisicoDescripcion COLLATE DATABASE_DEFAULT,
            '' COLLATE DATABASE_DEFAULT
          )
        ) AS [Estado fisico],
        CONCAT(CAST(sp.SPagoPredialInicialEjercicio AS varchar(4)), '- ', CAST(sp.SPagoPredialInicialPeriodo AS varchar(2))) AS [Periodo inicial],
        CONCAT(CAST(sp.SPagoPredialFinalEjercicio AS varchar(4)), '- ', CAST(sp.SPagoPredialFinalPeriodo AS varchar(2))) AS [Periodo final],
        RTRIM(COALESCE(sp.SPagoPredialSerie COLLATE DATABASE_DEFAULT, '' COLLATE DATABASE_DEFAULT)) +
          (' ' COLLATE DATABASE_DEFAULT) +
          CAST(sp.SPagoPredialFolio AS varchar(20)) AS Recibo,
        sp.SPagoPredialPagoFecha AS [Fecha de Pago],
        calc.Impuesto AS [Impuesto Corriente y Anticipado],
        calc.RezagoAnt AS [Rezago años anteriores],
        calc.Rezago AS Rezago,
        calc.Adicional AS Adicional,
        calc.Actualizacion AS Actualizacion,
        calc.Recargos AS Recargos,
        CAST(0 AS decimal(18,2)) AS Requerimiento,
        CAST(0 AS decimal(18,2)) AS Embargo,
        CAST(0 AS decimal(18,2)) AS Multa,
        calc.Descuentos AS Descuentos,
        calc.TotalDetalle AS Total
      FROM ALSPAGOPREDIAL sp
      LEFT JOIN AlPredio p
        ON p.CveFteMT = sp.CveFteMT
       AND p.PredioId = sp.PredioId
      LEFT JOIN XiPersonas per
        ON per.CveFteMT = p.CveFteMT
       AND per.CvePersona = p.CvePersona
      LEFT JOIN ALCALIFICATIVOPROPIETARIO cal
        ON cal.CveFteMT = p.CveFteMT
       AND cal.CalificativoPropietarioId = p.CalificativoPropietarioId
      LEFT JOIN AlEstadoFisico ef
        ON ef.CveFteMT = p.CveFteMT
       AND ef.EstadoFisicoId = p.EstadoFisicoId
      OUTER APPLY (
        SELECT
          SUM(COALESCE(rd.ImpIniRec, 0)) AS SumAll,
          SUM(CASE WHEN rd.CveFteIng = 1201010101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010101,
          SUM(CASE WHEN rd.CveFteIng = 1201010102 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010102,
          SUM(CASE WHEN rd.CveFteIng = 1201010103 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010103,
          SUM(CASE WHEN rd.CveFteIng = 1201010105 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201010105,
          SUM(CASE WHEN rd.CveFteIng = 1201020101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201020101,
          SUM(CASE WHEN rd.CveFteIng = 1201020102 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201020102,
          SUM(CASE WHEN rd.CveFteIng = 1201020104 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201020104,
          SUM(CASE WHEN rd.CveFteIng = 1201030101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201030101,
          SUM(CASE WHEN rd.CveFteIng = 1201030102 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201030102,
          SUM(CASE WHEN rd.CveFteIng = 1201030103 THEN rd.ImpIniRec ELSE 0 END) AS Sum1201030103,
          SUM(CASE WHEN rd.CveFteIng = 1701010101 THEN rd.ImpIniRec ELSE 0 END) AS Sum1701010101,
          SUM(CASE WHEN rd.CveFteIng IN (1201040110, 1201040108, 1201040104, 1201040101, 1201040106, 1201040107, 1201040105, 1701010102) THEN rd.ImpIniRec ELSE 0 END) AS SumDescuentos
        FROM COQRECIBODETALLE rd
        WHERE rd.CveFteMT = sp.CveFteMT
          AND rd.CveSerFol = sp.SPagoPredialSerie
          AND rd.CveFolio = sp.SPagoPredialFolio
      ) det
      OUTER APPLY (
        SELECT UPPER(RTRIM(CONVERT(varchar(50), p.PredioTipo))) AS PredioTipoNorm
      ) tipo
      OUTER APPLY (
        SELECT
          CAST(
            CASE
              WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201010103, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201010102, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201010101, 0)
              ELSE 0
            END
          AS decimal(18,2)) AS Impuesto,
          CAST(
            CASE
              WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201030101, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201030102, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201030103, 0)
              ELSE 0
            END
          AS decimal(18,2)) AS RezagoAnt,
          CAST(
            CASE
              WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201020101, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201020102, 0)
              WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201020104, 0)
              ELSE 0
            END
          AS decimal(18,2)) AS Rezago,
          CAST(COALESCE(det.Sum1201010105, 0) AS decimal(18,2)) AS Actualizacion,
          CAST(COALESCE(det.Sum1701010101, 0) AS decimal(18,2)) AS Recargos,
          CAST(ABS(COALESCE(det.SumDescuentos, 0)) AS decimal(18,2)) AS Descuentos,
          CAST(COALESCE(det.SumAll, 0) AS decimal(18,2)) AS TotalDetalle,
          CAST(
            COALESCE(det.SumAll, 0) -
            (
              (
                CASE
                  WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201010103, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201010102, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201010101, 0)
                  ELSE 0
                END
              ) +
              (
                CASE
                  WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201030101, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201030102, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201030103, 0)
                  ELSE 0
                END
              ) +
              (
                CASE
                  WHEN tipo.PredioTipoNorm IN ('RURAL', 'R', 'RUSTICO', 'RÚSTICO') THEN COALESCE(det.Sum1201020101, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 1 THEN COALESCE(det.Sum1201020102, 0)
                  WHEN tipo.PredioTipoNorm IN ('URBANO', 'U') AND p.EstadoFisicoId = 2 THEN COALESCE(det.Sum1201020104, 0)
                  ELSE 0
                END
              ) +
              COALESCE(det.Sum1201010105, 0) +
              COALESCE(det.Sum1701010101, 0) -
              ABS(COALESCE(det.SumDescuentos, 0))
            )
          AS decimal(18,2)) AS Adicional
      ) calc
      WHERE sp.CveFteMT = @CveFteMT
        AND (@PredioId IS NULL OR sp.PredioId = @PredioId)
        AND (@PagoFrom IS NULL OR sp.SPagoPredialPagoFecha >= @PagoFrom)
        AND (@PagoTo IS NULL OR sp.SPagoPredialPagoFecha < DATEADD(DAY, 1, @PagoTo))
        AND (
          @ClaveCatastral IS NULL OR
          COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT)) =
            (@ClaveCatastral COLLATE DATABASE_DEFAULT)
        )
        AND (
          @ClaveCatastralFrom IS NULL OR
          COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT)) >=
            (@ClaveCatastralFrom COLLATE DATABASE_DEFAULT)
        )
        AND (
          @ClaveCatastralTo IS NULL OR
          COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), '' COLLATE DATABASE_DEFAULT), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT)) <=
            (@ClaveCatastralTo COLLATE DATABASE_DEFAULT)
        )
      ORDER BY sp.SPagoPredialPagoFecha DESC, sp.SPagoPredialId DESC;
    `);
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.post("/api/consolidar", async (req, res) => {
  try {
    requireAdmin(req);

    const solicitudId = parseIntStrict(req.body.solicitudId, "solicitudId");
    const ano = parseIntStrict(req.body.ano, "ano");
    const grupoTramiteId = req.body.grupoTramiteId
      ? parseIntStrict(req.body.grupoTramiteId, "grupoTramiteId")
      : 42;
    const cveFteMT = (req.body.cveFteMT || "MTULUM").toString();
    const nuevoEstado = (req.body.nuevoEstado || "PP").toString();
    const vencimientoFecha =
      req.body.vencimientoFecha && req.body.vencimientoFecha.toString().trim()
        ? req.body.vencimientoFecha.toString().trim()
        : null;

    const pool = await getPool();
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const request = new sql.Request(tx);
      request.input("GrupoTramiteId", sql.Int, grupoTramiteId);
      request.input("SolicitudId", sql.Int, solicitudId);
      request.input("Ano", sql.Int, ano);
      request.input("CveFteMT", sql.VarChar(32), cveFteMT);
      request.input("NuevoEstado", sql.VarChar(8), nuevoEstado);
      request.input("Vencimiento", sql.VarChar(32), vencimientoFecha);

      const result = await request.query(`
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

        DECLARE @FtePrincipal int;
        DECLARE @FteSecundario int;
        DECLARE @ImporteTotal decimal(18,2);
        DECLARE @Cnt int;

        ;WITH x AS (
          SELECT
            SolicitudDetalleFteIngId,
            CAST(SolicitudDetalleImporteFijo AS decimal(18,2)) AS Importe,
            ROW_NUMBER() OVER (ORDER BY SolicitudDetalleFteIngId ASC) AS rn_asc,
            ROW_NUMBER() OVER (ORDER BY SolicitudDetalleFteIngId DESC) AS rn_desc,
            COUNT(*) OVER () AS cnt
          FROM TLSOLICITUDFUENTESINGRESO WITH (UPDLOCK, HOLDLOCK)
          WHERE GrupoTramiteId = @GrupoTramiteId
            AND SolicitudId = @SolicitudId
            AND SolicitudDetalleEjericicio = @Ano
            AND CveFteMT = @CveFteMT
        )
        SELECT
          @FtePrincipal = MAX(CASE WHEN rn_asc = 1 THEN SolicitudDetalleFteIngId END),
          @FteSecundario = MAX(CASE WHEN rn_desc = 1 THEN SolicitudDetalleFteIngId END),
          @ImporteTotal = SUM(Importe),
          @Cnt = MAX(cnt)
        FROM x;

        IF @Cnt <> 2 OR @FtePrincipal IS NULL OR @FteSecundario IS NULL OR @FtePrincipal = @FteSecundario
          THROW 51000, 'Se requieren exactamente 2 registros para consolidar (mismo filtro).', 1;

        UPDATE TLSOLICITUDFUENTESINGRESO
        SET SolicitudDetalleImporteFijo = @ImporteTotal
        WHERE GrupoTramiteId = @GrupoTramiteId
          AND SolicitudId = @SolicitudId
          AND SolicitudDetalleEjericicio = @Ano
          AND SolicitudDetalleFteIngId = @FtePrincipal
          AND CveFteMT = @CveFteMT;

        DELETE FROM TLSOLICITUDFUENTESINGRESO
        WHERE GrupoTramiteId = @GrupoTramiteId
          AND SolicitudId = @SolicitudId
          AND SolicitudDetalleEjericicio = @Ano
          AND SolicitudDetalleFteIngId = @FteSecundario
          AND CveFteMT = @CveFteMT;

        UPDATE TLSOLICITUD
        SET
          SolicitudEstado = @NuevoEstado,
          SolicitudVencimientoFecha =
            CASE
              WHEN @Vencimiento IS NULL THEN SolicitudVencimientoFecha
              ELSE COALESCE(
                TRY_CONVERT(datetime, @Vencimiento, 126),
                TRY_CONVERT(datetime, @Vencimiento, 23),
                TRY_CONVERT(datetime, @Vencimiento, 112),
                SolicitudVencimientoFecha
              )
            END
        WHERE GrupoTramiteId = @GrupoTramiteId
          AND SolicitudId = @SolicitudId;

        SELECT
          @FtePrincipal AS FtePrincipal,
          @FteSecundario AS FteSecundario,
          @ImporteTotal AS ImporteTotal;

        SELECT
          GrupoTramiteId,
          SolicitudId,
          SolicitudDetalleEjericicio,
          SolicitudDetalleFteIngId,
          CveFteMT,
          SolicitudDetalleImporteFijo
        FROM TLSOLICITUDFUENTESINGRESO
        WHERE GrupoTramiteId = @GrupoTramiteId
          AND SolicitudId = @SolicitudId
          AND SolicitudDetalleEjericicio = @Ano
          AND CveFteMT = @CveFteMT
        ORDER BY SolicitudDetalleFteIngId ASC;
      `);

      await tx.commit();

      const resumen = result.recordsets[0]?.[0] || null;
      const rows = result.recordsets[1] || [];

      res.json({
        ok: true,
        filtros: { grupoTramiteId, solicitudId, ano, cveFteMT },
        resumen,
        rows
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (e) {
    res.status(e.status || 400).json({ ok: false, error: e.message });
  }
});

const port = process.env.PORT ? Number(process.env.PORT) : 3000;
app.listen(port, () => {
  process.stdout.write(`http://localhost:${port}\n`);
});
