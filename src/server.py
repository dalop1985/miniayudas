import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
 
import pytds
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
 
load_dotenv()
 
BASE_DIR = Path(__file__).resolve().parent.parent
 
app = FastAPI()
 
 
def _normalize_bool(value: Any, fallback: bool = False) -> bool:
  if value is None or value == "":
    return fallback
  return str(value).strip().lower() in {"true", "1", "yes", "y", "si", "sí"}
 
 
def _parse_extra_params(raw: str) -> Dict[str, str]:
  params: Dict[str, str] = {}
  if not raw:
    return params
  for item in str(raw).split(";"):
    chunk = item.strip()
    if not chunk:
      continue
    parts = chunk.split("=")
    key = parts[0].strip().lower()
    val = "=".join(parts[1:]).strip() if len(parts) > 1 else ""
    params[key] = val
  return params
 
 
def get_db_target() -> Dict[str, Any]:
  server = os.getenv("DB_HOST") or os.getenv("DB_SERVER")
  database = os.getenv("DB_NAME") or os.getenv("DB_DATABASE") or "Tulum"
  user = os.getenv("DB_USER")
  password = os.getenv("DB_PASSWORD")
  port_raw = os.getenv("DB_PORT")
  port = int(port_raw) if port_raw and port_raw.isdigit() else None
  extra = _parse_extra_params(os.getenv("DB_EXTRA_PARAMS") or "")
 
  if not server:
    raise ValueError("Falta DB_HOST o DB_SERVER")
  if not user:
    raise ValueError("Falta DB_USER")
  if not password:
    raise ValueError("Falta DB_PASSWORD")
 
  encrypt = _normalize_bool(extra.get("encrypt"), _normalize_bool(os.getenv("DB_ENCRYPT"), True))
  trust_server_certificate = _normalize_bool(
    extra.get("trustservercertificate"),
    _normalize_bool(os.getenv("DB_TRUST_SERVER_CERT"), True),
  )
  tls_server_name = os.getenv("DB_TLS_SERVER_NAME") or ""
 
  return {
    "server": server,
    "port": port,
    "database": database,
    "user": user,
    "password": password,
    "encrypt": encrypt,
    "trustServerCertificate": trust_server_certificate,
    "serverName": tls_server_name or None,
  }
 
 
def _connection_string() -> str:
  return ""
 
 
def get_conn():
  cfg = get_db_target()
  cafile = os.getenv("DB_CAFILE") or ""
  validate_host = not bool(cfg["trustServerCertificate"])
  return pytds.connect(
    server=cfg["server"],
    database=cfg["database"],
    user=cfg["user"],
    password=cfg["password"],
    port=cfg["port"] or 1433,
    autocommit=False,
    login_timeout=15,
    timeout=60,
    cafile=cafile or None,
    validate_host=validate_host,
  )
 
 
def _rows(cursor) -> List[Dict[str, Any]]:
  while cursor.description is None:
    if not cursor.nextset():
      return []
  cols = [c[0] for c in cursor.description] if cursor.description else []
  return [dict(zip(cols, row)) for row in cursor.fetchall()]
 
 
def _csv_escape(value: Any) -> str:
  if value is None:
    return ""
  if isinstance(value, datetime):
    s = value.isoformat()
  else:
    s = str(value)
  if any(ch in s for ch in [",", "\"", "\n", "\r"]):
    return "\"" + s.replace("\"", "\"\"") + "\""
  return s
 
 
def _require_admin(x_admin_key: Optional[str]) -> None:
  expected = os.getenv("ADMIN_KEY")
  if not expected:
    raise HTTPException(status_code=503, detail="ADMIN_KEY no configurada en el servidor")
  if not x_admin_key or x_admin_key != expected:
    raise HTTPException(status_code=401, detail="No autorizado")
 
 
@app.get("/api/health")
def health() -> Dict[str, Any]:
  return {"ok": True}
 
 
@app.get("/api/test-connection")
def test_connection() -> JSONResponse:
  try:
    cfg = get_db_target()
    with get_conn() as conn:
      cur = conn.cursor()
      cur.execute(
        """
        SELECT
          DB_NAME() AS databaseName,
          @@SERVERNAME AS serverName,
          SUSER_SNAME() AS loginName,
          GETDATE() AS serverTime
        """
      )
      row = _rows(cur)[0] if cur.description else {}
    return JSONResponse(
      jsonable_encoder(
        {
          "ok": True,
          "message": "Conexion exitosa",
          "target": {
            "server": cfg["server"],
            "port": cfg["port"],
            "database": cfg["database"],
            "encrypt": bool(cfg["encrypt"]),
            "trustServerCertificate": bool(cfg["trustServerCertificate"]),
            "serverName": cfg["serverName"],
          },
          "connection": row,
        }
      )
    )
  except Exception as e:
    return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
 
 
@app.get("/api/fuentes")
def fuentes(
  solicitudId: int,
  ano: int,
  grupoTramiteId: int = 42,
  cveFteMT: str = "MTULUM",
) -> JSONResponse:
  try:
    with get_conn() as conn:
      cur = conn.cursor()
      cur.execute(
        """
        DECLARE @GrupoTramiteId int = %s;
        DECLARE @SolicitudId int = %s;
        DECLARE @Ano int = %s;
        DECLARE @CveFteMT varchar(32) = %s;
 
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
        """,
        (grupoTramiteId, solicitudId, ano, cveFteMT),
      )
      rows = _rows(cur)
    return JSONResponse(
      jsonable_encoder(
        {
          "ok": True,
          "filtros": {"grupoTramiteId": grupoTramiteId, "solicitudId": solicitudId, "ano": ano, "cveFteMT": cveFteMT},
          "count": len(rows),
          "rows": rows,
        }
      )
    )
  except Exception as e:
    return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
 
 
@app.post("/api/consolidar")
async def consolidar(request: Request, x_admin_key: Optional[str] = Header(default=None)) -> JSONResponse:
  _require_admin(x_admin_key)
  body = await request.json()
 
  try:
    solicitud_id = int(body.get("solicitudId"))
    ano = int(body.get("ano"))
    grupo_tramite_id = int(body.get("grupoTramiteId") or 42)
    cve_fte_mt = str(body.get("cveFteMT") or "MTULUM")
    nuevo_estado = str(body.get("nuevoEstado") or "PP")
    vencimiento = str(body.get("vencimientoFecha")).strip() if body.get("vencimientoFecha") else None
 
    conn = get_conn()
    try:
      cur = conn.cursor()
      cur.execute(
        """
        DECLARE @GrupoTramiteId int = %s;
        DECLARE @SolicitudId int = %s;
        DECLARE @Ano int = %s;
        DECLARE @CveFteMT varchar(32) = %s;
        DECLARE @NuevoEstado varchar(8) = %s;
        DECLARE @Vencimiento varchar(32) = %s;
 
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
        """,
        (grupo_tramite_id, solicitud_id, ano, cve_fte_mt, nuevo_estado, vencimiento),
      )
 
      resumen_rows = _rows(cur)
      resumen = resumen_rows[0] if resumen_rows else None
      cur.nextset()
      rows = _rows(cur)
      conn.commit()
    except Exception:
      conn.rollback()
      raise
    finally:
      conn.close()
 
    return JSONResponse(
      jsonable_encoder(
        {
          "ok": True,
          "filtros": {"grupoTramiteId": grupo_tramite_id, "solicitudId": solicitud_id, "ano": ano, "cveFteMT": cve_fte_mt},
          "resumen": resumen,
          "rows": rows,
        }
      )
    )
  except HTTPException:
    raise
  except Exception as e:
    return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
 
 
def _parse_date(value: Optional[str]) -> Optional[datetime]:
  if not value:
    return None
  try:
    return datetime.fromisoformat(value)
  except Exception:
    try:
      return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
      return None
 
 
_SABANA_PREDIALES_SELECT = """
DECLARE @CveFteMT varchar(32) = %s;
DECLARE @Q varchar(200) = %s;
DECLARE @FromAlta datetime = %s;
DECLARE @ToAlta datetime = %s;
DECLARE @Limit int = %s;
DECLARE @Offset int = %s;
 
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
"""
 
_SABANA_PREDIALES_CSV = """
DECLARE @CveFteMT varchar(32) = %s;
DECLARE @Q varchar(200) = %s;
DECLARE @FromAlta datetime = %s;
DECLARE @ToAlta datetime = %s;
DECLARE @MaxRows int = %s;
 
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
"""
 
 
@app.get("/api/reportes/prediales/sabana")
def sabana_prediales(request: Request) -> JSONResponse:
  try:
    cve_fte_mt = str(request.query_params.get("cveFteMT") or "MTULUM")
    q = (request.query_params.get("q") or "").strip()
    from_alta = _parse_date(request.query_params.get("fromAlta"))
    to_alta = _parse_date(request.query_params.get("toAlta"))
 
    limit_raw = request.query_params.get("limit")
    offset_raw = request.query_params.get("offset")
    limit = int(limit_raw) if limit_raw and str(limit_raw).isdigit() else 200
    offset = int(offset_raw) if offset_raw and str(offset_raw).isdigit() else 0
    limit = max(1, min(500, limit))
    offset = max(0, offset)
 
    with get_conn() as conn:
      cur = conn.cursor()
      cur.execute(
        _SABANA_PREDIALES_SELECT,
        (cve_fte_mt, q or None, from_alta, to_alta, limit + 1, offset),
      )
      data = _rows(cur)
 
    has_more = len(data) > limit
    rows = data[:limit] if has_more else data
    next_offset = offset + limit if has_more else None
 
    return JSONResponse(
      jsonable_encoder(
        {
          "ok": True,
          "filtros": {
            "cveFteMT": cve_fte_mt,
            "q": q,
            "fromAlta": request.query_params.get("fromAlta") or None,
            "toAlta": request.query_params.get("toAlta") or None,
            "limit": limit,
            "offset": offset,
          },
          "count": len(rows),
          "hasMore": has_more,
          "nextOffset": next_offset,
          "rows": rows,
        }
      )
    )
  except Exception as e:
    return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
 
 
@app.get("/api/reportes/prediales/sabana.csv")
def sabana_prediales_csv(request: Request) -> StreamingResponse:
  cve_fte_mt = str(request.query_params.get("cveFteMT") or "MTULUM")
  q = (request.query_params.get("q") or "").strip()
  from_alta = _parse_date(request.query_params.get("fromAlta"))
  to_alta = _parse_date(request.query_params.get("toAlta"))
 
  max_rows_raw = request.query_params.get("maxRows")
  max_rows = int(max_rows_raw) if max_rows_raw and str(max_rows_raw).isdigit() else 50000
  max_rows = max(1, min(200000, max_rows))
 
  columns = [
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
    "UltMovObservaciones",
  ]
 
  def gen() -> Iterable[bytes]:
    yield (",".join(columns) + "\n").encode("utf-8")
    conn = get_conn()
    try:
      cur = conn.cursor()
      cur.execute(_SABANA_PREDIALES_CSV, (cve_fte_mt, q or None, from_alta, to_alta, max_rows))
      while cur.description is None:
        if not cur.nextset():
          return
      cols = [c[0] for c in cur.description]
      index = {name: i for i, name in enumerate(cols)}
      while True:
        batch = cur.fetchmany(500)
        if not batch:
          break
        for row in batch:
          line = ",".join(_csv_escape(row[index[c]]) if c in index else "" for c in columns) + "\n"
          yield line.encode("utf-8")
    finally:
      conn.close()
 
  headers = {"Content-Disposition": 'attachment; filename="prediales_sabana.csv"'}
  return StreamingResponse(gen(), media_type="text/csv; charset=utf-8", headers=headers)
 
 
def _prediales_pagos_filters(params: Dict[str, Any]) -> Dict[str, Any]:
  ejercicio = int(params["ejercicio"]) if params.get("ejercicio") not in (None, "", "null") else None
  pago_from = _parse_date(params.get("pagoFrom"))
  pago_to = _parse_date(params.get("pagoTo"))
  if ejercicio:
    pago_from = datetime(ejercicio, 1, 1)
    pago_to = datetime(ejercicio, 12, 31)
 
  todos = _normalize_bool(params.get("todos"), False)
  clave_catastral = (params.get("claveCatastral") or "").strip()
  clave_from = (params.get("claveCatastralFrom") or "").strip()
  clave_to = (params.get("claveCatastralTo") or "").strip()
  predio_id_raw = params.get("predioId")
  predio_id = int(predio_id_raw) if predio_id_raw not in (None, "", "null") else None
 
  if todos:
    clave_catastral = ""
    clave_from = ""
    clave_to = ""
    predio_id = None
 
  return {
    "todos": todos,
    "claveCatastral": clave_catastral,
    "claveCatastralFrom": clave_from,
    "claveCatastralTo": clave_to,
    "predioId": predio_id,
    "ejercicio": ejercicio,
    "pagoFrom": pago_from,
    "pagoTo": pago_to,
  }
 
 
_SABANA_PAGOS_SELECT = """
DECLARE @CveFteMT varchar(32) = %s;
DECLARE @ClaveCatastral varchar(64) = %s;
DECLARE @ClaveCatastralFrom varchar(64) = %s;
DECLARE @ClaveCatastralTo varchar(64) = %s;
DECLARE @PredioId decimal(18,0) = %s;
DECLARE @PagoFrom datetime = %s;
DECLARE @PagoTo datetime = %s;
DECLARE @Limit int = %s;
DECLARE @Offset int = %s;
 
SELECT
  CAST(sp.PredioId AS int) AS [Clave],
  '''' + COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), ''), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT), '') AS [Clave Catastral],
  RTRIM(COALESCE(per.NombreCompletoPersona COLLATE DATABASE_DEFAULT, '')) AS [Propietario],
  RTRIM(COALESCE(per.RazonSocialPersona COLLATE DATABASE_DEFAULT, p.PredioContribuyenteNombre COLLATE DATABASE_DEFAULT, '')) AS [Razon social del contribuyente],
  RTRIM(COALESCE(p.PredioQroDireccion COLLATE DATABASE_DEFAULT, '')) AS [Direccion],
  RTRIM(COALESCE(p.PredioNombreColonia COLLATE DATABASE_DEFAULT, '')) AS [Colonia],
  RTRIM(p.PredioTipo) COLLATE DATABASE_DEFAULT AS [Tipo de Predio],
  CAST(COALESCE(p.PredioTerrenoImporte, 0) AS decimal(18,2)) AS [Valor del terreno],
  CAST(COALESCE(p.PredioTotalConstruccion, 0) AS decimal(18,2)) AS [Área construida],
  CAST(COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor de construcción],
  CAST(COALESCE(p.PredioTerrenoImporte, 0) + COALESCE(p.PredioTotalConstruccion, 0) + COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor catastral],
  RTRIM(COALESCE(cal.CalificativoPropietarioNombre COLLATE DATABASE_DEFAULT, '')) AS [Calificativo],
  RTRIM(COALESCE(ef.EstadoFisicoNombre COLLATE DATABASE_DEFAULT, '')) AS [Estado fisico],
  CONCAT(sp.SPagoPredialInicialEjercicio, '- ', sp.SPagoPredialInicialPeriodo) AS [Periodo inicial],
  CONCAT(sp.SPagoPredialFinalEjercicio, '- ', sp.SPagoPredialFinalPeriodo) AS [Periodo final],
  RTRIM(sp.SPagoPredialSerie COLLATE DATABASE_DEFAULT) + ' ' + CAST(sp.SPagoPredialFolio AS varchar(20)) AS [Recibo],
  sp.SPagoPredialPagoFecha AS [Fecha de Pago],
  calc.Impuesto AS [Impuesto Corriente y Anticipado],
  calc.RezagoAnt AS [Rezago años anteriores],
  calc.Rezago AS [Rezago],
  calc.Adicional AS [Adicional],
  calc.Actualizacion AS [Actualizacion],
  calc.Recargos AS [Recargos],
  CAST(0 AS decimal(18,2)) AS [Requerimiento],
  CAST(0 AS decimal(18,2)) AS [Embargo],
  CAST(0 AS decimal(18,2)) AS [Multa],
  calc.Descuentos AS [Descuentos],
  calc.TotalDetalle AS [Total]
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
OFFSET @Offset ROWS FETCH NEXT @Limit ROWS ONLY;
"""

_SABANA_PAGOS_CSV = """
DECLARE @CveFteMT varchar(32) = %s;
DECLARE @ClaveCatastral varchar(64) = %s;
DECLARE @ClaveCatastralFrom varchar(64) = %s;
DECLARE @ClaveCatastralTo varchar(64) = %s;
DECLARE @PredioId decimal(18,0) = %s;
DECLARE @PagoFrom datetime = %s;
DECLARE @PagoTo datetime = %s;
DECLARE @MaxRows int = %s;
 
SELECT TOP (@MaxRows)
  CAST(sp.PredioId AS int) AS [Clave],
  '''' + COALESCE(NULLIF(RTRIM(sp.SPagoPredialCveCatastral COLLATE DATABASE_DEFAULT), ''), RTRIM(p.PredioCveCatastral COLLATE DATABASE_DEFAULT), '') AS [Clave Catastral],
  RTRIM(COALESCE(per.NombreCompletoPersona COLLATE DATABASE_DEFAULT, '')) AS [Propietario],
  RTRIM(COALESCE(per.RazonSocialPersona COLLATE DATABASE_DEFAULT, p.PredioContribuyenteNombre COLLATE DATABASE_DEFAULT, '')) AS [Razon social del contribuyente],
  RTRIM(COALESCE(p.PredioQroDireccion COLLATE DATABASE_DEFAULT, '')) AS [Direccion],
  RTRIM(COALESCE(p.PredioNombreColonia COLLATE DATABASE_DEFAULT, '')) AS [Colonia],
  RTRIM(p.PredioTipo) COLLATE DATABASE_DEFAULT AS [Tipo de Predio],
  CAST(COALESCE(p.PredioTerrenoImporte, 0) AS decimal(18,2)) AS [Valor del terreno],
  CAST(COALESCE(p.PredioTotalConstruccion, 0) AS decimal(18,2)) AS [Área construida],
  CAST(COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor de construcción],
  CAST(COALESCE(p.PredioTerrenoImporte, 0) + COALESCE(p.PredioTotalConstruccion, 0) + COALESCE(p.PredioConstruccionImporte, 0) AS decimal(18,2)) AS [Valor catastral],
  RTRIM(COALESCE(cal.CalificativoPropietarioNombre COLLATE DATABASE_DEFAULT, '')) AS [Calificativo],
  RTRIM(COALESCE(ef.EstadoFisicoNombre COLLATE DATABASE_DEFAULT, '')) AS [Estado fisico],
  CONCAT(sp.SPagoPredialInicialEjercicio, '- ', sp.SPagoPredialInicialPeriodo) AS [Periodo inicial],
  CONCAT(sp.SPagoPredialFinalEjercicio, '- ', sp.SPagoPredialFinalPeriodo) AS [Periodo final],
  RTRIM(sp.SPagoPredialSerie COLLATE DATABASE_DEFAULT) + ' ' + CAST(sp.SPagoPredialFolio AS varchar(20)) AS [Recibo],
  sp.SPagoPredialPagoFecha AS [Fecha de Pago],
  calc.Impuesto AS [Impuesto Corriente y Anticipado],
  calc.RezagoAnt AS [Rezago años anteriores],
  calc.Rezago AS [Rezago],
  calc.Adicional AS [Adicional],
  calc.Actualizacion AS [Actualizacion],
  calc.Recargos AS [Recargos],
  CAST(0 AS decimal(18,2)) AS [Requerimiento],
  CAST(0 AS decimal(18,2)) AS [Embargo],
  CAST(0 AS decimal(18,2)) AS [Multa],
  calc.Descuentos AS [Descuentos],
  calc.TotalDetalle AS [Total]
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
ORDER BY sp.SPagoPredialPagoFecha DESC, sp.SPagoPredialId DESC;
"""
 
 
@app.get("/api/reportes/prediales/sabana-pagos")
def sabana_pagos(request: Request) -> JSONResponse:
  try:
    cve_fte_mt = str(request.query_params.get("cveFteMT") or "MTULUM")
    f = _prediales_pagos_filters(dict(request.query_params))
 
    limit_raw = request.query_params.get("limit")
    offset_raw = request.query_params.get("offset")
    limit = int(limit_raw) if limit_raw and str(limit_raw).isdigit() else 200
    offset = int(offset_raw) if offset_raw and str(offset_raw).isdigit() else 0
    limit = max(1, min(1000, limit))
    offset = max(0, offset)
 
    with get_conn() as conn:
      cur = conn.cursor()
      cur.execute(
        _SABANA_PAGOS_SELECT,
        (
          cve_fte_mt,
          f["claveCatastral"] or None,
          f["claveCatastralFrom"] or None,
          f["claveCatastralTo"] or None,
          f["predioId"],
          f["pagoFrom"],
          f["pagoTo"],
          limit + 1,
          offset,
        ),
      )
      data = _rows(cur)
 
    has_more = len(data) > limit
    rows = data[:limit] if has_more else data
    next_offset = offset + limit if has_more else None
 
    return JSONResponse(
      jsonable_encoder(
        {
          "ok": True,
          "filtros": {
            "cveFteMT": cve_fte_mt,
            "todos": f["todos"],
            "claveCatastral": f["claveCatastral"],
            "claveCatastralFrom": f["claveCatastralFrom"],
            "claveCatastralTo": f["claveCatastralTo"],
            "predioId": f["predioId"],
            "ejercicio": f["ejercicio"],
            "pagoFrom": f["pagoFrom"].isoformat() if f["pagoFrom"] else None,
            "pagoTo": f["pagoTo"].isoformat() if f["pagoTo"] else None,
            "limit": limit,
            "offset": offset,
          },
          "count": len(rows),
          "hasMore": has_more,
          "nextOffset": next_offset,
          "rows": rows,
        }
      )
    )
  except Exception as e:
    return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
 
 
@app.get("/api/reportes/prediales/sabana-pagos.csv")
def sabana_pagos_csv(request: Request) -> StreamingResponse:
  cve_fte_mt = str(request.query_params.get("cveFteMT") or "MTULUM")
  f = _prediales_pagos_filters(dict(request.query_params))
 
  max_rows_raw = request.query_params.get("maxRows")
  max_rows = int(max_rows_raw) if max_rows_raw and str(max_rows_raw).isdigit() else 50000
  max_rows = max(1, min(200000, max_rows))
 
  columns = [
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
    "Total",
  ]
 
  def gen() -> Iterable[bytes]:
    yield (",".join(columns) + "\n").encode("utf-8")
    conn = get_conn()
    try:
      cur = conn.cursor()
      cur.execute(
        _SABANA_PAGOS_CSV,
        (
          cve_fte_mt,
          f["claveCatastral"] or None,
          f["claveCatastralFrom"] or None,
          f["claveCatastralTo"] or None,
          f["predioId"],
          f["pagoFrom"],
          f["pagoTo"],
          max_rows,
        ),
      )
      while cur.description is None:
        if not cur.nextset():
          return
      cols = [c[0] for c in cur.description]
      index = {name: i for i, name in enumerate(cols)}
      while True:
        batch = cur.fetchmany(500)
        if not batch:
          break
        for row in batch:
          line = ",".join(_csv_escape(row[index[c]]) if c in index else "" for c in columns) + "\n"
          yield line.encode("utf-8")
    finally:
      conn.close()
 
  headers = {"Content-Disposition": 'attachment; filename="prediales_sabana.csv"'}
  return StreamingResponse(gen(), media_type="text/csv; charset=utf-8", headers=headers)
 
 
app.mount("/vendor/react", StaticFiles(directory=BASE_DIR / "node_modules" / "react" / "umd"), name="vendor-react")
app.mount("/vendor/react-dom", StaticFiles(directory=BASE_DIR / "node_modules" / "react-dom" / "umd"), name="vendor-react-dom")
app.mount("/vendor/babel", StaticFiles(directory=BASE_DIR / "node_modules" / "@babel" / "standalone"), name="vendor-babel")
app.mount("/", StaticFiles(directory=BASE_DIR / "public", html=True), name="public")
 
 
if __name__ == "__main__":
  import uvicorn
 
  port_raw = os.getenv("PORT") or "3000"
  port = int(port_raw) if port_raw.isdigit() else 3000
  uvicorn.run(app, host="0.0.0.0", port=port)
