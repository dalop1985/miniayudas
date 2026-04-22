const { useMemo, useState } = React;

function JsonBox({ value }) {
  const text = useMemo(() => {
    if (!value) return "Listo. Usa 'Probar conexión' o 'Vista previa'.";
    return typeof value === "string" ? value : JSON.stringify(value, null, 2);
  }, [value]);

  return <pre className="output">{text}</pre>;
}

function DataTable({ rows }) {
  if (!rows || rows.length === 0) {
    return <div className="empty">No hay filas para mostrar.</div>;
  }

  const columns = Object.keys(rows[0]);

  return (
    <div className="table-shell">
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {columns.map((column) => (
                <td key={column}>{row[column] == null ? "" : String(row[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TableWithColumns({ rows, columns }) {
  if (!rows || rows.length === 0) {
    return <div className="empty">No hay filas para mostrar.</div>;
  }

  const moneyKeys = new Set([
    "Valor catastral",
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
  ]);

  const moneyFormat = new Intl.NumberFormat("es-MX", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });

  function formatCell(key, value) {
    if (value == null) return "";
    if (moneyKeys.has(key) && typeof value === "number") return moneyFormat.format(value);
    if (typeof value === "string" && /T\d{2}:\d{2}:\d{2}/.test(value)) {
      const t = Date.parse(value);
      if (!Number.isNaN(t) && (key.includes("Fecha") || key.includes("Pago"))) {
        const d = new Date(t);
        const dd = String(d.getDate()).padStart(2, "0");
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const yy = String(d.getFullYear()).slice(-2);
        return `${dd}/${mm}/${yy}`;
      }
    }
    return String(value);
  }

  return (
    <div className="table-shell">
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {columns.map((c) => (
                <td key={c.key}>{formatCell(c.key, row[c.key])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Field({ label, children, hint }) {
  return (
    <label className="field">
      <span className="field-label">{label}</span>
      {children}
      {hint ? <span className="field-hint">{hint}</span> : null}
    </label>
  );
}

function StatCard({ title, value, tone = "neutral" }) {
  return (
    <div className={`stat-card ${tone}`}>
      <div className="stat-title">{title}</div>
      <div className="stat-value">{value}</div>
    </div>
  );
}

function App() {
  const [form, setForm] = useState({
    solicitudId: "784388",
    ano: "2026",
    grupoTramiteId: "42",
    cveFteMT: "MTULUM",
    nuevoEstado: "PP",
    vencimientoFecha: "2026-04-30",
    adminKey: ""
  });
  const [section, setSection] = useState("cambio");
  const [reportSection, setReportSection] = useState("prediales");
  const [predialReport, setPredialReport] = useState({
    todos: "0",
    claveCatastral: "",
    claveCatastralFrom: "",
    claveCatastralTo: "",
    predioId: "",
    pagoFrom: "",
    pagoTo: "",
    ejercicio: "",
    limit: "200",
    offset: "0",
    maxRows: "50000"
  });
  const [predialRows, setPredialRows] = useState([]);
  const [predialHasMore, setPredialHasMore] = useState(false);
  const [predialNextOffset, setPredialNextOffset] = useState(null);
  const [output, setOutput] = useState(null);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState("");
  const [connection, setConnection] = useState(null);
  const origin = typeof window !== "undefined" ? window.location.origin : "";

  function updateField(event) {
    const { name, value } = event.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  }

  function updatePredialReport(event) {
    const { name, value } = event.target;
    setPredialReport((prev) => {
      if (name === "todos") {
        const next = { ...prev, todos: value, offset: "0" };
        if (value === "1") {
          return { ...next, claveCatastral: "", claveCatastralFrom: "", claveCatastralTo: "", predioId: "" };
        }
        return next;
      }
      return { ...prev, [name]: value };
    });
  }

  function getPayload() {
    return {
      solicitudId: Number(form.solicitudId),
      ano: Number(form.ano),
      grupoTramiteId: Number(form.grupoTramiteId),
      cveFteMT: form.cveFteMT,
      nuevoEstado: form.nuevoEstado,
      vencimientoFecha: form.vencimientoFecha.trim() || null
    };
  }

  async function loadPredialesSabana(nextOffsetValue) {
    setLoading("prediales");
    setOutput("Cargando sábana de prediales...");
    try {
      const offsetToUse =
        typeof nextOffsetValue === "number" ? nextOffsetValue : Number(predialReport.offset || 0);
      const todos = predialReport.todos === "1";
      const query = new URLSearchParams({
        cveFteMT: form.cveFteMT || "MTULUM",
        limit: predialReport.limit || "200",
        offset: String(Number.isFinite(offsetToUse) ? offsetToUse : 0)
      });
      if (todos) query.set("todos", "1");
      if (!todos && predialReport.claveCatastral) query.set("claveCatastral", predialReport.claveCatastral);
      if (!todos && predialReport.claveCatastralFrom) query.set("claveCatastralFrom", predialReport.claveCatastralFrom);
      if (!todos && predialReport.claveCatastralTo) query.set("claveCatastralTo", predialReport.claveCatastralTo);
      if (!todos && predialReport.predioId) query.set("predioId", predialReport.predioId);
      if (predialReport.ejercicio) query.set("ejercicio", predialReport.ejercicio);
      if (predialReport.pagoFrom) query.set("pagoFrom", predialReport.pagoFrom);
      if (predialReport.pagoTo) query.set("pagoTo", predialReport.pagoTo);

      const response = await fetch(`/api/reportes/prediales/sabana-pagos?${query.toString()}`);
      const json = await response.json();
      setPredialRows(json.rows || []);
      setPredialHasMore(!!json.hasMore);
      setPredialNextOffset(json.nextOffset ?? null);
      setPredialReport((prev) => ({ ...prev, offset: String(json.filtros?.offset ?? offsetToUse) }));
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function exportPredialesCsv() {
    const todos = predialReport.todos === "1";
    const query = new URLSearchParams({
      cveFteMT: form.cveFteMT || "MTULUM",
      maxRows: predialReport.maxRows || "50000"
    });
    if (todos) query.set("todos", "1");
    if (!todos && predialReport.claveCatastral) query.set("claveCatastral", predialReport.claveCatastral);
    if (!todos && predialReport.claveCatastralFrom) query.set("claveCatastralFrom", predialReport.claveCatastralFrom);
    if (!todos && predialReport.claveCatastralTo) query.set("claveCatastralTo", predialReport.claveCatastralTo);
    if (!todos && predialReport.predioId) query.set("predioId", predialReport.predioId);
    if (predialReport.ejercicio) query.set("ejercicio", predialReport.ejercicio);
    if (predialReport.pagoFrom) query.set("pagoFrom", predialReport.pagoFrom);
    if (predialReport.pagoTo) query.set("pagoTo", predialReport.pagoTo);
    window.open(`/api/reportes/prediales/sabana-pagos.csv?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  async function testConnection() {
    setLoading("test");
    setOutput("Probando conexión...");
    try {
      const response = await fetch("/api/test-connection");
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json")
        ? await response.json()
        : { ok: false, error: await response.text() };
      setConnection(payload.connection || null);
      setOutput({ httpStatus: response.status, ...payload });
    } catch (error) {
      setConnection(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function preview() {
    const payload = getPayload();
    if (!Number.isInteger(payload.solicitudId) || !Number.isInteger(payload.ano) || !Number.isInteger(payload.grupoTramiteId)) {
      setOutput("Campos numéricos inválidos.");
      return;
    }

    setLoading("preview");
    setOutput("Consultando filas...");
    setRows([]);

    try {
      const query = new URLSearchParams({
        solicitudId: String(payload.solicitudId),
        ano: String(payload.ano),
        grupoTramiteId: String(payload.grupoTramiteId),
        cveFteMT: payload.cveFteMT
      });
      const response = await fetch(`/api/fuentes?${query.toString()}`);
      const json = await response.json();
      setRows(json.rows || []);
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function consolidate() {
    if (!form.adminKey) {
      setOutput("Falta la llave admin.");
      return;
    }

    if (!window.confirm("Esto actualizará y borrará registros. ¿Deseas continuar?")) {
      return;
    }

    setLoading("run");
    setOutput("Ejecutando consolidación...");

    try {
      const response = await fetch("/api/consolidar", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-admin-key": form.adminKey
        },
        body: JSON.stringify(getPayload())
      });
      const json = await response.json();
      setRows(json.rows || []);
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  return (
    <main className="page">
      <section className="hero">
        <div>
          <div className="eyebrow">MiniAyudas + SQL Server</div>
          <h1>Panel de consolidación de fuentes de ingreso</h1>
          <p className="hero-copy">
            Consulta registros, valida la conexión a la base <strong>Tulum</strong> y ejecuta la consolidación desde el navegador.
          </p>
        </div>
        <div className="hero-stats">
          <StatCard title="Motor" value="MSSQL" />
          <StatCard title="Base" value="Tulum" />
          <StatCard title="Estado conexión" value={connection ? "Activa" : "Pendiente"} tone={connection ? "success" : "neutral"} />
        </div>
      </section>

      <section className="two-col">
        <article className="panel">
          <div className="panel-header">
            <div>
              <h2>Conexión</h2>
              <p>Backend: {origin}</p>
            </div>
            <button className="ghost" onClick={testConnection} disabled={loading === "test"}>
              {loading === "test" ? "Probando..." : "Probar conexión"}
            </button>
          </div>
          {connection ? (
            <div className="connection-list">
              <div><span>Servidor</span><strong>{String(connection.serverName || "-")}</strong></div>
              <div><span>Base</span><strong>{String(connection.databaseName || "-")}</strong></div>
              <div><span>Login</span><strong>{String(connection.loginName || "-")}</strong></div>
              <div><span>Hora servidor</span><strong>{String(connection.serverTime || "-")}</strong></div>
            </div>
          ) : (
            <div className="empty">Aún no hay prueba de conexión.</div>
          )}
        </article>

        <article className="panel">
          <div className="panel-header">
            <div>
              <h2>Respuesta</h2>
              <p>JSON de la última acción ejecutada.</p>
            </div>
          </div>
          <JsonBox value={output} />
        </article>
      </section>

      <section className="tabs">
        <button
          type="button"
          className={`tab ${section === "cambio" ? "active" : ""}`}
          onClick={() => setSection("cambio")}
        >
          Cambio de pases
        </button>
        <button
          type="button"
          className={`tab ${section === "reportes" ? "active" : ""}`}
          onClick={() => setSection("reportes")}
        >
          Reportes
        </button>
      </section>

      {section === "cambio" ? (
        <>
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Cambio de pases</h2>
                <p>Vista previa y consolidación para fuentes de ingreso.</p>
              </div>
            </div>

            <div className="grid">
              <Field label="SolicitudId">
                <input name="solicitudId" value={form.solicitudId} onChange={updateField} inputMode="numeric" />
              </Field>
              <Field label="Año">
                <input name="ano" value={form.ano} onChange={updateField} inputMode="numeric" />
              </Field>
              <Field label="GrupoTramiteId">
                <input name="grupoTramiteId" value={form.grupoTramiteId} onChange={updateField} inputMode="numeric" />
              </Field>
              <Field label="CveFteMT">
                <input name="cveFteMT" value={form.cveFteMT} onChange={updateField} />
              </Field>
              <Field label="Nuevo estado">
                <input name="nuevoEstado" value={form.nuevoEstado} onChange={updateField} />
              </Field>
              <Field label="Vencimiento" hint="Formato sugerido: 2026-04-30">
                <input name="vencimientoFecha" value={form.vencimientoFecha} onChange={updateField} />
              </Field>
              <Field label="Llave admin" hint="Solo se usa al consolidar">
                <input name="adminKey" type="password" value={form.adminKey} onChange={updateField} />
              </Field>
            </div>

            <div className="actions">
              <button className="primary" onClick={preview} disabled={loading === "preview"}>
                {loading === "preview" ? "Consultando..." : "Vista previa"}
              </button>
              <button className="danger" onClick={consolidate} disabled={loading === "run"}>
                {loading === "run" ? "Procesando..." : "Consolidar"}
              </button>
            </div>
          </section>

          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Filas consultadas</h2>
                <p>Se muestran las filas devueltas por la vista previa o tras consolidar.</p>
              </div>
              <div className="pill">{rows.length} filas</div>
            </div>
            <DataTable rows={rows} />
          </section>
        </>
      ) : (
        <>
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Reportes</h2>
                <p>Selecciona el tipo de reporte.</p>
              </div>
            </div>

            <section className="tabs tabs-secondary">
              <button
                type="button"
                className={`tab ${reportSection === "prediales" ? "active" : ""}`}
                onClick={() => setReportSection("prediales")}
              >
                Prediales
              </button>
              <button
                type="button"
                className={`tab ${reportSection === "licencias" ? "active" : ""}`}
                onClick={() => setReportSection("licencias")}
              >
                Licencias de Funcionamiento
              </button>
              <button
                type="button"
                className={`tab ${reportSection === "saneamiento" ? "active" : ""}`}
                onClick={() => setReportSection("saneamiento")}
              >
                Saneamiento Ambiental
              </button>
              <button
                type="button"
                className={`tab ${reportSection === "isabi" ? "active" : ""}`}
                onClick={() => setReportSection("isabi")}
              >
                ISABI
              </button>
            </section>

            {reportSection === "prediales" ? (
              <>
                <div className="grid">
                  <Field label="CveFteMT">
                    <input name="cveFteMT" value={form.cveFteMT} onChange={updateField} />
                  </Field>
                  <Field label="Todos" hint="Ignora Predio/Claves">
                    <select name="todos" value={predialReport.todos} onChange={updatePredialReport}>
                      <option value="0">No</option>
                      <option value="1">Sí</option>
                    </select>
                  </Field>
                  <Field label="Clave catastral" hint="Exacta">
                    <input
                      name="claveCatastral"
                      value={predialReport.claveCatastral}
                      onChange={updatePredialReport}
                      placeholder="914001000040003-"
                      disabled={predialReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Rango clave (desde)">
                    <input
                      name="claveCatastralFrom"
                      value={predialReport.claveCatastralFrom}
                      onChange={updatePredialReport}
                      placeholder="902014003011001-10"
                      disabled={predialReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Rango clave (hasta)">
                    <input
                      name="claveCatastralTo"
                      value={predialReport.claveCatastralTo}
                      onChange={updatePredialReport}
                      placeholder="902015000119001-47"
                      disabled={predialReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Predio (Clave)">
                    <input
                      name="predioId"
                      value={predialReport.predioId}
                      onChange={updatePredialReport}
                      inputMode="numeric"
                      disabled={predialReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Ejercicio Fiscal" hint="Eje: 2025">
                    <input name="ejercicio" value={predialReport.ejercicio} onChange={updatePredialReport} inputMode="numeric" placeholder="2025" />
                  </Field>
                  <Field label="Pago desde" hint="YYYY-MM-DD">
                    <input name="pagoFrom" value={predialReport.pagoFrom} onChange={updatePredialReport} placeholder="2025-01-01" />
                  </Field>
                  <Field label="Pago hasta" hint="YYYY-MM-DD">
                    <input name="pagoTo" value={predialReport.pagoTo} onChange={updatePredialReport} placeholder="2025-12-31" />
                  </Field>
                  <Field label="Límite (preview)">
                    <input name="limit" value={predialReport.limit} onChange={updatePredialReport} inputMode="numeric" />
                  </Field>
                  <Field label="Offset (preview)">
                    <input name="offset" value={predialReport.offset} onChange={updatePredialReport} inputMode="numeric" />
                  </Field>
                  <Field label="Máx filas (CSV)">
                    <input name="maxRows" value={predialReport.maxRows} onChange={updatePredialReport} inputMode="numeric" />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={() => loadPredialesSabana()} disabled={loading === "prediales"}>
                    {loading === "prediales" ? "Cargando..." : "Cargar sábana"}
                  </button>
                  <button className="ghost" onClick={exportPredialesCsv}>
                    Exportar CSV
                  </button>
                  {predialHasMore ? (
                    <button
                      className="ghost"
                      onClick={() => (predialNextOffset != null ? loadPredialesSabana(predialNextOffset) : null)}
                      disabled={loading === "prediales" || predialNextOffset == null}
                    >
                      Siguiente página
                    </button>
                  ) : null}
                  <div className="pill">{predialRows.length} filas</div>
                </div>

                <div className="table-space">
                  <TableWithColumns
                    rows={predialRows}
                    columns={[
                      { key: "Clave", label: "Clave" },
                      { key: "Clave Catastral", label: "Clave Catastral" },
                      { key: "Propietario", label: "Propietario" },
                      { key: "Razon social del contribuyente", label: "Razon social del contribuyente" },
                      { key: "Direccion", label: "Direccion" },
                      { key: "Colonia", label: "Colonia" },
                      { key: "Tipo de Predio", label: "Tipo de Predio" },
                      { key: "Valor del terreno", label: "Valor del terreno" },
                      { key: "Área construida", label: "Área construida" },
                      { key: "Valor de construcción", label: "Valor de construcción" },
                      { key: "Valor catastral", label: "Valor catastral" },
                      { key: "Calificativo", label: "Calificativo" },
                      { key: "Estado fisico", label: "Estado fisico" },
                      { key: "Periodo inicial", label: "Periodo inicial" },
                      { key: "Periodo final", label: "Periodo final" },
                      { key: "Recibo", label: "Recibo" },
                      { key: "Fecha de Pago", label: "Fecha de Pago" },
                      { key: "Impuesto Corriente y Anticipado", label: "Impuesto Corriente y Anticipado" },
                      { key: "Rezago años anteriores", label: "Rezago años anteriores" },
                      { key: "Rezago", label: "Rezago" },
                      { key: "Adicional", label: "Adicional" },
                      { key: "Actualizacion", label: "Actualizacion" },
                      { key: "Recargos", label: "Recargos" },
                      { key: "Requerimiento", label: "Requerimiento" },
                      { key: "Embargo", label: "Embargo" },
                      { key: "Multa", label: "Multa" },
                      { key: "Descuentos", label: "Descuentos" },
                      { key: "Total", label: "Total" }
                    ]}
                  />
                </div>
              </>
            ) : null}

            {reportSection === "licencias" ? (
              <div className="empty">
                Reporte de Licencias de Funcionamiento: pendiente de definir filtros (giro, estatus, vencidas, rango de fechas) y salida (pantalla/Excel).
              </div>
            ) : null}

            {reportSection === "saneamiento" ? (
              <div className="empty">
                Reporte de Saneamiento Ambiental: pendiente de definir filtros (zona, estatus, rango de fechas) y salida (pantalla/Excel).
              </div>
            ) : null}

            {reportSection === "isabi" ? (
              <div className="empty">
                Reporte de ISABI: pendiente de definir filtros (escritura, notaría, rango de fechas, estatus) y salida (pantalla/Excel).
              </div>
            ) : null}
          </section>
        </>
      )}
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
