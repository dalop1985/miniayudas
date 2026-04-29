const { useEffect, useMemo, useState } = React;

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
    "Recargos rezago",
    "Requerimiento",
    "Embargo",
    "Multa",
    "Descuentos",
    "Derecho",
    "Licencia",
    "Lic Renovación",
    "Basura",
    "Actualizaciones",
    "Recargos",
    "Otros",
    "Tarifa Licencia",
    "Tarifa Basura",
    "Base Licencia Nueva",
    "Base Licencia Renovación",
    "Base Basura Nueva",
    "UMA (MXN)",
    "Total",
    "Valor del Terreno",
    "Valor de Construcción",
    "Valor Catastral",
    "Impuesto Actual",
    "Impuesto por bimestre",
    "Real",
    "Pronostico",
    "ErrorAbs"
    ,
    "Cuota",
    "Cuota original",
    "Monto actualizado",
    "Importe actualización",
    "Importe recargos",
    "Subtotal",
    "Monto anual",
    "Cuota mensual"
  ]);

  const moneyFormat = new Intl.NumberFormat("es-MX", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });

  const currencyFormat = new Intl.NumberFormat("es-MX", {
    style: "currency",
    currency: "MXN",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });

  const currencyAlwaysKeys = new Set([
    "Valor del Terreno",
    "Valor de Construcción",
    "Valor Catastral",
    "Impuesto Actual",
    "Impuesto por bimestre",
    "Tarifa Licencia",
    "Tarifa Basura",
    "Base Licencia Nueva",
    "Base Licencia Renovación",
    "Base Basura Nueva",
    "UMA (MXN)",
    "Real",
    "Pronostico",
    "ErrorAbs"
    ,
    "Monto anual",
    "Cuota mensual",
    "Cuota",
    "Cuota original",
    "Monto actualizado",
    "Importe actualización",
    "Importe recargos",
    "Subtotal"
  ]);

  const areaKeys = new Set(["Superficie del Terreno", "Área Construida"]);
  const areaFormat = new Intl.NumberFormat("es-MX", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });

  function formatCell(key, value) {
    if (value == null) return "";
    if (moneyKeys.has(key) && typeof value === "number") {
      if (currencyAlwaysKeys.has(key) || key.startsWith("Tarifa") || key.startsWith("Base ") || key === "UMA (MXN)") {
        return currencyFormat.format(value);
      }
      return moneyFormat.format(value);
    }
    if (areaKeys.has(key) && typeof value === "number") {
      return `${areaFormat.format(value)} m²`;
    }
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

  const hasGroups = columns.some((c) => c.group);
  const headerRow1 = [];
  const headerRow2 = [];
  if (hasGroups) {
    let i = 0;
    while (i < columns.length) {
      const c = columns[i];
      const group = c.group;
      if (!group) {
        headerRow1.push(
          <th key={`h1-${c.key}`} rowSpan={2}>
            {c.label}
          </th>
        );
        i += 1;
        continue;
      }
      let span = 1;
      while (i + span < columns.length && columns[i + span].group === group) span += 1;
      headerRow1.push(
        <th key={`g-${group}-${i}`} colSpan={span}>
          {group}
        </th>
      );
      for (let j = 0; j < span; j += 1) {
        const cc = columns[i + j];
        headerRow2.push(
          <th key={`h2-${cc.key}`}>
            {cc.label}
          </th>
        );
      }
      i += span;
    }
  }

  return (
    <div className="table-shell">
      <table>
        <thead>
          {hasGroups ? (
            <>
              <tr>{headerRow1}</tr>
              <tr>{headerRow2}</tr>
            </>
          ) : (
            <tr>
              {columns.map((c) => (
                <th key={c.key}>{c.label}</th>
              ))}
            </tr>
          )}
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

function toFiniteNumber(value) {
  if (value == null) return 0;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function AnalyticsLineChart({ rows, xKey, yKey, title, valueFormatter }) {
  if (!rows || rows.length === 0) {
    return (
      <div className="chart-card">
        <h3>{title}</h3>
        <div className="empty">No hay datos para graficar.</div>
      </div>
    );
  }

  const points = rows.map((row, index) => ({ x: index, label: String(row[xKey] ?? ""), y: toFiniteNumber(row[yKey]) }));
  const maxY = Math.max(...points.map((p) => p.y), 1);
  const minY = Math.min(...points.map((p) => p.y), 0);
  const range = Math.max(maxY - minY, 1);
  const width = 560;
  const height = 210;
  const padX = 34;
  const padY = 20;
  const innerW = width - padX * 2;
  const innerH = height - padY * 2;
  const stepX = points.length > 1 ? innerW / (points.length - 1) : 0;
  const polyline = points
    .map((p, i) => {
      const x = padX + i * stepX;
      const y = padY + innerH - ((p.y - minY) / range) * innerH;
      return `${x},${y}`;
    })
    .join(" ");
  const last = points[points.length - 1];
  const first = points[0];

  return (
    <div className="chart-card">
      <h3>{title}</h3>
      <svg className="chart-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
        <line x1={padX} y1={height - padY} x2={width - padX} y2={height - padY} className="chart-axis" />
        <line x1={padX} y1={padY} x2={padX} y2={height - padY} className="chart-axis" />
        <polyline points={polyline} className="chart-line" />
        {points.map((p, i) => {
          const x = padX + i * stepX;
          const y = padY + innerH - ((p.y - minY) / range) * innerH;
          return <circle key={`${p.label}-${i}`} cx={x} cy={y} r="2.9" className="chart-dot" />;
        })}
      </svg>
      <div className="chart-meta">
        <span>{first.label}</span>
        <strong>{valueFormatter ? valueFormatter(last.y) : String(last.y)}</strong>
        <span>{last.label}</span>
      </div>
    </div>
  );
}

function AnalyticsBarsChart({ rows, xKey, yKey, title, valueFormatter }) {
  if (!rows || rows.length === 0) {
    return (
      <div className="chart-card">
        <h3>{title}</h3>
        <div className="empty">No hay datos para graficar.</div>
      </div>
    );
  }

  const width = 560;
  const height = 210;
  const padX = 30;
  const padY = 20;
  const innerW = width - padX * 2;
  const innerH = height - padY * 2;
  const values = rows.map((row) => toFiniteNumber(row[yKey]));
  const maxY = Math.max(...values, 1);
  const step = innerW / rows.length;
  const barW = Math.max(6, step * 0.65);
  const maxItem = rows.reduce((acc, row) => {
    const n = toFiniteNumber(row[yKey]);
    return n > acc.n ? { n, label: String(row[xKey] ?? "") } : acc;
  }, { n: 0, label: "-" });

  return (
    <div className="chart-card">
      <h3>{title}</h3>
      <svg className="chart-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
        <line x1={padX} y1={height - padY} x2={width - padX} y2={height - padY} className="chart-axis" />
        {rows.map((row, i) => {
          const v = toFiniteNumber(row[yKey]);
          const barH = (v / maxY) * innerH;
          const x = padX + i * step + (step - barW) / 2;
          const y = height - padY - barH;
          return <rect key={`${row[xKey]}-${i}`} x={x} y={y} width={barW} height={barH} rx="4" className="chart-bar" />;
        })}
      </svg>
      <div className="chart-meta">
        <span>Pico: {maxItem.label}</span>
        <strong>{valueFormatter ? valueFormatter(maxItem.n) : String(maxItem.n)}</strong>
        <span>{rows.length} periodos</span>
      </div>
    </div>
  );
}

function AnalyticsMultiLineChart({ rows, xKey, series, title, valueFormatter }) {
  if (!rows || rows.length === 0) {
    return (
      <div className="chart-card">
        <h3>{title}</h3>
        <div className="empty">No hay datos para graficar.</div>
      </div>
    );
  }

  const width = 560;
  const height = 210;
  const padX = 34;
  const padY = 20;
  const innerW = width - padX * 2;
  const innerH = height - padY * 2;
  const stepX = rows.length > 1 ? innerW / (rows.length - 1) : 0;

  const allValues = [];
  for (const row of rows) {
    for (const s of series) {
      const v = row?.[s.key];
      const n = typeof v === "number" ? v : v == null ? NaN : toFiniteNumber(v);
      if (Number.isFinite(n)) allValues.push(n);
    }
  }
  const maxY = allValues.length ? Math.max(...allValues, 1) : 1;
  const minY = allValues.length ? Math.min(...allValues, 0) : 0;
  const range = Math.max(maxY - minY, 1);

  function xy(i, value) {
    const x = padX + i * stepX;
    const y = padY + innerH - ((value - minY) / range) * innerH;
    return { x, y };
  }

  function buildPath(key) {
    let d = "";
    let started = false;
    for (let i = 0; i < rows.length; i += 1) {
      const raw = rows[i]?.[key];
      const val = raw == null ? NaN : typeof raw === "number" ? raw : toFiniteNumber(raw);
      if (!Number.isFinite(val)) {
        started = false;
        continue;
      }
      const p = xy(i, val);
      if (!started) {
        d += `M ${p.x} ${p.y}`;
        started = true;
      } else {
        d += ` L ${p.x} ${p.y}`;
      }
    }
    return d;
  }

  const firstLabel = String(rows[0]?.[xKey] ?? "");
  const lastLabel = String(rows[rows.length - 1]?.[xKey] ?? "");
  const last = rows[rows.length - 1] || {};

  return (
    <div className="chart-card">
      <h3>{title}</h3>
      <div className="chart-legend">
        {series.map((s) => (
          <span className="legend-item" key={s.key}>
            <span className="legend-dot" style={{ background: s.color }} />
            {s.label}
          </span>
        ))}
      </div>
      <svg className="chart-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
        <line x1={padX} y1={height - padY} x2={width - padX} y2={height - padY} className="chart-axis" />
        <line x1={padX} y1={padY} x2={padX} y2={height - padY} className="chart-axis" />
        {series.map((s) => (
          <path
            key={s.key}
            d={buildPath(s.key)}
            fill="none"
            stroke={s.color}
            strokeWidth="2.2"
            strokeDasharray={s.dash ? s.dash : undefined}
          />
        ))}
        {series.map((s) =>
          rows.map((row, i) => {
            const raw = row?.[s.key];
            const val = raw == null ? NaN : typeof raw === "number" ? raw : toFiniteNumber(raw);
            if (!Number.isFinite(val)) return null;
            const p = xy(i, val);
            const labelX = String(row?.[xKey] ?? "");
            const labelY = valueFormatter ? valueFormatter(val) : String(val);
            return (
              <circle key={`${s.key}-${String(row?.[xKey] ?? i)}`} cx={p.x} cy={p.y} r="3" fill={s.color} opacity="0.95">
                <title>
                  {labelX} | {s.label}: {labelY}
                </title>
              </circle>
            );
          })
        )}
      </svg>
      <div className="chart-meta">
        <span>{firstLabel}</span>
        <strong>
          {series
            .map((s) => {
              const v = last?.[s.key];
              const n = v == null ? null : typeof v === "number" ? v : toFiniteNumber(v);
              if (n == null || !Number.isFinite(n)) return `${s.label}: -`;
              return `${s.label}: ${valueFormatter ? valueFormatter(n) : String(n)}`;
            })
            .join(" | ")}
        </strong>
        <span>{lastLabel}</span>
      </div>
    </div>
  );
}

function LoadingScreen({ title = "Cargando…" }) {
  return (
    <div className="auth-shell">
      <div className="auth-card">
        <div className="auth-title">{title}</div>
        <div className="auth-hint">Espera un momento.</div>
      </div>
    </div>
  );
}

function ErrorPage({ code, title, detail, onGoLogin, onGoHome }) {
  return (
    <div className="auth-shell">
      <div className="auth-card">
        <div className="auth-title">
          {code} — {title}
        </div>
        {detail ? <div className="auth-error">{detail}</div> : null}
        <div className="auth-actions">
          {onGoHome ? (
            <button className="ghost" type="button" onClick={onGoHome}>
              Ir al inicio
            </button>
          ) : null}
          {onGoLogin ? (
            <button className="primary" type="button" onClick={onGoLogin}>
              Iniciar sesión
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function LoginPage({ onLoggedIn }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [remember, setRemember] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem("login_username") || "";
      if (saved) {
        setUsername(saved);
        setRemember(true);
      }
    } catch (e) {
    }
  }, []);

  async function onSubmit(e) {
    e.preventDefault();
    if (busy) return;
    setError("");
    setBusy(true);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ username, password })
      });
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        setError((data && (data.detail || data.error)) || "No se pudo iniciar sesión.");
        return;
      }
      try {
        if (remember) window.localStorage.setItem("login_username", username);
        else window.localStorage.removeItem("login_username");
      } catch (e3) {
      }
      onLoggedIn?.(data?.user || null);
    } catch (e2) {
      setError(String(e2?.message || e2 || "Error desconocido"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login-shell">
      <header className="login-header">
        <div className="login-brand">
          <div className="login-logo" aria-hidden="true">
            MT
          </div>
          <div className="login-brand-title">Municipio de Tulum</div>
        </div>
        <div className="login-header-actions">
          <button className="login-icon-button" type="button" aria-label="Acción">
            <span className="login-icon-dot" aria-hidden="true" />
          </button>
          <div className="login-header-ghost" />
        </div>
      </header>

      <div className="login-grid">
        <div className="login-left">
          <form className="login-card" onSubmit={onSubmit}>
            <div className="login-card-title">Iniciar sesión</div>
            <div className="login-card-subtitle">Ingresa tus credenciales para acceder</div>

            <label className="login-field">
              <div className="login-label">Correo electrónico *</div>
              <input
                className="login-input"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoComplete="username"
                placeholder="nombre@ejemplo.com"
                required
                maxLength={120}
              />
            </label>

            <label className="login-field">
              <div className="login-label">Contraseña *</div>
              <div className="login-input-row">
                <input
                  className="login-input"
                  type={showPassword ? "text" : "password"}
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  autoComplete="current-password"
                  placeholder="Ingresa tu contraseña"
                  required
                  maxLength={256}
                />
                <button className="login-eye" type="button" onClick={() => setShowPassword((v) => !v)}>
                  {showPassword ? "Ocultar" : "Ver"}
                </button>
              </div>
            </label>

            <div className="login-row">
              <label className="login-remember">
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(!!e.target.checked)} />
                <span>Recordarme</span>
              </label>
              <button className="login-link" type="button" onClick={() => setError("Recuperación de contraseña pendiente de implementación.")}>
                ¿Olvidaste tu contraseña?
              </button>
            </div>

            {error ? <div className="login-error">{error}</div> : null}

            <button className="login-submit" type="submit" disabled={busy}>
              {busy ? "Iniciando…" : "Iniciar Sesión"}
            </button>

            <div className="login-footer">© 2026 Municipio de Tulum. Todos los derechos reservados.</div>
          </form>
        </div>

        <aside className="login-right" aria-hidden="true">
          <div className="login-hero">
            <div className="login-hero-title">Bienvenido a Municipio de Tulum</div>
            <div className="login-hero-text">“La Transformación Avanza” Accede a todas tus herramientas en un solo lugar.</div>
          </div>
        </aside>
      </div>
    </div>
  );
}

function IngresosHome({ user, onLogout }) {
  const isAdmin = String(user?.role || "").toLowerCase() === "admin";
  const [users, setUsers] = useState([]);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersError, setUsersError] = useState("");
  const [createForm, setCreateForm] = useState({ username: "", password: "", displayName: "", role: "cajero", isActive: true });
  const [createBusy, setCreateBusy] = useState(false);

  async function loadUsers() {
    setUsersError("");
    setUsersLoading(true);
    try {
      const res = await fetch("/api/users", { credentials: "include" });
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        setUsersError((data && (data.detail || data.error)) || "No se pudo cargar usuarios.");
        setUsers([]);
        return;
      }
      setUsers(Array.isArray(data?.rows) ? data.rows : []);
    } catch (e) {
      setUsersError(String(e?.message || e || "Error desconocido"));
      setUsers([]);
    } finally {
      setUsersLoading(false);
    }
  }

  useEffect(() => {
    if (!isAdmin) return;
    loadUsers();
  }, [isAdmin]);

  function updateCreateField(e) {
    const { name, value, type, checked } = e.target;
    setCreateForm((prev) => ({ ...prev, [name]: type === "checkbox" ? checked : value }));
  }

  async function createUser() {
    if (createBusy) return;
    setUsersError("");
    setCreateBusy(true);
    try {
      const res = await fetch("/api/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(createForm)
      });
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        setUsersError((data && (data.detail || data.error)) || "No se pudo crear usuario.");
        return;
      }
      setCreateForm({ username: "", password: "", displayName: "", role: "cajero", isActive: true });
      await loadUsers();
    } catch (e) {
      setUsersError(String(e?.message || e || "Error desconocido"));
    } finally {
      setCreateBusy(false);
    }
  }

  return (
    <div className="ingresos-shell">
      <div className="ingresos-topbar">
        <div>
          <div className="ingresos-title">Ingresos</div>
          <div className="ingresos-subtitle">
            {user?.displayName || user?.username || "Usuario"} • {user?.role || "-"}
          </div>
        </div>
        <div className="ingresos-actions">
          <button className="ghost" type="button" onClick={onLogout}>
            Salir
          </button>
        </div>
      </div>

      <div className="panel ingresos-panel">
        <div className="eyebrow">Módulos</div>
        <h2>Mapa de accesos (base)</h2>
        <div className="muted" style={{ marginTop: 10, lineHeight: 1.5 }}>
          Caja universales: cobro, cancelación, facturación, series/folios, lectura de pase, perfil cajero, cierre y
          reportes de cierre.
          <br />
          Administración: alta/baja de usuarios y permisos (pendiente de UI), auditoría de sesiones, configuración.
          <br />
          Proceso de numeración de pase (grupo trámite/trámite/folio): pendiente.
        </div>
      </div>

      {isAdmin ? (
        <div className="panel ingresos-panel">
          <div className="eyebrow">Configuración</div>
          <h2>Usuarios</h2>
          <div className="muted" style={{ marginTop: 10, lineHeight: 1.5 }}>
            Alta de usuarios desde el sistema (sin registro público). Roles: admin y cajero.
          </div>

          <div className="grid" style={{ marginTop: 14 }}>
            <Field label="Usuario">
              <input name="username" value={createForm.username} onChange={updateCreateField} maxLength={120} />
            </Field>
            <Field label="Contraseña">
              <input name="password" type="password" value={createForm.password} onChange={updateCreateField} maxLength={256} />
            </Field>
            <Field label="Nombre mostrado">
              <input name="displayName" value={createForm.displayName} onChange={updateCreateField} maxLength={200} />
            </Field>
            <Field label="Rol">
              <select name="role" value={createForm.role} onChange={updateCreateField}>
                <option value="cajero">cajero</option>
                <option value="admin">admin</option>
              </select>
            </Field>
          </div>

          <label className="auth-checkbox">
            <input name="isActive" type="checkbox" checked={Boolean(createForm.isActive)} onChange={updateCreateField} />
            Activo
          </label>

          {usersError ? <div className="auth-error">{usersError}</div> : null}

          <div className="actions" style={{ marginTop: 10 }}>
            <button className="primary" type="button" onClick={createUser} disabled={createBusy}>
              {createBusy ? "Creando..." : "Crear usuario"}
            </button>
            <button className="ghost" type="button" onClick={loadUsers} disabled={usersLoading}>
              {usersLoading ? "Cargando..." : "Recargar"}
            </button>
          </div>

          <div className="table-space">
            {users && users.length ? (
              <div className="table-shell">
                <table>
                  <thead>
                    <tr>
                      <th>Usuario</th>
                      <th>Nombre</th>
                      <th>Rol</th>
                      <th>Activo</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.map((u) => (
                      <tr key={String(u.id)}>
                        <td>{String(u.username || "")}</td>
                        <td>{String(u.displayName || "")}</td>
                        <td>{String(u.role || "")}</td>
                        <td>{u.isActive ? "Sí" : "No"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="empty">{usersLoading ? "Cargando..." : "No hay usuarios para mostrar."}</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function AdminApp({ user, onLogout, allowedSections, initialSection }) {
  const REPORT_CVE_FTE_MT = "MTULUM";
  const EXPORT_MAX_ROWS = "2000000";
  const role = String(user?.role || "").toLowerCase();
  const allowed = Array.isArray(allowedSections) && allowedSections.length ? allowedSections : ["ayudas"];
  const [form, setForm] = useState({
    solicitudId: "784388",
    ano: "2026",
    grupoTramiteId: "42",
    cveFteMT: "MTULUM",
    nuevoEstado: "PP",
    vencimientoFecha: "2026-04-30",
    adminKey: ""
  });
  const [section, setSection] = useState(() =>
    initialSection && allowed.includes(initialSection) ? initialSection : allowed.includes("inicio") ? "inicio" : allowed[0] || "ayudas"
  );
  const [ayudasSection, setAyudasSection] = useState("cambio");
  const [pasesCajaSection, setPasesCajaSection] = useState("predial");
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [inspectorTab, setInspectorTab] = useState("conexion");
  const [reportSection, setReportSection] = useState("prediales");
  const [analiticaSection, setAnaliticaSection] = useState("saneamiento");
  const [users, setUsers] = useState([]);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersError, setUsersError] = useState("");
  const [createUserForm, setCreateUserForm] = useState({
    username: "",
    password: "",
    displayName: "",
    role: "cajero",
    isActive: true
  });
  const [createUserBusy, setCreateUserBusy] = useState(false);
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
  const [licenciasReport, setLicenciasReport] = useState({
    todos: "0",
    tipo: "ambos",
    licenciaId: "",
    licenciaFrom: "",
    licenciaTo: "",
    pagoFrom: "",
    pagoTo: "",
    ejercicio: "",
    limit: "200",
    offset: "0",
    maxRows: "50000"
  });
  const [saneamientoReport, setSaneamientoReport] = useState({
    todos: "0",
    licenciaId: "",
    pagoFrom: "",
    pagoTo: "",
    ejercicio: "",
    limit: "200",
    offset: "0"
  });
  const [saneamientoAnalytics, setSaneamientoAnalytics] = useState({
    licenciaId: "",
    pagoFrom: "",
    pagoTo: "",
    ejercicio: "",
    backtestMonths: "6"
  });
  const [predialesAnalytics, setPredialesAnalytics] = useState({
    todos: "0",
    claveCatastral: "",
    claveCatastralFrom: "",
    claveCatastralTo: "",
    predioId: "",
    pagoFrom: "",
    pagoTo: "",
    ejercicio: "",
    backtestMonths: "6"
  });
  const [licenciasAnalytics, setLicenciasAnalytics] = useState({
    tipo: "ambos",
    licenciaId: "",
    licenciaFrom: "",
    licenciaTo: "",
    pagoFrom: "",
    pagoTo: "",
    ejercicio: "",
    backtestMonths: "6"
  });
  const [padronCatastralReport, setPadronCatastralReport] = useState({
    q: "",
    claveCatastral: "",
    claveMode: "contiene",
    predioId: "",
    propietario: "",
    apellidoPaterno: "",
    apellidoMaterno: "",
    nombre: "",
    calle: "",
    numero: "",
    estatus: "",
    adeudo: "todos",
    fromAlta: "",
    toAlta: "",
    offset: "0",
    maxRows: "200000"
  });
  const [cajaPredialForm, setCajaPredialForm] = useState(() => {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return {
      cveFteMT: "MTULUM",
      predioId: "",
      claveCatastral: "",
      claveMode: "exacto",
      tasaAlMillar: "3.0",
      diaVencimiento: "15",
      periodoInicioMes: "1",
      periodoInicioAnio: String(yyyy),
      periodoFinMes: String(d.getMonth() + 1),
      periodoFinAnio: String(yyyy),
      fechaPago: `${yyyy}-${mm}-${dd}`,
      tasasRecargosJson: `{\n  \"${yyyy}\": 0.015\n}`,
      tablaINPCJson: "{}"
    };
  });
  const [cajaPredialPredio, setCajaPredialPredio] = useState(null);
  const [cajaPredialPreview, setCajaPredialPreview] = useState(null);
  const [cajaPredialSearchTime, setCajaPredialSearchTime] = useState(null);
  const [cajaPredialError, setCajaPredialError] = useState("");
  const [criReport, setCriReport] = useState({
    reporte: "estado",
    idEnte: "",
    ejercicioFiscal: String(new Date().getFullYear()),
    periodo: "",
    limit: "200",
    offset: "0",
    maxRows: "50000"
  });
  const [predialRows, setPredialRows] = useState([]);
  const [predialHasMore, setPredialHasMore] = useState(false);
  const [predialNextOffset, setPredialNextOffset] = useState(null);
  const [predialUniverseTotals, setPredialUniverseTotals] = useState(null);
  const [licenciasRows, setLicenciasRows] = useState([]);
  const [licenciasHasMore, setLicenciasHasMore] = useState(false);
  const [licenciasNextOffset, setLicenciasNextOffset] = useState(null);
  const [licenciasUniverseTotals, setLicenciasUniverseTotals] = useState(null);
  const [saneamientoRows, setSaneamientoRows] = useState([]);
  const [saneamientoHasMore, setSaneamientoHasMore] = useState(false);
  const [saneamientoNextOffset, setSaneamientoNextOffset] = useState(null);
  const [saneamientoUniverseTotals, setSaneamientoUniverseTotals] = useState(null);
  const [saneamientoAnalyticsTotals, setSaneamientoAnalyticsTotals] = useState(null);
  const [saneamientoAnalyticsSeries, setSaneamientoAnalyticsSeries] = useState([]);
  const [saneamientoAnalyticsSearchTime, setSaneamientoAnalyticsSearchTime] = useState(null);
  const [saneamientoAnalyticsCanceladosCount, setSaneamientoAnalyticsCanceladosCount] = useState(0);
  const [saneamientoCanceladosOpen, setSaneamientoCanceladosOpen] = useState(false);
  const [saneamientoCanceladosRows, setSaneamientoCanceladosRows] = useState([]);
  const [saneamientoCanceladosHasMore, setSaneamientoCanceladosHasMore] = useState(false);
  const [saneamientoCanceladosNextOffset, setSaneamientoCanceladosNextOffset] = useState(null);
  const [saneamientoCanceladosSearchTime, setSaneamientoCanceladosSearchTime] = useState(null);
  const [saneamientoPronosticoSerie, setSaneamientoPronosticoSerie] = useState([]);
  const [saneamientoPronosticoVsReal, setSaneamientoPronosticoVsReal] = useState([]);
  const [saneamientoPronosticoModelo, setSaneamientoPronosticoModelo] = useState(null);
  const [saneamientoPronosticoSearchTime, setSaneamientoPronosticoSearchTime] = useState(null);
  const [predialesAnalyticsTotals, setPredialesAnalyticsTotals] = useState(null);
  const [predialesAnalyticsSeries, setPredialesAnalyticsSeries] = useState([]);
  const [predialesAnalyticsSearchTime, setPredialesAnalyticsSearchTime] = useState(null);
  const [predialesPronosticoSerie, setPredialesPronosticoSerie] = useState([]);
  const [predialesPronosticoVsReal, setPredialesPronosticoVsReal] = useState([]);
  const [predialesPronosticoModelo, setPredialesPronosticoModelo] = useState(null);
  const [predialesPronosticoSearchTime, setPredialesPronosticoSearchTime] = useState(null);
  const [licenciasAnalyticsTotals, setLicenciasAnalyticsTotals] = useState(null);
  const [licenciasAnalyticsSeries, setLicenciasAnalyticsSeries] = useState([]);
  const [licenciasAnalyticsSearchTime, setLicenciasAnalyticsSearchTime] = useState(null);
  const [licenciasPronosticoSerie, setLicenciasPronosticoSerie] = useState([]);
  const [licenciasPronosticoVsReal, setLicenciasPronosticoVsReal] = useState([]);
  const [licenciasPronosticoModelo, setLicenciasPronosticoModelo] = useState(null);
  const [licenciasPronosticoSearchTime, setLicenciasPronosticoSearchTime] = useState(null);
  const [padronCatastralRows, setPadronCatastralRows] = useState([]);
  const [padronCatastralHasMore, setPadronCatastralHasMore] = useState(false);
  const [padronCatastralNextOffset, setPadronCatastralNextOffset] = useState(null);
  const [criEntes, setCriEntes] = useState([]);
  const [criReportRows, setCriReportRows] = useState([]);
  const [criReportHasMore, setCriReportHasMore] = useState(false);
  const [criReportNextOffset, setCriReportNextOffset] = useState(null);
  const [factusInput, setFactusInput] = useState("");
  const [factusRows, setFactusRows] = useState([]);
  const [umaRows, setUmaRows] = useState([]);
  const [umaForm, setUmaForm] = useState({ vigenciaYear: "2025", umaMxn: "" });
  const [configSection, setConfigSection] = useState("umas");
  const [criCatalog, setCriCatalog] = useState(null);
  const [output, setOutput] = useState(null);
  const [activacionesSummary, setActivacionesSummary] = useState(null);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState("");
  const [connection, setConnection] = useState(null);
  const [predialSearchTime, setPredialSearchTime] = useState(null);
  const [licenciasSearchTime, setLicenciasSearchTime] = useState(null);
  const [saneamientoSearchTime, setSaneamientoSearchTime] = useState(null);
  const [padronCatastralSearchTime, setPadronCatastralSearchTime] = useState(null);
  const [criReportSearchTime, setCriReportSearchTime] = useState(null);
  const [searchTime, setSearchTime] = useState(null);
  const [factusSearchTime, setFactusSearchTime] = useState(null);
  const origin = typeof window !== "undefined" ? window.location.origin : "";
  const predialPageTotals = useMemo(() => {
    const keys = [
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

    function toNumber(value) {
      if (value == null) return 0;
      if (typeof value === "number" && Number.isFinite(value)) return value;
      const text = String(value).trim();
      if (!text) return 0;
      const normalized = text.replace(/[^0-9.-]/g, "");
      const num = Number(normalized);
      return Number.isFinite(num) ? num : 0;
    }

    const totals = {};
    for (const key of keys) totals[key] = 0;
    for (const row of predialRows || []) {
      for (const key of keys) {
        totals[key] += toNumber(row?.[key]);
      }
    }
    return totals;
  }, [predialRows]);
  const predialTotals = predialUniverseTotals || predialPageTotals;
  const licenciasPageTotals = useMemo(() => {
    const keys = [
      "Licencia",
      "Basura",
      "Actualizaciones",
      "Recargos",
      "Otros",
      "Total"
    ];

    function toNumber(value) {
      if (value == null) return 0;
      if (typeof value === "number" && Number.isFinite(value)) return value;
      const text = String(value).trim();
      if (!text) return 0;
      const normalized = text.replace(/[^0-9.-]/g, "");
      const num = Number(normalized);
      return Number.isFinite(num) ? num : 0;
    }

    const totals = {};
    for (const key of keys) totals[key] = 0;
    for (const row of licenciasRows || []) {
      for (const key of keys) {
        totals[key] += toNumber(row?.[key]);
      }
    }
    return totals;
  }, [licenciasRows]);
  const licenciasTotals = licenciasUniverseTotals || licenciasPageTotals;
  const saneamientoPageTotals = useMemo(() => {
    const keys = ["Derecho", "Actualizaciones", "Recargos", "Total"];

    function toNumber(value) {
      if (value == null) return 0;
      if (typeof value === "number" && Number.isFinite(value)) return value;
      const text = String(value).trim();
      if (!text) return 0;
      const normalized = text.replace(/[^0-9.-]/g, "");
      const num = Number(normalized);
      return Number.isFinite(num) ? num : 0;
    }

    const totals = {};
    for (const key of keys) totals[key] = 0;
    for (const row of saneamientoRows || []) {
      for (const key of keys) {
        totals[key] += toNumber(row?.[key]);
      }
    }
    return totals;
  }, [saneamientoRows]);
  const saneamientoTotals = saneamientoUniverseTotals || saneamientoPageTotals;
  const moneyMx = useMemo(
    () =>
      new Intl.NumberFormat("es-MX", {
        style: "currency",
        currency: "MXN",
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }),
    []
  );
  const numberMx = useMemo(
    () =>
      new Intl.NumberFormat("es-MX", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2
      }),
    []
  );
  const saneamientoPronosticoVsRealChart = useMemo(() => {
    return (saneamientoPronosticoVsReal || []).map((row) => {
      const isFuturePlaceholder = row && row.Real === 0 && row.ErrorAbs == null && row.ErrorPct == null;
      if (!isFuturePlaceholder) return row;
      return { ...row, Real: null };
    });
  }, [saneamientoPronosticoVsReal]);
  const predialesPronosticoVsRealChart = useMemo(() => {
    return (predialesPronosticoVsReal || []).map((row) => {
      const isFuturePlaceholder = row && row.Real === 0 && row.ErrorAbs == null && row.ErrorPct == null;
      if (!isFuturePlaceholder) return row;
      return { ...row, Real: null };
    });
  }, [predialesPronosticoVsReal]);
  const licenciasPronosticoVsRealChart = useMemo(() => {
    return (licenciasPronosticoVsReal || []).map((row) => {
      const isFuturePlaceholder = row && row.Real === 0 && row.ErrorAbs == null && row.ErrorPct == null;
      if (!isFuturePlaceholder) return row;
      return { ...row, Real: null };
    });
  }, [licenciasPronosticoVsReal]);
  const saneamientoPronosticoErrorAbsSeries = useMemo(() => {
    return (saneamientoPronosticoVsReal || []).map((row) => ({
      Periodo: row?.Periodo,
      ErrorAbs: row?.ErrorAbs == null ? 0 : toFiniteNumber(row.ErrorAbs)
    }));
  }, [saneamientoPronosticoVsReal]);
  const predialesPronosticoErrorAbsSeries = useMemo(() => {
    return (predialesPronosticoVsReal || []).map((row) => ({
      Periodo: row?.Periodo,
      ErrorAbs: row?.ErrorAbs == null ? 0 : toFiniteNumber(row.ErrorAbs)
    }));
  }, [predialesPronosticoVsReal]);
  const licenciasPronosticoErrorAbsSeries = useMemo(() => {
    return (licenciasPronosticoVsReal || []).map((row) => ({
      Periodo: row?.Periodo,
      ErrorAbs: row?.ErrorAbs == null ? 0 : toFiniteNumber(row.ErrorAbs)
    }));
  }, [licenciasPronosticoVsReal]);
  const saneamientoPronosticoIndiceSerie = useMemo(() => {
    const rows = saneamientoPronosticoVsRealChart || [];
    const firstPred = rows.find((r) => r && r.Pronostico != null);
    const base = firstPred ? toFiniteNumber(firstPred.Pronostico) : 0;
    if (!base) return [];
    return rows.map((row) => {
      const real = row?.Real == null ? null : toFiniteNumber(row.Real);
      const pred = row?.Pronostico == null ? null : toFiniteNumber(row.Pronostico);
      return {
        Periodo: row?.Periodo,
        RealIndex: real == null ? null : (real / base) * 100,
        PronosticoIndex: pred == null ? null : (pred / base) * 100
      };
    });
  }, [saneamientoPronosticoVsRealChart]);
  const predialesPronosticoIndiceSerie = useMemo(() => {
    const rows = predialesPronosticoVsRealChart || [];
    const firstPred = rows.find((r) => r && r.Pronostico != null);
    const base = firstPred ? toFiniteNumber(firstPred.Pronostico) : 0;
    if (!base) return [];
    return rows.map((row) => {
      const real = row?.Real == null ? null : toFiniteNumber(row.Real);
      const pred = row?.Pronostico == null ? null : toFiniteNumber(row.Pronostico);
      return {
        Periodo: row?.Periodo,
        RealIndex: real == null ? null : (real / base) * 100,
        PronosticoIndex: pred == null ? null : (pred / base) * 100
      };
    });
  }, [predialesPronosticoVsRealChart]);
  const licenciasPronosticoIndiceSerie = useMemo(() => {
    const rows = licenciasPronosticoVsRealChart || [];
    const firstPred = rows.find((r) => r && r.Pronostico != null);
    const base = firstPred ? toFiniteNumber(firstPred.Pronostico) : 0;
    if (!base) return [];
    return rows.map((row) => {
      const real = row?.Real == null ? null : toFiniteNumber(row.Real);
      const pred = row?.Pronostico == null ? null : toFiniteNumber(row.Pronostico);
      return {
        Periodo: row?.Periodo,
        RealIndex: real == null ? null : (real / base) * 100,
        PronosticoIndex: pred == null ? null : (pred / base) * 100
      };
    });
  }, [licenciasPronosticoVsRealChart]);
  const saneamientoAnalyticsSeriesAsc = useMemo(
    () => [...(saneamientoAnalyticsSeries || [])].sort((a, b) => String(a.Periodo).localeCompare(String(b.Periodo))),
    [saneamientoAnalyticsSeries]
  );
  const predialesAnalyticsSeriesAsc = useMemo(
    () => [...(predialesAnalyticsSeries || [])].sort((a, b) => String(a.Periodo).localeCompare(String(b.Periodo))),
    [predialesAnalyticsSeries]
  );
  const licenciasAnalyticsSeriesAsc = useMemo(
    () => [...(licenciasAnalyticsSeries || [])].sort((a, b) => String(a.Periodo).localeCompare(String(b.Periodo))),
    [licenciasAnalyticsSeries]
  );
  const saneamientoAnalyticsFeatures = useMemo(() => {
    const recActivos = (saneamientoAnalyticsSeriesAsc || []).reduce((acc, row) => acc + toFiniteNumber(row.Recibos), 0);
    const totalCobrado = toFiniteNumber(saneamientoAnalyticsTotals?.Total);
    const cancelados = toFiniteNumber(saneamientoAnalyticsCanceladosCount);
    const universo = recActivos + cancelados;
    const ticketPromedio = recActivos > 0 ? totalCobrado / recActivos : 0;
    const tasaCancelacion = universo > 0 ? (cancelados / universo) * 100 : 0;
    const first = saneamientoAnalyticsSeriesAsc.length ? toFiniteNumber(saneamientoAnalyticsSeriesAsc[0].Total) : 0;
    const last = saneamientoAnalyticsSeriesAsc.length ? toFiniteNumber(saneamientoAnalyticsSeriesAsc[saneamientoAnalyticsSeriesAsc.length - 1].Total) : 0;
    const tendenciaPct = first > 0 ? ((last - first) / first) * 100 : 0;
    return { recActivos, cancelados, universo, ticketPromedio, tasaCancelacion, tendenciaPct };
  }, [saneamientoAnalyticsSeriesAsc, saneamientoAnalyticsTotals, saneamientoAnalyticsCanceladosCount]);
  const predialesAnalyticsFeatures = useMemo(() => {
    const recActivos = (predialesAnalyticsSeriesAsc || []).reduce((acc, row) => acc + toFiniteNumber(row.Recibos), 0);
    const totalCobrado = toFiniteNumber(predialesAnalyticsTotals?.Total);
    const ticketPromedio = recActivos > 0 ? totalCobrado / recActivos : 0;
    const first = predialesAnalyticsSeriesAsc.length ? toFiniteNumber(predialesAnalyticsSeriesAsc[0].Total) : 0;
    const last = predialesAnalyticsSeriesAsc.length
      ? toFiniteNumber(predialesAnalyticsSeriesAsc[predialesAnalyticsSeriesAsc.length - 1].Total)
      : 0;
    const tendenciaPct = first > 0 ? ((last - first) / first) * 100 : 0;
    return { recActivos, ticketPromedio, tendenciaPct };
  }, [predialesAnalyticsSeriesAsc, predialesAnalyticsTotals]);
  const licenciasAnalyticsFeatures = useMemo(() => {
    const recActivos = (licenciasAnalyticsSeriesAsc || []).reduce((acc, row) => acc + toFiniteNumber(row.Recibos), 0);
    const totalCobrado = toFiniteNumber(licenciasAnalyticsTotals?.Total);
    const ticketPromedio = recActivos > 0 ? totalCobrado / recActivos : 0;
    const first = licenciasAnalyticsSeriesAsc.length ? toFiniteNumber(licenciasAnalyticsSeriesAsc[0].Total) : 0;
    const last = licenciasAnalyticsSeriesAsc.length
      ? toFiniteNumber(licenciasAnalyticsSeriesAsc[licenciasAnalyticsSeriesAsc.length - 1].Total)
      : 0;
    const tendenciaPct = first > 0 ? ((last - first) / first) * 100 : 0;
    return { recActivos, ticketPromedio, tendenciaPct };
  }, [licenciasAnalyticsSeriesAsc, licenciasAnalyticsTotals]);

  function updateField(event) {
    const { name, value } = event.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  }

  function updateCajaPredialField(event) {
    const { name, value } = event.target;
    setCajaPredialForm((prev) => ({ ...prev, [name]: value }));
  }

  function parseJsonIfPresent(text, label) {
    const raw = String(text || "").trim();
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch (e) {
      throw new Error(`${label}: JSON inválido`);
    }
  }

  async function cajasBuscarPredio() {
    setCajaPredialError("");
    setCajaPredialPreview(null);
    const predioId = String(cajaPredialForm.predioId || "").trim();
    if (!predioId) {
      setCajaPredialError("Captura el PredioId.");
      return;
    }

    setLoading("cajas_predio");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({ cveFteMT: cajaPredialForm.cveFteMT || "MTULUM" });
      query.set("predioId", predioId);
      const response = await fetch(`/api/cajas/predial/predio?${query.toString()}`);
      const json = await response.json().catch(() => null);
      const t1 = performance.now();
      setCajaPredialSearchTime(((t1 - t0) / 1000).toFixed(2));
      if (!response.ok || !json?.ok) {
        setCajaPredialPredio(null);
        setCajaPredialError(String(json?.detail || `HTTP ${response.status}`));
        setOutput(json || { ok: false, detail: `HTTP ${response.status}` });
        return;
      }
      setCajaPredialPredio(json.predio || null);
      setOutput(json);
    } catch (error) {
      setCajaPredialPredio(null);
      setCajaPredialSearchTime(null);
      setCajaPredialError(error.message || "Error al buscar predio.");
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function cajasVistaPreviaPredial() {
    setCajaPredialError("");
    setCajaPredialPreview(null);
    const t0 = performance.now();
    let tasasRecargos = null;
    let tablaINPC = null;
    try {
      tasasRecargos = parseJsonIfPresent(cajaPredialForm.tasasRecargosJson, "Tasas de recargos");
      tablaINPC = parseJsonIfPresent(cajaPredialForm.tablaINPCJson, "Tabla INPC");
    } catch (e) {
      setCajaPredialError(e.message || "JSON inválido.");
      return;
    }

    setLoading("cajas_preview");
    try {
      const payload = {
        cveFteMT: cajaPredialForm.cveFteMT || "MTULUM",
        predioId: cajaPredialForm.predioId ? Number(cajaPredialForm.predioId) : undefined,
        tasaAlMillar: Number(cajaPredialForm.tasaAlMillar),
        diaVencimiento: Number(cajaPredialForm.diaVencimiento),
        fechaPago: cajaPredialForm.fechaPago,
        periodoInicio: { mes: Number(cajaPredialForm.periodoInicioMes), anio: Number(cajaPredialForm.periodoInicioAnio) },
        periodoFin: { mes: Number(cajaPredialForm.periodoFinMes), anio: Number(cajaPredialForm.periodoFinAnio) },
        tasasRecargos: tasasRecargos || undefined,
        tablaINPC: tablaINPC || undefined,
        valorCatastral: cajaPredialPredio?.PredioValuoCatastralImporte ?? cajaPredialPredio?.PredioCatastralImporte ?? undefined
      };

      const response = await fetch("/api/cajas/predial/pase/preview", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      const json = await response.json().catch(() => null);
      const t1 = performance.now();
      setCajaPredialSearchTime(((t1 - t0) / 1000).toFixed(2));
      if (!response.ok || !json?.ok) {
        setCajaPredialPreview(null);
        setCajaPredialError(String(json?.detail || `HTTP ${response.status}`));
        setOutput(json || { ok: false, detail: `HTTP ${response.status}` });
        return;
      }
      setCajaPredialPreview(json);
      setOutput(json);
    } catch (error) {
      setCajaPredialPreview(null);
      setCajaPredialSearchTime(null);
      setCajaPredialError(error.message || "Error al calcular.");
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
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

  function updateLicenciasReport(event) {
    const { name, value } = event.target;
    setLicenciasReport((prev) => {
      if (name === "todos") {
        const next = { ...prev, todos: value, offset: "0" };
        if (value === "1") {
          return { ...next, licenciaId: "", licenciaFrom: "", licenciaTo: "" };
        }
        return next;
      }
      const next = { ...prev, [name]: value };
      if (
        name === "todos" ||
        name === "tipo" ||
        name === "ejercicio" ||
        name === "pagoFrom" ||
        name === "pagoTo" ||
        name === "licenciaId" ||
        name === "licenciaFrom" ||
        name === "licenciaTo"
      ) {
        next.offset = "0";
      }
      return next;
    });
    if (
      (name === "licenciaId" || name === "licenciaFrom" || name === "licenciaTo") &&
      licenciasReport.todos !== "1"
    ) {
      setLicenciasRows([]);
      setLicenciasHasMore(false);
      setLicenciasNextOffset(null);
      setLicenciasUniverseTotals(null);
      setLicenciasSearchTime(null);
    }
  }

  function updateSaneamientoReport(event) {
    const { name, value } = event.target;
    setSaneamientoReport((prev) => {
      if (name === "todos") {
        const next = { ...prev, todos: value, offset: "0" };
        if (value === "1") return { ...next, licenciaId: "" };
        return next;
      }
      const next = { ...prev, [name]: value };
      if (name === "licenciaId" || name === "ejercicio" || name === "pagoFrom" || name === "pagoTo" || name === "limit") {
        next.offset = "0";
      }
      return next;
    });
    if (name === "licenciaId" || name === "ejercicio" || name === "pagoFrom" || name === "pagoTo" || name === "todos") {
      setSaneamientoRows([]);
      setSaneamientoHasMore(false);
      setSaneamientoNextOffset(null);
      setSaneamientoUniverseTotals(null);
      setSaneamientoSearchTime(null);
    }
  }

  function updateSaneamientoAnalytics(event) {
    const { name, value } = event.target;
    setSaneamientoAnalytics((prev) => ({ ...prev, [name]: value }));
    if (name === "licenciaId" || name === "ejercicio" || name === "pagoFrom" || name === "pagoTo") {
      setSaneamientoAnalyticsTotals(null);
      setSaneamientoAnalyticsSeries([]);
      setSaneamientoAnalyticsSearchTime(null);
      setSaneamientoAnalyticsCanceladosCount(0);
      setSaneamientoCanceladosOpen(false);
      setSaneamientoCanceladosRows([]);
      setSaneamientoCanceladosHasMore(false);
      setSaneamientoCanceladosNextOffset(null);
      setSaneamientoCanceladosSearchTime(null);
      setSaneamientoPronosticoSerie([]);
      setSaneamientoPronosticoVsReal([]);
      setSaneamientoPronosticoModelo(null);
      setSaneamientoPronosticoSearchTime(null);
    }
  }

  function updatePredialesAnalytics(event) {
    const { name, value } = event.target;
    setPredialesAnalytics((prev) => {
      if (name === "todos") {
        const next = { ...prev, todos: value };
        if (value === "1") return { ...next, claveCatastral: "", claveCatastralFrom: "", claveCatastralTo: "", predioId: "" };
        return next;
      }
      return { ...prev, [name]: value };
    });
    if (
      name === "todos" ||
      name === "claveCatastral" ||
      name === "claveCatastralFrom" ||
      name === "claveCatastralTo" ||
      name === "predioId" ||
      name === "ejercicio" ||
      name === "pagoFrom" ||
      name === "pagoTo"
    ) {
      setPredialesAnalyticsTotals(null);
      setPredialesAnalyticsSeries([]);
      setPredialesAnalyticsSearchTime(null);
      setPredialesPronosticoSerie([]);
      setPredialesPronosticoVsReal([]);
      setPredialesPronosticoModelo(null);
      setPredialesPronosticoSearchTime(null);
    }
  }

  function updateLicenciasAnalytics(event) {
    const { name, value } = event.target;
    setLicenciasAnalytics((prev) => ({ ...prev, [name]: value }));
    if (
      name === "tipo" ||
      name === "licenciaId" ||
      name === "licenciaFrom" ||
      name === "licenciaTo" ||
      name === "ejercicio" ||
      name === "pagoFrom" ||
      name === "pagoTo"
    ) {
      setLicenciasAnalyticsTotals(null);
      setLicenciasAnalyticsSeries([]);
      setLicenciasAnalyticsSearchTime(null);
      setLicenciasPronosticoSerie([]);
      setLicenciasPronosticoVsReal([]);
      setLicenciasPronosticoModelo(null);
      setLicenciasPronosticoSearchTime(null);
    }
  }

  function updatePadronCatastralReport(event) {
    const { name, value } = event.target;
    setPadronCatastralReport((prev) => {
      const next = { ...prev, [name]: value };
      if (name === "q" || name === "fromAlta" || name === "toAlta" || name === "limit") {
        next.offset = "0";
      }
      return next;
    });
    if (name === "q" || name === "fromAlta" || name === "toAlta") {
      setPadronCatastralRows([]);
      setPadronCatastralHasMore(false);
      setPadronCatastralNextOffset(null);
      setPadronCatastralSearchTime(null);
    }
  }

  function updateCriReport(event) {
    const { name, value } = event.target;
    setCriReport((prev) => {
      const next = { ...prev, [name]: value };
      if (name === "reporte" || name === "idEnte" || name === "ejercicioFiscal" || name === "periodo" || name === "limit") {
        next.offset = "0";
      }
      return next;
    });
    if (name === "reporte" || name === "idEnte" || name === "ejercicioFiscal" || name === "periodo") {
      setCriReportRows([]);
      setCriReportHasMore(false);
      setCriReportNextOffset(null);
      setCriReportSearchTime(null);
    }
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

  async function loadLicenciasSabana(nextOffsetValue) {
    setLoading("licencias");
    setOutput("Cargando sábana de licencias...");
    const t0 = performance.now();
    try {
      const offsetToUse =
        typeof nextOffsetValue === "number" ? nextOffsetValue : Number(licenciasReport.offset || 0);
      if (!offsetToUse) setLicenciasUniverseTotals(null);
      if (
        !offsetToUse &&
        licenciasReport.todos !== "1" &&
        (licenciasReport.licenciaId || licenciasReport.licenciaFrom || licenciasReport.licenciaTo)
      ) {
        setLicenciasRows([]);
        setLicenciasHasMore(false);
        setLicenciasNextOffset(null);
        setLicenciasSearchTime(null);
      }
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        tipo: licenciasReport.tipo || "ambos",
        limit: licenciasReport.limit || "200",
        offset: String(Number.isFinite(offsetToUse) ? offsetToUse : 0)
      });
      if (licenciasReport.ejercicio) query.set("ejercicio", licenciasReport.ejercicio);
      if (licenciasReport.pagoFrom) query.set("pagoFrom", licenciasReport.pagoFrom);
      if (licenciasReport.pagoTo) query.set("pagoTo", licenciasReport.pagoTo);
      if (licenciasReport.todos !== "1") {
        if (licenciasReport.licenciaId) query.set("licenciaId", licenciasReport.licenciaId);
        if (licenciasReport.licenciaFrom) query.set("licenciaFrom", licenciasReport.licenciaFrom);
        if (licenciasReport.licenciaTo) query.set("licenciaTo", licenciasReport.licenciaTo);
      }

      const response = await fetch(`/api/reportes/licencias/funcionamiento?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setLicenciasSearchTime(((t1 - t0) / 1000).toFixed(2));
      setLicenciasRows(json.rows || []);
      setLicenciasHasMore(!!json.hasMore);
      setLicenciasNextOffset(json.nextOffset ?? null);
      setLicenciasUniverseTotals(json.totals || null);
      setLicenciasReport((prev) => ({ ...prev, offset: String(json.filtros?.offset ?? offsetToUse) }));
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function clearLicenciasSabana() {
    setLicenciasRows([]);
    setLicenciasHasMore(false);
    setLicenciasNextOffset(null);
    setLicenciasUniverseTotals(null);
    setLicenciasSearchTime(null);
    setOutput(null);
  }

  async function loadSaneamientoSabana(nextOffsetValue) {
    setLoading("saneamiento");
    setOutput("Cargando sábana de saneamiento ambiental...");
    const t0 = performance.now();
    try {
      const offsetToUse =
        typeof nextOffsetValue === "number" ? nextOffsetValue : Number(saneamientoReport.offset || 0);
      if (!offsetToUse) setSaneamientoUniverseTotals(null);

      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        limit: saneamientoReport.limit || "200",
        offset: String(Number.isFinite(offsetToUse) ? offsetToUse : 0)
      });
      if (saneamientoReport.todos !== "1" && saneamientoReport.licenciaId) {
        query.set("licenciaId", saneamientoReport.licenciaId);
      }
      if (saneamientoReport.ejercicio) query.set("ejercicio", saneamientoReport.ejercicio);
      if (saneamientoReport.pagoFrom) query.set("pagoFrom", saneamientoReport.pagoFrom);
      if (saneamientoReport.pagoTo) query.set("pagoTo", saneamientoReport.pagoTo);

      const response = await fetch(`/api/reportes/saneamiento/ambiental?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setSaneamientoSearchTime(((t1 - t0) / 1000).toFixed(2));
      setSaneamientoRows(json.rows || []);
      setSaneamientoHasMore(!!json.hasMore);
      setSaneamientoNextOffset(json.nextOffset ?? null);
      setSaneamientoUniverseTotals(json.totals || null);
      setSaneamientoReport((prev) => ({ ...prev, offset: String(json.filtros?.offset ?? offsetToUse) }));
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function clearSaneamientoSabana() {
    setSaneamientoRows([]);
    setSaneamientoHasMore(false);
    setSaneamientoNextOffset(null);
    setSaneamientoUniverseTotals(null);
    setSaneamientoSearchTime(null);
    setOutput(null);
  }

  async function loadSaneamientoAnalytics() {
    setLoading("saneamiento-analytics");
    setOutput("Cargando analítica de saneamiento ambiental...");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT
      });
      if (saneamientoAnalytics.licenciaId) query.set("licenciaId", saneamientoAnalytics.licenciaId);
      if (saneamientoAnalytics.ejercicio) query.set("ejercicio", saneamientoAnalytics.ejercicio);
      if (saneamientoAnalytics.pagoFrom) query.set("pagoFrom", saneamientoAnalytics.pagoFrom);
      if (saneamientoAnalytics.pagoTo) query.set("pagoTo", saneamientoAnalytics.pagoTo);

      const response = await fetch(`/api/analitica/saneamiento/ambiental?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setSaneamientoAnalyticsSearchTime(((t1 - t0) / 1000).toFixed(2));
      setSaneamientoAnalyticsTotals(json.totals || null);
      setSaneamientoAnalyticsSeries(json.series || []);
      setSaneamientoAnalyticsCanceladosCount(Number(json.canceladosCount || 0));
      setOutput(json);
    } catch (error) {
      setSaneamientoAnalyticsTotals(null);
      setSaneamientoAnalyticsSeries([]);
      setSaneamientoAnalyticsSearchTime(null);
      setSaneamientoAnalyticsCanceladosCount(0);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadSaneamientoCancelados(nextOffsetValue) {
    setLoading("saneamiento-cancelados");
    setOutput("Cargando recibos cancelados...");
    const t0 = performance.now();
    try {
      const offsetToUse = typeof nextOffsetValue === "number" ? nextOffsetValue : 0;
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        limit: "200",
        offset: String(Number.isFinite(offsetToUse) ? offsetToUse : 0)
      });
      if (saneamientoAnalytics.licenciaId) query.set("licenciaId", saneamientoAnalytics.licenciaId);
      if (saneamientoAnalytics.ejercicio) query.set("ejercicio", saneamientoAnalytics.ejercicio);
      if (saneamientoAnalytics.pagoFrom) query.set("pagoFrom", saneamientoAnalytics.pagoFrom);
      if (saneamientoAnalytics.pagoTo) query.set("pagoTo", saneamientoAnalytics.pagoTo);

      const response = await fetch(`/api/analitica/saneamiento/ambiental/cancelados?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setSaneamientoCanceladosSearchTime(((t1 - t0) / 1000).toFixed(2));
      setSaneamientoCanceladosRows(json.rows || []);
      setSaneamientoCanceladosHasMore(!!json.hasMore);
      setSaneamientoCanceladosNextOffset(json.nextOffset ?? null);
      setSaneamientoCanceladosOpen(true);
      setOutput(json);
    } catch (error) {
      setSaneamientoCanceladosRows([]);
      setSaneamientoCanceladosHasMore(false);
      setSaneamientoCanceladosNextOffset(null);
      setSaneamientoCanceladosSearchTime(null);
      setSaneamientoCanceladosOpen(true);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadSaneamientoPronosticos() {
    setLoading("saneamiento-pronosticos");
    setOutput("Calculando pronosticos...");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT
      });
      if (saneamientoAnalytics.licenciaId) query.set("licenciaId", saneamientoAnalytics.licenciaId);
      if (saneamientoAnalytics.ejercicio) query.set("ejercicio", saneamientoAnalytics.ejercicio);
      if (saneamientoAnalytics.pagoFrom) query.set("pagoFrom", saneamientoAnalytics.pagoFrom);
      if (saneamientoAnalytics.pagoTo) query.set("pagoTo", saneamientoAnalytics.pagoTo);
      if (saneamientoAnalytics.backtestMonths) query.set("backtestMonths", saneamientoAnalytics.backtestMonths);

      const response = await fetch(`/api/analitica/saneamiento/ambiental/pronostico?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setSaneamientoPronosticoSearchTime(((t1 - t0) / 1000).toFixed(2));
      setSaneamientoPronosticoSerie(json.serieCombinada || []);
      setSaneamientoPronosticoVsReal(json.pronosticosVsReal || []);
      setSaneamientoPronosticoModelo(json.modelo || null);
      setOutput(json);
    } catch (error) {
      setSaneamientoPronosticoSerie([]);
      setSaneamientoPronosticoVsReal([]);
      setSaneamientoPronosticoModelo(null);
      setSaneamientoPronosticoSearchTime(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadPredialesAnalytics() {
    setLoading("prediales-analytics");
    setOutput("Cargando analítica de prediales...");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        todos: predialesAnalytics.todos || "0"
      });
      if (predialesAnalytics.ejercicio) query.set("ejercicio", predialesAnalytics.ejercicio);
      if (predialesAnalytics.pagoFrom) query.set("pagoFrom", predialesAnalytics.pagoFrom);
      if (predialesAnalytics.pagoTo) query.set("pagoTo", predialesAnalytics.pagoTo);
      if (predialesAnalytics.todos !== "1") {
        if (predialesAnalytics.claveCatastral) query.set("claveCatastral", predialesAnalytics.claveCatastral);
        if (predialesAnalytics.claveCatastralFrom) query.set("claveCatastralFrom", predialesAnalytics.claveCatastralFrom);
        if (predialesAnalytics.claveCatastralTo) query.set("claveCatastralTo", predialesAnalytics.claveCatastralTo);
        if (predialesAnalytics.predioId) query.set("predioId", predialesAnalytics.predioId);
      }

      const response = await fetch(`/api/analitica/prediales/pagos?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setPredialesAnalyticsSearchTime(((t1 - t0) / 1000).toFixed(2));
      setPredialesAnalyticsTotals(json.totals || null);
      setPredialesAnalyticsSeries(json.series || []);
      setOutput(json);
    } catch (error) {
      setPredialesAnalyticsTotals(null);
      setPredialesAnalyticsSeries([]);
      setPredialesAnalyticsSearchTime(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadPredialesPronosticos() {
    setLoading("prediales-pronosticos");
    setOutput("Calculando pronosticos (prediales)...");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        todos: predialesAnalytics.todos || "0"
      });
      if (predialesAnalytics.ejercicio) query.set("ejercicio", predialesAnalytics.ejercicio);
      if (predialesAnalytics.pagoFrom) query.set("pagoFrom", predialesAnalytics.pagoFrom);
      if (predialesAnalytics.pagoTo) query.set("pagoTo", predialesAnalytics.pagoTo);
      if (predialesAnalytics.backtestMonths) query.set("backtestMonths", predialesAnalytics.backtestMonths);
      if (predialesAnalytics.todos !== "1") {
        if (predialesAnalytics.claveCatastral) query.set("claveCatastral", predialesAnalytics.claveCatastral);
        if (predialesAnalytics.claveCatastralFrom) query.set("claveCatastralFrom", predialesAnalytics.claveCatastralFrom);
        if (predialesAnalytics.claveCatastralTo) query.set("claveCatastralTo", predialesAnalytics.claveCatastralTo);
        if (predialesAnalytics.predioId) query.set("predioId", predialesAnalytics.predioId);
      }

      const response = await fetch(`/api/analitica/prediales/pagos/pronostico?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setPredialesPronosticoSearchTime(((t1 - t0) / 1000).toFixed(2));
      setPredialesPronosticoSerie(json.serieCombinada || []);
      setPredialesPronosticoVsReal(json.pronosticosVsReal || []);
      setPredialesPronosticoModelo(json.modelo || null);
      setOutput(json);
    } catch (error) {
      setPredialesPronosticoSerie([]);
      setPredialesPronosticoVsReal([]);
      setPredialesPronosticoModelo(null);
      setPredialesPronosticoSearchTime(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadLicenciasAnalytics() {
    setLoading("licencias-analytics");
    setOutput("Cargando analítica de licencias...");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        tipo: licenciasAnalytics.tipo || "ambos"
      });
      if (licenciasAnalytics.ejercicio) query.set("ejercicio", licenciasAnalytics.ejercicio);
      if (licenciasAnalytics.pagoFrom) query.set("pagoFrom", licenciasAnalytics.pagoFrom);
      if (licenciasAnalytics.pagoTo) query.set("pagoTo", licenciasAnalytics.pagoTo);
      if (licenciasAnalytics.licenciaId) query.set("licenciaId", licenciasAnalytics.licenciaId);
      if (licenciasAnalytics.licenciaFrom) query.set("licenciaFrom", licenciasAnalytics.licenciaFrom);
      if (licenciasAnalytics.licenciaTo) query.set("licenciaTo", licenciasAnalytics.licenciaTo);

      const response = await fetch(`/api/analitica/licencias/funcionamiento?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setLicenciasAnalyticsSearchTime(((t1 - t0) / 1000).toFixed(2));
      setLicenciasAnalyticsTotals(json.totals || null);
      setLicenciasAnalyticsSeries(json.series || []);
      setOutput(json);
    } catch (error) {
      setLicenciasAnalyticsTotals(null);
      setLicenciasAnalyticsSeries([]);
      setLicenciasAnalyticsSearchTime(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadLicenciasPronosticos() {
    setLoading("licencias-pronosticos");
    setOutput("Calculando pronosticos (licencias)...");
    const t0 = performance.now();
    try {
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        tipo: licenciasAnalytics.tipo || "ambos"
      });
      if (licenciasAnalytics.ejercicio) query.set("ejercicio", licenciasAnalytics.ejercicio);
      if (licenciasAnalytics.pagoFrom) query.set("pagoFrom", licenciasAnalytics.pagoFrom);
      if (licenciasAnalytics.pagoTo) query.set("pagoTo", licenciasAnalytics.pagoTo);
      if (licenciasAnalytics.licenciaId) query.set("licenciaId", licenciasAnalytics.licenciaId);
      if (licenciasAnalytics.licenciaFrom) query.set("licenciaFrom", licenciasAnalytics.licenciaFrom);
      if (licenciasAnalytics.licenciaTo) query.set("licenciaTo", licenciasAnalytics.licenciaTo);
      if (licenciasAnalytics.backtestMonths) query.set("backtestMonths", licenciasAnalytics.backtestMonths);

      const response = await fetch(`/api/analitica/licencias/funcionamiento/pronostico?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setLicenciasPronosticoSearchTime(((t1 - t0) / 1000).toFixed(2));
      setLicenciasPronosticoSerie(json.serieCombinada || []);
      setLicenciasPronosticoVsReal(json.pronosticosVsReal || []);
      setLicenciasPronosticoModelo(json.modelo || null);
      setOutput(json);
    } catch (error) {
      setLicenciasPronosticoSerie([]);
      setLicenciasPronosticoVsReal([]);
      setLicenciasPronosticoModelo(null);
      setLicenciasPronosticoSearchTime(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadPadronCatastralSabana(nextOffsetValue) {
    setLoading("padronCatastral");
    setOutput("Cargando padrón catastral...");
    const t0 = performance.now();
    try {
      const hasFilters =
        !!(padronCatastralReport.q || "").trim() ||
        !!(padronCatastralReport.claveCatastral || "").trim() ||
        !!(padronCatastralReport.predioId || "").trim() ||
        !!(padronCatastralReport.propietario || "").trim() ||
        !!(padronCatastralReport.apellidoPaterno || "").trim() ||
        !!(padronCatastralReport.apellidoMaterno || "").trim() ||
        !!(padronCatastralReport.nombre || "").trim() ||
        !!(padronCatastralReport.calle || "").trim() ||
        !!(padronCatastralReport.numero || "").trim() ||
        !!(padronCatastralReport.estatus || "").trim() ||
        (padronCatastralReport.adeudo || "todos") !== "todos" ||
        !!(padronCatastralReport.fromAlta || "").trim() ||
        !!(padronCatastralReport.toAlta || "").trim();

      const pageSize = hasFilters ? 200 : 50;
      const offsetToUse =
        typeof nextOffsetValue === "number" ? nextOffsetValue : 0;

      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
        limit: String(pageSize),
        offset: String(Number.isFinite(offsetToUse) ? offsetToUse : 0)
      });
      if (padronCatastralReport.q) query.set("q", padronCatastralReport.q);
      if (padronCatastralReport.claveCatastral) query.set("claveCatastral", padronCatastralReport.claveCatastral);
      if (padronCatastralReport.claveMode) query.set("claveMode", padronCatastralReport.claveMode);
      if (padronCatastralReport.predioId) query.set("predioId", padronCatastralReport.predioId);
      if (padronCatastralReport.propietario) query.set("propietario", padronCatastralReport.propietario);
      if (padronCatastralReport.apellidoPaterno) query.set("apellidoPaterno", padronCatastralReport.apellidoPaterno);
      if (padronCatastralReport.apellidoMaterno) query.set("apellidoMaterno", padronCatastralReport.apellidoMaterno);
      if (padronCatastralReport.nombre) query.set("nombre", padronCatastralReport.nombre);
      if (padronCatastralReport.calle) query.set("calle", padronCatastralReport.calle);
      if (padronCatastralReport.numero) query.set("numero", padronCatastralReport.numero);
      if (padronCatastralReport.estatus) query.set("estatus", padronCatastralReport.estatus);
      if (padronCatastralReport.adeudo && padronCatastralReport.adeudo !== "todos") query.set("adeudo", padronCatastralReport.adeudo);
      if (padronCatastralReport.fromAlta) query.set("fromAlta", padronCatastralReport.fromAlta);
      if (padronCatastralReport.toAlta) query.set("toAlta", padronCatastralReport.toAlta);

      const response = await fetch(`/api/reportes/prediales/sabana?${query.toString()}`);
      const json = await response.json();

      const t1 = performance.now();
      setPadronCatastralSearchTime(((t1 - t0) / 1000).toFixed(2));
      setPadronCatastralRows(json.rows || []);
      setPadronCatastralHasMore(!!json.hasMore);
      setPadronCatastralNextOffset(json.nextOffset ?? null);
      setPadronCatastralReport((prev) => ({ ...prev, offset: String(json.filtros?.offset ?? offsetToUse) }));
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadCriEntes() {
    setLoading("cri-entes");
    setOutput("Cargando entes públicos (CRI)...");
    try {
      const response = await fetch("/api/reportes/cri/entes");
      const json = await response.json();
      setCriEntes(json.items || []);
      setOutput(json);
    } catch (error) {
      setCriEntes([]);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadCriReporte(nextOffsetValue) {
    setLoading("cri-report");
    setOutput("Cargando reporte CRI...");
    const t0 = performance.now();
    try {
      const offsetToUse = typeof nextOffsetValue === "number" ? nextOffsetValue : Number(criReport.offset || 0);
      const endpoint = criReport.reporte === "resumen" ? "/api/reportes/cri/resumen-por-rubro" : "/api/reportes/cri/estado-analitico";
      const query = new URLSearchParams({
        limit: criReport.limit || "200",
        offset: String(Number.isFinite(offsetToUse) ? offsetToUse : 0)
      });
      if (criReport.idEnte && criReport.reporte !== "resumen") query.set("idEnte", criReport.idEnte);
      if (criReport.ejercicioFiscal) query.set("ejercicioFiscal", criReport.ejercicioFiscal);
      if (criReport.periodo) query.set("periodo", criReport.periodo);

      const response = await fetch(`${endpoint}?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setCriReportSearchTime(((t1 - t0) / 1000).toFixed(2));
      setCriReportRows(json.rows || []);
      setCriReportHasMore(!!json.hasMore);
      setCriReportNextOffset(json.nextOffset ?? null);
      setCriReport((prev) => ({ ...prev, offset: String(json.filtros?.offset ?? offsetToUse) }));
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function exportCriCsv() {
    const endpoint = criReport.reporte === "resumen" ? "/api/reportes/cri/resumen-por-rubro.csv" : "/api/reportes/cri/estado-analitico.csv";
    const query = new URLSearchParams({
      maxRows: EXPORT_MAX_ROWS
    });
    if (criReport.idEnte && criReport.reporte !== "resumen") query.set("idEnte", criReport.idEnte);
    if (criReport.ejercicioFiscal) query.set("ejercicioFiscal", criReport.ejercicioFiscal);
    if (criReport.periodo) query.set("periodo", criReport.periodo);
    window.open(`${endpoint}?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  const criReportColumns = useMemo(() => {
    if (criReport.reporte === "resumen") {
      return [
        { key: "clave_rubro", label: "Clave rubro" },
        { key: "nombre_rubro", label: "Nombre rubro" },
        { key: "ejercicio_fiscal", label: "Ejercicio" },
        { key: "periodo", label: "Periodo" },
        { key: "Estimado", label: "Estimado" },
        { key: "Modificado", label: "Modificado" },
        { key: "Devengado", label: "Devengado" },
        { key: "Recaudado", label: "Recaudado" },
        { key: "Diferencia_Monto", label: "Diferencia" }
      ];
    }
    return [
      { key: "Ente_Publico", label: "Ente Público" },
      { key: "Nivel_Gobierno", label: "Nivel" },
      { key: "ejercicio_fiscal", label: "Ejercicio" },
      { key: "periodo", label: "Periodo" },
      { key: "clave_rubro", label: "Rubro" },
      { key: "nombre_rubro", label: "Nombre rubro" },
      { key: "clave_tipo", label: "Tipo" },
      { key: "nombre_tipo", label: "Nombre tipo" },
      { key: "Estimado", label: "Estimado" },
      { key: "Modificado", label: "Modificado" },
      { key: "Devengado", label: "Devengado" },
      { key: "Recaudado", label: "Recaudado" },
      { key: "Diferencia (%)", label: "Diferencia (%)" }
    ];
  }, [criReport.reporte]);

  function clearPadronCatastralSabana() {
    setPadronCatastralRows([]);
    setPadronCatastralHasMore(false);
    setPadronCatastralNextOffset(null);
    setPadronCatastralSearchTime(null);
    setOutput(null);
  }

  function exportPadronCatastralCsv() {
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      maxRows: padronCatastralReport.maxRows || "200000"
    });
    if (padronCatastralReport.q) query.set("q", padronCatastralReport.q);
    if (padronCatastralReport.claveCatastral) query.set("claveCatastral", padronCatastralReport.claveCatastral);
    if (padronCatastralReport.claveMode) query.set("claveMode", padronCatastralReport.claveMode);
    if (padronCatastralReport.predioId) query.set("predioId", padronCatastralReport.predioId);
    if (padronCatastralReport.propietario) query.set("propietario", padronCatastralReport.propietario);
    if (padronCatastralReport.apellidoPaterno) query.set("apellidoPaterno", padronCatastralReport.apellidoPaterno);
    if (padronCatastralReport.apellidoMaterno) query.set("apellidoMaterno", padronCatastralReport.apellidoMaterno);
    if (padronCatastralReport.nombre) query.set("nombre", padronCatastralReport.nombre);
    if (padronCatastralReport.calle) query.set("calle", padronCatastralReport.calle);
    if (padronCatastralReport.numero) query.set("numero", padronCatastralReport.numero);
    if (padronCatastralReport.estatus) query.set("estatus", padronCatastralReport.estatus);
    if (padronCatastralReport.adeudo && padronCatastralReport.adeudo !== "todos") query.set("adeudo", padronCatastralReport.adeudo);
    if (padronCatastralReport.fromAlta) query.set("fromAlta", padronCatastralReport.fromAlta);
    if (padronCatastralReport.toAlta) query.set("toAlta", padronCatastralReport.toAlta);
    window.open(`/api/reportes/prediales/sabana.csv?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  function exportPadronCatastralExcel() {
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      maxRows: padronCatastralReport.maxRows || "200000"
    });
    if (padronCatastralReport.q) query.set("q", padronCatastralReport.q);
    if (padronCatastralReport.claveCatastral) query.set("claveCatastral", padronCatastralReport.claveCatastral);
    if (padronCatastralReport.claveMode) query.set("claveMode", padronCatastralReport.claveMode);
    if (padronCatastralReport.predioId) query.set("predioId", padronCatastralReport.predioId);
    if (padronCatastralReport.propietario) query.set("propietario", padronCatastralReport.propietario);
    if (padronCatastralReport.apellidoPaterno) query.set("apellidoPaterno", padronCatastralReport.apellidoPaterno);
    if (padronCatastralReport.apellidoMaterno) query.set("apellidoMaterno", padronCatastralReport.apellidoMaterno);
    if (padronCatastralReport.nombre) query.set("nombre", padronCatastralReport.nombre);
    if (padronCatastralReport.calle) query.set("calle", padronCatastralReport.calle);
    if (padronCatastralReport.numero) query.set("numero", padronCatastralReport.numero);
    if (padronCatastralReport.estatus) query.set("estatus", padronCatastralReport.estatus);
    if (padronCatastralReport.adeudo && padronCatastralReport.adeudo !== "todos") query.set("adeudo", padronCatastralReport.adeudo);
    if (padronCatastralReport.fromAlta) query.set("fromAlta", padronCatastralReport.fromAlta);
    if (padronCatastralReport.toAlta) query.set("toAlta", padronCatastralReport.toAlta);
    window.open(`/api/reportes/prediales/sabana.xlsx?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  async function loadUmas() {
    setLoading("umas");
    setOutput("Cargando UMAs...");
    try {
      const response = await fetch("/api/config/umas");
      const json = await response.json();
      setUmaRows(json.items || []);
      setOutput(json);
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  async function loadCri() {
    setLoading("cri");
    setOutput("Cargando CRI...");
    try {
      const response = await fetch("/api/cri/catalogo");
      const json = await response.json();
      setCriCatalog(json);
      setOutput(json);
    } catch (error) {
      setCriCatalog(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function updateCreateUserField(e) {
    const { name, value, type, checked } = e.target;
    setCreateUserForm((prev) => ({ ...prev, [name]: type === "checkbox" ? !!checked : value }));
  }

  async function loadUsers() {
    setUsersError("");
    setUsersLoading(true);
    try {
      const res = await fetch("/api/users", { credentials: "include" });
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        setUsers([]);
        setUsersError((data && (data.detail || data.error)) || "No se pudo cargar usuarios.");
        return;
      }
      setUsers(Array.isArray(data.users) ? data.users : []);
    } catch (e) {
      setUsers([]);
      setUsersError("Error de conexión.");
    } finally {
      setUsersLoading(false);
    }
  }

  async function createUser() {
    if (createUserBusy) return;
    const username = String(createUserForm.username || "").trim();
    const password = String(createUserForm.password || "");
    const displayName = String(createUserForm.displayName || "").trim();
    const roleValue = String(createUserForm.role || "cajero").trim();
    if (!username || !password) {
      setUsersError("Usuario y contraseña son obligatorios.");
      return;
    }
    setUsersError("");
    setCreateUserBusy(true);
    try {
      const res = await fetch("/api/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          username,
          password,
          displayName,
          role: roleValue,
          isActive: !!createUserForm.isActive
        })
      });
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        setUsersError((data && (data.detail || data.error)) || "No se pudo crear usuario.");
        return;
      }
      setCreateUserForm({ username: "", password: "", displayName: "", role: "cajero", isActive: true });
      await loadUsers();
    } catch (e) {
      setUsersError("Error de conexión.");
    } finally {
      setCreateUserBusy(false);
    }
  }

  async function bootstrapCri() {
    if (!form.adminKey) {
      setOutput("Falta la llave admin.");
      return;
    }

    setLoading("cri-bootstrap");
    setOutput("Creando tablas CRI...");
    try {
      const response = await fetch("/api/admin/bootstrap-microservicios", {
        method: "POST",
        headers: {
          "x-admin-key": form.adminKey
        }
      });
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json")
        ? await response.json()
        : { ok: false, error: await response.text() };
      setOutput({ httpStatus: response.status, ...payload });
      if (response.ok) await loadCri();
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function updateUmaField(event) {
    const { name, value } = event.target;
    setUmaForm((prev) => ({ ...prev, [name]: value }));
  }

  async function saveUma() {
    setLoading("umas-save");
    setOutput("Guardando UMA...");
    try {
      const payload = {
        vigenciaYear: Number(umaForm.vigenciaYear),
        umaMxn: umaForm.umaMxn === "" ? null : Number(umaForm.umaMxn)
      };
      const response = await fetch("/api/config/umas", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const json = await response.json();
      setOutput(json);
      await loadUmas();
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }
 
  function editUmaRow(row) {
    if (!row) return;
    setUmaForm({
      vigenciaYear: String(row.vigenciaYear ?? ""),
      umaMxn: row.umaMxn == null ? "" : String(row.umaMxn)
    });
  }
 
  async function deleteUmaRow(vigenciaYear) {
    const year = Number(vigenciaYear);
    if (!Number.isFinite(year)) return;
    if (!window.confirm(`¿Eliminar UMA para vigencia ${year}?`)) return;
 
    setLoading("umas-delete");
    setOutput("Eliminando UMA...");
    try {
      const payload = { vigenciaYear: year, umaMxn: null };
      const response = await fetch("/api/config/umas", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const json = await response.json();
      setOutput(json);
      await loadUmas();
    } catch (error) {
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  function exportLicenciasCsv() {
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      tipo: licenciasReport.tipo || "ambos",
      maxRows: EXPORT_MAX_ROWS
    });
    if (licenciasReport.ejercicio) query.set("ejercicio", licenciasReport.ejercicio);
    if (licenciasReport.pagoFrom) query.set("pagoFrom", licenciasReport.pagoFrom);
    if (licenciasReport.pagoTo) query.set("pagoTo", licenciasReport.pagoTo);
    if (licenciasReport.todos !== "1") {
      if (licenciasReport.licenciaId) query.set("licenciaId", licenciasReport.licenciaId);
      if (licenciasReport.licenciaFrom) query.set("licenciaFrom", licenciasReport.licenciaFrom);
      if (licenciasReport.licenciaTo) query.set("licenciaTo", licenciasReport.licenciaTo);
    }
    window.open(`/api/reportes/licencias/funcionamiento.csv?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  function exportLicenciasExcel() {
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      tipo: licenciasReport.tipo || "ambos",
      maxRows: EXPORT_MAX_ROWS
    });
    if (licenciasReport.ejercicio) query.set("ejercicio", licenciasReport.ejercicio);
    if (licenciasReport.pagoFrom) query.set("pagoFrom", licenciasReport.pagoFrom);
    if (licenciasReport.pagoTo) query.set("pagoTo", licenciasReport.pagoTo);
    if (licenciasReport.todos !== "1") {
      if (licenciasReport.licenciaId) query.set("licenciaId", licenciasReport.licenciaId);
      if (licenciasReport.licenciaFrom) query.set("licenciaFrom", licenciasReport.licenciaFrom);
      if (licenciasReport.licenciaTo) query.set("licenciaTo", licenciasReport.licenciaTo);
    }
    window.open(`/api/reportes/licencias/funcionamiento.xlsx?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  function exportSaneamientoCsv() {
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      maxRows: EXPORT_MAX_ROWS
    });
    if (saneamientoReport.ejercicio) query.set("ejercicio", saneamientoReport.ejercicio);
    if (saneamientoReport.pagoFrom) query.set("pagoFrom", saneamientoReport.pagoFrom);
    if (saneamientoReport.pagoTo) query.set("pagoTo", saneamientoReport.pagoTo);
    if (saneamientoReport.todos !== "1" && saneamientoReport.licenciaId) query.set("licenciaId", saneamientoReport.licenciaId);
    window.open(`/api/reportes/saneamiento/ambiental.csv?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  function exportSaneamientoExcel() {
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      maxRows: EXPORT_MAX_ROWS
    });
    if (saneamientoReport.ejercicio) query.set("ejercicio", saneamientoReport.ejercicio);
    if (saneamientoReport.pagoFrom) query.set("pagoFrom", saneamientoReport.pagoFrom);
    if (saneamientoReport.pagoTo) query.set("pagoTo", saneamientoReport.pagoTo);
    if (saneamientoReport.todos !== "1" && saneamientoReport.licenciaId) query.set("licenciaId", saneamientoReport.licenciaId);
    window.open(`/api/reportes/saneamiento/ambiental.xlsx?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  async function loadPredialesSabana(nextOffsetValue) {
    setLoading("prediales");
    setOutput("Cargando sábana de prediales...");
    const t0 = performance.now();
    try {
      const offsetToUse =
        typeof nextOffsetValue === "number" ? nextOffsetValue : Number(predialReport.offset || 0);
      if (!offsetToUse) setPredialUniverseTotals(null);
      const todos = predialReport.todos === "1";
      const query = new URLSearchParams({
        cveFteMT: REPORT_CVE_FTE_MT,
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
      const t1 = performance.now();
      setPredialSearchTime(((t1 - t0) / 1000).toFixed(2));
      setPredialRows(json.rows || []);
      setPredialHasMore(!!json.hasMore);
      setPredialNextOffset(json.nextOffset ?? null);
      setPredialUniverseTotals(json.totals || null);
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
      cveFteMT: REPORT_CVE_FTE_MT,
      maxRows: EXPORT_MAX_ROWS
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

  function exportPredialesExcel() {
    const todos = predialReport.todos === "1";
    const query = new URLSearchParams({
      cveFteMT: REPORT_CVE_FTE_MT,
      maxRows: EXPORT_MAX_ROWS
    });
    if (todos) query.set("todos", "1");
    if (!todos && predialReport.claveCatastral) query.set("claveCatastral", predialReport.claveCatastral);
    if (!todos && predialReport.claveCatastralFrom) query.set("claveCatastralFrom", predialReport.claveCatastralFrom);
    if (!todos && predialReport.claveCatastralTo) query.set("claveCatastralTo", predialReport.claveCatastralTo);
    if (!todos && predialReport.predioId) query.set("predioId", predialReport.predioId);
    if (predialReport.ejercicio) query.set("ejercicio", predialReport.ejercicio);
    if (predialReport.pagoFrom) query.set("pagoFrom", predialReport.pagoFrom);
    if (predialReport.pagoTo) query.set("pagoTo", predialReport.pagoTo);
    window.open(`/api/reportes/prediales/sabana-pagos.xlsx?${query.toString()}`, "_blank", "noopener,noreferrer");
  }

  async function loadFactus() {
    setLoading("factus");
    setOutput("Consultando Factus...");
    const t0 = performance.now();
    try {
      const response = await fetch("/api/reportes/factus", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cveFteMT: form.cveFteMT || "MTULUM",
          input: factusInput
        })
      });
      const json = await response.json();
      const t1 = performance.now();
      setFactusSearchTime(((t1 - t0) / 1000).toFixed(2));
      setFactusRows(json.rows || []);
      setOutput(json);
    } catch (error) {
      setFactusRows([]);
      setFactusSearchTime(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
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
    const t0 = performance.now();

    try {
      const query = new URLSearchParams({
        solicitudId: String(payload.solicitudId),
        ano: String(payload.ano),
        grupoTramiteId: String(payload.grupoTramiteId),
        cveFteMT: payload.cveFteMT
      });
      const response = await fetch(`/api/fuentes?${query.toString()}`);
      const json = await response.json();
      const t1 = performance.now();
      setSearchTime(((t1 - t0) / 1000).toFixed(2));
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

  async function executeActivaciones() {
    if (!form.adminKey) {
      setOutput("Falta la llave admin.");
      return;
    }

    if (!window.confirm("Esto ejecutará actualizaciones masivas. ¿Deseas continuar?")) {
      return;
    }

    setLoading("activaciones");
    setOutput("Ejecutando activaciones...");
    setActivacionesSummary(null);

    try {
      const response = await fetch("/api/activaciones", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-admin-key": form.adminKey
        },
        body: JSON.stringify({})
      });
      const json = await response.json();
      if (json && json.ok && json.updated_rows) {
        const obligacion = Number(json.updated_rows.KUMiObligacion);
        const documento = Number(json.updated_rows.KUMiDocumento);
        const paquete = Number(json.updated_rows.COQFORMASPAGOPAQUETE);
        setActivacionesSummary({
          obligacion: Number.isFinite(obligacion) ? obligacion : 0,
          documento: Number.isFinite(documento) ? documento : 0,
          paquete: Number.isFinite(paquete) ? paquete : 0
        });
      } else {
        setActivacionesSummary(null);
      }
      setOutput(json);
    } catch (error) {
      setActivacionesSummary(null);
      setOutput({ ok: false, error: error.message });
    } finally {
      setLoading("");
    }
  }

  const criRubrosRows = useMemo(() => {
    const rubros = criCatalog?.rubros || [];
    return rubros.map((r) => ({
      clave_rubro: r.clave_rubro ?? "",
      nombre_rubro: r.nombre_rubro ?? "",
      tipos: Array.isArray(r.tipos) ? r.tipos.length : 0
    }));
  }, [criCatalog]);

  const criTiposRows = useMemo(() => {
    const rubros = criCatalog?.rubros || [];
    const out = [];
    for (const r of rubros) {
      for (const t of r.tipos || []) {
        out.push({
          clave_rubro: r.clave_rubro ?? "",
          clave_tipo: t.clave_tipo ?? "",
          nombre_tipo: t.nombre_tipo ?? ""
        });
      }
    }
    return out;
  }, [criCatalog]);

  const criClasesRows = useMemo(() => {
    const clases = criCatalog?.clases || [];
    return clases.map((c) => ({
      clave_rubro: c.clave_rubro ?? "",
      clave_tipo: c.clave_tipo ?? "",
      clave_clase: c.clave_clase ?? "",
      nombre_clase: c.nombre_clase ?? ""
    }));
  }, [criCatalog]);

  const criConceptosRows = useMemo(() => {
    const conceptos = criCatalog?.conceptos || [];
    return conceptos.map((c) => ({
      clave_rubro: c.clave_rubro ?? "",
      clave_tipo: c.clave_tipo ?? "",
      clave_clase: c.clave_clase ?? "",
      clave_concepto: c.clave_concepto ?? "",
      nombre_concepto: c.nombre_concepto ?? ""
    }));
  }, [criCatalog]);

  useEffect(() => {
    if (!allowed.includes(section)) {
      setSection(allowed.includes("inicio") ? "inicio" : allowed[0] || "ayudas");
    }
  }, [allowed.join("|"), section]);

  function sectionToPath(nextSection) {
    if (nextSection === "inicio") return "/ingresos";
    if (nextSection === "cajas") return "/ingresos/cajas";
    if (nextSection === "reportes") return "/ingresos/reportes";
    if (nextSection === "analitica") return "/ingresos/analitica";
    if (nextSection === "padronCatastral") return "/ingresos/padron-catastral";
    if (nextSection === "pasesCaja") return "/ingresos/pases-caja";
    if (nextSection === "config") return "/ingresos/configuracion";
    if (nextSection === "ayudas") return "/ingresos/ayudas";
    return "/ingresos";
  }

  function pathToSection(pathname) {
    const p = String(pathname || "");
    if (p === "/ingresos" || p === "/ingresos/") return "inicio";
    if (!p.startsWith("/ingresos/")) return null;
    const tail = p.slice("/ingresos/".length);
    if (tail === "inicio") return "inicio";
    if (tail === "cajas") return "cajas";
    if (tail === "reportes") return "reportes";
    if (tail === "analitica") return "analitica";
    if (tail === "padron-catastral") return "padronCatastral";
    if (tail === "pases-caja") return "pasesCaja";
    if (tail === "configuracion") return "config";
    if (tail === "ayudas") return "ayudas";
    return null;
  }

  function navigateSection(nextSection) {
    const next = String(nextSection || "");
    if (!next) return;
    setSection(next);
    const nextPath = sectionToPath(next);
    try {
      if (typeof window !== "undefined" && window.location && window.location.pathname !== nextPath) {
        window.history.pushState({}, "", nextPath);
      }
    } catch (e) {
    }
  }

  useEffect(() => {
    function onPop() {
      const next = pathToSection(window.location.pathname);
      if (next && allowed.includes(next)) setSection(next);
    }
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, [allowed.join("|")]);

  const sectionTitle = useMemo(() => {
    if (section === "inicio") return "Bienvenida";
    if (section === "cajas") return "Cajas";
    if (section === "ayudas") return "Ayudas";
    if (section === "reportes") return "Reportes";
    if (section === "analitica") return "Analítica";
    if (section === "padronCatastral") return "Padrón Catastral";
    if (section === "pasesCaja") return "Pases de caja";
    if (section === "config") return "Configuración";
    return "MiniAyudas";
  }, [section]);

  const ayudasTitle = useMemo(() => {
    if (ayudasSection === "factus") return "Factus";
    if (ayudasSection === "activaciones") return "Activaciones";
    return "Cambio de pases";
  }, [ayudasSection]);

  const pasesCajaTitle = useMemo(() => {
    if (pasesCajaSection === "predial") return "Predial";
    if (pasesCajaSection === "licenciasBasura") return "Licencias/Basura";
    if (pasesCajaSection === "isabi") return "ISABI";
    if (pasesCajaSection === "zofemat") return "ZOFEMAT";
    return "Saneamiento";
  }, [pasesCajaSection]);

  function CardTile({ title, description, onClick }) {
    return (
      <button type="button" className="card-tile" onClick={onClick}>
        <div className="card-tile-title">{title}</div>
        <div className="card-tile-desc">{description}</div>
        <div className="card-tile-footer">
          <span className="pill">Abrir</span>
        </div>
      </button>
    );
  }

  function renderBienvenida() {
    const displayName = String(user?.displayName || user?.username || "");
    const roleLabel = role === "admin" ? "Administrador" : role === "dir_ingresos" ? "Dir. Ingresos" : role === "cajero" ? "Cajero" : "Usuario";

    const ingresosCards = [];
    if (allowed.includes("cajas")) {
      ingresosCards.push({
        key: "cajas",
        title: "Cajas",
        description: "Operación de caja y cobro.",
        onClick: () => navigateSection("cajas")
      });
    }
    if (allowed.includes("reportes")) {
      ingresosCards.push({
        key: "reportes",
        title: "Reportes",
        description: "Consultas, listados y exportación.",
        onClick: () => navigateSection("reportes")
      });
    }
    if (allowed.includes("analitica")) {
      ingresosCards.push({
        key: "analitica",
        title: "Analítica",
        description: "Indicadores y tableros.",
        onClick: () => navigateSection("analitica")
      });
    }
    if (allowed.includes("padronCatastral")) {
      ingresosCards.push({
        key: "padronCatastral",
        title: "Padrón Catastral",
        description: "Búsqueda y reportes del padrón.",
        onClick: () => navigateSection("padronCatastral")
      });
    }
    if (allowed.includes("pasesCaja")) {
      ingresosCards.push({
        key: "pasesCaja",
        title: "Pases de caja",
        description: "Trámites y emisión de pases.",
        onClick: () => {
          navigateSection("pasesCaja");
          setPasesCajaSection("predial");
        }
      });
    }

    const adminCards = [];
    if (allowed.includes("ayudas")) {
      adminCards.push({
        key: "ayudas",
        title: "Ayudas",
        description: "Herramientas y utilidades.",
        onClick: () => navigateSection("ayudas")
      });
    }
    if (allowed.includes("config")) {
      adminCards.push({
        key: "config",
        title: "Configuración",
        description: "Parámetros y catálogos.",
        onClick: () => {
          navigateSection("config");
          setConfigSection("umas");
          loadUmas();
        }
      });
    }

    return (
      <div>
        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Bienvenido{displayName ? `, ${displayName}` : ""}</h2>
              <p>Selecciona un módulo para continuar.</p>
            </div>
            <div className="pill">{roleLabel}</div>
          </div>
        </section>

        {ingresosCards.length ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Ingresos</h2>
                <p>Módulos operativos.</p>
              </div>
            </div>
            <div className="cards-grid">
              {ingresosCards.map((c) => (
                <CardTile key={c.key} title={c.title} description={c.description} onClick={c.onClick} />
              ))}
            </div>
          </section>
        ) : null}

        {adminCards.length ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Administración</h2>
                <p>Herramientas del sistema.</p>
              </div>
            </div>
            <div className="cards-grid">
              {adminCards.map((c) => (
                <CardTile key={c.key} title={c.title} description={c.description} onClick={c.onClick} />
              ))}
            </div>
          </section>
        ) : null}
      </div>
    );
  }

  function renderPredialPaseCaja() {
    return (
      <div>
        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Pase de caja · Predial</h2>
              <p>Búsqueda de predio y cálculo de vista previa (sin timbrado / sin emisión de recibo).</p>
            </div>
            <div className="pill">
              {cajaPredialPredio ? "Predio cargado" : "Sin predio"} {cajaPredialSearchTime ? `| ⏱️ ${cajaPredialSearchTime}s` : ""}
            </div>
          </div>

          <div className="grid">
            <Field label="CveFteMT">
              <input name="cveFteMT" value={cajaPredialForm.cveFteMT} onChange={updateCajaPredialField} placeholder="MTULUM" />
            </Field>
            <Field label="PredioId" hint="Búsqueda por AlPredio.PredioId">
              <input name="predioId" value={cajaPredialForm.predioId} onChange={updateCajaPredialField} inputMode="numeric" placeholder="12345" />
            </Field>
          </div>

          <div className="actions">
            <button className="primary" type="button" onClick={cajasBuscarPredio} disabled={loading === "cajas_predio"}>
              {loading === "cajas_predio" ? "Buscando..." : "Buscar predio"}
            </button>
          </div>

          {cajaPredialError ? <div className="empty">{cajaPredialError}</div> : null}

          {cajaPredialPredio ? (
            <div className="connection-list">
              <div><span>PredioId</span><strong>{String(cajaPredialPredio.PredioId ?? "-")}</strong></div>
              <div><span>Clave catastral</span><strong>{String(cajaPredialPredio.PredioCveCatastral ?? "-")}</strong></div>
              <div><span>Nombre propietario</span><strong>{String(cajaPredialPredio.PropietarioNombre ?? "-")}</strong></div>
              <div><span>RFC</span><strong>{String(cajaPredialPredio.PropietarioRFC ?? "-")}</strong></div>
              <div>
                <span>Dirección</span>
                <strong>
                  {[
                    cajaPredialPredio.PredioCalle,
                    cajaPredialPredio.PredioNumExt,
                    cajaPredialPredio.PredioNumInt,
                    cajaPredialPredio.PredioCodigoPostal ? `CP ${cajaPredialPredio.PredioCodigoPostal}` : ""
                  ]
                    .filter(Boolean)
                    .join(" ")}
                </strong>
              </div>
              <div>
                <span>Valor catastral</span>
                <strong>
                  {moneyMx.format(
                    Number(cajaPredialPredio.PredioValuoCatastralImporte ?? cajaPredialPredio.PredioCatastralImporte ?? 0) || 0
                  )}
                </strong>
              </div>
            </div>
          ) : (
            <div className="empty">Busca un predio para mostrar sus datos.</div>
          )}
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Vista previa</h2>
              <p>Captura el periodo y parámetros. La actualización/recargos se calcula si envías INPC y tasas.</p>
            </div>
            <div className="pill">
              {cajaPredialPreview?.resumen?.total_a_pagar != null
                ? `Total: ${moneyMx.format(Number(cajaPredialPreview.resumen.total_a_pagar) || 0)}`
                : "Sin cálculo"}
            </div>
          </div>

          <div className="grid">
            <Field label="Tasa al millar">
              <input name="tasaAlMillar" value={cajaPredialForm.tasaAlMillar} onChange={updateCajaPredialField} inputMode="decimal" placeholder="3.0" />
            </Field>
            <Field label="Día vencimiento">
              <input name="diaVencimiento" value={cajaPredialForm.diaVencimiento} onChange={updateCajaPredialField} inputMode="numeric" placeholder="15" />
            </Field>
            <Field label="Fecha de pago" hint="Formato: YYYY-MM-DD">
              <input name="fechaPago" value={cajaPredialForm.fechaPago} onChange={updateCajaPredialField} placeholder="2026-04-28" />
            </Field>
            <Field label="Periodo inicio (mes)">
              <select name="periodoInicioMes" value={cajaPredialForm.periodoInicioMes} onChange={updateCajaPredialField}>
                {[
                  "Enero",
                  "Febrero",
                  "Marzo",
                  "Abril",
                  "Mayo",
                  "Junio",
                  "Julio",
                  "Agosto",
                  "Septiembre",
                  "Octubre",
                  "Noviembre",
                  "Diciembre"
                ].map((m, idx) => (
                  <option key={m} value={String(idx + 1)}>
                    {m}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Periodo inicio (año)">
              <input name="periodoInicioAnio" value={cajaPredialForm.periodoInicioAnio} onChange={updateCajaPredialField} inputMode="numeric" placeholder="2026" />
            </Field>
            <Field label="Periodo fin (mes)">
              <select name="periodoFinMes" value={cajaPredialForm.periodoFinMes} onChange={updateCajaPredialField}>
                {[
                  "Enero",
                  "Febrero",
                  "Marzo",
                  "Abril",
                  "Mayo",
                  "Junio",
                  "Julio",
                  "Agosto",
                  "Septiembre",
                  "Octubre",
                  "Noviembre",
                  "Diciembre"
                ].map((m, idx) => (
                  <option key={m} value={String(idx + 1)}>
                    {m}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Periodo fin (año)">
              <input name="periodoFinAnio" value={cajaPredialForm.periodoFinAnio} onChange={updateCajaPredialField} inputMode="numeric" placeholder="2026" />
            </Field>
          </div>

          <div className="grid">
            <Field label="Tasas recargos (JSON)" hint='Ej: {"2026": 0.015}'>
              <textarea name="tasasRecargosJson" value={cajaPredialForm.tasasRecargosJson} onChange={updateCajaPredialField} rows={6} />
            </Field>
            <Field label="Tabla INPC (JSON)" hint='Ej: {"2026-03": 135.2, "2026-02": 134.8}'>
              <textarea name="tablaINPCJson" value={cajaPredialForm.tablaINPCJson} onChange={updateCajaPredialField} rows={6} />
            </Field>
          </div>

          <div className="actions">
            <button className="primary" type="button" onClick={cajasVistaPreviaPredial} disabled={loading === "cajas_preview"}>
              {loading === "cajas_preview" ? "Calculando..." : "Calcular vista previa"}
            </button>
          </div>

          {cajaPredialError ? <div className="empty">{cajaPredialError}</div> : null}
        </section>

        {cajaPredialPreview ? (
          <>
            <section className="panel">
              <div className="panel-header">
                <div>
                  <h2>Resumen</h2>
                  <p>Totales calculados con base en el periodo y la fecha de pago.</p>
                </div>
                <div className="pill">{moneyMx.format(Number(cajaPredialPreview?.resumen?.total_a_pagar) || 0)}</div>
              </div>
              <div className="connection-list">
                <div><span>Monto anual</span><strong>{moneyMx.format(Number(cajaPredialPreview?.parametros?.monto_anual) || 0)}</strong></div>
                <div><span>Cuota mensual</span><strong>{moneyMx.format(Number(cajaPredialPreview?.parametros?.cuota_mensual) || 0)}</strong></div>
                <div><span>Cuotas originales</span><strong>{moneyMx.format(Number(cajaPredialPreview?.resumen?.total_cuotas_originales) || 0)}</strong></div>
                <div><span>Actualización</span><strong>{moneyMx.format(Number(cajaPredialPreview?.resumen?.total_actualizacion) || 0)}</strong></div>
                <div><span>Recargos</span><strong>{moneyMx.format(Number(cajaPredialPreview?.resumen?.total_recargos) || 0)}</strong></div>
                <div><span>Total a pagar</span><strong>{moneyMx.format(Number(cajaPredialPreview?.resumen?.total_a_pagar) || 0)}</strong></div>
              </div>
            </section>

            <section className="panel">
              <div className="panel-header">
                <div>
                  <h2>Desglose bimestral</h2>
                  <p>Agrupado por bimestre (suma de meses).</p>
                </div>
                <div className="pill">{(cajaPredialPreview?.desglose_bimestral || []).length} bimestres</div>
              </div>
              <TableWithColumns
                rows={(cajaPredialPreview?.desglose_bimestral || []).map((r) => ({
                  "Bimestre": r?.bimestre_label || "",
                  "Cuota original": Number(r?.subtotal_original) || 0,
                  "Importe actualización": Number(r?.subtotal_actualizacion) || 0,
                  "Importe recargos": Number(r?.subtotal_recargos) || 0,
                  "Subtotal": Number(r?.subtotal_bimestre) || 0
                }))}
                columns={[
                  { key: "Bimestre", label: "Bimestre" },
                  { key: "Cuota original", label: "Cuota original", group: "Importes" },
                  { key: "Importe actualización", label: "Importe actualización", group: "Importes" },
                  { key: "Importe recargos", label: "Importe recargos", group: "Importes" },
                  { key: "Subtotal", label: "Subtotal", group: "Importes" }
                ]}
              />
            </section>

            <section className="panel">
              <div className="panel-header">
                <div>
                  <h2>Desglose mensual</h2>
                  <p>Detalle por mes (cuota, actualización y recargos).</p>
                </div>
                <div className="pill">{(cajaPredialPreview?.desglose_mensual || []).length} meses</div>
              </div>
              <TableWithColumns
                rows={(cajaPredialPreview?.desglose_mensual || []).map((r) => ({
                  "Mes": r?.mes_label || "",
                  "Vencimiento": r?.fecha_vencimiento || "",
                  "Cuota original": Number(r?.cuota_original) || 0,
                  "Factor": r?.factor_actualizacion == null ? "" : String(r.factor_actualizacion),
                  "Monto actualizado": Number(r?.monto_actualizado) || 0,
                  "Importe actualización": Number(r?.importe_actualizacion) || 0,
                  "Meses recargo": Number(r?.meses_recargo) || 0,
                  "Tasa": r?.tasa_recargos == null ? "" : String(r.tasa_recargos),
                  "Importe recargos": Number(r?.importe_recargos) || 0,
                  "Subtotal": Number(r?.subtotal) || 0,
                  "Estatus": r?.status || ""
                }))}
                columns={[
                  { key: "Mes", label: "Mes" },
                  { key: "Vencimiento", label: "Vencimiento" },
                  { key: "Cuota original", label: "Cuota original", group: "Importes" },
                  { key: "Factor", label: "Factor", group: "Actualización" },
                  { key: "Monto actualizado", label: "Monto actualizado", group: "Importes" },
                  { key: "Importe actualización", label: "Importe actualización", group: "Importes" },
                  { key: "Meses recargo", label: "Meses", group: "Recargos" },
                  { key: "Tasa", label: "Tasa", group: "Recargos" },
                  { key: "Importe recargos", label: "Importe recargos", group: "Importes" },
                  { key: "Subtotal", label: "Subtotal", group: "Importes" },
                  { key: "Estatus", label: "Estatus" }
                ]}
              />
            </section>
          </>
        ) : null}
      </div>
    );
  }

  return (
    <div className="admin-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="brand-title">MiniAyudas</div>
          <div className="brand-subtitle">{role === "admin" ? "Panel Admin" : role === "dir_ingresos" ? "Dir. Ingresos" : role === "cajero" ? "Cajero" : "Panel"}</div>
        </div>
        <nav className="sidebar-nav">
          <button type="button" className={`nav-item ${section === "inicio" ? "active" : ""}`} onClick={() => navigateSection("inicio")}>
            Bienvenida
          </button>
        </nav>

        <div className="sidebar-footer">
          <div className={`status-dot ${connection ? "ok" : "warn"}`} />
          <div className="sidebar-status">
            <div className="status-title">Conexión</div>
            <div className="status-value">{connection ? "Activa" : "Pendiente"}</div>
          </div>
        </div>
      </aside>
      <div className="admin-main">
        <header className="topbar">
          <div className="topbar-left">
            <div className="topbar-title">{sectionTitle}</div>
            <div className="topbar-meta">
              <span className="pill">Backend: {origin}</span>
              <span className="pill">SQL: {connection ? "Activa" : "Pendiente"}</span>
              {section === "ayudas" ? <span className="pill">Módulo: {ayudasTitle}</span> : null}
              {section === "pasesCaja" ? <span className="pill">Trámite: {pasesCajaTitle}</span> : null}
            </div>
          </div>
          <div className="topbar-actions">
            {section !== "inicio" ? (
              <button className="ghost topbar-back" type="button" title="Volver a bienvenida" aria-label="Volver a bienvenida" onClick={() => navigateSection("inicio")}>
                ←
              </button>
            ) : null}
            {role === "admin" ? (
              <>
                <button className="ghost" onClick={testConnection} disabled={loading === "test"}>
                  {loading === "test" ? "Probando..." : "Probar conexión"}
                </button>
                <button
                  className="ghost"
                  type="button"
                  onClick={() => {
                    setInspectorTab("conexion");
                    setInspectorOpen(true);
                  }}
                >
                  Conexión
                </button>
                <button
                  className="ghost"
                  type="button"
                  onClick={() => {
                    setInspectorTab("respuesta");
                    setInspectorOpen(true);
                  }}
                >
                  Respuesta
                </button>
                {inspectorOpen ? (
                  <button className="ghost" type="button" onClick={() => setInspectorOpen(false)}>
                    Ocultar
                  </button>
                ) : null}
              </>
            ) : null}
            <span className="pill">{String(user?.displayName || user?.username || "")}</span>
            <button className="ghost" type="button" onClick={() => onLogout && onLogout()}>
              Salir
            </button>
          </div>
        </header>

        <div className="admin-content">
          {inspectorOpen ? (
            <section className="panel inspector-panel">
              <div className="panel-header">
                <div>
                  <h2>{inspectorTab === "conexion" ? "Conexión" : "Respuesta"}</h2>
                  <p>{inspectorTab === "conexion" ? "Detalles de la conexión actual." : "JSON de la última acción ejecutada."}</p>
                </div>
                <button className="ghost" type="button" onClick={() => setInspectorOpen(false)}>
                  Cerrar
                </button>
              </div>

              <section className="tabs tabs-secondary">
                <button
                  type="button"
                  className={`tab ${inspectorTab === "conexion" ? "active" : ""}`}
                  onClick={() => setInspectorTab("conexion")}
                >
                  Conexión
                </button>
                <button
                  type="button"
                  className={`tab ${inspectorTab === "respuesta" ? "active" : ""}`}
                  onClick={() => setInspectorTab("respuesta")}
                >
                  Respuesta
                </button>
              </section>

              {inspectorTab === "conexion" ? (
                connection ? (
                  <div className="connection-list">
                    <div><span>Servidor</span><strong>{String(connection.serverName || "-")}</strong></div>
                    <div><span>Base</span><strong>{String(connection.databaseName || "-")}</strong></div>
                    <div><span>Login</span><strong>{String(connection.loginName || "-")}</strong></div>
                    <div><span>Hora servidor</span><strong>{String(connection.serverTime || "-")}</strong></div>
                  </div>
                ) : (
                  <div className="empty">Aún no hay prueba de conexión.</div>
                )
              ) : (
                <JsonBox value={output} />
              )}
            </section>
          ) : null}

          {section === "inicio" ? renderBienvenida() : null}

          {section === "cajas" ? (
            <section className="panel">
              <div className="panel-header">
                <div>
                  <h2>Cajas</h2>
                  <p>Módulo en preparación.</p>
                </div>
              </div>
              <div className="empty">Temporalmente deshabilitado.</div>
            </section>
          ) : null}

          {section === "pasesCaja" ? (
            <div>
              <section className="panel">
                <div className="panel-header">
                  <div>
                    <h2>Pases de caja</h2>
                    <p>Trámites disponibles.</p>
                  </div>
                </div>

                <section className="tabs tabs-secondary">
                  <button
                    type="button"
                    className={`tab ${pasesCajaSection === "predial" ? "active" : ""}`}
                    onClick={() => setPasesCajaSection("predial")}
                  >
                    Predial
                  </button>
                  <button
                    type="button"
                    className={`tab ${pasesCajaSection === "licenciasBasura" ? "active" : ""}`}
                    onClick={() => setPasesCajaSection("licenciasBasura")}
                  >
                    Licencias/Basura
                  </button>
                  <button
                    type="button"
                    className={`tab ${pasesCajaSection === "isabi" ? "active" : ""}`}
                    onClick={() => setPasesCajaSection("isabi")}
                  >
                    ISABI
                  </button>
                  <button
                    type="button"
                    className={`tab ${pasesCajaSection === "zofemat" ? "active" : ""}`}
                    onClick={() => setPasesCajaSection("zofemat")}
                  >
                    ZOFEMAT
                  </button>
                  <button
                    type="button"
                    className={`tab ${pasesCajaSection === "saneamiento" ? "active" : ""}`}
                    onClick={() => setPasesCajaSection("saneamiento")}
                  >
                    Saneamiento
                  </button>
                </section>
              </section>

              {pasesCajaSection === "predial" ? (
                renderPredialPaseCaja()
              ) : (
                <section className="panel">
                  <div className="empty">Pendiente de implementación.</div>
                </section>
              )}
            </div>
          ) : null}

      {section === "ayudas" ? (
        <>
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Ayudas</h2>
                <p>Selecciona el módulo.</p>
              </div>
            </div>

            <section className="tabs tabs-secondary">
              <button
                type="button"
                className={`tab ${ayudasSection === "cambio" ? "active" : ""}`}
                onClick={() => setAyudasSection("cambio")}
              >
                Cambio de pases
              </button>
              <button
                type="button"
                className={`tab ${ayudasSection === "factus" ? "active" : ""}`}
                onClick={() => setAyudasSection("factus")}
              >
                Factus
              </button>
              <button
                type="button"
                className={`tab ${ayudasSection === "activaciones" ? "active" : ""}`}
                onClick={() => setAyudasSection("activaciones")}
              >
                Activaciones
              </button>
            </section>
          </section>

          {ayudasSection === "cambio" ? (
            <div>
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
                  <div className="pill">
                    {rows.length} filas {searchTime ? `| ⏱️ ${searchTime}s` : ""}
                  </div>
                </div>
                <DataTable rows={rows} />
              </section>
            </div>
          ) : null}

          {ayudasSection === "factus" ? (
            <>
              <section className="panel">
                <div className="panel-header">
                  <div>
                    <h2>Factus</h2>
                    <p>Captura serie y folios para consultar el desglose del recibo.</p>
                  </div>
                  <div className="pill">
                    {factusRows.length} filas {factusSearchTime ? `| ⏱️ ${factusSearchTime}s` : ""}
                  </div>
                </div>

                <div className="grid">
                  <Field label="CveFteMT">
                    <input name="cveFteMT" value={form.cveFteMT} onChange={updateField} />
                  </Field>
                  <Field
                    label="Serie y folios"
                    hint="Ejemplo: Serie AU - Folios 3400, 5000, 3350 (una serie por línea, respeta el orden)"
                  >
                    <textarea
                      value={factusInput}
                      onChange={(e) => setFactusInput(e.target.value)}
                      rows={5}
                      placeholder={"Serie AU - Folios 3400, 5000, 3350\nSerie BA - Folios 120, 121"}
                    />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={loadFactus} disabled={loading === "factus"}>
                    {loading === "factus" ? "Consultando..." : "Consultar Factus"}
                  </button>
                </div>
              </section>

              <section className="panel">
                <div className="panel-header">
                  <div>
                    <h2>Resultados</h2>
                    <p>La primera fila del recibo muestra los datos generales; las siguientes filas son el desglose por concepto.</p>
                  </div>
                </div>
                <TableWithColumns
                  rows={factusRows}
                  columns={[
                    { key: "Serie", label: "Serie" },
                    { key: "Folio", label: "Folio" },
                    { key: "Fecha", label: "Fecha" },
                    { key: "Nombre", label: "Nombre" },
                    { key: "RFC", label: "RFC" },
                    { key: "Observaciones", label: "Observaciones" },
                    { key: "Concepto", label: "Concepto" },
                    { key: "Total", label: "Total" }
                  ]}
                />
              </section>
            </>
          ) : null}

          {ayudasSection === "activaciones" ? (
            <>
              <section className="panel">
                <div className="panel-header">
                  <div>
                    <h2>Activaciones</h2>
                    <p>Actualiza estatus a AP y cuenta bancaria (desde el inicio del mes en curso).</p>
                  </div>
                </div>

                <div className="grid">
                  <Field label="Llave admin" hint="Obligatoria para ejecutar">
                    <input name="adminKey" type="password" value={form.adminKey} onChange={updateField} />
                  </Field>
                </div>

                <div className="actions">
                  <button className="danger" onClick={executeActivaciones} disabled={loading === "activaciones"}>
                    {loading === "activaciones" ? "Ejecutando..." : "Ejecutar activaciones"}
                  </button>
                </div>
              </section>

              {activacionesSummary ? (
                <section className="panel">
                  <div className="panel-header">
                    <div>
                      <h2>Resumen</h2>
                      <p>Conteo devuelto por la ejecución.</p>
                    </div>
                  </div>
                  <DataTable
                    rows={[
                      { Tabla: "PendientesKUMiObligacion", Registros: `${activacionesSummary.obligacion} registros` },
                      { Tabla: "PendientesKUMiDocumento", Registros: `${activacionesSummary.documento} registros` },
                      { Tabla: "COQFORMASPAGOPAQUETE", Registros: `${activacionesSummary.paquete} registros` }
                    ]}
                  />
                </section>
              ) : null}
            </>
          ) : null}
        </>
      ) : null}

      {section === "reportes" ? (
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
              className={`tab ${reportSection === "cri" ? "active" : ""}`}
              onClick={() => {
                setReportSection("cri");
                loadCriEntes();
              }}
            >
              CRI
            </button>
          </section>

          {reportSection === "prediales" ? (
            <>
                <div className="grid">
                  <Field label="Todos" hint="Ignora Predio/Claves">
                    <div className="toggle-switch">
                      <button
                        type="button"
                        className={predialReport.todos === "0" ? "active" : ""}
                        onClick={() => updatePredialReport({ target: { name: "todos", value: "0" } })}
                      >
                        No
                      </button>
                      <button
                        type="button"
                        className={predialReport.todos === "1" ? "active" : ""}
                        onClick={() => updatePredialReport({ target: { name: "todos", value: "1" } })}
                      >
                        Sí
                      </button>
                    </div>
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
                </div>

                <div className="actions">
                  <button className="primary" onClick={() => loadPredialesSabana()} disabled={loading === "prediales"}>
                    {loading === "prediales" ? "Cargando..." : "Cargar sábana"}
                  </button>
                  <button className="ghost" onClick={exportPredialesCsv}>
                    Exportar CSV
                  </button>
                  <button className="ghost" onClick={exportPredialesExcel}>
                    Exportar Excel
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
                  <div className="pill">
                    {predialRows.length} filas {predialSearchTime ? `| ⏱️ ${predialSearchTime}s` : ""}
                  </div>
                </div>
                {predialUniverseTotals != null || predialRows.length ? (
                  <div className="totals-bar" role="group" aria-label="Totales (universo)">
                    <div className="totals-item">
                      <div className="totals-label">Impuesto Corriente y Anticipado</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Impuesto Corriente y Anticipado"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Rezago años anteriores</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Rezago años anteriores"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Rezago</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Rezago"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Adicional</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Adicional"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Actualizacion</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Actualizacion"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Recargos</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Recargos"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Requerimiento</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Requerimiento"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Embargo</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Embargo"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Multa</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Multa"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Descuentos</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Descuentos"] || 0)}</div>
                    </div>
                    <div className="totals-item totals-item-total">
                      <div className="totals-label">Total</div>
                      <div className="totals-value">{moneyMx.format(predialTotals["Total"] || 0)}</div>
                    </div>
                  </div>
                ) : null}

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
              <>
                <div className="grid">
                  <Field label="Todos" hint="Ignora No. Licencia/Rangos">
                    <div className="toggle-switch">
                      <button
                        type="button"
                        className={licenciasReport.todos === "0" ? "active" : ""}
                        onClick={() => updateLicenciasReport({ target: { name: "todos", value: "0" } })}
                      >
                        No
                      </button>
                      <button
                        type="button"
                        className={licenciasReport.todos === "1" ? "active" : ""}
                        onClick={() => updateLicenciasReport({ target: { name: "todos", value: "1" } })}
                      >
                        Sí
                      </button>
                    </div>
                  </Field>
                  <Field label="Tipo">
                    <div className="toggle-switch disabled">
                      <button
                        type="button"
                        className={licenciasReport.tipo === "basura" ? "active" : ""}
                      >
                        Basura
                      </button>
                      <button
                        type="button"
                        className={licenciasReport.tipo === "ambos" ? "active" : ""}
                      >
                        Ambos
                      </button>
                      <button
                        type="button"
                        className={licenciasReport.tipo === "licencia" ? "active" : ""}
                      >
                        Licencia
                      </button>
                    </div>
                  </Field>
                  <Field label="No. Licencia" hint="Busca en ReciboPredioId">
                    <input
                      name="licenciaId"
                      value={licenciasReport.licenciaId}
                      onChange={updateLicenciasReport}
                      inputMode="numeric"
                      placeholder="3443"
                      disabled={licenciasReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Rango licencia (desde)">
                    <input
                      name="licenciaFrom"
                      value={licenciasReport.licenciaFrom}
                      onChange={updateLicenciasReport}
                      inputMode="numeric"
                      placeholder="3000"
                      disabled={licenciasReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Rango licencia (hasta)">
                    <input
                      name="licenciaTo"
                      value={licenciasReport.licenciaTo}
                      onChange={updateLicenciasReport}
                      inputMode="numeric"
                      placeholder="4000"
                      disabled={licenciasReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Ejercicio Fiscal" hint="Eje: 2025 (rango 01/01/2025 a 31/12/2025)">
                    <input
                      name="ejercicio"
                      value={licenciasReport.ejercicio}
                      onChange={updateLicenciasReport}
                      inputMode="numeric"
                      placeholder="2025"
                    />
                  </Field>
                  <Field label="Fecha desde" hint="YYYY-MM-DD (si llenas rango, ignora Ejercicio)">
                    <input name="pagoFrom" value={licenciasReport.pagoFrom} onChange={updateLicenciasReport} placeholder="2025-01-01" />
                  </Field>
                  <Field label="Fecha hasta" hint="YYYY-MM-DD">
                    <input name="pagoTo" value={licenciasReport.pagoTo} onChange={updateLicenciasReport} placeholder="2026-01-31" />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={() => loadLicenciasSabana()} disabled={loading === "licencias"}>
                    {loading === "licencias" ? "Cargando..." : "Cargar sábana"}
                  </button>
                  <button className="danger" onClick={clearLicenciasSabana} type="button">
                    Borrar sábana
                  </button>
                  <button className="ghost" onClick={exportLicenciasCsv}>
                    Exportar CSV
                  </button>
                  <button className="ghost" onClick={exportLicenciasExcel}>
                    Exportar Excel
                  </button>
                  {licenciasHasMore ? (
                    <button
                      className="ghost"
                      onClick={() => (licenciasNextOffset != null ? loadLicenciasSabana(licenciasNextOffset) : null)}
                      disabled={loading === "licencias" || licenciasNextOffset == null}
                    >
                      Siguiente página
                    </button>
                  ) : null}
                  <div className="pill">
                    {licenciasRows.length} filas {licenciasSearchTime ? `| ⏱️ ${licenciasSearchTime}s` : ""}
                  </div>
                </div>

                {licenciasUniverseTotals != null || licenciasRows.length ? (
                  <div className="totals-bar" role="group" aria-label="Totales (universo)">
                    <div className="totals-item">
                      <div className="totals-label">Licencia</div>
                      <div className="totals-value">{moneyMx.format(licenciasTotals["Licencia"] || 0)}</div>
                    </div>
                  <div className="totals-item">
                    <div className="totals-label">Lic Renovación</div>
                    <div className="totals-value">{moneyMx.format(licenciasTotals["Lic Renovación"] || 0)}</div>
                  </div>
                    <div className="totals-item">
                      <div className="totals-label">Basura</div>
                      <div className="totals-value">{moneyMx.format(licenciasTotals["Basura"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Actualizaciones</div>
                      <div className="totals-value">{moneyMx.format(licenciasTotals["Actualizaciones"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Recargos</div>
                      <div className="totals-value">{moneyMx.format(licenciasTotals["Recargos"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Otros</div>
                      <div className="totals-value">{moneyMx.format(licenciasTotals["Otros"] || 0)}</div>
                    </div>
                    <div className="totals-item totals-item-total">
                      <div className="totals-label">Total</div>
                      <div className="totals-value">{moneyMx.format(licenciasTotals["Total"] || 0)}</div>
                    </div>
                  </div>
                ) : null}

                <div className="table-space">
                  <TableWithColumns
                    rows={licenciasRows}
                    columns={[
                      { key: "No. Licencia", label: "No. Licencia" },
                      { key: "Serie", label: "Serie" },
                      { key: "Folio", label: "Folio" },
                      { key: "Fecha", label: "Fecha" },
                      { key: "Nombre", label: "Nombre" },
                      { key: "RFC", label: "RFC" },
                      { key: "Observaciones", label: "Observaciones" },
                        { key: "Domicilio Licencia", label: "Domicilio Licencia" },
                        { key: "Domicilio Local", label: "Domicilio Local" },
                        { key: "Tipo Establecimiento", label: "Tipo Establecimiento" },
                        { key: "Giro", label: "Giro" },
                        { key: "Base Licencia Nueva", label: "Base Licencia Nueva" },
                        { key: "Base Licencia Renovación", label: "Base Licencia Renovación" },
                        { key: "Base Basura Nueva", label: "Base Basura Nueva" },
                      { key: "Tipo", label: "Tipo" },
                        { key: "Tarifa Licencia", label: "Tarifa Licencia" },
                        { key: "Tarifa Basura", label: "Tarifa Basura" },
                      { key: "Licencia", label: "Licencia", group: "Lo Cobrado" },
                      { key: "Lic Renovación", label: "Lic Renovación", group: "Lo Cobrado" },
                      { key: "Basura", label: "Basura", group: "Lo Cobrado" },
                      { key: "Actualizaciones", label: "Actualizaciones", group: "Lo Cobrado" },
                      { key: "Recargos", label: "Recargos", group: "Lo Cobrado" },
                      { key: "Otros", label: "Otros", group: "Lo Cobrado" },
                      { key: "Total", label: "Total", group: "Lo Cobrado" }
                    ]}
                  />
                </div>
              </>
            ) : null}

            {reportSection === "saneamiento" ? (
              <>
                <div className="grid">
                  <Field label="Todos" hint="Ignora No. Licencia">
                    <div className="toggle-switch">
                      <button
                        type="button"
                        className={saneamientoReport.todos === "0" ? "active" : ""}
                        onClick={() => updateSaneamientoReport({ target: { name: "todos", value: "0" } })}
                      >
                        No
                      </button>
                      <button
                        type="button"
                        className={saneamientoReport.todos === "1" ? "active" : ""}
                        onClick={() => updateSaneamientoReport({ target: { name: "todos", value: "1" } })}
                      >
                        Sí
                      </button>
                    </div>
                  </Field>
                  <Field label="No. Licencia" hint="Busca en TESANEAMIENTOAMBIENTAL.LicenciasFuncionamientoId">
                    <input
                      name="licenciaId"
                      value={saneamientoReport.licenciaId}
                      onChange={updateSaneamientoReport}
                      inputMode="numeric"
                      placeholder="568"
                      disabled={saneamientoReport.todos === "1"}
                    />
                  </Field>
                  <Field label="Ejercicio Fiscal" hint="Eje: 2025 (rango 01/01/2025 a 31/12/2025)">
                    <input
                      name="ejercicio"
                      value={saneamientoReport.ejercicio}
                      onChange={updateSaneamientoReport}
                      inputMode="numeric"
                      placeholder="2025"
                    />
                  </Field>
                  <Field label="Fecha desde" hint="YYYY-MM-DD (si llenas rango, ignora Ejercicio)">
                    <input name="pagoFrom" value={saneamientoReport.pagoFrom} onChange={updateSaneamientoReport} placeholder="2025-01-01" />
                  </Field>
                  <Field label="Fecha hasta" hint="YYYY-MM-DD">
                    <input name="pagoTo" value={saneamientoReport.pagoTo} onChange={updateSaneamientoReport} placeholder="2025-12-31" />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={() => loadSaneamientoSabana()} disabled={loading === "saneamiento"}>
                    {loading === "saneamiento" ? "Cargando..." : "Cargar sábana"}
                  </button>
                  <button className="danger" onClick={clearSaneamientoSabana} type="button">
                    Borrar sábana
                  </button>
                  <button className="ghost" onClick={exportSaneamientoCsv} type="button">
                    Descargar CSV
                  </button>
                  <button className="ghost" onClick={exportSaneamientoExcel} type="button">
                    Descargar Excel
                  </button>
                  {saneamientoHasMore ? (
                    <button
                      className="ghost"
                      onClick={() => (saneamientoNextOffset != null ? loadSaneamientoSabana(saneamientoNextOffset) : null)}
                      disabled={loading === "saneamiento" || saneamientoNextOffset == null}
                    >
                      Siguiente página
                    </button>
                  ) : null}
                  <div className="pill">
                    {saneamientoRows.length} filas {saneamientoSearchTime ? `| ⏱️ ${saneamientoSearchTime}s` : ""}
                  </div>
                </div>

                {saneamientoUniverseTotals != null || saneamientoRows.length ? (
                  <div className="totals-bar" role="group" aria-label="Totales (universo)">
                    <div className="totals-item">
                      <div className="totals-label">Derecho</div>
                      <div className="totals-value">{moneyMx.format(saneamientoTotals["Derecho"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Actualizaciones</div>
                      <div className="totals-value">{moneyMx.format(saneamientoTotals["Actualizaciones"] || 0)}</div>
                    </div>
                    <div className="totals-item">
                      <div className="totals-label">Recargos</div>
                      <div className="totals-value">{moneyMx.format(saneamientoTotals["Recargos"] || 0)}</div>
                    </div>
                    <div className="totals-item totals-item-total">
                      <div className="totals-label">Total</div>
                      <div className="totals-value">{moneyMx.format(saneamientoTotals["Total"] || 0)}</div>
                    </div>
                  </div>
                ) : null}

                <div className="table-space">
                  <TableWithColumns
                    rows={saneamientoRows}
                    columns={[
                      { key: "PredioId", label: "Padrón" },
                      { key: "No. Licencia", label: "No. Licencia" },
                      { key: "Serie", label: "Serie" },
                      { key: "Folio", label: "Folio" },
                      { key: "Fecha", label: "Fecha" },
                      { key: "Nombre", label: "Nombre" },
                      { key: "RFC", label: "RFC" },
                      { key: "Observaciones", label: "Observaciones" },
                      { key: "Domicilio Licencia", label: "Domicilio Licencia" },
                      { key: "Domicilio Local", label: "Domicilio Local" },
                      { key: "Giro", label: "Giro" },
                      { key: "No. Cuartos", label: "No. Cuartos" },
                      { key: "Periodo pagado", label: "Periodo pagado" },
                      { key: "Derecho", label: "Derecho", group: "Lo Cobrado" },
                      { key: "Actualizaciones", label: "Actualizaciones", group: "Lo Cobrado" },
                      { key: "Recargos", label: "Recargos", group: "Lo Cobrado" },
                      { key: "Total", label: "Total", group: "Lo Cobrado" }
                    ]}
                  />
                </div>
              </>
            ) : null}

            {reportSection === "cri" ? (
              <>
                <div className="grid">
                  <Field label="Reporte">
                    <select name="reporte" value={criReport.reporte} onChange={updateCriReport}>
                      <option value="estado">Estado Analítico de Ingresos</option>
                      <option value="resumen">Resumen por Rubro</option>
                    </select>
                  </Field>
                  <Field label="Ente Público" hint="Solo aplica al estado analítico">
                    <select name="idEnte" value={criReport.idEnte} onChange={updateCriReport} disabled={criReport.reporte === "resumen"}>
                      <option value="">Todos</option>
                      {(criEntes || []).map((e) => (
                        <option key={String(e.id_ente)} value={String(e.id_ente)}>
                          {String(e.ejercicio_fiscal)} — {String(e.nombre)}
                        </option>
                      ))}
                    </select>
                  </Field>
                  <Field label="Ejercicio fiscal">
                    <input name="ejercicioFiscal" value={criReport.ejercicioFiscal} onChange={updateCriReport} inputMode="numeric" placeholder="2026" />
                  </Field>
                  <Field label="Periodo" hint="1 a 12 (vacío = todos)">
                    <input name="periodo" value={criReport.periodo} onChange={updateCriReport} inputMode="numeric" placeholder="1" />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={() => loadCriReporte()} disabled={loading === "cri-report"}>
                    {loading === "cri-report" ? "Cargando..." : "Cargar reporte"}
                  </button>
                  <button className="ghost" onClick={exportCriCsv} type="button">
                    Exportar CSV
                  </button>
                  {criReportHasMore ? (
                    <button
                      className="ghost"
                      onClick={() => (criReportNextOffset != null ? loadCriReporte(criReportNextOffset) : null)}
                      disabled={loading === "cri-report" || criReportNextOffset == null}
                      type="button"
                    >
                      Siguiente página
                    </button>
                  ) : null}
                  <div className="pill">
                    {criReportRows.length} filas {criReportSearchTime ? `| ⏱️ ${criReportSearchTime}s` : ""}
                  </div>
                </div>

                <div className="table-space">
                  <TableWithColumns rows={criReportRows} columns={criReportColumns} />
                </div>
              </>
            ) : null}
        </section>
      ) : null}

      {section === "analitica" ? (
        <>
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Analítica</h2>
                <p>Exploración y métricas por módulo.</p>
              </div>
            </div>

            <section className="tabs tabs-secondary">
              <button
                type="button"
                className={`tab ${analiticaSection === "saneamiento" ? "active" : ""}`}
                onClick={() => setAnaliticaSection("saneamiento")}
              >
                Saneamiento Ambiental
              </button>
              <button
                type="button"
                className={`tab ${analiticaSection === "prediales" ? "active" : ""}`}
                onClick={() => setAnaliticaSection("prediales")}
              >
                Prediales
              </button>
              <button
                type="button"
                className={`tab ${analiticaSection === "licencias" ? "active" : ""}`}
                onClick={() => setAnaliticaSection("licencias")}
              >
                Licencia de Funcionamiento
              </button>
            </section>

            {analiticaSection === "saneamiento" ? (
              <>
                <div className="grid">
                  <Field label="Ejercicio Fiscal" hint="Eje: 2025 (rango 01/01/2025 a 31/12/2025)">
                    <input
                      name="ejercicio"
                      value={saneamientoAnalytics.ejercicio}
                      onChange={updateSaneamientoAnalytics}
                      inputMode="numeric"
                      placeholder="2025"
                    />
                  </Field>
                  <Field label="Fecha desde" hint="YYYY-MM-DD (si llenas rango, ignora Ejercicio)">
                    <input
                      name="pagoFrom"
                      value={saneamientoAnalytics.pagoFrom}
                      onChange={updateSaneamientoAnalytics}
                      placeholder="2025-01-01"
                    />
                  </Field>
                  <Field label="Fecha hasta" hint="YYYY-MM-DD">
                    <input
                      name="pagoTo"
                      value={saneamientoAnalytics.pagoTo}
                      onChange={updateSaneamientoAnalytics}
                      placeholder="2025-12-31"
                    />
                  </Field>
                  <Field label="No. Licencia" hint="Filtra por LicenciasFuncionamientoId">
                    <input
                      name="licenciaId"
                      value={saneamientoAnalytics.licenciaId}
                      onChange={updateSaneamientoAnalytics}
                      inputMode="numeric"
                      placeholder="568"
                    />
                  </Field>
                  <Field label="Backtest (meses)" hint="Pronóstico vs real en meses recientes">
                    <input
                      name="backtestMonths"
                      value={saneamientoAnalytics.backtestMonths}
                      onChange={updateSaneamientoAnalytics}
                      inputMode="numeric"
                      placeholder="6"
                    />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={loadSaneamientoAnalytics} disabled={loading === "saneamiento-analytics"}>
                    {loading === "saneamiento-analytics" ? "Cargando..." : "Cargar analítica"}
                  </button>
                  <button className="primary" onClick={loadSaneamientoPronosticos} disabled={loading === "saneamiento-pronosticos"} type="button">
                    {loading === "saneamiento-pronosticos" ? "Calculando..." : "Pronosticos"}
                  </button>
                  <button
                    className="ghost"
                    onClick={() => (saneamientoCanceladosOpen ? setSaneamientoCanceladosOpen(false) : loadSaneamientoCancelados(0))}
                    disabled={loading === "saneamiento-cancelados"}
                    type="button"
                  >
                    {loading === "saneamiento-cancelados"
                      ? "Cargando..."
                      : saneamientoCanceladosOpen
                        ? "Ocultar cancelados"
                        : "Ver cancelados"}
                  </button>
                  <div className="pill">
                    {saneamientoAnalyticsSeries.length} periodos
                    {saneamientoAnalyticsSearchTime ? ` | ⏱️ ${saneamientoAnalyticsSearchTime}s` : ""}
                    {` | Cancelados: ${saneamientoAnalyticsCanceladosCount}`}
                    {saneamientoPronosticoSearchTime ? ` | Pronosticos: ⏱️ ${saneamientoPronosticoSearchTime}s` : ""}
                  </div>
                </div>

                {saneamientoAnalyticsTotals ? (
                  <div className="hero-stats" role="group" aria-label="KPIs">
                    <StatCard title="Derecho" value={moneyMx.format(Number(saneamientoAnalyticsTotals.Derecho || 0))} />
                    <StatCard title="Actualizaciones" value={moneyMx.format(Number(saneamientoAnalyticsTotals.Actualizaciones || 0))} />
                    <StatCard title="Recargos" value={moneyMx.format(Number(saneamientoAnalyticsTotals.Recargos || 0))} />
                    <StatCard title="Total" tone="success" value={moneyMx.format(Number(saneamientoAnalyticsTotals.Total || 0))} />
                  </div>
                ) : null}
              </>
            ) : null}

            {analiticaSection === "prediales" ? (
              <>
                <div className="grid">
                  <Field label="Todos" hint="Ignora filtros de predio/clave">
                    <div className="toggle-switch">
                      <button
                        type="button"
                        className={predialesAnalytics.todos === "0" ? "active" : ""}
                        onClick={() => updatePredialesAnalytics({ target: { name: "todos", value: "0" } })}
                      >
                        No
                      </button>
                      <button
                        type="button"
                        className={predialesAnalytics.todos === "1" ? "active" : ""}
                        onClick={() => updatePredialesAnalytics({ target: { name: "todos", value: "1" } })}
                      >
                        Sí
                      </button>
                    </div>
                  </Field>
                  <Field label="Clave catastral" hint="Exacta (si llenas, ignora rangos)">
                    <input
                      name="claveCatastral"
                      value={predialesAnalytics.claveCatastral}
                      onChange={updatePredialesAnalytics}
                      placeholder="010101010101010"
                      disabled={predialesAnalytics.todos === "1"}
                    />
                  </Field>
                  <Field label="Clave catastral (desde)">
                    <input
                      name="claveCatastralFrom"
                      value={predialesAnalytics.claveCatastralFrom}
                      onChange={updatePredialesAnalytics}
                      placeholder="010101010101010"
                      disabled={predialesAnalytics.todos === "1"}
                    />
                  </Field>
                  <Field label="Clave catastral (hasta)">
                    <input
                      name="claveCatastralTo"
                      value={predialesAnalytics.claveCatastralTo}
                      onChange={updatePredialesAnalytics}
                      placeholder="010101010101999"
                      disabled={predialesAnalytics.todos === "1"}
                    />
                  </Field>
                  <Field label="Padrón" hint="PredioId">
                    <input
                      name="predioId"
                      value={predialesAnalytics.predioId}
                      onChange={updatePredialesAnalytics}
                      inputMode="numeric"
                      placeholder="12345"
                      disabled={predialesAnalytics.todos === "1"}
                    />
                  </Field>
                  <Field label="Ejercicio Fiscal" hint="Eje: 2025 (rango 01/01/2025 a 31/12/2025)">
                    <input
                      name="ejercicio"
                      value={predialesAnalytics.ejercicio}
                      onChange={updatePredialesAnalytics}
                      inputMode="numeric"
                      placeholder="2025"
                    />
                  </Field>
                  <Field label="Fecha desde" hint="YYYY-MM-DD (si llenas rango, ignora Ejercicio)">
                    <input name="pagoFrom" value={predialesAnalytics.pagoFrom} onChange={updatePredialesAnalytics} placeholder="2025-01-01" />
                  </Field>
                  <Field label="Fecha hasta" hint="YYYY-MM-DD">
                    <input name="pagoTo" value={predialesAnalytics.pagoTo} onChange={updatePredialesAnalytics} placeholder="2025-12-31" />
                  </Field>
                  <Field label="Backtest (meses)" hint="Pronóstico vs real en meses recientes">
                    <input
                      name="backtestMonths"
                      value={predialesAnalytics.backtestMonths}
                      onChange={updatePredialesAnalytics}
                      inputMode="numeric"
                      placeholder="6"
                    />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={loadPredialesAnalytics} disabled={loading === "prediales-analytics"}>
                    {loading === "prediales-analytics" ? "Cargando..." : "Cargar analítica"}
                  </button>
                  <button className="primary" onClick={loadPredialesPronosticos} disabled={loading === "prediales-pronosticos"} type="button">
                    {loading === "prediales-pronosticos" ? "Calculando..." : "Pronosticos"}
                  </button>
                  <div className="pill">
                    {predialesAnalyticsSeries.length} periodos
                    {predialesAnalyticsSearchTime ? ` | ⏱️ ${predialesAnalyticsSearchTime}s` : ""}
                    {predialesPronosticoSearchTime ? ` | Pronosticos: ⏱️ ${predialesPronosticoSearchTime}s` : ""}
                  </div>
                </div>

                {predialesAnalyticsTotals ? (
                  <div className="hero-stats" role="group" aria-label="KPIs">
                    <StatCard
                      title="Impuesto"
                      value={moneyMx.format(Number(predialesAnalyticsTotals["Impuesto Corriente y Anticipado"] || 0))}
                    />
                    <StatCard
                      title="Rezago ant."
                      value={moneyMx.format(Number(predialesAnalyticsTotals["Rezago años anteriores"] || 0))}
                    />
                    <StatCard title="Adicional" value={moneyMx.format(Number(predialesAnalyticsTotals["Adicional"] || 0))} />
                    <StatCard title="Total" tone="success" value={moneyMx.format(Number(predialesAnalyticsTotals["Total"] || 0))} />
                  </div>
                ) : null}
              </>
            ) : null}

            {analiticaSection === "licencias" ? (
              <>
                <div className="grid">
                  <Field label="Tipo" hint="Ambos / Licencia / Basura">
                    <div className="toggle-switch">
                      <button
                        type="button"
                        className={licenciasAnalytics.tipo === "basura" ? "active" : ""}
                        onClick={() => updateLicenciasAnalytics({ target: { name: "tipo", value: "basura" } })}
                      >
                        Basura
                      </button>
                      <button
                        type="button"
                        className={licenciasAnalytics.tipo === "ambos" ? "active" : ""}
                        onClick={() => updateLicenciasAnalytics({ target: { name: "tipo", value: "ambos" } })}
                      >
                        Ambos
                      </button>
                      <button
                        type="button"
                        className={licenciasAnalytics.tipo === "licencia" ? "active" : ""}
                        onClick={() => updateLicenciasAnalytics({ target: { name: "tipo", value: "licencia" } })}
                      >
                        Licencia
                      </button>
                    </div>
                  </Field>
                  <Field label="No. Licencia" hint="Busca en ReciboPredioId">
                    <input
                      name="licenciaId"
                      value={licenciasAnalytics.licenciaId}
                      onChange={updateLicenciasAnalytics}
                      inputMode="numeric"
                      placeholder="3443"
                    />
                  </Field>
                  <Field label="Rango licencia (desde)">
                    <input
                      name="licenciaFrom"
                      value={licenciasAnalytics.licenciaFrom}
                      onChange={updateLicenciasAnalytics}
                      inputMode="numeric"
                      placeholder="3000"
                    />
                  </Field>
                  <Field label="Rango licencia (hasta)">
                    <input
                      name="licenciaTo"
                      value={licenciasAnalytics.licenciaTo}
                      onChange={updateLicenciasAnalytics}
                      inputMode="numeric"
                      placeholder="4000"
                    />
                  </Field>
                  <Field label="Ejercicio Fiscal" hint="Eje: 2025 (rango 01/01/2025 a 31/12/2025)">
                    <input
                      name="ejercicio"
                      value={licenciasAnalytics.ejercicio}
                      onChange={updateLicenciasAnalytics}
                      inputMode="numeric"
                      placeholder="2025"
                    />
                  </Field>
                  <Field label="Fecha desde" hint="YYYY-MM-DD (si llenas rango, ignora Ejercicio)">
                    <input name="pagoFrom" value={licenciasAnalytics.pagoFrom} onChange={updateLicenciasAnalytics} placeholder="2025-01-01" />
                  </Field>
                  <Field label="Fecha hasta" hint="YYYY-MM-DD">
                    <input name="pagoTo" value={licenciasAnalytics.pagoTo} onChange={updateLicenciasAnalytics} placeholder="2025-12-31" />
                  </Field>
                  <Field label="Backtest (meses)" hint="Pronóstico vs real en meses recientes">
                    <input
                      name="backtestMonths"
                      value={licenciasAnalytics.backtestMonths}
                      onChange={updateLicenciasAnalytics}
                      inputMode="numeric"
                      placeholder="6"
                    />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={loadLicenciasAnalytics} disabled={loading === "licencias-analytics"}>
                    {loading === "licencias-analytics" ? "Cargando..." : "Cargar analítica"}
                  </button>
                  <button className="primary" onClick={loadLicenciasPronosticos} disabled={loading === "licencias-pronosticos"} type="button">
                    {loading === "licencias-pronosticos" ? "Calculando..." : "Pronosticos"}
                  </button>
                  <div className="pill">
                    {licenciasAnalyticsSeries.length} periodos
                    {licenciasAnalyticsSearchTime ? ` | ⏱️ ${licenciasAnalyticsSearchTime}s` : ""}
                    {licenciasPronosticoSearchTime ? ` | Pronosticos: ⏱️ ${licenciasPronosticoSearchTime}s` : ""}
                  </div>
                </div>

                {licenciasAnalyticsTotals ? (
                  <div className="hero-stats" role="group" aria-label="KPIs">
                    <StatCard title="Licencia" value={moneyMx.format(Number(licenciasAnalyticsTotals["Licencia"] || 0))} />
                    <StatCard title="Basura" value={moneyMx.format(Number(licenciasAnalyticsTotals["Basura"] || 0))} />
                    <StatCard title="Otros" value={moneyMx.format(Number(licenciasAnalyticsTotals["Otros"] || 0))} />
                    <StatCard title="Total" tone="success" value={moneyMx.format(Number(licenciasAnalyticsTotals["Total"] || 0))} />
                  </div>
                ) : null}
              </>
            ) : null}
          </section>

          {analiticaSection === "saneamiento" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Graficas para analisis</h2>
                <p>Tendencias y variables derivadas listas para modelos de machine learning.</p>
              </div>
            </div>
            <div className="feature-pills">
              <div className="feature-pill">Ticket promedio: {moneyMx.format(saneamientoAnalyticsFeatures.ticketPromedio)}</div>
              <div className="feature-pill">Tasa cancelacion: {numberMx.format(saneamientoAnalyticsFeatures.tasaCancelacion)}%</div>
              <div className="feature-pill">Tendencia total: {numberMx.format(saneamientoAnalyticsFeatures.tendenciaPct)}%</div>
              <div className="feature-pill">Universo recibos: {numberMx.format(saneamientoAnalyticsFeatures.universo)}</div>
            </div>
            <div className="charts-grid">
              <AnalyticsLineChart
                rows={saneamientoAnalyticsSeriesAsc}
                xKey="Periodo"
                yKey="Total"
                title="Tendencia de recaudacion total"
                valueFormatter={(value) => moneyMx.format(value)}
              />
              <AnalyticsBarsChart
                rows={saneamientoAnalyticsSeriesAsc}
                xKey="Periodo"
                yKey="Recibos"
                title="Volumen de recibos por periodo"
                valueFormatter={(value) => numberMx.format(value)}
              />
            </div>
          </section>) : null}

          {analiticaSection === "prediales" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Graficas para analisis</h2>
                <p>Tendencias y variables derivadas listas para modelos de machine learning.</p>
              </div>
            </div>
            <div className="feature-pills">
              <div className="feature-pill">Ticket promedio: {moneyMx.format(predialesAnalyticsFeatures.ticketPromedio)}</div>
              <div className="feature-pill">Tendencia total: {numberMx.format(predialesAnalyticsFeatures.tendenciaPct)}%</div>
              <div className="feature-pill">Universo recibos: {numberMx.format(predialesAnalyticsFeatures.recActivos)}</div>
            </div>
            <div className="charts-grid">
              <AnalyticsLineChart
                rows={predialesAnalyticsSeriesAsc}
                xKey="Periodo"
                yKey="Total"
                title="Tendencia de recaudacion total"
                valueFormatter={(value) => moneyMx.format(value)}
              />
              <AnalyticsBarsChart
                rows={predialesAnalyticsSeriesAsc}
                xKey="Periodo"
                yKey="Recibos"
                title="Volumen de recibos por periodo"
                valueFormatter={(value) => numberMx.format(value)}
              />
            </div>
          </section>) : null}

          {analiticaSection === "licencias" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Graficas para analisis</h2>
                <p>Tendencias y variables derivadas listas para modelos de machine learning.</p>
              </div>
            </div>
            <div className="feature-pills">
              <div className="feature-pill">Ticket promedio: {moneyMx.format(licenciasAnalyticsFeatures.ticketPromedio)}</div>
              <div className="feature-pill">Tendencia total: {numberMx.format(licenciasAnalyticsFeatures.tendenciaPct)}%</div>
              <div className="feature-pill">Universo recibos: {numberMx.format(licenciasAnalyticsFeatures.recActivos)}</div>
            </div>
            <div className="charts-grid">
              <AnalyticsLineChart
                rows={licenciasAnalyticsSeriesAsc}
                xKey="Periodo"
                yKey="Total"
                title="Tendencia de recaudacion total"
                valueFormatter={(value) => moneyMx.format(value)}
              />
              <AnalyticsBarsChart
                rows={licenciasAnalyticsSeriesAsc}
                xKey="Periodo"
                yKey="Recibos"
                title="Volumen de recibos por periodo"
                valueFormatter={(value) => numberMx.format(value)}
              />
            </div>
          </section>) : null}

          {analiticaSection === "saneamiento" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Pronosticos</h2>
                <p>Pronostico de recaudacion mensual y comparacion pronostico vs real.</p>
              </div>
            </div>
            <div className="feature-pills">
              <div className="feature-pill">
                Modelo: {saneamientoPronosticoModelo?.tipo ? String(saneamientoPronosticoModelo.tipo) : "-"}
              </div>
              <div className="feature-pill">
                MAE backtest: {saneamientoPronosticoModelo?.maeBacktest != null ? moneyMx.format(Number(saneamientoPronosticoModelo.maeBacktest || 0)) : "-"}
              </div>
              <div className="feature-pill">
                MAPE backtest: {saneamientoPronosticoModelo?.mapeBacktest != null ? `${numberMx.format(Number(saneamientoPronosticoModelo.mapeBacktest || 0))}%` : "-"}
              </div>
              <div className="feature-pill">
                Puntos: {saneamientoPronosticoModelo?.n != null ? numberMx.format(Number(saneamientoPronosticoModelo.n || 0)) : "-"}
              </div>
            </div>
            <div className="charts-grid">
              <AnalyticsMultiLineChart
                rows={saneamientoPronosticoSerie}
                xKey="Periodo"
                title="Pronosticos (Total)"
                valueFormatter={(value) => moneyMx.format(value)}
                series={[
                  { key: "Real", label: "Real", color: "#22c55e" },
                  { key: "Pronostico", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
              <AnalyticsMultiLineChart
                rows={saneamientoPronosticoVsRealChart}
                xKey="Periodo"
                title="Pronosticos vs Real"
                valueFormatter={(value) => moneyMx.format(value)}
                series={[
                  { key: "Real", label: "Real", color: "#22c55e" },
                  { key: "Pronostico", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
              <AnalyticsBarsChart
                rows={saneamientoPronosticoErrorAbsSeries}
                xKey="Periodo"
                yKey="ErrorAbs"
                title="Diferencia absoluta (|Real - Pronostico|)"
                valueFormatter={(value) => moneyMx.format(value)}
              />
              <AnalyticsMultiLineChart
                rows={saneamientoPronosticoIndiceSerie}
                xKey="Periodo"
                title="Indice (Base=100) - Pronostico vs Real"
                valueFormatter={(value) => numberMx.format(value)}
                series={[
                  { key: "RealIndex", label: "Real", color: "#22c55e" },
                  { key: "PronosticoIndex", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={saneamientoPronosticoVsReal}
                columns={[
                  { key: "Periodo", label: "Periodo" },
                  { key: "Real", label: "Real" },
                  { key: "Pronostico", label: "Pronostico" },
                  { key: "ErrorAbs", label: "ErrorAbs" },
                  { key: "ErrorPct", label: "ErrorPct" }
                ]}
              />
            </div>
          </section>) : null}

          {analiticaSection === "prediales" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Pronosticos</h2>
                <p>Pronostico de recaudacion mensual y comparacion pronostico vs real.</p>
              </div>
            </div>
            <div className="feature-pills">
              <div className="feature-pill">
                Modelo: {predialesPronosticoModelo?.tipo ? String(predialesPronosticoModelo.tipo) : "-"}
              </div>
              <div className="feature-pill">
                MAE backtest: {predialesPronosticoModelo?.maeBacktest != null ? moneyMx.format(Number(predialesPronosticoModelo.maeBacktest || 0)) : "-"}
              </div>
              <div className="feature-pill">
                MAPE backtest: {predialesPronosticoModelo?.mapeBacktest != null ? `${numberMx.format(Number(predialesPronosticoModelo.mapeBacktest || 0))}%` : "-"}
              </div>
              <div className="feature-pill">
                Puntos: {predialesPronosticoModelo?.n != null ? numberMx.format(Number(predialesPronosticoModelo.n || 0)) : "-"}
              </div>
            </div>
            <div className="charts-grid">
              <AnalyticsMultiLineChart
                rows={predialesPronosticoSerie}
                xKey="Periodo"
                title="Pronosticos (Total)"
                valueFormatter={(value) => moneyMx.format(value)}
                series={[
                  { key: "Real", label: "Real", color: "#22c55e" },
                  { key: "Pronostico", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
              <AnalyticsMultiLineChart
                rows={predialesPronosticoVsRealChart}
                xKey="Periodo"
                title="Pronosticos vs Real"
                valueFormatter={(value) => moneyMx.format(value)}
                series={[
                  { key: "Real", label: "Real", color: "#22c55e" },
                  { key: "Pronostico", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
              <AnalyticsBarsChart
                rows={predialesPronosticoErrorAbsSeries}
                xKey="Periodo"
                yKey="ErrorAbs"
                title="Diferencia absoluta (|Real - Pronostico|)"
                valueFormatter={(value) => moneyMx.format(value)}
              />
              <AnalyticsMultiLineChart
                rows={predialesPronosticoIndiceSerie}
                xKey="Periodo"
                title="Indice (Base=100) - Pronostico vs Real"
                valueFormatter={(value) => numberMx.format(value)}
                series={[
                  { key: "RealIndex", label: "Real", color: "#22c55e" },
                  { key: "PronosticoIndex", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={predialesPronosticoVsReal}
                columns={[
                  { key: "Periodo", label: "Periodo" },
                  { key: "Real", label: "Real" },
                  { key: "Pronostico", label: "Pronostico" },
                  { key: "ErrorAbs", label: "ErrorAbs" },
                  { key: "ErrorPct", label: "ErrorPct" }
                ]}
              />
            </div>
          </section>) : null}

          {analiticaSection === "licencias" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Pronosticos</h2>
                <p>Pronostico de recaudacion mensual y comparacion pronostico vs real.</p>
              </div>
            </div>
            <div className="feature-pills">
              <div className="feature-pill">
                Modelo: {licenciasPronosticoModelo?.tipo ? String(licenciasPronosticoModelo.tipo) : "-"}
              </div>
              <div className="feature-pill">
                MAE backtest: {licenciasPronosticoModelo?.maeBacktest != null ? moneyMx.format(Number(licenciasPronosticoModelo.maeBacktest || 0)) : "-"}
              </div>
              <div className="feature-pill">
                MAPE backtest: {licenciasPronosticoModelo?.mapeBacktest != null ? `${numberMx.format(Number(licenciasPronosticoModelo.mapeBacktest || 0))}%` : "-"}
              </div>
              <div className="feature-pill">
                Puntos: {licenciasPronosticoModelo?.n != null ? numberMx.format(Number(licenciasPronosticoModelo.n || 0)) : "-"}
              </div>
            </div>
            <div className="charts-grid">
              <AnalyticsMultiLineChart
                rows={licenciasPronosticoSerie}
                xKey="Periodo"
                title="Pronosticos (Total)"
                valueFormatter={(value) => moneyMx.format(value)}
                series={[
                  { key: "Real", label: "Real", color: "#22c55e" },
                  { key: "Pronostico", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
              <AnalyticsMultiLineChart
                rows={licenciasPronosticoVsRealChart}
                xKey="Periodo"
                title="Pronosticos vs Real"
                valueFormatter={(value) => moneyMx.format(value)}
                series={[
                  { key: "Real", label: "Real", color: "#22c55e" },
                  { key: "Pronostico", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
              <AnalyticsBarsChart
                rows={licenciasPronosticoErrorAbsSeries}
                xKey="Periodo"
                yKey="ErrorAbs"
                title="Diferencia absoluta (|Real - Pronostico|)"
                valueFormatter={(value) => moneyMx.format(value)}
              />
              <AnalyticsMultiLineChart
                rows={licenciasPronosticoIndiceSerie}
                xKey="Periodo"
                title="Indice (Base=100) - Pronostico vs Real"
                valueFormatter={(value) => numberMx.format(value)}
                series={[
                  { key: "RealIndex", label: "Real", color: "#22c55e" },
                  { key: "PronosticoIndex", label: "Pronostico", color: "#38bdf8", dash: "6 4" }
                ]}
              />
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={licenciasPronosticoVsReal}
                columns={[
                  { key: "Periodo", label: "Periodo" },
                  { key: "Real", label: "Real" },
                  { key: "Pronostico", label: "Pronostico" },
                  { key: "ErrorAbs", label: "ErrorAbs" },
                  { key: "ErrorPct", label: "ErrorPct" }
                ]}
              />
            </div>
          </section>) : null}

          {analiticaSection === "saneamiento" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Serie mensual</h2>
                <p>Totales agrupados por mes (según filtros).</p>
              </div>
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={saneamientoAnalyticsSeries}
                columns={[
                  { key: "Periodo", label: "Periodo" },
                  { key: "Recibos", label: "Recibos" },
                  { key: "Licencias", label: "Licencias" },
                  { key: "Derecho", label: "Derecho", group: "Lo Cobrado" },
                  { key: "Actualizaciones", label: "Actualizaciones", group: "Lo Cobrado" },
                  { key: "Recargos", label: "Recargos", group: "Lo Cobrado" },
                  { key: "Total", label: "Total", group: "Lo Cobrado" }
                ]}
              />
            </div>
          </section>) : null}

          {analiticaSection === "prediales" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Serie mensual</h2>
                <p>Totales agrupados por mes (según filtros).</p>
              </div>
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={predialesAnalyticsSeries}
                columns={[
                  { key: "Periodo", label: "Periodo" },
                  { key: "Recibos", label: "Recibos" },
                  { key: "Total", label: "Total", group: "Lo Cobrado" }
                ]}
              />
            </div>
          </section>) : null}

          {analiticaSection === "licencias" ? (
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Serie mensual</h2>
                <p>Totales agrupados por mes (según filtros).</p>
              </div>
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={licenciasAnalyticsSeries}
                columns={[
                  { key: "Periodo", label: "Periodo" },
                  { key: "Recibos", label: "Recibos" },
                  { key: "Total", label: "Total", group: "Lo Cobrado" }
                ]}
              />
            </div>
          </section>) : null}

          {analiticaSection === "saneamiento" && saneamientoCanceladosOpen ? (
            <section className="panel">
              <div className="panel-header">
                <div>
                  <h2>Recibos cancelados</h2>
                  <p>Detalle de recibos con EdoRec = C y motivo (ConceptoCanRecibo).</p>
                </div>
              </div>
              <div className="actions">
                <button className="ghost" onClick={() => loadSaneamientoCancelados(0)} disabled={loading === "saneamiento-cancelados"} type="button">
                  {loading === "saneamiento-cancelados" ? "Cargando..." : "Actualizar"}
                </button>
                <button className="danger" onClick={() => setSaneamientoCanceladosOpen(false)} type="button">
                  Cerrar
                </button>
                {saneamientoCanceladosHasMore ? (
                  <button
                    className="ghost"
                    onClick={() => (saneamientoCanceladosNextOffset != null ? loadSaneamientoCancelados(saneamientoCanceladosNextOffset) : null)}
                    disabled={loading === "saneamiento-cancelados"}
                    type="button"
                  >
                    Siguiente página
                  </button>
                ) : null}
                <div className="pill">
                  {saneamientoCanceladosRows.length} filas
                  {saneamientoCanceladosSearchTime ? ` | ⏱️ ${saneamientoCanceladosSearchTime}s` : ""}
                  {` | Total cancelados: ${saneamientoAnalyticsCanceladosCount}`}
                </div>
              </div>
              <div className="table-space">
                <TableWithColumns
                  rows={saneamientoCanceladosRows}
                  columns={[
                    { key: "Serie", label: "Serie" },
                    { key: "Folio", label: "Folio" },
                    { key: "Fecha", label: "Fecha" },
                    { key: "Fecha cancelación", label: "Fecha cancelación" },
                    { key: "Usuario cancelación", label: "Usuario cancelación" },
                    { key: "Motivo cancelación", label: "Motivo cancelación" },
                    { key: "No. Licencia", label: "No. Licencia" },
                    { key: "Padrón", label: "Padrón" },
                    { key: "Nombre", label: "Nombre" },
                    { key: "RFC", label: "RFC" },
                    { key: "Observaciones", label: "Observaciones" }
                  ]}
                />
              </div>
            </section>
          ) : null}
        </>
      ) : null}

      {section === "padronCatastral" ? (
        <>
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Padrón Catastral</h2>
                <p>Consulta y exportación del padrón catastral.</p>
              </div>
            </div>

            <div className="grid">
              <Field label="Padrón">
                <input name="predioId" value={padronCatastralReport.predioId} onChange={updatePadronCatastralReport} inputMode="numeric" placeholder="12345" />
              </Field>
              <Field label="Clave catastral">
                <input name="claveCatastral" value={padronCatastralReport.claveCatastral} onChange={updatePadronCatastralReport} placeholder="914001000040003-" />
              </Field>
              <Field label="Modo clave">
                <select name="claveMode" value={padronCatastralReport.claveMode} onChange={updatePadronCatastralReport}>
                  <option value="contiene">Contiene</option>
                  <option value="exacto">Exacto</option>
                </select>
              </Field>
              <Field label="Propietario">
                <input name="propietario" value={padronCatastralReport.propietario} onChange={updatePadronCatastralReport} placeholder="Nombre / razón social" />
              </Field>
              <Field label="Apellido paterno">
                <input name="apellidoPaterno" value={padronCatastralReport.apellidoPaterno} onChange={updatePadronCatastralReport} placeholder="Paterno" />
              </Field>
              <Field label="Apellido materno">
                <input name="apellidoMaterno" value={padronCatastralReport.apellidoMaterno} onChange={updatePadronCatastralReport} placeholder="Materno" />
              </Field>
              <Field label="Nombre">
                <input name="nombre" value={padronCatastralReport.nombre} onChange={updatePadronCatastralReport} placeholder="Nombre" />
              </Field>
              <Field label="Calle">
                <input name="calle" value={padronCatastralReport.calle} onChange={updatePadronCatastralReport} placeholder="Av. ..." />
              </Field>
              <Field label="Número">
                <input name="numero" value={padronCatastralReport.numero} onChange={updatePadronCatastralReport} placeholder="Ext/Int" />
              </Field>
              <Field label="Estatus">
                <input name="estatus" value={padronCatastralReport.estatus} onChange={updatePadronCatastralReport} placeholder="A" />
              </Field>
              <Field label="Adeudo">
                <select name="adeudo" value={padronCatastralReport.adeudo} onChange={updatePadronCatastralReport}>
                  <option value="todos">Todos</option>
                  <option value="con">Con adeudo</option>
                  <option value="sin">Sin adeudo</option>
                </select>
              </Field>
              <Field label="Alta desde" hint="YYYY-MM-DD">
                <input name="fromAlta" value={padronCatastralReport.fromAlta} onChange={updatePadronCatastralReport} placeholder="2003-01-01" />
              </Field>
              <Field label="Alta hasta" hint="YYYY-MM-DD">
                <input name="toAlta" value={padronCatastralReport.toAlta} onChange={updatePadronCatastralReport} placeholder="2003-12-31" />
              </Field>
              <Field label="Buscar" hint="Clave predial / propietario">
                <input name="q" value={padronCatastralReport.q} onChange={updatePadronCatastralReport} placeholder="Texto libre" />
              </Field>
              <Field label="Máx filas export">
                <input name="maxRows" value={padronCatastralReport.maxRows} onChange={updatePadronCatastralReport} inputMode="numeric" />
              </Field>
            </div>

            <div className="actions">
              <button className="primary" onClick={() => loadPadronCatastralSabana(0)} disabled={loading === "padronCatastral"}>
                {loading === "padronCatastral" ? "Cargando..." : "Cargar padrón"}
              </button>
              <button className="ghost" onClick={exportPadronCatastralExcel} type="button">
                Exportar Excel
              </button>
              <button className="ghost" onClick={exportPadronCatastralCsv} type="button">
                Exportar CSV
              </button>
              <button className="danger" onClick={clearPadronCatastralSabana} type="button">
                Borrar sábana
              </button>
              {padronCatastralHasMore ? (
                <button
                  className="ghost"
                  onClick={() => (padronCatastralNextOffset != null ? loadPadronCatastralSabana(padronCatastralNextOffset) : null)}
                  disabled={loading === "padronCatastral" || padronCatastralNextOffset == null}
                >
                  Siguiente página
                </button>
              ) : null}
              <div className="pill">
                {padronCatastralRows.length} filas {padronCatastralSearchTime ? `| ⏱️ ${padronCatastralSearchTime}s` : ""}
              </div>
            </div>
          </section>

          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Resultados</h2>
                <p>Filas cargadas del padrón.</p>
              </div>
            </div>
            <div className="table-space">
              <TableWithColumns
                rows={padronCatastralRows}
                columns={[
                  { key: "Padrón", label: "Padrón" },
                  { key: "Clave Catastral", label: "Clave Catastral" },
                  { key: "Fecha de Alta del Predio", label: "Fecha de Alta del Predio" },
                  { key: "Calle", label: "Calle" },
                  { key: "Código Postal", label: "Código Postal" },
                  { key: "Número", label: "Número" },
                  { key: "Propietario", label: "Propietario" },
                  { key: "Estatus", label: "Estatus" },
                  { key: "Datos escriturales", label: "Datos escriturales" },
                  { key: "Superficie del Terreno", label: "Superficie del Terreno" },
                  { key: "Valor del Terreno", label: "Valor del Terreno" },
                  { key: "Área Construida", label: "Área Construida" },
                  { key: "Valor de Construcción", label: "Valor de Construcción" },
                  { key: "Valor Catastral", label: "Valor Catastral" },
                  { key: "Año del valor catastral", label: "Año del valor catastral" },
                  { key: "Impuesto Actual", label: "Impuesto Actual" },
                  { key: "Impuesto por bimestre", label: "Impuesto por bimestre" },
                  { key: "Ut.Bim.Pagado", label: "Ut.Bim.Pagado" },
                  { key: "Tipo de Predio", label: "Tipo de Predio" },
                  { key: "Estado Físico", label: "Estado Físico" },
                  { key: "Ejer - Per", label: "Ejer - Per" }
                ]}
              />
            </div>
          </section>
        </>
      ) : null}

      {section === "config" ? (
        <>
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>Configuración</h2>
                <p>Selecciona el módulo a configurar.</p>
              </div>
            </div>

            <section className="tabs tabs-secondary">
              <button
                type="button"
                className={`tab ${configSection === "umas" ? "active" : ""}`}
                onClick={() => {
                  setConfigSection("umas");
                  loadUmas();
                }}
              >
                UMAS
              </button>
              <button
                type="button"
                className={`tab ${configSection === "cri" ? "active" : ""}`}
                onClick={() => {
                  setConfigSection("cri");
                  loadCri();
                }}
              >
                CRI
              </button>
              {role === "admin" ? (
                <button
                  type="button"
                  className={`tab ${configSection === "usuarios" ? "active" : ""}`}
                  onClick={() => {
                    setConfigSection("usuarios");
                    loadUsers();
                  }}
                >
                  Usuarios
                </button>
              ) : null}
            </section>

            {configSection === "umas" ? (
              <>
                <div className="panel-header">
                  <div>
                    <h2>UMAS</h2>
                    <p>Diccionario de UMAs por año de vigencia (01/feb a 31/ene).</p>
                  </div>
                  <div className="pill">{umaRows.length} registros</div>
                </div>

                <div className="grid">
                  <Field label="Vigencia (año)">
                    <input name="vigenciaYear" value={umaForm.vigenciaYear} onChange={updateUmaField} inputMode="numeric" placeholder="2025" />
                  </Field>
                  <Field label="UMA (MXN)">
                    <input name="umaMxn" value={umaForm.umaMxn} onChange={updateUmaField} inputMode="decimal" placeholder="103.74" />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={saveUma} disabled={loading === "umas-save"}>
                    {loading === "umas-save" ? "Guardando..." : "Guardar UMA"}
                  </button>
                  <button className="ghost" onClick={loadUmas} disabled={loading === "umas"}>
                    {loading === "umas" ? "Cargando..." : "Recargar"}
                  </button>
                </div>

                <div className="table-space">
                  {umaRows && umaRows.length ? (
                    <div className="table-shell">
                      <table>
                        <thead>
                          <tr>
                            <th>Vigencia</th>
                            <th>UMA (MXN)</th>
                            <th>Acciones</th>
                          </tr>
                        </thead>
                        <tbody>
                          {umaRows.map((r) => (
                            <tr key={String(r.vigenciaYear)}>
                              <td>{String(r.vigenciaYear)}</td>
                              <td>{moneyMx.format(Number(r.umaMxn || 0))}</td>
                              <td>
                                <button className="ghost" type="button" onClick={() => editUmaRow(r)}>
                                  Editar
                                </button>{" "}
                                <button
                                  className="danger"
                                  type="button"
                                  onClick={() => deleteUmaRow(r.vigenciaYear)}
                                  disabled={loading === "umas-delete"}
                                >
                                  Eliminar
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty">Aún no hay UMAs configuradas.</div>
                  )}
                </div>
              </>
            ) : null}

            {configSection === "usuarios" ? (
              <>
                <div className="panel-header">
                  <div>
                    <h2>Usuarios</h2>
                    <p>Alta de usuarios (sin registro público). Solo administrador.</p>
                  </div>
                  <div className="pill">{users.length} usuarios</div>
                </div>

                {usersError ? <div className="alert error">{usersError}</div> : null}

                <div className="grid">
                  <Field label="Usuario">
                    <input name="username" value={createUserForm.username} onChange={updateCreateUserField} placeholder="usuario" autoComplete="off" />
                  </Field>
                  <Field label="Contraseña">
                    <input name="password" value={createUserForm.password} onChange={updateCreateUserField} type="password" placeholder="••••••••" autoComplete="new-password" />
                  </Field>
                  <Field label="Nombre">
                    <input name="displayName" value={createUserForm.displayName} onChange={updateCreateUserField} placeholder="Nombre visible" />
                  </Field>
                  <Field label="Rol">
                    <select name="role" value={createUserForm.role} onChange={updateCreateUserField}>
                      <option value="admin">Administrador</option>
                      <option value="cajero">Cajero</option>
                      <option value="dir_ingresos">Dir. Ingresos</option>
                    </select>
                  </Field>
                  <Field label="Activo">
                    <label className="check">
                      <input name="isActive" type="checkbox" checked={!!createUserForm.isActive} onChange={updateCreateUserField} />
                      <span>Habilitado</span>
                    </label>
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" type="button" onClick={createUser} disabled={createUserBusy}>
                    {createUserBusy ? "Creando..." : "Crear usuario"}
                  </button>
                  <button className="ghost" type="button" onClick={loadUsers} disabled={usersLoading}>
                    {usersLoading ? "Cargando..." : "Recargar"}
                  </button>
                </div>

                <div className="table-space">
                  {users && users.length ? (
                    <div className="table-shell">
                      <table>
                        <thead>
                          <tr>
                            <th>Usuario</th>
                            <th>Nombre</th>
                            <th>Rol</th>
                            <th>Activo</th>
                          </tr>
                        </thead>
                        <tbody>
                          {users.map((u) => (
                            <tr key={String(u.id)}>
                              <td>{String(u.username || "")}</td>
                              <td>{String(u.displayName || "")}</td>
                              <td>{String(u.role || "")}</td>
                              <td>{u.isActive ? "Sí" : "No"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty">{usersLoading ? "Cargando..." : "No hay usuarios para mostrar."}</div>
                  )}
                </div>
              </>
            ) : null}

            {configSection === "cri" ? (
              <>
                <div className="panel-header">
                  <div>
                    <h2>CRI</h2>
                    <p>Catálogo CRI en la base MicroServicios.</p>
                  </div>
                  <div className="pill">
                    {(criCatalog?.counts?.rubros ?? 0)} rubros | {(criCatalog?.counts?.tipos ?? 0)} tipos |{" "}
                    {(criCatalog?.counts?.clases ?? 0)} clases | {(criCatalog?.counts?.conceptos ?? 0)} conceptos
                  </div>
                </div>

                <div className="grid">
                  <Field label="Llave admin">
                    <input name="adminKey" value={form.adminKey} onChange={updateField} type="password" placeholder="Obligatoria para crear tablas" />
                  </Field>
                </div>

                <div className="actions">
                  <button className="primary" onClick={bootstrapCri} disabled={loading === "cri-bootstrap"}>
                    {loading === "cri-bootstrap" ? "Creando..." : "Crear/Actualizar tablas CRI"}
                  </button>
                  <button className="ghost" onClick={loadCri} disabled={loading === "cri"}>
                    {loading === "cri" ? "Cargando..." : "Recargar"}
                  </button>
                </div>

                <div className="table-space">
                  {criCatalog?.ok && criRubrosRows.length ? (
                    <>
                      <TableWithColumns
                        rows={criRubrosRows}
                        columns={[
                          { key: "clave_rubro", label: "Clave rubro" },
                          { key: "nombre_rubro", label: "Nombre rubro" },
                          { key: "tipos", label: "Tipos" }
                        ]}
                      />
                      <TableWithColumns
                        rows={criTiposRows}
                        columns={[
                          { key: "clave_rubro", label: "Rubro" },
                          { key: "clave_tipo", label: "Clave tipo" },
                          { key: "nombre_tipo", label: "Nombre tipo" }
                        ]}
                      />
                      <TableWithColumns
                        rows={criClasesRows}
                        columns={[
                          { key: "clave_rubro", label: "Rubro" },
                          { key: "clave_tipo", label: "Tipo" },
                          { key: "clave_clase", label: "Clave clase" },
                          { key: "nombre_clase", label: "Nombre clase" }
                        ]}
                      />
                      <TableWithColumns
                        rows={criConceptosRows}
                        columns={[
                          { key: "clave_rubro", label: "Rubro" },
                          { key: "clave_tipo", label: "Tipo" },
                          { key: "clave_clase", label: "Clase" },
                          { key: "clave_concepto", label: "Clave concepto" },
                          { key: "nombre_concepto", label: "Nombre concepto" }
                        ]}
                      />
                    </>
                  ) : (
                    <div className="empty">Aún no hay catálogo CRI cargado o falta crear las tablas.</div>
                  )}
                </div>
              </>
            ) : null}
          </section>
        </>
      ) : null}
        </div>
      </div>
    </div>
  );
}

function App() {
  const [path, setPath] = useState(() => (typeof window !== "undefined" ? window.location.pathname || "/" : "/"));
  const [authUser, setAuthUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(false);
  const [authChecked, setAuthChecked] = useState(false);

  useEffect(() => {
    function onPop() {
      setPath(window.location.pathname || "/");
    }
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  function navigate(to) {
    const next = String(to || "/");
    if (next === path) return;
    window.history.pushState({}, "", next);
    setPath(next);
  }

  useEffect(() => {
    let cancelled = false;
    setAuthLoading(true);
    fetch("/api/auth/me", { credentials: "include" })
      .then(async (res) => {
        if (!res.ok) {
          setAuthUser(null);
          return;
        }
        const data = await res.json().catch(() => null);
        if (!cancelled) setAuthUser(data?.user || null);
      })
      .catch(() => {
        if (!cancelled) setAuthUser(null);
      })
      .finally(() => {
        if (!cancelled) {
          setAuthLoading(false);
          setAuthChecked(true);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  async function logout() {
    try {
      await fetch("/api/auth/logout", { method: "POST", credentials: "include" });
    } catch (e) {
    } finally {
      setAuthUser(null);
      navigate("/login");
    }
  }

  useEffect(() => {
    if (!authChecked || authLoading) return;
    if (!authUser && path !== "/login") {
      try {
        window.sessionStorage.setItem("next_path", path);
      } catch (e) {
      }
      navigate("/login");
      return;
    }
    if (authUser && path === "/login") {
      let nextPath = "/";
      try {
        nextPath = window.sessionStorage.getItem("next_path") || "/";
        window.sessionStorage.removeItem("next_path");
      } catch (e) {
      }
      navigate(nextPath);
      return;
    }
    if (authUser) {
      const role = String(authUser?.role || "").toLowerCase();
      if (path === "/") {
        navigate("/ingresos");
        return;
      }
      if (path === "/ingresos" || path === "/ingresos/") return;
    }
  }, [path, authChecked, authLoading, authUser]);

  function ingresosSectionFromPath(p) {
    const pathname = String(p || "");
    if (pathname === "/" || pathname === "/ingresos" || pathname === "/ingresos/") return "inicio";
    if (!pathname.startsWith("/ingresos/")) return "__unknown__";
    const tail = pathname.slice("/ingresos/".length);
    if (tail === "inicio") return "inicio";
    if (tail === "cajas") return "cajas";
    if (tail === "reportes") return "reportes";
    if (tail === "analitica") return "analitica";
    if (tail === "padron-catastral") return "padronCatastral";
    if (tail === "pases-caja") return "pasesCaja";
    if (tail === "configuracion") return "config";
    if (tail === "ayudas") return "ayudas";
    return "__unknown__";
  }

  if (path === "/login") {
    if (!authChecked || authLoading) return <LoadingScreen title="Verificando sesión…" />;
    if (authUser) return <LoadingScreen title="Redirigiendo…" />;
    return (
      <LoginPage
        onLoggedIn={(u) => {
          setAuthUser(u);
          let nextPath = "/";
          try {
            nextPath = window.sessionStorage.getItem("next_path") || "/";
            window.sessionStorage.removeItem("next_path");
          } catch (e) {
          }
          navigate(nextPath);
        }}
      />
    );
  }

  if (!authChecked || authLoading) return <LoadingScreen title="Cargando…" />;
  if (!authUser) return <LoadingScreen title="Redirigiendo…" />;

  const role = String(authUser?.role || "").toLowerCase();
  const allowedSections =
    role === "cajero"
      ? ["inicio", "cajas"]
      : role === "dir_ingresos"
        ? ["inicio", "cajas", "reportes", "analitica", "padronCatastral", "pasesCaja"]
        : ["inicio", "cajas", "reportes", "analitica", "padronCatastral", "pasesCaja", "ayudas", "config"];

  if (path === "/401") {
    return <ErrorPage code="401" title="No autenticado" detail="Necesitas iniciar sesión." onGoLogin={() => navigate("/login")} onGoHome={() => navigate("/ingresos")} />;
  }
  if (path === "/403") {
    return <ErrorPage code="403" title="Sin permisos" detail="No tienes permisos para ver esta pantalla." onGoHome={() => navigate("/ingresos")} onGoLogin={() => navigate("/login")} />;
  }
  if (path === "/404") {
    return <ErrorPage code="404" title="No encontrado" detail="La pantalla no existe." onGoHome={() => navigate("/ingresos")} onGoLogin={() => navigate("/login")} />;
  }
  if (path === "/500") {
    return <ErrorPage code="500" title="Error" detail="Ocurrió un error inesperado." onGoHome={() => navigate("/ingresos")} onGoLogin={() => navigate("/login")} />;
  }

  const requestedSection = ingresosSectionFromPath(path);
  if (requestedSection === "__unknown__") {
    return <ErrorPage code="404" title="No encontrado" detail="La pantalla no existe." onGoHome={() => navigate("/ingresos")} onGoLogin={() => navigate("/login")} />;
  }
  if (requestedSection && !allowedSections.includes(requestedSection)) {
    return <ErrorPage code="403" title="Sin permisos" detail="No tienes permisos para ver esta pantalla." onGoHome={() => navigate("/ingresos")} onGoLogin={() => navigate("/login")} />;
  }

  return <AdminApp key={String(authUser?.role || "user")} user={authUser} onLogout={logout} allowedSections={allowedSections} initialSection={requestedSection} />;
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
