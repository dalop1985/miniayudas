# Interfaz web para consolidar fuentes de ingreso

App en Node.js que expone una interfaz en navegador para:

- Ver (vista previa) las filas de `TLSOLICITUDFUENTESINGRESO` por Solicitud/año
- Consolidar exactamente 2 filas (suma importes, actualiza la principal, borra la secundaria) dentro de una transacción
- Actualizar `TLSOLICITUD` (estado y opcionalmente vencimiento)

## Requisitos

- Node.js 18+ recomendado
- Acceso a SQL Server con la base `Tulum`

## Configuración

Crea un archivo `.env` en la raíz:

```
PORT=3000

DB_SERVER=tu-servidor
DB_PORT=1433
DB_DATABASE=Tulum
DB_USER=tu-usuario
DB_PASSWORD=tu-password
DB_ENCRYPT=true
DB_TRUST_SERVER_CERT=true

ADMIN_KEY=una-llave-larga
```

Notas:

- `ADMIN_KEY` es obligatoria para ejecutar consolidación. La vista previa funciona sin llave.
- Para `vencimientoFecha` en la UI se acepta `YYYY-MM-DD` o `YYYY-MM-DDTHH:mm:ss`.

## Uso

Instalar dependencias:

```
npm install
```

Levantar servidor:

```
npm run dev
```

Abrir:

- http://localhost:3000

