# SUPER DUPAZA 5.26.6 — D1 aislado del camino operativo

## Estado
PostgreSQL/Supabase pasa a ser el único motor operativo de la aplicación.

### PostgreSQL-only
- /api/source/status
- /api/not-loaded
- /api/report-config
- /api/report-config/labels
- /api/name-normalization/list
- /api/name-normalization/save
- /api/name-normalization/delete
- /api/seller-targets
- /api/seller-targets/save
- /api/manual-api-import/list
- /api/manual-api-import
- /api/manual-sap/list
- /api/manual-sap/mark
- /api/manual-sap/resolve
- /api/summary
- /api/advisor-summary
- /api/report-options
- Queue sync
- Cron sync

## Importador manual
El importador Excel/API ahora ejecuta dentro de una transacción PostgreSQL:

BEGIN
-> lógica de importación
-> report_orders
-> COMMIT

Si falla:
ROLLBACK

Respuesta incluye:
- engine: POSTGRESQL
- via: HYPERDRIVE
- transactionCommitted: true
- d1Modified: false

## D1 retirado
Todas las rutas:
- /api/d1/*
- /d1-admin
- /d1-export-final

responden HTTP 410 Gone.

## Binding DB
Todavía se conserva EN ESTA FASE solamente para confirmar que ninguna ruta operativa lo necesita.

No borrarlo manualmente todavía.

## Auditoría
GET /api/postgres-runtime-audit
Header:
X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>

Esperado:
- ok = true
- engine = POSTGRESQL
- d1OperationalReads = false
- d1OperationalWrites = false
- d1QueueSync = false
- d1CronSync = false
- manualApiImporter = POSTGRESQL
- legacyD1Routes = 410_GONE
- readyForFinalDbBindingRemovalTest = true

## Validación de ruta retirada
Abrir:
GET /api/d1/test

Debe devolver HTTP 410 con:
retired: true
engine: POSTGRESQL

## Cron
Se mantienen:
- */5 * * * *
- 17 12 * * *

## Próxima fase
5.26.7:
- eliminar binding DB de wrangler.toml
- desplegar sin D1
- validar dashboard + Queue + Cron
