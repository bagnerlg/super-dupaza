# SUPER DUPAZA 5.26.1 — PostgreSQL Shadow Reads

Esta fase NO cambia las rutas normales del dashboard.

## D1 sigue activo
Las rutas originales:
- /api/source/status
- /api/not-loaded
- /api/report-config
- /api/name-normalization/list
- /api/seller-targets
- /api/manual-api-import/list
- /api/manual-sap/list
- /api/summary
- /api/advisor-summary
- /api/report-options

siguen usando D1.

## Nuevas rutas PostgreSQL
- /api/pg/source/status
- /api/pg/not-loaded
- /api/pg/report-config
- /api/pg/name-normalization/list
- /api/pg/seller-targets
- /api/pg/manual-api-import/list
- /api/pg/manual-sap/list
- /api/pg/summary
- /api/pg/advisor-summary
- /api/pg/report-options

Las rutas PG leen exclusivamente:
Worker -> pg -> Hyperdrive -> Supabase -> super_dupaza

## Pruebas recomendadas
1. /api/pg/source/status
2. /api/pg/report-options
3. /api/pg/summary?date_from=2026-10-01&date_to=2026-10-27
4. /api/pg/advisor-summary?date_from=2026-10-01&date_to=2026-10-27
5. /api/pg/not-loaded

Todas deben devolver:
"source": "POSTGRESQL"

## Seguridad
/api/pg/manual-api-import/list conserva el header X-Super-Dupaza-Admin.

## Importante
No recrear Cron todavía.
No eliminar DB.
No cambiar todavía las rutas normales a PostgreSQL.

Después de validar estas lecturas, 5.26.2 moverá cada módulo con sus escrituras
correspondientes para evitar divergencia D1/Supabase.
