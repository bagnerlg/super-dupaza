# SUPER DUPAZA 5.26.2 — PostgreSQL Primary Reads + Dual Control Writes

## Qué cambia
Las rutas normales del dashboard ahora intentan PostgreSQL primero y usan D1 solo como fallback:

- /api/source/status
- /api/not-loaded
- /api/report-config
- /api/name-normalization/list
- /api/seller-targets
- /api/manual-sap/list
- /api/summary
- /api/advisor-summary
- /api/report-options

Cuando PostgreSQL responde correctamente, la respuesta incluye:
- databasePrimary: "POSTGRESQL"
- fallbackUsed: false

Si PostgreSQL falla:
- usa D1
- databasePrimary: "D1_FALLBACK"
- fallbackUsed: true

## Escrituras de control en doble escritura
PostgreSQL es primario y D1 queda como espejo temporal para:
- POST /api/report-config/labels
- POST /api/name-normalization/save
- POST /api/name-normalization/delete
- POST /api/seller-targets/save
- POST /api/manual-sap/mark
- POST /api/manual-sap/resolve

Las respuestas pueden incluir:
- mirroredToD1: true
- mirroredToD1: false + mirrorWarning si el espejo D1 falla

## Aún sigue en D1
- Sincronización BO/VT
- Sincronización ZasConnect
- manual API importer
- Queue pesada de sincronización
- cualquier tarea de reconstrucción que todavía no haya sido migrada

## Cron
NO se incluyen Cron triggers. Deben seguir deshabilitados durante esta fase.

## Prueba segura de escritura PostgreSQL
Endpoint:
GET /api/postgres-write-smoke

Header:
X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>

La prueba ejecuta:
BEGIN
UPDATE sin cambio
ROLLBACK

Resultado esperado:
{
  "ok": true,
  "writePermission": true,
  "rolledBack": true,
  "rowsTouched": 1
}

No deja cambios persistentes.

## Validación recomendada después del deploy
1. /api/postgres-write-smoke (con header)
2. /api/summary?date_from=2026-10-01&date_to=2026-10-27
3. /api/advisor-summary?date_from=2026-10-01&date_to=2026-10-27
4. /api/not-loaded
5. Abrir dashboard normal

En las rutas normales esperamos:
databasePrimary = POSTGRESQL
fallbackUsed = false

## Nota sobre alias y metas
La aparición de metas bajo alias como ALEJANDRA SIAN / ERICK SIQUIBACHE / YESSICA CARRERA
ya existe en la lógica D1 original. Se conserva intencionalmente durante la migración.
