# SUPER DUPAZA 5.26.3 — PostgreSQL Sync Shadow Test

## Estado previo validado
- Dashboard normal: PostgreSQL primario.
- D1 fallback: activo.
- Escritura PostgreSQL: validada con BEGIN/ROLLBACK.
- Cron: todavía deshabilitados.

## Qué agrega esta fase
Un sync manual protegido que ejecuta la lógica actual de:
- ZasConnect
- BO / VT
- loaded_refs
- report_orders
- sync_state

pero escribiendo en Supabase/PostgreSQL por Hyperdrive.

## Importante
La Queue normal TODAVÍA ejecuta el flujo D1.
No se recrean Cron.
No se elimina DB.

## Endpoint de prueba
POST /api/postgres-sync-test?days=7

Header:
X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>

El sync PostgreSQL corre dentro de una transacción.
Si ocurre un error:
ROLLBACK.

Si termina:
COMMIT.

El endpoint NO modifica D1.

## Estado después de la prueba
GET /api/postgres-sync-state

También requiere X-Super-Dupaza-Admin.

## PowerShell
$token = "TU_D1_SYNC_TOKEN"

Invoke-RestMethod `
  -Method POST `
  -Uri "https://super-dupaza.bagnerlg.workers.dev/api/postgres-sync-test?days=7" `
  -Headers @{"X-Super-Dupaza-Admin"=$token}

Resultado esperado:
- ok: True
- engine: POSTGRESQL
- via: HYPERDRIVE
- transactionCommitted: True
- queueSwitched: False
- d1Modified: False

Después validar:
- /api/source/status
- /api/summary
- /api/not-loaded

Si esto pasa, Fase 5.26.4 cambia el Queue consumer a PostgreSQL.
