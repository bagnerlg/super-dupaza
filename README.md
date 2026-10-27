# SUPER DUPAZA 5.26.4 — Queue -> PostgreSQL

## Cambio principal
El consumidor de Queue `super-dupaza-sync` ya NO ejecuta el sync pesado contra D1.

Ahora ejecuta:

Queue
-> runQueuedSync()
-> syncIncrementalToPostgres()
-> pg
-> Hyperdrive
-> Supabase PostgreSQL
-> schema super_dupaza

## Comportamiento de errores
Si PostgreSQL falla:
- se registra QUEUE_ERROR en super_dupaza.sync_state
- el error se vuelve a lanzar
- Cloudflare Queue aplica retry

NO se hace fallback silencioso a D1 en el consumer.

## D1
El binding DB se mantiene temporalmente porque:
- todavía hay endpoints administrativos/legacy
- sirve de respaldo estático durante la transición

El sync pesado de Queue ya no actualiza D1.

## Cron
Todavía NO se incluyen Cron triggers.
Primero se valida manualmente Queue -> PostgreSQL.

## Prueba manual de Queue
POST /api/postgres-queue-test

Header:
X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>

PowerShell:

$token = "TU_D1_SYNC_TOKEN"

Invoke-RestMethod `
  -Method POST `
  -Uri "https://super-dupaza.bagnerlg.workers.dev/api/postgres-queue-test" `
  -Headers @{"X-Super-Dupaza-Admin"=$token}

Debe responder inmediatamente:
- ok: True
- queued: True
- engine: POSTGRESQL

Luego consultar:

Invoke-RestMethod `
  -Uri "https://super-dupaza.bagnerlg.workers.dev/api/postgres-sync-state" `
  -Headers @{"X-Super-Dupaza-Admin"=$token} |
  ConvertTo-Json -Depth 10

En syncState, SCHEDULER debe terminar con:
- event: QUEUE_SUCCESS
- engine: POSTGRESQL
- via: HYPERDRIVE
- last_error: null

## Después
Si Queue manual pasa, Fase 5.26.5 reactivará los Cron:
- */5 * * * *
- 17 12 * * *

y PostgreSQL quedará como motor automático principal.
