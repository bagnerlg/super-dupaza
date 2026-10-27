# SUPER DUPAZA 5.26.5 — Cron -> Queue -> PostgreSQL

## Estado validado antes de esta fase
- PostgreSQL/Supabase validado.
- Dashboard normal: PostgreSQL primario.
- D1 fallback: activo para lecturas.
- Escrituras de control: PostgreSQL + espejo temporal D1.
- Sync manual PostgreSQL: OK.
- Queue manual -> PostgreSQL: OK.
- SCHEDULER terminó con QUEUE_SUCCESS.
- transactionCommitted=true.
- d1Modified=false.
- last_error=null.

## Cambio de esta fase
Se reactivan los Cron originales mediante wrangler.toml:

[triggers]
crons = [ "*/5 * * * *", "17 12 * * *" ]

Flujo automático:

Cron
-> scheduled()
-> SUPER_DUPAZA_SYNC_QUEUE
-> queue()
-> runQueuedSync()
-> syncIncrementalToPostgres()
-> Hyperdrive
-> Supabase PostgreSQL

## Significado
*/5 * * * *
- Encola sincronización incremental de 7 días cada 5 minutos.

17 12 * * *
- Encola sincronización de 30 días una vez al día.

Cloudflare Cron usa UTC.
En Guatemala:
- */5 sigue siendo cada 5 minutos.
- 17 12 * * * = 06:17 hora Guatemala.

## Queue
Se conserva:
- max_batch_size = 1
- max_batch_timeout = 1
- max_retries = 3
- max_concurrency = 1

Así los jobs no se pisan.

## D1
El binding DB se conserva todavía.
La Queue automática NO sincroniza D1.
D1 queda como respaldo/legacy temporal.

## Después del deploy
Verificar en Cloudflare:
Workers & Pages
-> super-dupaza
-> Configuración / Triggers
-> Cron Triggers

Deben aparecer:
- */5 * * * *
- 17 12 * * *

## Validación operativa
Consultar:

GET /api/postgres-sync-state

con:
X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>

En SCHEDULER se espera:
- event = QUEUE_SUCCESS
- engine = POSTGRESQL
- via = HYPERDRIVE
- last_error = null

El campo cron indicará cuál trigger originó la ejecución.

## Próxima fase
Después de validar operación automática:
5.26.6 = retirar dependencias D1 restantes y preparar eliminación del binding DB.
