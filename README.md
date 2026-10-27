# SUPER DUPAZA 5.26.0 — GitHub baseline

Objetivo: pasar el Worker real a Workers Builds/GitHub SIN cambiar todavía D1 como motor.

## Conserva
- DB -> D1 super-dupaza
- HYPERDRIVE -> super-dupaza
- SUPER_DUPAZA_SYNC_QUEUE -> super-dupaza-sync
- Queue consumer: batch 1 / timeout 1 / retries 3 / concurrency 1
- Variables configuradas en Dashboard gracias a `--keep-vars`
- Secrets existentes (Cloudflare los preserva)

## No incluye
- Cron triggers. Deben seguir eliminados durante la migración.
- Cambio de dashboard a PostgreSQL. D1 sigue siendo el motor.

## Deploy command en Cloudflare Builds
`npx wrangler deploy --keep-vars`

## Prueba nueva
GET `/api/postgres-smoke`

Requiere header:
`X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>`

Debe devolver:
- ok: true
- verified: true
- current_user: super_dupaza_worker
- loaded_refs: 7031
- report_orders: 2435
- zas_orders: 430
- zas_order_lines: 1200

Cuando esta prueba funcione dentro del Worker real, se inicia Fase 5.26.1:
migración de lecturas D1 -> PostgreSQL.

#nuevo
