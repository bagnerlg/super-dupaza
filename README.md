# SUPER DUPAZA 5.27.0 FINAL

Arquitectura final de producción:

```text
Cron Cloudflare
  -> Queue super-dupaza-sync
  -> Worker SUPER DUPAZA
  -> pg
  -> Cloudflare Hyperdrive
  -> Supabase PostgreSQL
  -> schema super_dupaza
```

D1 ya no es un binding ni una dependencia operativa.

## Estrategia de sincronización optimizada

### FAST_INCREMENTAL — cada 5 minutos
Cron:

```text
*/5 * * * *
```

- Revisa ZasConnect únicamente en la ventana reciente de 7 días.
- Pide hasta 100 filas por página por defecto (`ZAS_PAGE_SIZE`, rango 25-200).
- Compara `source_hash` de cabeceras y líneas.
- Solo hace UPSERT de órdenes que realmente cambiaron.
- Solo reemplaza líneas para órdenes cuyo hash de líneas cambió.
- BO/VT primero consulta únicamente `status`.
- BO/VT descarga el conjunto completo solo cuando cambia `sourceModified` / `lastImport` o existe un backfill requerido.
- NO descarga el API completo de ZasConnect para reconciliación histórica.
- NO ejecuta los chequeos históricos de drift/materialización en cada vuelta.
- La Queue no solicita el snapshot posterior al COMMIT; esto ahorra dos sentencias SQL por ejecución automática.

### FULL_RECONCILE — una vez al día
Cron:

```text
17 12 * * *
```

Cloudflare Cron usa UTC; esto corresponde a 06:17 en Guatemala.

- Ventana reciente de 30 días.
- Ejecuta reconciliación completa de ZasConnect para detectar órdenes antiguas eliminadas/canceladas que no aparecerían en la ventana rápida.
- Verifica flags históricos de materialización.
- Ejecuta detección de drift de `report_orders`.
- Mantiene la red de seguridad para modificaciones antiguas.

Esta combinación evita releer toda la historia cada 5 minutos sin perder la comprobación completa diaria.

## Por qué no usamos solo `updatedAt`

Las filas de ZasConnect pueden incluir `updatedAt`/`updated_at` y se guarda como `source_updated_at`, pero el API actual se pagina principalmente por el orden del export y el corte probado se basa en `orderDate`. No se ha verificado un filtro servidor `updatedSince` ni ordenamiento garantizado por fecha de modificación.

Por seguridad, 5.27.0 conserva:

1. ventana reciente + hashes cada 5 minutos;
2. reconciliación completa diaria.

Si ZasConnect incorpora más adelante `updatedSince`, cursor incremental o webhook de cambio, ese será el siguiente salto de eficiencia.

## Capacidad Free revisada — 28-oct-2026

### Cloudflare Workers Free

- 100,000 requests/día.
- 128 MB de memoria.
- 10 ms de CPU para requests HTTP y Cron; la espera de red/DB no cuenta como CPU.
- Queue consumers tienen hasta 15 minutos de wall-clock y CPU configurable hasta 5 minutos.

### Cloudflare Hyperdrive Free

- 100,000 sentencias SQL/día.
- Aproximadamente 20 conexiones de origen por configuración.
- Máximo 60 segundos por sentencia.

Cada SELECT/INSERT/UPDATE/DELETE ejecutado por Hyperdrive cuenta como una sentencia.

### Cloudflare Queues Free

- 10,000 operaciones/día.
- Un mensaje pequeño procesado normalmente consume aproximadamente 3 operaciones: write + read + delete.

Con dos Cron actuales hay aproximadamente 289 mensajes/día (288 rápidos + 1 diario), equivalentes a ~867 operaciones/día antes de retries: menos de 9% de la cuota gratuita.

### Supabase Free

- 500 MB de cuota de tamaño de base de datos por proyecto.
- Shared CPU / 500 MB RAM.
- 5 GB de egress + 5 GB cached egress según la página de precios.
- El disco físico del proyecto Free es fijo de 2 GB y entra en modo read-only cerca de 95% de uso.
- Los proyectos Free pueden pausarse tras actividad insuficiente durante aproximadamente una semana; el Cron cada 5 minutos mantiene actividad real de DB.
- Free no ofrece backups descargables desde la plataforma; para un sistema crítico conviene mantener backup externo o considerar Pro.

Fuentes oficiales:
- https://developers.cloudflare.com/workers/platform/limits/
- https://developers.cloudflare.com/workers/platform/pricing/
- https://developers.cloudflare.com/hyperdrive/platform/limits/
- https://developers.cloudflare.com/queues/platform/pricing/
- https://developers.cloudflare.com/queues/platform/limits/
- https://supabase.com/docs/guides/platform/billing-on-supabase
- https://supabase.com/docs/guides/platform/database-size
- https://supabase.com/docs/guides/platform/free-project-pausing
- https://supabase.com/pricing

## Endpoint de capacidad

Nuevo endpoint protegido:

```text
GET /api/postgres-usage
X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>
```

Ejemplo PowerShell:

```powershell
Invoke-RestMethod `
  -Uri "https://super-dupaza.bagnerlg.workers.dev/api/postgres-usage" `
  -Headers @{"X-Super-Dupaza-Admin"=$token} |
  ConvertTo-Json -Depth 10
```

Devuelve:

- tamaño total de la base PostgreSQL;
- tamaño del esquema `super_dupaza`;
- porcentaje aproximado frente a 500 MB;
- tamaño y filas estimadas por tabla;
- conexiones activas;
- `max_connections` de PostgreSQL;
- referencias de los límites Free de Workers, Hyperdrive y Queues;
- estrategia FAST/FULL configurada.

## Índice final recomendado

Ejecutar una vez en Supabase SQL Editor:

```text
sql/01_FINAL_OPTIMIZATION.sql
```

Agrega un índice por `loaded_refs(report_date)`, usado por la reconciliación diaria, y actualiza estadísticas del planner.

## Variables y bindings

Bindings requeridos:

```text
HYPERDRIVE -> super-dupaza
SUPER_DUPAZA_SYNC_QUEUE -> super-dupaza-sync
```

No debe existir binding `DB` / D1.

Secrets/variables existentes se conservan; no se incluyen valores sensibles en este repositorio.

Opcional:

```text
ZAS_PAGE_SIZE=100
```

Si no existe, el Worker usa 100 automáticamente. El código limita el valor a 25-200.

## Deploy

El proyecto mantiene:

```json
"deploy": "wrangler deploy --keep-vars"
```

Por GitHub/Cloudflare Workers Builds, reemplazar:

```text
package.json
wrangler.toml
README.md
src/index.js
sql/01_FINAL_OPTIMIZATION.sql
```

## Validación posterior al deploy

1. Auditoría:

```text
GET /api/postgres-runtime-audit
```

Debe indicar:

```text
phase = 5.27.0
engine = POSTGRESQL
migrationComplete = true
syncStrategy = FAST_5M + FULL_RECONCILE_DAILY
```

2. Capacidad:

```text
GET /api/postgres-usage
```

3. Después de un Cron de 5 minutos:

```text
GET /api/postgres-sync-state
```

En `SCHEDULER.metadata_json` debe verse:

```text
syncMode = FAST_INCREMENTAL
event = QUEUE_SUCCESS
fullApiReconcile = false
```

4. Después del Cron diario:

```text
syncMode = FULL_RECONCILE
fullApiReconcile = true
```

## Hyperdrive cache

Se recomienda mantener el Hyperdrive operativo con cache de queries deshabilitado para este Worker porque hay lecturas inmediatamente después de escrituras y se prioriza consistencia.

Si el dashboard creciera mucho en usuarios, una optimización futura sería crear una segunda configuración Hyperdrive con cache habilitado exclusivamente para endpoints de lectura tolerantes a 30-60 segundos de staleness, manteniendo el binding actual sin cache para sync, configuración y read-after-write.
