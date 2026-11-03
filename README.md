# SUPER DUPAZA 5.27.1 — ZasConnect Stable Auth

## Problema corregido

El Worker usaba el login web de ZasConnect mediante un `Next-Action` de Next.js.

Los IDs de Server Actions pueden cambiar entre builds. Cuando eso ocurre:

- login devuelve HTTP 409
- body: `Server Action unavailable.`
- Queue entra en `QUEUE_ERROR`
- PostgreSQL deja de recibir ventas nuevas

## Estrategia 5.27.1

SUPER DUPAZA acepta dos modos:

### 1. BEARER_TOKEN — recomendado

Secret de Cloudflare:

`ZAS_EXPORT_TOKEN`

El Worker llama directamente:

`GET https://zasconnect.zashome.com.gt/api/orders/export-data?...`

con:

`Authorization: Bearer <ZAS_EXPORT_TOKEN>`

No hace login web.
No usa `crm_session`.
No usa `ZAS_NEXT_ACTION`.

### 2. LEGACY_SERVER_ACTION — compatibilidad temporal

Si `ZAS_EXPORT_TOKEN` NO existe, conserva:

- ZAS_LOGIN_EMAIL
- ZAS_LOGIN_PASSWORD
- ZAS_NEXT_ACTION

Si la Action está vencida ahora devuelve un error explícito:

`ZASCONNECT_LEGACY_ACTION_STALE`

## Endpoint de diagnóstico

GET `/api/zasconnect-auth-test`

Header:

`X-Super-Dupaza-Admin: <D1_SYNC_TOKEN>`

Respuesta esperada final:

```json
{
  "ok": true,
  "source": "ZASCONNECT",
  "authMode": "BEARER_TOKEN"
}
```

## Cambio requerido en ZasConnect

El endpoint existente `/api/orders/export-data` debe aceptar un token de servicio
además de la sesión humana actual.

Ver:

`zasconnect-patch/IMPLEMENTACION.md`

## Importante

No reutilizar `D1_SYNC_TOKEN`.
Crear un token distinto y de alta entropía para esta integración.
