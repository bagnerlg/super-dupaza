# SUPER DUPAZA 5.27.3 — ZasConnect Stable Auth + Alerta de unidades + Copiado alineado

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

## Nuevo en 5.27.2 — Alerta visual de múltiples unidades

En **Órdenes no cargadas**, cada línea de SKU cuya `quantity` sea numérica y superior a 1
muestra una advertencia ámbar **⚠ N** en la columna **Cantidad** y una marca tenue
en **Código artículo**. El contador indica cuántas líneas visibles requieren atención,
respetando filtros. Sirve tanto para componentes de combos como para productos sueltos.

- Se mantiene la advertencia de pedidos de fecha anterior; la cantidad resaltada tiene prioridad visual solo en sus dos celdas.
- Al tocar/copiar la celda, se copia la **cantidad numérica original**, sin el símbolo de advertencia.
- El tooltip explica que deben verificarse las unidades al cargar en SAP.
- El Excel conserva las cantidades exactas y colorea de ámbar la celda **Cantidad** cuando es mayor a 1.
- No se modifican APIs, modelo de datos, PostgreSQL, Queue, Cron, secrets ni autenticación de ZasConnect.

**Alcance:** se compara contra 1 unidad por línea, porque el dashboard no consulta
la lista maestra de materiales (BOM) del SKU padre. Una futura validación estricta
contra combos cuya composición original contenga 2 o más de un componente
requeriría la composición maestra de cada SKU padre. Esta marca es una advertencia
operativa, no un cambio automático de unidades.


## Nuevo en 5.27.3 — Copiar y reconocer unidades revisadas

**Órdenes no cargadas**: al hacer clic en una celda de **Cantidad**, se copia
solo su número original y, si el portapapeles confirmó el copiado, desaparece
la alerta ámbar de **esa línea** (también su marca en Código artículo).
Copiar nombre, SKU u otra columna **no** da por revisada la cantidad. Si el
copiado falla, la alerta permanece. La marca de copiado dura la sesión de la
pestaña (sessionStorage), sobrevive a filtros, cambios de vista y recargas
mientras la sesión continúe; si cambia la cantidad, vuelve la advertencia.
La alerta de otro SKU no se elimina por accidente.

**📋 CANTIDADES PADRE**: copia una única columna de texto con una fila por
cada renglón actualmente visible en la tabla. Se agrega la cantidad de cada
línea principal y un valor vacío para cada hijo. Por ejemplo, si hay una línea
principal con cantidad 1, tres líneas hijas y luego otra línea principal con
cantidad 1, el portapapeles tiene exactamente:

```text
1



1
```

Es decir, 5 filas alineadas (3 filas vacías intermedias); copiar a Excel o
una rejilla de SAP no debe desplazar la cantidad del siguiente padre.
**Nunca reemplaza cantidades de los hijos por cero ni las modifica en origen**:
solo las omite en este modo de copiado. Cuando el copiado se realiza, solo
las cantidades principales incluidas se consideran copiadas. Las alertas de
hijos con cantidad mayor a 1 siguen presentes para evitar omisiones.

El origen de datos actualmente identifica un primer renglón por *tracking*,
pero **no** informa explícitamente qué líneas pertenecen a cada código de combo
padre. Por defecto se considera principal la primera línea del tracking.
Cuando un pedido contiene un segundo artículo padre, pulsar **+ PADRE** en
la columna **Control** de esa línea lo añade como principal para el copiado
alineado. Es una marca exclusivamente del navegador, no cambia SAP, DB, Queue
ni el artículo original, y puede desmarcarse. No se pretende deducir BOM o
cantidades padre sin catálogo maestro.

Los encabezados con símbolo ⧉ permiten copiar toda la columna visible sin
eliminar filas: campos generales conservan blancos en líneas hijas;
Código artículo / Descripción / Cantidad copian valores reales por SKU.
Al copiar **la columna Cantidad completa**, se consideran copiadas las
cantidades de todas las líneas visibles (incluidos hijos).

**Prueba local:** `npm test` (5 casos de alineación, alertas, cambio de unidades,
y segundos padres dentro del mismo pedido). Además se verificó la sintaxis
del Worker y del JavaScript de la interfaz. **No se desplegó a producción.**
