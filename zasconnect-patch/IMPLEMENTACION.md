# Parche requerido en ZasConnect

Archivo probable:

`app/api/orders/export-data/route.ts`

No reemplaces la lógica de consulta/exportación existente. Solo cambia la frontera
de autenticación.

## 1. Crear variable secreta en ZasConnect

Nombre:

`SUPER_DUPAZA_EXPORT_TOKEN`

Debe estar disponible en runtime del backend y nunca exponerse con prefijo
`NEXT_PUBLIC_`.

## 2. Agregar helper

```ts
function hasSuperDupazaServiceAccess(request: Request) {
  const expected = String(
    process.env.SUPER_DUPAZA_EXPORT_TOKEN || ""
  ).trim();

  if (!expected) return false;

  const authorization =
    request.headers.get("authorization") || "";

  return authorization === `Bearer ${expected}`;
}
```

## 3. Adaptar el guard de sesión

Actualmente el endpoint devuelve `401 Sesión requerida` cuando no existe una
sesión CRM.

La lógica debe quedar conceptualmente así:

```ts
export async function GET(request: Request) {
  const serviceAccess =
    hasSuperDupazaServiceAccess(request);

  if (!serviceAccess) {
    // CONSERVAR AQUÍ EL CÓDIGO DE SESIÓN QUE YA TIENE ZASCONNECT.
    //
    // const session = ...
    // if (!session) {
    //   return NextResponse.json(
    //     { ok: false, error: "Sesión requerida" },
    //     { status: 401 }
    //   );
    // }
  }

  const url = new URL(request.url);
  const companyCode =
    String(url.searchParams.get("companyCode") || "")
      .trim()
      .toUpperCase();

  // El token de SUPER DUPAZA debe tener alcance SOLO DUPAZA.
  if (serviceAccess && companyCode !== "DUPAZA") {
    return Response.json(
      {
        ok: false,
        error: "Token de integración limitado a DUPAZA."
      },
      { status: 403 }
    );
  }

  // A PARTIR DE AQUÍ:
  // conservar exactamente la lógica actual de export-data.
}
```

## 4. Generar un token seguro en PowerShell

```powershell
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$bytes = New-Object byte[] 32
$rng.GetBytes($bytes)
$token = [Convert]::ToBase64String($bytes)
$token
```

Guardar EL MISMO valor:

### En ZasConnect
`SUPER_DUPAZA_EXPORT_TOKEN`

### En Cloudflare Worker super-dupaza
`ZAS_EXPORT_TOKEN`

No pegar el token en GitHub.

## 5. Orden recomendado de despliegue

1. Agregar soporte Bearer en ZasConnect.
2. Desplegar ZasConnect.
3. Probar el endpoint con Bearer desde backend/Postman/PowerShell.
4. Agregar `ZAS_EXPORT_TOKEN` como Secret en Cloudflare Worker.
5. Desplegar SUPER DUPAZA 5.27.1.
6. Consultar `/api/postgres-runtime-audit`.
7. Debe aparecer:
   `zasConnectAuthMode = BEARER_TOKEN`
8. Consultar `/api/zasconnect-auth-test`.
9. Debe responder `ok=true`.
10. Esperar un Cron de 5 minutos y validar `QUEUE_SUCCESS`.

## Después

Cuando BEARER_TOKEN esté validado, puedes eliminar del Worker:

- ZAS_LOGIN_EMAIL
- ZAS_LOGIN_PASSWORD
- ZAS_NEXT_ACTION

La versión 5.27.1 todavía conserva compatibilidad legacy para hacer el cambio sin
interrumpir producción.
