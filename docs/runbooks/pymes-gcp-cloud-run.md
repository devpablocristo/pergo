# PerGo para Pymes en Cloud Run

Este runbook gobierna el despliegue de PerGo para Pymes v3 en el proyecto
compartido `pymes-dev-352318`, región `us-central1`. La automatización no crea
ni contrata NATS: STG y PRD requieren cuentas externas ya preparadas, con
credenciales diferentes por workload.

## Topología cerrada

| Recurso | Nombre por ambiente | Exposición | Identidad |
|---|---|---|---|
| API/admin | `pymes-v3-pergo-api-{stg,prd}` | Cloud Run autenticado | `pymes-v3-pergo-api-{env}` |
| callbacks | `pymes-v3-pergo-webhook-{stg,prd}` | `allUsers` | `pymes-v3-pergo-webhook-{env}` |
| entregas | `pymes-v3-pergo-worker-{stg,prd}` | Worker Pool sin URL | `pymes-v3-pergo-worker-{env}` |
| migraciones | `pymes-v3-pergo-migrate-{stg,prd}` | Job sin URL | `pymes-v3-pergo-migrate-{env}` |

El servicio API no es público. Su binding `roles/run.invoker` contiene
exclusivamente:

```text
serviceAccount:pymes-v3-worker-{env}@pymes-dev-352318.iam.gserviceaccount.com
```

Pymes llama ese servicio con:

- la API key PerGo en `Authorization`;
- el token OIDC de Cloud Run en `X-Serverless-Authorization`;
- el URL del servicio API como audience del token.

El callback admite `allUsers` porque Meta y Telegram no pueden presentar IAM de
Google. El argumento y `PERGO_RUNTIME_PROFILE` quedan fijados a `webhook`; el
middleware de PerGo sólo deja pasar `/webhooks/*`, `/healthz` y `/readyz`.
`/admin/*` y `/api/*` responden `404` en ese workload.

Los cuatro workloads usan:

- la misma imagen inmutable `pymes/pergo@sha256:...`;
- Cloud SQL connector hacia
  `pymes-dev-352318:us-central1:pymes-dev-db`;
- Direct VPC egress `all-traffic` por
  `default/pymes-v3-serverless`, con el NAT compartido ya existente;
- una cuenta de servicio y credenciales PostgreSQL/NATS propias;
- `PERGO_MEDIA_MODE=disabled`; este rollout admite mensajes de texto.

WhatsApp Web, su pairing y sus sesiones no forman parte del despliegue
separado. El piloto usa WhatsApp Cloud (WABA).

## Tres comandos con responsabilidades separadas

```bash
scripts/deploy/pergo-gcp-plan.sh
scripts/deploy/pergo-gcp-audit.sh
scripts/deploy/pergo-gcp-apply.sh
```

- `plan` valida inputs locales y muestra nombres, pins y orden; nunca llama
  `gcloud`.
- `audit` sólo lee GCP, valida contenidos secretos sin imprimirlos, ejecuta el
  preflight SQL y comprueba workloads/IAM/probes.
- `apply` primero ejecuta exactamente los mismos preflights. Sólo después de
  una confirmación ligada a proyecto, ambiente, commit y digest crea
  identidades/IAM, ejecuta migraciones y despliega. Termina ejecutando
  `audit`; no existe flag para omitirlo.

Las pruebas determinísticas usan binarios falsos y no acceden a GCP:

```bash
make deploy-gcp-test
```

## Inventario compartido que debe existir

La automatización acepta únicamente:

```text
project        pymes-dev-352318
region         us-central1
Artifact Repo  pymes (DOCKER)
Cloud SQL      pymes-dev-db (POSTGRES_16)
network        default
subnet         pymes-v3-serverless / 10.120.0.0/24
KMS key        pymes-v3-{env}/secrets
```

No cambia la disponibilidad, IP, backups ni configuración global de Cloud SQL:
la instancia es compartida con otros productos. PerGo entra sólo mediante el
Cloud SQL connector.

### Bloqueo de IAM encontrado el 2026-08-02

La lectura real del proyecto encontró cuatro accessors de Secret Manager a
nivel proyecto:

```text
github-actions@pymes-dev-352318.iam.gserviceaccount.com
pymes-core-runtime-stg@pymes-dev-352318.iam.gserviceaccount.com
pymes-github-actions-stg@pymes-dev-352318.iam.gserviceaccount.com
pymes-vertical-runtime-stg@pymes-dev-352318.iam.gserviceaccount.com
```

Mientras conserven cualquier rol que incluya
`secretmanager.versions.access` en el proyecto, también podrían leer cualquier
secreto PerGo nuevo. Esto incluye `roles/secretmanager.secretAccessor`,
`roles/secretmanager.admin`, roles básicos o roles personalizados que contengan
ese permiso. La única excepción explícita es el owner humano existente
`user:softponti@gmail.com`; no se admiten otros users ni identidades
`serviceAccount:`, `group:`, `domain:`, `allUsers` o `allAuthenticatedUsers` con
ese permiso a nivel proyecto. El preflight inspecciona `includedPermissions` y
bloquea `apply` hasta que esos grants se migren a bindings sobre los secretos
concretos que cada identidad realmente consume. Sobre cada secreto PerGo, el
único rol capaz de leer payloads permitido es
`roles/secretmanager.secretAccessor`, con los miembros exactos del perfil. No
se debe ampliar la excepción para ocultar el problema.

## PostgreSQL: preparación humana

Cada ambiente tiene una base lógica y cinco roles:

```text
pergo_stg / pergo_prd
pergo_{env}_runtime     NOLOGIN
pergo_{env}_api         LOGIN
pergo_{env}_webhook     LOGIN
pergo_{env}_worker      LOGIN
pergo_{env}_migrate     LOGIN
```

Un DBA debe:

1. crear la base con `pergo_{env}_migrate` como owner;
2. crear los cuatro roles LOGIN sin atributos administrativos;
3. agregar API, webhook y worker al rol NOLOGIN `runtime`;
4. dar `CONNECT`, `USAGE` y al migrador `CREATE` en `public`;
5. fijar, **para el rol migrate**, default privileges de tablas
   `SELECT/INSERT/UPDATE/DELETE` y secuencias `SELECT/USAGE/UPDATE` a
   `runtime`;
6. establecer las contraseñas mediante `\password`, sin incluirlas en SQL,
   historial de shell o tickets.

Antes de migrar una base que ya tenga tablas, el DBA también concede esos
privilegios sobre todas las tablas y secuencias existentes. Ningún rol PerGo
puede tener `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION` o
`BYPASSRLS`.

El auditor se conecta mediante un archivo local `pg_service` de modo `0400` o
`0600`; la contraseña no aparece en argumentos:

```ini
[pergo_stg_audit]
host=127.0.0.1
port=5432
dbname=pergo_stg
user=pergo_stg_migrate
password=OBTENER_DESDE_EL_GESTOR_SEGURO
sslmode=disable
```

El ejemplo asume un Cloud SQL Auth Proxy local ya autenticado. Nunca se
commitea ese archivo.

```bash
export PERGO_DB_PGSERVICE_FILE=/ruta/segura/pergo-stg.pg_service.conf
export PERGO_DB_PGSERVICE=pergo_stg_audit
```

El preflight ejecuta
[`pergo-role-preflight.sql`](../../scripts/deploy/sql/pergo-role-preflight.sql)
y aborta si falta un rol, membresía, privilegio o default ACL.

## Secret Manager

Cloud Run debe consumir secretos globales con una réplica user-managed en
`us-central1`, cifrada por:

```text
projects/pymes-dev-352318/locations/us-central1/keyRings/pymes-v3-{env}/cryptoKeys/secrets
```

No use un **regional secret**: Cloud Run Worker Pools no admite ese tipo de
recurso. La ubicación de la única réplica sí es regional. Para crear el
container, use una replication policy equivalente a:

```yaml
replication:
  userManaged:
    replicas:
    - location: us-central1
      customerManagedEncryption:
        kmsKeyName: projects/pymes-dev-352318/locations/us-central1/keyRings/pymes-v3-ENV/cryptoKeys/secrets
```

Se requieren estos containers por ambiente:

| Secreto | Consumidor |
|---|---|
| `pymes-v3-{env}-pergo-db-api` | API |
| `pymes-v3-{env}-pergo-db-webhook` | webhook |
| `pymes-v3-{env}-pergo-db-worker` | worker |
| `pymes-v3-{env}-pergo-db-migrate` | migrate |
| `pymes-v3-{env}-pergo-nats-api` | API |
| `pymes-v3-{env}-pergo-nats-webhook` | webhook |
| `pymes-v3-{env}-pergo-nats-worker` | worker |
| `pymes-v3-{env}-pergo-nats-migrate` | migrate |
| `pymes-v3-{env}-pergo-kek` | los cuatro |
| `pymes-v3-{env}-pergo-session-secret` | API |
| `pymes-v3-{env}-pergo-admin-password` | API |
| `pymes-v3-{env}-pergo-nats-ca` | opcional, los cuatro |

Cada valor se carga desde un archivo local protegido:

```bash
gcloud secrets versions add NOMBRE \
  --project=pymes-dev-352318 \
  --data-file=/ruta/segura/valor
```

No use `--data-file=-` con valores pegados en la terminal y nunca envíe
credenciales por chat. El deploy exige una versión numérica explícita; `latest`
está prohibido.

Los cuatro DSN usan usuarios y contraseñas distintos, autoridad vacía y un único
parámetro `host` con el socket exacto:

```text
postgres://pergo_ENV_PROFILE:PASSWORD_URL_ENCODED@/pergo_ENV?host=/cloudsql/pymes-dev-352318:us-central1:pymes-dev-db
```

La KEK decodifica a 32 bytes aleatorios. El secreto de sesión tiene al menos 32
caracteres y la contraseña admin al menos 16. STG y PRD no comparten valores.

Los containers Pymes ya llamados
`pymes-v3-{env}-pergo-api-key` y
`pymes-v3-{env}-pergo-webhook-secrets` no son secretos de arranque del runtime
PerGo: pertenecen a la integración Pymes↔PerGo y se completan durante el
onboarding/piloto correspondiente.

## NATS externo

El operador de NATS/Synadia entrega, sin que estos scripts creen recursos:

- cuenta `pymes-pergo-stg` y cuenta `pymes-pergo-prd` separadas;
- endpoint `tls://` o `wss://`, nunca credenciales en la URL;
- cuatro archivos `.creds` diferentes por ambiente;
- identidad `migrate` con administración sólo de streams/consumers PerGo;
- API y webhook con publicación limitada a sus subjects V2;
- worker con fetch/ack de consumers y publicaciones de resultado;
- producción con al menos tres réplicas.

El job `migrate` crea/reconcilia contratos JetStream y el guard de ambiente.
Los runtimes sólo verifican/bindean. Si existe una cuenta legada, siga
[el rollout coordinado 038–042](038-042-coordinated-rollout.md); este apply
siempre fija `PERGO_NATS_ADOPT_DRAINED_LEGACY=false`.

Las credenciales se montan como archivo en:

```text
/var/run/secrets/nats/nats.creds
```

Nunca se convierten en una variable de entorno. Un CA privado opcional se monta
en `/var/run/secrets/nats/ca.pem`.

## Variables de release

Ejemplo STG; los números son pins reales a completar por el operador:

```bash
export PERGO_GCP_ENV=stg
export PERGO_RELEASE_SHA=COMMIT_GO_DE_40_HEX
export PERGO_IMAGE=us-central1-docker.pkg.dev/pymes-dev-352318/pymes/pergo@sha256:DIGEST_DE_64_HEX
export PERGO_EXTERNAL_URL=https://URL_PUBLICA_DEL_WEBHOOK
export PERGO_NATS_URLS=tls://ENDPOINT_NATS:4222
export PERGO_NATS_ACCOUNT=pymes-pergo-stg
export PERGO_NATS_STREAM_REPLICAS=1

export PERGO_DB_API_SECRET_VERSION=NUMERO
export PERGO_DB_WEBHOOK_SECRET_VERSION=NUMERO
export PERGO_DB_WORKER_SECRET_VERSION=NUMERO
export PERGO_DB_MIGRATE_SECRET_VERSION=NUMERO
export PERGO_NATS_API_SECRET_VERSION=NUMERO
export PERGO_NATS_WEBHOOK_SECRET_VERSION=NUMERO
export PERGO_NATS_WORKER_SECRET_VERSION=NUMERO
export PERGO_NATS_MIGRATE_SECRET_VERSION=NUMERO
export PERGO_KEK_SECRET_VERSION=NUMERO
export PERGO_SESSION_SECRET_VERSION=NUMERO
export PERGO_ADMIN_PASSWORD_SECRET_VERSION=NUMERO
```

PRD cambia ambiente/cuenta/URL/pins, exige
`PERGO_NATS_STREAM_REPLICAS>=3` y usa secretos completamente distintos.

## Ejecución

1. Ejecute tests y plan:

   ```bash
   make deploy-gcp-test
   scripts/deploy/pergo-gcp-plan.sh
   ```

2. Complete cada preflight faltante hasta que el audit llegue a la parte de
   workloads. Antes del primer apply es esperado que informe que todavía no
   existen identidades/workloads.

3. Copie **exactamente** la confirmación que imprime `plan`:

   ```bash
   export PERGO_GCP_APPLY_CONFIRM='apply:PROYECTO:ENV:COMMIT:sha256:DIGEST'
   ```

4. Ejecute:

   ```bash
   scripts/deploy/pergo-gcp-apply.sh
   ```

El apply crea IAM, despliega el job, lo ejecuta con `--wait`, y sólo después
promueve worker, callback y API. Si la migración falla no despliega nuevos
runtimes.

## Validación posterior

`audit` prueba:

- digest, service accounts, Cloud SQL, Direct VPC y pins de secretos;
- ausencia de claves de service account administradas por usuarios;
- IAM exacto de cada secreto y servicio;
- webhook `/healthz` y `/readyz` en `200`;
- webhook `/admin/` y `/api/v1/messages` en `404`;
- API `/healthz` sin OIDC en `401/403`;
- cero referencias `latest`.

Después, el piloto agrega verificaciones autenticadas desde
`pymes-v3-worker-{env}`, alta de workspace/API key, callback WABA firmado,
mensaje de texto y entrega idempotente.

## Roll-forward y recuperación

Las migraciones 038–042 y JetStream V2 no permiten un rollback binario aislado.
Ante una falla:

1. bloquee nuevo ingreso público si hay riesgo de protocolo;
2. preserve KEK, base y JetStream;
3. corrija la imagen y repita `migrate`;
4. promueva API, webhook y worker con el mismo digest corregido;
5. restaure PostgreSQL y NATS juntos únicamente si el roll-forward es
   imposible y existe evidencia coordinada previa.

No haga `goose down`, no borre streams ni cambie KEK para intentar recuperar.

## Entradas que requieren una persona

La automatización deliberadamente no puede inventar:

- contrato/cuentas/usuarios `.creds` del proveedor NATS;
- contraseñas y archivo `pg_service` del DBA;
- valores/versiones de Secret Manager;
- migración de los accessors Secret Manager globales a IAM por secreto;
- dominio/callback autorizado en Meta o Telegram;
- build publicado y digest de Artifact Registry;
- workspace, API key y número WABA controlado para el piloto.

Hasta que existan esas entradas, `apply` aborta antes de la primera mutación.
