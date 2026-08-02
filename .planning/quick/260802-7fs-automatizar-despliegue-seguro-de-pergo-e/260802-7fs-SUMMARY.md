---
status: complete
task: Automatizar despliegue seguro de PerGo en GCP compartido
completed: 2026-08-02
---

# Resumen

PerGo ahora tiene entrypoints separados `plan`, `audit` y `apply` para la
topología Pymes en `pymes-dev-352318/us-central1`. La automatización fija imagen
por digest, Cloud SQL connector, Direct VPC/NAT, identidades por perfil,
secretos versionados, credenciales NATS `.creds` por workload, migraciones
antes del rollout e IAM exacto para API privada y callback público.

El preflight comprueba la réplica/CMEK de Secret Manager, inspecciona los
payloads sin imprimirlos, rechaza credenciales repetidas, valida roles/default
ACL de PostgreSQL y se niega a operar si existe acceso Secret Manager a nivel
proyecto. `apply` requiere una confirmación ligada a commit/digest, no permite
omitir el audit final y nunca provisiona Synadia/NATS.

## Validación

- `make deploy-gcp-test`
- `bash -n` sobre los siete scripts shell
- `go test ./cmd/pergo -run 'TestRuntimeProfile(RouteIsolation|ProcessRoles)' -count=1`
- `git diff --check`
- Gitleaks sobre scripts, runbook y artefactos GSD

Todos los checks pasaron. Las pruebas de deploy usan `gcloud`, `curl` y `psql`
falsos; esta tarea no ejecutó mutaciones GCP.

## Entradas externas pendientes

- cuentas y cuatro `.creds` NATS por ambiente;
- base/roles/contraseñas y archivo `pg_service`;
- containers y versiones de secretos;
- retiro de cuatro accessors Secret Manager existentes a nivel proyecto;
- imagen publicada por digest;
- URL/callback de proveedor y datos del piloto.

El runbook documenta esas entradas; su ausencia hace que `apply` falle antes de
la primera mutación.
