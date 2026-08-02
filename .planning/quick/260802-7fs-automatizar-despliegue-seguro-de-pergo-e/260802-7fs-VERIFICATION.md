---
status: passed
verified: 2026-08-02
---

# Verificación

| Must have | Evidencia |
|---|---|
| STG/PRD aislados | nombres derivados del ambiente, cuentas NATS exactas, bases/roles/secretos/SAs separados |
| Imagen inmutable | regex exacta del repositorio `pymes/pergo@sha256` y test que rechaza `:latest` |
| API privada | policy reemplazada con el único worker Pymes; audit rechaza invokers adicionales |
| Callback limitado | perfil `webhook`, probes HTTP y tests de `/admin`/`/api` en `404` |
| Worker Pool + migrate Job | comandos y orden cubiertos por fake determinístico |
| `.creds` por perfil | mounts de archivo, formatos validados y hashes distintos |
| Cloud SQL + Direct VPC | pins exactos y argumentos verificados en tests |
| Secrets numéricos/CMEK | versiones positivas, única réplica, CMEK exacta y rechazo de `latest` |
| Plan/audit sin mutaciones | fake en modo prohibido y aserciones de verbos mutantes |
| Apply fail-closed | confirmación incorrecta y preflight incompleto no alcanzan ninguna mutación |
| CI/runbook | target Make y step GitHub Actions; runbook operativo completo |

No se verificó un rollout real porque las credenciales humanas/proveedor todavía
no existen y la tarea prohíbe mutar GCP. Esa ausencia no es un hueco del código:
es un preflight deliberado y probado.

