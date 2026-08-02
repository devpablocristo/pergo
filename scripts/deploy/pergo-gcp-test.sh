#!/usr/bin/env bash
set -euo pipefail
set +x
umask 077

deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_bin="${deploy_dir}/testdata/pergo-gcp/bin"
test_tmp=$(mktemp -d "${TMPDIR:-/tmp}/pergo-gcp-test.XXXXXX")
trap 'rm -rf "$test_tmp"' EXIT

for ignored_path in \
  local-nats.creds \
  local.pg_service.conf \
  local-ca.pem \
  local-private.key; do
  git -C "${deploy_dir}/../.." check-ignore --quiet "$ignored_path" || {
    printf 'FAIL: operator credential file patterns must stay ignored\n' >&2
    exit 1
  }
done

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local file=$1
  local expected=$2
  grep -F -- "$expected" "$file" >/dev/null ||
    fail "${file} does not contain: ${expected}"
}

assert_not_contains() {
  local file=$1
  local unexpected=$2
  if grep -F -- "$unexpected" "$file" >/dev/null; then
    fail "${file} unexpectedly contains: ${unexpected}"
  fi
}

assert_no_mutation_commands() {
  local file=$1
  if grep -E -- \
    '(^| )(create|deploy|execute|add-iam-policy-binding|set-iam-policy)( |$)' \
    "$file" >/dev/null; then
    fail "${file} contains a mutating gcloud command"
  fi
}

configure_environment() {
  local env=$1
  export PERGO_GCP_ENV="$env"
  export PERGO_GCP_PROJECT=pymes-dev-352318
  export PERGO_GCP_REGION=us-central1
  export PERGO_ARTIFACT_REPOSITORY=pymes
  export PERGO_CLOUDSQL_INSTANCE_ID=pymes-dev-db
  export PERGO_VPC_NETWORK=default
  export PERGO_VPC_SUBNET=pymes-v3-serverless
  export PERGO_RELEASE_SHA=0123456789abcdef0123456789abcdef01234567
  export PERGO_IMAGE=us-central1-docker.pkg.dev/pymes-dev-352318/pymes/pergo@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  export PERGO_EXTERNAL_URL="https://pymes-v3-pergo-webhook-${env}-123456789012.us-central1.run.app"
  export PERGO_NATS_URLS=tls://connect.example.test:4222
  export PERGO_NATS_ACCOUNT="pymes-pergo-${env}"
  if [[ "$env" == "prd" ]]; then
    export PERGO_NATS_STREAM_REPLICAS=3
  else
    export PERGO_NATS_STREAM_REPLICAS=1
  fi
  export PERGO_DB_API_SECRET_VERSION=1
  export PERGO_DB_WEBHOOK_SECRET_VERSION=2
  export PERGO_DB_WORKER_SECRET_VERSION=3
  export PERGO_DB_MIGRATE_SECRET_VERSION=4
  export PERGO_NATS_API_SECRET_VERSION=5
  export PERGO_NATS_WEBHOOK_SECRET_VERSION=6
  export PERGO_NATS_WORKER_SECRET_VERSION=7
  export PERGO_NATS_MIGRATE_SECRET_VERSION=8
  export PERGO_KEK_SECRET_VERSION=9
  export PERGO_SESSION_SECRET_VERSION=10
  export PERGO_ADMIN_PASSWORD_SECRET_VERSION=11
  unset PERGO_NATS_CA_SECRET_VERSION
  export PERGO_WORKER_INSTANCES=1
  export PERGO_GCLOUD_BIN="${fixture_bin}/gcloud"
  export PERGO_CURL_BIN="${fixture_bin}/curl"
  export PERGO_PSQL_BIN="${fixture_bin}/psql"
  export PERGO_DB_PGSERVICE_FILE="${test_tmp}/pg-service.conf"
  export PERGO_DB_PGSERVICE=pergo_audit
  printf '%s\n' '[pergo_audit]' 'host=/cloudsql/fake' >"$PERGO_DB_PGSERVICE_FILE"
  chmod 600 "$PERGO_DB_PGSERVICE_FILE"
  export FAKE_GCLOUD_LOG="${test_tmp}/gcloud.log"
  export FAKE_GCLOUD_STATE_DIR="${test_tmp}/state"
  mkdir -p "$FAKE_GCLOUD_STATE_DIR"
  : >"$FAKE_GCLOUD_LOG"
}

run_expect_failure() {
  local output=$1
  shift
  if "$@" >"$output" 2>&1; then
    fail "command unexpectedly succeeded: $*"
  fi
}

configure_environment stg
export FAKE_GCLOUD_MODE=forbid
plan_output="${test_tmp}/plan.out"
"${deploy_dir}/pergo-gcp-plan.sh" >"$plan_output"
[[ ! -s "$FAKE_GCLOUD_LOG" ]] || fail "plan called gcloud"
assert_contains "$plan_output" 'PERGO GCP PLAN (READ-ONLY)'
assert_contains "$plan_output" 'pymes-v3-pergo-api-stg'
assert_contains "$plan_output" 'serviceAccount:pymes-v3-worker-stg@pymes-dev-352318.iam.gserviceaccount.com'
assert_contains "$plan_output" '@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
assert_not_contains "$plan_output" 'session-stg-'

configure_environment stg
export FAKE_GCLOUD_MODE=complete
invalid_output="${test_tmp}/invalid-image.out"
PERGO_IMAGE=us-central1-docker.pkg.dev/pymes-dev-352318/pymes/pergo:latest \
  run_expect_failure "$invalid_output" "${deploy_dir}/pergo-gcp-plan.sh"
assert_contains "$invalid_output" 'pinned by @sha256'
[[ ! -s "$FAKE_GCLOUD_LOG" ]] || fail "invalid plan called gcloud"

configure_environment stg
export FAKE_GCLOUD_MODE=complete
audit_output="${test_tmp}/audit.out"
"${deploy_dir}/pergo-gcp-audit.sh" >"$audit_output"
assert_contains "$audit_output" 'PERGO GCP AUDIT PASSED'
assert_contains "$FAKE_GCLOUD_LOG" 'secrets versions access'
assert_contains "$FAKE_GCLOUD_LOG" 'run worker-pools describe'
assert_contains "$FAKE_GCLOUD_LOG" 'run jobs describe'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=missing-secret
missing_output="${test_tmp}/missing-secret.out"
run_expect_failure "$missing_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$missing_output" 'version 7'
assert_contains "$missing_output" 'DISABLED'
assert_not_contains "$FAKE_GCLOUD_LOG" 'run services describe'

configure_environment stg
export FAKE_GCLOUD_MODE=missing-secret
export PERGO_GCP_APPLY_CONFIRM="apply:pymes-dev-352318:stg:${PERGO_RELEASE_SHA}:${PERGO_IMAGE##*@}"
missing_apply_output="${test_tmp}/missing-apply.out"
run_expect_failure "$missing_apply_output" "${deploy_dir}/pergo-gcp-apply.sh"
assert_contains "$missing_apply_output" 'DISABLED'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=duplicate-nats
duplicate_output="${test_tmp}/duplicate-nats.out"
run_expect_failure "$duplicate_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$duplicate_output" 'four distinct NATS .creds files'
assert_not_contains "$FAKE_GCLOUD_LOG" 'run services describe'

configure_environment stg
export FAKE_GCLOUD_MODE=duplicate-db-password
duplicate_db_output="${test_tmp}/duplicate-db.out"
run_expect_failure "$duplicate_db_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$duplicate_db_output" 'four distinct database passwords'
assert_not_contains "$FAKE_GCLOUD_LOG" 'run services describe'

configure_environment stg
export FAKE_GCLOUD_MODE=db-socket-smuggling
socket_output="${test_tmp}/db-socket.out"
run_expect_failure "$socket_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$socket_output" 'unexpected database socket'
assert_not_contains "$FAKE_GCLOUD_LOG" 'run services describe'

configure_environment stg
export FAKE_GCLOUD_MODE=project-secret-accessor
accessor_output="${test_tmp}/project-accessor.out"
run_expect_failure "$accessor_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$accessor_output" 'secretmanager.versions.access'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=project-secret-admin
admin_output="${test_tmp}/project-admin.out"
run_expect_failure "$admin_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$admin_output" 'roles/secretmanager.admin'
assert_contains "$admin_output" 'secretmanager.versions.access'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=project-custom-secret-access
custom_output="${test_tmp}/project-custom.out"
run_expect_failure "$custom_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$custom_output" 'customSecretReader'
assert_contains "$custom_output" 'group:unexpected@example.test'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=project-unknown-role
unknown_role_output="${test_tmp}/project-unknown-role.out"
run_expect_failure "$unknown_role_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$unknown_role_output" 'refusing to assume it is safe'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=secret-admin
secret_admin_output="${test_tmp}/secret-admin.out"
run_expect_failure "$secret_admin_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$secret_admin_output" 'access-capable IAM role roles/secretmanager.admin'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=existing-sa-extra-role
export PERGO_GCP_APPLY_CONFIRM="apply:pymes-dev-352318:stg:${PERGO_RELEASE_SHA}:${PERGO_IMAGE##*@}"
extra_sa_role_output="${test_tmp}/extra-sa-role.out"
run_expect_failure "$extra_sa_role_output" "${deploy_dir}/pergo-gcp-apply.sh"
assert_contains "$extra_sa_role_output" 'unexpected project-level IAM roles'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=mutable-resource-secret
mutable_output="${test_tmp}/mutable-resource.out"
run_expect_failure "$mutable_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$mutable_output" 'mutable secret version'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment stg
export FAKE_GCLOUD_MODE=extra-api-invoker
invoker_output="${test_tmp}/extra-invoker.out"
run_expect_failure "$invoker_output" "${deploy_dir}/pergo-gcp-audit.sh"
assert_contains "$invoker_output" 'invoker policy drift'
assert_no_mutation_commands "$FAKE_GCLOUD_LOG"

configure_environment prd
export FAKE_GCLOUD_MODE=complete
export PERGO_GCP_APPLY_CONFIRM=wrong
wrong_confirmation="${test_tmp}/wrong-confirmation.out"
run_expect_failure "$wrong_confirmation" "${deploy_dir}/pergo-gcp-apply.sh"
assert_contains "$wrong_confirmation" 'PERGO_GCP_APPLY_CONFIRM drift'
[[ ! -s "$FAKE_GCLOUD_LOG" ]] ||
  fail "wrong confirmation reached gcloud"

configure_environment prd
export FAKE_GCLOUD_MODE=sas-missing
export PERGO_GCP_APPLY_CONFIRM="apply:pymes-dev-352318:prd:${PERGO_RELEASE_SHA}:${PERGO_IMAGE##*@}"
apply_output="${test_tmp}/apply.out"
"${deploy_dir}/pergo-gcp-apply.sh" >"$apply_output"
assert_contains "$apply_output" 'PERGO GCP AUDIT PASSED'
assert_contains "$FAKE_GCLOUD_LOG" 'iam service-accounts create pymes-v3-pergo-api-prd'
assert_contains "$FAKE_GCLOUD_LOG" 'run jobs deploy pymes-v3-pergo-migrate-prd'
assert_contains "$FAKE_GCLOUD_LOG" 'run jobs execute pymes-v3-pergo-migrate-prd'
assert_contains "$FAKE_GCLOUD_LOG" 'run worker-pools deploy pymes-v3-pergo-worker-prd'
assert_contains "$FAKE_GCLOUD_LOG" 'run deploy pymes-v3-pergo-webhook-prd'
assert_contains "$FAKE_GCLOUD_LOG" 'run deploy pymes-v3-pergo-api-prd'
assert_contains "$FAKE_GCLOUD_LOG" '--image=us-central1-docker.pkg.dev/pymes-dev-352318/pymes/pergo@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
assert_contains "$FAKE_GCLOUD_LOG" '--network=default'
assert_contains "$FAKE_GCLOUD_LOG" '--subnet=pymes-v3-serverless'
assert_contains "$FAKE_GCLOUD_LOG" '--vpc-egress=all-traffic'
assert_contains "$FAKE_GCLOUD_LOG" '--set-cloudsql-instances=pymes-dev-352318:us-central1:pymes-dev-db'
assert_contains "$FAKE_GCLOUD_LOG" '/var/run/secrets/nats/nats.creds=pymes-v3-prd-pergo-nats-worker:7'
assert_not_contains "$FAKE_GCLOUD_LOG" ':latest'

migrate_line=$(grep -n 'run jobs execute pymes-v3-pergo-migrate-prd' "$FAKE_GCLOUD_LOG" | head -1 | cut -d: -f1)
worker_line=$(grep -n 'run worker-pools deploy pymes-v3-pergo-worker-prd' "$FAKE_GCLOUD_LOG" | head -1 | cut -d: -f1)
webhook_line=$(grep -n 'run deploy pymes-v3-pergo-webhook-prd' "$FAKE_GCLOUD_LOG" | head -1 | cut -d: -f1)
api_line=$(grep -n 'run deploy pymes-v3-pergo-api-prd' "$FAKE_GCLOUD_LOG" | head -1 | cut -d: -f1)
((migrate_line < worker_line && worker_line < webhook_line && webhook_line < api_line)) ||
  fail "apply did not preserve migrate -> worker -> webhook -> api order"

printf 'PASS: PerGo GCP deployment scripts are deterministic and fail closed\n'
