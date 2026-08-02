#!/usr/bin/env bash
set -euo pipefail
set +x
umask 077

deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=pergo-gcp-lib.sh
source "${deploy_dir}/pergo-gcp-lib.sh"

pergo_load_config audit
pergo_external_preflight

pergo_sorted_unique() {
  sed '/^[[:space:]]*$/d' | sort -u
}

pergo_assert_profile_service_account() {
  local profile=$1
  local account
  account=$(pergo_profile_sa "$profile")
  local listed
  listed=$(pergo_gcloud iam service-accounts list \
    --filter="email=${account}" --format='value(email)')
  pergo_expect_exact "${profile} service account" "$account" "$listed"
  local user_keys
  user_keys=$(pergo_gcloud iam service-accounts keys list \
    --iam-account="$account" --managed-by=user --format='value(name)')
  [[ -z "$user_keys" ]] ||
    pergo_die "${profile} service account has forbidden user-managed keys"

  local cloudsql_binding
  cloudsql_binding=$(pergo_gcloud projects get-iam-policy \
    "$PERGO_GCP_PROJECT" \
    --flatten='bindings[].members' \
    --filter="bindings.role=roles/cloudsql.client AND bindings.members=serviceAccount:${account}" \
    --format='value(bindings.members)')
  pergo_expect_exact "${profile} Cloud SQL IAM" \
    "serviceAccount:${account}" "$cloudsql_binding"

  local project_roles
  project_roles=$(pergo_gcloud projects get-iam-policy \
    "$PERGO_GCP_PROJECT" \
    --flatten='bindings[].members' \
    --filter="bindings.members=serviceAccount:${account}" \
    --format='value(bindings.role)' | pergo_sorted_unique)
  pergo_expect_exact "${profile} project-level IAM roles" \
    "roles/cloudsql.client" "$project_roles"
}

pergo_assert_secret_iam() {
  local key=$1
  pergo_assert_secret_access_policy "$key" exact
}

pergo_assert_resource_payload() {
  local kind=$1
  local name=$2
  local profile=$3
  local payload=$4
  local db_key="db_${profile}"
  local nats_key="nats_${profile}"
  local value
  for value in \
    "$PERGO_IMAGE" \
    "$(pergo_profile_sa "$profile")" \
    "$PERGO_CLOUDSQL_CONNECTION" \
    "$PERGO_VPC_NETWORK" \
    "$PERGO_VPC_SUBNET" \
    "all-traffic" \
    "$PERGO_APPLICATION_ENVIRONMENT" \
    "$profile" \
    "$PERGO_NATS_URLS" \
    "$PERGO_NATS_ACCOUNT" \
    "$(pergo_secret_name "$db_key")" \
    "$(pergo_secret_version "$db_key")" \
    "$(pergo_secret_name "$nats_key")" \
    "$(pergo_secret_version "$nats_key")" \
    "$(pergo_secret_name kek)" \
    "$(pergo_secret_version kek)" \
    "/var/run/secrets/nats/nats.creds"; do
    pergo_expect_contains "${kind} ${name}" "$payload" "$value"
  done
  [[ "$payload" != *'"latest"'* && "$payload" != *':latest'* ]] ||
    pergo_die "${kind} ${name} contains a mutable secret version"
  if [[ "$profile" == "api" || "$profile" == "webhook" ]]; then
    pergo_expect_contains "${kind} ${name} startup/readiness" "$payload" "/readyz"
    pergo_expect_contains "${kind} ${name} liveness" "$payload" "/healthz"
  fi
  if [[ "$profile" == "api" ]]; then
    pergo_expect_contains "${kind} ${name}" "$payload" "$(pergo_secret_name session)"
    pergo_expect_contains "${kind} ${name}" "$payload" "$(pergo_secret_name admin)"
  fi
}

pergo_assert_invoker_policy() {
  local service=$1
  local expected_member=$2
  local members
  members=$(pergo_gcloud run services get-iam-policy "$service" \
    --region="$PERGO_GCP_REGION" \
    --flatten='bindings[].members' \
    --filter='bindings.role=roles/run.invoker' \
    --format='value(bindings.members)' | pergo_sorted_unique)
  pergo_expect_exact "${service} invoker policy" "$expected_member" "$members"
}

for profile in "${PERGO_PROFILES[@]}"; do
  pergo_assert_profile_service_account "$profile"
done
for key in "${PERGO_REQUIRED_SECRET_KEYS[@]}"; do
  pergo_assert_secret_iam "$key"
done

api_service=$(pergo_profile_resource api)
webhook_service=$(pergo_profile_resource webhook)
worker_pool=$(pergo_profile_resource worker)
migrate_job=$(pergo_profile_resource migrate)

api_payload=$(pergo_gcloud run services describe "$api_service" \
  --region="$PERGO_GCP_REGION" --format=json)
webhook_payload=$(pergo_gcloud run services describe "$webhook_service" \
  --region="$PERGO_GCP_REGION" --format=json)
worker_payload=$(pergo_gcloud run worker-pools describe "$worker_pool" \
  --region="$PERGO_GCP_REGION" --format=json)
migrate_payload=$(pergo_gcloud run jobs describe "$migrate_job" \
  --region="$PERGO_GCP_REGION" --format=json)

pergo_assert_resource_payload service "$api_service" api "$api_payload"
pergo_assert_resource_payload service "$webhook_service" webhook "$webhook_payload"
pergo_assert_resource_payload worker-pool "$worker_pool" worker "$worker_payload"
pergo_assert_resource_payload job "$migrate_job" migrate "$migrate_payload"
worker_instances=$(pergo_gcloud run worker-pools describe "$worker_pool" \
  --region="$PERGO_GCP_REGION" \
  --format='value(scaling.manualInstanceCount)')
pergo_expect_exact "worker pool instance count" \
  "$PERGO_WORKER_INSTANCES" "$worker_instances"
pergo_expect_contains "migration retry policy" "$migrate_payload" '"maxRetries": 0'

pergo_assert_invoker_policy "$api_service" \
  "serviceAccount:${PERGO_PYMES_CALLER_SA}"
pergo_assert_invoker_policy "$webhook_service" "allUsers"

public_job_members=$(pergo_gcloud run jobs get-iam-policy "$migrate_job" \
  --region="$PERGO_GCP_REGION" \
  --flatten='bindings[].members' \
  --format='value(bindings.members)' |
  awk '$0 == "allUsers" || $0 == "allAuthenticatedUsers"' |
  pergo_sorted_unique)
[[ -z "$public_job_members" ]] ||
  pergo_die "${migrate_job} must not have public IAM members"

http_audit=${PERGO_GCP_HTTP_AUDIT:-true}
case "$http_audit" in
  true|false) ;;
  *) pergo_die "PERGO_GCP_HTTP_AUDIT must be true or false" ;;
esac
if [[ "$http_audit" == "true" ]]; then
  pergo_require_command "$PERGO_CURL_BIN"
  webhook_url=$(pergo_gcloud run services describe "$webhook_service" \
    --region="$PERGO_GCP_REGION" --format='value(status.url)')
  api_url=$(pergo_gcloud run services describe "$api_service" \
    --region="$PERGO_GCP_REGION" --format='value(status.url)')
  [[ "$webhook_url" == https://* && "$api_url" == https://* ]] ||
    pergo_die "Cloud Run services do not expose canonical HTTPS URLs"

  status=$("$PERGO_CURL_BIN" --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --max-time 15 "${webhook_url}/healthz")
  pergo_expect_exact "webhook liveness" "200" "$status"
  status=$("$PERGO_CURL_BIN" --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --max-time 15 "${webhook_url}/readyz")
  pergo_expect_exact "webhook readiness" "200" "$status"
  status=$("$PERGO_CURL_BIN" --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --max-time 15 "${webhook_url}/admin/")
  pergo_expect_exact "public webhook admin isolation" "404" "$status"
  status=$("$PERGO_CURL_BIN" --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --max-time 15 "${webhook_url}/api/v1/messages")
  pergo_expect_exact "public webhook API isolation" "404" "$status"
  status=$("$PERGO_CURL_BIN" --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --max-time 15 "${api_url}/healthz")
  case "$status" in
    401|403) ;;
    *) pergo_die "private API must reject unauthenticated requests; got HTTP ${status}" ;;
  esac
fi

pergo_note "PERGO GCP AUDIT PASSED: ${PERGO_GCP_ENV} is digest-pinned, isolated and ready"
