#!/usr/bin/env bash
set -euo pipefail
set +x
umask 077

deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=pergo-gcp-lib.sh
source "${deploy_dir}/pergo-gcp-lib.sh"

pergo_load_config apply

expected_confirmation=$(pergo_expected_confirmation)
pergo_expect_exact "PERGO_GCP_APPLY_CONFIRM" \
  "$expected_confirmation" "${PERGO_GCP_APPLY_CONFIRM:-}"

# Nothing below this line runs until all provider, database, role, secret,
# image, network and encryption prerequisites have been proven.
pergo_external_preflight

apply_tmp=$(mktemp -d "${TMPDIR:-/tmp}/pergo-gcp-apply.XXXXXX")
trap 'rm -rf "$apply_tmp"' EXIT

pergo_ensure_service_account() {
  local profile=$1
  local account_id account listed
  account_id=$(pergo_profile_account_id "$profile")
  account=$(pergo_profile_sa "$profile")
  listed=$(pergo_gcloud iam service-accounts list \
    --filter="email=${account}" --format='value(email)')
  if [[ -z "$listed" ]]; then
    pergo_gcloud iam service-accounts create "$account_id" \
      --display-name="Pymes v3 PerGo ${profile} ${PERGO_GCP_ENV}"
  else
    pergo_expect_exact "${profile} service account" "$account" "$listed"
  fi
  local keys
  keys=$(pergo_gcloud iam service-accounts keys list \
    --iam-account="$account" --managed-by=user --format='value(name)')
  [[ -z "$keys" ]] ||
    pergo_die "${profile} service account has forbidden user-managed keys"

  pergo_gcloud projects add-iam-policy-binding "$PERGO_GCP_PROJECT" \
    --member="serviceAccount:${account}" \
    --role=roles/cloudsql.client \
    --condition=None >/dev/null
}

pergo_grant_secret_access() {
  local key=$1
  local secret member
  secret=$(pergo_secret_name "$key")

  # Recheck immediately before each write as a defense against policy drift
  # after the all-secret preflight.
  pergo_assert_secret_access_policy "$key" subset
  local allowed
  allowed=$(pergo_secret_expected_members "$key" |
    sed '/^[[:space:]]*$/d' | sort -u)

  while IFS= read -r member; do
    [[ -n "$member" ]] || continue
    pergo_gcloud secrets add-iam-policy-binding "$secret" \
      --member="$member" \
      --role=roles/secretmanager.secretAccessor \
      --condition=None >/dev/null
  done <<<"$allowed"
}

for profile in "${PERGO_PROFILES[@]}"; do
  pergo_ensure_service_account "$profile"
done
for key in "${PERGO_REQUIRED_SECRET_KEYS[@]}"; do
  pergo_grant_secret_access "$key"
done

declare -a common_args
pergo_common_cloud_run_args migrate common_args
pergo_gcloud run jobs deploy "$(pergo_profile_resource migrate)" \
  "${common_args[@]}" \
  --tasks=1 \
  --parallelism=1 \
  --max-retries=0 \
  --task-timeout=30m

pergo_gcloud run jobs execute "$(pergo_profile_resource migrate)" \
  --region="$PERGO_GCP_REGION" --wait

pergo_common_cloud_run_args worker common_args
pergo_gcloud run worker-pools deploy "$(pergo_profile_resource worker)" \
  "${common_args[@]}" \
  --instances="$PERGO_WORKER_INSTANCES"

pergo_deploy_http_service() {
  local profile=$1
  local service
  service=$(pergo_profile_resource "$profile")
  pergo_common_cloud_run_args "$profile" common_args
  pergo_gcloud run deploy "$service" \
    "${common_args[@]}" \
    --port=8080 \
    --ingress=all \
    --no-allow-unauthenticated \
    --min-instances=0 \
    --max-instances=10 \
    --concurrency=80 \
    --timeout=60s \
    --startup-probe='httpGet.path=/readyz,httpGet.port=8080,timeoutSeconds=2,periodSeconds=2,failureThreshold=30' \
    --liveness-probe='httpGet.path=/healthz,httpGet.port=8080,timeoutSeconds=2,periodSeconds=10,failureThreshold=3' \
    --readiness-probe='httpGet.path=/readyz,httpGet.port=8080,timeoutSeconds=2,periodSeconds=5,failureThreshold=2'
}

pergo_deploy_http_service webhook
pergo_deploy_http_service api

api_policy="${apply_tmp}/api-policy.yaml"
webhook_policy="${apply_tmp}/webhook-policy.yaml"
pergo_render_service_policy "$(pergo_profile_resource api)" \
  "serviceAccount:${PERGO_PYMES_CALLER_SA}" "$api_policy"
pergo_render_service_policy "$(pergo_profile_resource webhook)" \
  "allUsers" "$webhook_policy"
pergo_gcloud run services set-iam-policy "$(pergo_profile_resource api)" \
  "$api_policy" --region="$PERGO_GCP_REGION" >/dev/null
pergo_gcloud run services set-iam-policy "$(pergo_profile_resource webhook)" \
  "$webhook_policy" --region="$PERGO_GCP_REGION" >/dev/null

# Post-apply verification is mandatory. The audit includes exact IAM, resource
# configuration, secret pins, profile isolation, readiness and public/private
# HTTP behavior.
export PERGO_GCP_HTTP_AUDIT=true
"${deploy_dir}/pergo-gcp-audit.sh"
