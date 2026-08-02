#!/usr/bin/env bash

# Shared, fail-closed helpers for the Pymes-managed PerGo deployment.
# This file is sourced by plan/audit/apply entrypoints; it never executes a
# cloud mutation on its own.

set -euo pipefail
set +x
umask 077

PERGO_DEPLOY_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PERGO_REPO_ROOT=$(cd "${PERGO_DEPLOY_DIR}/../.." && pwd)

pergo_die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

pergo_note() {
  printf '%s\n' "$*"
}

pergo_require_command() {
  command -v "$1" >/dev/null 2>&1 ||
    pergo_die "required command not found: $1"
}

pergo_require_value() {
  local variable=$1
  [[ -n "${!variable:-}" ]] || pergo_die "set ${variable}"
}

pergo_validate_resource_name() {
  local label=$1
  local value=$2
  [[ "$value" =~ ^[a-z]([-a-z0-9]*[a-z0-9])?$ ]] ||
    pergo_die "${label} must be a lowercase GCP resource name"
}

pergo_validate_secret_version() {
  local variable=$1
  local value=${!variable:-}
  [[ "$value" =~ ^[1-9][0-9]*$ ]] ||
    pergo_die "${variable} must be an explicit positive numeric version"
}

pergo_secret_name() {
  local key=$1
  case "$key" in
    db_api) printf '%s-pergo-db-api' "$PERGO_PREFIX" ;;
    db_webhook) printf '%s-pergo-db-webhook' "$PERGO_PREFIX" ;;
    db_worker) printf '%s-pergo-db-worker' "$PERGO_PREFIX" ;;
    db_migrate) printf '%s-pergo-db-migrate' "$PERGO_PREFIX" ;;
    nats_api) printf '%s-pergo-nats-api' "$PERGO_PREFIX" ;;
    nats_webhook) printf '%s-pergo-nats-webhook' "$PERGO_PREFIX" ;;
    nats_worker) printf '%s-pergo-nats-worker' "$PERGO_PREFIX" ;;
    nats_migrate) printf '%s-pergo-nats-migrate' "$PERGO_PREFIX" ;;
    kek) printf '%s-pergo-kek' "$PERGO_PREFIX" ;;
    session) printf '%s-pergo-session-secret' "$PERGO_PREFIX" ;;
    admin) printf '%s-pergo-admin-password' "$PERGO_PREFIX" ;;
    nats_ca) printf '%s-pergo-nats-ca' "$PERGO_PREFIX" ;;
    *) pergo_die "unknown PerGo secret key: ${key}" ;;
  esac
}

pergo_secret_version() {
  local key=$1
  case "$key" in
    db_api) printf '%s' "$PERGO_DB_API_SECRET_VERSION" ;;
    db_webhook) printf '%s' "$PERGO_DB_WEBHOOK_SECRET_VERSION" ;;
    db_worker) printf '%s' "$PERGO_DB_WORKER_SECRET_VERSION" ;;
    db_migrate) printf '%s' "$PERGO_DB_MIGRATE_SECRET_VERSION" ;;
    nats_api) printf '%s' "$PERGO_NATS_API_SECRET_VERSION" ;;
    nats_webhook) printf '%s' "$PERGO_NATS_WEBHOOK_SECRET_VERSION" ;;
    nats_worker) printf '%s' "$PERGO_NATS_WORKER_SECRET_VERSION" ;;
    nats_migrate) printf '%s' "$PERGO_NATS_MIGRATE_SECRET_VERSION" ;;
    kek) printf '%s' "$PERGO_KEK_SECRET_VERSION" ;;
    session) printf '%s' "$PERGO_SESSION_SECRET_VERSION" ;;
    admin) printf '%s' "$PERGO_ADMIN_PASSWORD_SECRET_VERSION" ;;
    nats_ca)
      [[ -n "${PERGO_NATS_CA_SECRET_VERSION:-}" ]] ||
        pergo_die "PERGO_NATS_CA_SECRET_VERSION is not configured"
      printf '%s' "$PERGO_NATS_CA_SECRET_VERSION"
      ;;
    *) pergo_die "unknown PerGo secret key: ${key}" ;;
  esac
}

pergo_profile_sa() {
  local profile=$1
  printf 'pymes-v3-pergo-%s-%s@%s.iam.gserviceaccount.com' \
    "$profile" "$PERGO_GCP_ENV" "$PERGO_GCP_PROJECT"
}

pergo_profile_account_id() {
  local profile=$1
  printf 'pymes-v3-pergo-%s-%s' "$profile" "$PERGO_GCP_ENV"
}

pergo_profile_resource() {
  local profile=$1
  printf 'pymes-v3-pergo-%s-%s' "$profile" "$PERGO_GCP_ENV"
}

pergo_profile_db_role() {
  local profile=$1
  printf 'pergo_%s_%s' "$PERGO_GCP_ENV" "$profile"
}

pergo_load_config() {
  local caller=${1:-unknown}

  pergo_require_value PERGO_GCP_ENV
  case "$PERGO_GCP_ENV" in
    stg) PERGO_APPLICATION_ENVIRONMENT=staging ;;
    prd) PERGO_APPLICATION_ENVIRONMENT=production ;;
    *) pergo_die "PERGO_GCP_ENV must be stg or prd" ;;
  esac

  PERGO_GCP_PROJECT=${PERGO_GCP_PROJECT:-pymes-dev-352318}
  PERGO_GCP_REGION=${PERGO_GCP_REGION:-us-central1}
  PERGO_ARTIFACT_REPOSITORY=${PERGO_ARTIFACT_REPOSITORY:-pymes}
  PERGO_CLOUDSQL_INSTANCE_ID=${PERGO_CLOUDSQL_INSTANCE_ID:-pymes-dev-db}
  PERGO_VPC_NETWORK=${PERGO_VPC_NETWORK:-default}
  PERGO_VPC_SUBNET=${PERGO_VPC_SUBNET:-pymes-v3-serverless}
  PERGO_GCLOUD_BIN=${PERGO_GCLOUD_BIN:-gcloud}
  PERGO_CURL_BIN=${PERGO_CURL_BIN:-curl}
  PERGO_PSQL_BIN=${PERGO_PSQL_BIN:-psql}

  [[ "$PERGO_GCP_PROJECT" == "pymes-dev-352318" ]] ||
    pergo_die "PerGo Pymes deploy is locked to project pymes-dev-352318"
  [[ "$PERGO_GCP_REGION" == "us-central1" ]] ||
    pergo_die "PerGo Pymes deploy is locked to region us-central1"
  [[ "$PERGO_ARTIFACT_REPOSITORY" == "pymes" ]] ||
    pergo_die "PerGo image repository must be pymes"
  [[ "$PERGO_CLOUDSQL_INSTANCE_ID" == "pymes-dev-db" ]] ||
    pergo_die "PerGo Cloud SQL instance must be pymes-dev-db"
  [[ "$PERGO_VPC_NETWORK" == "default" ]] ||
    pergo_die "PerGo Direct VPC network must be default"
  [[ "$PERGO_VPC_SUBNET" == "pymes-v3-serverless" ]] ||
    pergo_die "PerGo Direct VPC subnet must be pymes-v3-serverless"

  PERGO_PREFIX="pymes-v3-${PERGO_GCP_ENV}"
  PERGO_DATABASE="pergo_${PERGO_GCP_ENV}"
  PERGO_RUNTIME_DB_ROLE="pergo_${PERGO_GCP_ENV}_runtime"
  PERGO_CLOUDSQL_CONNECTION="${PERGO_GCP_PROJECT}:${PERGO_GCP_REGION}:${PERGO_CLOUDSQL_INSTANCE_ID}"
  PERGO_SECRETS_KMS_KEY="projects/${PERGO_GCP_PROJECT}/locations/${PERGO_GCP_REGION}/keyRings/${PERGO_PREFIX}/cryptoKeys/secrets"
  PERGO_PYMES_CALLER_SA="pymes-v3-worker-${PERGO_GCP_ENV}@${PERGO_GCP_PROJECT}.iam.gserviceaccount.com"

  pergo_require_value PERGO_IMAGE
  local image_pattern="^${PERGO_GCP_REGION}-docker\\.pkg\\.dev/${PERGO_GCP_PROJECT}/${PERGO_ARTIFACT_REPOSITORY}/pergo@sha256:[0-9a-f]{64}$"
  [[ "$PERGO_IMAGE" =~ $image_pattern ]] ||
    pergo_die "PERGO_IMAGE must be the pymes/pergo image pinned by @sha256:<64 lowercase hex>"

  pergo_require_value PERGO_RELEASE_SHA
  [[ "$PERGO_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]] ||
    pergo_die "PERGO_RELEASE_SHA must be exactly 40 lowercase hexadecimal characters"

  pergo_require_value PERGO_EXTERNAL_URL
  [[ "$PERGO_EXTERNAL_URL" =~ ^https://[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(\:[1-9][0-9]{0,4})?$ ]] ||
    pergo_die "PERGO_EXTERNAL_URL must be an HTTPS origin without path, credentials, query or fragment"

  pergo_require_value PERGO_NATS_URLS
  [[ "$PERGO_NATS_URLS" != *$'\n'* &&
     "$PERGO_NATS_URLS" != *$'\r'* &&
     "$PERGO_NATS_URLS" != *[[:space:]]* &&
     "$PERGO_NATS_URLS" != *"|"* &&
     "$PERGO_NATS_URLS" != *"@"* ]] ||
    pergo_die "PERGO_NATS_URLS contains unsafe delimiters, whitespace or URL userinfo"
  local nats_url
  local -a nats_urls
  IFS=',' read -r -a nats_urls <<<"$PERGO_NATS_URLS"
  ((${#nats_urls[@]} > 0)) ||
    pergo_die "PERGO_NATS_URLS must contain at least one address"
  for nats_url in "${nats_urls[@]}"; do
    [[ "$nats_url" =~ ^(tls|wss)://[A-Za-z0-9.-]+(\:[1-9][0-9]{0,4})?(/[A-Za-z0-9._~/-]*)?$ ]] ||
      pergo_die "every NATS address must use tls:// or wss:// without credentials"
  done

  pergo_require_value PERGO_NATS_ACCOUNT
  [[ "$PERGO_NATS_ACCOUNT" == "pymes-pergo-${PERGO_GCP_ENV}" ]] ||
    pergo_die "PERGO_NATS_ACCOUNT must be pymes-pergo-${PERGO_GCP_ENV}"
  if [[ -n "${PERGO_NATS_TLS_SERVER_NAME:-}" ]]; then
    [[ "$PERGO_NATS_TLS_SERVER_NAME" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] ||
      pergo_die "PERGO_NATS_TLS_SERVER_NAME must be a DNS name"
  fi

  PERGO_NATS_STREAM_REPLICAS=${PERGO_NATS_STREAM_REPLICAS:-}
  if [[ -z "$PERGO_NATS_STREAM_REPLICAS" ]]; then
    if [[ "$PERGO_GCP_ENV" == "prd" ]]; then
      PERGO_NATS_STREAM_REPLICAS=3
    else
      PERGO_NATS_STREAM_REPLICAS=1
    fi
  fi
  [[ "$PERGO_NATS_STREAM_REPLICAS" =~ ^[1-9][0-9]*$ ]] ||
    pergo_die "PERGO_NATS_STREAM_REPLICAS must be a positive integer"
  if [[ "$PERGO_GCP_ENV" == "prd" ]] &&
    ((PERGO_NATS_STREAM_REPLICAS < 3)); then
    pergo_die "production requires PERGO_NATS_STREAM_REPLICAS >= 3"
  fi

  local version_variable
  for version_variable in \
    PERGO_DB_API_SECRET_VERSION \
    PERGO_DB_WEBHOOK_SECRET_VERSION \
    PERGO_DB_WORKER_SECRET_VERSION \
    PERGO_DB_MIGRATE_SECRET_VERSION \
    PERGO_NATS_API_SECRET_VERSION \
    PERGO_NATS_WEBHOOK_SECRET_VERSION \
    PERGO_NATS_WORKER_SECRET_VERSION \
    PERGO_NATS_MIGRATE_SECRET_VERSION \
    PERGO_KEK_SECRET_VERSION \
    PERGO_SESSION_SECRET_VERSION \
    PERGO_ADMIN_PASSWORD_SECRET_VERSION; do
    pergo_validate_secret_version "$version_variable"
  done
  if [[ -n "${PERGO_NATS_CA_SECRET_VERSION:-}" ]]; then
    pergo_validate_secret_version PERGO_NATS_CA_SECRET_VERSION
  fi

  PERGO_WORKER_INSTANCES=${PERGO_WORKER_INSTANCES:-1}
  [[ "$PERGO_WORKER_INSTANCES" =~ ^[1-9][0-9]*$ ]] ||
    pergo_die "PERGO_WORKER_INSTANCES must be a positive integer"

  local name
  for name in \
    "$PERGO_PREFIX" \
    "$PERGO_ARTIFACT_REPOSITORY" \
    "$PERGO_CLOUDSQL_INSTANCE_ID" \
    "$PERGO_VPC_NETWORK" \
    "$PERGO_VPC_SUBNET"; do
    pergo_validate_resource_name resource "$name"
  done

  PERGO_PROFILES=(api webhook worker migrate)
  PERGO_REQUIRED_SECRET_KEYS=(
    db_api db_webhook db_worker db_migrate
    nats_api nats_webhook nats_worker nats_migrate
    kek session admin
  )
  if [[ -n "${PERGO_NATS_CA_SECRET_VERSION:-}" ]]; then
    PERGO_REQUIRED_SECRET_KEYS+=(nats_ca)
  fi

  export \
    PERGO_APPLICATION_ENVIRONMENT \
    PERGO_ARTIFACT_REPOSITORY \
    PERGO_CLOUDSQL_CONNECTION \
    PERGO_CLOUDSQL_INSTANCE_ID \
    PERGO_DATABASE \
    PERGO_GCP_ENV \
    PERGO_GCP_PROJECT \
    PERGO_GCP_REGION \
    PERGO_IMAGE \
    PERGO_NATS_ACCOUNT \
    PERGO_NATS_STREAM_REPLICAS \
    PERGO_NATS_TLS_SERVER_NAME \
    PERGO_NATS_URLS \
    PERGO_PREFIX \
    PERGO_RELEASE_SHA \
    PERGO_RUNTIME_DB_ROLE \
    PERGO_SECRETS_KMS_KEY \
    PERGO_VPC_NETWORK \
    PERGO_VPC_SUBNET

  [[ "$caller" != "apply" || -z "${PERGO_GCP_APPLY_CONFIRM:-}" ||
     "$PERGO_GCP_APPLY_CONFIRM" != *$'\n'* ]] ||
    pergo_die "PERGO_GCP_APPLY_CONFIRM contains a newline"
}

pergo_gcloud() {
  "$PERGO_GCLOUD_BIN" "$@" --project="$PERGO_GCP_PROJECT"
}

pergo_iam_role_has_permission() {
  local role=$1
  local permission=$2
  local permissions
  if ! permissions=$(pergo_gcloud iam roles describe "$role" \
    --format='value(includedPermissions)'); then
    pergo_die "cannot inspect IAM role ${role}; refusing to assume it is safe"
  fi
  awk -v wanted="$permission" '
    BEGIN { RS = "[,;[:space:]]+" }
    $0 == wanted { found = 1 }
    END { exit(found ? 0 : 1) }
  ' <<<"$permissions"
}

pergo_assert_no_project_permission() {
  local permission=$1
  local purpose=$2
  local allowed_member=${3:-}
  local bindings role member
  bindings=$(pergo_gcloud projects get-iam-policy "$PERGO_GCP_PROJECT" \
    --flatten='bindings[].members' \
    --format='value(bindings.role,bindings.members)' |
    sed '/^[[:space:]]*$/d' | sort -u)
  while IFS=$'\t' read -r role member; do
    [[ -n "$role" && -n "$member" ]] ||
      pergo_die "cannot parse project IAM role/member binding"
    if pergo_iam_role_has_permission "$role" "$permission"; then
      if [[ -n "$allowed_member" && "$member" == "$allowed_member" ]]; then
        continue
      fi
      pergo_die \
        "project-wide IAM binding ${role} -> ${member} grants ${permission}; ${purpose} requires resource-level IAM"
    fi
  done <<<"$bindings"
}

pergo_secret_accessor_members() {
  local secret=$1
  pergo_gcloud secrets get-iam-policy "$secret" \
    --flatten='bindings[].members' \
    --filter='bindings.role=roles/secretmanager.secretAccessor' \
    --format='value(bindings.members)' |
    sed '/^[[:space:]]*$/d' | sort -u
}

pergo_assert_secret_access_policy() {
  local key=$1
  local mode=${2:-exact}
  local secret
  secret=$(pergo_secret_name "$key")

  local roles role
  roles=$(pergo_gcloud secrets get-iam-policy "$secret" \
    --flatten='bindings[]' \
    --format='value(bindings.role)' |
    sed '/^[[:space:]]*$/d' | sort -u)
  while IFS= read -r role; do
    [[ -n "$role" ]] || continue
    if pergo_iam_role_has_permission \
      "$role" secretmanager.versions.access &&
      [[ "$role" != "roles/secretmanager.secretAccessor" ]]; then
      pergo_die \
        "secret ${secret} has access-capable IAM role ${role}; only roles/secretmanager.secretAccessor is allowed"
    fi
  done <<<"$roles"

  local expected actual
  expected=$(pergo_secret_expected_members "$key" |
    sed '/^[[:space:]]*$/d' | sort -u)
  actual=$(pergo_secret_accessor_members "$secret")
  case "$mode" in
    exact)
      pergo_expect_exact "secret accessor policy for ${secret}" \
        "$expected" "$actual"
      ;;
    subset)
      local unexpected
      unexpected=$(comm -23 \
        <(printf '%s\n' "$actual") \
        <(printf '%s\n' "$expected"))
      [[ -z "$unexpected" ]] ||
        pergo_die "secret ${secret} already has unexpected accessor bindings"
      ;;
    *)
      pergo_die "unknown secret IAM validation mode: ${mode}"
      ;;
  esac
}

pergo_project_roles_for_member() {
  local member=$1
  pergo_gcloud projects get-iam-policy "$PERGO_GCP_PROJECT" \
    --flatten='bindings[].members' \
    --filter="bindings.members=${member}" \
    --format='value(bindings.role)' |
    sed '/^[[:space:]]*$/d' | sort -u
}

pergo_assert_no_user_managed_sa_keys() {
  local label=$1
  local account=$2
  local user_keys
  user_keys=$(pergo_gcloud iam service-accounts keys list \
    --iam-account="$account" --managed-by=user --format='value(name)')
  [[ -z "$user_keys" ]] ||
    pergo_die "${label} service account has forbidden user-managed keys"
}

pergo_existing_identity_preflight() {
  local profile account listed project_roles
  for profile in "${PERGO_PROFILES[@]}"; do
    account=$(pergo_profile_sa "$profile")
    listed=$(pergo_gcloud iam service-accounts list \
      --filter="email=${account}" --format='value(email)')
    if [[ -z "$listed" ]]; then
      continue
    fi
    pergo_expect_exact "${profile} service account" "$account" "$listed"
    pergo_assert_no_user_managed_sa_keys "$profile" "$account"
    project_roles=$(pergo_project_roles_for_member "serviceAccount:${account}")
    case "$project_roles" in
      ""|roles/cloudsql.client) ;;
      *)
        pergo_die \
          "${profile} service account has unexpected project-level IAM roles: ${project_roles}"
        ;;
    esac
  done

  listed=$(pergo_gcloud iam service-accounts list \
    --filter="email=${PERGO_PYMES_CALLER_SA}" --format='value(email)')
  pergo_expect_exact "Pymes OIDC caller service account" \
    "$PERGO_PYMES_CALLER_SA" "$listed"
  pergo_assert_no_user_managed_sa_keys \
    "Pymes OIDC caller" "$PERGO_PYMES_CALLER_SA"
}

pergo_expect_exact() {
  local label=$1
  local expected=$2
  local actual=$3
  [[ "$actual" == "$expected" ]] ||
    pergo_die "${label} drift: expected '${expected}', got '${actual:-<empty>}'"
}

pergo_expect_contains() {
  local label=$1
  local haystack=$2
  local needle=$3
  [[ "$haystack" == *"$needle"* ]] ||
    pergo_die "${label} is missing required value '${needle}'"
}

pergo_assert_api_enabled() {
  local api=$1
  local enabled
  enabled=$(pergo_gcloud services list --enabled \
    --filter="config.name=${api}" --format='value(config.name)')
  pergo_expect_exact "API ${api}" "$api" "$enabled"
}

pergo_secret_expected_members() {
  local key=$1
  case "$key" in
    db_api|nats_api|session|admin)
      printf 'serviceAccount:%s\n' "$(pergo_profile_sa api)"
      ;;
    db_webhook|nats_webhook)
      printf 'serviceAccount:%s\n' "$(pergo_profile_sa webhook)"
      ;;
    db_worker|nats_worker)
      printf 'serviceAccount:%s\n' "$(pergo_profile_sa worker)"
      ;;
    db_migrate|nats_migrate)
      printf 'serviceAccount:%s\n' "$(pergo_profile_sa migrate)"
      ;;
    kek|nats_ca)
      local profile
      for profile in "${PERGO_PROFILES[@]}"; do
        printf 'serviceAccount:%s\n' "$(pergo_profile_sa "$profile")"
      done
      ;;
    *) pergo_die "unknown secret membership key: ${key}" ;;
  esac
}

pergo_validate_db_secret_file() {
  local key=$1
  local file=$2
  local profile=${key#db_}
  local expected_role
  expected_role=$(pergo_profile_db_role "$profile")
  local line_count
  line_count=$(awk 'END {print NR}' "$file")
  pergo_expect_exact "$(pergo_secret_name "$key") line count" "1" "$line_count"
  local value
  IFS= read -r value <"$file" ||
    pergo_die "$(pergo_secret_name "$key") is empty"
  [[ "$value" != *$'\n'* &&
     "$value" != *$'\r'* &&
     "$value" != *[[:space:]]* ]] ||
    pergo_die "$(pergo_secret_name "$key") contains whitespace or multiple lines"
  [[ "$value" =~ ^postgres(ql)?:// ]] ||
    pergo_die "$(pergo_secret_name "$key") must contain a PostgreSQL URL"
  [[ "$value" != *"#"* ]] ||
    pergo_die "$(pergo_secret_name "$key") must not contain a URL fragment"

  local remainder=${value#*://}
  local userinfo=${remainder%%@*}
  [[ "$userinfo" != "$remainder" ]] ||
    pergo_die "$(pergo_secret_name "$key") has no authenticated database user"
  [[ "$userinfo" == *":"* ]] ||
    pergo_die "$(pergo_secret_name "$key") has no database password"
  local username=${userinfo%%:*}
  pergo_expect_exact "$(pergo_secret_name "$key") user" "$expected_role" "$username"
  local password=${userinfo#*:}
  [[ -n "$password" ]] ||
    pergo_die "$(pergo_secret_name "$key") has an empty database password"

  local location=${remainder#*@}
  [[ "$location" != *"@"* ]] ||
    pergo_die "$(pergo_secret_name "$key") must URL-encode reserved password characters"
  local path_and_query=$location
  local database_path=${path_and_query%%\?*}
  pergo_expect_exact "$(pergo_secret_name "$key") socket authority and database" \
    "/${PERGO_DATABASE}" "$database_path"
  local query=
  if [[ "$path_and_query" == *"?"* ]]; then
    query=${path_and_query#*\?}
  fi
  [[ -n "$query" ]] ||
    pergo_die "$(pergo_secret_name "$key") must configure the Cloud SQL Unix socket"

  local raw_socket="host=/cloudsql/${PERGO_CLOUDSQL_CONNECTION}"
  local encoded_socket="host=%2Fcloudsql%2F${PERGO_CLOUDSQL_CONNECTION//:/%3A}"
  local parameter
  local host_parameters=0
  local -a parameters
  IFS='&' read -r -a parameters <<<"$query"
  for parameter in "${parameters[@]}"; do
    case "$parameter" in
      "$raw_socket"|"$encoded_socket")
        ((host_parameters += 1))
        ;;
      host=*)
        pergo_die "$(pergo_secret_name "$key") points at an unexpected database socket"
        ;;
    esac
  done
  ((host_parameters == 1)) ||
    pergo_die "$(pergo_secret_name "$key") must use one exact Cloud SQL Unix socket parameter"

  PERGO_VALIDATED_DB_PASSWORD_HASH=$(
    printf '%s' "$password" | sha256sum | awk '{print $1}'
  )
}

pergo_validate_nats_secret_file() {
  local key=$1
  local file=$2
  grep -q -- '-----BEGIN NATS USER JWT-----' "$file" ||
    pergo_die "$(pergo_secret_name "$key") does not contain a NATS user JWT"
  grep -q -- '-----END NATS USER JWT-----' "$file" ||
    pergo_die "$(pergo_secret_name "$key") has an incomplete NATS user JWT"
  grep -q -- '-----BEGIN USER NKEY SEED-----' "$file" ||
    pergo_die "$(pergo_secret_name "$key") does not contain a NATS user seed"
  grep -q -- '-----END USER NKEY SEED-----' "$file" ||
    pergo_die "$(pergo_secret_name "$key") has an incomplete NATS user seed"
}

pergo_validate_kek_secret_file() {
  local file=$1
  local decoded="${PERGO_PREFLIGHT_TMP}/kek.decoded"
  if ! base64 --decode "$file" >"$decoded" 2>/dev/null; then
    pergo_die "$(pergo_secret_name kek) is not valid base64"
  fi
  local size
  size=$(wc -c <"$decoded" | tr -d '[:space:]')
  pergo_expect_exact "$(pergo_secret_name kek) decoded length" "32" "$size"
  local unique_bytes
  unique_bytes=$(od -An -tu1 -v "$decoded" | tr ' ' '\n' |
    sed '/^$/d' | sort -u | wc -l | tr -d '[:space:]')
  ((unique_bytes >= 8)) ||
    pergo_die "$(pergo_secret_name kek) is structurally trivial"
}

pergo_validate_text_secret_file() {
  local key=$1
  local file=$2
  local minimum=$3
  local line_count
  line_count=$(awk 'END {print NR}' "$file")
  pergo_expect_exact "$(pergo_secret_name "$key") line count" "1" "$line_count"
  local value
  IFS= read -r value <"$file" ||
    pergo_die "$(pergo_secret_name "$key") is empty"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] ||
    pergo_die "$(pergo_secret_name "$key") must be one line"
  ((${#value} >= minimum)) ||
    pergo_die "$(pergo_secret_name "$key") must contain at least ${minimum} characters"
  case "$value" in
    pergo-dev-2026|troque-esta-senha|change-me|changeme)
      pergo_die "$(pergo_secret_name "$key") contains a known development value"
      ;;
  esac
}

pergo_fetch_and_validate_secrets() {
  PERGO_PREFLIGHT_TMP=$(mktemp -d "${TMPDIR:-/tmp}/pergo-gcp-preflight.XXXXXX")
  export PERGO_PREFLIGHT_TMP
  trap 'rm -rf "$PERGO_PREFLIGHT_TMP"' EXIT

  local key secret version metadata state file
  local -A nats_hashes=()
  local -A db_hashes=()
  local -A db_password_hashes=()
  for key in "${PERGO_REQUIRED_SECRET_KEYS[@]}"; do
    secret=$(pergo_secret_name "$key")
    version=$(pergo_secret_version "$key")
    metadata=$(pergo_gcloud secrets describe "$secret" \
      --flatten='replication.userManaged.replicas[]' \
      --format='value(replication.userManaged.replicas.location,replication.userManaged.replicas.customerManagedEncryption.kmsKeyName)')
    pergo_expect_exact "secret ${secret} replication and CMEK" \
      "${PERGO_GCP_REGION}"$'\t'"${PERGO_SECRETS_KMS_KEY}" "$metadata"
    state=$(pergo_gcloud secrets versions describe "$version" \
      --secret="$secret" --format='value(state)')
    pergo_expect_exact "secret ${secret} version ${version}" "ENABLED" "$state"

    file="${PERGO_PREFLIGHT_TMP}/${key}"
    pergo_gcloud secrets versions access "$version" \
      --secret="$secret" --out-file="$file" >/dev/null
    chmod 600 "$file"
    [[ -s "$file" ]] ||
      pergo_die "secret ${secret} version ${version} is empty"

    case "$key" in
      db_*)
        pergo_validate_db_secret_file "$key" "$file"
        db_hashes["$(sha256sum "$file" | awk '{print $1}')"]=$key
        db_password_hashes["$PERGO_VALIDATED_DB_PASSWORD_HASH"]=$key
        ;;
      nats_*)
        if [[ "$key" == "nats_ca" ]]; then
          grep -q -- '-----BEGIN CERTIFICATE-----' "$file" ||
            pergo_die "${secret} does not contain a PEM certificate"
        else
          pergo_validate_nats_secret_file "$key" "$file"
          nats_hashes["$(sha256sum "$file" | awk '{print $1}')"]=$key
        fi
        ;;
      kek) pergo_validate_kek_secret_file "$file" ;;
      session) pergo_validate_text_secret_file "$key" "$file" 32 ;;
      admin) pergo_validate_text_secret_file "$key" "$file" 16 ;;
    esac
  done

  ((${#nats_hashes[@]} == 4)) ||
    pergo_die "api, webhook, worker and migrate must use four distinct NATS .creds files"
  ((${#db_hashes[@]} == 4)) ||
    pergo_die "api, webhook, worker and migrate must use four distinct database credentials"
  ((${#db_password_hashes[@]} == 4)) ||
    pergo_die "api, webhook, worker and migrate must use four distinct database passwords"

  rm -rf "$PERGO_PREFLIGHT_TMP"
  unset PERGO_PREFLIGHT_TMP
  trap - EXIT
}

pergo_sql_role_preflight() {
  pergo_require_value PERGO_DB_PGSERVICE_FILE
  pergo_require_value PERGO_DB_PGSERVICE
  [[ "$PERGO_DB_PGSERVICE_FILE" == /* &&
     -f "$PERGO_DB_PGSERVICE_FILE" ]] ||
    pergo_die "PERGO_DB_PGSERVICE_FILE must be an absolute path to a local pg_service file"
  [[ "$PERGO_DB_PGSERVICE" =~ ^[A-Za-z0-9_-]+$ ]] ||
    pergo_die "PERGO_DB_PGSERVICE must be a simple service name"
  local mode
  mode=$(stat -c '%a' "$PERGO_DB_PGSERVICE_FILE")
  case "$mode" in
    400|600) ;;
    *) pergo_die "PERGO_DB_PGSERVICE_FILE must have mode 0400 or 0600" ;;
  esac
  pergo_require_command "$PERGO_PSQL_BIN"

  PGSERVICEFILE="$PERGO_DB_PGSERVICE_FILE" \
    "$PERGO_PSQL_BIN" \
      "service=${PERGO_DB_PGSERVICE} dbname=${PERGO_DATABASE}" \
      --no-psqlrc \
      --set=ON_ERROR_STOP=1 \
      --set="expected_database=${PERGO_DATABASE}" \
      --set="runtime_role=${PERGO_RUNTIME_DB_ROLE}" \
      --set="api_role=$(pergo_profile_db_role api)" \
      --set="webhook_role=$(pergo_profile_db_role webhook)" \
      --set="worker_role=$(pergo_profile_db_role worker)" \
      --set="migrate_role=$(pergo_profile_db_role migrate)" \
      --file="${PERGO_DEPLOY_DIR}/sql/pergo-role-preflight.sql" \
      >/dev/null
}

pergo_external_preflight() {
  pergo_require_command "$PERGO_GCLOUD_BIN"
  pergo_require_command base64
  pergo_require_command sha256sum
  pergo_require_command od

  local api
  for api in \
    artifactregistry.googleapis.com \
    compute.googleapis.com \
    iam.googleapis.com \
    run.googleapis.com \
    secretmanager.googleapis.com \
    sqladmin.googleapis.com; do
    pergo_assert_api_enabled "$api"
  done

  local repository_format
  repository_format=$(pergo_gcloud artifacts repositories describe \
    "$PERGO_ARTIFACT_REPOSITORY" --location="$PERGO_GCP_REGION" \
    --format='value(format)')
  pergo_expect_exact "Artifact Registry repository format" "DOCKER" "$repository_format"
  pergo_gcloud artifacts docker images describe "$PERGO_IMAGE" \
    --format='value(image_summary.digest)' >/dev/null

  local sql_metadata
  sql_metadata=$(pergo_gcloud sql instances describe \
    "$PERGO_CLOUDSQL_INSTANCE_ID" \
    --format='value(databaseVersion,region,connectionName)')
  pergo_expect_exact "Cloud SQL instance" \
    $'POSTGRES_16\tus-central1\tpymes-dev-352318:us-central1:pymes-dev-db' \
    "$sql_metadata"
  local database_name
  database_name=$(pergo_gcloud sql databases describe "$PERGO_DATABASE" \
    --instance="$PERGO_CLOUDSQL_INSTANCE_ID" --format='value(name)')
  pergo_expect_exact "Cloud SQL logical database" "$PERGO_DATABASE" "$database_name"

  local network subnet
  network=$(pergo_gcloud compute networks describe "$PERGO_VPC_NETWORK" \
    --format='value(name)')
  pergo_expect_exact "Direct VPC network" "$PERGO_VPC_NETWORK" "$network"
  subnet=$(pergo_gcloud compute networks subnets describe "$PERGO_VPC_SUBNET" \
    --region="$PERGO_GCP_REGION" --format='value(name,ipCidrRange,privateIpGoogleAccess)')
  pergo_expect_exact "Direct VPC subnet" \
    $'pymes-v3-serverless\t10.120.0.0/24\tTrue' "$subnet"

  local kms_key
  kms_key=$(pergo_gcloud kms keys describe secrets \
    --keyring="$PERGO_PREFIX" --location="$PERGO_GCP_REGION" \
    --format='value(name)')
  pergo_expect_exact "Secret Manager CMEK" "$PERGO_SECRETS_KMS_KEY" "$kms_key"

  pergo_assert_no_project_permission \
    secretmanager.versions.access \
    "PerGo secret isolation" \
    "user:softponti@gmail.com"
  pergo_existing_identity_preflight

  pergo_fetch_and_validate_secrets
  pergo_sql_role_preflight

  # Inspect every secret policy before apply is allowed to make its first
  # mutation. Checking only the accessor role is insufficient: admin, basic or
  # custom roles can also carry secretmanager.versions.access.
  local key
  for key in "${PERGO_REQUIRED_SECRET_KEYS[@]}"; do
    pergo_assert_secret_access_policy "$key" subset
  done
}

pergo_profile_env_argument() {
  local profile=$1
  local value
  value=$(
    printf '%s' \
    "^|^PERGO_ENVIRONMENT=${PERGO_APPLICATION_ENVIRONMENT}" \
    "|PERGO_RUNTIME_PROFILE=${profile}" \
    "|PERGO_RUN_MIGRATIONS=false" \
    "|PERGO_NATS_URLS=${PERGO_NATS_URLS}" \
    "|PERGO_NATS_CREDS_FILE=/var/run/secrets/nats/nats.creds" \
    "|PERGO_NATS_ACCOUNT=${PERGO_NATS_ACCOUNT}" \
    "|PERGO_NATS_STREAM_REPLICAS=${PERGO_NATS_STREAM_REPLICAS}" \
    "|PERGO_NATS_ADOPT_DRAINED_LEGACY=false" \
    "|PERGO_SERVER_PORT=8080" \
    "|PERGO_DEBUG_PORT=6060" \
    "|PERGO_EXTERNAL_URL=${PERGO_EXTERNAL_URL}" \
    "|PERGO_META_GRAPH_VERSION=v25.0" \
    "|PERGO_MEDIA_MODE=disabled" \
      "|PERGO_WHATSAPP_MOCK_ENABLED=false"
  )
  if [[ -n "${PERGO_NATS_CA_SECRET_VERSION:-}" ]]; then
    value+="|PERGO_NATS_CA_FILE=/var/run/secrets/nats/ca.pem"
  fi
  if [[ -n "${PERGO_NATS_TLS_SERVER_NAME:-}" ]]; then
    value+="|PERGO_NATS_TLS_SERVER_NAME=${PERGO_NATS_TLS_SERVER_NAME}"
  fi
  printf '%s' "$value"
}

pergo_profile_secret_argument() {
  local profile=$1
  local db_key="db_${profile}"
  local nats_key="nats_${profile}"
  local value=
  value="PERGO_DATABASE_URL=$(pergo_secret_name "$db_key"):$(pergo_secret_version "$db_key")"
  value+=",PERGO_KEK_BASE64=$(pergo_secret_name kek):$(pergo_secret_version kek)"
  value+=",/var/run/secrets/nats/nats.creds=$(pergo_secret_name "$nats_key"):$(pergo_secret_version "$nats_key")"
  if [[ -n "${PERGO_NATS_CA_SECRET_VERSION:-}" ]]; then
    value+=",/var/run/secrets/nats/ca.pem=$(pergo_secret_name nats_ca):$(pergo_secret_version nats_ca)"
  fi
  if [[ "$profile" == "api" ]]; then
    value+=",PERGO_SESSION_SECRET=$(pergo_secret_name session):$(pergo_secret_version session)"
    value+=",PERGO_ADMIN_PASSWORD=$(pergo_secret_name admin):$(pergo_secret_version admin)"
  fi
  printf '%s' "$value"
}

pergo_common_cloud_run_args() {
  local profile=$1
  local -n target=$2
  target=(
    --region="$PERGO_GCP_REGION"
    --image="$PERGO_IMAGE"
    --service-account="$(pergo_profile_sa "$profile")"
    --args="$profile"
    --set-cloudsql-instances="$PERGO_CLOUDSQL_CONNECTION"
    --network="$PERGO_VPC_NETWORK"
    --subnet="$PERGO_VPC_SUBNET"
    --vpc-egress=all-traffic
    --cpu=1
    --memory=512Mi
    --set-env-vars="$(pergo_profile_env_argument "$profile")"
    --set-secrets="$(pergo_profile_secret_argument "$profile")"
    --labels="app=pergo,product=pymes-v3,env=${PERGO_GCP_ENV},profile=${profile},release=${PERGO_RELEASE_SHA}"
  )
}

pergo_render_service_policy() {
  local service=$1
  local member=$2
  local destination=$3
  {
    printf 'bindings:\n'
    printf -- '- members:\n'
    printf '  - %s\n' "$member"
    printf '  role: roles/run.invoker\n'
    printf 'version: 1\n'
  } >"$destination"
  chmod 600 "$destination"
  pergo_note "IAM policy prepared for ${service}: roles/run.invoker -> ${member}"
}

pergo_expected_confirmation() {
  printf 'apply:%s:%s:%s:%s' \
    "$PERGO_GCP_PROJECT" "$PERGO_GCP_ENV" "$PERGO_RELEASE_SHA" \
    "${PERGO_IMAGE##*@}"
}
