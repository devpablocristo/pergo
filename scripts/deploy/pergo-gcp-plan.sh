#!/usr/bin/env bash
set -euo pipefail
set +x
umask 077

deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=pergo-gcp-lib.sh
source "${deploy_dir}/pergo-gcp-lib.sh"

pergo_load_config plan

cat <<EOF
PERGO GCP PLAN (READ-ONLY)
project:             ${PERGO_GCP_PROJECT}
region:              ${PERGO_GCP_REGION}
environment:         ${PERGO_GCP_ENV} (${PERGO_APPLICATION_ENVIRONMENT})
release source:      ${PERGO_RELEASE_SHA}
image:               ${PERGO_IMAGE}
Cloud SQL:           ${PERGO_CLOUDSQL_CONNECTION}/${PERGO_DATABASE}
Direct VPC:          ${PERGO_VPC_NETWORK}/${PERGO_VPC_SUBNET} (all-traffic)
external NATS:       ${PERGO_NATS_URLS}
NATS account label:  ${PERGO_NATS_ACCOUNT}
NATS replicas:       ${PERGO_NATS_STREAM_REPLICAS}
public origin:       ${PERGO_EXTERNAL_URL}

Planned workloads:
  private API service:  $(pergo_profile_resource api)
    identity: $(pergo_profile_sa api)
    invoker:  serviceAccount:${PERGO_PYMES_CALLER_SA}
  public callback service: $(pergo_profile_resource webhook)
    identity: $(pergo_profile_sa webhook)
    invoker:  allUsers
    runtime exposure: /webhooks/*, /healthz and /readyz only
  worker pool: $(pergo_profile_resource worker)
    identity: $(pergo_profile_sa worker)
    instances: ${PERGO_WORKER_INSTANCES}
  migration job: $(pergo_profile_resource migrate)
    identity: $(pergo_profile_sa migrate)
    retries: 0; executed and awaited before runtime rollout

Required preflight:
  - enabled GCP APIs, Docker repository and exact image digest
  - shared PG16 instance, logical database and least-privilege SQL roles
  - existing Direct VPC subnet 10.120.0.0/24 with Private Google Access
  - global Secret Manager containers replicated only in us-central1 with CMEK
  - enabled numeric secret versions and safe payload shapes
  - four different DB credentials and four different NATS .creds files
  - no project-wide roles/secretmanager.secretAccessor binding

Required secrets (names and pinned versions; values are never printed):
EOF

for key in "${PERGO_REQUIRED_SECRET_KEYS[@]}"; do
  printf '  - %-45s version %s\n' \
    "$(pergo_secret_name "$key")" "$(pergo_secret_version "$key")"
done

cat <<EOF

Mutation order used by apply:
  1. require the exact release confirmation string;
  2. run every external, secret and SQL-role preflight;
  3. create four profile-specific service accounts and least-privilege IAM;
  4. deploy the migrate job and wait for a successful execution;
  5. deploy worker pool, webhook service, then private API service;
  6. replace service invoker policies with their exact intended principals;
  7. run the complete remote audit and HTTP isolation probes.

Apply confirmation (not executed by this plan):
  $(pergo_expected_confirmation)
EOF
