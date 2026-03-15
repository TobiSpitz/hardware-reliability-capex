#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Deploy CAPEX Dashboard + Review UI (+ optional refresh job) to Cloud Run.
#
# Usage:
#   ./deploy.sh
#   ./deploy.sh --v2 --seed
#   ./deploy.sh --no-job
#   PROJECT=my-project REGION=us-central1 ./deploy.sh
# ---------------------------------------------------------------------------
set -euo pipefail

PROJECT="${PROJECT:-mfg-eng-19197}"
REGION="${REGION:-us-central1}"
BUCKET="${GCS_BUCKET:-capex-pipeline-data}"
ARTIFACT_REPO="${ARTIFACT_REPO:-capex}"
IMAGE_NAME="${IMAGE_NAME:-capex-app}"
IMAGE_TAG_VALUE="${IMAGE_TAG_VALUE:-latest}"
IMAGE_TAG="us-central1-docker.pkg.dev/${PROJECT}/${ARTIFACT_REPO}/${IMAGE_NAME}:${IMAGE_TAG_VALUE}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

BQ_ANALYTICS_PROJECT="${BQ_ANALYTICS_PROJECT:-$PROJECT}"
BQ_ANALYTICS_DATASET="${BQ_ANALYTICS_DATASET:-capex_analytics}"
BQ_QUERY_PROJECT="${BQ_QUERY_PROJECT:-$PROJECT}"
ODOO_SOURCE_PROJECT="${ODOO_SOURCE_PROJECT:-gtm-analytics-447201}"
ODOO_SOURCE_DATASET="${ODOO_SOURCE_DATASET:-odoo_public}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-capex-dashboard-sa@${PROJECT}.iam.gserviceaccount.com}"
REFRESH_JOB_NAME="${REFRESH_JOB_NAME:-capex-refresh-job}"
REFRESH_EXECUTION_MODE="${REFRESH_EXECUTION_MODE:-}"

USE_SECRET_MANAGER="${USE_SECRET_MANAGER:-true}"
GOOGLE_CLIENT_ID_SECRET="${GOOGLE_CLIENT_ID_SECRET:-capex-google-client-id}"
GOOGLE_CLIENT_SECRET_SECRET="${GOOGLE_CLIENT_SECRET_SECRET:-capex-google-client-secret}"
FLASK_SECRET_KEY_SECRET="${FLASK_SECRET_KEY_SECRET:-capex-flask-secret-key}"

SEED=false
V2=false
DEPLOY_JOB=true
for arg in "$@"; do
  case "$arg" in
    --seed) SEED=true ;;
    --v2) V2=true ;;
    --no-job) DEPLOY_JOB=false ;;
    --no-secrets) USE_SECRET_MANAGER=false ;;
  esac
done

if [ -z "$REFRESH_EXECUTION_MODE" ]; then
  if [ "$DEPLOY_JOB" = true ]; then
    REFRESH_EXECUTION_MODE="job"
  else
    REFRESH_EXECUTION_MODE="subprocess"
  fi
fi

ENV_COMMON="GCS_BUCKET=$BUCKET,BQ_ANALYTICS_PROJECT=$BQ_ANALYTICS_PROJECT,BQ_ANALYTICS_DATASET=$BQ_ANALYTICS_DATASET,BQ_QUERY_PROJECT=$BQ_QUERY_PROJECT,ODOO_SOURCE_PROJECT=$ODOO_SOURCE_PROJECT,ODOO_SOURCE_DATASET=$ODOO_SOURCE_DATASET,USE_SIGNED_IN_USER_GCP=false,REFRESH_USE_LOGGED_IN_OAUTH=true,PREFER_BIGQUERY_MAPPED_CSV=true,ALLOW_MAPPED_CSV_FALLBACK=false,WRITE_MAPPED_CSV_TO_BIGQUERY=true,WRITE_MAPPED_CSV_TO_BIGQUERY_STRICT=true,REFRESH_TIMEOUT_SEC=1800,REFRESH_EXECUTION_MODE=$REFRESH_EXECUTION_MODE,REFRESH_JOB_NAME=$REFRESH_JOB_NAME,REFRESH_JOB_REGION=$REGION,REFRESH_JOB_PROJECT=$PROJECT"
SECRET_SPEC="GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID_SECRET}:latest,GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET_SECRET}:latest,FLASK_SECRET_KEY=${FLASK_SECRET_KEY_SECRET}:latest"

if [ "$USE_SECRET_MANAGER" = false ]; then
  : "${GOOGLE_CLIENT_ID:?Missing GOOGLE_CLIENT_ID environment variable}"
  : "${GOOGLE_CLIENT_SECRET:?Missing GOOGLE_CLIENT_SECRET environment variable}"
  : "${FLASK_SECRET_KEY:?Missing FLASK_SECRET_KEY environment variable}"
  AUTH_ENV_COMMON="GOOGLE_CLIENT_ID=$GOOGLE_CLIENT_ID,GOOGLE_CLIENT_SECRET=$GOOGLE_CLIENT_SECRET,FLASK_SECRET_KEY=$FLASK_SECRET_KEY"
fi

echo "==> Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  storage.googleapis.com \
  iap.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  --project="$PROJECT"

echo "==> Ensuring Artifact Registry repo exists..."
gcloud artifacts repositories describe "$ARTIFACT_REPO" \
  --project="$PROJECT" --location="$REGION" --format="value(name)" 2>/dev/null \
|| gcloud artifacts repositories create "$ARTIFACT_REPO" \
  --project="$PROJECT" --location="$REGION" \
  --repository-format=docker \
  --description="CAPEX dashboard images"

echo "==> Ensuring runtime service account exists..."
SA_ID="${SERVICE_ACCOUNT%@*}"
SA_ID="${SA_ID##*/}"
gcloud iam service-accounts describe "$SERVICE_ACCOUNT" --project="$PROJECT" >/dev/null 2>&1 \
|| gcloud iam service-accounts create "$SA_ID" \
  --project="$PROJECT" \
  --display-name="CAPEX Dashboard Runtime SA"

echo "==> Building container image..."
gcloud builds submit "$SCRIPT_DIR" \
  --project="$PROJECT" \
  --tag="$IMAGE_TAG" \
  --timeout=900

deploy_service() {
  local service_name="$1"
  local app_mode="$2"
  echo "==> Deploying ${service_name}..."
  if [ "$USE_SECRET_MANAGER" = true ]; then
    gcloud run deploy "$service_name" \
      --project="$PROJECT" \
      --region="$REGION" \
      --image="$IMAGE_TAG" \
      --platform=managed \
      --service-account="$SERVICE_ACCOUNT" \
      --set-env-vars="APP_MODE=${app_mode},$ENV_COMMON" \
      --set-secrets="$SECRET_SPEC" \
      --memory=1Gi \
      --cpu=1 \
      --min-instances=0 \
      --max-instances=3 \
      --timeout=1800 \
      --no-allow-unauthenticated
  else
    gcloud run deploy "$service_name" \
      --project="$PROJECT" \
      --region="$REGION" \
      --image="$IMAGE_TAG" \
      --platform=managed \
      --service-account="$SERVICE_ACCOUNT" \
      --set-env-vars="APP_MODE=${app_mode},$ENV_COMMON,$AUTH_ENV_COMMON" \
      --memory=1Gi \
      --cpu=1 \
      --min-instances=0 \
      --max-instances=3 \
      --timeout=1800 \
      --no-allow-unauthenticated
  fi
}

deploy_service "capex-dashboard" "dashboard"
deploy_service "capex-review" "review"

if [ "$V2" = true ]; then
  deploy_service "capex-dashboard-v2" "dashboard_v2"
fi

if [ "$DEPLOY_JOB" = true ]; then
  echo "==> Deploying $REFRESH_JOB_NAME..."
  if [ "$USE_SECRET_MANAGER" = true ]; then
    gcloud run jobs deploy "$REFRESH_JOB_NAME" \
      --project="$PROJECT" \
      --region="$REGION" \
      --image="$IMAGE_TAG" \
      --service-account="$SERVICE_ACCOUNT" \
      --set-env-vars="$ENV_COMMON" \
      --set-secrets="$SECRET_SPEC" \
      --command=python \
      --args=refresh_job_runner.py \
      --tasks=1 \
      --max-retries=1 \
      --task-timeout=1800s \
      --memory=1Gi \
      --cpu=1
  else
    gcloud run jobs deploy "$REFRESH_JOB_NAME" \
      --project="$PROJECT" \
      --region="$REGION" \
      --image="$IMAGE_TAG" \
      --service-account="$SERVICE_ACCOUNT" \
      --set-env-vars="$ENV_COMMON,$AUTH_ENV_COMMON" \
      --command=python \
      --args=refresh_job_runner.py \
      --tasks=1 \
      --max-retries=1 \
      --task-timeout=1800s \
      --memory=1Gi \
      --cpu=1
  fi
fi

if [ "$SEED" = true ]; then
  echo "==> Backing up + uploading local data/ to gs://$BUCKET/ ..."
  python3 "$SCRIPT_DIR/push_clean_to_cloud.py" --gcs-bucket "$BUCKET" --project "$PROJECT" --major-update
  gcloud storage ls "gs://$BUCKET/" --project="$PROJECT"
fi

echo "==> Granting runtime IAM roles to service account ($SERVICE_ACCOUNT)..."
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/storage.objectAdmin" \
  --project="$PROJECT" >/dev/null 2>&1 || true

gcloud projects add-iam-policy-binding "$BQ_ANALYTICS_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.jobUser" >/dev/null 2>&1 || true

if [ "$BQ_QUERY_PROJECT" != "$BQ_ANALYTICS_PROJECT" ]; then
  gcloud projects add-iam-policy-binding "$BQ_QUERY_PROJECT" \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.jobUser" >/dev/null 2>&1 || true
fi

gcloud projects add-iam-policy-binding "$BQ_ANALYTICS_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.dataEditor" >/dev/null 2>&1 || true

gcloud projects add-iam-policy-binding "$ODOO_SOURCE_PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/bigquery.dataViewer" >/dev/null 2>&1 || true

echo ""
echo "================================================================"
echo "Deployment complete."
echo ""
echo "Dashboard: $(gcloud run services describe capex-dashboard --project="$PROJECT" --region="$REGION" --format='value(status.url)')"
echo "Review UI: $(gcloud run services describe capex-review --project="$PROJECT" --region="$REGION" --format='value(status.url)')"
if [ "$V2" = true ]; then
  echo "V2 Staging: $(gcloud run services describe capex-dashboard-v2 --project="$PROJECT" --region="$REGION" --format='value(status.url)')"
fi
if [ "$DEPLOY_JOB" = true ]; then
  echo "Refresh job: $REFRESH_JOB_NAME (Cloud Run Job)"
  echo "Run now: gcloud run jobs execute $REFRESH_JOB_NAME --project=$PROJECT --region=$REGION --wait"
fi
echo ""
echo "Auth posture: Cloud Run private + in-app Google OAuth."
echo "If you need IAP, use setup_iap.ps1 after deploy."
echo "================================================================"
