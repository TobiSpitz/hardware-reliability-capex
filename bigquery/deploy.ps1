param(
    [switch]$Seed,
    [switch]$V2,
    [switch]$NoJob,
    [switch]$NoSecrets,
    [string]$OwnerEmail  # Your email for lockout recovery + access request notifications. E.g. -OwnerEmail "you@basepowercompany.com"
)

$PROJECT = if ($env:PROJECT) { $env:PROJECT } else { "mfg-eng-19197" }
$REGION = if ($env:REGION) { $env:REGION } else { "us-central1" }
$BUCKET = if ($env:GCS_BUCKET) { $env:GCS_BUCKET } else { "capex-pipeline-data" }
$ARTIFACT_REPO = if ($env:ARTIFACT_REPO) { $env:ARTIFACT_REPO } else { "capex" }
$IMAGE_NAME = if ($env:IMAGE_NAME) { $env:IMAGE_NAME } else { "capex-app" }
$IMAGE_TAG_VALUE = if ($env:IMAGE_TAG_VALUE) { $env:IMAGE_TAG_VALUE } else { "latest" }
$IMAGE_TAG = "us-central1-docker.pkg.dev/$PROJECT/$ARTIFACT_REPO/$($IMAGE_NAME):$IMAGE_TAG_VALUE"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path

$BQ_ANALYTICS_PROJECT = if ($env:BQ_ANALYTICS_PROJECT) { $env:BQ_ANALYTICS_PROJECT } else { $PROJECT }
$BQ_ANALYTICS_DATASET = if ($env:BQ_ANALYTICS_DATASET) { $env:BQ_ANALYTICS_DATASET } else { "capex_analytics" }
$BQ_QUERY_PROJECT = if ($env:BQ_QUERY_PROJECT) { $env:BQ_QUERY_PROJECT } else { $PROJECT }
$ODOO_SOURCE_PROJECT = if ($env:ODOO_SOURCE_PROJECT) { $env:ODOO_SOURCE_PROJECT } else { "gtm-analytics-447201" }
$ODOO_SOURCE_DATASET = if ($env:ODOO_SOURCE_DATASET) { $env:ODOO_SOURCE_DATASET } else { "odoo_public" }
$SERVICE_ACCOUNT = if ($env:SERVICE_ACCOUNT) { $env:SERVICE_ACCOUNT } else { "capex-dashboard-sa@$PROJECT.iam.gserviceaccount.com" }
$REFRESH_JOB_NAME = if ($env:REFRESH_JOB_NAME) { $env:REFRESH_JOB_NAME } else { "capex-refresh-job" }
$REFRESH_EXECUTION_MODE = if ($env:REFRESH_EXECUTION_MODE) { $env:REFRESH_EXECUTION_MODE } else { "" }

$USE_SECRET_MANAGER = -not $NoSecrets
$DEPLOY_JOB = -not $NoJob

$GOOGLE_CLIENT_ID_SECRET = if ($env:GOOGLE_CLIENT_ID_SECRET) { $env:GOOGLE_CLIENT_ID_SECRET } else { "capex-google-client-id" }
$GOOGLE_CLIENT_SECRET_SECRET = if ($env:GOOGLE_CLIENT_SECRET_SECRET) { $env:GOOGLE_CLIENT_SECRET_SECRET } else { "capex-google-client-secret" }
$FLASK_SECRET_KEY_SECRET = if ($env:FLASK_SECRET_KEY_SECRET) { $env:FLASK_SECRET_KEY_SECRET } else { "capex-flask-secret-key" }

if (-not $REFRESH_EXECUTION_MODE) {
    if ($DEPLOY_JOB) {
        $REFRESH_EXECUTION_MODE = "job"
    } else {
        $REFRESH_EXECUTION_MODE = "subprocess"
    }
}

$ENV_COMMON = "GCS_BUCKET=$BUCKET,BQ_ANALYTICS_PROJECT=$BQ_ANALYTICS_PROJECT,BQ_ANALYTICS_DATASET=$BQ_ANALYTICS_DATASET,BQ_QUERY_PROJECT=$BQ_QUERY_PROJECT,ODOO_SOURCE_PROJECT=$ODOO_SOURCE_PROJECT,ODOO_SOURCE_DATASET=$ODOO_SOURCE_DATASET,USE_SIGNED_IN_USER_GCP=false,REFRESH_USE_LOGGED_IN_OAUTH=true,PREFER_BIGQUERY_MAPPED_CSV=true,ALLOW_MAPPED_CSV_FALLBACK=false,WRITE_MAPPED_CSV_TO_BIGQUERY=true,WRITE_MAPPED_CSV_TO_BIGQUERY_STRICT=true,REFRESH_TIMEOUT_SEC=1800,REFRESH_EXECUTION_MODE=$REFRESH_EXECUTION_MODE,REFRESH_JOB_NAME=$REFRESH_JOB_NAME,REFRESH_JOB_REGION=$REGION,REFRESH_JOB_PROJECT=$PROJECT"
$SETTINGS_OWNER = if ($OwnerEmail) { $OwnerEmail } else { $env:SETTINGS_OWNER_EMAIL }
if ($SETTINGS_OWNER) {
    $ENV_COMMON += ",SETTINGS_OWNER_EMAIL=$SETTINGS_OWNER"
}
$SECRET_SPEC = "GOOGLE_CLIENT_ID=$($GOOGLE_CLIENT_ID_SECRET):latest,GOOGLE_CLIENT_SECRET=$($GOOGLE_CLIENT_SECRET_SECRET):latest,FLASK_SECRET_KEY=$($FLASK_SECRET_KEY_SECRET):latest"

if (-not $USE_SECRET_MANAGER) {
    $CLIENT_ID = $env:GOOGLE_CLIENT_ID
    $CLIENT_SECRET = $env:GOOGLE_CLIENT_SECRET
    $FLASK_SECRET = $env:FLASK_SECRET_KEY
    $missingVars = @()
    if (-not $CLIENT_ID) { $missingVars += "GOOGLE_CLIENT_ID" }
    if (-not $CLIENT_SECRET) { $missingVars += "GOOGLE_CLIENT_SECRET" }
    if (-not $FLASK_SECRET) { $missingVars += "FLASK_SECRET_KEY" }
    if ($missingVars.Count -gt 0) {
        throw "Missing required variables when -NoSecrets is used: $($missingVars -join ', ')"
    }
    $AUTH_ENV_COMMON = "GOOGLE_CLIENT_ID=$CLIENT_ID,GOOGLE_CLIENT_SECRET=$CLIENT_SECRET,FLASK_SECRET_KEY=$FLASK_SECRET"
}

Write-Host "==> Enabling required APIs..."
gcloud services enable `
    run.googleapis.com `
    storage.googleapis.com `
    iap.googleapis.com `
    artifactregistry.googleapis.com `
    cloudbuild.googleapis.com `
    bigquery.googleapis.com `
    cloudscheduler.googleapis.com `
    --project=$PROJECT
if ($LASTEXITCODE -ne 0) { throw "Failed to enable required APIs" }

Write-Host "==> Ensuring Artifact Registry repo exists..."
$ErrorActionPreference = "SilentlyContinue"
gcloud artifacts repositories describe $ARTIFACT_REPO `
    --project=$PROJECT --location=$REGION --format="value(name)" 2>$null
$ErrorActionPreference = "Stop"
if ($LASTEXITCODE -ne 0) {
    gcloud artifacts repositories create $ARTIFACT_REPO `
        --project=$PROJECT --location=$REGION `
        --repository-format=docker `
        --description="CAPEX dashboard images"
    if ($LASTEXITCODE -ne 0) { throw "Failed to create Artifact Registry repo" }
}

Write-Host "==> Ensuring runtime service account exists..."
$saId = ($SERVICE_ACCOUNT -split "@")[0]
$ErrorActionPreference = "SilentlyContinue"
gcloud iam service-accounts describe $SERVICE_ACCOUNT --project=$PROJECT 2>$null
$ErrorActionPreference = "Stop"
if ($LASTEXITCODE -ne 0) {
    gcloud iam service-accounts create $saId `
        --project=$PROJECT `
        --display-name="CAPEX Dashboard Runtime SA"
    if ($LASTEXITCODE -ne 0) { throw "Failed to create service account $SERVICE_ACCOUNT" }
}

Write-Host "==> Building container image..."
gcloud builds submit $SCRIPT_DIR `
    --project=$PROJECT `
    --tag=$IMAGE_TAG `
    --timeout=900
if ($LASTEXITCODE -ne 0) { throw "Cloud Build failed" }

function Deploy-Service([string]$serviceName, [string]$appMode) {
    Write-Host "==> Deploying $serviceName..."
    if ($USE_SECRET_MANAGER) {
        gcloud run deploy $serviceName `
            --project=$PROJECT `
            --region=$REGION `
            --image=$IMAGE_TAG `
            --platform=managed `
            --service-account=$SERVICE_ACCOUNT `
            --set-env-vars="APP_MODE=$appMode,$ENV_COMMON" `
            --set-secrets=$SECRET_SPEC `
            --memory=1Gi `
            --cpu=1 `
            --min-instances=0 `
            --max-instances=3 `
            --timeout=1800 `
            --no-allow-unauthenticated
    } else {
        gcloud run deploy $serviceName `
            --project=$PROJECT `
            --region=$REGION `
            --image=$IMAGE_TAG `
            --platform=managed `
            --service-account=$SERVICE_ACCOUNT `
            --set-env-vars="APP_MODE=$appMode,$ENV_COMMON,$AUTH_ENV_COMMON" `
            --memory=1Gi `
            --cpu=1 `
            --min-instances=0 `
            --max-instances=3 `
            --timeout=1800 `
            --no-allow-unauthenticated
    }
    if ($LASTEXITCODE -ne 0) { throw "Deploy failed for $serviceName" }
}

Deploy-Service "capex-dashboard" "dashboard"
Deploy-Service "capex-review" "review"
if ($V2) {
    Deploy-Service "capex-dashboard-v2" "dashboard_v2"
}

if ($DEPLOY_JOB) {
    Write-Host "==> Deploying $REFRESH_JOB_NAME..."
    if ($USE_SECRET_MANAGER) {
        gcloud run jobs deploy $REFRESH_JOB_NAME `
            --project=$PROJECT `
            --region=$REGION `
            --image=$IMAGE_TAG `
            --service-account=$SERVICE_ACCOUNT `
            --set-env-vars=$ENV_COMMON `
            --set-secrets=$SECRET_SPEC `
            --command=python `
            --args=refresh_job_runner.py `
            --tasks=1 `
            --max-retries=1 `
            --task-timeout=1800s `
            --memory=1Gi `
            --cpu=1
    } else {
        gcloud run jobs deploy $REFRESH_JOB_NAME `
            --project=$PROJECT `
            --region=$REGION `
            --image=$IMAGE_TAG `
            --service-account=$SERVICE_ACCOUNT `
            --set-env-vars="$ENV_COMMON,$AUTH_ENV_COMMON" `
            --command=python `
            --args=refresh_job_runner.py `
            --tasks=1 `
            --max-retries=1 `
            --task-timeout=1800s `
            --memory=1Gi `
            --cpu=1
    }
    if ($LASTEXITCODE -ne 0) { throw "Failed to deploy $REFRESH_JOB_NAME" }
}

if ($Seed) {
    Write-Host "==> Backing up + uploading local data/ to gs://$BUCKET/ ..."
    python "$SCRIPT_DIR\push_clean_to_cloud.py" --gcs-bucket "$BUCKET" --project "$PROJECT" --major-update
    if ($LASTEXITCODE -ne 0) { throw "Seed upload failed" }
    gcloud storage ls "gs://$BUCKET/" --project=$PROJECT
}

Write-Host "==> Granting runtime IAM roles to $SERVICE_ACCOUNT ..."
$ErrorActionPreference = "SilentlyContinue"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" `
    --member="serviceAccount:$SERVICE_ACCOUNT" `
    --role="roles/storage.objectAdmin" `
    --project=$PROJECT 2>$null
gcloud projects add-iam-policy-binding $BQ_ANALYTICS_PROJECT `
    --member="serviceAccount:$SERVICE_ACCOUNT" `
    --role="roles/bigquery.jobUser" 2>$null
if ($BQ_QUERY_PROJECT -ne $BQ_ANALYTICS_PROJECT) {
    gcloud projects add-iam-policy-binding $BQ_QUERY_PROJECT `
        --member="serviceAccount:$SERVICE_ACCOUNT" `
        --role="roles/bigquery.jobUser" 2>$null
}
gcloud projects add-iam-policy-binding $BQ_ANALYTICS_PROJECT `
    --member="serviceAccount:$SERVICE_ACCOUNT" `
    --role="roles/bigquery.dataEditor" 2>$null
gcloud projects add-iam-policy-binding $ODOO_SOURCE_PROJECT `
    --member="serviceAccount:$SERVICE_ACCOUNT" `
    --role="roles/bigquery.dataViewer" 2>$null
$ErrorActionPreference = "Stop"

$dashUrl = gcloud run services describe capex-dashboard --project=$PROJECT --region=$REGION --format="value(status.url)"
$reviewUrl = gcloud run services describe capex-review --project=$PROJECT --region=$REGION --format="value(status.url)"

Write-Host ""
Write-Host "================================================================"
Write-Host "Deployment complete."
Write-Host "Dashboard: $dashUrl"
Write-Host "Review UI: $reviewUrl"
if ($V2) {
    $v2Url = gcloud run services describe capex-dashboard-v2 --project=$PROJECT --region=$REGION --format="value(status.url)"
    Write-Host "V2 Staging: $v2Url"
}
if ($DEPLOY_JOB) {
    Write-Host "Refresh job: $REFRESH_JOB_NAME"
    Write-Host "Run now: gcloud run jobs execute $REFRESH_JOB_NAME --project=$PROJECT --region=$REGION --wait"
}
Write-Host "Auth posture: Cloud Run private + in-app Google OAuth."
Write-Host "Use setup_iap.ps1 if you also want IAP at the edge."
if ($SETTINGS_OWNER) {
    Write-Host "Lockout recovery: SETTINGS_OWNER_EMAIL=$SETTINGS_OWNER (always allowed)"
}
Write-Host "Access requests: Add SMTP_USER + SMTP_PASSWORD env (or secrets) to Cloud Run for email notifications."
Write-Host "================================================================"
