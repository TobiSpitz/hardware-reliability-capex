# ---------------------------------------------------------------------------
# Enable IAP on Cloud Run services and restrict to @basepowercompany.com
#
# Run AFTER deploy.ps1 has completed successfully.
#
# Prerequisites:
#   - OAuth consent screen must be configured for the mfg-eng project
#     (GCP Console > APIs & Services > OAuth consent screen)
#     Set to "Internal" so only basepowercompany.com users can authenticate.
#
# Usage:
#   .\setup_iap.ps1
# ---------------------------------------------------------------------------
$ErrorActionPreference = "Stop"

$PROJECT = "mfg-eng-19197"
$REGION  = "us-central1"
$DOMAIN  = "basepowercompany.com"

# ---- 1. Ensure IAP API is enabled ----------------------------------------
Write-Host "==> Enabling IAP API..."
gcloud services enable iap.googleapis.com --project=$PROJECT

# ---- 2. Get the Cloud Run service URLs -----------------------------------
$dashUrl = gcloud run services describe capex-dashboard `
    --project=$PROJECT --region=$REGION --format="value(status.url)"
$reviewUrl = gcloud run services describe capex-review `
    --project=$PROJECT --region=$REGION --format="value(status.url)"

Write-Host "  Dashboard URL: $dashUrl"
Write-Host "  Review URL:    $reviewUrl"

# ---- 3. Grant IAP-secured Web App User role to the domain ----------------
# This allows all @basepowercompany.com users to access IAP-protected resources
Write-Host "==> Granting IAP access to domain: $DOMAIN ..."

# For Cloud Run, IAP is configured via the backend service.
# The simplest approach: use Cloud Run's built-in IAM to grant the invoker role
# to the entire domain, then enable IAP in the console.

foreach ($svc in @("capex-dashboard", "capex-review")) {
    Write-Host "  Granting Cloud Run Invoker to domain:$DOMAIN on $svc ..."
    gcloud run services add-iam-policy-binding $svc `
        --project=$PROJECT `
        --region=$REGION `
        --member="domain:$DOMAIN" `
        --role="roles/run.invoker"
}

Write-Host ""
Write-Host "================================================================"
Write-Host "  IAP Setup Instructions"
Write-Host ""
Write-Host "  The deploy scripts set --no-allow-unauthenticated, so only"
Write-Host "  authenticated users can reach the services. The domain"
Write-Host "  $DOMAIN has been granted the Cloud Run Invoker role."
Write-Host ""
Write-Host "  To enable full IAP (with the Google SSO login page):"
Write-Host "  1. Go to: https://console.cloud.google.com/security/iap?project=$PROJECT"
Write-Host "  2. Configure the OAuth consent screen (set to 'Internal')"
Write-Host "  3. Toggle IAP ON for both Cloud Run backend services"
Write-Host "  4. Add 'domain:$DOMAIN' as an IAP-secured Web App User"
Write-Host ""
Write-Host "  After that, employees visit the Cloud Run URLs and get"
Write-Host "  a Google SSO prompt automatically."
Write-Host ""
Write-Host "  Dashboard: $dashUrl"
Write-Host "  Review UI: $reviewUrl"
Write-Host "================================================================"
