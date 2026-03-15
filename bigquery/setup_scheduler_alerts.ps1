# ---------------------------------------------------------------------------
# Configure Cloud Scheduler + Gmail alerting for CAPEX refresh operations.
#
# What this script does:
#   1) Creates/updates a daily Cloud Scheduler trigger for the Cloud Run Job
#   2) Creates log-based metrics for refresh success/failure events
#   3) Creates email notification channels
#   4) Creates/updates alert policies:
#      - refresh failure detected
#      - no refresh success event for > 26 hours (staleness)
#
# Prerequisites:
#   - deploy.ps1 has already deployed capex-refresh-job
#   - your gcloud identity has IAM rights for Scheduler, Monitoring, Logging
# ---------------------------------------------------------------------------
param(
    [string]$Project = "mfg-eng-19197",
    [string]$Region = "us-central1",
    [string]$JobName = "capex-refresh-job",
    [string]$SchedulerJobName = "capex-refresh-daily",
    [string]$SchedulerServiceAccount = "",
    [string]$Schedule = "",
    [string]$TimeZone = "",
    [string[]]$AlertEmails = @(),
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$settingsPath = Join-Path $scriptDir "data\dashboard_settings.json"

function Read-SettingsValue([string]$key, [object]$defaultValue) {
    if (-not (Test-Path $settingsPath)) { return $defaultValue }
    try {
        $raw = Get-Content -Raw $settingsPath | ConvertFrom-Json
        $value = $raw.$key
        if ($null -eq $value -or ($value -is [string] -and [string]::IsNullOrWhiteSpace($value))) {
            return $defaultValue
        }
        return $value
    } catch {
        return $defaultValue
    }
}

if (-not $Schedule) { $Schedule = [string](Read-SettingsValue "ops_refresh_cron" "0 8 * * *") }
if (-not $TimeZone) { $TimeZone = [string](Read-SettingsValue "ops_refresh_timezone" "Etc/UTC") }
if (-not $AlertEmails -or $AlertEmails.Count -eq 0) {
    $loaded = Read-SettingsValue "ops_alert_emails" @()
    if ($loaded -is [System.Array]) {
        $AlertEmails = @($loaded | ForEach-Object { "$_".Trim().ToLowerInvariant() } | Where-Object { $_ })
    }
}
if (-not $SchedulerServiceAccount) { $SchedulerServiceAccount = "capex-scheduler-sa@$Project.iam.gserviceaccount.com" }

Write-Host "==> Project: $Project"
Write-Host "==> Region: $Region"
Write-Host "==> Job: $JobName"
Write-Host "==> Schedule: $Schedule ($TimeZone)"
Write-Host "==> Scheduler SA: $SchedulerServiceAccount"
Write-Host "==> Alert emails: $($AlertEmails -join ', ')"

if ($DryRun) {
    Write-Host "Dry run only. No changes applied."
    exit 0
}

Write-Host "==> Enabling required APIs..."
gcloud services enable `
    cloudscheduler.googleapis.com `
    monitoring.googleapis.com `
    logging.googleapis.com `
    run.googleapis.com `
    --project=$Project

Write-Host "==> Ensuring scheduler service account exists..."
$saId = ($SchedulerServiceAccount -split "@")[0]
$ErrorActionPreference = "SilentlyContinue"
gcloud iam service-accounts describe $SchedulerServiceAccount --project=$Project 2>$null
$ErrorActionPreference = "Stop"
if ($LASTEXITCODE -ne 0) {
    gcloud iam service-accounts create $saId `
        --project=$Project `
        --display-name="CAPEX Refresh Scheduler SA"
}

Write-Host "==> Granting Scheduler SA Cloud Run invoke permission..."
gcloud projects add-iam-policy-binding $Project `
    --member="serviceAccount:$SchedulerServiceAccount" `
    --role="roles/run.invoker" | Out-Null

$uri = "https://$Region-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$Project/jobs/$($JobName):run"

Write-Host "==> Creating/updating scheduler job ($SchedulerJobName)..."
$ErrorActionPreference = "SilentlyContinue"
gcloud scheduler jobs describe $SchedulerJobName --project=$Project --location=$Region 2>$null
$exists = ($LASTEXITCODE -eq 0)
$ErrorActionPreference = "Stop"

if ($exists) {
    gcloud scheduler jobs update http $SchedulerJobName `
        --project=$Project `
        --location=$Region `
        --schedule="$Schedule" `
        --time-zone="$TimeZone" `
        --uri="$uri" `
        --http-method=POST `
        --oauth-service-account-email=$SchedulerServiceAccount `
        --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
} else {
    gcloud scheduler jobs create http $SchedulerJobName `
        --project=$Project `
        --location=$Region `
        --schedule="$Schedule" `
        --time-zone="$TimeZone" `
        --uri="$uri" `
        --http-method=POST `
        --oauth-service-account-email=$SchedulerServiceAccount `
        --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
}

function Ensure-LogMetric([string]$name, [string]$description, [string]$filter) {
    $ErrorActionPreference = "SilentlyContinue"
    gcloud logging metrics describe $name --project=$Project 2>$null | Out-Null
    $exists = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = "Stop"
    if ($exists) {
        Write-Host "  Metric exists: $name"
        return
    }
    Write-Host "  Creating metric: $name"
    gcloud logging metrics create $name `
        --project=$Project `
        --description="$description" `
        --log-filter="$filter" | Out-Null
}

Write-Host "==> Ensuring log-based metrics..."
Ensure-LogMetric `
    -name "capex_refresh_failures" `
    -description "Count refresh job failures emitted by refresh_job_runner.py" `
    -filter "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"$JobName\" AND jsonPayload.event=\"refresh_job_failed\""

Ensure-LogMetric `
    -name "capex_refresh_successes" `
    -description "Count refresh job successes emitted by refresh_job_runner.py" `
    -filter "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"$JobName\" AND jsonPayload.event=\"refresh_job_succeeded\""

$channelNames = @()
foreach ($email in $AlertEmails) {
    if ([string]::IsNullOrWhiteSpace($email)) { continue }
    Write-Host "==> Ensuring notification channel for $email ..."
    $existing = gcloud beta monitoring channels list `
        --project=$Project `
        --filter="type=\"email\" AND labels.email_address=\"$email\"" `
        --format="value(name)" 2>$null
    if ($existing) {
        $channelNames += $existing.Trim()
        continue
    }
    $created = gcloud beta monitoring channels create `
        --project=$Project `
        --display-name="CAPEX Ops $email" `
        --type="email" `
        --channel-labels="email_address=$email" `
        --format="value(name)"
    if ($created) { $channelNames += $created.Trim() }
}

if (-not $channelNames -or $channelNames.Count -eq 0) {
    Write-Warning "No alert email channels configured. Scheduler setup completed without alert policies."
    exit 0
}

function Upsert-AlertPolicy([string]$displayName, [string]$policyJson) {
    $tmp = [System.IO.Path]::GetTempFileName()
    Set-Content -Path $tmp -Value $policyJson -Encoding UTF8
    $existingName = gcloud alpha monitoring policies list `
        --project=$Project `
        --format="value(name,displayName)" | `
        Where-Object { $_ -like "*$displayName" } | `
        ForEach-Object { ($_ -split "\s+")[0] } | `
        Select-Object -First 1
    if ($existingName) {
        Write-Host "  Updating alert policy: $displayName"
        gcloud alpha monitoring policies update $existingName --project=$Project --policy-from-file=$tmp | Out-Null
    } else {
        Write-Host "  Creating alert policy: $displayName"
        gcloud alpha monitoring policies create --project=$Project --policy-from-file=$tmp | Out-Null
    }
    Remove-Item $tmp -ErrorAction SilentlyContinue
}

$channelsJson = ($channelNames | ConvertTo-Json -Compress)

Write-Host "==> Ensuring alert policies..."
$failurePolicy = @"
{
  "displayName": "CAPEX Refresh Job Failure",
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": $channelsJson,
  "documentation": {
    "content": "CAPEX refresh job emitted a failure event. Check Cloud Run Job logs for run_id and output_tail.",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "refresh_job_failed log detected",
      "conditionMatchedLog": {
        "filter": "resource.type=\\"cloud_run_job\\" AND resource.labels.job_name=\\"$JobName\\" AND jsonPayload.event=\\"refresh_job_failed\\""
      }
    }
  ]
}
"@

$stalePolicy = @"
{
  "displayName": "CAPEX Refresh Stale (No Success > 26h)",
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": $channelsJson,
  "documentation": {
    "content": "No successful CAPEX refresh has been observed for more than 26 hours.",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "No refresh success events",
      "conditionAbsent": {
        "filter": "resource.type=\\"cloud_run_job\\" AND metric.type=\\"logging.googleapis.com/user/capex_refresh_successes\\"",
        "duration": "93600s",
        "trigger": { "count": 1 },
        "aggregations": [
          {
            "alignmentPeriod": "3600s",
            "perSeriesAligner": "ALIGN_SUM"
          }
        ]
      }
    }
  ]
}
"@

Upsert-AlertPolicy -displayName "CAPEX Refresh Job Failure" -policyJson $failurePolicy
Upsert-AlertPolicy -displayName "CAPEX Refresh Stale (No Success > 26h)" -policyJson $stalePolicy

Write-Host ""
Write-Host "================================================================"
Write-Host "Scheduler + alerting setup complete."
Write-Host "Scheduler job: $SchedulerJobName"
Write-Host "Cloud Run job: $JobName"
Write-Host "Alert channels: $($AlertEmails -join ', ')"
Write-Host "================================================================"
