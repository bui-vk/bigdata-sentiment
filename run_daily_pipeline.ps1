<#
.SYNOPSIS
    FINTEL — Daily pipeline runner
    Fetches yesterday's stock prices and loads them into Hive.

.DESCRIPTION
    Designed to be scheduled daily via Windows Task Scheduler.
    Trigger: daily at 07:00 (after US market close + Yahoo Finance update).

    To register as a scheduled task (run once in an admin PowerShell):

        $action  = New-ScheduledTaskAction -Execute "powershell.exe" `
                       -Argument "-NonInteractive -File C:\Users\victo\bigdata-sentiment\run_daily_pipeline.ps1"
        $trigger = New-ScheduledTaskTrigger -Daily -At "07:00"
        Register-ScheduledTask -TaskName "FINTEL-Daily" -Action $action -Trigger $trigger -RunLevel Highest

    Logs are written to .\logs\pipeline_YYYY-MM-DD.log
#>

$ProjectDir = $PSScriptRoot
$LogDir     = Join-Path $ProjectDir "logs"
$LogFile    = Join-Path $LogDir ("pipeline_" + (Get-Date -Format "yyyy-MM-dd") + ".log")

# Ensure logs folder exists
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

function Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $msg
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

Set-Location $ProjectDir
Log "=== FINTEL daily pipeline START ==="

# ---------------------------------------------------------------------------
# Step 1 — Fetch yesterday's prices (finbert container, yfinance → HDFS)
# ---------------------------------------------------------------------------
Log "Step 1: Fetching price data from Yahoo Finance..."
$result = docker-compose run --rm finbert python /app/fetch_price_data.py 2>&1
$result | Add-Content -Path $LogFile
if ($LASTEXITCODE -ne 0) {
    Log "ERROR: fetch_price_data.py failed (exit $LASTEXITCODE) — aborting."
    exit 1
}
Log "Step 1 complete."

# ---------------------------------------------------------------------------
# Step 2 — Load prices into Hive (spark-master)
# ---------------------------------------------------------------------------
Log "Step 2: Loading prices into Hive (spark-master)..."
$result = docker exec spark-master /spark/bin/spark-submit /tmp/job5_load_prices.py 2>&1
$result | Add-Content -Path $LogFile
if ($LASTEXITCODE -ne 0) {
    Log "ERROR: job5_load_prices.py failed (exit $LASTEXITCODE)"
    exit 1
}
Log "Step 2 complete."

Log "=== FINTEL daily pipeline END ==="