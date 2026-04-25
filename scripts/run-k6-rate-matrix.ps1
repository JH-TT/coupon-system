param(
    [string]$TargetUrl = "http://localhost:8081/api/v1/coupons/issue",
    [string]$RatesCsv = "2000,3000,4000,5000",
    [int]$Repeats = 3,
    [string]$Duration = "10s",
    [int]$PreAllocatedVUs = 2000,
    [int]$MaxVUs = 5000,
    [long]$CouponId = 1,
    [string]$OutputDir = "artifacts/rate-matrix"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot "run-k6-with-check.ps1"

$Rates = @($RatesCsv -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" } | ForEach-Object { [int]$_ })
if ($Rates.Count -eq 0) {
    throw "No valid rates parsed from RatesCsv=$RatesCsv"
}

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

Write-Host ""
Write-Host "=== k6 rate matrix run ==="
Write-Host ("TargetUrl={0}" -f $TargetUrl)
Write-Host ("Rates={0}" -f ($Rates -join ", "))
Write-Host ("Repeats={0}" -f $Repeats)
Write-Host ""

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$baseOutputDir = if ([System.IO.Path]::IsPathRooted($OutputDir)) { $OutputDir } else { Join-Path $repoRoot $OutputDir }
$runOutputDir = Join-Path $baseOutputDir $timestamp
New-Item -ItemType Directory -Path $runOutputDir -Force | Out-Null

$allResults = @()

function Get-Average {
    param([double[]]$Values)

    $valid = @($Values | Where-Object { $_ -ne $null })
    if ($valid.Count -eq 0) {
        return $null
    }

    [double]$sum = 0
    foreach ($v in $valid) {
        $sum += $v
    }

    return ($sum / $valid.Count)
}

Push-Location $repoRoot
try {
    foreach ($rate in $Rates) {
        for ($run = 1; $run -le $Repeats; $run++) {
            Write-Host ""
            Write-Host ("--- RATE={0}, RUN={1}/{2} ---" -f $rate, $run, $Repeats)

            $env:TARGET_URL = $TargetUrl
            $env:RATE = [string]$rate
            $env:DURATION = $Duration
            $env:PRE_ALLOCATED_VUS = [string]$PreAllocatedVUs
            $env:MAX_VUS = [string]$MaxVUs

            $summaryPath = Join-Path $runOutputDir ("summary-rate{0}-run{1}.json" -f $rate, $run)
            $resultPath = Join-Path $runOutputDir ("result-rate{0}-run{1}.json" -f $rate, $run)

            powershell -ExecutionPolicy Bypass -File $runner -CouponId $CouponId -ClearCouponIssueBeforeRun -ResetRedisCounterBeforeRun -SummaryExportPath $summaryPath -ResultJsonPath $resultPath

            if ($LASTEXITCODE -ne 0) {
                throw "Run failed for RATE=$rate RUN=$run"
            }

            if (Test-Path $resultPath) {
                $resultObj = Get-Content $resultPath -Raw | ConvertFrom-Json
                $allResults += $resultObj
            }
        }
    }
}
finally {
    Remove-Item Env:TARGET_URL -ErrorAction SilentlyContinue
    Remove-Item Env:RATE -ErrorAction SilentlyContinue
    Remove-Item Env:DURATION -ErrorAction SilentlyContinue
    Remove-Item Env:PRE_ALLOCATED_VUS -ErrorAction SilentlyContinue
    Remove-Item Env:MAX_VUS -ErrorAction SilentlyContinue
    Pop-Location
}

Write-Host ""
Write-Host "k6 rate matrix finished."
Write-Host ("Artifacts: {0}" -f $runOutputDir)

if ($allResults.Count -gt 0) {
    Write-Host ""
    Write-Host "=== Per-run summary ==="
    $perRun = foreach ($r in $allResults) {
        [PSCustomObject]@{
            rate = $r.rate
            reqs = if ($r.k6 -and $r.k6.HttpReqCount -ne $null) { [math]::Round([double]$r.k6.HttpReqCount, 0) } else { $null }
            reqPerSec = if ($r.k6 -and $r.k6.HttpReqRate -ne $null) { [math]::Round([double]$r.k6.HttpReqRate, 2) } else { $null }
            failedPct = if ($r.k6 -and $r.k6.HttpReqFailedRate -ne $null) { [math]::Round(([double]$r.k6.HttpReqFailedRate * 100), 2) } else { $null }
            p95Ms = if ($r.k6 -and $r.k6.HttpReqDurationP95 -ne $null) { [math]::Round([double]$r.k6.HttpReqDurationP95, 2) } else { $null }
            dropped = if ($r.k6 -and $r.k6.DroppedIterations -ne $null) { [math]::Round([double]$r.k6.DroppedIterations, 0) } else { $null }
            redisDelta = $r.delta.redis
            dbDelta = $r.delta.dbTotalRows
            kafkaLogEndDelta = $r.delta.kafkaLogEndOffset
        }
    }

    $perRun | Sort-Object rate | Format-Table -AutoSize

    Write-Host ""
    Write-Host "=== Average by rate ==="
    $avgRows = @()

    foreach ($g in ($allResults | Group-Object rate | Sort-Object Name)) {
        $items = @($g.Group)
        $avgReqPerSec = Get-Average -Values @($items | ForEach-Object { if ($_.k6) { [double]$_.k6.HttpReqRate } else { $null } })
        $avgFailedPct = Get-Average -Values @($items | ForEach-Object { if ($_.k6) { [double]$_.k6.HttpReqFailedRate * 100 } else { $null } })
        $avgP95Ms = Get-Average -Values @($items | ForEach-Object { if ($_.k6) { [double]$_.k6.HttpReqDurationP95 } else { $null } })
        $avgDropped = Get-Average -Values @($items | ForEach-Object { if ($_.k6) { [double]$_.k6.DroppedIterations } else { $null } })
        $avgDbDelta = Get-Average -Values @($items | ForEach-Object { [double]$_.delta.dbTotalRows })

        $avgRows += [PSCustomObject]@{
            rate = [int]$g.Name
            runs = $items.Count
            avgReqPerSec = if ($avgReqPerSec -ne $null) { [math]::Round($avgReqPerSec, 2) } else { $null }
            avgFailedPct = if ($avgFailedPct -ne $null) { [math]::Round($avgFailedPct, 2) } else { $null }
            avgP95Ms = if ($avgP95Ms -ne $null) { [math]::Round($avgP95Ms, 2) } else { $null }
            avgDropped = if ($avgDropped -ne $null) { [math]::Round($avgDropped, 0) } else { $null }
            avgDbDelta = if ($avgDbDelta -ne $null) { [math]::Round($avgDbDelta, 0) } else { $null }
        }
    }

    $avgRows | Format-Table -AutoSize
}
