param(
    [string]$KafkaContainer = "coupon-kafka",
    [string]$KafkaBootstrap = "localhost:9092",
    [string]$GroupId = "coupon-issue-group",
    [string]$Topic = "coupon-issue",
    [int]$IntervalSeconds = 10,
    [int]$Samples = 6
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Samples -lt 2) {
    throw "Samples must be >= 2"
}

if ($IntervalSeconds -lt 1) {
    throw "IntervalSeconds must be >= 1"
}

function Parse-LongOrNull {
    param([string]$Raw)

    $trimmed = $Raw.Trim()
    [int64]$parsed = 0

    if ([int64]::TryParse($trimmed, [ref]$parsed)) {
        return $parsed
    }

    return $null
}

function Get-LagSnapshot {
    $lines = docker exec $KafkaContainer kafka-consumer-groups --bootstrap-server $KafkaBootstrap --group $GroupId --describe 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read Kafka consumer group: $GroupId"
    }

    $targets = @($lines | Where-Object { $_ -match "^\s*$GroupId\s+$Topic\s+\d+\s+" })
    if ($targets.Count -eq 0) {
        throw "No partition rows found for group=$GroupId topic=$Topic"
    }

    [int64]$totalLag = 0
    [int64]$knownPartitions = 0

    foreach ($line in $targets) {
        $parts = @(($line -split "\s+") | Where-Object { $_ -ne "" })
        if ($parts.Count -lt 6) {
            continue
        }

        $lag = Parse-LongOrNull -Raw $parts[5]
        if ($null -eq $lag) {
            continue
        }

        $totalLag += $lag
        $knownPartitions++
    }

    return [PSCustomObject]@{
        Time = Get-Date
        TotalLag = $totalLag
        KnownPartitions = $knownPartitions
    }
}

$snapshots = @()

Write-Host ""
Write-Host "=== Kafka lag drain measurement ==="
Write-Host ("group={0}, topic={1}, samples={2}, interval={3}s" -f $GroupId, $Topic, $Samples, $IntervalSeconds)
Write-Host ""

for ($i = 1; $i -le $Samples; $i++) {
    $snapshot = Get-LagSnapshot
    $snapshots += $snapshot

    Write-Host ("[{0}] lag={1} (knownPartitions={2})" -f $snapshot.Time.ToString("HH:mm:ss"), $snapshot.TotalLag, $snapshot.KnownPartitions)

    if ($i -lt $Samples) {
        Start-Sleep -Seconds $IntervalSeconds
    }
}

$first = $snapshots[0]
$last = $snapshots[$snapshots.Count - 1]
$elapsedSec = [math]::Max(1.0, ($last.Time - $first.Time).TotalSeconds)
$delta = $first.TotalLag - $last.TotalLag
$ratePerSec = $delta / $elapsedSec

Write-Host ""
Write-Host "=== Summary ==="
Write-Host ("startLag={0}, endLag={1}, delta={2}" -f $first.TotalLag, $last.TotalLag, $delta)
Write-Host ("elapsedSec={0:N1}, drainPerSec={1:N2}" -f $elapsedSec, $ratePerSec)

if ($delta -gt 0) {
    Write-Host "Result: draining (lag decreasing)."
} elseif ($delta -lt 0) {
    Write-Host "Result: lag increasing (ingress > consume)."
} else {
    Write-Host "Result: no net change."
}
