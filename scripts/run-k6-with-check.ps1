param(
    [long]$CouponId = 1,
    [string]$RedisContainer = "coupon-redis",
    [int]$RedisPort = 6379,
    [string]$RedisKeyPrefix = "coupon",
    [string]$KafkaContainer = "coupon-kafka",
    [string]$KafkaBootstrap = "localhost:9092",
    [string]$KafkaGroup = "coupon-issue-group",
    [string]$KafkaTopic = "coupon-issue",
    [string]$MysqlContainer = "coupon-mysql",
    [string]$MysqlDatabase = "coupon",
    [string]$MysqlUser = "coupon",
    [string]$MysqlPassword = "coupon",
    [string]$K6ScriptPath = "k6/issue-coupon.js",
    [int]$ConsumerDrainWaitSeconds = 5,
    [switch]$SaveK6Json,
    [string]$K6JsonOutput = "k6-raw.json",
    [string]$SummaryExportPath = "",
    [string]$ResultJsonPath = "",
    [switch]$ClearCouponIssueBeforeRun,
    [switch]$ResetRedisCounterBeforeRun,
    [switch]$SkipK6
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$redisKey = "$RedisKeyPrefix`:$CouponId`:count"

function Resolve-RepoPath {
    param([string]$PathValue)

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return $PathValue
    }

    return (Join-Path $repoRoot $PathValue)
}

function Get-K6MetricsFromSummary {
    param([string]$SummaryPath)

    if ([string]::IsNullOrWhiteSpace($SummaryPath) -or -not (Test-Path $SummaryPath)) {
        return $null
    }

    $summary = Get-Content $SummaryPath -Raw | ConvertFrom-Json
    $metrics = $summary.metrics

    if ($null -eq $metrics) {
        return $null
    }

    function Get-MetricNumber {
        param(
            [object]$Metric,
            [string]$Primary,
            [string]$Fallback = ""
        )

        if ($null -eq $Metric) {
            return $null
        }

        if ($Metric.PSObject.Properties.Name -contains "values") {
            $values = $Metric.values
            if ($null -ne $values) {
                $first = $values.PSObject.Properties[$Primary]
                if ($null -ne $first) {
                    return [double]$first.Value
                }

                if (-not [string]::IsNullOrWhiteSpace($Fallback)) {
                    $second = $values.PSObject.Properties[$Fallback]
                    if ($null -ne $second) {
                        return [double]$second.Value
                    }
                }
            }

            return $null
        }

        $p1 = $Metric.PSObject.Properties[$Primary]
        if ($null -ne $p1) {
            return [double]$p1.Value
        }

        if (-not [string]::IsNullOrWhiteSpace($Fallback)) {
            $p2 = $Metric.PSObject.Properties[$Fallback]
            if ($null -ne $p2) {
                return [double]$p2.Value
            }
        }

        return $null
    }

    $httpReqsMetricProp = $metrics.PSObject.Properties["http_reqs"]
    $httpReqDurationMetricProp = $metrics.PSObject.Properties["http_req_duration"]
    $httpReqFailedMetricProp = $metrics.PSObject.Properties["http_req_failed"]
    $droppedMetricProp = $metrics.PSObject.Properties["dropped_iterations"]

    $httpReqsMetric = if ($null -ne $httpReqsMetricProp) { $httpReqsMetricProp.Value } else { $null }
    $httpReqDurationMetric = if ($null -ne $httpReqDurationMetricProp) { $httpReqDurationMetricProp.Value } else { $null }
    $httpReqFailedMetric = if ($null -ne $httpReqFailedMetricProp) { $httpReqFailedMetricProp.Value } else { $null }
    $droppedMetric = if ($null -ne $droppedMetricProp) { $droppedMetricProp.Value } else { $null }

    return [PSCustomObject]@{
        HttpReqCount = Get-MetricNumber -Metric $httpReqsMetric -Primary "count"
        HttpReqRate = Get-MetricNumber -Metric $httpReqsMetric -Primary "rate"
        HttpReqFailedRate = Get-MetricNumber -Metric $httpReqFailedMetric -Primary "rate" -Fallback "value"
        HttpReqDurationP95 = Get-MetricNumber -Metric $httpReqDurationMetric -Primary "p(95)"
        DroppedIterations = Get-MetricNumber -Metric $droppedMetric -Primary "count"
    }
}

function Get-RedisCounter {
    $output = docker exec $RedisContainer redis-cli -p $RedisPort GET $redisKey

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read Redis counter"
    }

    $last = ($output | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($last)) {
        return [int64]0
    }

    return [int64]$last.Trim()
}

function Reset-RedisCounter {
    $output = docker exec $RedisContainer redis-cli -p $RedisPort DEL $redisKey

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to reset Redis counter"
    }

    return [int64](($output | Select-Object -Last 1).Trim())
}

function Get-CouponIssueStats {
    param([long]$QueryCouponId)

    $query = "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT user_id) AS distinct_users FROM coupon_issue WHERE coupon_id=$QueryCouponId;"
    $output = docker exec $MysqlContainer mysql -N "-u$MysqlUser" "-p$MysqlPassword" -D $MysqlDatabase -e $query

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read MySQL coupon_issue stats"
    }

    $line = ($output | Select-Object -Last 1).Trim()
    $parts = ($line -split "\s+") | Where-Object { $_ -ne "" }
    if ($parts.Count -lt 2) {
        throw "Failed to parse MySQL coupon_issue stats: $line"
    }

    return [PSCustomObject]@{
        TotalRows      = [int64]$parts[0]
        DistinctUsers  = [int64]$parts[1]
    }
}

function Clear-CouponIssue {
    $query = "DELETE FROM coupon_issue;"
    $output = docker exec $MysqlContainer mysql -N "-u$MysqlUser" "-p$MysqlPassword" -D $MysqlDatabase -e $query

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to clear coupon_issue table"
    }

    return $output
}

function Get-KafkaGroupSnapshot {
    $lines = docker exec $KafkaContainer kafka-consumer-groups --bootstrap-server $KafkaBootstrap --group $KafkaGroup --describe 2>$null

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read Kafka consumer group"
    }

    $targets = @($lines | Where-Object { $_ -match "^\s*$KafkaGroup\s+$KafkaTopic\s+\d+\s+" })
    if ($targets.Count -eq 0) {
        throw "Could not find partition info for group=$KafkaGroup, topic=$KafkaTopic"
    }

    function Parse-KafkaLong {
        param([string]$RawValue)

        $trimmed = $RawValue.Trim()
        [int64]$parsed = 0

        if ([int64]::TryParse($trimmed, [ref]$parsed)) {
            return $parsed
        }

        return [int64]-1
    }

    function Sum-OrUnknown {
        param([int64[]]$Values)

        if ($Values.Count -eq 0) {
            return [int64]-1
        }

        if (@($Values | Where-Object { $_ -lt 0 }).Count -gt 0) {
            return [int64]-1
        }

        [int64]$sum = 0
        foreach ($v in $Values) {
            $sum += $v
        }

        return $sum
    }

    $currentOffsets = @()
    $logEndOffsets = @()
    $lags = @()

    foreach ($target in $targets) {
        $parts = ($target -split "\s+") | Where-Object { $_ -ne "" }
        if ($parts.Count -lt 6) {
            throw "Failed to parse Kafka consumer group output: $target"
        }

        $currentOffsets += (Parse-KafkaLong -RawValue $parts[3])
        $logEndOffsets += (Parse-KafkaLong -RawValue $parts[4])
        $lags += (Parse-KafkaLong -RawValue $parts[5])
    }

    return [PSCustomObject]@{
        CurrentOffset = Sum-OrUnknown -Values $currentOffsets
        LogEndOffset  = Sum-OrUnknown -Values $logEndOffsets
        Lag           = Sum-OrUnknown -Values $lags
    }
}

function Format-OffsetValue {
    param([int64]$Value)

    if ($Value -lt 0) {
        return "N/A"
    }

    return $Value
}

function Get-DeltaValue {
    param(
        [int64]$Before,
        [int64]$After
    )

    if ($Before -lt 0 -or $After -lt 0) {
        return $null
    }

    return ($After - $Before)
}

function Print-Snapshot {
    param(
        [string]$Label,
        [int64]$RedisCount,
        [pscustomobject]$DbStats,
        [pscustomobject]$KafkaStats
    )

    Write-Host "[$Label]"
    Write-Host ("Redis {0}={1}" -f $redisKey, $RedisCount)
    Write-Host ("DB total_rows={0}, distinct_users={1}" -f $DbStats.TotalRows, $DbStats.DistinctUsers)
    Write-Host (
        "Kafka current={0}, logEnd={1}, lag={2}" -f
        (Format-OffsetValue -Value $KafkaStats.CurrentOffset),
        (Format-OffsetValue -Value $KafkaStats.LogEndOffset),
        (Format-OffsetValue -Value $KafkaStats.Lag)
    )
    Write-Host ""
}

if ($ClearCouponIssueBeforeRun) {
    Write-Host "Clearing table: coupon_issue"
    Clear-CouponIssue | Out-Null
}

if ($ResetRedisCounterBeforeRun) {
    Write-Host ("Resetting Redis key: {0}" -f $redisKey)
    Reset-RedisCounter | Out-Null
}

$beforeRedis = Get-RedisCounter
$beforeDb = Get-CouponIssueStats -QueryCouponId $CouponId
$beforeKafka = Get-KafkaGroupSnapshot

Write-Host ""
Write-Host "=== k6 Before/After Auto Check ==="
Write-Host ("CouponId={0}" -f $CouponId)
Write-Host ("k6 script={0}" -f $K6ScriptPath)
Write-Host ""
Print-Snapshot -Label "Before" -RedisCount $beforeRedis -DbStats $beforeDb -KafkaStats $beforeKafka

if (-not $SkipK6) {
    $resolvedK6Script = Resolve-RepoPath -PathValue $K6ScriptPath

    if (-not (Test-Path $resolvedK6Script)) {
        throw "k6 script not found: $resolvedK6Script"
    }

    $k6Args = @("run", $resolvedK6Script)
    if ($SaveK6Json) {
        $resolvedJsonPath = Resolve-RepoPath -PathValue $K6JsonOutput
        $k6Args += @("--out", "json=$resolvedJsonPath")
    }

    $resolvedSummaryPath = $null
    if (-not [string]::IsNullOrWhiteSpace($SummaryExportPath)) {
        $resolvedSummaryPath = Resolve-RepoPath -PathValue $SummaryExportPath
        $k6Args += @("--summary-export", $resolvedSummaryPath)
    }

    Write-Host "Running k6..."
    & k6 @k6Args

    $k6ExitCode = $LASTEXITCODE
    if ($k6ExitCode -eq 99) {
        Write-Host "k6 finished with threshold failures (exit code=99). Continuing for before/after diagnosis."
    } elseif ($k6ExitCode -ne 0) {
        throw "k6 run failed (exit code=$k6ExitCode)"
    }

    if ($ConsumerDrainWaitSeconds -gt 0) {
        Write-Host ("Waiting {0}s for Kafka consumer drain..." -f $ConsumerDrainWaitSeconds)
        Start-Sleep -Seconds $ConsumerDrainWaitSeconds
    }
}

$afterRedis = Get-RedisCounter
$afterDb = Get-CouponIssueStats -QueryCouponId $CouponId
$afterKafka = Get-KafkaGroupSnapshot

Print-Snapshot -Label "After" -RedisCount $afterRedis -DbStats $afterDb -KafkaStats $afterKafka

$deltaRedis = $afterRedis - $beforeRedis
$deltaDbRows = $afterDb.TotalRows - $beforeDb.TotalRows
$deltaDbDistinct = $afterDb.DistinctUsers - $beforeDb.DistinctUsers
$deltaKafkaCurrent = Get-DeltaValue -Before $beforeKafka.CurrentOffset -After $afterKafka.CurrentOffset
$deltaKafkaLogEnd = Get-DeltaValue -Before $beforeKafka.LogEndOffset -After $afterKafka.LogEndOffset

Write-Host "[Delta]"
Write-Host ("Redis delta={0}" -f $deltaRedis)
Write-Host ("DB total_rows delta={0}" -f $deltaDbRows)
Write-Host ("DB distinct_users delta={0}" -f $deltaDbDistinct)
Write-Host ("Kafka current_offset delta={0}" -f ($(if ($null -eq $deltaKafkaCurrent) { "N/A" } else { $deltaKafkaCurrent })))
Write-Host ("Kafka log_end_offset delta={0}" -f ($(if ($null -eq $deltaKafkaLogEnd) { "N/A" } else { $deltaKafkaLogEnd })))
Write-Host ""

Write-Host "[Quick Diagnosis]"
if ($deltaRedis -gt 0 -and $deltaDbRows -le 0 -and $null -ne $deltaKafkaCurrent -and $deltaKafkaCurrent -gt 0) {
    Write-Host "Redis increased + Kafka consumed + DB did not grow: likely duplicate userId requests or DB write failures."
} elseif ($null -ne $deltaKafkaCurrent -and $null -ne $deltaKafkaLogEnd -and $deltaKafkaCurrent -le 0 -and $deltaKafkaLogEnd -gt 0) {
    Write-Host "Publish happened but consume did not catch up yet: consumer may be slow or stalled (check lag)."
} elseif ($null -ne $deltaKafkaLogEnd -and $deltaKafkaLogEnd -le 0) {
    Write-Host "Kafka publish likely did not happen (check producer/app logs)."
} elseif ($deltaDbRows -gt 0) {
    Write-Host "Kafka -> DB pipeline is working."
} else {
    Write-Host "Need additional logs (app/nginx/kafka) for diagnosis."
}

if ($SaveK6Json) {
    Write-Host ""
    Write-Host ("k6 raw output saved: {0}" -f (Resolve-RepoPath -PathValue $K6JsonOutput))
    Write-Host "Check instance distribution: powershell -ExecutionPolicy Bypass -File scripts/parse-k6-instance.ps1 -InputFile k6-raw.json"
}

$k6Metrics = $null
if (-not $SkipK6) {
    $summaryPathForRead = if (-not [string]::IsNullOrWhiteSpace($SummaryExportPath)) { Resolve-RepoPath -PathValue $SummaryExportPath } else { "" }
    $k6Metrics = Get-K6MetricsFromSummary -SummaryPath $summaryPathForRead
}

if (-not [string]::IsNullOrWhiteSpace($ResultJsonPath)) {
    $resolvedResultPath = Resolve-RepoPath -PathValue $ResultJsonPath
    $result = [PSCustomObject]@{
        targetUrl = $env:TARGET_URL
        rate = if ($env:RATE) { [int]$env:RATE } else { $null }
        before = [PSCustomObject]@{
            redis = $beforeRedis
            dbTotalRows = $beforeDb.TotalRows
            dbDistinctUsers = $beforeDb.DistinctUsers
            kafkaCurrentOffset = $beforeKafka.CurrentOffset
            kafkaLogEndOffset = $beforeKafka.LogEndOffset
            kafkaLag = $beforeKafka.Lag
        }
        after = [PSCustomObject]@{
            redis = $afterRedis
            dbTotalRows = $afterDb.TotalRows
            dbDistinctUsers = $afterDb.DistinctUsers
            kafkaCurrentOffset = $afterKafka.CurrentOffset
            kafkaLogEndOffset = $afterKafka.LogEndOffset
            kafkaLag = $afterKafka.Lag
        }
        delta = [PSCustomObject]@{
            redis = $deltaRedis
            dbTotalRows = $deltaDbRows
            dbDistinctUsers = $deltaDbDistinct
            kafkaCurrentOffset = $deltaKafkaCurrent
            kafkaLogEndOffset = $deltaKafkaLogEnd
        }
        k6 = $k6Metrics
    }

    $resultDir = Split-Path -Parent $resolvedResultPath
    if (-not [string]::IsNullOrWhiteSpace($resultDir)) {
        New-Item -ItemType Directory -Path $resultDir -Force | Out-Null
    }

    $result | ConvertTo-Json -Depth 8 | Set-Content -Path $resolvedResultPath -Encoding UTF8
    Write-Host ("Result JSON saved: {0}" -f $resolvedResultPath)
}
