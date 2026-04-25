param(
    [long]$UserId = 987654321,
    [long]$CouponId = 1,
    [string]$ApiUrl = "http://localhost:8081/api/v1/coupons/issue",
    [string]$KafkaContainer = "coupon-kafka",
    [string]$KafkaBootstrap = "localhost:9092",
    [string]$KafkaGroup = "coupon-issue-group",
    [string]$KafkaTopic = "coupon-issue",
    [string]$MysqlContainer = "coupon-mysql",
    [string]$MysqlDatabase = "coupon",
    [string]$MysqlUser = "coupon",
    [string]$MysqlPassword = "coupon",
    [int]$WaitSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-CouponIssueCount {
    param([long]$QueryCouponId, [long]$QueryUserId)

    $query = "SELECT COUNT(*) FROM coupon_issue WHERE coupon_id=$QueryCouponId AND user_id=$QueryUserId;"
    $output = docker exec $MysqlContainer mysql -N "-u$MysqlUser" "-p$MysqlPassword" -D $MysqlDatabase -e $query

    if ($LASTEXITCODE -ne 0) {
        throw "MySQL 조회 실패"
    }

    $value = $output | Select-Object -Last 1
    return [int64]$value.Trim()
}

function Get-KafkaGroupSnapshot {
    $lines = docker exec $KafkaContainer kafka-consumer-groups --bootstrap-server $KafkaBootstrap --group $KafkaGroup --describe 2>$null

    if ($LASTEXITCODE -ne 0) {
        throw "Kafka consumer group 조회 실패"
    }

    $target = $lines | Where-Object { $_ -match "^\s*$KafkaGroup\s+$KafkaTopic\s+\d+\s+" } | Select-Object -First 1

    if (-not $target) {
        throw "consumer group=$KafkaGroup, topic=$KafkaTopic 파티션 정보를 찾지 못했습니다."
    }

    $parts = ($target -split "\s+") | Where-Object { $_ -ne "" }

    if ($parts.Count -lt 6) {
        throw "Kafka consumer group 출력 파싱 실패: $target"
    }

    return [PSCustomObject]@{
        Group         = $parts[0]
        Topic         = $parts[1]
        Partition     = [int]$parts[2]
        CurrentOffset = [int64]$parts[3]
        LogEndOffset  = [int64]$parts[4]
        Lag           = [int64]$parts[5]
    }
}

$beforeDb = Get-CouponIssueCount -QueryCouponId $CouponId -QueryUserId $UserId
$beforeKafka = Get-KafkaGroupSnapshot

$body = @{ userId = $UserId; couponId = $CouponId } | ConvertTo-Json -Compress
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri $ApiUrl -ContentType "application/json" -Body $body
$sw.Stop()

$deadline = (Get-Date).AddSeconds($WaitSeconds)
$afterDb = $beforeDb
$afterKafka = $beforeKafka

do {
    Start-Sleep -Milliseconds 500
    $afterDb = Get-CouponIssueCount -QueryCouponId $CouponId -QueryUserId $UserId
    $afterKafka = Get-KafkaGroupSnapshot

    if (($afterDb -gt $beforeDb) -or ($afterKafka.CurrentOffset -gt $beforeKafka.CurrentOffset)) {
        break
    }
} while ((Get-Date) -lt $deadline)

Write-Host ""
Write-Host "=== Kafka End-to-End Check ==="
Write-Host ("API: {0}" -f $ApiUrl)
Write-Host ("Request userId={0}, couponId={1}" -f $UserId, $CouponId)
Write-Host ("HTTP status={0}, durationMs={1}" -f $response.StatusCode, $sw.ElapsedMilliseconds)
Write-Host ""

Write-Host "[DB] coupon_issue rows for (couponId,userId)"
Write-Host ("before={0}, after={1}, delta={2}" -f $beforeDb, $afterDb, ($afterDb - $beforeDb))
Write-Host ""

Write-Host "[Kafka] consumer group offsets"
Write-Host ("before current={0}, logEnd={1}, lag={2}" -f $beforeKafka.CurrentOffset, $beforeKafka.LogEndOffset, $beforeKafka.Lag)
Write-Host ("after  current={0}, logEnd={1}, lag={2}" -f $afterKafka.CurrentOffset, $afterKafka.LogEndOffset, $afterKafka.Lag)
Write-Host ("delta  current={0}, logEnd={1}" -f ($afterKafka.CurrentOffset - $beforeKafka.CurrentOffset), ($afterKafka.LogEndOffset - $beforeKafka.LogEndOffset))
Write-Host ""

if ($afterKafka.CurrentOffset -gt $beforeKafka.CurrentOffset) {
    Write-Host "Kafka consume observed: YES"
} else {
    Write-Host "Kafka consume observed: NO (timeout or no new message)"
}

if ($afterDb -gt $beforeDb) {
    Write-Host "DB insert observed: YES"
} else {
    Write-Host "DB insert observed: NO (timeout or write failure)"
}
