param(
    [string]$InputFile = "k6-raw.json"
)

if (-not (Test-Path $InputFile)) {
    Write-Error "Input file not found: $InputFile"
    exit 1
}

$instanceCounts = @{}
$instanceStatusCounts = @{}
$totalMatched = 0

Get-Content $InputFile | ForEach-Object {
    $line = $_.Trim()
    if ([string]::IsNullOrWhiteSpace($line)) { return }

    try {
        $obj = $line | ConvertFrom-Json
    } catch {
        return
    }

    if ($obj.type -ne "Point") { return }
    if ($obj.metric -ne "requests_by_instance") { return }

    $instance = $obj.data.tags.instance
    $status = $obj.data.tags.status

    if ([string]::IsNullOrWhiteSpace($instance)) {
        $instance = "unknown"
    }
    if ([string]::IsNullOrWhiteSpace($status)) {
        $status = "unknown"
    }

    if (-not $instanceCounts.ContainsKey($instance)) {
        $instanceCounts[$instance] = 0
    }
    $instanceCounts[$instance]++

    $pairKey = "$instance`t$status"
    if (-not $instanceStatusCounts.ContainsKey($pairKey)) {
        $instanceStatusCounts[$pairKey] = 0
    }
    $instanceStatusCounts[$pairKey]++

    $totalMatched++
}

Write-Host ""
Write-Host "=== requests_by_instance total points ==="
Write-Host $totalMatched

Write-Host ""
Write-Host "=== instance distribution ==="
$instanceCounts.GetEnumerator() |
    Sort-Object -Property Value -Descending |
    ForEach-Object {
        [PSCustomObject]@{
            instance = $_.Key
            count = $_.Value
        }
    } | Format-Table -AutoSize

Write-Host ""
Write-Host "=== instance + status distribution ==="
$instanceStatusCounts.GetEnumerator() |
    Sort-Object -Property Value -Descending |
    ForEach-Object {
        $parts = $_.Key -split "`t"
        [PSCustomObject]@{
            instance = $parts[0]
            status = $parts[1]
            count = $_.Value
        }
    } | Format-Table -AutoSize
