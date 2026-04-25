param(
    [ValidateSet("default", "bench")]
    [string]$Mode = "default"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$nginxDir = Join-Path $repoRoot "nginx"
$source = Join-Path $nginxDir ("nginx.{0}.conf" -f $Mode)
$target = Join-Path $nginxDir "nginx.conf"

if (-not (Test-Path $source)) {
    throw "Nginx mode config not found: $source"
}

Copy-Item -Path $source -Destination $target -Force
Write-Host ("Applied nginx mode: {0}" -f $Mode)
Write-Host ("Config copied: {0} -> {1}" -f $source, $target)

Push-Location $repoRoot
try {
    docker compose up -d --force-recreate nginx
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to recreate nginx container"
    }
} finally {
    Pop-Location
}

Write-Host "Nginx container recreated successfully."
