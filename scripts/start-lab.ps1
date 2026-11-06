$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($env:OS -eq 'Windows_NT') {
    $activeBenchmark = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^python(?:w)?\.exe$' -and $_.CommandLine -match 'benchmark_redundancy\.py'
    }
    if ($activeBenchmark) {
        Write-Host 'A workload benchmark is still running. Startup is deferred so its deliberate node outage is preserved.'
        Write-Host 'Check progress: Get-Content test-results/benchmark-full.log -Tail 25'
        Write-Host 'Wait for the final PASS, then run this startup script again.'
        return
    }
}
function Check([string]$step) { if ($LASTEXITCODE -ne 0) { throw "$step failed; inspect the output above." } }
& $PSScriptRoot/init-env.ps1
if (-not (Test-Path .venv/Scripts/python.exe)) { py -3.12 -m venv .venv; Check 'Create virtual environment' }
& ./.venv/Scripts/python.exe -m pip install -r requirements-dev.txt
Check 'Install dependencies'
docker compose config --quiet
Check 'Compose validation'
docker compose build storage-node-1 ui
Check 'Build application images'
docker compose up -d --no-build --wait --wait-timeout 360
Check 'Start lab services'
docker compose ps
$recovery = Invoke-RestMethod http://127.0.0.1:8020/status
if ($recovery.paused) {
    Write-Host 'Automatic repair is paused. Let an active benchmark finish. If a previous benchmark was interrupted, resume it using TESTING.md.'
}
Write-Host 'READY: http://localhost:8080 — API docs: http://localhost:8080/api/docs'
Write-Host 'Use API_KEY from .env. Next: read TESTING.md and capture each phase.'
