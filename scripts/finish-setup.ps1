param([int[]] $WaitForProcess = @())

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$statusPath = Join-Path (Get-Location) '.setup-status.log'
Start-Transcript -Path $statusPath -Force | Out-Null
try {
    Write-Host 'IN PROGRESS: environment setup. Keep Docker Desktop running.'
    if ($WaitForProcess.Count -gt 0) {
        Write-Host 'Waiting for the existing image downloads and cluster creation...'
        foreach ($processIdToWait in $WaitForProcess) {
            Wait-Process -Id $processIdToWait -ErrorAction SilentlyContinue
        }
    }
    & (Join-Path $PSScriptRoot 'setup.ps1')
    & (Join-Path $PSScriptRoot 'setup-kubernetes.ps1')
    # Allow Prometheus's first scrape after containers become healthy.
    $verified = $false
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        & ./.venv/Scripts/python.exe scripts/verify.py
        if ($LASTEXITCODE -eq 0) { $verified = $true; break }
        Start-Sleep -Seconds 15
    }
    if (-not $verified) { throw 'Messaging/monitoring validation failed. See output above.' }
    Write-Host 'COMPLETE: Python dependencies, all four Compose services, messaging, monitoring, and Kubernetes node readiness verified.'
} catch {
    Write-Host "FAILED: $($_.Exception.Message)"
    exit 1
} finally {
    Stop-Transcript | Out-Null
}
