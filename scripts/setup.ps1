$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

function Assert-Exit([string] $Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit $LASTEXITCODE)." }
}

if (-not (Test-Path .venv/Scripts/python.exe)) {
    py -3.12 -m venv .venv
    Assert-Exit 'Create Python environment'
}
& ./.venv/Scripts/python.exe -m pip install -r requirements.txt
Assert-Exit 'Install Python dependencies'

if (-not (Test-Path .env)) {
    $rabbitSecret = [Guid]::NewGuid().ToString('N')
    $grafanaSecret = [Guid]::NewGuid().ToString('N')
    @(
        'RABBITMQ_USER=midterm'
        "RABBITMQ_PASSWORD=$rabbitSecret"
        'GRAFANA_USER=admin'
        "GRAFANA_PASSWORD=$grafanaSecret"
    ) | Set-Content .env
}

docker info --format '{{.ServerVersion}}'
Assert-Exit 'Docker engine check: start Docker Desktop if needed'
docker compose up -d --wait --wait-timeout 240
Assert-Exit 'Start supporting services'
Write-Host 'Services ready. Run .venv\Scripts\python.exe scripts\verify.py to check messaging and monitoring.'
