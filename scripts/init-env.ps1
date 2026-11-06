$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not (Test-Path .env)) { New-Item -ItemType File .env | Out-Null }
$existing = Get-Content .env -Raw
$defaults = @{
    RABBITMQ_USER = 'midterm'
    RABBITMQ_PASSWORD = [Guid]::NewGuid().ToString('N')
    GRAFANA_USER = 'admin'
    GRAFANA_PASSWORD = [Guid]::NewGuid().ToString('N')
    POSTGRES_PASSWORD = [Guid]::NewGuid().ToString('N')
    API_KEY = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N'))
}
foreach ($entry in $defaults.GetEnumerator()) {
    if ($existing -notmatch "(?m)^$($entry.Key)=") {
        Add-Content -Path .env -Value "$($entry.Key)=$($entry.Value)" -Encoding UTF8
    }
}
Write-Host 'Local credentials configured. Open .env privately to find API_KEY.'
