$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$kindPath = Join-Path (Get-Location) '.tools/kind.exe'
if (-not (Test-Path $kindPath)) {
    New-Item -ItemType Directory -Path .tools -Force | Out-Null
    Invoke-WebRequest https://kind.sigs.k8s.io/dl/v0.33.0/kind-windows-amd64 -OutFile $kindPath
}
& $kindPath version
if ($LASTEXITCODE -ne 0) { throw 'kind installation failed.' }
$clusters = & $kindPath get clusters
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect kind clusters. Check Docker Desktop.' }
if ($clusters -notcontains 'midterm') {
    & $kindPath create cluster --name midterm --config infra/kind.yaml --wait 180s
    if ($LASTEXITCODE -ne 0) { throw 'Kubernetes cluster creation failed.' }
}
kubectl --context kind-midterm wait --for=condition=Ready node --all --timeout=180s
if ($LASTEXITCODE -ne 0) { throw 'Kubernetes nodes are not ready.' }
kubectl --context kind-midterm get nodes
