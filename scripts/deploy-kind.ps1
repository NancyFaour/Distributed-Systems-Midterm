param([string]$Context = 'kind-midterm')
$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
function Check([string]$step){if($LASTEXITCODE -ne 0){throw "$step failed."}}
if ($Context -ne 'kind-midterm') { throw 'This helper targets kind-midterm. See DEPLOYMENT.md for other contexts.' }
& "$PSScriptRoot/init-env.ps1"
docker compose build storage-node-1 ui
Check 'Build images'
$runtimeDigest = (docker image inspect distributed-storage-node:local --format '{{.Id}}').Replace('sha256:','').Substring(0,12)
$uiDigest = (docker image inspect distributed-storage-ui:local --format '{{.Id}}').Replace('sha256:','').Substring(0,12)
$runtimeImage = "distributed-storage-node:lab-$runtimeDigest"
$uiImage = "distributed-storage-ui:lab-$uiDigest"
docker image tag distributed-storage-node:local $runtimeImage
Check 'Tag runtime image'
docker image tag distributed-storage-ui:local $uiImage
Check 'Tag UI image'
& ./.venv/Scripts/python.exe scripts/generate-kubernetes.py --app-image $runtimeImage --ui-image $uiImage
Check 'Generate manifests'
& ./.venv/Scripts/python.exe scripts/preserve-kubernetes-ha.py --context $Context
Check 'Preserve metadata backend'
& ./.venv/Scripts/python.exe scripts/load-kind-images.py --cluster midterm $runtimeImage $uiImage
Check 'Load images into kind'
kubectl --context $Context create namespace storage-lab --dry-run=client -o yaml | kubectl --context $Context apply -f -
Check 'Create namespace'
& ./.venv/Scripts/python.exe scripts/kubernetes-secrets.py | kubectl --context $Context apply -f -
Check 'Install secrets without displaying values'
kubectl --context $Context -n storage-lab delete job storage-migrate --ignore-not-found
Check 'Replace disposable migration job'
kubectl --context $Context apply -f infra/kubernetes/lab.yaml
Check 'Apply manifests'
Write-Host 'Deployment submitted. Inspect: kubectl --context kind-midterm -n storage-lab get pods,pvc'
Write-Host 'Open UI: kubectl --context kind-midterm -n storage-lab port-forward service/ui 8081:80'
Write-Host 'See DEPLOYMENT.md for Metrics Server, PostgreSQL HA and cloud deployment.'
