param([string]$Context='kind-midterm')
$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($Context -notlike 'kind-*') { throw 'This helper applies a local-only Metrics Server TLS option; use it only with kind.' }
kubectl --context $Context apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.29/releases/cnpg-1.29.3.yaml
if($LASTEXITCODE -ne 0){throw 'CloudNativePG operator installation failed'}
kubectl --context $Context apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
if($LASTEXITCODE -ne 0){throw 'Metrics Server installation failed'}
$deployment=kubectl --context $Context -n kube-system get deployment metrics-server -o json | ConvertFrom-Json
if('--kubelet-insecure-tls' -notin $deployment.spec.template.spec.containers[0].args){
    kubectl --context $Context -n kube-system patch deployment metrics-server --type=json --patch-file infra/kubernetes/metrics-server-kind-patch.json
    if($LASTEXITCODE -ne 0){throw 'Local Metrics Server configuration failed'}
}
Write-Host 'Operator and Metrics Server submitted. Wait for their deployments to become Ready.'
