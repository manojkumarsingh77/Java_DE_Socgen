#!/usr/bin/env bash
# =====================================================================================
# deploy-aks.sh
# Applies the manifest set for a single environment (test or prod) in the correct
# order: namespace -> secret plumbing -> configmap -> SparkApplication.
#
# Usage:
#   ./scripts/deploy-aks.sh test
#   ./scripts/deploy-aks.sh prod
# =====================================================================================
set -euo pipefail

ENVIRONMENT="${1:?Usage: $0 <test|prod>}"

if [[ "${ENVIRONMENT}" != "test" && "${ENVIRONMENT}" != "prod" ]]; then
  echo "ERROR: environment must be 'test' or 'prod', got '${ENVIRONMENT}'" >&2
  exit 1
fi

MANIFEST_DIR="$(dirname "$0")/../k8s/${ENVIRONMENT}"

if [[ "${ENVIRONMENT}" == "prod" ]]; then
  echo ">>> PRODUCTION DEPLOYMENT — confirm promotion has been validated in test."
  read -r -p "    Type 'promote-to-prod' to continue: " CONFIRMATION
  if [[ "${CONFIRMATION}" != "promote-to-prod" ]]; then
    echo "Aborted." >&2
    exit 1
  fi
fi

echo ">>> [1/4] Applying namespace"
kubectl apply -f "${MANIFEST_DIR}/namespace.yaml"

echo ">>> [2/4] Applying Workload Identity ServiceAccount + SecretProviderClass"
kubectl apply -f "${MANIFEST_DIR}/secretproviderclass.yaml"

echo ">>> [3/4] Applying externalized ConfigMap"
kubectl apply -f "${MANIFEST_DIR}/configmap.yaml"

echo ">>> [4/4] Submitting SparkApplication"
kubectl apply -f "${MANIFEST_DIR}/spark-application.yaml"

echo ""
echo "Deployed to namespace retail-platform-${ENVIRONMENT}."
echo "Watch status with:"
echo "  kubectl get sparkapplication -n retail-platform-${ENVIRONMENT} -w"
echo "Tail driver logs with:"
echo "  kubectl logs -n retail-platform-${ENVIRONMENT} -l spark-role=driver -f"
