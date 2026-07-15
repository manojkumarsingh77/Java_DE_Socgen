#!/usr/bin/env bash
# =====================================================================================
# build-and-push-acr.sh
# Builds the shaded jar + Docker image and pushes ONE immutable, versioned image to
# Azure Container Registry. This same image tag is later promoted unchanged from
# the test namespace to the prod namespace — see LAB_GUIDE.md Part 6.
#
# Usage:
#   ./scripts/build-and-push-acr.sh <acr-login-server> <version>
#   ./scripts/build-and-push-acr.sh retailplatformacr.azurecr.io 1.0.0
# =====================================================================================
set -euo pipefail

ACR_LOGIN_SERVER="${1:?Usage: $0 <acr-login-server> <version>}"
VERSION="${2:?Usage: $0 <acr-login-server> <version>}"
IMAGE="${ACR_LOGIN_SERVER}/retail-multienv-demo:${VERSION}"

echo ">>> [1/4] Compiling and shading jar with Maven"
mvn -B -f "$(dirname "$0")/../pom.xml" clean package -DskipTests

echo ">>> [2/4] Building Docker image: ${IMAGE}"
docker build -f "$(dirname "$0")/../docker/Dockerfile" -t "${IMAGE}" "$(dirname "$0")/.."

echo ">>> [3/4] Logging in to ACR: ${ACR_LOGIN_SERVER}"
az acr login --name "$(echo "${ACR_LOGIN_SERVER}" | cut -d'.' -f1)"

echo ">>> [4/4] Pushing image: ${IMAGE}"
docker push "${IMAGE}"

echo ""
echo "Image pushed: ${IMAGE}"
echo "Next: update the 'image:' field in k8s/test/spark-application.yaml to this tag,"
echo "      then run ./scripts/deploy-aks.sh test"
