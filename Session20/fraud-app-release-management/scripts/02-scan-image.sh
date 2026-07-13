#!/usr/bin/env bash
# Vulnerability-scan gate. Locally this runs Trivy against the image you
# just built; in a real Azure pipeline this is the equivalent of Microsoft
# Defender for Cloud's ACR image scanning (or an `az acr task` that runs
# Trivy/Grype as part of an ACR Task). Either way, the principle is the
# same: HIGH/CRITICAL findings BLOCK promotion to the next environment.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="${1:-}"
if [ -z "${IMAGE}" ] && [ -f .last-build.env ]; then
  # shellcheck disable=SC1091
  source .last-build.env
  IMAGE="fraud-app:${IMAGE_TAG}"
fi
if [ -z "${IMAGE}" ]; then
  echo "Usage: $0 <image:tag>   (or run 01-build-and-version.sh first)"
  exit 1
fi

echo ">> Scanning ${IMAGE} with Trivy (severity gate: HIGH,CRITICAL)"

set +e
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v trivy-cache:/root/.cache/ \
  aquasec/trivy:latest image \
  --severity HIGH,CRITICAL \
  --exit-code 1 \
  --ignore-unfixed \
  "${IMAGE}"
STATUS=$?
set -e

if [ ${STATUS} -ne 0 ]; then
  echo ""
  echo "SECURITY GATE FAILED: HIGH/CRITICAL vulnerabilities found in ${IMAGE}."
  echo "Promotion blocked. Patch the base image / dependencies and rebuild."
  exit 1
fi

echo ""
echo "SECURITY GATE PASSED: no unfixed HIGH/CRITICAL vulnerabilities in ${IMAGE}."
