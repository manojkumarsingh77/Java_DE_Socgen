#!/usr/bin/env bash
# Promotes the tested "stage" artifact to "prod-green" - the release
# candidate slot. "prod-blue" (current live production) is left completely
# untouched, so production traffic keeps flowing on the known-good version
# until the canary rollout explicitly earns its way to 100% green.
set -euo pipefail

IMAGE_TAG="${1:-stage}"
REGISTRY="localhost:5000"

docker pull "${REGISTRY}/fraud-app:${IMAGE_TAG}"
docker tag "${REGISTRY}/fraud-app:${IMAGE_TAG}" "${REGISTRY}/fraud-app:prod-green"
docker push "${REGISTRY}/fraud-app:prod-green"

echo ">> Promoted ${IMAGE_TAG} -> prod-green (release candidate)."
echo ">> prod-blue remains the stable, 100%-traffic version until canary completes."
echo ">> If this is the VERY FIRST release, bootstrap prod-blue too:"
echo "     docker tag ${REGISTRY}/fraud-app:${IMAGE_TAG} ${REGISTRY}/fraud-app:prod-blue"
echo "     docker push ${REGISTRY}/fraud-app:prod-blue"
