#!/usr/bin/env bash
# Pushes the built, scanned image into the (local-registry) ACR stand-in
# under both its immutable version tag and the floating "dev" tag.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE_TAG="${1:-}"
if [ -z "${IMAGE_TAG}" ] && [ -f .last-build.env ]; then
  # shellcheck disable=SC1091
  source .last-build.env
fi
if [ -z "${IMAGE_TAG:-}" ]; then
  echo "Usage: $0 <image-tag>   (or run 01-build-and-version.sh first)"
  exit 1
fi

REGISTRY="localhost:5000"

docker tag "fraud-app:${IMAGE_TAG}" "${REGISTRY}/fraud-app:${IMAGE_TAG}"
docker tag "fraud-app:${IMAGE_TAG}" "${REGISTRY}/fraud-app:dev"

docker push "${REGISTRY}/fraud-app:${IMAGE_TAG}"
docker push "${REGISTRY}/fraud-app:dev"

echo ">> Pushed ${REGISTRY}/fraud-app:${IMAGE_TAG} and :dev"
echo "IMAGE_TAG=${IMAGE_TAG}" > .last-build.env
