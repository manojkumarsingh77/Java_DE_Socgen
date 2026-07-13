#!/usr/bin/env bash
# Builds the Docker image with a real, traceable version tag:
#   <semver>-<short-git-sha>
# and bakes APP_VERSION / GIT_COMMIT / BUILD_DATE into OCI labels + runtime
# env vars (see Dockerfile) so the running service can report exactly what
# it is via GET /version.
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="${1:-1.0.0}"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
IMAGE_TAG="${VERSION}-${GIT_SHA}"

echo ">> Building fraud-app:${IMAGE_TAG}"
echo "   APP_VERSION=${VERSION}  GIT_COMMIT=${GIT_SHA}  BUILD_DATE=${BUILD_DATE}"

docker build \
  --build-arg APP_VERSION="${VERSION}" \
  --build-arg GIT_COMMIT="${GIT_SHA}" \
  --build-arg BUILD_DATE="${BUILD_DATE}" \
  -t "fraud-app:${IMAGE_TAG}" \
  .

echo "IMAGE_TAG=${IMAGE_TAG}" > .last-build.env
echo ">> Built fraud-app:${IMAGE_TAG}  (tag recorded in .last-build.env)"
