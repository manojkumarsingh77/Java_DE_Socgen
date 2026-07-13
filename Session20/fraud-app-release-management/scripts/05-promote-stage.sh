#!/usr/bin/env bash
# GitOps promotion: retags the EXACT SAME artifact from dev -> stage.
# No source is recompiled and no image layer is rebuilt - this is the core
# guarantee of an artifact-promotion pipeline: what you tested is byte-for-
# byte what you ship.
set -euo pipefail

IMAGE_TAG="${1:-}"
if [ -z "${IMAGE_TAG}" ] && [ -f "$(dirname "$0")/../.last-build.env" ]; then
  # shellcheck disable=SC1091
  source "$(dirname "$0")/../.last-build.env"
fi
if [ -z "${IMAGE_TAG:-}" ]; then
  echo "Usage: $0 <image-tag>"
  exit 1
fi

REGISTRY="localhost:5000"

docker pull "${REGISTRY}/fraud-app:${IMAGE_TAG}"
docker tag "${REGISTRY}/fraud-app:${IMAGE_TAG}" "${REGISTRY}/fraud-app:stage"
docker push "${REGISTRY}/fraud-app:stage"

echo ">> Promoted ${IMAGE_TAG} -> stage (retag only)."
echo ">> Deploy with: docker compose -f docker-compose.stage.yml up -d"
