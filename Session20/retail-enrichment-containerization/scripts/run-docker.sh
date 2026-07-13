#!/usr/bin/env bash
# Build the hardened image and run it with EXPLICIT resource constraints
# and hardening flags on the "docker run" command line. This is the most
# unambiguous way to demonstrate constraints (works on any Docker version,
# no compose-version caveats).
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE_NAME="retail-enrichment:1.0.0"

echo ">> [1/3] Building multi-stage hardened image: ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" .

mkdir -p output

echo ">> [2/3] Running container with resource constraints + hardening..."
docker run --rm \
  --name retail-enrichment-job \
  --memory=1g \
  --memory-reservation=512m \
  --cpus=1.5 \
  --pids-limit=100 \
  --read-only \
  --security-opt no-new-privileges:true \
  --cap-drop=ALL \
  --tmpfs /tmp:size=256m,mode=1777 \
  -v "$(pwd)/output:/app/output" \
  "${IMAGE_NAME}"

echo ">> [3/3] Done. Enriched output is in ./output"
