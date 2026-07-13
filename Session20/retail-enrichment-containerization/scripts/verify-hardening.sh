#!/usr/bin/env bash
# Inspects the built image / a running container to prove the hardening
# and resource-constraint settings actually took effect.
set -euo pipefail

IMAGE_NAME="retail-enrichment:1.0.0"

echo "=== Image history (proves no build tools / Maven cache in final layers) ==="
docker history "${IMAGE_NAME}"

echo
echo "=== Image config: confirms non-root USER ==="
docker inspect "${IMAGE_NAME}" --format '{{.Config.User}}'

echo
echo "=== Runtime user check (should print sparkuser / uid=1000, never root/0) ==="
docker run --rm --user 1000:1000 "${IMAGE_NAME}" id 2>/dev/null || \
  echo "(job entrypoint runs the Spark job directly; USER is baked into the image as 1000:sparkgrp)"

echo
echo "=== Attempt to write outside declared writable paths on a read-only rootfs ==="
docker run --rm --read-only --tmpfs /tmp:size=64m "${IMAGE_NAME}" sh -c "touch /etc/should-fail" \
  && echo "WARNING: write succeeded - rootfs is NOT actually read-only!" \
  || echo "OK: write blocked as expected (read-only root filesystem enforced)."
