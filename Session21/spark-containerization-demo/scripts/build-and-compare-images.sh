#!/bin/bash
# Builds all three teaching images and prints a size comparison table.
# This is the script to run live for the "Docker Multi-Stage Builds" topic.
set -e
cd "$(dirname "$0")/.."

echo "=== Building Act 1: naive (single-stage) image ==="
docker build -f docker/Dockerfile.naive -t spark-training/demo:naive .

echo "=== Building Act 2: multi-stage image ==="
docker build -f docker/Dockerfile.multistage -t spark-training/demo:multistage .

echo "=== Building Act 3: hardened multi-stage image ==="
docker build -f docker/Dockerfile.hardened -t spark-training/demo:hardened .

echo
echo "=== IMAGE SIZE COMPARISON ==="
docker images spark-training/demo --format "table {{.Tag}}\t{{.Size}}\t{{.CreatedSince}}"

echo
echo "=== LAYER HISTORY (hardened image) ==="
docker history spark-training/demo:hardened --human --format "table {{.CreatedBy}}\t{{.Size}}" | head -20

echo
echo "=== USER the process runs as (naive vs hardened) ==="
echo -n "naive    : "; docker run --rm --entrypoint whoami spark-training/demo:naive || true
echo -n "hardened : "; docker run --rm --entrypoint whoami spark-training/demo:hardened || true
