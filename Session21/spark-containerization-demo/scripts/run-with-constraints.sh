#!/bin/bash
# Runs the SAME hardened image under three different --memory/--cpus budgets so
# learners see ContainerDiagnostics report a different picture each time.
# This is the script to run live for the "Resource Constraints" topic.
set -e
cd "$(dirname "$0")/.."

IMAGE="spark-training/demo:hardened"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Image $IMAGE not found - building it first ..."
  docker build -f docker/Dockerfile.hardened -t "$IMAGE" .
fi

echo
echo "############################################################"
echo "# 1) Generous budget: 2 CPU / 2g memory"
echo "############################################################"
docker run --rm --memory=2g --cpus=2 \
  -e JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0" \
  "$IMAGE" diagnostics

echo
echo "############################################################"
echo "# 2) Realistic small pod: 1 CPU / 512m memory"
echo "############################################################"
docker run --rm --memory=512m --cpus=1 \
  -e JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:ActiveProcessorCount=1" \
  "$IMAGE" diagnostics

echo
echo "############################################################"
echo "# 3) Tight budget: 0.5 CPU / 256m memory - watch numbers shrink accordingly"
echo "############################################################"
docker run --rm --memory=256m --cpus=0.5 \
  -e JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:ActiveProcessorCount=1" \
  "$IMAGE" diagnostics

echo
echo "############################################################"
echo "# 4) Memory-pressure simulation under the tight budget"
echo "#    (open 'docker stats' in another terminal while this runs)"
echo "############################################################"
docker run --rm --memory=256m --cpus=0.5 \
  -e JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError" \
  -e MEM_STRESS_STEP_MB=20 -e MEM_STRESS_STOP_PERCENT=95 \
  "$IMAGE" stress-memory
