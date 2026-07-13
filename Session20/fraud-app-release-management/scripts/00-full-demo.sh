#!/usr/bin/env bash
# THE FULL DEMO: runs the entire Dev -> Stage -> Prod pipeline end to end,
# with a pause before each stage so an instructor can narrate what's about
# to happen. Mirrors exactly what .github/workflows/ci-cd.yml automates.
set -euo pipefail
cd "$(dirname "$0")/.."

pause() {
  echo ""
  read -r -p ">>> Press ENTER to continue: $1 ..." _
}

VERSION="${1:-1.0.0}"

echo "############################################################"
echo "# FRAUD APP RELEASE MANAGEMENT - FULL PIPELINE DEMO"
echo "############################################################"

pause "run unit tests (mvn test) - the CI 'build & test' gate"
mvn -B test

pause "start the local registry (ACR stand-in)"
bash scripts/03-start-local-registry.sh

pause "build the versioned Docker image"
bash scripts/01-build-and-version.sh "${VERSION}"
# shellcheck disable=SC1091
source .last-build.env

pause "run the Trivy security-scan gate (blocks on HIGH/CRITICAL)"
bash scripts/02-scan-image.sh "fraud-app:${IMAGE_TAG}"

pause "push the scanned image to the registry as :dev and :${IMAGE_TAG}"
bash scripts/04-push-dev.sh "${IMAGE_TAG}"

pause "promote dev -> stage (retag only, no rebuild)"
bash scripts/05-promote-stage.sh "${IMAGE_TAG}"

pause "deploy stage and smoke test it"
docker compose -f docker-compose.stage.yml up -d
sleep 3
if command -v curl >/dev/null 2>&1; then
  curl -sf http://localhost:8082/health && echo " -> stage healthy"
else
  echo "(curl not found on host - open http://localhost:8082/health in a browser to verify)"
fi

pause "promote stage -> prod-green (release candidate; prod-blue untouched)"
bash scripts/06-promote-prod.sh stage
# Bootstrap prod-blue on the very first run of this demo, so there's a
# stable version to compare against.
if ! docker pull localhost:5000/fraud-app:prod-blue >/dev/null 2>&1; then
  echo ">> No prod-blue yet - bootstrapping it from the same image for this first run."
  docker tag "localhost:5000/fraud-app:stage" "localhost:5000/fraud-app:prod-blue"
  docker push "localhost:5000/fraud-app:prod-blue"
fi

pause "deploy the Blue/Green pair behind Nginx, starting at 0% green"
bash scripts/07-deploy-blue-green.sh

pause "canary ramp: shift 10% of traffic to green"
bash scripts/08-canary-rollout.sh 10
sleep 2

pause "canary ramp: shift 50% of traffic to green"
bash scripts/08-canary-rollout.sh 50
sleep 2

pause "canary ramp: shift 100% of traffic to green (full cutover)"
bash scripts/08-canary-rollout.sh 100
sleep 2

pause "finalize: green becomes the new stable blue for the next release cycle"
bash scripts/10-finalize-promotion.sh

echo ""
echo "############################################################"
echo "# DEMO COMPLETE."
echo "# Try:  bash scripts/09-rollback.sh   (instant rollback demo)"
echo "############################################################"
