#!/bin/bash
# Swaps in a scan-report fixture containing a CRITICAL CVE, so the next `ci`
# run demonstrates the ACR-security-scanning gate blocking the pipeline.
set -e
cd "$(dirname "$0")/.."
cp config/vulnerability-findings-critical.json config/vulnerability-findings.json
echo "[inject-critical-vuln] config/vulnerability-findings.json now contains a CRITICAL finding."
echo "[inject-critical-vuln] Run './scripts/run-local.sh ci' (or mode=ci) now to see CI blocked."
