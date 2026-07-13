#!/bin/bash
# ILLUSTRATIVE - the real-infrastructure version of CanaryReleaseManager.java.
set -e
VERSION="$1"
echo "Would progressively shift 10% -> 25% -> 50% -> 100% of traffic to $VERSION"
echo "at the load balancer / service mesh layer, watching an error-rate metric"
echo "between waves and reverting to 0% automatically if it exceeds threshold."
echo "See CanaryReleaseManager.java for the fully-working local simulation."
