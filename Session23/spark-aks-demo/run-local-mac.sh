#!/usr/bin/env bash
# Run FraudAggregationJob locally on macOS (Apple Silicon M1 Max) inside a plain JVM.
# Usage: ./run-local-mac.sh   (run "mvn clean package" first)
set -euo pipefail

JAR=target/spark-aks-fraud-demo.jar
if [ ! -f "$JAR" ]; then
  echo "Building project first..."
  mvn -q -B clean package -DskipTests
fi

java \
  -Xms2g -Xmx6g \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -Djava.security.manager=allow \
  -Dspark.master=local[*] \
  -cp "$JAR" com.bank.spark.aks.FraudAggregationJob
