#!/usr/bin/env bash
# ============================================================================
# spark-submit: Cross-Region Failover Simulation -> AKS (Spark-on-Kubernetes, client-agnostic cluster mode)
# Prereqs:
#   - AKS cluster reachable via kubectl context, RBAC ServiceAccount `spark` bound
#     to the `spark-operator` (or edit) Role in the target namespace
#   - Image pushed to ACR and AKS granted AcrPull (az aks update --attach-acr)
# ============================================================================
set -euo pipefail

ACR_NAME="bankdracr"                       # <-- your Azure Container Registry name
IMAGE="${ACR_NAME}.azurecr.io/dr-ha-spark-demo:1.0.0"
NAMESPACE="data-platform-dr"
K8S_API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"

spark-submit \
  --master "k8s://${K8S_API_SERVER}" \
  --deploy-mode cluster \
  --name cross-region-failover-simulation \
  --class com.retailbank.dr.Main \
  \
  --conf spark.kubernetes.namespace="${NAMESPACE}" \
  --conf spark.kubernetes.container.image="${IMAGE}" \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  \
  --conf spark.driver.memory=2g \
  --conf spark.driver.cores=1 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --conf spark.dynamicAllocation.enabled=false \
  \
  --conf spark.kubernetes.driver.request.cores=1 \
  --conf spark.kubernetes.driver.limit.cores=2 \
  --conf spark.kubernetes.executor.request.cores=2 \
  --conf spark.kubernetes.executor.limit.cores=2 \
  \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  \
  --conf spark.kubernetes.driver.label.workload=dr-drill \
  --conf spark.kubernetes.executor.label.workload=dr-drill \
  --conf spark.kubernetes.node.selector.agentpool=dataeng \
  \
  --conf spark.kubernetes.driver.podTemplateFile=k8s/driver-pod-template.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=k8s/executor-pod-template.yaml \
  \
  local:///opt/dr-ha-demo/jars/app.jar
