#!/usr/bin/env bash
set -euo pipefail

# Prerequisites:
#   1. az aks get-credentials --resource-group <rg> --name <aks-cluster-name>
#   2. Image pushed to ACR: az acr build --registry <acr-name> --image spark-aks-fraud-demo:1.0.0 .
#   3. ServiceAccount + RoleBinding granting the driver permission to create/watch/delete pods:
#        kubectl create serviceaccount spark-driver-sa -n spark-jobs
#        kubectl create rolebinding spark-driver-rb --clusterrole=edit \
#          --serviceaccount=spark-jobs:spark-driver-sa -n spark-jobs

K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')

/opt/spark/bin/spark-submit \
  --master k8s://${K8S_API_SERVER} \
  --deploy-mode cluster \
  --name retail-banking-fraud-aggregation \
  --class com.bank.spark.aks.FraudAggregationJob \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver-sa \
  --conf spark.kubernetes.container.image=<acr-name>.azurecr.io/spark-aks-fraud-demo:1.0.0 \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.kubernetes.driver.podTemplateFile=/opt/spark/work-dir/k8s/driver-pod-template.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=/opt/spark/work-dir/k8s/executor-pod-template.yaml \
  --conf spark.driver.memory=2g \
  --conf spark.driver.cores=1 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=6 \
  --conf spark.kubernetes.allocation.batch.size=3 \
  --conf spark.kubernetes.executor.deleteOnTermination=true \
  local:///opt/spark/jars/spark-aks-fraud-demo.jar
