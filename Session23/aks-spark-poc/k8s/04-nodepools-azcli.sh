#!/usr/bin/env bash
# =============================================================================
# TOPIC: Node Pools + Autoscaler Internals (infrastructure provisioning)
# Run once to provision the AKS cluster and its 4 node pools.
# Fill in RG / CLUSTER_NAME / LOCATION before running.
# =============================================================================
set -euo pipefail

RG="rg-banking-spark-poc"
CLUSTER_NAME="aks-banking-poc"
LOCATION="centralindia"

# ---------------------------------------------------------------------------
# 1) Create the AKS cluster with the SYSTEM node pool + Calico network policy
#    + Cluster Autoscaler enabled on the system pool itself.
# ---------------------------------------------------------------------------
az aks create \
  --resource-group "$RG" \
  --name "$CLUSTER_NAME" \
  --location "$LOCATION" \
  --node-count 2 \
  --node-vm-size Standard_D4s_v5 \
  --nodepool-name systempool \
  --nodepool-tags "role=system" \
  --network-plugin azure \
  --network-plugin-mode overlay \
  --network-policy calico \
  --enable-cluster-autoscaler \
  --min-count 2 \
  --max-count 3 \
  --node-taints "CriticalAddonsOnly=true:NoSchedule" \
  --enable-addons azure-keyvault-secrets-provider \
  --enable-workload-identity \
  --enable-oidc-issuer \
  --generate-ssh-keys

# ---------------------------------------------------------------------------
# 2) DRIVER pool - isolated from executors so executor OOM-kills never evict
#    the Spark driver.
# ---------------------------------------------------------------------------
az aks nodepool add \
  --resource-group "$RG" \
  --cluster-name "$CLUSTER_NAME" \
  --name driverpool \
  --node-vm-size Standard_D8s_v5 \
  --mode User \
  --labels role=spark-driver \
  --node-count 2 \
  --enable-cluster-autoscaler \
  --min-count 1 \
  --max-count 4

# ---------------------------------------------------------------------------
# 3) DEV executor pool - small, aggressively autoscaled (min=1) to minimize
#    idle cost during off-hours.
# ---------------------------------------------------------------------------
az aks nodepool add \
  --resource-group "$RG" \
  --cluster-name "$CLUSTER_NAME" \
  --name devuserpool \
  --node-vm-size Standard_D8s_v5 \
  --mode User \
  --labels agentpool=dev-userpool env=dev \
  --node-taints "workload=dev-spark:NoSchedule" \
  --node-count 1 \
  --enable-cluster-autoscaler \
  --min-count 1 \
  --max-count 8

# ---------------------------------------------------------------------------
# 4) STAGING executor pool.
# ---------------------------------------------------------------------------
az aks nodepool add \
  --resource-group "$RG" \
  --cluster-name "$CLUSTER_NAME" \
  --name stagingpool \
  --node-vm-size Standard_D8s_v5 \
  --mode User \
  --labels agentpool=staging-userpool env=staging \
  --node-taints "workload=staging-spark:NoSchedule" \
  --node-count 2 \
  --enable-cluster-autoscaler \
  --min-count 2 \
  --max-count 8

# ---------------------------------------------------------------------------
# 5) PROD executor pool - memory-optimized SKU to absorb region-skewed
#    shuffle partitions without executor OOM.
# ---------------------------------------------------------------------------
az aks nodepool add \
  --resource-group "$RG" \
  --cluster-name "$CLUSTER_NAME" \
  --name prodhighmem \
  --node-vm-size Standard_E16s_v5 \
  --mode User \
  --labels agentpool=prodhighmem env=prod \
  --node-taints "workload=prod-spark:NoSchedule" \
  --node-count 4 \
  --enable-cluster-autoscaler \
  --min-count 4 \
  --max-count 32

echo "All node pools provisioned. Verify with: az aks nodepool list -g $RG --cluster-name $CLUSTER_NAME -o table"
