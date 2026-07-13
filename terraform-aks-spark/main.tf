############################################
# Resource Group
############################################
resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

############################################
# Log Analytics Workspace (for AKS Container Insights)
############################################
resource "azurerm_log_analytics_workspace" "this" {
  name                = var.log_analytics_workspace_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = var.log_analytics_sku
  retention_in_days   = var.log_analytics_retention_days
  tags                = var.tags
}

############################################
# Virtual Network + Subnet
############################################
resource "azurerm_virtual_network" "this" {
  name                = var.vnet_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  address_space       = var.vnet_address_space
  tags                = var.tags
}

resource "azurerm_subnet" "aks_nodes" {
  name                 = var.subnet_name
  resource_group_name  = azurerm_resource_group.this.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = var.subnet_address_prefix
}

############################################
# Azure Container Registry
############################################
resource "azurerm_container_registry" "this" {
  name                = var.acr_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.acr_sku
  admin_enabled       = false
  tags                = var.tags
}

############################################
# AKS Cluster
############################################
resource "azurerm_kubernetes_cluster" "this" {
  name                = var.aks_cluster_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  dns_prefix          = var.aks_dns_prefix
  sku_tier            = var.aks_sku_tier
  kubernetes_version  = var.kubernetes_version

  default_node_pool {
    name           = "agentpool"
    vm_size        = var.node_vm_size
    node_count     = var.node_count
    vnet_subnet_id = azurerm_subnet.aks_nodes.id

    # Manual scaling, matches "Scale method: Manual, Node count: 2" from the portal steps.
    enable_auto_scaling = false

    upgrade_settings {
      max_surge = "10%"
    }

    tags = var.tags
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin    = "azure"
    load_balancer_sku = "standard"
    service_cidr      = var.aks_service_cidr
    dns_service_ip    = var.aks_dns_service_ip
  }

  # NOTE: the oms_agent block (AKS "Container Insights" managed monitoring
  # add-on) is intentionally OMITTED. Enabling it provisions a legacy
  # Microsoft.OperationsManagement/solutions/ContainerInsights resource,
  # which this subscription's Azure Policy ("UnextAzurePolicy_AllowOnlyWhitelistedServices")
  # rejects with RequestDisallowedByPolicy. The Log Analytics Workspace
  # itself is still created below and is on the whitelist — see
  # azurerm_monitor_diagnostic_setting.aks below for an opt-in, policy-safe
  # way to send AKS logs/metrics to it instead.

  tags = var.tags

  depends_on = [
    azurerm_subnet.aks_nodes
  ]
}

############################################
# Optional: AKS -> Log Analytics via Diagnostic Settings
# This is a DIFFERENT resource type (Microsoft.Insights/diagnosticSettings)
# than the blocked ContainerInsights solution, so it does not trip the same
# policy. It streams control-plane logs/metrics straight into the workspace
# without the managed Container Insights add-on. Disabled by default so the
# base apply is guaranteed to succeed; flip enable_diagnostic_settings to
# true once you've confirmed it's not also blocked in your tenant.
############################################
resource "azurerm_monitor_diagnostic_setting" "aks" {
  count = var.enable_diagnostic_settings ? 1 : 0

  name                       = "aks-diag-to-law"
  target_resource_id         = azurerm_kubernetes_cluster.this.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category = "kube-apiserver"
  }

  enabled_log {
    category = "kube-controller-manager"
  }

  enabled_log {
    category = "kube-scheduler"
  }

  metric {
    category = "AllMetrics"
  }
}

############################################
# ACR <-> AKS integration
# Grants the AKS cluster's kubelet identity the AcrPull role
# on the registry, which is the Terraform-native equivalent of the
# portal's "Integrate with Azure Container Registry" step.
############################################
resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                            = azurerm_container_registry.this.id
  role_definition_name             = "AcrPull"
  principal_id                     = azurerm_kubernetes_cluster.this.kubelet_identity[0].object_id
  skip_service_principal_aad_check = true
}
