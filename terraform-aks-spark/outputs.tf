output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "aks_cluster_name" {
  value = azurerm_kubernetes_cluster.this.name
}

output "aks_get_credentials_command" {
  description = "Run this locally to fetch kubeconfig for kubectl/spark-submit."
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.this.name} --name ${azurerm_kubernetes_cluster.this.name} --overwrite-existing"
}

output "acr_login_server" {
  value = azurerm_container_registry.this.login_server
}

output "acr_name" {
  value = azurerm_container_registry.this.name
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.this.workspace_id
}

output "vnet_id" {
  value = azurerm_virtual_network.this.id
}

output "aks_node_subnet_id" {
  value = azurerm_subnet.aks_nodes.id
}

output "aks_kube_config_host" {
  value     = azurerm_kubernetes_cluster.this.kube_config.0.host
  sensitive = true
}
