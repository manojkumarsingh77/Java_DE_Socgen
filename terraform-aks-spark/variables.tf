variable "subscription_id" {
  description = "Azure subscription ID (npunext-1680261103285). Run `az account show --query id -o tsv` to get it."
  type        = string
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "East US 2"
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
  default     = "rg-aks-spark-fundamentals-eus2"
}

variable "log_analytics_workspace_name" {
  description = "Log Analytics workspace name."
  type        = string
  default     = "law-aks-fundamentals"
}

variable "log_analytics_sku" {
  description = "Log Analytics workspace SKU."
  type        = string
  default     = "PerGB2018"
}

variable "log_analytics_retention_days" {
  description = "Log Analytics data retention in days."
  type        = number
  default     = 30
}

variable "vnet_name" {
  description = "Virtual network name."
  type        = string
  default     = "vnet-aks-fundamentals"
}

variable "vnet_address_space" {
  description = "Address space for the VNet."
  type        = list(string)
  default     = ["10.241.0.0/16"]
}

variable "subnet_name" {
  description = "Subnet name for AKS nodes."
  type        = string
  default     = "snet-aks-nodes"
}

variable "subnet_address_prefix" {
  description = "Address prefix for the AKS node subnet."
  type        = list(string)
  default     = ["10.241.0.0/22"]
}

variable "acr_name" {
  description = <<-EOT
    Azure Container Registry name. Must be GLOBALLY unique, 5-50 chars,
    lowercase letters and numbers only (no dashes, no underscores).
    Replace <init> below with your initials, e.g. acrsparkfundjs.
  EOT
  type        = string
  default     = "acrsparkfundinit"

  validation {
    condition     = can(regex("^[a-z0-9]{5,50}$", var.acr_name))
    error_message = "acr_name must be 5-50 lowercase alphanumeric characters only."
  }
}

variable "acr_sku" {
  description = "ACR SKU tier."
  type        = string
  default     = "Basic"
}

variable "aks_cluster_name" {
  description = "AKS cluster name."
  type        = string
  default     = "aks-spark-fundamentals"
}

variable "aks_dns_prefix" {
  description = "DNS prefix for the AKS API server FQDN."
  type        = string
  default     = "aks-spark-fundamentals"
}

variable "aks_sku_tier" {
  description = "AKS control plane pricing tier: Free, Standard, or Premium."
  type        = string
  default     = "Free"
}

variable "kubernetes_version" {
  description = "Kubernetes version. Leave null to use AKS's current default version in the region."
  type        = string
  default     = null
}

variable "node_vm_size" {
  description = "VM size for the default (system) node pool."
  type        = string
  default     = "Standard_D2s_v3"
}

variable "node_count" {
  description = "Fixed node count for the default node pool (manual scaling, no autoscaler)."
  type        = number
  default     = 2
}

variable "aks_service_cidr" {
  description = "CIDR for Kubernetes internal services. Must NOT overlap with the VNet address space."
  type        = string
  default     = "10.242.0.0/16"
}

variable "aks_dns_service_ip" {
  description = "IP address within the service CIDR used for cluster DNS (kube-dns)."
  type        = string
  default     = "10.242.0.10"
}

variable "enable_diagnostic_settings" {
  description = <<-EOT
    If true, creates an azurerm_monitor_diagnostic_setting sending AKS
    control-plane logs/metrics to the Log Analytics workspace. Left false
    by default because this lab subscription's Azure Policy
    ("UnextAzurePolicy_AllowOnlyWhitelistedServices") has been observed
    blocking AKS's managed Container Insights add-on (a different, legacy
    resource type). Diagnostic settings are a different resource type and
    are expected to be policy-safe, but are left opt-in here so the base
    apply is guaranteed not to fail on a policy you haven't tested yet.
  EOT
  type        = bool
  default     = false
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default = {
    project     = "aks-spark-fundamentals"
    environment = "training"
    managed_by  = "terraform"
  }
}
