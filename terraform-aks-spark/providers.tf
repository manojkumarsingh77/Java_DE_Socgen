terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.107"
    }
  }

  # Local state by default. If you want remote state (recommended for teams),
  # uncomment and configure an azurerm backend pointing to a storage account
  # you provision separately (chicken-and-egg problem, so keep it manual/local
  # for a fundamentals cluster like this one).
  #
  # backend "azurerm" {
  #   resource_group_name  = "rg-tfstate"
  #   storage_account_name = "sttfstateuniquename"
  #   container_name       = "tfstate"
  #   key                  = "aks-spark-fundamentals.tfstate"
  # }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }

  subscription_id = var.subscription_id
}
