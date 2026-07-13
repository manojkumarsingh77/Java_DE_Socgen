# Copy this file to terraform.tfvars and fill in the real values.
# terraform.tfvars is gitignored by default convention — do NOT commit it if it
# ever contains secrets (it doesn't here, but keep the habit).

subscription_id = "51ca1e02-af60-4202-bf73-2d1b8c356a5d" # az account show --query id -o tsv

# Replace <init> with your initials — must be globally unique across all of Azure.
acr_name = "acrsparkdemo"
