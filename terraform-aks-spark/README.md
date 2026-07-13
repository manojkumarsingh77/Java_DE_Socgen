# AKS + Spark Fundamentals — Terraform

Provisions, in one `apply`:

- Resource Group — `rg-aks-spark-fundamentals-eus2`
- Log Analytics Workspace — `law-aks-fundamentals`
- Virtual Network `10.241.0.0/16` + subnet `snet-aks-nodes` `10.241.0.0/22`
- Azure Container Registry (Basic SKU)
- AKS cluster (`Free` tier, Azure CNI, 2x `Standard_D2s_v3`, manual scaling, Container Insights wired to the Log Analytics workspace)
- AcrPull role assignment from the AKS kubelet identity to the ACR (Terraform's equivalent of the portal's "Integrate with ACR" step)

All in **East US 2**, matching the manual steps you listed.

## Prerequisites (local machine)

1. **Terraform CLI** ≥ 1.5
   - macOS: `brew install terraform`
   - Windows: `choco install terraform` or download from terraform.io
2. **Azure CLI**
   - macOS: `brew install azure-cli`
   - Windows: `winget install Microsoft.AzureCLI`
3. Login and select the right subscription:
   ```bash
   az login
   az account set --subscription "npunext-1680261103285"
   az account show --query id -o tsv   # confirm — copy into terraform.tfvars
   ```

Terraform's `azurerm` provider uses your Azure CLI session for auth automatically (no service principal needed for a single-user run).

## Running it

```bash
cd terraform-aks-spark
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: set subscription_id, and change acr_name to something
# globally unique (e.g. acrsparkfundjs123)

terraform init
terraform plan -out tfplan
terraform apply tfplan
```

Deployment takes ~6-10 minutes (AKS cluster creation dominates), matching the portal's 5-8 minute wait.

## After apply

Fetch kubeconfig and confirm the cluster:

```bash
terraform output -raw aks_get_credentials_command | bash
kubectl get nodes
```

Confirm ACR is pullable from AKS:

```bash
kubectl run acrtest --rm -it --image=$(terraform output -raw acr_login_server)/hello-world:latest --restart=Never
```

## Tearing down

```bash
terraform destroy
```

## Notes / gotchas

- **ACR name uniqueness**: `acr_name` must be globally unique across all of Azure, lowercase alphanumeric only, 5-50 chars. The default in `variables.tf` will fail — override it in `terraform.tfvars`.
- **Service CIDR**: The AKS internal service CIDR (`10.242.0.0/16`) is deliberately outside the VNet's `10.241.0.0/16` range to avoid overlap, since Azure CNI (non-overlay) assigns pod IPs directly from the VNet subnet.
- **Kubernetes version**: left as `null` so AKS picks its current default in East US 2 at apply time, matching the portal step "leave the default selected." Pin it explicitly (e.g. `kubernetes_version = "1.29"`) once you've decided on a version for repeatable builds.
- **State file**: this config uses local state by default (`terraform.tfstate` in this folder). Fine for a personal training cluster; don't commit `terraform.tfstate` or `.terraform/` to git (add a `.gitignore`).
- **Idempotency**: re-running `terraform apply` after the first successful apply is a no-op unless you change a variable — this is the main advantage over manually clicking through the portal.

### Lab subscription policy compatibility (`UnextAzurePolicy_AllowOnlyWhitelistedServices`)

This config was adjusted after hitting `RequestDisallowedByPolicy` on `ContainerInsights(law-aks-fundamentals)`. Root cause: AKS's `oms_agent` block (the managed "Container Insights" monitoring add-on) provisions a legacy `Microsoft.OperationsManagement/solutions/ContainerInsights` resource, which is not on this subscription's whitelist — even though the Log Analytics Workspace itself is allowed.

Fix applied: the `oms_agent` block was removed from `azurerm_kubernetes_cluster.this`. The Log Analytics workspace is still created (it's independently whitelisted) but nothing wires AKS monitoring into it by default. An opt-in `azurerm_monitor_diagnostic_setting.aks` resource is included, gated by `enable_diagnostic_settings` (default `false`) — that's a different ARM resource type (`Microsoft.Insights/diagnosticSettings`) than the blocked one, and is the modern policy-safe way to stream AKS control-plane logs/metrics to Log Analytics. Set `enable_diagnostic_settings = true` in `terraform.tfvars` if you want to test it; leave it `false` for the guaranteed-clean base apply.

Everything else in this config was cross-checked against your lab's allowed-services list:
- AKS tier `Free` ✅, node size `Standard_D2s_v3` ✅ (matches `Standard_D2S_v3`)
- ACR SKU `Basic` ✅
- VNet, Load Balancer `Standard`, Log Analytics Workspace, RBAC role assignments — all ✅
- Region `East US 2` ✅

If you extend this config later (Event Hubs, Key Vault, Storage/ADLS Gen2, Application Insights, Managed Identity, Recovery Services Vault) stick to the SKUs listed as allowed — anything outside that whitelist will hit the same `RequestDisallowedByPolicy` error.

## IntelliJ IDEA setup for Azure + Terraform

Install these plugins via `Settings/Preferences → Plugins → Marketplace`:

1. **HashiCorp Terraform / HCL** (JetBrains-published) — syntax highlighting, autocomplete, inline `terraform validate`, and a "Run" gutter icon for `.tf` files so you can `init`/`plan`/`apply` straight from the editor.
2. **Azure Toolkit for IntelliJ** (Microsoft-published) — sign in with your Azure account inside IntelliJ, browse subscriptions/resource groups, and view AKS clusters, ACR registries, and Log Analytics workspaces without leaving the IDE. Useful for eyeballing what Terraform created.
3. *(Optional)* **Kubernetes** plugin (JetBrains-published) — lets you browse cluster resources (pods, deployments, services) directly from IntelliJ once you've run `az aks get-credentials`, which is handy when you deploy the Spark driver/executor pods this cluster is meant for.

### Wiring the Terraform plugin to a real binary

`Settings → Languages & Frameworks → Terraform` → point "Terraform executable" at your installed binary (`which terraform` on macOS, `where terraform` on Windows). After that, right-click any `.tf` file → **Terraform** → Init / Plan / Apply, or use the green gutter run icons.

### Azure Toolkit sign-in

`View → Tool Windows → Azure` → **Azure Sign In** → Device Login (simplest cross-platform option) → select subscription `npunext-1680261103285`. This is separate from your `az login` CLI session but can use the same account.
