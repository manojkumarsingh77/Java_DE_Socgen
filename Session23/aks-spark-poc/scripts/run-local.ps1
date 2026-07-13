# =============================================================================
# run-local.ps1 - Build and run the POC locally on Windows 11
# Usage: .\scripts\run-local.ps1 -EnvName dev
# =============================================================================
param(
    [string]$EnvName = "dev"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

$EnvFile = Join-Path $ProjectRoot "run-configs\.env"
if (Test-Path $EnvFile) {
    Write-Host "Loading environment variables from run-configs\.env"
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
        $parts = $_ -split '=', 2
        if ($parts.Length -eq 2) {
            [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
        }
    }
}

$env:APP_ENV = $EnvName
if (-not $env:POD_NAMESPACE) { $env:POD_NAMESPACE = "banking-$EnvName" }
if (-not $env:AZURE_MODE) { $env:AZURE_MODE = "false" }
if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop" }
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"

Write-Host "=============================================================="
Write-Host " Building fat jar via Maven Shade Plugin..."
Write-Host "=============================================================="
Set-Location $ProjectRoot
mvn -q clean package "-DskipTests"

Write-Host "=============================================================="
Write-Host " Running POC | APP_ENV=$EnvName | AZURE_MODE=$($env:AZURE_MODE)"
Write-Host "=============================================================="

$JavaOpts = @(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

java -Xms2g -Xmx6g @JavaOpts `
    "-DAPP_ENV=$EnvName" `
    "-Dhadoop.home.dir=$env:HADOOP_HOME" `
    -jar target\aks-spark-poc-shaded.jar

Write-Host "=============================================================="
Write-Host " Done. Check .\data\$EnvName\raw and .\data\$EnvName\curated"
Write-Host "=============================================================="
