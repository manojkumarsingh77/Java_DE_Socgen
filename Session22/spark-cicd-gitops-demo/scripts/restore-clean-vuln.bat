@echo off
cd /d "%~dp0.."
(
echo {
echo   "scanTool": "trivy (simulated)",
echo   "scannedAt": "runtime",
echo   "findings": [
echo     { "id": "CVE-2024-1111", "package": "libexample1", "severity": "MEDIUM" },
echo     { "id": "CVE-2024-2222", "package": "libexample2", "severity": "LOW" },
echo     { "id": "CVE-2024-3333", "package": "libexample3", "severity": "HIGH" }
echo   ]
echo }
) > config\vulnerability-findings.json
echo [restore-clean-vuln] config\vulnerability-findings.json restored to a clean/passing report.
