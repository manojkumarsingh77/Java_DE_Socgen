@echo off
cd /d "%~dp0.."
copy /Y config\vulnerability-findings-critical.json config\vulnerability-findings.json >nul
echo [inject-critical-vuln] config\vulnerability-findings.json now contains a CRITICAL finding.
echo [inject-critical-vuln] Run scripts\run-local.bat ci now to see CI blocked.
