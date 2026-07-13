@echo off
cd /d "%~dp0.."
for %%f in (.registry\*) do (
  if not "%%~nxf"==".gitkeep" del /q "%%f"
)
echo [reset-demo-state] .registry\ cleared. Next 'ci' run starts fresh at version 1.0.0.
