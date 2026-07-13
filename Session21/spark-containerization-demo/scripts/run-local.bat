@echo off
REM Builds the fat jar and runs it directly (no Docker) - use this to demo the
REM BASELINE ("uncontainerized") behaviour before introducing Docker at all.
REM Usage: scripts\run-local.bat [diagnostics|job|stress-memory|stress-cpu|all]
cd /d "%~dp0.."

echo [run-local.bat] Building fat jar with Maven ...
call mvn -q clean package -DskipTests
if errorlevel 1 goto :eof

set MODE=%1
if "%MODE%"=="" set MODE=all

set JOPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED

echo [run-local.bat] Running mode = %MODE%
java %JOPTS% -jar target\spark-containerization-demo-1.0.0.jar %MODE%
