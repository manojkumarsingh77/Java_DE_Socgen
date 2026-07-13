@echo off
REM Builds all three teaching images and prints a size comparison table.
cd /d "%~dp0.."

echo === Building Act 1: naive (single-stage) image ===
docker build -f docker\Dockerfile.naive -t spark-training/demo:naive .
if errorlevel 1 goto :eof

echo === Building Act 2: multi-stage image ===
docker build -f docker\Dockerfile.multistage -t spark-training/demo:multistage .
if errorlevel 1 goto :eof

echo === Building Act 3: hardened multi-stage image ===
docker build -f docker\Dockerfile.hardened -t spark-training/demo:hardened .
if errorlevel 1 goto :eof

echo.
echo === IMAGE SIZE COMPARISON ===
docker images spark-training/demo --format "table {{.Tag}}\t{{.Size}}\t{{.CreatedSince}}"

echo.
echo === USER the process runs as (naive vs hardened) ===
docker run --rm --entrypoint whoami spark-training/demo:naive
docker run --rm --entrypoint whoami spark-training/demo:hardened
