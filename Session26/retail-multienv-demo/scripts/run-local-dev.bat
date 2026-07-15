@echo off
REM =====================================================================================
REM run-local-dev.bat — Windows 11 (x64)
REM Runs the packaged jar locally against the 'dev' environment config.
REM Requires HADOOP_HOME to point at a directory containing winutils.exe + hadoop.dll
REM for the Hadoop version bundled with Spark 3.5.1 (Hadoop 3.3.x winutils build).
REM =====================================================================================
setlocal

cd /d "%~dp0\.."

if "%HADOOP_HOME%"=="" (
    echo ERROR: HADOOP_HOME is not set. See LAB_GUIDE.md "Windows Hadoop native binaries" section.
    exit /b 1
)

set RETAIL_SECRET_ADLS_RETAILPLATFORMDEVSA_ACCOUNT_KEY=not-needed-for-local-filesystem-output

call mvn -B clean package -DskipTests
if errorlevel 1 exit /b 1

java ^
  --add-opens=java.base/java.lang=ALL-UNNAMED ^
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED ^
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED ^
  --add-opens=java.base/java.io=ALL-UNNAMED ^
  --add-opens=java.base/java.net=ALL-UNNAMED ^
  --add-opens=java.base/java.nio=ALL-UNNAMED ^
  --add-opens=java.base/java.util=ALL-UNNAMED ^
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED ^
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED ^
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED ^
  --add-opens=java.base/sun.security.action=ALL-UNNAMED ^
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED ^
  --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED ^
  --add-opens=java.base/java.nio.charset=ALL-UNNAMED ^
  --add-opens=java.base/javax.security.auth=ALL-UNNAMED ^
  -Djdk.reflect.useDirectMethodHandle=false ^
  -Dretail.env=dev ^
  -Dhadoop.home.dir=%HADOOP_HOME% ^
  -Dio.netty.tryReflectionSetAccessible=true ^
  -Xms1g -Xmx4g ^
  -cp target\retail-multienv-demo.jar ^
  com.retailbank.dataplatform.Main

endlocal
