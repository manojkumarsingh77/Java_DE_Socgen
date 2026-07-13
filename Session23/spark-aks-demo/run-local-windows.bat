@echo off
REM Run FraudAggregationJob locally on Windows 11 x64 inside a plain JVM.
REM Prerequisite: HADOOP_HOME must point to a folder containing bin\winutils.exe + hadoop.dll
REM Usage: run-local-windows.bat   (run "mvn clean package" first)

set JAR=target\spark-aks-fraud-demo.jar
if not exist "%JAR%" (
  echo Building project first...
  call mvn -q -B clean package -DskipTests
)

if "%HADOOP_HOME%"=="" (
  echo WARNING: HADOOP_HOME is not set. Defaulting to C:\hadoop
  set HADOOP_HOME=C:\hadoop
)

java ^
  -Xms2g -Xmx6g ^
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
  --add-opens=java.base/sun.security.action=ALL-UNNAMED ^
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED ^
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED ^
  -Djava.security.manager=allow ^
  -Dspark.master=local[*] ^
  -Dhadoop.home.dir=%HADOOP_HOME% ^
  -cp "%JAR%" com.bank.spark.aks.FraudAggregationJob
