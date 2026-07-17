# Cross-Region Failover Simulation — Retail Banking DR/HA Demo

Session 22 · Module: **Disaster Recovery & High Availability**
Topics: RPO/RTO modeling · Cross-region replication · Backup strategy · Failure drills

See **`LAB_GUIDE.md`** for the full step-by-step walkthrough. This file only
covers the raw run configuration.

## IntelliJ IDEA — Run Configuration

**Main class:** `com.retailbank.dr.Main`

### VM Options — macOS (Apple Silicon M1 Max)

```
-Xms2g -Xmx6g
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
-Djava.security.manager=allow
-Djava.net.preferIPv4Stack=true
```

*Notes for M1 Max:* no `-Dhadoop.home.dir` is required for local `local[*]` runs
since we use local filesystem paths under `java.io.tmpdir`, not `winutils`-dependent
HDFS APIs. `rocksdbjni` (pulled transitively if you later add Structured Streaming
state stores) ships a multi-arch fat jar (darwin-aarch64 included) from version 6.20+
— no classifier override needed. `netty-all` resolves its native `epoll`/`kqueue`
transports automatically per-platform; do not manually pin a Linux classifier.

### VM Options — Windows 11 (x64)

```
-Xms2g -Xmx6g
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
-Djava.security.manager=allow
-Dhadoop.home.dir=C:\hadoop
```

*Notes for Windows:* `-Dhadoop.home.dir` must point to a directory containing
`bin\winutils.exe` matching your Hadoop 3.3.x baseline, or Delta/Spark's local
filesystem `FileOutputCommitter` throws `NullPointerException` on `chmod` during
writes. Download winutils.exe for hadoop-3.3.6 and place it at `C:\hadoop\bin\winutils.exe`.
Also set environment variable `HADOOP_HOME=C:\hadoop` at the Windows OS level
(Run Configuration → Environment Variables) as a belt-and-suspenders fallback.

### Program arguments
None required — all configuration is in `AppConfig.defaultLocalDemo()`.
Edit that record directly to change batch counts, lag, or drill trigger point.

## Command-line run (either OS)

```bash
mvn clean package -DskipTests
java $VM_OPTIONS -cp target/cross-region-failover-simulation.jar com.retailbank.dr.Main
```

## Output

- Console: live replication log lines + final drill report table
- `${java.io.tmpdir}/dr-ha-spark-demo/reports/dr_drill_report_<epoch>.json`
- Delta tables under `${java.io.tmpdir}/dr-ha-spark-demo/delta/{primary,secondary}_txn_ledger`
