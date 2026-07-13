# Retail Banking Payment Streaming Demo
### Kafka + Spark Structured Streaming + Delta Lake, in IntelliJ, on Java 17

This is a 5-application demo that simulates a retail bank's real-time
payment pipeline and then runs an SRE-style incident investigation on top
of it - using the Kafka broker you already have running in Docker.

```
Customer  ->  App2 Producer  ->  Kafka topic  ->  App3 Spark Streaming
                                                     |  fraud check
                                                     |  payment gateway sim
                                                     v
                                              Delta Lake table
                                                /            \
                                   App5 Dashboard        App4 Incident
                                   (live view)           Investigator
                                                          (root cause)
```

---

## 0. What you already have

From your `docker ps` output:

| Container ID | Image                          | Port mapping  |
|---------------|---------------------------------|---------------|
| `7596bf1ccfa0` | `confluentinc/cp-kafka:7.5.0`   | `9092:9092`   |
| `f00729113dbc` | `confluentinc/cp-zookeeper:7.5.0` | `2181:2181` |

All commands below use the **container ID** `7596bf1ccfa0` for the broker.
If you've since recreated the container, run `docker ps` again and
swap in whatever ID (or container *name*) shows up.

---

## 1. Project layout

```
payment-streaming-demo/
├── pom.xml
├── README.md                     <- this file
└── src/main
    ├── resources/log4j2.properties
    └── java/com/bank/retail/streaming/
        ├── model/
        │   ├── PaymentOrderEvent.java        (Kafka message contract)
        │   └── ProcessedPaymentEvent.java     (Delta table row contract)
        ├── util/
        │   ├── SyntheticDataGenerator.java    (synthetic banking dataset)
        │   └── JsonUtil.java
        ├── app1/KafkaTopicSetupApp.java        <-- APP 1
        ├── app2/PaymentOrderProducerApp.java   <-- APP 2
        ├── app3/
        │   ├── PaymentStreamProcessorApp.java  <-- APP 3 (main pipeline)
        │   ├── PaymentGatewaySimulator.java    (fraud + payment logic)
        │   └── GoldenSignalsReporter.java      (latency/traffic/errors/saturation)
        ├── app4/IncidentInvestigatorApp.java   <-- APP 4
        └── app5/DashboardSummaryApp.java       <-- APP 5
```

Every file already contains extensive inline comments explaining **why**
each line is written the way it is - this README explains how to **run**
everything and ties it back to the business problem each app solves.

---

## 2. Import into IntelliJ

1. **File → Open** → select the `payment-streaming-demo` folder (the one
   containing `pom.xml`). IntelliJ will detect it as a Maven project.
2. When prompted, choose **"Load Maven project automatically"**.
3. **File → Project Structure → Project** → set **SDK = 17** and
   **Language level = 17**.
4. Wait for Maven to finish downloading dependencies (Spark, Delta, Kafka
   client, Jackson, Log4j2) - this can take a few minutes the first time.
   Watch the Maven tool window for "BUILD SUCCESS".

> **Note on this sandbox:** I wrote and carefully hand-verified every file
> in this project (including a full `javac` syntax pass), but I do **not**
> have network access to Maven Central in this environment, so I could not
> run an actual `mvn package` here. The very first thing to do once it's
> imported in IntelliJ is **`mvn clean compile`** (Maven tool window → your
> module → Lifecycle → compile) to confirm a clean build on your machine
> before running anything.

---

## 3. VM Options - REQUIRED for Apps 3, 4, 5 (Spark)

Java 17 locks down reflective access to JDK internals that Spark's engine
needs. Without these flags you'll hit errors like
`InaccessibleObjectException` or `IllegalAccessError` the moment Spark
starts up. **Apps 1 and 2 do NOT need this** (they're plain Kafka client
code, no Spark).

For **each** of `PaymentStreamProcessorApp`, `IncidentInvestigatorApp`,
`DashboardSummaryApp`:

1. **Run → Edit Configurations…**
2. Select (or create) the Application run config for that class.
3. Click **"Modify options" → "Add VM options"**.
4. Paste this entire block into the **VM options** field:

```
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false
```

5. Set **"Module"** to this project's module, and **"Main class"** to the
   correct class for that run config.
6. Click **Apply**.

(These are the same `--add-opens` flags Spark's own launch scripts add
automatically on Java 17+ - we're just doing manually what `spark-submit`
would otherwise do for you, since IntelliJ runs the JVM directly.)

---

## 4. Create the Kafka topics (Application 1)

You have two equivalent options - pick either, both produce the same
result. Do this **once** before running App2/App3.

### Option A - Docker CLI (fastest)
```bash
docker exec -it 7596bf1ccfa0 kafka-topics --create \
  --topic retail.payments.orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it 7596bf1ccfa0 kafka-topics --create \
  --topic retail.payments.dlq \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# verify:
docker exec -it 7596bf1ccfa0 kafka-topics --list --bootstrap-server localhost:9092
```

### Option B - run App1 from IntelliJ
Run **`KafkaTopicSetupApp.main()`** (no VM options needed). It connects to
`localhost:9092` and creates the same two topics idempotently (safe to
re-run). You should see:
```
Created topic 'retail.payments.orders' with 3 partitions, replication factor 1
Created topic 'retail.payments.dlq' with 1 partitions, replication factor 1
```

> If you see a connection error here, confirm `docker ps` still shows the
> Kafka container with port `9092:9092` mapped, and that nothing else on
> your machine is already bound to port 9092.

---

## 5. Run Application 2 - Payment Order Producer

Run **`PaymentOrderProducerApp.main()`** (no VM options needed).

It generates **200 fully-synthetic retail banking payment orders**
(see §8 below for exactly what the data looks like) and publishes them as
JSON to `retail.payments.orders`, one every 50-300ms. Console output looks
like:
```
[1/200] sent orderId=ORD-483921 correlationId=7e1b... -> partition=1 offset=0 amount=1542.30 channel=UPI
[2/200] sent orderId=ORD-119387 correlationId=2a9f... -> partition=0 offset=0 amount=89200.00 channel=CARD
...
Finished publishing 200 orders.
```

You can re-run this any time you want more traffic flowing (e.g. while
App3 is running, to watch it process live).

---

## 6. Run Application 3 - Spark Streaming Processor

Run **`PaymentStreamProcessorApp.main()`** **with the VM options from §3**.

This is the main pipeline. On startup you'll see:
```
Streaming query started (id=...). Waiting for orders...
```
Then, every 5 seconds (one micro-batch), structured log lines like:
```
14:32:05.102 INFO  [...] correlationId= GoldenSignalsReporter - GOLDEN_SIGNALS batchId=3 traffic=18 errorRatePct=11.1 avgLatencyMs=420 p95LatencyMs=3120 maxLatencyMs=4980 slaBreaches=2 throughputPerSec=3.6 saturated=false
14:32:05.090 WARN  [...] correlationId=7e1b3f... PaymentGatewaySimulator - SLA_BREACH orderId=ORD-552310 latencyMs=3120 thresholdMs=1000 merchantCategory=ECOMMERCE
```

**Open the Spark UI** at <http://localhost:4040> while this is running -
click the **"Structured Streaming"** tab to watch micro-batch durations,
input/processing rates, etc. live. This is the same UI a real Spark
operations team watches in production.

Leave this running. Open a **second** IntelliJ Run window and re-run App2
a couple of times so there's a steady stream of orders (and therefore a
real incident pattern) for Apps 4 and 5 to find.

---

## 7. Run Application 4 - Incident Investigator

After App3 has processed at least one or two batches (give it ~30-60
seconds with App2 producing), run **`IncidentInvestigatorApp.main()`**
**with the VM options from §3**.

This is the SRE root-cause walkthrough - it prints, step by step:
1. Overall payment status breakdown
2. SLA breaches grouped by merchant category and channel
3. The 10 individual slowest transactions (with `correlationId` you can
   grep App3's console output for)
4. Fraud-blocked transactions
5. An **automated root-cause hypothesis** - it will correctly call out
   `ECOMMERCE` as the category causing the slowdown, because that's the
   category `PaymentGatewaySimulator` deliberately injects slow calls into.

---

## 8. Run Application 5 - Live Dashboard

While App2/App3 are still running, run **`DashboardSummaryApp.main()`**
**with the VM options from §3**. It refreshes every 10 seconds:
```
------------------------------------------------------------------
 RETAIL BANKING PAYMENTS - LIVE GOLDEN SIGNALS DASHBOARD
 Window: last 120s   |   Refreshed at: 14:33:10.512
------------------------------------------------------------------
 TRAFFIC        : 64 payments
 SUCCESS        : 53
 FAILED         : 4
 FRAUD_BLOCKED  : 1
 ERROR RATE     : 7.8%
 AVG LATENCY    : 510 ms
 P95 LATENCY    : 3340 ms
 SLA BREACHES   : 6
 STATUS         : DEGRADED - investigate with App4 (IncidentInvestigatorApp)
------------------------------------------------------------------
```
Stop it with IntelliJ's red Stop button (it loops forever by design, like
a real dashboard).

---

## 9. The SRE story to actually demo

1. Start App3, then App2 - point at the **console logs** scrolling by.
2. Open **Spark UI → Structured Streaming** - show the live micro-batch
   metrics.
3. Run App5 next to it - point out `STATUS: DEGRADED` once enough traffic
   has flowed.
4. "We've been paged - payments are slow. Let's investigate" → run App4.
   Walk through Steps 1-5 of its output live - this is the punchline: the
   tool **tells you which category, with numbers, and gives you the exact
   correlationIds to trace**.
5. Pick one `correlationId` from App4's "Top 10 slowest" list, and `grep`
   it directly out of App3's console/log output (or redirect App3's run
   to a file ahead of time) to show the full life of that one transaction:
   fraud check → gateway call → SLA breach - all tied together by that one
   ID.

---

## 10. "Where is the method that solves the problem?"

| Business problem | Class | Method |
|---|---|---|
| Generate realistic synthetic retail-banking orders | `SyntheticDataGenerator` | `generateOrder()` |
| Create Kafka topics idempotently | `KafkaTopicSetupApp` | `createTopicIfMissing()` |
| Publish orders to Kafka | `PaymentOrderProducerApp` | `publishOrders()` |
| Wire the whole streaming pipeline | `PaymentStreamProcessorApp` | `runPipeline()` |
| Fraud check + simulated payment gateway call | `PaymentGatewaySimulator` | `processPayment()` |
| Compute the 4 Golden Signals per micro-batch | `GoldenSignalsReporter` | `report()` |
| SRE root-cause investigation | `IncidentInvestigatorApp` | `investigateIncident()` |
| Live dashboard refresh | `DashboardSummaryApp` | `renderDashboard()` |

---

## 11. The synthetic dataset (Retail Banking domain)

Every field is generated, not real. Example JSON message published to
Kafka by App2:
```json
{
  "correlationId": "7e1b3f2a-4c9d-4a11-9e35-0c1d2f3a4b5c",
  "orderId": "ORD-552310",
  "customerId": "CUST-48213",
  "customerName": "Ananya Iyer",
  "accountNumber": "418273XXXXXX5678",
  "ifscCode": "TRST0418273",
  "bankName": "TrustIndia Bank",
  "channel": "ECOMMERCE",
  "merchantCategory": "ECOMMERCE",
  "merchantName": "UrbanCart Online",
  "amount": 2899.0,
  "currency": "INR",
  "deviceId": "DEV-9f3a2b1c",
  "newDevice": false,
  "city": "Bengaluru",
  "orderTimestamp": 1751212345000
}
```
See `SyntheticDataGenerator.java` for the full generation rules (name
pools, fictional bank/merchant lists, amount distributions, fraud-trigger
rates).

---

## 12. Cleanup / reset between demo runs

```bash
# Delete the Delta table + streaming checkpoint so App3 starts fresh:
rm -rf spark-demo-data        # (run from the project root)

# Optionally reset the Kafka topics too:
docker exec -it 7596bf1ccfa0 kafka-topics --delete --topic retail.payments.orders --bootstrap-server localhost:9092
docker exec -it 7596bf1ccfa0 kafka-topics --delete --topic retail.payments.dlq --bootstrap-server localhost:9092
# then re-run App1 (or Option A commands) to recreate them.
```

---

## 13. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `InaccessibleObjectException` / `IllegalAccessError` on startup | Missing VM options - see §3. Re-check they're on the **Spark** run configs (3, 4, 5), not just saved but actually applied to the config you're running. |
| `UnknownTopicOrPartitionException` | Run App1 / the docker commands in §4 first. |
| Producer/consumer can't connect to `localhost:9092` | Confirm `docker ps` shows the Kafka container mapping `9092:9092`, and that you're not running IntelliJ inside a different container/VM where `localhost` doesn't reach your Docker host. |
| `NoSuchMethodError` / `NoClassDefFoundError` involving Jackson or Kafka classes | A dependency version drifted. Re-check `pom.xml` properties match this README, then **File → Invalidate Caches / Restart** in IntelliJ and let Maven re-resolve. |
| On Windows: errors mentioning `winutils.exe` or `HADOOP_HOME` | Spark's Hadoop libraries expect a tiny `winutils.exe` shim even for purely local filesystem use. Download a Hadoop 3.x `winutils.exe` matching your Hadoop version, place it under `C:\hadoop\bin\winutils.exe`, and set environment variable `HADOOP_HOME=C:\hadoop` before launching IntelliJ. |
| Delta write fails with a path/permission error | Make sure the IntelliJ run configuration's **"Working directory"** is the project root (the default) - `DELTA_PATH`/`CHECKPOINT_PATH` are both built relative to `user.dir`. |
| App4/App5 print "No data found yet" | App3 hasn't completed a non-empty micro-batch yet. Make sure App2 has actually sent orders **after** App3 started (App3 uses `startingOffsets=latest`), then wait one 5-second trigger interval. |
| Port `4040` (Spark UI) already in use | You have another Spark app already running from an earlier test - stop it first, or Spark will silently bind UI to `4041`, etc. - check the App3 startup logs for the actual bound port. |
