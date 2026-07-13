# Observability Demo — Java 17 + Spark + Kafka

Structured logging, correlation IDs, golden signals, Prometheus metrics, Zipkin tracing,
SLA dashboards, and alert-fatigue mitigation, wired into a real Kafka → Spark Structured
Streaming pipeline.

This README is the **fast path**: unzip → start infrastructure → open in IntelliJ → run.
For the full teaching deep-dive (why each design choice was made, workshop exercises,
deep dive on each topic, MTTR runbook), see **[`docs/Observability-Spark-Kafka-Training-Demo.md`](docs/Observability-Spark-Kafka-Training-Demo.md)**.

---

## 0. Prerequisites

| Tool | Version |
|---|---|
| JDK | **17** exactly (`java -version`) |
| IntelliJ IDEA | 2023.x+ (Community is fine) |
| Docker Desktop / Docker Engine | with Compose v2 (`docker compose version`) |
| RAM free for Docker | ~6 GB |

**Windows only:** Spark's local checkpointing needs `winutils.exe`. If you see a
`UnsatisfiedLinkError`/`NullPointerException` from `org.apache.hadoop.util.Shell` on
startup, download `winutils.exe` (Hadoop 3.3.x build) into `C:\hadoop\bin`, set
`HADOOP_HOME=C:\hadoop`, add `%HADOOP_HOME%\bin` to `PATH`, restart IntelliJ.

---

## 1. Unzip and stop any conflicting containers

```bash
unzip observability-demo.zip
cd observability-demo
```

If you already have standalone Kafka/Zookeeper containers running on ports `9092`/`2181`
(e.g. via plain `docker run`), stop them first — this project's Compose stack uses the
**same images and ports**, just with extra wiring for the observability tools:

```bash
docker ps                       # find the container names/IDs
docker stop <kafka-container> <zookeeper-container>
docker rm <kafka-container> <zookeeper-container>
```

## 2. Start the infrastructure

```bash
cd docker
docker compose up -d
docker compose ps               # wait until kafka/zookeeper show "healthy" (~30-60s)
```

This brings up: `zookeeper`, `kafka`, `kafka-exporter`, `zipkin`, `prometheus`,
`alertmanager`, `loki`, `promtail`, `grafana` — all pre-wired together.

Sanity-check in a browser:

| Service | URL |
|---|---|
| Zipkin | http://localhost:9411 |
| Prometheus | http://localhost:9090/targets |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 (login `admin`/`admin`, or browse anonymously) |

Grafana should already show a **"Order Pipeline - Golden Signals & SLA"** dashboard
under the **Observability Demo** folder (auto-provisioned) — it'll just be empty/flat
until you start the Java apps in the next step.

## 3. Open the project in IntelliJ

```
File → Open... → select the observability-demo/ folder (the one with pom.xml)
```

- Let the Maven importer finish (first run downloads Spark/Kafka/Brave/Micrometer/etc.
  from Maven Central — needs internet access, a few hundred MB).
- Confirm the Project SDK is Java 17: `File → Project Structure → Project → SDK`.
- The `logs/` folder is already included (empty) — it must exist before you run the
  apps, and it does.

## 4. Run it — Run configurations are already included

This project ships with two pre-configured IntelliJ Run Configurations
(`.idea/runConfigurations/`), so they appear directly in the **Run** dropdown at the top
of the IntelliJ window — no setup needed:

1. Select **`OrderStreamProcessor`** from the Run dropdown → click ▶ Run.
   Wait ~10–20s for Spark to initialize. You'll see JSON log lines on the console
   ending with `"...is now consuming from topic 'orders-topic'..."`.
2. Select **`OrderEventProducer`** from the Run dropdown → click ▶ Run.
   You'll immediately see JSON log lines like `"Producing order event amount=... "`.

Within a few seconds, `OrderStreamProcessor`'s console starts logging
`"Processing order event..."` / `"Successfully processed order event..."` — that's the
full pipeline working end-to-end.

> **If IntelliJ shows "module not specified" on either run config:** open
> `Run → Edit Configurations`, select the config, and pick the project's module from the
> **Module** dropdown (one click) — this can happen if IntelliJ named the imported
> Maven module differently than `observability-demo-spark-kafka`.

### Running manually instead (if you'd rather not use the bundled configs)

`Run → Edit Configurations → + → Application`, then:

| Field | OrderStreamProcessor | OrderEventProducer |
|---|---|---|
| Main class | `com.training.observability.consumer.OrderStreamProcessor` | `com.training.observability.producer.OrderEventProducer` |
| VM options | `-Dservice.name=order-stream-processor --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED` | `-Dservice.name=order-producer` |
| Working directory | `$PROJECT_DIR$` | `$PROJECT_DIR$` |

(The `--add-opens` flags are required for Spark 3.5 on Java 17+ — without them you'll see
`InaccessibleObjectException` at Spark startup.)

## 5. Explore the running demo

| Where | What to do |
|---|---|
| http://localhost:9090/graph | Run `sum(rate(orders_produced_total[1m]))` etc. — golden signals live |
| http://localhost:3000 | Open the SLA dashboard — traffic/errors/latency/saturation panels updating every 10s |
| http://localhost:9411 | Find a trace, click into it — see the `produce-order-event` → `send` → `process-order-event` span chain |
| Grafana → Explore → Loki | `{service="order-stream-processor"} \| json \| orderId="<id-from-zipkin>"` — correlated logs for one request |
| http://localhost:9093 | Alerts will appear here once you trigger failures (see below) |

**Trigger a failure / alert** (stop a service, or set `FAILURE_RATE`/`FAILURE_INJECTION_RATE`
as a VM option / env var to e.g. `0.4` and restart) — full instructions and an explained
worked example are in the deep-dive doc, section 6 and 8.

## 6. Shut everything down

```bash
# Stop the two Run configurations in IntelliJ (red square)
cd docker
docker compose down          # add -v to also wipe Prometheus/Grafana/Loki data volumes
```

---

## Project layout

```
observability-demo/
├── README.md                          <- you are here
├── docs/
│   └── Observability-Spark-Kafka-Training-Demo.md   <- full deep-dive + workshop guide
├── pom.xml
├── .idea/runConfigurations/            <- pre-built IntelliJ run configs
├── logs/                               <- JSON logs written here at runtime (Promtail reads this)
├── src/main/resources/logback.xml
├── src/main/java/com/training/observability/
│   ├── config/        (ObservabilityConfig, GoldenSignals)
│   ├── model/          (OrderEvent)
│   ├── util/            (CorrelationIdSupport)
│   ├── producer/        (OrderEventProducer)
│   └── consumer/        (OrderStreamProcessor)
└── docker/
    ├── docker-compose.yml
    ├── prometheus/ (prometheus.yml, alerts.yml)
    ├── alertmanager/ (alertmanager.yml)
    ├── loki/ (loki-config.yml)
    ├── promtail/ (promtail-config.yml)
    └── grafana/ (provisioning + dashboards)
```

## Configuration reference (env vars / -D system properties)

All optional — defaults already match the Docker stack above.

| Variable | Default | Used by | Purpose |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | both | Kafka broker address |
| `ORDERS_TOPIC` | `orders-topic` | both | Kafka topic name |
| `ZIPKIN_ENDPOINT` | `http://localhost:9411/api/v2/spans` | both | where spans are POSTed |
| `METRICS_PORT` | `8081` (producer) / `8082` (consumer) | both | Prometheus scrape port |
| `PRODUCE_INTERVAL_MS` | `400` | producer | base delay between sends |
| `FAILURE_INJECTION_RATE` | `0.0` | producer | 0.0–1.0 chance of a simulated production-side failure |
| `FAILURE_RATE` | `0.05` | consumer | 0.0–1.0 chance of a simulated downstream-timeout failure |
| `LATENCY_INJECTION_MAX_MS` | `0` | consumer | if >0, injects `random(0..N)` ms delay per record |
| `TRIGGER_INTERVAL_SECONDS` | `5` | consumer | Spark micro-batch trigger interval |
| `CHECKPOINT_LOCATION` | `./checkpoint/order-stream-processor` | consumer | Spark checkpoint dir |

## Troubleshooting

See **section 9** of `docs/Observability-Spark-Kafka-Training-Demo.md` for a full table
of symptom → cause → fix (port conflicts, missing `logs/` folder, two-SLF4J-bindings
warnings, Windows `winutils`, etc.).
