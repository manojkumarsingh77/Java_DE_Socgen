# Black Friday Surge Handling
### Operating Apache Spark on AKS Safely — Instructor-Led Code Demo

A dependency-free Java 17 console application that simulates four
Spark-on-AKS operational concepts through one running retail narrative:
a Black-Friday Spark Structured Streaming job (`checkout-events-aggregator`)
and its batch cousin (`order-fraud-scoring`).

**Topics covered (see `DESIGN_NOTES.md` for the full class/method map):**
1. Driver / Executor pods
2. Resource tuning (memory overhead, executor sizing, OOM simulation)
3. Pod Disruption Budgets
4. Rolling updates (surge cluster/node-image upgrade)

Each topic prints, in order: **Problem Statement → Requirement → Proposed
Solution → Live Demo → Key Takeaway** — designed to be projected during a
live training session, one topic at a time or all four back to back.

---

## Requirements

- **JDK 17 or newer** (that's it — this project has **zero third-party
  dependencies**, so there is nothing to download from Maven Central and
  no version-compatibility risk with any Spark/Kubernetes library).
- IntelliJ IDEA (Community or Ultimate) — or any editor/terminal, this is a
  plain Maven project.

Verified to build and run identically on:
- macOS (Apple Silicon, M1 Max) — Temurin/OpenJDK 17+
- Windows 10/11 — Temurin/OpenJDK 17+

## Run it in IntelliJ

1. Unzip the project.
2. **File → Open...** → select the unzipped `spark-aks-surge` folder.
3. IntelliJ detects the `pom.xml` and imports it as a Maven project.
   No internet access is required for this step — there are no
   dependencies to resolve.
4. Make sure **Project SDK** is set to Java 17+
   (**File → Project Structure → Project → SDK**).
5. Open `src/main/java/com/retail/sparkaks/Main.java`.
6. Click the green ▶ run icon next to `public static void main`, or
   right-click the file → **Run 'Main.main()'**.
7. Read the console output top to bottom, pairing it with `DESIGN_NOTES.md`.

### Running just one topic

`Main` accepts an optional single argument: `1`, `2`, `3`, or `4`.

In IntelliJ: **Run → Edit Configurations… → Program arguments** → enter `3`
to jump straight to the Pod Disruption Budget demo, for example.

From a terminal:
```bash
mvn package
java -jar target/spark-aks-surge-handling.jar 3
```

## Run it from the command line (no IntelliJ)

```bash
mvn compile exec:java -Dexec.mainClass=com.retail.sparkaks.Main
```
or, without even needing the exec plugin:
```bash
mvn package
java -jar target/spark-aks-surge-handling.jar
```
or with plain `javac`/`java` (works even without Maven installed):
```bash
find src -name "*.java" > sources.txt
javac -d out --release 17 @sources.txt
java -cp out com.retail.sparkaks.Main
```

## Project layout

```
spark-aks-surge/
├── pom.xml
├── README.md
├── DESIGN_NOTES.md                 <- class/method map per topic (read this while presenting)
└── src/main/java/com/retail/sparkaks/
    ├── Main.java                   <- entry point, runs all 4 topic demos in order
    ├── common/                     <- shared domain model used by every topic
    │   ├── Banner.java             (Problem/Requirement/Solution/Demo console formatting)
    │   ├── AksNode.java            (simplified AKS worker node)
    │   └── SparkPod.java           (driver/executor pod: requests, overhead, OOM state)
    ├── sparkpods/                  <- Topic 1: Driver / Executor Pods
    │   ├── SparkJob.java
    │   └── DriverExecutorDemo.java
    ├── resourcetuning/             <- Topic 2: Resource Tuning
    │   ├── ResourceTuningAdvisor.java
    │   └── ResourceTuningDemo.java
    ├── pdb/                        <- Topic 3: Pod Disruption Budgets
    │   ├── PodDisruptionBudget.java
    │   ├── EvictionController.java
    │   └── PdbDemo.java
    └── rollingupdate/              <- Topic 4: Rolling Updates
        ├── RollingUpgradeSimulator.java
        └── RollingUpdateDemo.java
```

## Why no real Spark / Kubernetes cluster is used

This is intentional and is itself a teaching decision: the goal of the
session is for every learner — regardless of laptop OS, cloud access, or
local Docker/Kubernetes setup — to see the *exact same*, reproducible
behavior the moment they hit Run. The simulated `AksNode`/`SparkPod` model
mirrors real Kubernetes semantics closely enough (requests/limits, taints,
cordon, the Eviction API's admission check, Spark's own memoryOverhead
formula) that the mental model transfers directly once learners are back
on a real AKS cluster with `kubectl` and the Azure CLI.
