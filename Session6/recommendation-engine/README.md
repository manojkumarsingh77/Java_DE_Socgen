# Retail Banking Recommendation Engine Pre-Processor
## Performance Tuning Demo: P99 800ms → 200ms

---

## Business Requirement

A major retail bank (HDFC / Axis / ICICI style) requires that when a customer opens the mobile banking app, the home screen must display **personalised product recommendations** within **200ms (P99 SLA)**. Recommendations include: pre-approved loans, credit cards, mutual fund SIPs, and fixed deposits — all personalised to the customer's credit score, income, debt profile, and segment (RETAIL / PREMIER / WEALTH / SME).

**Current state:** Pre-processor P99 = **800ms** → customer sees a loading spinner → 23% drop in conversion rate.  
**Target:** P99 = **200ms** → instant personalised tiles → projected +18% conversion uplift.

---

## What This Demo Covers

| Concept | Where in Code | What You'll See |
|---------|--------------|-----------------|
| **CPU Profiling** | `ThreadDumpAnalyzer.printCpuProfile()` | Per-thread CPU ms, hot methods |
| **Thread Dumps** | `ThreadDumpAnalyzer.captureAndPrint()` | BLOCKED threads on scoringLock |
| **JMH Design** | `RecommendationBenchmark.java` | @State, @Warmup, @Fork, Blackhole |
| **False Sharing** | `FalseSharingDemo.java` | 6x speedup from cache-line padding |
| **IO Bottleneck** | `IOBottleneckDemo.java` | Disk vs Cache: 100-1000x difference |
| **SLA Tuning P95/P99** | `LatencyTracker.java` | HdrHistogram, coordinated omission fix |
| **Load Modeling** | `LoadModelingSimulator.java` | Little's Law, STEADY/RAMP/SPIKE profiles |

---

## Project Structure

```
recommendation-engine/
├── pom.xml
└── src/main/java/com/bank/recommendation/
    ├── RecommendationEngineDemo.java          ← MAIN ENTRY POINT (run this first)
    ├── model/
    │   ├── CustomerProfile.java               ← Domain model (shows false sharing problem)
    │   ├── PaddedCustomerProfile.java         ← Domain model (false sharing fix)
    │   └── CustomerDataGenerator.java         ← Load modeling: realistic synthetic data
    ├── engine/
    │   ├── SlowRecommendationEngine.java      ← BEFORE: all 5 bottlenecks present
    │   └── FastRecommendationEngine.java      ← AFTER: all optimisations applied
    ├── profiling/
    │   ├── FalseSharingDemo.java              ← Standalone false sharing demo
    │   └── ThreadDumpAnalyzer.java            ← Thread dump + CPU profiling
    ├── io/
    │   └── IOBottleneckDemo.java              ← 4 I/O strategies compared
    ├── benchmark/
    │   └── RecommendationBenchmark.java       ← JMH benchmarks
    └── tuning/
        ├── LatencyTracker.java                ← HdrHistogram P95/P99 tracking
        └── LoadModelingSimulator.java         ← Load modeling (STEADY/RAMP/SPIKE)
```

---

## Step-by-Step Instructions for IntelliJ + Java 17

### Step 1: Open the Project

1. Open IntelliJ IDEA (Community or Ultimate)
2. **File → Open** → select the `recommendation-engine` folder
3. IntelliJ detects `pom.xml` → click **"Load Maven Project"**
4. Wait for Maven to download dependencies (requires internet)
5. Verify: bottom status bar shows "Indexing complete"

### Step 2: Set Java 17 SDK

1. **File → Project Structure** (Ctrl+Alt+Shift+S)
2. Under **Project SDK**: click dropdown → select or add **Java 17**
   - If not listed: click "+ Add SDK" → "Download JDK" → version 17
3. **Project language level**: 17 (Pattern matching, records, sealed classes)
4. Click OK

### Step 3: Configure Run Configuration with JVM Flags

These JVM flags are critical for the False Sharing demo:

1. Open `RecommendationEngineDemo.java`
2. Right-click in the file → **"Modify Run Configuration"**
3. Under **"VM options"** add:
   ```
   -Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -XX:-RestrictContended
   ```
4. Click **Apply → OK**

> **Why `-XX:-RestrictContended`?**  
> The `@jdk.internal.vm.annotation.Contended` annotation (used in PaddedCustomerProfile)  
> is restricted by default to JDK internal classes. This flag removes that restriction.

### Step 4: Run the Main Demo

1. Open `RecommendationEngineDemo.java`
2. Click the green ▶ button next to `public static void main`
3. Watch the console output for all 7 demos

**Expected output (abridged):**
```
╔═══════════════════════════════════════════════════════════════════╗
║     RETAIL BANKING RECOMMENDATION ENGINE PRE-PROCESSOR           ║
║     Performance Tuning: P99  800ms  →  200ms                    ║
╚═══════════════════════════════════════════════════════════════════╝

████████████████████████████████████████████████████████████████████████
  DEMO 1 – Quick Latency Comparison: Slow vs Fast Engine
████████████████████████████████████████████████████████████████████████
...
╔══════════════════════════════════════════════════════════╗
║  LATENCY REPORT  ─  SLOW ENGINE                         ║
╠══════════════════════════════════════════════════════════╣
║  P50         │    45ms │   50ms │ ✅ PASS                ║
║  P95         │   180ms │  150ms │ ❌ FAIL                ║
║  P99         │   780ms │  200ms │ ❌ FAIL                ║
╚══════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════╗
║  LATENCY REPORT  ─  FAST ENGINE                         ║
╠══════════════════════════════════════════════════════════╣
║  P50         │    28ms │   50ms │ ✅ PASS                ║
║  P95         │   120ms │  150ms │ ✅ PASS                ║
║  P99         │   185ms │  200ms │ ✅ PASS                ║
╚══════════════════════════════════════════════════════════╝
```

---

### Step 5: Run the JMH Benchmark

**Option A – Run from IntelliJ (easiest):**
1. Open `RecommendationBenchmark.java`
2. Click ▶ next to `public static void main`
3. Add the same VM options as Step 3
4. Takes ~3-5 minutes; watch real-time results

**Option B – Build fat JAR and run:**
```bash
mvn package -DskipTests
java -jar target/recommendation-engine-benchmarks.jar \
     -wi 2 -i 3 -f 1 -rf text -rff jmh-results.txt
```

**Expected JMH output:**
```
Benchmark                                    Mode  Cnt   Score   Error  Units
RecommendationBenchmark.slowEngine_single    avgt    3  52.847 ± 3.241  ms/op
RecommendationBenchmark.fastEngine_single    avgt    3  27.193 ± 1.832  ms/op
RecommendationBenchmark.fastEngine_throughput thrpt  3   35.4  ± 2.1  ops/s
```

> The JMH score (avg time) is lower than P99 because benchmarks run single-threaded.  
> P99 impact is observed under concurrent load (Demo 6 – Load Modeling).

---

### Step 6: Take a Thread Dump While Slow Engine Runs

To see lock contention **visually** in IntelliJ:

1. Change line in `RecommendationEngineDemo.java`:
   ```java
   private static final int DEMO_CUSTOMERS = 10_000; // increase to slow it down
   ```
2. Run the demo
3. While Demo 1 (Slow Engine) is printing, go to IntelliJ toolbar:
   - **Run** menu → **"Get Thread Dump"** (or camera icon in Threads panel)
4. Look for threads showing:
   ```
   "pool-1-thread-3" BLOCKED on lock owned by "pool-1-thread-1"
       at com.bank.recommendation.engine.SlowRecommendationEngine.computeRiskScore
       waiting to lock <0x...> (java.util.concurrent.locks.ReentrantLock$NonfairSync)
   ```
5. During Fast Engine processing:
   - Zero BLOCKED threads
   - Threads show `ForkJoinPool.WorkQueue.runWork` in RUNNABLE state

---

### Step 7: Use IntelliJ CPU Profiler

1. Add this dependency to pom.xml or run via command line
2. In IntelliJ Ultimate: **Run → Profile** (Ctrl+Alt+F5)
3. Select **CPU Profiler → "Wall Clock"** (captures time including I/O waits)
4. Run Demo 1 and Demo 6
5. In the flame graph, look for:

**Slow Engine flame graph hot spots:**
```
ReentrantLock.lock()
  └── AbstractQueuedSynchronizer.acquire()
        └── LockSupport.park()   ← threads blocked here
```

**Fast Engine flame graph hot spots:**
```
FalseSharingDemo$BadScoringCounters.riskCount (volatile write)
FastRecommendationEngine.computeRiskScore() ← actual work
Math.min() / Math.max()
```

**For IntelliJ Community Edition**, use **async-profiler**:
```bash
# Download async-profiler
curl -L https://github.com/async-profiler/async-profiler/releases/latest/download/async-profiler-3.0-linux-x64.tar.gz | tar xz

# While demo is running:
./profiler.sh -e cpu -d 30 -f flamegraph.html $(jps | grep RecommendationEngineDemo | cut -d' ' -f1)

# Open flamegraph.html in browser
```

---

### Step 8: Enable Java Flight Recorder (JFR)

Add to VM options:
```
-XX:StartFlightRecording=duration=60s,filename=recording.jfr,settings=profile
```

Then open `recording.jfr` in:
- IntelliJ: File → Open → select .jfr (Ultimate only)
- JDK Mission Control: `jmc recording.jfr`

Look for:
- **jdk.FileRead** events > 1ms → IO bottleneck confirmed
- **jdk.ThreadPark** events → lock contention
- **jdk.GarbageCollection** events > 20ms → GC pressure

---

## Code Explanation by Concept

### 1. CPU Profiling
**File:** `ThreadDumpAnalyzer.java` → `printCpuProfile()`  
Uses `ThreadMXBean.getThreadCpuTime(threadId)` to get per-thread CPU time in nanoseconds. Combined with thread state (BLOCKED / RUNNABLE), this identifies:
- Which threads are burning CPU (high ns + RUNNABLE = real work)
- Which are wasting CPU slots (high ns + BLOCKED = waiting for lock)

In production, this is replaced by **async-profiler** or **IntelliJ Profiler** which give flame graphs showing the exact call stack consuming CPU.

### 2. Thread Dumps
**File:** `ThreadDumpAnalyzer.java` → `captureAndPrint()` and `demonstrateLockContention()`  
Uses `ThreadMXBean.dumpAllThreads()` to programmatically capture what `jstack <pid>` produces. Counts threads by state and shows blocked threads with their lock owner. The `demonstrateLockContention()` method starts 20 threads competing for one lock — exactly what `SlowRecommendationEngine` does — and dumps the state mid-run.

### 3. JMH Design
**File:** `RecommendationBenchmark.java`  
Five JMH concepts demonstrated:
- `@State(Scope.Benchmark)` — shared engine, separate per-thread profile index
- `@Warmup(iterations=2)` — JIT warm-up before measurement
- `@Fork(1)` with `-Xlog:gc` — clean JVM with GC logging
- `Blackhole.consume()` — prevents dead-code elimination of engine call results
- `@BenchmarkMode(Mode.AverageTime)` vs `Mode.Throughput` — different views of performance

### 4. False Sharing
**File:** `FalseSharingDemo.java`, `PaddedCustomerProfile.java`  
`BadScoringCounters`: 4 volatile longs packed adjacently → all 4 fit in 1-2 cache lines → writes by different threads invalidate each other.  
`PaddedScoringCounters`: 7 dummy longs before + 7 after each hot field → each field on its own 64-byte cache line → zero cross-thread cache invalidation.  
Measured speedup: typically 3-8x on 4+ core machines.

### 5. IO Bottleneck Analysis
**File:** `IOBottleneckDemo.java`  
Benchmarks 4 strategies (BufferedReader, readAllBytes, NIO FileChannel, Caffeine cache). At 5000 req/s, reading a 400-byte rules file from disk 5000 times/sec is the dominant cost. Caffeine cache reduces this to nanoseconds — the single biggest P99 improvement in this system.

### 6. SLA-Driven Tuning (P95/P99)
**File:** `LatencyTracker.java`  
Uses **HdrHistogram** (High Dynamic Range Histogram) instead of ArrayList to track latencies. Key features:
- `recordValueWithExpectedInterval()` implements **coordinated omission correction** — inserts phantom samples for requests that couldn't even start during a GC pause, giving *true* P99
- Constant memory regardless of request count (~30KB)
- Thread-safe without locks (CAS internally)
- Automatic SLA breach detection every 100 requests

### 7. Load Modeling
**File:** `LoadModelingSimulator.java`, `CustomerDataGenerator.java`  
Three concepts:
- **Open-loop scheduling**: uses `ScheduledExecutorService.scheduleAtFixedRate()` — requests arrive on wall-clock schedule, not after previous completes (correct model for HTTP APIs)
- **Little's Law**: `N = λ × W` → thread pool size calculation (see code comments)
- **Three load profiles**: STEADY (baseline), RAMP (morning rush), SPIKE (salary credit event)

---

## Root Causes and Fixes Summary

| # | Root Cause | Symptom | Fix | P99 Saving |
|---|-----------|---------|-----|-----------|
| 1 | Global `ReentrantLock` | 70% threads BLOCKED in thread dump | CompletableFuture parallel pipeline | ~350ms |
| 2 | Disk I/O on hot path | `FileInputStream.read()` hot in profiler | Caffeine in-memory rule cache | ~250ms |
| 3 | False sharing | Extra cache-line bouncing between cores | 128-byte padding on volatile fields | ~100ms |
| 4 | Object allocation storm | Frequent Minor GC (50-100ms pauses) | Records + pre-sized collections | ~50ms |
| 5 | Wrong thread pool size | Queue saturation under load | Little's Law sizing | ~50ms |
| **Total** | | **P99 = 800ms** | | **→ P99 = 200ms** |

---

## Dependencies (pom.xml)

| Library | Version | Purpose |
|---------|---------|---------|
| `jmh-core` | 1.37 | Benchmarking framework |
| `HdrHistogram` | 2.1.12 | P99 measurement with coordinated omission correction |
| `caffeine` | 3.1.8 | High-performance in-memory caching |
| `logback-classic` | 1.4.14 | Structured logging |
| `junit-jupiter` | 5.10.1 | Unit testing |
