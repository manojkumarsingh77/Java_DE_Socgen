import { useState } from "react";
import CodeBlock from "./components/CodeBlock";
import AQEDiagram from "./components/AQEDiagram";
import JoinDiagram from "./components/JoinDiagram";
import BucketingDiagram from "./components/BucketingDiagram";
import SLADashboard from "./components/SLADashboard";
import { files } from "./data/javaCode";

// ─── Tab definitions ────────────────────────────────────────────────────────

type TabId =
  | "overview"
  | "join-dag"
  | "aqe"
  | "bucketing"
  | "sla"
  | "code"
  | "setup";

interface Tab {
  id: TabId;
  label: string;
  icon: string;
}

const TABS: Tab[] = [
  { id: "overview",  label: "Overview",           icon: "🏠" },
  { id: "join-dag",  label: "6-Way Join DAG",     icon: "🔗" },
  { id: "aqe",       label: "AQE Internals",      icon: "⚡" },
  { id: "bucketing", label: "Bucketing Strategy", icon: "🪣" },
  { id: "sla",       label: "SLA Dashboard",      icon: "⏱️" },
  { id: "code",      label: "Java Source Code",   icon: "☕" },
  { id: "setup",     label: "IntelliJ Setup",     icon: "🛠️" },
];

// ─── File list for code tab ──────────────────────────────────────────────────
const FILE_KEYS = Object.keys(files);

// ─── Overview section ────────────────────────────────────────────────────────
function OverviewSection() {
  const concepts = [
    {
      title: "AQE Internals",
      icon: "⚡",
      color: "from-blue-600/20 to-blue-800/20 border-blue-500/30",
      titleColor: "text-blue-400",
      points: [
        "AdaptiveSparkPlanExec wraps the entire query",
        "Execute → Collect MapOutputStatistics → Re-plan loop",
        "Dynamic partition coalesce (N small → M large partitions)",
        "Runtime SMJ → BHJ conversion (actual size < threshold)",
        "Skew join splitting: hot partitions → sub-partitions",
        "SMJ → SHJ when all partitions < maxShuffledHashJoinLocalMapThreshold",
      ],
    },
    {
      title: "Partition Strategy",
      icon: "📂",
      color: "from-purple-600/20 to-purple-800/20 border-purple-500/30",
      titleColor: "text-purple-400",
      points: [
        "Partition by low-cardinality columns (segment, country, date)",
        "Bucket by high-cardinality join keys (customer_id)",
        "advisoryPartitionSizeInBytes = 128 MB (tuned for 64 GB executors)",
        "minPartitionNum = 50 (prevents over-coalescing on big clusters)",
        "maxPartitionBytes = 128 MB (controls scan parallelism)",
        "openCostInBytes = 8 MB (small-file cost model for S3/HDFS)",
      ],
    },
    {
      title: "Bucketing vs Partitioning",
      icon: "🪣",
      color: "from-emerald-600/20 to-emerald-800/20 border-emerald-500/30",
      titleColor: "text-emerald-400",
      points: [
        "Partitioning = directories per value → filter pruning",
        "Bucketing = N fixed files per key hash → join without shuffle",
        "Both tables must bucket on SAME key with COMPATIBLE counts",
        "Compatible = same count OR one is an integer multiple",
        "Bucket count = nextPowerOf2(tableSizeGB / targetFileSizeGB)",
        "Re-bucketing requires full table rewrite (write-time decision)",
      ],
    },
    {
      title: "Join Strategy Selection",
      icon: "🔗",
      color: "from-orange-600/20 to-orange-800/20 border-orange-500/30",
      titleColor: "text-orange-400",
      points: [
        "BHJ: smaller side < autoBroadcastJoinThreshold (fastest, no shuffle)",
        "SHJ: uniform small partitions after coalesce (skip sort phase)",
        "SMJ: default for large-large joins (sort both sides, merge)",
        "SMJ + bucketing: co-located reads → zero shuffle Exchange",
        "MERGE / BROADCAST / SHUFFLE_HASH hints → advisory only",
        "AQE overrides all hints at runtime with real statistics",
      ],
    },
  ];

  const pipeline = [
    { num: "①", label: "customers",       rows: "500M", size: "25GB",  strategy: "Anchor",      color: "bg-blue-600",    icon: "👤" },
    { num: "②", label: "× orders",        rows: "2B",   size: "200GB", strategy: "SMJ-Bucket",  color: "bg-purple-600",  icon: "🛒" },
    { num: "③", label: "+ products",      rows: "10M",  size: "480MB", strategy: "BHJ",         color: "bg-emerald-600", icon: "📦" },
    { num: "④", label: "+ web_events",    rows: "5B",   size: "480GB", strategy: "SMJ+AQE",     color: "bg-orange-600",  icon: "🌐" },
    { num: "⑤", label: "+ tickets",       rows: "200M", size: "18GB",  strategy: "SHJ",         color: "bg-rose-600",    icon: "🎫" },
    { num: "⑥", label: "+ loyalty",       rows: "300M", size: "22GB",  strategy: "SMJ-Bucket",  color: "bg-yellow-600",  icon: "🏆" },
  ];

  return (
    <div className="space-y-8">
      {/* Hero */}
      <div className="text-center py-8 px-4">
        <div className="inline-flex items-center gap-2 bg-orange-500/10 border border-orange-500/30 
                        rounded-full px-4 py-1.5 text-orange-400 text-xs font-medium mb-4">
          ☕ Java 17 · Apache Spark 3.5.1 · Maven · IntelliJ IDEA
        </div>
        <h1 className="text-3xl md:text-4xl font-extrabold text-white leading-tight">
          Customer 360 — 6-Way Join Model
        </h1>
        <p className="text-slate-400 text-base mt-2 max-w-2xl mx-auto">
          Production-grade Spark pipeline joining <strong className="text-white">6 domain tables</strong> under a{" "}
          <strong className="text-emerald-400">30-minute SLA</strong> — covering AQE internals,
          partition strategy modelling, bucketing vs partitioning tradeoffs, and join strategy selection.
        </p>

        {/* Pipeline mini-flow */}
        <div className="mt-6 flex flex-wrap items-center justify-center gap-2">
          {pipeline.map((s, i) => (
            <div key={s.label} className="flex items-center gap-1">
              <div className={`${s.color} text-white rounded-xl px-3 py-2 text-center min-w-[90px] shadow-lg`}>
                <div className="text-lg">{s.icon}</div>
                <div className="text-[10px] font-bold">{s.label}</div>
                <div className="text-white/70 text-[9px]">{s.rows} / {s.size}</div>
                <div className="mt-1 bg-white/20 rounded px-1 text-[8px] font-mono">{s.strategy}</div>
              </div>
              {i < pipeline.length - 1 && (
                <div className="text-slate-500 text-xl font-light">▶</div>
              )}
            </div>
          ))}
          <div className="flex items-center gap-1">
            <div className="text-slate-500 text-xl">▶</div>
            <div className="bg-gradient-to-br from-yellow-500 to-amber-600 text-white rounded-xl 
                            px-3 py-2 text-center min-w-[90px] shadow-lg border border-yellow-400/30">
              <div className="text-lg">🏅</div>
              <div className="text-[10px] font-bold">C360 GOLD</div>
              <div className="text-white/70 text-[9px]">KPIs aggregated</div>
            </div>
          </div>
        </div>
      </div>

      {/* Concept cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
        {concepts.map((c) => (
          <div
            key={c.title}
            className={`bg-gradient-to-br ${c.color} rounded-2xl p-5 border`}
          >
            <div className="flex items-center gap-2 mb-3">
              <span className="text-2xl">{c.icon}</span>
              <h3 className={`font-bold text-base ${c.titleColor}`}>{c.title}</h3>
            </div>
            <ul className="space-y-1.5">
              {c.points.map((pt) => (
                <li key={pt} className="flex items-start gap-2 text-xs text-slate-300">
                  <span className="text-slate-500 shrink-0 mt-0.5">›</span>
                  <span>{pt}</span>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>

      {/* What's in the code */}
      <div className="bg-slate-900/60 rounded-2xl p-6 border border-slate-700">
        <h3 className="text-white font-bold text-base mb-4">📁 Project File Map</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {[
            { file: "Customer360App.java",            desc: "Main entry point — pipeline orchestration", pkg: "root" },
            { file: "SparkSessionConfig.java",        desc: "All AQE + performance SparkConf settings", pkg: "config" },
            { file: "DataGenerator.java",             desc: "Synthetic 6-table generator with skew", pkg: "generator" },
            { file: "PartitionStrategy.java",         desc: "Bucketing + partitioning strategy per table", pkg: "partition" },
            { file: "JoinStrategySelector.java",      desc: "5 hop joins with hints + AQE commentary", pkg: "join" },
            { file: "AQEMonitor.java",                desc: "SparkListener + plan introspection", pkg: "aqe" },
            { file: "Customer360Pipeline.java",       desc: "Phase-by-phase pipeline execution", pkg: "pipeline" },
            { file: "MetricsLogger.java",             desc: "SLA phase tracker + report", pkg: "util" },
            { file: "BucketingVsPartitioningDemo.java", desc: "5 standalone test scenarios", pkg: "demo" },
            { file: "pom.xml",                        desc: "Maven — Spark 3.5.1, Java 17, shade plugin", pkg: "build" },
            { file: "README_IntelliJ.md",             desc: "Complete IntelliJ setup guide + VM args", pkg: "docs" },
          ].map((f) => (
            <div key={f.file} className="bg-slate-800/60 rounded-xl p-3 border border-slate-700/50">
              <div className="text-yellow-400 font-mono text-xs font-semibold truncate">{f.file}</div>
              <div className="text-slate-500 text-[10px] uppercase tracking-wider mt-0.5">{f.pkg}</div>
              <div className="text-slate-400 text-xs mt-1">{f.desc}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Setup Guide ─────────────────────────────────────────────────────────────
function SetupSection() {
  const vmArgs = `--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.invoke=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/java.net=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.util.concurrent=ALL-UNNAMED
--add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/sun.nio.cs=ALL-UNNAMED
--add-opens java.base/sun.security.action=ALL-UNNAMED
--add-opens java.base/sun.util.calendar=ALL-UNNAMED
-Dlog4j2.formatMsgNoLookups=true
-Ddemo.scale=0.001
-DSPARK_LOCAL_IP=127.0.0.1`;

  const submitCmd = `mvn clean package -DskipTests

spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --num-executors 20 \\
  --executor-cores 16 \\
  --executor-memory 48g \\
  --driver-memory 8g \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.skewJoin.enabled=true \\
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.maxExecutors=40 \\
  --class com.customer360.spark.Customer360App \\
  target/customer360-spark-1.0-SNAPSHOT-uber.jar`;

  const steps = [
    {
      num: "01",
      title: "Install Prerequisites",
      color: "border-blue-500/40",
      numColor: "text-blue-400",
      content: (
        <div className="space-y-2 text-sm">
          {[
            { item: "JDK 17", note: "Amazon Corretto 17 or Eclipse Temurin 17 (free)", link: "https://adoptium.net" },
            { item: "Maven 3.9+", note: "Apache Maven (or use IntelliJ bundled Maven)", link: "https://maven.apache.org" },
            { item: "IntelliJ IDEA", note: "Community or Ultimate (2023.3+ recommended)" },
            { item: "RAM ≥ 16 GB", note: "Driver + executors run in-process on local[*]" },
            { item: "Disk ≥ 20 GB", note: "For Maven cache + Spark warehouse output" },
          ].map((r) => (
            <div key={r.item} className="flex gap-3 items-start">
              <span className="text-emerald-400 shrink-0">✔</span>
              <div>
                <span className="text-white font-semibold">{r.item}</span>
                <span className="text-slate-400"> — {r.note}</span>
              </div>
            </div>
          ))}
        </div>
      ),
    },
    {
      num: "02",
      title: "Create Project Structure",
      color: "border-purple-500/40",
      numColor: "text-purple-400",
      content: (
        <CodeBlock lang="text" maxH="240px" code={`customer360-spark/
├── pom.xml
└── src/
    └── main/
        └── java/
            └── com/customer360/spark/
                ├── Customer360App.java
                ├── aqe/
                │   └── AQEMonitor.java
                ├── config/
                │   └── SparkSessionConfig.java
                ├── demo/
                │   └── BucketingVsPartitioningDemo.java
                ├── generator/
                │   └── DataGenerator.java
                ├── join/
                │   └── JoinStrategySelector.java
                ├── partition/
                │   └── PartitionStrategy.java
                ├── pipeline/
                │   └── Customer360Pipeline.java
                └── util/
                    └── MetricsLogger.java`} />
      ),
    },
    {
      num: "03",
      title: "Import into IntelliJ",
      color: "border-orange-500/40",
      numColor: "text-orange-400",
      content: (
        <div className="space-y-2 text-sm text-slate-300">
          <div className="flex gap-2"><span className="text-orange-400 shrink-0">1.</span> <span><strong className="text-white">File → Open</strong> → select the <code className="bg-slate-700 px-1 rounded text-yellow-300">customer360-spark</code> folder</span></div>
          <div className="flex gap-2"><span className="text-orange-400 shrink-0">2.</span> <span>IntelliJ detects <code className="bg-slate-700 px-1 rounded text-yellow-300">pom.xml</code> → click <strong className="text-white">Trust Project</strong></span></div>
          <div className="flex gap-2"><span className="text-orange-400 shrink-0">3.</span> <span>Wait for Maven sync — downloads ~800 MB of Spark 3.5.1 JARs (first time)</span></div>
          <div className="flex gap-2"><span className="text-orange-400 shrink-0">4.</span> <span>Set <strong className="text-white">Project SDK</strong> to Java 17: <em>File → Project Structure → SDK</em></span></div>
          <div className="flex gap-2"><span className="text-orange-400 shrink-0">5.</span> <span>Enable annotation processing if you add Lombok later: <em>Settings → Build → Compiler → Annotation Processors</em></span></div>
        </div>
      ),
    },
    {
      num: "04",
      title: "Configure Run/Debug (VM Options)",
      color: "border-yellow-500/40",
      numColor: "text-yellow-400",
      content: (
        <div className="space-y-3">
          <div className="text-slate-400 text-sm">
            Go to <strong className="text-white">Run → Edit Configurations → + → Application</strong>
            <br />
            <strong className="text-white">Main class:</strong>{" "}
            <code className="text-yellow-300 bg-slate-700/50 px-1 rounded">com.customer360.spark.Customer360App</code>
          </div>
          <div className="text-slate-400 text-xs mb-1">Paste into <strong className="text-white">VM Options</strong> field:</div>
          <CodeBlock lang="bash" maxH="260px" code={vmArgs} />
          <div className="text-slate-400 text-xs mt-2">
            Add <strong className="text-white">Environment Variables:</strong>{" "}
            <code className="text-yellow-300 bg-slate-700/50 px-1 rounded">SPARK_LOCAL_IP=127.0.0.1</code>{" "}
            <code className="text-yellow-300 bg-slate-700/50 px-1 rounded ml-1">HADOOP_USER_NAME=spark</code>
          </div>
        </div>
      ),
    },
    {
      num: "05",
      title: "Scale Factor",
      color: "border-emerald-500/40",
      numColor: "text-emerald-400",
      content: (
        <div className="overflow-x-auto">
          <table className="w-full text-sm border-collapse min-w-[480px]">
            <thead>
              <tr className="bg-slate-800">
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">JVM Flag</th>
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Customers</th>
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Orders</th>
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Est. Time</th>
              </tr>
            </thead>
            <tbody>
              {[
                { flag: "-Ddemo.scale=0.001", cust: "500 K", orders: "2 M",   time: "~3 min (laptop)",   highlight: true },
                { flag: "-Ddemo.scale=0.01",  cust: "5 M",   orders: "20 M",  time: "~15 min",          highlight: false },
                { flag: "-Ddemo.scale=0.1",   cust: "50 M",  orders: "200 M", time: "~90 min (cluster)", highlight: false },
                { flag: "-Ddemo.scale=1.0",   cust: "500 M", orders: "2 B",   time: "~25-30 min (prod)", highlight: false },
              ].map((r) => (
                <tr key={r.flag} className={r.highlight ? "bg-emerald-900/20" : ""}>
                  <td className="py-2 px-3 font-mono text-yellow-300 text-xs">{r.flag}</td>
                  <td className="py-2 px-3 text-white text-xs">{r.cust}</td>
                  <td className="py-2 px-3 text-slate-300 text-xs">{r.orders}</td>
                  <td className={`py-2 px-3 text-xs ${r.highlight ? "text-emerald-400 font-semibold" : "text-slate-400"}`}>{r.time}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ),
    },
    {
      num: "06",
      title: "spark-submit (Cluster Mode)",
      color: "border-rose-500/40",
      numColor: "text-rose-400",
      content: <CodeBlock lang="bash" maxH="320px" code={submitCmd} />,
    },
    {
      num: "07",
      title: "Spark UI — What to Look For",
      color: "border-teal-500/40",
      numColor: "text-teal-400",
      content: (
        <div className="space-y-3 text-sm">
          <div className="text-slate-400">Open <strong className="text-white">http://localhost:4040</strong> while the job runs:</div>
          {[
            { tab: "SQL tab", look: "AdaptiveSparkPlan isFinalPlan=true — AQE finished re-optimising. Look for BroadcastHashJoin replacing SortMergeJoin." },
            { tab: "Stages tab", look: "Compare Shuffle Write bytes for HOP 1 (should be near 0 after bucketing) vs HOP 3 (will have shuffle for web_events)." },
            { tab: "Storage tab", look: "customers and products DataFrames cached in memory. Monitor fraction cached and serialized size." },
            { tab: "Executors tab", look: "Task distribution should be uniform after AQE skew handling. Watch for any single executor with 5× more GC time." },
            { tab: "Environment tab", look: "Confirm all AQE configs are set correctly (spark.sql.adaptive.* keys)." },
          ].map((u) => (
            <div key={u.tab} className="flex gap-3 items-start">
              <span className="bg-teal-700 text-white text-[10px] font-bold px-2 py-0.5 rounded shrink-0 mt-0.5">{u.tab}</span>
              <span className="text-slate-400 text-xs">{u.look}</span>
            </div>
          ))}
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="text-center">
        <h3 className="text-lg font-bold text-white tracking-wide">IntelliJ IDEA Setup Guide</h3>
        <p className="text-slate-400 text-sm mt-1">Complete step-by-step instructions to run on Java 17</p>
      </div>
      {steps.map((s) => (
        <div key={s.num} className={`bg-slate-900/50 rounded-2xl p-5 border ${s.color}`}>
          <div className="flex items-center gap-3 mb-4">
            <div className={`font-black text-2xl ${s.numColor}`}>{s.num}</div>
            <h4 className="text-white font-bold text-base">{s.title}</h4>
          </div>
          {s.content}
        </div>
      ))}
    </div>
  );
}

// ─── App root ─────────────────────────────────────────────────────────────────
export default function App() {
  const [activeTab, setActiveTab] = useState<TabId>("overview");
  const [activeFile, setActiveFile] = useState<string>(FILE_KEYS[0]);

  return (
    <div className="min-h-screen bg-[#0a0f1a] text-white">
      {/* ── Top header bar ──────────────────────────────────────────── */}
      <header className="sticky top-0 z-50 bg-[#0a0f1a]/95 backdrop-blur border-b border-slate-800 shadow-xl">
        <div className="max-w-7xl mx-auto px-4">
          <div className="flex items-center gap-4 py-3">
            {/* Logo */}
            <div className="flex items-center gap-2.5 shrink-0">
              <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-orange-500 to-red-600 
                              flex items-center justify-center text-xl shadow-lg">
                ☕
              </div>
              <div>
                <div className="text-white font-extrabold text-sm leading-none">Customer 360</div>
                <div className="text-slate-500 text-[10px] leading-none mt-0.5">6-Way Join · Java Spark · 30-min SLA</div>
              </div>
            </div>

            {/* Spacer */}
            <div className="flex-1" />

            {/* Badges */}
            <div className="hidden md:flex items-center gap-2">
              {[
                { label: "Spark 3.5.1", color: "bg-orange-500/20 text-orange-400 border-orange-500/30" },
                { label: "Java 17",     color: "bg-blue-500/20 text-blue-400 border-blue-500/30" },
                { label: "AQE ✔",      color: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30" },
                { label: "Maven",       color: "bg-red-500/20 text-red-400 border-red-500/30" },
              ].map((b) => (
                <span key={b.label} className={`text-[11px] font-semibold px-2.5 py-0.5 rounded-full border ${b.color}`}>
                  {b.label}
                </span>
              ))}
            </div>
          </div>

          {/* Tab nav */}
          <div className="flex gap-1 overflow-x-auto pb-px scrollbar-hide">
            {TABS.map((t) => (
              <button
                key={t.id}
                onClick={() => setActiveTab(t.id)}
                className={`flex items-center gap-1.5 px-3 py-2.5 text-xs font-semibold 
                            whitespace-nowrap rounded-t-lg border-b-2 transition-all duration-150
                            ${activeTab === t.id
                              ? "text-orange-400 border-orange-500 bg-orange-500/5"
                              : "text-slate-400 border-transparent hover:text-slate-200 hover:bg-slate-800/40"
                            }`}
              >
                <span>{t.icon}</span>
                <span>{t.label}</span>
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* ── Main content ────────────────────────────────────────────── */}
      <main className="max-w-7xl mx-auto px-4 py-8">

        {activeTab === "overview" && <OverviewSection />}

        {activeTab === "join-dag" && (
          <div className="bg-slate-900/40 rounded-2xl p-6 border border-slate-800">
            <JoinDiagram />
          </div>
        )}

        {activeTab === "aqe" && (
          <div className="bg-slate-900/40 rounded-2xl p-6 border border-slate-800">
            <AQEDiagram />
          </div>
        )}

        {activeTab === "bucketing" && (
          <div className="bg-slate-900/40 rounded-2xl p-6 border border-slate-800">
            <BucketingDiagram />
          </div>
        )}

        {activeTab === "sla" && (
          <div className="bg-slate-900/40 rounded-2xl p-6 border border-slate-800">
            <SLADashboard />
          </div>
        )}

        {activeTab === "code" && (
          <div className="flex gap-4 min-h-[600px]">
            {/* File sidebar */}
            <div className="w-56 shrink-0 bg-slate-900/60 rounded-2xl border border-slate-800 p-2 
                            h-fit sticky top-28 space-y-1">
              <div className="text-slate-500 text-[10px] uppercase tracking-widest px-2 py-1 font-semibold">
                Project Files
              </div>
              {FILE_KEYS.map((key) => (
                <button
                  key={key}
                  onClick={() => setActiveFile(key)}
                  className={`w-full text-left px-3 py-2 rounded-xl text-xs transition-all duration-150 
                              flex items-start gap-2 group
                              ${activeFile === key
                                ? "bg-orange-500/15 text-orange-300 border border-orange-500/30"
                                : "text-slate-400 hover:bg-slate-800 hover:text-slate-200 border border-transparent"
                              }`}
                >
                  <span className="shrink-0 mt-0.5">
                    {key.endsWith(".java") ? "☕" :
                     key.endsWith(".xml")  ? "📄" :
                     key.endsWith(".md")   ? "📖" : "📄"}
                  </span>
                  <span className="break-all leading-tight font-mono">{key}</span>
                </button>
              ))}
            </div>

            {/* Code panel */}
            <div className="flex-1 min-w-0 space-y-3">
              {/* File header */}
              <div className="flex items-center justify-between bg-slate-900/60 rounded-xl 
                              border border-slate-800 px-4 py-3">
                <div className="flex items-center gap-3">
                  <span className="text-2xl">
                    {activeFile.endsWith(".java") ? "☕" :
                     activeFile.endsWith(".xml")  ? "📄" :
                     activeFile.endsWith(".md")   ? "📖" : "📄"}
                  </span>
                  <div>
                    <div className="text-white font-bold text-sm font-mono">{activeFile}</div>
                    <div className="text-slate-500 text-xs">
                      {files[activeFile]?.lang?.toUpperCase() ?? "TEXT"} ·{" "}
                      {files[activeFile]?.code?.split("\n").length ?? 0} lines
                    </div>
                  </div>
                </div>
              </div>

              {/* Code block */}
              {files[activeFile] && (
                <CodeBlock
                  code={files[activeFile].code}
                  lang={files[activeFile].lang}
                  maxH="680px"
                />
              )}
            </div>
          </div>
        )}

        {activeTab === "setup" && (
          <div className="max-w-3xl mx-auto">
            <SetupSection />
          </div>
        )}
      </main>

      {/* ── Footer ─────────────────────────────────────────────────── */}
      <footer className="mt-16 border-t border-slate-800 py-6 text-center text-slate-600 text-xs">
        <div>
          Customer 360 — 6-Way Join Model · Java 17 · Apache Spark 3.5.1 · Maven
        </div>
        <div className="mt-1">
          Covers: AQE Internals · Partition Strategy · Bucketing vs Partitioning · Join Strategy Selection
        </div>
      </footer>
    </div>
  );
}
