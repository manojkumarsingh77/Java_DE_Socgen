/**
 * Bucketing vs Partitioning comparison diagram
 */
export default function BucketingDiagram() {
  const comparisons = [
    {
      aspect: "Storage Layout",
      partitioning: "Directories per column value\n/data/segment=GOLD/\n/data/segment=SILVER/",
      bucketing: "Fixed N bucket files per table\n/data/part-00000-bucket0.parquet\n/data/part-00000-bucket1.parquet",
      partColor: "text-blue-400",
      buckColor: "text-emerald-400",
    },
    {
      aspect: "Best For",
      partitioning: "Filter pruning on low-cardinality\ncolumns (country, date, status)",
      bucketing: "Eliminating shuffle in\nrepeat large-table JOINs on same key",
      partColor: "text-blue-400",
      buckColor: "text-emerald-400",
    },
    {
      aspect: "Join Impact",
      partitioning: "No join benefit — still shuffles\nboth sides during Sort-Merge Join",
      bucketing: "ZERO shuffle — Spark reads\nbucket N from both tables together",
      partColor: "text-red-400",
      buckColor: "text-emerald-400",
    },
    {
      aspect: "Cardinality",
      partitioning: "Must be LOW (< 10K values)\nHigh cardinality = file explosion",
      bucketing: "Works with HIGH cardinality\n(millions of customer_ids)",
      partColor: "text-blue-400",
      buckColor: "text-emerald-400",
    },
    {
      aspect: "Flexibility",
      partitioning: "Can add new partitions dynamically\n(INSERT INTO with new value)",
      bucketing: "Fixed at write time — re-bucketing\nrequires full table rewrite",
      partColor: "text-emerald-400",
      buckColor: "text-red-400",
    },
    {
      aspect: "Query Pruning",
      partitioning: "Excellent — skips entire partition\ndirectories via metadata filter",
      bucketing: "Limited — must touch all buckets\nunless bucket-pruning enabled",
      partColor: "text-emerald-400",
      buckColor: "text-orange-400",
    },
  ];

  const decisionTree = [
    {
      q: "Do you often filter on this column?",
      yes: "Use PARTITIONING (e.g. order_date, country_code)",
      no: "→ next question",
      yesColor: "text-blue-400",
    },
    {
      q: "Is cardinality LOW (< 10K unique values)?",
      yes: "Use PARTITIONING (safe — no file explosion)",
      no: "Avoid partitioning on this column",
      yesColor: "text-blue-400",
      noColor: "text-red-400",
    },
    {
      q: "Do you repeatedly JOIN two large tables on the same key?",
      yes: "Use BUCKETING on that join key",
      no: "Use AQE broadcast or default SMJ",
      yesColor: "text-emerald-400",
    },
    {
      q: "Are both tables bucketed on the SAME key with compatible counts?",
      yes: "✔ Shuffle eliminated — pure bucket-merge SMJ",
      no: "⚠ Exchange re-added on mismatched side — verify counts!",
      yesColor: "text-emerald-400",
      noColor: "text-orange-400",
    },
  ];

  const c360Strategy = [
    { table: "customers",       partition: "segment (4 values)",    bucket: "customer_id / 1024 buckets", reason: "Low-card partition; join anchor" },
    { table: "orders",          partition: "channel (4 values)",     bucket: "customer_id / 4096 buckets", reason: "4× customer buckets = compatible" },
    { table: "products",        partition: "category (6 values)",    bucket: "— (broadcast only)",         reason: "480 MB ≤ broadcast threshold" },
    { table: "web_events",      partition: "device_type (3 values)", bucket: "customer_id / 4096 buckets", reason: "Largest table; pre-sorted for SMJ" },
    { table: "support_tickets", partition: "priority (4 values)",    bucket: "customer_id / 1024 buckets", reason: "AQE converts to SHJ post-agg" },
    { table: "loyalty_rewards", partition: "tier (4 values)",        bucket: "customer_id / 1024 buckets", reason: "Bucket-merge with customers" },
  ];

  return (
    <div className="w-full space-y-8">
      <div className="text-center">
        <h3 className="text-lg font-bold text-white tracking-wide">
          Bucketing vs Partitioning — Complete Comparison
        </h3>
        <p className="text-slate-400 text-sm mt-1">
          Strategy modelling for the Customer 360 pipeline
        </p>
      </div>

      {/* Comparison grid */}
      <div className="overflow-x-auto">
        <table className="w-full min-w-[640px] border-collapse text-sm">
          <thead>
            <tr>
              <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700 w-1/4">Aspect</th>
              <th className="text-left py-2 px-3 border-b border-slate-700 w-3/8">
                <span className="text-blue-400 font-bold">📂 Partitioning</span>
                <div className="text-slate-500 font-normal text-xs">partitionBy("column")</div>
              </th>
              <th className="text-left py-2 px-3 border-b border-slate-700 w-3/8">
                <span className="text-emerald-400 font-bold">🪣 Bucketing</span>
                <div className="text-slate-500 font-normal text-xs">bucketBy(N, "key")</div>
              </th>
            </tr>
          </thead>
          <tbody>
            {comparisons.map((c, i) => (
              <tr key={c.aspect} className={i % 2 === 0 ? "bg-slate-800/30" : ""}>
                <td className="py-3 px-3 text-slate-300 font-medium text-xs align-top">{c.aspect}</td>
                <td className={`py-3 px-3 ${c.partColor} text-xs align-top whitespace-pre-line leading-relaxed`}>
                  {c.partitioning}
                </td>
                <td className={`py-3 px-3 ${c.buckColor} text-xs align-top whitespace-pre-line leading-relaxed`}>
                  {c.bucketing}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Decision Tree */}
      <div className="bg-slate-900/60 rounded-xl p-5 border border-slate-700">
        <div className="text-white font-bold text-sm mb-4">🌳 Decision Tree — When to use which strategy?</div>
        <div className="space-y-3">
          {decisionTree.map((d, i) => (
            <div key={i} className="flex gap-3 items-start">
              <div className="shrink-0 w-6 h-6 rounded-full bg-slate-700 text-slate-300 text-xs 
                              flex items-center justify-center font-bold mt-0.5">{i + 1}</div>
              <div className="flex-1">
                <div className="text-slate-200 text-xs font-semibold">{d.q}</div>
                <div className="mt-1 flex flex-wrap gap-2">
                  <span className="text-[11px]">
                    <span className="text-slate-500">YES →</span>{" "}
                    <span className={d.yesColor || "text-emerald-400"}>{d.yes}</span>
                  </span>
                  {d.no && (
                    <span className="text-[11px]">
                      <span className="text-slate-500">NO →</span>{" "}
                      <span className={d.noColor || "text-slate-400"}>{d.no}</span>
                    </span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* C360 Strategy Table */}
      <div>
        <div className="text-white font-bold text-sm mb-3">📋 Customer 360 — Applied Strategy per Table</div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[720px] border-collapse text-xs">
            <thead>
              <tr className="bg-slate-800">
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Table</th>
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Partition By</th>
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Bucket By</th>
                <th className="text-left text-slate-400 font-semibold py-2 px-3 border-b border-slate-700">Rationale</th>
              </tr>
            </thead>
            <tbody>
              {c360Strategy.map((r, i) => (
                <tr key={r.table} className={i % 2 === 0 ? "bg-slate-800/20" : ""}>
                  <td className="py-2 px-3 text-blue-300 font-mono font-medium">{r.table}</td>
                  <td className="py-2 px-3 text-blue-400">{r.partition}</td>
                  <td className="py-2 px-3 text-emerald-400">{r.bucket}</td>
                  <td className="py-2 px-3 text-slate-400">{r.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Bucket count formula */}
      <div className="bg-gradient-to-br from-slate-900 to-slate-800 rounded-xl p-5 border border-slate-700">
        <div className="text-white font-bold text-sm mb-3">📐 Bucket Count Formula</div>
        <div className="font-mono text-sm text-yellow-300 bg-slate-950/60 rounded-lg p-3 border border-slate-700">
          buckets = nextPowerOf2(totalTableSizeGB / targetBucketFileSizeGB)
        </div>
        <div className="mt-3 space-y-2 text-xs">
          {[
            { t: "customers (25 GB)", f: "25 / 0.025 = 1000 → nextPow2 = 1024 buckets ✔" },
            { t: "orders (200 GB)", f: "200 / 0.05 = 4000 → nextPow2 = 4096 buckets (4× customers ✔)" },
            { t: "web_events (480 GB)", f: "480 / 0.12 = 4000 → 4096 buckets (same as orders ✔)" },
            { t: "support (18 GB)", f: "18 / 0.018 = 1000 → 1024 buckets (1× customers ✔)" },
            { t: "loyalty (22 GB)", f: "22 / 0.022 = 1000 → 1024 buckets (1× customers ✔)" },
          ].map((b) => (
            <div key={b.t} className="flex gap-2">
              <span className="text-blue-300 w-32 shrink-0">{b.t}</span>
              <span className="text-slate-400">→</span>
              <span className="text-emerald-400">{b.f}</span>
            </div>
          ))}
        </div>
        <div className="mt-3 text-xs text-slate-500">
          ⚠️  Compatible bucket counts: smaller must be a divisor of larger (e.g. 1024, 2048, 4096 all compatible with 4096).
          Mismatched counts force an Exchange on the larger-bucketed side.
        </div>
      </div>
    </div>
  );
}
