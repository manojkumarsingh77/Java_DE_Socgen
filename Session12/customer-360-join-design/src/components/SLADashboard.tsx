/**
 * SLA Budget Dashboard — 30-minute breakdown
 */
export default function SLADashboard() {
  const SLA_TOTAL = 1800; // 30 min in seconds

  const phases = [
    { name: "Data Generation",   secs: 118, color: "bg-blue-500",    icon: "⚙️",  note: "Synthetic data for 6 tables (scale=1.0)" },
    { name: "Bucket Write",       secs: 187, color: "bg-purple-500",  icon: "💾",  note: "Write all tables as bucketed Parquet to Hive" },
    { name: "HOP 1: Cust×Orders", secs: 312, color: "bg-indigo-500",  icon: "🔗",  note: "SMJ bucketed — zero shuffle bytes" },
    { name: "HOP 2: +Products",   secs:  48, color: "bg-emerald-500", icon: "📦",  note: "BHJ — broadcast(products) 480 MB" },
    { name: "HOP 3: +WebEvents",  secs: 398, color: "bg-orange-500",  icon: "🌐",  note: "SMJ + AQE skew split for VIP keys" },
    { name: "HOP 4: +Support",    secs: 201, color: "bg-rose-500",    icon: "🎫",  note: "SHJ — AQE converts post-coalesce" },
    { name: "HOP 5: +Loyalty",    secs: 189, color: "bg-yellow-500",  icon: "🏆",  note: "SMJ bucket-merge — zero network I/O" },
    { name: "Aggregation",        secs: 224, color: "bg-teal-500",    icon: "📊",  note: "C360 Gold layer KPI computation" },
    { name: "Output / Report",    secs: 103, color: "bg-slate-500",   icon: "📤",  note: "Write Parquet + print SLA report" },
  ];

  const total = phases.reduce((a, p) => a + p.secs, 0);
  const withinSLA = total <= SLA_TOTAL;

  const joinStrategies = [
    { hop: "HOP 1", strategy: "Sort-Merge Join",         badge: "SMJ",     color: "bg-purple-600",  reason: "Bucketed tables (1024 buckets): Exchange eliminated. AQE monitors for skew on VIP keys." },
    { hop: "HOP 2", strategy: "Broadcast Hash Join",     badge: "BHJ",     color: "bg-emerald-600", reason: "broadcast(products) — 480 MB fits in executor memory. Fastest possible join type." },
    { hop: "HOP 3", strategy: "Sort-Merge + AQE Skew",  badge: "SMJ+AQE", color: "bg-orange-600",  reason: "web_events pre-aggregated. AQE splits 2 GB VIP partition into 8×250 MB sub-partitions." },
    { hop: "HOP 4", strategy: "Shuffle Hash Join",       badge: "SHJ",     color: "bg-rose-600",    reason: "Post-agg tickets are uniform & small. AQE coalesces to 64 MB/partition → prefers SHJ." },
    { hop: "HOP 5", strategy: "Sort-Merge Join",         badge: "SMJ",     color: "bg-yellow-600",  reason: "Bucket-to-bucket merge (both 1024×customer_id). Zero shuffle bytes — pure local read." },
  ];

  const clusterSpec = [
    { label: "Executors",       value: "20 nodes" },
    { label: "Cores / executor", value: "16 vCPU" },
    { label: "RAM / executor",  value: "64 GB" },
    { label: "Driver RAM",      value: "8 GB" },
    { label: "Storage",         value: "NVMe SSD (local)" },
    { label: "Network",         value: "25 Gbps" },
    { label: "Shuffle I/O",     value: "~340 GB total" },
    { label: "Spark version",   value: "3.5.1" },
  ];

  return (
    <div className="w-full space-y-8">
      <div className="text-center">
        <h3 className="text-lg font-bold text-white tracking-wide">
          30-Minute SLA Budget — Phase Breakdown
        </h3>
        <p className="text-slate-400 text-sm mt-1">
          Production cluster: 20 × 16-core × 64 GB executors · scale = 1.0
        </p>
      </div>

      {/* SLA bar */}
      <div className="bg-slate-900 rounded-xl p-5 border border-slate-700">
        <div className="flex justify-between text-xs text-slate-400 mb-2">
          <span>0 min</span>
          <span className={withinSLA ? "text-emerald-400 font-bold" : "text-red-400 font-bold"}>
            Total: {Math.floor(total / 60)}m {total % 60}s / SLA: 30m
          </span>
          <span>30 min</span>
        </div>

        {/* Stacked bar */}
        <div className="flex h-8 rounded-lg overflow-hidden border border-slate-700">
          {phases.map((p) => (
            <div
              key={p.name}
              className={`${p.color} transition-all duration-700 relative group`}
              style={{ width: `${(p.secs / SLA_TOTAL) * 100}%` }}
            >
              <div className="absolute inset-0 flex items-center justify-center">
                <span className="text-white text-[9px] font-bold hidden group-hover:block">
                  {Math.floor(p.secs / 60)}m{p.secs % 60}s
                </span>
              </div>
              {/* Tooltip */}
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 hidden group-hover:block
                              bg-slate-800 text-white text-[10px] rounded px-2 py-1 whitespace-nowrap
                              border border-slate-600 z-10 shadow-xl">
                {p.icon} {p.name}: {Math.floor(p.secs / 60)}m {p.secs % 60}s
              </div>
            </div>
          ))}
          {/* Remaining SLA */}
          {SLA_TOTAL > total && (
            <div
              className="bg-slate-700/30 border-l border-dashed border-slate-500"
              style={{ width: `${((SLA_TOTAL - total) / SLA_TOTAL) * 100}%` }}
            />
          )}
        </div>

        {/* SLA marker */}
        <div className="relative mt-1">
          <div className="flex justify-end">
            <span className="text-[10px] text-slate-500">← SLA buffer: {Math.floor((SLA_TOTAL - total) / 60)}m {(SLA_TOTAL - total) % 60}s remaining</span>
          </div>
        </div>
      </div>

      {/* Phase cards */}
      <div className="space-y-2">
          {phases.map((p) => (
          <div key={p.name} className="flex gap-3 items-center bg-slate-800/40 rounded-xl px-4 py-3 
                                       border border-slate-700/50 hover:border-slate-600 transition-colors">
            <div className="text-xl w-8 text-center shrink-0">{p.icon}</div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-white text-xs font-semibold">{p.name}</span>
                <div className={`h-1.5 flex-1 rounded-full ${p.color} opacity-30`}></div>
                <div className={`h-1.5 rounded-full ${p.color}`}
                  style={{ width: `${(p.secs / Math.max(...phases.map(x => x.secs))) * 80}px` }}></div>
              </div>
              <div className="text-slate-500 text-[11px] mt-0.5">{p.note}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className="text-white font-mono text-sm font-bold">
                {Math.floor(p.secs / 60)}m {p.secs % 60}s
              </div>
              <div className="text-slate-500 text-[10px]">
                {((p.secs / SLA_TOTAL) * 100).toFixed(1)}% of SLA
              </div>
            </div>
          </div>
        ))}

        {/* Total row */}
        <div className={`flex gap-3 items-center rounded-xl px-4 py-3 border-2 
                        ${withinSLA
                          ? "bg-emerald-900/30 border-emerald-500/50"
                          : "bg-red-900/30 border-red-500/50"}`}>
          <div className="text-xl w-8 text-center shrink-0">{withinSLA ? "✅" : "❌"}</div>
          <div className="flex-1 text-white font-bold text-sm">
            Pipeline Total — {withinSLA ? "WITHIN SLA ✔" : "SLA BREACHED ✘"}
          </div>
          <div className="shrink-0 text-right">
            <div className={`font-mono text-sm font-bold ${withinSLA ? "text-emerald-400" : "text-red-400"}`}>
              {Math.floor(total / 60)}m {total % 60}s
            </div>
            <div className="text-slate-500 text-[10px]">SLA: 30m 00s</div>
          </div>
        </div>
      </div>

      {/* Join strategy summary */}
      <div>
        <div className="text-white font-bold text-sm mb-3">🔗 Join Strategy Selection — Why Each Hop Uses Its Strategy</div>
        <div className="space-y-3">
          {joinStrategies.map((j) => (
            <div key={j.hop} className="bg-slate-800/50 rounded-xl p-4 border border-slate-700 flex gap-4">
              <div className="shrink-0">
                <div className="text-slate-400 text-xs font-semibold">{j.hop}</div>
                <div className={`mt-1 ${j.color} text-white text-[10px] font-bold px-2 py-0.5 rounded-full text-center`}>
                  {j.badge}
                </div>
              </div>
              <div>
                <div className="text-white text-xs font-semibold">{j.strategy}</div>
                <div className="text-slate-400 text-xs mt-1 leading-relaxed">{j.reason}</div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Cluster spec */}
      <div className="bg-slate-900/60 rounded-xl p-5 border border-slate-700">
        <div className="text-white font-bold text-sm mb-3">🖥️  Reference Cluster Specification</div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {clusterSpec.map((s) => (
            <div key={s.label} className="bg-slate-800 rounded-lg p-3 text-center">
              <div className="text-slate-400 text-[10px] uppercase tracking-wider">{s.label}</div>
              <div className="text-white font-bold text-xs mt-1">{s.value}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
