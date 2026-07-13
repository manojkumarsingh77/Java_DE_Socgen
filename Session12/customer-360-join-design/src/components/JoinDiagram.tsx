/**
 * 6-Way Join DAG — visual flow diagram
 */
export default function JoinDiagram() {
  const tables = [
    {
      name: "customers",
      rows: "500M",
      size: "25 GB",
      strategy: "ANCHOR",
      color: "from-blue-600 to-blue-800",
      border: "border-blue-500/50",
      icon: "👤",
    },
    {
      name: "orders",
      rows: "2B",
      size: "200 GB",
      strategy: "Sort-Merge Join\n(Bucketed, no shuffle)",
      joinType: "SMJ",
      joinColor: "bg-purple-600",
      color: "from-purple-600 to-purple-800",
      border: "border-purple-500/50",
      icon: "🛒",
    },
    {
      name: "products",
      rows: "10M",
      size: "480 MB",
      strategy: "Broadcast Hash Join\n(auto-broadcast ≤ 200 MB)",
      joinType: "BHJ",
      joinColor: "bg-emerald-600",
      color: "from-emerald-600 to-emerald-800",
      border: "border-emerald-500/50",
      icon: "📦",
    },
    {
      name: "web_events",
      rows: "5B (agg→)",
      size: "480 GB",
      strategy: "Sort-Merge Join\n+ AQE Skew Split",
      joinType: "SMJ+AQE",
      joinColor: "bg-orange-600",
      color: "from-orange-600 to-orange-800",
      border: "border-orange-500/50",
      icon: "🌐",
    },
    {
      name: "support_tickets",
      rows: "200M",
      size: "18 GB",
      strategy: "Shuffle Hash Join\n(AQE SMJ→SHJ)",
      joinType: "SHJ",
      joinColor: "bg-rose-600",
      color: "from-rose-600 to-rose-800",
      border: "border-rose-500/50",
      icon: "🎫",
    },
    {
      name: "loyalty_rewards",
      rows: "300M",
      size: "22 GB",
      strategy: "Sort-Merge Join\n(Bucket-to-bucket, 0 shuffle)",
      joinType: "SMJ",
      joinColor: "bg-yellow-600",
      color: "from-yellow-600 to-yellow-800",
      border: "border-yellow-500/50",
      icon: "🏆",
    },
  ];

  const hops = [
    {
      label: "HOP 1",
      left: "customers",
      right: "orders",
      strategy: "Sort-Merge Join",
      type: "SMJ",
      color: "text-purple-400",
      bg: "bg-purple-900/30 border-purple-500/30",
      note: "Bucketed on customer_id (1024 buckets) → Exchange eliminated",
    },
    {
      label: "HOP 2",
      left: "+orders result",
      right: "products",
      strategy: "Broadcast Hash Join",
      type: "BHJ",
      color: "text-emerald-400",
      bg: "bg-emerald-900/30 border-emerald-500/30",
      note: "Products 480 MB < 200 MB AQE threshold → broadcast(products)",
    },
    {
      label: "HOP 3",
      left: "+products result",
      right: "web_events (aggregated)",
      strategy: "Sort-Merge + AQE Skew",
      type: "SMJ+AQE",
      color: "text-orange-400",
      bg: "bg-orange-900/30 border-orange-500/30",
      note: "VIP top-1000 customers cause skew → AQE splits 2GB partition into 8×250MB",
    },
    {
      label: "HOP 4",
      left: "+web result",
      right: "support_tickets",
      strategy: "Shuffle Hash Join",
      type: "SHJ",
      color: "text-rose-400",
      bg: "bg-rose-900/30 border-rose-500/30",
      note: "Tickets post-agg ≈ uniform 8 GB → AQE coalesces to 64MB/partition → SHJ preferred",
    },
    {
      label: "HOP 5",
      left: "+tickets result",
      right: "loyalty_rewards",
      strategy: "Sort-Merge Join",
      type: "SMJ",
      color: "text-yellow-400",
      bg: "bg-yellow-900/30 border-yellow-500/30",
      note: "Both tables bucketed 1024×customer_id with sortBy → pure bucket-merge, 0 bytes shuffle",
    },
  ];

  return (
    <div className="w-full space-y-6">
      <div className="text-center">
        <h3 className="text-lg font-bold text-white tracking-wide">
          Customer 360 — 6-Way Join DAG
        </h3>
        <p className="text-slate-400 text-sm mt-1">
          Visual representation of all 5 join hops with strategy selection rationale
        </p>
      </div>

      {/* Table inventory */}
      <div className="overflow-x-auto pb-2">
        <div className="flex items-center gap-3 min-w-[720px] justify-center">
          {tables.map((t, i) => (
            <div key={t.name} className="flex items-center">
              <div
                className={`bg-gradient-to-br ${t.color} rounded-xl px-3 py-2.5 text-center 
                            min-w-[95px] shadow-lg border ${t.border} relative`}
              >
                <div className="text-xl mb-1">{t.icon}</div>
                <div className="text-white text-[11px] font-bold">{t.name}</div>
                <div className="text-white/70 text-[10px]">{t.rows} rows</div>
                <div className="text-white/60 text-[9px]">{t.size}</div>
                {t.joinType && (
                  <div className={`absolute -top-2 -right-2 ${t.joinColor} text-white text-[8px] 
                                  font-bold px-1.5 py-0.5 rounded-full shadow-lg border border-white/20`}>
                    {t.joinType}
                  </div>
                )}
              </div>
              {i < tables.length - 1 && (
                <div className="flex flex-col items-center mx-1">
                  <div className="w-4 h-0.5 bg-slate-600"></div>
                  <div className="text-slate-500 text-xs">▶</div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Hop detail cards */}
      <div className="space-y-3">
        {hops.map((h) => (
          <div key={h.label} className={`rounded-xl p-4 border ${h.bg} flex gap-4 items-start`}>
            <div className="shrink-0">
              <div className={`font-bold text-sm ${h.color} uppercase tracking-wider`}>{h.label}</div>
              <div className={`mt-1 px-2 py-0.5 rounded-full text-white text-[10px] font-bold 
                              ${h.type === "BHJ" ? "bg-emerald-600" :
                                h.type === "SHJ" ? "bg-rose-600" :
                                h.type === "SMJ+AQE" ? "bg-orange-600" :
                                "bg-purple-600"}`}>
                {h.type}
              </div>
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-white text-sm font-semibold">
                <span className="text-slate-400 font-normal">[</span>
                {h.left}
                <span className="text-slate-400 font-normal">] × [</span>
                {h.right}
                <span className="text-slate-400 font-normal">]</span>
              </div>
              <div className={`text-xs ${h.color} mt-0.5 font-medium`}>{h.strategy}</div>
              <div className="text-slate-400 text-xs mt-1 leading-relaxed">{h.note}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Gold layer */}
      <div className="bg-gradient-to-r from-yellow-900/40 to-amber-900/40 rounded-xl p-4 
                      border border-yellow-500/40 text-center">
        <div className="text-2xl mb-1">🏅</div>
        <div className="text-yellow-300 font-bold text-sm">CUSTOMER_360_GOLD</div>
        <div className="text-slate-400 text-xs mt-1">
          Partitioned by segment + country_code · KPIs: total_orders, total_revenue,
          avg_order_value, csat_score, loyalty_balance, c360_classification
        </div>
      </div>
    </div>
  );
}
