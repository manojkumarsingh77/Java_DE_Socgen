/**
 * AQE Internals — animated SVG diagram
 */
export default function AQEDiagram() {
  return (
    <div className="w-full overflow-x-auto pb-2">
      <div className="min-w-[800px]">
        {/* ── Header ─────────────────────────────────────────────── */}
        <div className="text-center mb-6">
          <h3 className="text-lg font-bold text-white tracking-wide">
            AQE Re-Optimise Loop — Internals
          </h3>
          <p className="text-slate-400 text-sm mt-1">
            Spark executes stages iteratively, collecting real statistics before re-planning
          </p>
        </div>

        {/* ── Main flow ──────────────────────────────────────────── */}
        <div className="flex items-start justify-center gap-3 flex-wrap">

          {/* Stage boxes */}
          {[
            { label: "Stage 1\nTable Scan", color: "from-blue-600 to-blue-800", icon: "🗂️" },
            { label: "Stage 2\nShuffle Write", color: "from-purple-600 to-purple-800", icon: "✍️" },
            { label: "Stage 3\nMapOutput\nStatistics", color: "from-orange-600 to-orange-800", icon: "📊" },
            { label: "Stage 4\nRe-Optimise\nPlan", color: "from-emerald-600 to-emerald-800", icon: "🔁" },
            { label: "Stage 5\nExec New\nQueryStage", color: "from-rose-600 to-rose-800", icon: "⚡" },
          ].map((s, i) => (
            <div key={i} className="flex items-center">
              <div
                className={`bg-gradient-to-br ${s.color} rounded-xl px-4 py-3 text-center 
                            min-w-[110px] shadow-lg border border-white/10`}
              >
                <div className="text-2xl mb-1">{s.icon}</div>
                <div className="text-white text-xs font-semibold whitespace-pre-line leading-tight">
                  {s.label}
                </div>
              </div>
              {i < 4 && (
                <div className="flex items-center mx-1">
                  <div className="w-6 h-0.5 bg-slate-500"></div>
                  <div className="text-slate-400 text-lg">▶</div>
                </div>
              )}
            </div>
          ))}
        </div>

        {/* ── AQE Decision Cards ─────────────────────────────────── */}
        <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">

          <div className="bg-gradient-to-br from-slate-800 to-slate-900 rounded-xl p-4 border border-blue-500/30">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-2xl">🔀</span>
              <span className="text-blue-400 font-bold text-sm">Dynamic Partition Coalesce</span>
            </div>
            <div className="space-y-2">
              <div className="text-xs text-slate-400">Before AQE:</div>
              <div className="flex gap-1 flex-wrap">
                {Array(8).fill(0).map((_, i) => (
                  <div key={i} className="w-7 h-3 rounded-sm bg-blue-900/60 border border-blue-700/50 text-[9px] text-blue-400 flex items-center justify-center">
                    5MB
                  </div>
                ))}
              </div>
              <div className="text-xs text-slate-400 mt-2">After AQE coalesce (128 MB target):</div>
              <div className="flex gap-1">
                {[1].map((_, i) => (
                  <div key={i} className="h-3 rounded-sm bg-blue-500/70 border border-blue-400/50 text-[9px] text-white flex items-center justify-center px-3">
                    ~128 MB
                  </div>
                ))}
              </div>
              <div className="text-[10px] text-emerald-400 mt-1">
                ✔ 8 tasks → 1 task  •  87.5% overhead reduction
              </div>
            </div>
          </div>

          <div className="bg-gradient-to-br from-slate-800 to-slate-900 rounded-xl p-4 border border-orange-500/30">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-2xl">🔄</span>
              <span className="text-orange-400 font-bold text-sm">Dynamic Join Strategy</span>
            </div>
            <div className="space-y-2">
              <div className="bg-slate-700/50 rounded-lg p-2 text-xs">
                <div className="text-slate-400">Static plan:</div>
                <div className="text-orange-300 font-mono">SortMergeJoin</div>
                <div className="text-slate-500 text-[10px]">(estimated 50 MB small side)</div>
              </div>
              <div className="flex justify-center text-slate-400 text-lg">⬇</div>
              <div className="bg-emerald-900/30 rounded-lg p-2 text-xs border border-emerald-500/30">
                <div className="text-slate-400">AQE runtime plan:</div>
                <div className="text-emerald-300 font-mono">BroadcastHashJoin</div>
                <div className="text-slate-500 text-[10px]">(actual 8 MB &lt; 200 MB threshold)</div>
              </div>
            </div>
          </div>

          <div className="bg-gradient-to-br from-slate-800 to-slate-900 rounded-xl p-4 border border-rose-500/30">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-2xl">⚖️</span>
              <span className="text-rose-400 font-bold text-sm">Skew Join Splitting</span>
            </div>
            <div className="space-y-2">
              <div className="text-xs text-slate-400">Before — hot partition:</div>
              <div className="flex items-end gap-1 h-10">
                <div className="w-7 bg-rose-600 rounded-t" style={{ height: "80%" }}><div className="text-[8px] text-white text-center">2GB</div></div>
                <div className="w-7 bg-slate-600 rounded-t" style={{ height: "25%" }}></div>
                <div className="w-7 bg-slate-600 rounded-t" style={{ height: "20%" }}></div>
                <div className="w-7 bg-slate-600 rounded-t" style={{ height: "22%" }}></div>
                <div className="w-7 bg-slate-600 rounded-t" style={{ height: "18%" }}></div>
              </div>
              <div className="text-xs text-slate-400 mt-1">After AQE split (5× median = 250 MB):</div>
              <div className="flex items-end gap-0.5 h-10">
                {Array(8).fill(0).map((_, i) => (
                  <div key={i} className="w-5 bg-emerald-600/80 rounded-t" style={{ height: "30%" }}></div>
                ))}
                <div className="w-7 bg-slate-600 rounded-t ml-1" style={{ height: "25%" }}></div>
                <div className="w-7 bg-slate-600 rounded-t" style={{ height: "20%" }}></div>
              </div>
              <div className="text-[10px] text-emerald-400 mt-1">
                ✔ 1×2GB → 8×250MB sub-partitions
              </div>
            </div>
          </div>
        </div>

        {/* ── Config Reference ───────────────────────────────────── */}
        <div className="mt-6 bg-slate-900/80 rounded-xl p-4 border border-slate-700">
          <div className="text-slate-300 font-semibold text-sm mb-3">⚙️  Key AQE Configuration Parameters</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            {[
              { key: "spark.sql.adaptive.enabled", val: "true", desc: "Master switch" },
              { key: "spark.sql.adaptive.advisoryPartitionSizeInBytes", val: "128m", desc: "Target coalesce size" },
              { key: "spark.sql.adaptive.autoBroadcastJoinThreshold", val: "200m", desc: "Runtime BHJ conversion limit" },
              { key: "spark.sql.adaptive.skewJoin.skewedPartitionFactor", val: "5", desc: "5× median = skewed" },
              { key: "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", val: "256m", desc: "Min absolute skew size" },
              { key: "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", val: "64m", desc: "SMJ → SHJ trigger" },
            ].map((c) => (
              <div key={c.key} className="flex gap-2 items-start text-xs">
                <div className="font-mono text-yellow-400 shrink-0 leading-tight">{c.key}</div>
                <div className="text-emerald-400 font-bold shrink-0">= {c.val}</div>
                <div className="text-slate-500">// {c.desc}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
