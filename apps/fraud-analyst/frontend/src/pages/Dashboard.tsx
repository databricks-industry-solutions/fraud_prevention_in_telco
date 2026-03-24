import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  AlertTriangle, CheckCircle2, Clock, Shield, Users,
  TrendingUp, Loader2, Target, ArrowUpRight, ArrowDownRight,
  Minus, DollarSign, Brain,
} from 'lucide-react'
import {
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, LineChart, Line, Area,
} from 'recharts'
import {
  fetchPipelineSummary, fetchEnginePerformance, fetchForecast,
  fetchEnvironmentalFactors, fetchMitigationEffectiveness,
  fetchTeamPerformance, fetchSLATracking,
  type PipelineSummary, type EnginePerformance, type ForecastData,
  type EnvironmentalFactors, type MitigationStep, type TeamPerformance,
  type SLATracking,
} from '../lib/api'

// ── Helpers ──────────────────────────────────────────────────

const fmtUSD = (v: number) => v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(1)}M` : v >= 1_000 ? `$${(v / 1_000).toFixed(0)}K` : `$${v.toFixed(0)}`

function DeltaBadge({ current, prior, invertColor }: { current: number | null; prior: number | null; invertColor?: boolean }) {
  if (current == null || prior == null || prior === 0) return <span className="text-[10px] text-gray-500 flex items-center gap-0.5"><Minus className="w-3 h-3" />—</span>
  const pct = Math.round(((current - prior) / prior) * 100)
  if (pct === 0) return <span className="text-[10px] text-gray-500 flex items-center gap-0.5"><Minus className="w-3 h-3" />0%</span>
  const isUp = pct > 0
  const isGood = invertColor ? !isUp : isUp
  const Icon = isUp ? ArrowUpRight : ArrowDownRight
  return <span className={`text-[10px] flex items-center gap-0.5 ${isGood ? 'text-green-400' : 'text-red-400'}`}><Icon className="w-3 h-3" />{Math.abs(pct)}%</span>
}

function DarkTooltip({ active, payload, label }: any) {
  if (!active || !payload) return null
  return (
    <div className="bg-[#1e2130] border border-gray-700 rounded px-3 py-2 text-xs shadow-lg">
      <p className="text-gray-400 mb-1">{label}</p>
      {payload.map((p: any) => (
        <p key={p.name} style={{ color: p.color || p.fill || p.stroke }} className="font-mono">
          {p.name}: {typeof p.value === 'number' ? p.value.toLocaleString() : p.value}
        </p>
      ))}
    </div>
  )
}

// ── Main Component ───────────────────────────────────────────

export default function Dashboard() {
  const navigate = useNavigate()
  const [period, setPeriod] = useState<string>('month')
  const [summary, setSummary] = useState<PipelineSummary | null>(null)
  const [engine, setEngine] = useState<EnginePerformance | null>(null)
  const [forecast, setForecast] = useState<ForecastData | null>(null)
  const [envFactors, setEnvFactors] = useState<EnvironmentalFactors | null>(null)
  const [mitigations, setMitigations] = useState<MitigationStep[]>([])
  const [team, setTeam] = useState<TeamPerformance | null>(null)
  const [sla, setSla] = useState<SLATracking | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      fetchPipelineSummary(period),
      fetchEnginePerformance(period),
      fetchForecast(),
      fetchEnvironmentalFactors(),
      fetchMitigationEffectiveness(),
      fetchTeamPerformance(period),
      fetchSLATracking(),
    ])
      .then(([s, eng, f, e, mit, t, sl]) => {
        setSummary(s)
        setEngine(eng)
        setForecast(f)
        setEnvFactors(e)
        setMitigations(mit.mitigations)
        setTeam(t)
        setSla(sl as SLATracking)
      })
      .finally(() => setLoading(false))
  }, [period])

  if (loading) return <div className="flex items-center justify-center py-20 text-gray-500"><Loader2 className="w-5 h-5 animate-spin mr-2" />Loading management view…</div>
  if (!summary || !team || !envFactors) return null

  const cur = summary.current
  const pm = summary.prior_month
  const ax = { fill: '#6b7280', fontSize: 10 }
  const al = { stroke: '#374151' }

  // Forecast chart data
  const forecastData = (forecast?.series || []).map(s => ({
    day: s.day ? new Date(s.day + 'T00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '',
    actual: s.actual_cases, forecast: s.forecast_cases,
    upper: s.forecast_cases_upper, lower: s.forecast_cases_lower,
  }))
  if (forecastData.length > 0) {
    let lastIdx = -1
    for (let i = forecastData.length - 1; i >= 0; i--) { if (forecastData[i].actual != null) { lastIdx = i; break } }
    if (lastIdx >= 0 && lastIdx < forecastData.length - 1) {
      forecastData[lastIdx].forecast = forecastData[lastIdx].actual
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-lg font-semibold text-white">Management View</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Today: {new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}
            {(summary as any)?.latest_date && (
              <span> &middot; Data through {new Date((summary as any).latest_date + 'T00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-1 bg-[#1e2130] rounded-lg p-0.5">
          {(['day', 'week', 'month', 'quarter'] as const).map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={`px-3 py-1.5 rounded text-xs font-medium transition ${
                period === p ? 'bg-gray-700/60 text-white' : 'text-gray-400 hover:text-white hover:bg-gray-800'
              }`}
            >
              {p.charAt(0).toUpperCase() + p.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {/* ── 1. KPI Cards ───────────────────────────────────── */}
      <div className="grid grid-cols-2 lg:grid-cols-6 gap-3">
        <KPICard label="Open Cases" value={(cur.open_cases ?? 0).toLocaleString()} icon={Clock} color="text-yellow-400" prior={pm.open_cases} current={cur.open_cases} priorLabel={`vs prior ${period}`} />
        <KPICard label="Closed (MTD)" value={(cur.closed_cases).toLocaleString()} icon={CheckCircle2} color="text-green-400" prior={pm.closed_cases} current={cur.closed_cases} priorLabel={`vs prior ${period}`} />
        <KPICard label="Resolution Rate" value={`${cur.resolution_rate ?? 0}%`} icon={TrendingUp} color="text-blue-400" prior={pm.resolution_rate} current={cur.resolution_rate} priorLabel={`vs prior ${period}`} />
        <KPICard label="Escalation Rate" value={`${cur.escalation_rate ?? 0}%`} icon={AlertTriangle} color="text-purple-400" prior={pm.escalation_rate} current={cur.escalation_rate} priorLabel={`vs prior ${period}`} invertDelta />
        <KPICard label="Open Exposure" value={fmtUSD(cur.open_exposure ?? 0)} icon={DollarSign} color="text-red-400" prior={pm.open_exposure} current={cur.open_exposure} priorLabel={`vs prior ${period}`} invertDelta />
        <KPICard label="Total Exposure (MTD)" value={fmtUSD(cur.total_exposure ?? 0)} icon={DollarSign} color="text-orange-400" prior={pm.total_exposure} current={cur.total_exposure} priorLabel={`vs prior ${period}`} invertDelta />
      </div>

      {/* ── 2. Period Comparison ────────────────────────────── */}
      <Section title="Period Comparison" icon={Target}>
        <table className="w-full text-xs">
          <thead><tr className="border-b border-gray-800">
            <th className="px-4 py-2 text-left font-medium text-gray-500">Metric</th>
            <th className="px-4 py-2 text-right font-medium text-white">This {period.charAt(0).toUpperCase() + period.slice(1)}</th>
            <th className="px-4 py-2 text-right font-medium text-gray-500">Prior {period.charAt(0).toUpperCase() + period.slice(1)}</th>
            <th className="px-4 py-2 text-right font-medium text-gray-500">Prior {period === 'quarter' ? 'Year' : 'Qtr'}{period === 'month' ? ' (avg/mo)' : ''}</th>
            <th className="px-4 py-2 text-right font-medium text-gray-500">Same Period LY</th>
          </tr></thead>
          <tbody>
            {(() => {
              // For prior quarter column: show average per period (divide by number of periods in the window)
              const pqDiv = period === 'day' ? 7 : period === 'week' ? 4 : period === 'month' ? 3 : 4
              return (<>
            <CmpRow label="Total Cases" c={cur.total_cases} pm={pm.total_cases} pq={Math.round(summary.prior_quarter.total_cases/pqDiv)} py={summary.prior_year.total_cases} />
            <CmpRow label="Cases Closed" c={cur.closed_cases} pm={pm.closed_cases} pq={Math.round(summary.prior_quarter.closed_cases/pqDiv)} py={summary.prior_year.closed_cases} />
            <CmpRow label="Total Exposure" c={cur.total_exposure} pm={pm.total_exposure} pq={Math.round((summary.prior_quarter.total_exposure??0)/pqDiv)} py={summary.prior_year.total_exposure} fmt={fmtUSD} inv />
            <CmpRow label="Resolution Rate" c={cur.resolution_rate} pm={pm.resolution_rate} pq={summary.prior_quarter.resolution_rate} py={summary.prior_year.resolution_rate} suffix="%" />
            <CmpRow label="Escalation Rate" c={cur.escalation_rate} pm={pm.escalation_rate} pq={summary.prior_quarter.escalation_rate} py={summary.prior_year.escalation_rate} suffix="%" inv />
              </>)
            })()}
          </tbody>
        </table>
      </Section>

      {/* ── 3. Engine Performance ──────────────────────────── */}
      {engine?.current && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <div className="bg-[#161922] border border-gray-800 rounded-lg p-4">
            <span className="text-[10px] uppercase tracking-wider text-gray-500">Auto-Blocked</span>
            <div className="text-xl font-semibold text-white mt-1">{(engine.current.autoblock_cases).toLocaleString()}</div>
            <div className="flex items-center gap-1.5 mt-1">
              <DeltaBadge current={engine.current.autoblock_cases} prior={engine.prior?.autoblock_cases} />
              <span className="text-[10px] text-gray-500">vs prior {period}</span>
            </div>
          </div>
          <div className="bg-[#161922] border border-green-500/20 rounded-lg p-4">
            <span className="text-[10px] uppercase tracking-wider text-gray-500">$ Saved by Engine</span>
            <div className="text-xl font-semibold text-green-400 mt-1">{fmtUSD(engine.current.saved_by_autoblock)}</div>
            <div className="flex items-center gap-1.5 mt-1">
              <DeltaBadge current={engine.current.saved_by_autoblock} prior={engine.prior?.saved_by_autoblock} />
              <span className="text-[10px] text-gray-500">vs prior {period}</span>
            </div>
          </div>
          <div className={`bg-[#161922] border rounded-lg p-4 ${engine.current.false_positives > 0 ? 'border-yellow-500/20' : 'border-gray-800'}`}>
            <span className="text-[10px] uppercase tracking-wider text-gray-500">False Positives</span>
            <div className="text-xl font-semibold text-white mt-1">{(engine.current.false_positives).toLocaleString()}</div>
            <div className="text-xs text-yellow-400 mt-1">{fmtUSD(engine.current.lost_by_fp)} blocked incorrectly</div>
          </div>
          <div className={`bg-[#161922] border rounded-lg p-4 ${engine.current.false_negatives > 0 ? 'border-red-500/20' : 'border-gray-800'}`}>
            <span className="text-[10px] uppercase tracking-wider text-gray-500">Missed Fraud</span>
            <div className="text-xl font-semibold text-white mt-1">{(engine.current.false_negatives).toLocaleString()}</div>
            <div className="text-xs text-red-400 mt-1">{fmtUSD(engine.current.lost_by_fn)} exposure missed</div>
          </div>
        </div>
      )}

      {/* ── 4. Forecast ──────────────────────────────────────── */}
      <section className="bg-[#161922] border border-gray-800 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
          <div>
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider flex items-center gap-1.5">
              <Brain className="w-3.5 h-3.5 text-cyan-400" />
              30-Day Forward Forecast
            </h2>
            <p className="text-[10px] text-gray-600 mt-0.5">Based on 60-day trend · Independent of period filter above</p>
          </div>
          {(forecast as any)?.method && (
            <span className={`text-[10px] px-2 py-0.5 rounded font-medium ${
              (forecast as any).method === 'ai_forecast' ? 'bg-cyan-500/15 text-cyan-400' : 'bg-gray-700 text-gray-400'
            }`}>
              {(forecast as any).method === 'ai_forecast' ? 'AI Forecast' : 'Linear Regression'}
            </span>
          )}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-0">
          <div className="lg:col-span-2 p-4">
            {forecast?.error ? (
              <p className="text-xs text-red-400">{forecast.error}</p>
            ) : (
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={forecastData} margin={{ top: 4, right: 16, bottom: 0, left: -12 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="day" tick={ax} axisLine={al} tickLine={false} interval={Math.max(0, Math.floor(forecastData.length / 8))} />
                  <YAxis tick={ax} axisLine={al} tickLine={false} label={{ value: 'cases / day', angle: -90, position: 'insideLeft', style: { fill: '#4b5563', fontSize: 9 }, offset: 15 }} />
                  <Tooltip content={<DarkTooltip />} />
                  <Area type="monotone" dataKey="upper" stroke="none" fill="#eab30820" name="Upper Bound" />
                  <Area type="monotone" dataKey="lower" stroke="none" fill="#0f1117" name="Lower Bound" />
                  <Line type="monotone" dataKey="actual" name="Daily Cases (Actual)" stroke="#3b82f6" strokeWidth={2} dot={false} connectNulls={false} />
                  <Line type="monotone" dataKey="forecast" name="Daily Cases (Forecast)" stroke="#eab308" strokeWidth={2} strokeDasharray="5 5" dot={false} connectNulls={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
            <div className="flex gap-4 mt-2 text-[10px] text-gray-500">
              <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-blue-500 rounded" /> Actual (60 days)</span>
              <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-yellow-500 rounded" style={{ borderTop: '2px dashed #eab308', height: 0 }} /> Forecast (30 days)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 bg-yellow-500/10 rounded" /> 95% Confidence</span>
            </div>
          </div>
          <div className="lg:border-l border-t lg:border-t-0 border-gray-800 p-4 space-y-3">
            {(() => {
              const predicted = forecast?.summary?.predicted_cases_30d ?? 0
              // Use team data (70-95 range) to match forecast population
              const analystCount = team.analysts.length
              const currentCases = team.analysts.reduce((s, a) => s + a.total, 0)
              return (<>
                <div>
                  <span className="text-[10px] uppercase tracking-wider text-gray-500">Predicted Volume</span>
                  <div className="text-2xl font-semibold text-white mt-1">{predicted.toLocaleString()} <span className="text-sm text-gray-500">total</span></div>
                  <div className="text-[10px] text-gray-500">~{Math.round(predicted / 30)}/day over next 30 days</div>
                </div>
                <div className="border-t border-gray-800 pt-3">
                  <span className="text-[10px] uppercase tracking-wider text-gray-500">Staffing</span>
                  <div className="text-lg font-semibold text-white mt-1">{analystCount} <span className="text-sm text-gray-500">analysts</span></div>
                  <div className="text-[10px] text-gray-500">Currently handling {currentCases} cases (~{analystCount > 0 ? Math.round(currentCases / analystCount) : 0}/analyst)</div>
                </div>
                {sla && (
                  <div className="border-t border-gray-800 pt-3">
                    <span className="text-[10px] uppercase tracking-wider text-gray-500">SLA</span>
                    <div className="flex items-baseline gap-2 mt-1">
                      <span className={`text-lg font-semibold ${sla.pending_over_72h > 10 ? 'text-red-400' : 'text-white'}`}>{sla.pending_over_72h}</span>
                      <span className="text-[10px] text-gray-500">&gt;72h</span>
                      <span className={`text-lg font-semibold ${sla.pending_over_24h > 50 ? 'text-yellow-400' : 'text-white'}`}>{sla.pending_over_24h}</span>
                      <span className="text-[10px] text-gray-500">&gt;24h</span>
                    </div>
                    <div className="text-[10px] text-gray-500">Avg resolution: {sla.avg_resolution_hours.toFixed(0)}h · {fmtUSD(sla.total_pending_exposure)} at risk</div>
                  </div>
                )}
              </>)
            })()}
          </div>
        </div>
      </section>

      {/* ── 4. Mitigation Effectiveness + Risk Hotspots ────── */}
      <Section title="Mitigation Effectiveness (Analyst Decisions)" icon={Shield}>
        <table className="w-full text-xs">
          <thead><tr className="border-b border-gray-800">
            <th className="px-3 py-2 text-left font-medium text-gray-500">Mitigation Step</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">Cases</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">Success Rate</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">Avg Exposure</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">$ Saved</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">Resolved</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">Escalated</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">FP</th>
            <th className="px-3 py-2 text-right font-medium text-gray-500">FN</th>
          </tr></thead>
          <tbody>
            {mitigations.map((m, i) => (
              <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/40 transition">
                <td className="px-3 py-2 text-white font-medium max-w-[200px] truncate" title={m.step}>{m.step}</td>
                <td className="px-3 py-2 text-right text-gray-300 font-mono">{m.total_cases.toLocaleString()}</td>
                <td className="px-3 py-2 text-right font-mono">
                  <span className={m.success_rate >= 70 ? 'text-green-400' : m.success_rate >= 50 ? 'text-yellow-400' : 'text-red-400'}>{m.success_rate}%</span>
                </td>
                <td className="px-3 py-2 text-right text-gray-300 font-mono">{fmtUSD(m.avg_exposure)}</td>
                <td className="px-3 py-2 text-right text-green-400 font-mono font-medium">{fmtUSD(m.exposure_saved)}</td>
                <td className="px-3 py-2 text-right text-blue-400 font-mono">{m.resolved}</td>
                <td className="px-3 py-2 text-right text-purple-400 font-mono">{m.escalated}</td>
                <td className="px-3 py-2 text-right text-yellow-400 font-mono">{m.false_positives || '—'}</td>
                <td className="px-3 py-2 text-right text-red-400 font-mono">{m.false_negatives || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      {/* ── 5. Risk Hotspots (Drivers + Regional) ──────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Section title="Top Fraud Risk Drivers" icon={AlertTriangle} compact>
          <table className="w-full text-xs">
            <thead><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Risk Reason</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Cases</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Esc. Rate</th>
            </tr></thead>
            <tbody>
              {envFactors.risk_reasons.slice(0, 8).map((r, i) => (
                <tr key={i} className="border-b border-gray-800/50">
                  <td className="px-3 py-1.5 text-gray-300 max-w-[180px] truncate" title={r.reason}>{r.reason}</td>
                  <td className="px-3 py-1.5 text-right text-gray-400 font-mono">{r.cases.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right text-orange-400 font-mono">{fmtUSD(r.exposure)}</td>
                  <td className="px-3 py-1.5 text-right font-mono"><span className={r.escalation_rate > 23 ? 'text-red-400' : 'text-gray-400'}>{r.escalation_rate}%</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>

        <Section title="Regional Exposure" icon={DollarSign} compact>
          <table className="w-full text-xs">
            <thead><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Region</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Critical</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Cases</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Esc. Rate</th>
            </tr></thead>
            <tbody>
              {envFactors.regions.map((r, i) => (
                <tr key={i} className="border-b border-gray-800/50">
                  <td className="px-3 py-1.5 text-white font-medium">{r.region}</td>
                  <td className="px-3 py-1.5 text-right text-orange-400 font-mono">{fmtUSD(r.exposure)}</td>
                  <td className="px-3 py-1.5 text-right text-red-400 font-mono">{r.critical_cases}</td>
                  <td className="px-3 py-1.5 text-right text-gray-400 font-mono">{r.cases.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right font-mono"><span className={r.escalation_rate > 23 ? 'text-red-400' : 'text-gray-400'}>{r.escalation_rate}%</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>
      </div>

      {/* ── 6. Team Performance ─────────────────────────────── */}
      <Section title="Team Performance" icon={Users} headerRight={
        <div className="flex gap-3 text-[10px] text-gray-500">
          <span>Resolution: <span className="text-green-400">≥{team.benchmarks.target_resolution_rate}%</span></span>
          <span>Escalation: <span className="text-yellow-400">≤{team.benchmarks.target_escalation_rate}%</span></span>
          <span>Volume: <span className="text-blue-400">{team.benchmarks.target_cases_per_month}/mo</span></span>
        </div>
      }>
        <div className="max-h-[480px] overflow-y-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-[#161922] z-10">
              <tr className="border-b border-gray-800">
                <th className="px-3 py-2 text-left font-medium text-gray-500">Analyst</th>
                <th className="px-3 py-2 text-right font-medium text-gray-500">Total</th>
                <th className="px-3 py-2 text-right font-medium text-gray-500">Pending</th>
                <th className="px-3 py-2 text-right font-medium text-gray-500">Resolution</th>
                <th className="px-3 py-2 text-right font-medium text-gray-500">Escalation</th>
                <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
                <th className="px-3 py-2 text-right font-medium text-gray-500">$ Resolved</th>
                <th className="px-3 py-2 text-center font-medium text-gray-500">Status</th>
              </tr>
            </thead>
            <tbody>
              {team.analysts.map((a, i) => {
                const resOk = a.resolution_rate >= team!.benchmarks.target_resolution_rate
                const escOk = a.escalation_rate <= team!.benchmarks.target_escalation_rate
                const status = resOk && escOk ? 'on-track' : !resOk && !escOk ? 'at-risk' : 'watch'
                return (
                  <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/40 transition">
                    <td className="px-3 py-2">
                      <button
                        onClick={() => {
                          const qs = new URLSearchParams({ analyst: a.analyst, min_score: '70', max_score: '95' })
                          if ((summary as any).ref_month) qs.set('date_from', (summary as any).ref_month)
                          navigate(`/analyst?${qs}`)
                        }}
                        className="text-blue-400 hover:text-blue-300 hover:underline font-medium text-left cursor-pointer"
                        title={`View ${a.analyst}'s cases`}
                      >
                        {a.analyst || '—'}
                      </button>
                    </td>
                    <td className="px-3 py-2 text-right text-gray-300 font-mono">{a.total}</td>
                    <td className={`px-3 py-2 text-right font-mono ${a.pending > 1 ? 'text-yellow-400' : 'text-gray-400'}`}>{a.pending}</td>
                    <td className="px-3 py-2 text-right font-mono"><span className={resOk ? 'text-green-400' : 'text-red-400'}>{a.resolution_rate}%</span></td>
                    <td className="px-3 py-2 text-right font-mono"><span className={escOk ? 'text-green-400' : 'text-red-400'}>{a.escalation_rate}%</span></td>
                    <td className="px-3 py-2 text-right text-orange-400 font-mono">{fmtUSD(a.total_exposure)}</td>
                    <td className="px-3 py-2 text-right text-green-400 font-mono">{fmtUSD(a.exposure_resolved)}</td>
                    <td className="px-3 py-2 text-center">
                      <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${
                        status === 'on-track' ? 'bg-green-500/15 text-green-400' : status === 'at-risk' ? 'bg-red-500/15 text-red-400' : 'bg-yellow-500/15 text-yellow-400'
                      }`}>{status === 'on-track' ? 'On Track' : status === 'at-risk' ? 'At Risk' : 'Watch'}</span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </Section>
    </div>
  )
}

// ── Sub-components ───────────────────────────────────────────

function KPICard({ label, value, icon: Icon, color, prior, current, priorLabel, invertDelta }: {
  label: string; value: string; icon: React.ElementType; color: string
  prior: number | null; current: number | null; priorLabel: string; invertDelta?: boolean
}) {
  return (
    <div className="bg-[#161922] border border-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[10px] uppercase tracking-wider text-gray-500">{label}</span>
        <Icon className={`w-4 h-4 ${color}`} />
      </div>
      <div className="text-xl font-semibold text-white">{value}</div>
      <div className="flex items-center gap-1.5 mt-1">
        <DeltaBadge current={current} prior={prior} invertColor={invertDelta} />
        <span className="text-[10px] text-gray-500">{priorLabel}</span>
      </div>
    </div>
  )
}

function Section({ title, icon: Icon, children, headerRight, compact }: {
  title: string; icon: React.ElementType; children: React.ReactNode; headerRight?: React.ReactNode; compact?: boolean
}) {
  return (
    <section className="bg-[#161922] border border-gray-800 rounded-lg overflow-hidden">
      <div className={`px-4 ${compact ? 'py-2' : 'py-3'} border-b border-gray-800 flex items-center justify-between`}>
        <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider flex items-center gap-1.5">
          <Icon className="w-3.5 h-3.5" />{title}
        </h2>
        {headerRight}
      </div>
      <div className="overflow-x-auto">{children}</div>
    </section>
  )
}

function CmpRow({ label, c, pm, pq, py, suffix, fmt, inv }: {
  label: string; c: number | null; pm: number | null; pq: number | null; py: number | null
  suffix?: string; fmt?: (v: number) => string; inv?: boolean
}) {
  const f = (v: number | null) => { if (v == null) return '—'; return fmt ? fmt(v) : suffix ? `${v}${suffix}` : v.toLocaleString() }
  return (
    <tr className="border-b border-gray-800/50">
      <td className="px-4 py-2 text-gray-300 font-medium">{label}</td>
      <td className="px-4 py-2 text-right text-white font-mono font-medium">{f(c)}</td>
      <td className="px-4 py-2 text-right"><div className="flex items-center justify-end gap-1.5"><span className="text-gray-400 font-mono">{f(pm)}</span><DeltaBadge current={c} prior={pm} invertColor={inv} /></div></td>
      <td className="px-4 py-2 text-right"><div className="flex items-center justify-end gap-1.5"><span className="text-gray-400 font-mono">{f(pq)}</span><DeltaBadge current={c} prior={pq} invertColor={inv} /></div></td>
      <td className="px-4 py-2 text-right"><div className="flex items-center justify-end gap-1.5"><span className="text-gray-400 font-mono">{f(py)}</span><DeltaBadge current={c} prior={py} invertColor={inv} /></div></td>
    </tr>
  )
}
