import { useEffect, useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  DollarSign, ShieldCheck, AlertTriangle, TrendingDown, TrendingUp,
  Loader2, ArrowUpRight, ArrowDownRight, Minus, Target, Zap, Users,
  MessageCircle, Send, X, Minimize2, Maximize2,
} from 'lucide-react'
import {
  Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ComposedChart, Line, Area,
} from 'recharts'
import {
  fetchFinancialSummary, fetchMonthlyFinancials, fetchQuarterlyTrend,
  fetchRegionalTeams, fetchPipelineChanges, fetchRiskDistribution, fetchTopExposureCases,
  type FinancialSummary, type MonthlyFinancial, type QuarterlyTrend,
  type RegionalTeam, type PipelineChange, type RiskBucket, type TopExposureCase,
} from '../lib/api'

const fmtUSD = (v: number) => v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(1)}M` : v >= 1_000 ? `$${(v / 1_000).toFixed(0)}K` : `$${v.toFixed(0)}`
const fmtFull = (v: number) => `$${v.toLocaleString('en-US', { maximumFractionDigits: 0 })}`

function Delta({ current, prior, inv }: { current: number; prior: number; inv?: boolean }) {
  if (!prior) return <span className="text-[10px] text-gray-500"><Minus className="w-3 h-3 inline" />—</span>
  const pct = Math.round(((current - prior) / Math.abs(prior)) * 100)
  if (pct === 0) return <span className="text-[10px] text-gray-500">0%</span>
  const up = pct > 0
  const good = inv ? !up : up
  const I = up ? ArrowUpRight : ArrowDownRight
  return <span className={`text-[10px] inline-flex items-center gap-0.5 ${good ? 'text-green-400' : 'text-red-400'}`}><I className="w-3 h-3" />{Math.abs(pct)}%</span>
}

function Tip({ active, payload, label }: any) {
  if (!active || !payload) return null
  return <div className="bg-[#1e2130] border border-gray-700 rounded px-3 py-2 text-xs shadow-lg">
    <p className="text-gray-400 mb-1">{label}</p>
    {payload.map((p: any) => <p key={p.name} style={{ color: p.color || p.fill || p.stroke }} className="font-mono">{p.name}: {typeof p.value === 'number' ? fmtUSD(p.value) : p.value}</p>)}
  </div>
}

const ax = { fill: '#6b7280', fontSize: 10 }
const al = { stroke: '#374151' }

export default function Analytics() {
  const navigate = useNavigate()
  const [fin, setFin] = useState<FinancialSummary | null>(null)
  const [monthly, setMonthly] = useState<MonthlyFinancial[]>([])
  const [quarters, setQuarters] = useState<QuarterlyTrend[]>([])
  const [teams, setTeams] = useState<RegionalTeam[]>([])
  const [pipeline, setPipeline] = useState<PipelineChange[]>([])
  const [risk, setRisk] = useState<RiskBucket[]>([])
  const [topCases, setTopCases] = useState<TopExposureCase[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetchFinancialSummary(), fetchMonthlyFinancials(), fetchQuarterlyTrend(),
      fetchRegionalTeams(), fetchPipelineChanges(), fetchRiskDistribution(), fetchTopExposureCases(),
    ]).then(([f, m, q, t, p, r, tc]) => {
      setFin(f); setMonthly(m.months); setQuarters(q.quarters); setTeams(t.teams)
      setPipeline(p.changes); setRisk(r.buckets); setTopCases(tc.cases)
    }).finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex items-center justify-center py-20 text-gray-500"><Loader2 className="w-5 h-5 animate-spin mr-2" />Loading executive view…</div>
  if (!fin?.current_month) return null

  const c = fin.current_month, p = fin.prior_month, tgt = fin.targets

  return (
    <div className="space-y-5">
      <div>
        <h1 className="text-lg font-semibold text-white">Fraud Detection P&L</h1>
        <p className="text-xs text-gray-500 mt-0.5">Platform financial performance · Latest month vs prior month{tgt ? ' · Targets from operational plan' : ''}</p>
      </div>

      {/* ── Financial KPIs ─────────────────────────────────── */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
        <KPI label="Net Savings" value={fmtUSD(c.net_savings)} color="text-green-400" border="border-green-500/20" icon={DollarSign} delta={<Delta current={c.net_savings} prior={p.net_savings} />} sub="saved − lost" />
        <KPI label="$ Saved by Engine" value={fmtUSD(c.saved_by_autoblock)} color="text-green-400" icon={ShieldCheck} delta={<Delta current={c.saved_by_autoblock} prior={p.saved_by_autoblock} />} sub={`${c.autoblock_cases.toLocaleString()} blocked`} />
        <KPI label="$ Lost (False Positives)" value={fmtUSD(c.lost_by_fp)} color="text-yellow-400" icon={AlertTriangle} delta={<Delta current={c.lost_by_fp} prior={p.lost_by_fp} inv />} sub={`${c.fp_cases} legitimate blocked · ${c.fp_rate}% FP rate`} />
        <KPI label="$ Lost (Missed Fraud)" value={fmtUSD(c.lost_by_fn)} color="text-red-400" icon={TrendingDown} delta={<Delta current={c.lost_by_fn} prior={p.lost_by_fn} inv />} sub={`${c.fn_cases} missed`} />
        <KPI label="Engine Accuracy" value={`${c.autoblock_accuracy.toFixed(1)}%`} color="text-blue-400" icon={Target} delta={<Delta current={c.autoblock_accuracy} prior={p.autoblock_accuracy} />} sub={`${c.resolution_rate}% resolution rate`} />
      </div>

      {/* ── Exposure + Target Banner ──────────────────────── */}
      <div className="bg-[#161922] border border-gray-800 rounded-lg px-4 py-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Zap className="w-4 h-4 text-orange-400" />
            <span className="text-xs text-gray-400">Total Exposure</span>
            <span className="text-lg font-semibold text-white">{fmtFull(c.total_exposure)}</span>
            <Delta current={c.total_exposure} prior={p.total_exposure} inv />
          </div>
          <div className="flex items-center gap-4 text-xs text-gray-500">
            {tgt && <span>Target: <span className={c.total_exposure <= tgt.target_max_exposure ? 'text-green-400' : 'text-red-400'}>{fmtUSD(tgt.target_max_exposure)}</span></span>}
            <span>Prior: {fmtUSD(p.total_exposure)}</span>
          </div>
        </div>
        {tgt && (
          <div className="mt-2 flex gap-4 text-[10px] text-gray-500">
            <span>Target Autoblock: <span className={c.autoblock_accuracy >= tgt.target_autoblock_rate ? 'text-green-400' : 'text-red-400'}>{tgt.target_autoblock_rate}%</span> (actual: {c.autoblock_accuracy.toFixed(1)}%)</span>
            <span>Target FP Rate: <span className={c.fp_rate <= tgt.target_max_fp_rate ? 'text-green-400' : 'text-red-400'}>≤{tgt.target_max_fp_rate}%</span> (actual: {c.fp_rate}%)</span>
            <span>Target Resolution: <span className={c.resolution_rate >= tgt.target_resolution_rate ? 'text-green-400' : 'text-red-400'}>≥{tgt.target_resolution_rate}%</span> (actual: {c.resolution_rate}%)</span>
          </div>
        )}
      </div>

      {/* ── Quarterly P&L ──────────────────────────────────── */}
      <Sec title="Quarterly P&L" icon={TrendingUp}>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Quarter</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Cases</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
              <th className="px-3 py-2 text-right font-medium text-green-500/70">$ Saved</th>
              <th className="px-3 py-2 text-right font-medium text-yellow-500/70">$ Lost (FP)</th>
              <th className="px-3 py-2 text-right font-medium text-red-500/70">$ Lost (FN)</th>
              <th className="px-3 py-2 text-right font-medium text-blue-500/70">Net Savings</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">QoQ</th>
            </tr></thead>
            <tbody>
              {quarters.map((q, i) => (
                <tr key={i} className="border-b border-gray-800/50">
                  <td className="px-3 py-2 text-white font-medium">{q.quarter}</td>
                  <td className="px-3 py-2 text-right text-gray-300 font-mono">{q.total_cases.toLocaleString()}</td>
                  <td className="px-3 py-2 text-right text-gray-300 font-mono">{fmtUSD(q.total_exposure)}</td>
                  <td className="px-3 py-2 text-right text-green-400 font-mono">{fmtUSD(q.saved)}</td>
                  <td className="px-3 py-2 text-right text-yellow-400 font-mono">{fmtUSD(q.lost_fp)}</td>
                  <td className="px-3 py-2 text-right text-red-400 font-mono">{fmtUSD(q.lost_fn)}</td>
                  <td className="px-3 py-2 text-right text-blue-400 font-mono font-semibold">{fmtUSD(q.net_savings)}</td>
                  <td className="px-3 py-2 text-right">{q.qoq_growth != null ? <span className={q.qoq_growth >= 0 ? 'text-green-400' : 'text-red-400'}>{q.qoq_growth > 0 ? '+' : ''}{q.qoq_growth}%</span> : <span className="text-gray-600">—</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Sec>

      {/* ── Monthly Trend + Pipeline Changes ───────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 bg-[#161922] border border-gray-800 rounded-lg p-4">
          <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Financial Trend</h2>
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart data={monthly.map(m => ({ month: new Date(m.month + '-01').toLocaleDateString('en-US', { month: 'short', year: '2-digit' }), '$ Saved': m.saved, '$ Lost (FP)': m.lost_fp, '$ Lost (FN)': m.lost_fn, 'Net': m.net_savings, 'Target': m.target_exposure }))} margin={{ top: 4, right: 8, bottom: 0, left: -12 }}>
              <defs><linearGradient id="gS" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#22c55e" stopOpacity={0.3} /><stop offset="100%" stopColor="#22c55e" stopOpacity={0.02} /></linearGradient></defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" tick={ax} axisLine={al} tickLine={false} />
              <YAxis tick={ax} axisLine={al} tickLine={false} tickFormatter={(v: number) => fmtUSD(v)} />
              <Tooltip content={<Tip />} />
              <Area type="monotone" dataKey="$ Saved" fill="url(#gS)" stroke="#22c55e" strokeWidth={2} />
              <Bar dataKey="$ Lost (FP)" fill="#eab308" barSize={6} radius={[2, 2, 0, 0]} />
              <Bar dataKey="$ Lost (FN)" fill="#ef4444" barSize={6} radius={[2, 2, 0, 0]} />
              <Line type="monotone" dataKey="Net" stroke="#3b82f6" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="Target" stroke="#6b7280" strokeWidth={1} strokeDasharray="4 4" dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
          <div className="flex gap-3 mt-2 text-[10px] text-gray-500">
            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-green-500" /> Saved</span>
            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-yellow-500" /> Lost (FP)</span>
            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-red-500" /> Lost (FN)</span>
            <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-blue-500 rounded" /> Net</span>
            <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-gray-500 rounded" style={{ borderTop: '1px dashed #6b7280', height: 0 }} /> Target</span>
          </div>
        </div>

        {/* Pipeline Changes */}
        <Sec title="Pipeline Changes (MoM)" icon={TrendingUp} compact>
          <table className="w-full text-xs">
            <thead><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Status</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Current</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Prior</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Change</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">$ Change</th>
            </tr></thead>
            <tbody>
              {pipeline.map((p, i) => (
                <tr key={i} className="border-b border-gray-800/50">
                  <td className="px-3 py-1.5 text-gray-300 font-medium">{p.status?.replace('_', ' ')}</td>
                  <td className="px-3 py-1.5 text-right text-white font-mono">{p.current_cases}</td>
                  <td className="px-3 py-1.5 text-right text-gray-500 font-mono">{p.prior_cases}</td>
                  <td className="px-3 py-1.5 text-right font-mono"><span className={p.case_change > 0 ? 'text-yellow-400' : p.case_change < 0 ? 'text-green-400' : 'text-gray-500'}>{p.case_change > 0 ? '+' : ''}{p.case_change}</span></td>
                  <td className="px-3 py-1.5 text-right font-mono"><span className={p.exposure_change > 0 ? 'text-red-400' : 'text-green-400'}>{p.exposure_change > 0 ? '+' : ''}{fmtUSD(p.exposure_change)}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </Sec>
      </div>

      {/* ── Regional Teams (Manager Proxy) ─────────────────── */}
      <Sec title="Regional Team Performance" icon={Users} headerRight={
        <span className="text-[10px] text-gray-500">Region as team proxy · Click to drill into analyst view</span>
      }>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-[#161922] z-10"><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Region</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Team</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Cases</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
              <th className="px-3 py-2 text-right font-medium text-green-500/70">Net Savings</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Resolution</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Autoblock</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">FP Rate</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Pending</th>
              <th className="px-3 py-2 text-center font-medium text-gray-500">vs Target</th>
            </tr></thead>
            <tbody>
              {teams.map((t, i) => {
                const resOk = t.resolution_rate >= t.target_resolution_rate
                const abOk = t.autoblock_rate >= t.target_autoblock_rate
                const fpOk = t.fp_rate <= t.target_max_fp_rate
                const score = [resOk, abOk, fpOk].filter(Boolean).length
                return (
                  <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/40 cursor-pointer transition" onClick={() => navigate(`/analyst?region=${t.region}`)}>
                    <td className="px-3 py-2 text-blue-400 font-medium hover:underline">{t.region}</td>
                    <td className="px-3 py-2 text-right text-gray-400">{t.analysts}</td>
                    <td className="px-3 py-2 text-right text-gray-300 font-mono">{t.total_cases}</td>
                    <td className="px-3 py-2 text-right text-orange-400 font-mono">{fmtUSD(t.total_exposure)}</td>
                    <td className="px-3 py-2 text-right text-green-400 font-mono font-semibold">{fmtUSD(t.net_savings)}</td>
                    <td className="px-3 py-2 text-right font-mono"><span className={resOk ? 'text-green-400' : 'text-red-400'}>{t.resolution_rate}%</span></td>
                    <td className="px-3 py-2 text-right font-mono"><span className={abOk ? 'text-green-400' : 'text-yellow-400'}>{t.autoblock_rate}%</span></td>
                    <td className="px-3 py-2 text-right font-mono"><span className={fpOk ? 'text-green-400' : 'text-red-400'}>{t.fp_rate}%</span></td>
                    <td className="px-3 py-2 text-right text-yellow-400 font-mono">{t.pending}</td>
                    <td className="px-3 py-2 text-center">
                      <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${score === 3 ? 'bg-green-500/15 text-green-400' : score >= 2 ? 'bg-yellow-500/15 text-yellow-400' : 'bg-red-500/15 text-red-400'}`}>
                        {score === 3 ? 'On Target' : score >= 2 ? 'Watch' : 'Below'}
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </Sec>

      {/* ── Risk Distribution + Top Exposure ────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Sec title="Risk Distribution" icon={AlertTriangle} compact>
          <table className="w-full text-xs">
            <thead><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Severity</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Cases</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Avg/Case</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Share</th>
            </tr></thead>
            <tbody>
              {risk.map((r, i) => (
                <tr key={i} className="border-b border-gray-800/50">
                  <td className="px-3 py-1.5"><span className={`font-medium ${r.category === 'Critical' ? 'text-red-400' : r.category === 'Very High' ? 'text-orange-400' : r.category === 'High' ? 'text-yellow-400' : 'text-gray-300'}`}>{r.category}</span></td>
                  <td className="px-3 py-1.5 text-right text-gray-300 font-mono">{r.case_count.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right text-orange-400 font-mono">{fmtUSD(r.total_exposure)}</td>
                  <td className="px-3 py-1.5 text-right text-gray-400 font-mono">{fmtUSD(r.avg_exposure)}</td>
                  <td className="px-3 py-1.5 text-right text-gray-400 font-mono">{r.pct_of_total}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Sec>

        <Sec title="Highest Exposure — Pending" icon={AlertTriangle} compact>
          <table className="w-full text-xs">
            <thead><tr className="border-b border-gray-800">
              <th className="px-3 py-2 text-left font-medium text-gray-500">Customer</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Exposure</th>
              <th className="px-3 py-2 text-right font-medium text-gray-500">Score</th>
              <th className="px-3 py-2 text-left font-medium text-gray-500">Region</th>
            </tr></thead>
            <tbody>
              {topCases.slice(0, 7).map((tc, i) => (
                <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/40 cursor-pointer transition" onClick={() => navigate(`/cases/${tc.transaction_id}`)}>
                  <td className="px-3 py-1.5 text-white font-medium">{tc.customer_name}</td>
                  <td className="px-3 py-1.5 text-right text-red-400 font-mono font-semibold">{fmtFull(parseFloat(tc.case_exposure_usd))}</td>
                  <td className="px-3 py-1.5 text-right"><span className="bg-red-500/20 text-red-400 px-1.5 py-0.5 rounded font-mono text-[10px]">{parseFloat(tc.fraud_score).toFixed(0)}</span></td>
                  <td className="px-3 py-1.5 text-gray-400">{tc.transaction_region}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Sec>
      </div>

      <GenieChat />
    </div>
  )
}

function GenieChat() {
  const [open, setOpen] = useState(false)
  const [minimized, setMinimized] = useState(false)
  const [messages, setMessages] = useState<{ role: string; content: string }[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [convId, setConvId] = useState<string | null>(null)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => { if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight }, [messages])

  async function send(e: React.FormEvent) {
    e.preventDefault()
    if (!input.trim() || loading) return
    const q = input.trim()
    setMessages(prev => [...prev, { role: 'user', content: q }])
    setInput('')
    setLoading(true)
    try {
      const res = await fetch('/api/executive/genie', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q, conversation_id: convId }),
      })
      const data = await res.json()
      setConvId(data.conversation_id)
      setMessages(prev => [...prev, { role: 'assistant', content: data.reply || 'No response.' }])
    } catch {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Genie unavailable.' }])
    } finally {
      setLoading(false)
    }
  }

  if (!open) return (
    <button onClick={() => setOpen(true)} className="fixed bottom-5 right-5 bg-purple-600 hover:bg-purple-500 text-white rounded-full p-3 shadow-lg transition z-50" title="Ask Genie">
      <MessageCircle className="w-5 h-5" />
    </button>
  )

  return (
    <div className={`fixed bottom-5 right-5 bg-[#161922] border border-gray-700 rounded-lg shadow-2xl z-50 flex flex-col transition-all ${minimized ? 'w-72 h-10' : 'w-96 h-[480px]'}`}>
      <div className="flex items-center justify-between px-3 py-2 border-b border-gray-800 shrink-0">
        <span className="text-xs font-semibold text-gray-300 flex items-center gap-1.5">
          <MessageCircle className="w-3.5 h-3.5 text-purple-400" />
          Genie — Fraud Intelligence
        </span>
        <div className="flex gap-1">
          <button onClick={() => setMinimized(!minimized)} className="p-1 text-gray-500 hover:text-white">
            {minimized ? <Maximize2 className="w-3 h-3" /> : <Minimize2 className="w-3 h-3" />}
          </button>
          <button onClick={() => { setOpen(false); setMinimized(false) }} className="p-1 text-gray-500 hover:text-white"><X className="w-3 h-3" /></button>
        </div>
      </div>
      {!minimized && (<>
        <div ref={scrollRef} className="flex-1 overflow-y-auto px-3 py-2 space-y-3">
          {messages.length === 0 && (
            <div className="text-gray-500 text-xs py-4 space-y-2">
              <p className="text-center">Ask anything about fraud operations data.</p>
              <div className="space-y-1 mt-2">
                {['What is our net savings this quarter?', 'Which region has the highest FP rate?', 'Show pending cases by exposure'].map(q => (
                  <button key={q} onClick={() => setInput(q)} className="block w-full text-left px-2 py-1.5 rounded text-[10px] text-gray-400 hover:text-purple-300 hover:bg-purple-500/10 transition truncate">{q}</button>
                ))}
              </div>
            </div>
          )}
          {messages.map((m, i) => (
            <div key={i} className={`text-xs leading-relaxed ${m.role === 'user' ? 'bg-purple-600/20 text-purple-200 rounded-lg px-3 py-2 ml-8' : 'text-gray-300 pr-8'}`}>
              <div className="whitespace-pre-wrap font-mono">{m.content}</div>
            </div>
          ))}
          {loading && <div className="flex items-center gap-1.5 text-xs text-gray-500"><Loader2 className="w-3 h-3 animate-spin" />Querying data...</div>}
        </div>
        <form onSubmit={send} className="border-t border-gray-800 px-3 py-2 shrink-0">
          <div className="flex gap-2">
            <input type="text" value={input} onChange={e => setInput(e.target.value)} placeholder="Ask about fraud data..." className="flex-1 bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:border-purple-500" disabled={loading} />
            <button type="submit" disabled={!input.trim() || loading} className="bg-purple-600 hover:bg-purple-500 disabled:bg-gray-700 text-white rounded p-1.5 transition"><Send className="w-3.5 h-3.5" /></button>
          </div>
        </form>
      </>)}
    </div>
  )
}

function KPI({ label, value, color, border, icon: I, delta, sub }: { label: string; value: string; color: string; border?: string; icon: React.ElementType; delta: React.ReactNode; sub: string }) {
  return <div className={`bg-[#161922] border ${border || 'border-gray-800'} rounded-lg p-4`}>
    <div className="flex items-center justify-between mb-1"><span className="text-[10px] uppercase tracking-wider text-gray-500">{label}</span><I className={`w-4 h-4 ${color}`} /></div>
    <div className={`text-xl font-semibold ${color}`}>{value}</div>
    <div className="flex items-center gap-1.5 mt-1">{delta}<span className="text-[10px] text-gray-500">vs prior month</span></div>
    <div className="text-[10px] text-gray-500 mt-0.5">{sub}</div>
  </div>
}

function Sec({ title, icon: I, children, headerRight, compact }: { title: string; icon: React.ElementType; children: React.ReactNode; headerRight?: React.ReactNode; compact?: boolean }) {
  return <section className="bg-[#161922] border border-gray-800 rounded-lg overflow-hidden">
    <div className={`px-4 ${compact ? 'py-2' : 'py-3'} border-b border-gray-800 flex items-center justify-between`}>
      <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider flex items-center gap-1.5"><I className="w-3.5 h-3.5" />{title}</h2>
      {headerRight}
    </div>
    <div className="overflow-x-auto">{children}</div>
  </section>
}
