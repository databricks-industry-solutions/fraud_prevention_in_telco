import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  DollarSign,
  TrendingUp,
  MapPin,
  Shield,
  Loader2,
  CheckCircle2,
  XCircle,
  Clock,
} from 'lucide-react'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
  Label,
  BarChart,
  Bar,
} from 'recharts'
import {
  fetchDashboardStats,
  fetchTrends,
  fetchGeo,
  fetchTopRisk,
  type DashboardStats,
  type TrendDay,
  type GeoRegion,
  type TopRiskCase,
} from '../lib/api'

function StatCard({
  label,
  value,
  icon: Icon,
  color,
}: {
  label: string
  value: string | number
  icon: React.ElementType
  color: string
}) {
  return (
    <div className="bg-[#161922] border border-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[10px] uppercase tracking-wider text-gray-500">
          {label}
        </span>
        <Icon className={`w-4 h-4 ${color}`} />
      </div>
      <div className="text-xl font-semibold text-white">{value}</div>
    </div>
  )
}

function DarkTooltip({ active, payload, label }: any) {
  if (!active || !payload) return null
  return (
    <div className="bg-[#1e2130] border border-gray-700 rounded px-3 py-2 text-xs shadow-lg">
      <p className="text-gray-400 mb-1">{label}</p>
      {payload.map((p: any) => (
        <p key={p.name} style={{ color: p.color }} className="font-mono">
          {p.name}: {p.value.toLocaleString()}
        </p>
      ))}
    </div>
  )
}

function RegionTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  const data = payload[0].payload
  return (
    <div className="bg-[#1e2130] border border-gray-700 rounded px-3 py-2 text-xs shadow-lg">
      <p className="text-gray-400 mb-1">{label}</p>
      <p className="text-blue-400 font-mono">Cases: {data.cases.toLocaleString()}</p>
      <p className="text-green-400 font-mono">
        Exposure: ${(data.exposure / 1_000_000).toFixed(1)}M
      </p>
      <p className="text-amber-400 font-mono">Avg Score: {data.avgScore.toFixed(1)}</p>
      <p className="text-red-400 font-mono">Fraud: {data.fraud.toLocaleString()}</p>
    </div>
  )
}

function PieCenterLabel({ viewBox, total }: any) {
  if (!viewBox || viewBox.cx == null) return null
  const { cx, cy } = viewBox
  return (
    <text x={cx} y={cy} textAnchor="middle" dominantBaseline="central">
      <tspan x={cx} dy="-0.4em" fill="#ffffff" fontSize={18} fontWeight={600}>
        {total.toLocaleString()}
      </tspan>
      <tspan x={cx} dy="1.4em" fill="#6b7280" fontSize={10}>
        total
      </tspan>
    </text>
  )
}

export default function Dashboard() {
  const navigate = useNavigate()
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [trends, setTrends] = useState<TrendDay[]>([])
  const [regions, setRegions] = useState<GeoRegion[]>([])
  const [topRisk, setTopRisk] = useState<TopRiskCase[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetchDashboardStats(),
      fetchTrends(),
      fetchGeo(),
      fetchTopRisk(),
    ])
      .then(([s, t, g, r]) => {
        setStats(s)
        setTrends(t.trends.slice(0, 14)) // last 14 days
        setRegions(g.regions)
        setTopRisk(r.cases)
      })
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20 text-gray-500">
        <Loader2 className="w-5 h-5 animate-spin mr-2" />
        Loading dashboard...
      </div>
    )
  }

  if (!stats) return null

  // Prepare trend data (parse strings to numbers, format dates)
  const trendData = [...trends].reverse().map((t) => ({
    day: new Date(t.day + 'T00:00').toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
    }),
    cases: parseInt(t.case_count),
    fraud: parseInt(t.fraud_count),
    avgScore: parseFloat(t.avg_score),
  }))

  // Status distribution for pie chart
  const statusData = [
    { name: 'Pending', value: stats.pending_count, color: '#eab308' },
    { name: 'Confirmed Fraud', value: stats.confirmed_fraud_count, color: '#ef4444' },
    { name: 'False Positive', value: stats.false_positive_count, color: '#22c55e' },
    { name: 'Escalated', value: stats.escalated_count, color: '#a855f7' },
  ].filter((d) => d.value > 0)

  // Region data for bar chart
  const regionData = regions.map((r) => ({
    region: r.transaction_region,
    cases: parseInt(r.case_count),
    exposure: parseFloat(r.total_exposure),
    avgScore: parseFloat(r.avg_score),
    fraud: parseInt(r.fraud_count),
  }))

  const axisStyle = { fill: '#6b7280', fontSize: 10 }
  const axisLineStyle = { stroke: '#374151' }

  return (
    <div className="space-y-6">
      <h1 className="text-lg font-semibold text-white">Dashboard</h1>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          label="Total Cases"
          value={stats.total_cases.toLocaleString()}
          icon={Activity}
          color="text-blue-400"
        />
        <StatCard
          label="Pending Review"
          value={stats.pending_count.toLocaleString()}
          icon={Clock}
          color="text-yellow-400"
        />
        <StatCard
          label="Confirmed Fraud"
          value={stats.confirmed_fraud_count.toLocaleString()}
          icon={AlertTriangle}
          color="text-red-400"
        />
        <StatCard
          label="Total Exposure"
          value={`$${(stats.total_exposure_usd / 1_000_000).toFixed(1)}M`}
          icon={DollarSign}
          color="text-green-400"
        />
      </div>

      {/* Second stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          label="Avg Fraud Score"
          value={stats.avg_fraud_score.toFixed(1)}
          icon={TrendingUp}
          color="text-amber-400"
        />
        <StatCard
          label="High Risk"
          value={stats.high_risk_count.toLocaleString()}
          icon={Shield}
          color="text-red-400"
        />
        <StatCard
          label="False Positives"
          value={stats.false_positive_count.toLocaleString()}
          icon={XCircle}
          color="text-green-400"
        />
        <StatCard
          label="Escalated"
          value={stats.escalated_count.toLocaleString()}
          icon={CheckCircle2}
          color="text-purple-400"
        />
      </div>

      {/* Charts grid — trend + status pie */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Trend Area Chart (spans 2 cols) */}
        <section className="lg:col-span-2 bg-[#161922] border border-gray-800 rounded-lg p-4">
          <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
            <TrendingUp className="w-3.5 h-3.5" />
            Daily Trends (14 days)
          </h2>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={trendData} margin={{ top: 4, right: 4, bottom: 0, left: -12 }}>
              <defs>
                <linearGradient id="gradCases" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="gradFraud" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#ef4444" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#ef4444" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="day"
                tick={axisStyle}
                axisLine={axisLineStyle}
                tickLine={false}
              />
              <YAxis
                tick={axisStyle}
                axisLine={axisLineStyle}
                tickLine={false}
              />
              <Tooltip content={<DarkTooltip />} />
              <Area
                type="monotone"
                dataKey="cases"
                name="Cases"
                stroke="#3b82f6"
                strokeWidth={2}
                fill="url(#gradCases)"
              />
              <Area
                type="monotone"
                dataKey="fraud"
                name="Fraud"
                stroke="#ef4444"
                strokeWidth={2}
                fill="url(#gradFraud)"
              />
            </AreaChart>
          </ResponsiveContainer>
          <div className="flex gap-4 mt-2 text-[10px] text-gray-500">
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-blue-500" /> Cases
            </span>
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-red-500" /> Fraud
            </span>
          </div>
        </section>

        {/* Status Pie Chart */}
        <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
          <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
            <Activity className="w-3.5 h-3.5" />
            Status Breakdown
          </h2>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie
                data={statusData}
                cx="50%"
                cy="50%"
                innerRadius={55}
                outerRadius={80}
                paddingAngle={3}
                dataKey="value"
                stroke="none"
              >
                {statusData.map((entry, i) => (
                  <Cell key={i} fill={entry.color} />
                ))}
                <Label
                  content={<PieCenterLabel total={stats.total_cases} />}
                  position="center"
                />
              </Pie>
              <Tooltip content={<DarkTooltip />} />
              <Legend
                verticalAlign="bottom"
                iconSize={8}
                formatter={(value: string) => (
                  <span className="text-[10px] text-gray-400">{value}</span>
                )}
              />
            </PieChart>
          </ResponsiveContainer>
        </section>
      </div>

      {/* Regional Bar Chart */}
      <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
        <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
          <MapPin className="w-3.5 h-3.5" />
          Regional Breakdown
        </h2>
        <ResponsiveContainer width="100%" height={Math.max(220, regionData.length * 36)}>
          <BarChart
            data={regionData}
            layout="vertical"
            margin={{ top: 0, right: 20, bottom: 0, left: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
            <XAxis
              type="number"
              tick={axisStyle}
              axisLine={axisLineStyle}
              tickLine={false}
            />
            <YAxis
              type="category"
              dataKey="region"
              tick={axisStyle}
              axisLine={axisLineStyle}
              tickLine={false}
              width={100}
            />
            <Tooltip content={<RegionTooltip />} />
            <Bar dataKey="cases" fill="#3b82f6" radius={[0, 4, 4, 0]} barSize={18} />
          </BarChart>
        </ResponsiveContainer>
      </section>

      {/* Top Risk Cases */}
      <section className="bg-[#161922] border border-gray-800 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider flex items-center gap-1.5">
            <AlertTriangle className="w-3.5 h-3.5 text-red-500" />
            Top Risk — Priority Queue
          </h2>
        </div>
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="px-4 py-2 text-left font-medium text-gray-500">Customer</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Score</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Exposure</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Risk Reason</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Region</th>
            </tr>
          </thead>
          <tbody>
            {topRisk.map((c) => (
              <tr
                key={c.transaction_id}
                className="border-b border-gray-800/50 hover:bg-gray-800/40 cursor-pointer transition"
                onClick={() => navigate(`/cases/${c.transaction_id}`)}
              >
                <td className="px-4 py-2">
                  <div className="text-white">{c.customer_name}</div>
                  <div className="text-gray-500 text-[10px] font-mono">
                    {c.transaction_id}
                  </div>
                </td>
                <td className="px-4 py-2">
                  <span className="bg-red-500/20 text-red-400 border border-red-500/30 px-1.5 py-0.5 rounded font-mono font-medium">
                    {parseFloat(c.fraud_score).toFixed(1)}
                  </span>
                </td>
                <td className="px-4 py-2 text-gray-300 font-mono">
                  ${parseFloat(c.case_exposure_usd).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </td>
                <td className="px-4 py-2 text-gray-400">{c.risk_reason_engine}</td>
                <td className="px-4 py-2 text-gray-400">{c.transaction_region}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  )
}
