import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'
import type { CustomerHistoryItem } from '../lib/api'

interface CustomerSparklineProps {
  history: CustomerHistoryItem[]
  currentTxnId: string
}

function SparkTooltip({ active, payload }: any) {
  if (!active || !payload?.[0]) return null
  const d = payload[0].payload
  return (
    <div className="bg-[#1e2130] border border-gray-700 rounded px-2 py-1.5 text-[10px] shadow-lg">
      <p className="text-gray-400">{d.date}</p>
      <p className="text-white font-mono">Score: {d.score.toFixed(1)}</p>
      <p className="text-gray-300 font-mono">${d.cost.toFixed(2)}</p>
      <p className="text-gray-500">{d.type}</p>
      {d.isCurrent && <p className="text-amber-400 font-medium">← Current case</p>}
    </div>
  )
}

export default function CustomerSparkline({ history, currentTxnId }: CustomerSparklineProps) {
  if (history.length < 2) {
    return (
      <p className="text-xs text-gray-500 italic">Not enough transaction history for this customer.</p>
    )
  }

  const data = [...history].reverse().map(h => ({
    date: h.transaction_date ? new Date(h.transaction_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '',
    score: parseFloat(h.fraud_score ?? '0'),
    cost: parseFloat(h.transaction_cost ?? '0'),
    type: h.transaction_type ?? '',
    status: h.review_status ?? '',
    isCurrent: h.transaction_id === currentTxnId,
  }))

  return (
    <div>
      <ResponsiveContainer width="100%" height={120}>
        <AreaChart data={data} margin={{ top: 5, right: 5, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="scoreGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#ef4444" stopOpacity={0.3} />
              <stop offset="100%" stopColor="#ef4444" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis
            dataKey="date"
            tick={{ fill: '#6b7280', fontSize: 9 }}
            axisLine={{ stroke: '#374151' }}
            tickLine={false}
          />
          <YAxis
            domain={[0, 100]}
            tick={{ fill: '#6b7280', fontSize: 9 }}
            axisLine={false}
            tickLine={false}
            width={25}
          />
          <Tooltip content={<SparkTooltip />} />
          <ReferenceLine y={80} stroke="#ef4444" strokeDasharray="3 3" strokeOpacity={0.4} />
          <ReferenceLine y={50} stroke="#eab308" strokeDasharray="3 3" strokeOpacity={0.3} />
          <Area
            type="monotone"
            dataKey="score"
            stroke="#ef4444"
            fill="url(#scoreGrad)"
            strokeWidth={1.5}
          />
        </AreaChart>
      </ResponsiveContainer>
      <div className="flex gap-3 mt-1 text-[10px] text-gray-500">
        <span>{data.length} transactions</span>
        <span>Avg score: {(data.reduce((s, d) => s + d.score, 0) / data.length).toFixed(1)}</span>
      </div>
    </div>
  )
}
