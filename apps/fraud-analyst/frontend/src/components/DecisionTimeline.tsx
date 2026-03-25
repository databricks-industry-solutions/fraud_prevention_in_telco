import { CheckCircle2, XCircle, AlertTriangle, AlertOctagon, Clock } from 'lucide-react'
import type { DecisionHistoryItem } from '../lib/api'

interface DecisionTimelineProps {
  history: DecisionHistoryItem[]
}

const decisionConfig: Record<string, { icon: React.ElementType; color: string; bg: string }> = {
  confirmed_fraud: { icon: AlertOctagon, color: 'text-red-400', bg: 'bg-red-500/20' },
  false_positive: { icon: XCircle, color: 'text-green-400', bg: 'bg-green-500/20' },
  escalated: { icon: AlertTriangle, color: 'text-purple-400', bg: 'bg-purple-500/20' },
  reviewed: { icon: CheckCircle2, color: 'text-blue-400', bg: 'bg-blue-500/20' },
}

export default function DecisionTimeline({ history }: DecisionTimelineProps) {
  if (history.length === 0) {
    return (
      <p className="text-xs text-gray-500 italic">No previous decisions recorded.</p>
    )
  }

  return (
    <div className="relative space-y-0">
      {/* Vertical line */}
      <div className="absolute left-3 top-3 bottom-3 w-px bg-gray-700" />

      {history.map((item, idx) => {
        const config = decisionConfig[item.review_status ?? ''] ?? { icon: Clock, color: 'text-gray-400', bg: 'bg-gray-500/20' }
        const Icon = config.icon
        const date = item.last_review_date ? new Date(item.last_review_date).toLocaleString() : 'Unknown date'

        return (
          <div key={item.transaction_id || idx} className="relative pl-8 pb-4">
            {/* Timeline dot */}
            <div className={`absolute left-1 top-1 w-4 h-4 rounded-full ${config.bg} flex items-center justify-center`}>
              <Icon className={`w-2.5 h-2.5 ${config.color}`} />
            </div>

            {/* Content */}
            <div className="bg-gray-800/50 rounded px-3 py-2">
              <div className="flex items-center justify-between mb-1">
                <span className={`text-xs font-medium ${config.color}`}>
                  {(item.review_status ?? 'unknown').replace(/_/g, ' ')}
                </span>
                <span className="text-[10px] text-gray-500">{date}</span>
              </div>
              <div className="text-[10px] text-gray-400 mb-1">
                by {item.assigned_analyst || 'Unknown'}
              </div>
              {item.analyst_notes && (
                <p className="text-xs text-gray-300 mt-1">{item.analyst_notes}</p>
              )}
              {item.mitigation_steps && (
                <p className="text-[10px] text-gray-500 mt-1">
                  Mitigation: {item.mitigation_steps}
                </p>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}
