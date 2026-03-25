interface StatusBadgeProps {
  status: string | null
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  const colorMap: Record<string, string> = {
    pending_review: 'bg-yellow-500/20 text-yellow-400',
    confirmed_fraud: 'bg-red-500/20 text-red-400',
    false_positive: 'bg-green-500/20 text-green-400',
    escalated: 'bg-purple-500/20 text-purple-400',
    reviewed: 'bg-blue-500/20 text-blue-400',
  }
  const colors = colorMap[status ?? ''] ?? 'bg-gray-500/20 text-gray-400'

  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${colors}`}>
      {(status ?? 'unknown').replace(/_/g, ' ')}
    </span>
  )
}
