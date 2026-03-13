interface ScoreBarProps {
  score: number
  showLabel?: boolean
}

export default function ScoreBar({ score, showLabel = true }: ScoreBarProps) {
  const pct = Math.min(Math.max(score, 0), 100)
  const color = score >= 80 ? 'bg-red-500' : score >= 50 ? 'bg-amber-500' : 'bg-green-500'
  const textColor = score >= 80 ? 'text-red-400' : score >= 50 ? 'text-amber-400' : 'text-green-400'

  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-800 rounded-full h-2 overflow-hidden">
        <div className={`h-full rounded-full ${color} transition-all`} style={{ width: `${pct}%` }} />
      </div>
      {showLabel && (
        <span className={`text-xs font-mono font-semibold ${textColor} w-8 text-right`}>
          {score.toFixed(0)}
        </span>
      )}
    </div>
  )
}
