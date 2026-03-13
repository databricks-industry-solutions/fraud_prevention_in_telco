import { AlertTriangle, Globe, Clock, Smartphone, ShieldAlert, Zap, MapPin, CreditCard } from 'lucide-react'

interface RiskIndicatorsProps {
  riskReason: string | null
  fraudScore: number
  highRisk: boolean
}

const RISK_PATTERNS: { pattern: RegExp; label: string; icon: React.ElementType; severity: 'high' | 'medium' | 'low' }[] = [
  { pattern: /sim\s*swap/i, label: 'SIM Swap', icon: Smartphone, severity: 'high' },
  { pattern: /geo\s*velocity|geo\s*anomal/i, label: 'Geo Anomaly', icon: Globe, severity: 'high' },
  { pattern: /after\s*hours|off.?hours/i, label: 'After Hours', icon: Clock, severity: 'medium' },
  { pattern: /dark\s*web/i, label: 'Dark Web', icon: ShieldAlert, severity: 'high' },
  { pattern: /account\s*takeover/i, label: 'Account Takeover', icon: Zap, severity: 'high' },
  { pattern: /unusual\s*location|location\s*anomal/i, label: 'Location Anomaly', icon: MapPin, severity: 'medium' },
  { pattern: /high\s*value|large\s*amount/i, label: 'High Value', icon: CreditCard, severity: 'medium' },
  { pattern: /velocity|rapid/i, label: 'Velocity Alert', icon: Zap, severity: 'medium' },
]

const severityColor = {
  high: 'bg-red-500/15 border-red-500/30 text-red-400',
  medium: 'bg-amber-500/15 border-amber-500/30 text-amber-400',
  low: 'bg-blue-500/15 border-blue-500/30 text-blue-400',
}

export default function RiskIndicators({ riskReason, fraudScore, highRisk }: RiskIndicatorsProps) {
  const reason = riskReason ?? ''
  const matched: { pattern: RegExp; label: string; icon: React.ElementType; severity: 'high' | 'medium' | 'low' }[] =
    RISK_PATTERNS.filter(p => p.pattern.test(reason))

  // If no patterns matched but there's a reason, show it as a generic indicator
  if (matched.length === 0 && reason) {
    matched.push({ pattern: /.*/, label: reason.slice(0, 30), icon: AlertTriangle, severity: fraudScore >= 80 ? 'high' : 'medium' })
  }

  if (matched.length === 0 && !highRisk) return null

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
      {highRisk && !matched.some(m => m.label === 'High Risk') && (
        <div className={`flex items-center gap-1.5 px-2 py-1.5 rounded border text-xs font-medium ${severityColor.high}`}>
          <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
          High Risk Flag
        </div>
      )}
      {matched.map(({ label, icon: Icon, severity }) => (
        <div key={label} className={`flex items-center gap-1.5 px-2 py-1.5 rounded border text-xs font-medium ${severityColor[severity]}`}>
          <Icon className="w-3.5 h-3.5 shrink-0" />
          {label}
        </div>
      ))}
    </div>
  )
}
