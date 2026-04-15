import { useState, useEffect, useCallback, type CSSProperties } from 'react'
import {
  Shield, AlertTriangle, CheckCircle, XCircle, Clock, Bot, User,
  Play, RotateCcw, Zap, MapPin, Cpu, Smartphone,
  ShieldCheck, ShieldX, ShieldAlert, ArrowRight, Loader2, Lightbulb,
  Network, Signal, Phone, History, Database,
} from 'lucide-react'

/* ═══════════════════════════════════════════════════════════════
   Constants & Types
   ═══════════════════════════════════════════════════════════════ */

interface CDRRule {
  id: string
  name: string
  description: string
  result: 'pass' | 'fail' | 'warn'
  detail: string
}

interface DeviceTrust {
  label: string
  score: number
  detail: string
}

interface FraudCase {
  id: number
  title: string
  customer: string
  transactionType: string
  amount: string
  location: string
  locationDetail: string
  time: string
  outcome: 'blocked' | 'review' | 'passed'
  score: number
  rules: CDRRule[]
  device: DeviceTrust
  agentSummary?: string
  assignedAnalyst?: string
  recommendation: { title: string; body: string }
}

const CASES: FraudCase[] = [
  {
    id: 1,
    title: 'International SIM Swap Fraud',
    customer: 'Ahmed K.',
    transactionType: 'SIM Swap + Device Purchase',
    amount: '$2,450',
    location: 'Lagos, NG → London, GB',
    locationDetail: 'Two countries in 45 minutes',
    time: '02:14 AM local',
    outcome: 'blocked',
    score: 98.7,
    rules: [
      { id: 'R1', name: 'Impossible Travel', description: 'Consecutive locations physically unreachable', result: 'fail', detail: 'Lagos → London in 45 min · 5,100 km · 6,800 km/h implied speed' },
      { id: 'R5', name: 'Cell / IP Country Mismatch', description: 'Cell tower country differs from IP geolocation', result: 'fail', detail: 'Cell tower: Nigeria (NG) · IP geolocation: United Kingdom (GB)' },
      { id: 'R7', name: 'Rapid Cell Tower Hop', description: 'Multiple distant towers in short window', result: 'fail', detail: '3 tower changes across 120 km in under 2 minutes' },
      { id: 'R13', name: 'Roaming Anomaly', description: 'Multiple countries in short timeframe', result: 'fail', detail: '2 countries visited within 45 minutes while roaming' },
    ],
    device: { label: 'Untrusted', score: 18, detail: 'New device · No encryption · SELinux disabled' },
    agentSummary: 'High-confidence fraud detected. All four CDR rules triggered simultaneously — impossible travel from Lagos to London (6,800 km/h implied speed), cell tower country (NG) mismatches IP geolocation (GB), rapid cell tower hopping across 120 km in 2 minutes, and multi-country roaming within a 45-minute window. Device is previously unseen with no encryption and SELinux disabled. This pattern is consistent with a SIM cloning attack where the cloned SIM is activated on a compromised device abroad. Transaction auto-blocked. Customer notification sent via SMS to registered backup number.',
    recommendation: { title: 'Enforce Multi-Factor SIM Verification', body: 'CDR analysis flagged impossible travel on this SIM swap request. Implementing mandatory biometric verification for all SIM change requests could prevent 89% of similar fraud attempts.' },
  },
  {
    id: 2,
    title: 'Suspicious After-Hours Activity',
    customer: 'Maria S.',
    transactionType: 'Premium Service Activation',
    amount: '$890',
    location: 'São Paulo, BR',
    locationDetail: 'Known home region, unusual timing',
    time: '03:47 AM local',
    outcome: 'review',
    score: 78.3,
    rules: [
      { id: 'R1', name: 'Impossible Travel', description: 'Consecutive locations physically unreachable', result: 'pass', detail: 'All events within São Paulo metro area · Max 12 km distance' },
      { id: 'R5', name: 'Cell / IP Country Mismatch', description: 'Cell tower country differs from IP geolocation', result: 'warn', detail: 'Cell tower: Brazil (BR) · IP geolocation: Brazil (BR) via VPN endpoint' },
      { id: 'R7', name: 'Rapid Cell Tower Hop', description: 'Multiple distant towers in short window', result: 'pass', detail: 'Normal movement pattern · 3 towers over 2 hours' },
      { id: 'R13', name: 'Roaming Anomaly', description: 'Multiple countries in short timeframe', result: 'pass', detail: 'Single country (BR) · No roaming detected' },
    ],
    device: { label: 'Medium Trust', score: 62, detail: 'Known device · VPN active · Encrypted' },
    agentSummary: 'Mixed signals detected. Location pattern is consistent with customer\'s home area in São Paulo, but the transaction was initiated at 3:47 AM through an active VPN connection — unusual for this subscriber\'s profile. Premium service activation of $890 is 3.2x higher than their average transaction. CDR records show normal cell tower progression but the VPN masks the true originating IP. Recommend manual verification — this could be legitimate late-night usage or an account compromise through credential theft.',
    assignedAnalyst: 'Priya R.',
    recommendation: { title: 'Deploy VPN-Aware Risk Scoring', body: '12% of uncertain cases involve active VPN connections. Training risk models on VPN usage patterns alongside CDR data would reduce false positives by an estimated 34% while catching more account compromises.' },
  },
  {
    id: 3,
    title: 'Routine Device Upgrade',
    customer: 'James L.',
    transactionType: 'Device Upgrade',
    amount: '$1,200',
    location: 'Chicago, IL',
    locationDetail: 'Matches home address on file',
    time: '2:15 PM local',
    outcome: 'passed',
    score: 12.4,
    rules: [
      { id: 'R1', name: 'Impossible Travel', description: 'Consecutive locations physically unreachable', result: 'pass', detail: 'All events within 5 km of registered home address' },
      { id: 'R5', name: 'Cell / IP Country Mismatch', description: 'Cell tower country differs from IP geolocation', result: 'pass', detail: 'Cell tower: United States (US) · IP geolocation: United States (US)' },
      { id: 'R7', name: 'Rapid Cell Tower Hop', description: 'Multiple distant towers in short window', result: 'pass', detail: 'Single tower connection · Stable for 3+ hours' },
      { id: 'R13', name: 'Roaming Anomaly', description: 'Multiple countries in short timeframe', result: 'pass', detail: 'Single country (US) · No roaming · Known carrier' },
    ],
    device: { label: 'Trusted', score: 94, detail: 'Known device · Encrypted · SELinux enforcing · MFA active' },
    agentSummary: 'No fraud indicators found. All CDR rules passed cleanly — subscriber remained within 5 km of registered home address in Chicago, cell tower and IP geolocation both confirm United States, single stable tower connection for 3+ hours with no hopping, and no roaming activity detected. Device is a known handset with full encryption, SELinux enforcing, and active MFA. Transaction amount of $1,200 is within normal range for device upgrades on this account. Historical pattern shows this customer upgrades every 18 months — last upgrade was 19 months ago. Transaction approved with zero friction.',
    recommendation: { title: 'Automate CDR Pattern Learning', body: 'The engine\'s cell-tower analysis correctly identified normal commute patterns. Extending this to learn per-subscriber movement baselines would further reduce the manual review queue by an estimated 23%.' },
  },
]

const TOTAL_STEPS = { blocked: 15, review: 15, passed: 15 }
const STEP_MS = 650

/* ═══════════════════════════════════════════════════════════════
   Animation helpers
   ═══════════════════════════════════════════════════════════════ */

const fade = (vis: boolean): CSSProperties => ({
  opacity: vis ? 1 : 0,
  transform: vis ? 'translateY(0)' : 'translateY(14px)',
  transition: 'all 0.5s cubic-bezier(0.4,0,0.2,1)',
})

const popIn = (vis: boolean): CSSProperties => ({
  opacity: vis ? 1 : 0,
  transform: vis ? 'scale(1)' : 'scale(0.7)',
  transition: 'all 0.45s cubic-bezier(0.34,1.56,0.64,1)',
})

const outcomeColor = {
  blocked: { bg: 'bg-red-500/10', border: 'border-red-500/30', text: 'text-red-400', glow: 'shadow-red-500/20' },
  review: { bg: 'bg-amber-500/10', border: 'border-amber-500/30', text: 'text-amber-400', glow: 'shadow-amber-500/20' },
  passed: { bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', text: 'text-emerald-400', glow: 'shadow-emerald-500/20' },
}

const outcomeLabel = { blocked: 'TRANSACTION BLOCKED', review: 'REVIEW REQUIRED', passed: 'TRANSACTION APPROVED' }
const outcomeIcon = { blocked: ShieldX, review: ShieldAlert, passed: ShieldCheck }

/* ═══════════════════════════════════════════════════════════════
   Sub-components
   ═══════════════════════════════════════════════════════════════ */

function RuleCheck({ rule, visible, evaluating }: { rule: CDRRule; visible: boolean; evaluating: boolean }) {
  const color = rule.result === 'pass' ? 'text-emerald-400' : rule.result === 'fail' ? 'text-red-400' : 'text-amber-400'
  const Icon = rule.result === 'pass' ? CheckCircle : rule.result === 'fail' ? XCircle : AlertTriangle
  const label = rule.result === 'pass' ? 'PASS' : rule.result === 'fail' ? 'FAIL' : 'WARN'

  return (
    <div style={fade(visible)} className="flex items-start gap-2.5 py-2 border-b border-gray-800/50 last:border-0">
      <div className="mt-0.5 shrink-0">
        {evaluating ? (
          <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />
        ) : (
          <Icon className={`w-4 h-4 ${color}`} />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-mono text-gray-500">{rule.id}</span>
          <span className="text-xs font-medium text-gray-200">{rule.name}</span>
          {!evaluating && (
            <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${
              rule.result === 'pass' ? 'bg-emerald-500/15 text-emerald-400'
                : rule.result === 'fail' ? 'bg-red-500/15 text-red-400'
                : 'bg-amber-500/15 text-amber-400'
            }`}>{label}</span>
          )}
        </div>
        {!evaluating && (
          <p className="text-[10px] text-gray-500 mt-0.5 leading-relaxed">{rule.detail}</p>
        )}
      </div>
    </div>
  )
}

function ScoreBar({ score, visible }: { score: number; visible: boolean }) {
  const color = score >= 95 ? 'bg-red-500' : score >= 70 ? 'bg-amber-500' : 'bg-emerald-500'
  const textColor = score >= 95 ? 'text-red-400' : score >= 70 ? 'text-amber-400' : 'text-emerald-400'
  return (
    <div style={fade(visible)} className="py-2">
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-[10px] uppercase tracking-wider text-gray-500">Fraud Score</span>
        <span className={`text-lg font-bold font-mono ${textColor}`}>{visible ? score.toFixed(1) : '—'}</span>
      </div>
      <div className="h-2 bg-gray-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${color} transition-all duration-1000 ease-out`}
          style={{ width: visible ? `${score}%` : '0%' }}
        />
      </div>
      <div className="flex justify-between mt-1 text-[9px] text-gray-600 font-mono">
        <span>0</span>
        <span className="text-amber-600">70 — Flagged</span>
        <span className="text-red-600">95 — Auto-block</span>
        <span>100</span>
      </div>
    </div>
  )
}

function DecisionPopup({ outcome, visible }: { outcome: 'blocked' | 'review' | 'passed'; visible: boolean }) {
  const c = outcomeColor[outcome]
  const Icon = outcomeIcon[outcome]
  return (
    <div
      style={popIn(visible)}
      className={`absolute inset-0 z-20 flex items-center justify-center backdrop-blur-sm rounded-xl pointer-events-none`}
    >
      <div className={`${c.bg} ${c.border} border-2 rounded-2xl px-8 py-6 text-center shadow-2xl ${c.glow}`}>
        <Icon className={`w-12 h-12 ${c.text} mx-auto mb-3`} />
        <div className={`text-lg font-bold ${c.text} tracking-wide`}>{outcomeLabel[outcome]}</div>
        <div className="text-xs text-gray-400 mt-1">
          {outcome === 'blocked' && 'Engine auto-blocked · Customer notified'}
          {outcome === 'review' && 'Escalated to AgentBricks for analysis'}
          {outcome === 'passed' && 'All checks clear · Transaction processed'}
        </div>
      </div>
    </div>
  )
}

function AgentSection({ summary, analyst, outcome, step, agentStartStep }: {
  summary: string; analyst?: string; outcome: 'blocked' | 'review' | 'passed'; step: number; agentStartStep: number
}) {
  const headerVisible = step >= agentStartStep
  const typingActive = step >= agentStartStep + 1
  const actionVisible = step >= agentStartStep + 3

  const [typedChars, setTypedChars] = useState(0)
  useEffect(() => {
    if (!typingActive) { setTypedChars(0); return }
    if (typedChars >= summary.length) return
    const timer = setTimeout(() => setTypedChars(c => Math.min(c + 2, summary.length)), 18)
    return () => clearTimeout(timer)
  }, [typedChars, typingActive, summary.length])

  return (
    <div style={fade(headerVisible)} className="mt-3 border border-purple-500/20 bg-purple-500/5 rounded-lg p-3">
      <div className="flex items-center gap-2 mb-2">
        <Bot className="w-4 h-4 text-purple-400" />
        <span className="text-xs font-semibold text-purple-300">AgentBricks — Case Analysis</span>
        {typingActive && typedChars < summary.length && (
          <Loader2 className="w-3 h-3 text-purple-400 animate-spin" />
        )}
      </div>
      {typingActive && (
        <p className="text-[11px] text-gray-300 leading-relaxed font-mono">
          {summary.slice(0, typedChars)}
          {typedChars < summary.length && <span className="inline-block w-1.5 h-3 bg-purple-400 ml-0.5 animate-pulse" />}
        </p>
      )}
      <div style={fade(actionVisible)} className={`mt-3 flex items-center gap-2 rounded px-3 py-2 ${
        outcome === 'blocked' ? 'bg-red-500/10' : outcome === 'passed' ? 'bg-emerald-500/10' : 'bg-purple-500/10'
      }`}>
        {outcome === 'blocked' && (<>
          <ShieldX className="w-3.5 h-3.5 text-red-300" />
          <span className="text-xs text-red-200">Transaction blocked · Customer notified via SMS</span>
          <ArrowRight className="w-3 h-3 text-red-400 ml-auto" />
          <span className="text-[10px] text-red-400">Alert Log</span>
        </>)}
        {outcome === 'review' && (<>
          <User className="w-3.5 h-3.5 text-purple-300" />
          <span className="text-xs text-purple-200">Assigned to <strong>{analyst}</strong></span>
          <a
            href="https://fraud-analyst-7474656585748611.aws.databricksapps.com/analyst"
            target="_blank"
            rel="noopener noreferrer"
            className="ml-auto flex items-center gap-1 px-2.5 py-1 rounded bg-purple-500/20 hover:bg-purple-500/40 text-purple-300 hover:text-white transition cursor-pointer"
          >
            <ArrowRight className="w-3 h-3" />
            <span className="text-[10px] font-semibold">Case Queue</span>
          </a>
        </>)}
        {outcome === 'passed' && (<>
          <ShieldCheck className="w-3.5 h-3.5 text-emerald-300" />
          <span className="text-xs text-emerald-200">Transaction approved · Zero friction</span>
          <ArrowRight className="w-3 h-3 text-emerald-400 ml-auto" />
          <span className="text-[10px] text-emerald-400">Completed</span>
        </>)}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════
   Case Simulation — two-column layout
   ═══════════════════════════════════════════════════════════════ */

function CaseSimulation({ fraudCase, playing }: {
  fraudCase: FraudCase; playing: boolean
}) {
  const [step, setStep] = useState(-1)
  const [started, setStarted] = useState(false)
  const maxStep = TOTAL_STEPS[fraudCase.outcome]

  useEffect(() => {
    if (!playing) { setStarted(false); setStep(-1); return }
    setStarted(true)
    setStep(0)
  }, [playing])

  useEffect(() => {
    if (!started || step < 0 || step >= maxStep) return
    const delay = step === 8 ? 1200 : STEP_MS
    const t = setTimeout(() => setStep(s => s + 1), delay)
    return () => clearTimeout(t)
  }, [step, started, maxStep])

  const c = outcomeColor[fraudCase.outcome]
  const showTxn = step >= 0
  const showScanHeader = step >= 1
  const ruleSteps = [2, 3, 4, 5]
  const showDevice = step >= 6
  const showScore = step >= 7
  const showDecision = step >= 9
  const showDecisionDismiss = step >= 10
  const agentStartStep = 11

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
      {/* ── Left: Transaction Analysis ── */}
      <div className={`relative bg-[#161922] border ${c.border} rounded-xl overflow-hidden flex flex-col`}>
        <div className={`px-4 py-3 border-b border-gray-800 ${c.bg}`}>
          <div className="flex items-center gap-2">
            {fraudCase.outcome === 'blocked' && <ShieldX className="w-4 h-4 text-red-400" />}
            {fraudCase.outcome === 'review' && <ShieldAlert className="w-4 h-4 text-amber-400" />}
            {fraudCase.outcome === 'passed' && <ShieldCheck className="w-4 h-4 text-emerald-400" />}
            <span className={`text-xs font-bold uppercase tracking-wider ${c.text}`}>
              {fraudCase.outcome === 'blocked' ? 'Auto-Blocked' : fraudCase.outcome === 'review' ? 'Sent to Review' : 'Approved'}
            </span>
          </div>
          <h3 className="text-sm font-semibold text-white mt-1">{fraudCase.title}</h3>
        </div>

        <div className="p-4 flex-1 relative">
          {showDecision && !showDecisionDismiss && (
            <DecisionPopup outcome={fraudCase.outcome} visible={showDecision} />
          )}

          <div style={fade(showTxn)}>
            <div className="flex items-center gap-2 mb-2">
              <Zap className="w-3.5 h-3.5 text-blue-400" />
              <span className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">Incoming Transaction</span>
            </div>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs mb-3">
              <div><span className="text-gray-500">Customer</span><p className="text-white font-medium">{fraudCase.customer}</p></div>
              <div><span className="text-gray-500">Amount</span><p className="text-white font-mono font-semibold">{fraudCase.amount}</p></div>
              <div><span className="text-gray-500">Type</span><p className="text-gray-300">{fraudCase.transactionType}</p></div>
              <div><span className="text-gray-500">Time</span><p className="text-gray-300">{fraudCase.time}</p></div>
              <div className="col-span-2 flex items-center gap-1 mt-0.5">
                <MapPin className="w-3 h-3 text-gray-500" />
                <span className="text-gray-300">{fraudCase.location}</span>
                <span className="text-[10px] text-gray-600 ml-1">({fraudCase.locationDetail})</span>
              </div>
            </div>
          </div>

          <div style={fade(showScanHeader)}>
            <div className="flex items-center gap-2 mb-1 mt-1">
              <Network className="w-3.5 h-3.5 text-cyan-400" />
              <span className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">CDR Rules Evaluation</span>
              {showScanHeader && step < 6 && (
                <span className="flex items-center gap-1 text-[10px] text-cyan-400">
                  <Signal className="w-3 h-3 animate-pulse" /> Scanning...
                </span>
              )}
            </div>
            <div className="border border-gray-800 rounded-lg px-3 py-1 mb-2 bg-[#0f1117]/50">
              {fraudCase.rules.map((rule, i) => (
                <RuleCheck
                  key={rule.id}
                  rule={rule}
                  visible={step >= ruleSteps[i]}
                  evaluating={step === ruleSteps[i]}
                />
              ))}
            </div>
          </div>

          <div style={fade(showDevice)} className="mb-1">
            <div className="flex items-center gap-2 mb-1">
              <Smartphone className="w-3.5 h-3.5 text-gray-400" />
              <span className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">Device Trust</span>
            </div>
            <div className="flex items-center gap-3 bg-[#0f1117]/50 border border-gray-800 rounded-lg px-3 py-2">
              <div className="shrink-0">
                <div className={`text-sm font-bold font-mono ${
                  fraudCase.device.score >= 80 ? 'text-emerald-400' : fraudCase.device.score >= 50 ? 'text-amber-400' : 'text-red-400'
                }`}>{fraudCase.device.score}/100</div>
                <div className={`text-[9px] font-semibold ${
                  fraudCase.device.score >= 80 ? 'text-emerald-500' : fraudCase.device.score >= 50 ? 'text-amber-500' : 'text-red-500'
                }`}>{fraudCase.device.label}</div>
              </div>
              <div className="text-[10px] text-gray-500">{fraudCase.device.detail}</div>
            </div>
          </div>

          <ScoreBar score={fraudCase.score} visible={showScore} />
        </div>
      </div>

      {/* ── Right: AgentBricks + AI Insight + Action ── */}
      <div className="space-y-4">
        {/* AgentBricks Summary */}
        {fraudCase.agentSummary && step >= agentStartStep && (
          <AgentSection
            summary={fraudCase.agentSummary}
            analyst={fraudCase.assignedAnalyst}
            outcome={fraudCase.outcome}
            step={step}
            agentStartStep={agentStartStep}
          />
        )}

        {/* AI Recommendation */}
        <div style={fade(step >= agentStartStep + 2)}>
          <div className="bg-[#161922] border border-blue-500/20 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-2">
              <Lightbulb className="w-4 h-4 text-blue-400" />
              <span className="text-xs font-semibold text-blue-300">AI Recommendation</span>
            </div>
            <h3 className="text-sm font-semibold text-white mb-1.5">{fraudCase.recommendation.title}</h3>
            <p className="text-xs text-gray-400 leading-relaxed">{fraudCase.recommendation.body}</p>
          </div>
        </div>

        {/* Engine Action */}
        <div style={fade(step >= 9)} className={`bg-[#161922] border rounded-lg p-4 ${c.border}`}>
          <div className="flex items-center gap-2 mb-2">
            <Cpu className="w-4 h-4 ${c.text}" />
            <span className={`text-xs font-semibold ${c.text}`}>Engine Decision</span>
          </div>
          <div className="flex items-center gap-3">
            {fraudCase.outcome === 'blocked' && <ShieldX className="w-8 h-8 text-red-400" />}
            {fraudCase.outcome === 'review' && <ShieldAlert className="w-8 h-8 text-amber-400" />}
            {fraudCase.outcome === 'passed' && <ShieldCheck className="w-8 h-8 text-emerald-400" />}
            <div>
              <div className={`text-sm font-bold ${c.text}`}>{outcomeLabel[fraudCase.outcome]}</div>
              <div className="text-xs text-gray-500 mt-0.5">
                {fraudCase.outcome === 'blocked' && `Score: ${fraudCase.score} · Auto-blocked · Customer notified via SMS`}
                {fraudCase.outcome === 'review' && `Score: ${fraudCase.score} · Escalated to AgentBricks · Assigned to ${fraudCase.assignedAnalyst}`}
                {fraudCase.outcome === 'passed' && `Score: ${fraudCase.score} · All checks clear · Zero friction approval`}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════
   KPI Cards
   ═══════════════════════════════════════════════════════════════ */

function KPIRow() {
  const kpis = [
    { label: 'Frauds Predicted', value: '2,847', sub: 'this month', color: 'text-red-400', icon: AlertTriangle },
    { label: 'Auto-Blocked', value: '1,923', sub: '67.5% of flagged', color: 'text-orange-400', icon: ShieldX },
    { label: 'Under Review', value: '492', sub: '17.3% of flagged', color: 'text-amber-400', icon: Clock },
    { label: 'Approved', value: '432', sub: '15.2% of total', color: 'text-emerald-400', icon: ShieldCheck },
    { label: 'Money Saved', value: '$4.2M', sub: 'fraud prevented', color: 'text-green-400', icon: Shield },
    { label: 'Engine Accuracy', value: '96.8%', sub: 'true positive rate', color: 'text-blue-400', icon: Cpu },
  ]
  return (
    <div className="grid grid-cols-3 lg:grid-cols-6 gap-3">
      {kpis.map(k => (
        <div key={k.label} className="bg-[#161922] border border-gray-800 rounded-lg p-3 text-center">
          <k.icon className={`w-4 h-4 ${k.color} mx-auto mb-1`} />
          <div className={`text-lg font-bold ${k.color} font-mono`}>{k.value}</div>
          <div className="text-[10px] text-gray-500 uppercase tracking-wider mt-0.5">{k.label}</div>
          <div className="text-[9px] text-gray-600">{k.sub}</div>
        </div>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════
   Pipeline diagram
   ═══════════════════════════════════════════════════════════════ */

function PipelineDiagram() {
  const dataSources = [
    { icon: Phone, label: 'CDR Data', sub: 'Call / SMS / Data', color: 'text-cyan-400', bg: 'bg-cyan-500/10 border-cyan-500/20' },
    { icon: Smartphone, label: 'Device SDK', sub: 'Trust / Encryption', color: 'text-violet-400', bg: 'bg-violet-500/10 border-violet-500/20' },
    { icon: History, label: 'Txn History', sub: 'Patterns / Velocity', color: 'text-sky-400', bg: 'bg-sky-500/10 border-sky-500/20' },
  ]
  const engine = [
    { icon: Network, label: 'Rule Engine', sub: 'R1·R5·R7·R13', color: 'text-blue-400', bg: 'bg-blue-500/10 border-blue-500/20' },
    { icon: Cpu, label: 'Risk Scoring', sub: 'ML + Rules', color: 'text-purple-400', bg: 'bg-purple-500/10 border-purple-500/20' },
  ]
  const outcomes = [
    { icon: ShieldX, label: 'Block', color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
    { icon: Bot, label: 'Agent Review', color: 'text-amber-400', bg: 'bg-amber-500/10 border-amber-500/20' },
    { icon: ShieldCheck, label: 'Approve', color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
  ]

  return (
    <div className="bg-[#161922] border border-gray-800 rounded-lg p-5">
      <div className="flex items-center justify-center gap-3 flex-wrap">
        {/* 3 parallel data sources, each with its own angled arrow */}
        <div className="flex flex-col gap-1.5">
          {dataSources.map((s, i) => {
            const angle = i === 0 ? 'rotate-[30deg]' : i === 2 ? 'rotate-[-30deg]' : ''
            return (
              <div key={s.label} className="flex items-center gap-2">
                <div className={`${s.bg} border rounded-lg px-3 py-1.5 flex items-center gap-2 min-w-[140px]`}>
                  <s.icon className={`w-4 h-4 ${s.color} shrink-0`} />
                  <div>
                    <div className={`text-[10px] font-semibold ${s.color}`}>{s.label}</div>
                    <div className="text-[9px] text-gray-500">{s.sub}</div>
                  </div>
                </div>
                <ArrowRight className={`w-4 h-4 ${s.color} shrink-0 opacity-60 ${angle}`} />
              </div>
            )
          })}
        </div>

        {/* Engine stages */}
        {engine.map((s, i) => (
          <div key={s.label} className="flex items-center gap-3">
            <div className={`${s.bg} border rounded-lg px-4 py-3 text-center min-w-[100px]`}>
              <s.icon className={`w-5 h-5 ${s.color} mx-auto mb-1`} />
              <div className={`text-[10px] font-semibold ${s.color}`}>{s.label}</div>
              <div className="text-[9px] text-gray-500">{s.sub}</div>
            </div>
            {i < engine.length - 1 && <ArrowRight className="w-4 h-4 text-gray-600 shrink-0" />}
          </div>
        ))}

        <ArrowRight className="w-4 h-4 text-gray-600 shrink-0" />

        {/* 3 outcomes */}
        <div className="flex flex-col gap-1.5">
          {outcomes.map(o => (
            <div key={o.label} className={`${o.bg} border rounded-lg px-3 py-1.5 flex items-center gap-2 min-w-[120px]`}>
              <o.icon className={`w-3.5 h-3.5 ${o.color}`} />
              <span className={`text-[10px] font-semibold ${o.color}`}>{o.label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════
   Main Page — Interactive stepper flow
   ═══════════════════════════════════════════════════════════════ */

const scenarioLabels = [
  { label: 'Auto-Blocked', sub: 'SIM Swap Fraud', color: 'text-red-400', bg: 'bg-red-500', ring: 'ring-red-400/50' },
  { label: 'Sent to Review', sub: 'After-Hours Activity', color: 'text-amber-400', bg: 'bg-amber-500', ring: 'ring-amber-400/50' },
  { label: 'Approved', sub: 'Device Upgrade', color: 'text-emerald-400', bg: 'bg-emerald-500', ring: 'ring-emerald-400/50' },
]

// active: 0=not started, 1/2/3=which scenario
type ActiveScenario = 0 | 1 | 2 | 3

export default function FraudEngineLive() {
  const [active, setActive] = useState<ActiveScenario>(0)
  const [key, setKey] = useState(0)
  const [visited, setVisited] = useState<Set<number>>(new Set())
  const [autoplay, setAutoplay] = useState(false)

  const goTo = useCallback((n: ActiveScenario) => {
    if (n === active) return
    setKey(k => k + 1)
    setActive(n)
    if (n >= 1 && n <= 3) {
      setVisited(prev => new Set(prev).add(n))
    }
  }, [active])

  const resetDemo = useCallback(() => {
    setActive(0)
    setKey(k => k + 1)
    setVisited(new Set())
    setAutoplay(false)
  }, [])

  const startAutoplay = useCallback(() => {
    setAutoplay(true)
    setKey(k => k + 1)
    setActive(1)
    setVisited(new Set([1]))
  }, [])

  // Autoplay: advance to next scenario after each finishes
  useEffect(() => {
    if (!autoplay) return
    if (active === 0) return
    // 13s per case (15 steps × ~650ms + extra for typing)
    const next: ActiveScenario | null = active === 1 ? 2 : active === 2 ? 3 : null
    if (!next) { setAutoplay(false); return }
    const t = setTimeout(() => {
      setKey(k => k + 1)
      setActive(next)
      if (next >= 1 && next <= 3) {
        setVisited(prev => new Set(prev).add(next))
      }
    }, 13000)
    return () => clearTimeout(t)
  }, [active, autoplay])

  return (
    <div className="space-y-6 max-w-[1400px] mx-auto">
      {/* ── Hero ───────────────────────────────────── */}
      <div className="text-center">
        <div className="flex items-center justify-center gap-3 mb-2">
          <div className="p-2 bg-red-500/10 rounded-xl">
            <Shield className="w-7 h-7 text-red-400" />
          </div>
          <h1 className="text-2xl font-bold text-white">Fraud Detection Engine</h1>
        </div>
        <p className="text-sm text-gray-400 max-w-2xl mx-auto">
          Real-time CDR-based fraud detection pipeline. Every transaction is evaluated against network rules,
          device signals, and behavioral patterns — blocking fraud instantly, escalating uncertainty to AI agents,
          and approving legitimate transactions without friction.
        </p>
      </div>

      {/* ── KPI Row ────────────────────────────────── */}
      <KPIRow />

      {/* ── Pipeline Diagram ──────────────────────── */}
      <PipelineDiagram />

      {/* ── Start / Stepper Controls ──────────────── */}
      {active === 0 ? (
        <div className="flex items-center justify-center gap-3">
          <button
            onClick={() => goTo(1)}
            className="flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-lg transition shadow-lg shadow-blue-500/20"
          >
            <Play className="w-4 h-4" /> Start Demo
          </button>
          <button
            onClick={startAutoplay}
            className="flex items-center gap-2 px-6 py-3 bg-gray-700 hover:bg-gray-600 text-gray-300 text-sm font-medium rounded-lg transition"
          >
            <Play className="w-4 h-4" /> Auto-Play All
          </button>
        </div>
      ) : (
        <div className="space-y-2">
          {/* Clickable scenario stepper */}
          <div className="flex items-center justify-center gap-3">
            {scenarioLabels.map((s, i) => {
              const n = (i + 1) as ActiveScenario
              const isCurrent = active === n
              const wasVisited = visited.has(n)
              return (
                <div key={i} className="flex items-center gap-3">
                  <button
                    onClick={() => goTo(n)}
                    className={`flex items-center gap-2.5 px-4 py-2.5 rounded-lg border transition-all cursor-pointer ${
                      isCurrent
                        ? `${s.bg}/20 border-current ${s.color} ring-2 ${s.ring} ring-offset-2 ring-offset-[#0f1117]`
                        : wasVisited
                        ? 'bg-gray-800 border-gray-700 text-gray-300 hover:bg-gray-700'
                        : 'bg-[#161922] border-gray-800 text-gray-500 hover:border-gray-600 hover:text-gray-300'
                    }`}
                  >
                    <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold shrink-0 ${
                      isCurrent ? `${s.bg} text-white` : wasVisited ? 'bg-gray-700 text-gray-300' : 'bg-gray-800 text-gray-600'
                    }`}>{n}</div>
                    <div className="text-left">
                      <div className={`text-xs font-semibold ${isCurrent ? s.color : ''}`}>{s.label}</div>
                      <div className="text-[10px] text-gray-500">{s.sub}</div>
                    </div>
                  </button>
                </div>
              )
            })}

            <div className="w-px h-6 bg-gray-800 mx-1" />
            {autoplay && (
              <button
                onClick={() => setAutoplay(false)}
                className="flex items-center gap-1.5 px-3 py-2 bg-blue-500/10 border border-blue-500/30 rounded-lg text-blue-400 text-xs transition hover:bg-blue-500/20"
              >
                <Loader2 className="w-3.5 h-3.5 animate-spin" /> Auto-Playing...
              </button>
            )}
            <button
              onClick={resetDemo}
              className="flex items-center gap-1.5 px-3 py-2 text-gray-500 hover:text-gray-300 text-xs transition"
            >
              <RotateCcw className="w-3.5 h-3.5" /> Reset
            </button>
          </div>
        </div>
      )}

      {/* ── Case simulation (2-column: analysis left, insights right) ── */}
      {active >= 1 && active <= 3 && (
        <div key={`${key}-${active}`}>
          <CaseSimulation
            fraudCase={CASES[active - 1]}
            playing={true}
          />
        </div>
      )}
    </div>
  )
}
