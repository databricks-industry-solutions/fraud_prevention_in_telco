import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowLeft,
  AlertTriangle,
  MapPin,
  Smartphone,
  Shield,
  User,
  DollarSign,
  Clock,
  CheckCircle2,
  XCircle,
  AlertOctagon,
  Loader2,
  Send,
  TrendingUp,
} from 'lucide-react'
import { fetchCaseDetail, fetchCustomerHistory, fetchDecisionHistory, fetchSimilarCases, submitAction, type CaseDetailData, type CustomerHistoryItem, type DecisionHistoryItem, type SimilarCasesResponse } from '../lib/api'
import CustomerSparkline from '../components/CustomerSparkline'
import ChatPanel from '../components/ChatPanel'
import StatusBadge from '../components/StatusBadge'
import ScoreBar from '../components/ScoreBar'
import RiskIndicators from '../components/RiskIndicators'
import LocationMap from '../components/LocationMap'
import DecisionTimeline from '../components/DecisionTimeline'

function Field({ label, value, mono }: { label: string; value: string | null | undefined; mono?: boolean }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-gray-500 mb-0.5">{label}</dt>
      <dd className={`text-sm text-gray-200 ${mono ? 'font-mono' : ''}`}>{value || '-'}</dd>
    </div>
  )
}

export default function CaseDetail() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [data, setData] = useState<CaseDetailData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  // Action form state
  const [decision, setDecision] = useState('')
  const [analystName, setAnalystName] = useState('')
  const [notes, setNotes] = useState('')
  const [mitigation, setMitigation] = useState('')
  const [confidence, setConfidence] = useState('medium')
  const [submitting, setSubmitting] = useState(false)
  const [actionResult, setActionResult] = useState<string | null>(null)
  const [history, setHistory] = useState<DecisionHistoryItem[]>([])
  const [customerHistory, setCustomerHistory] = useState<CustomerHistoryItem[]>([])
  const [similar, setSimilar] = useState<SimilarCasesResponse | null>(null)

  useEffect(() => {
    if (!id) return
    setLoading(true)
    fetchCaseDetail(id)
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
    fetchDecisionHistory(id).then(res => setHistory(res.history)).catch(() => {})
    fetchCustomerHistory(id).then(res => setCustomerHistory(res.history)).catch(() => {})
    fetchSimilarCases(id).then(setSimilar).catch(() => {})
  }, [id])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!id || !decision || !analystName.trim()) return
    setSubmitting(true)
    setActionResult(null)
    try {
      const res = await submitAction(id, {
        decision,
        analyst_name: analystName,
        notes,
        mitigation_action: mitigation,
        confidence_level: confidence,
      })
      setActionResult(`Decision recorded: ${res.decision} — ${res.transaction_id}`)
      // Refresh case data and decision history
      const refreshed = await fetchCaseDetail(id)
      setData(refreshed)
      fetchDecisionHistory(id).then(res => setHistory(res.history)).catch(() => {})
      // Navigate back to queue after brief delay so the analyst sees confirmation
      setTimeout(() => navigate(-1), 1500)
    } catch (err: any) {
      setActionResult(`Error: ${err.message}`)
    } finally {
      setSubmitting(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20 text-gray-500">
        <Loader2 className="w-5 h-5 animate-spin mr-2" />
        Loading case...
      </div>
    )
  }

  if (error || !data?.case) {
    return (
      <div className="text-center py-20 text-red-400">
        {error || 'Case not found'}
      </div>
    )
  }

  const c = data.case
  const d = data.device
  const fraudScore = parseFloat(c.fraud_score ?? '0')

  return (
    <div>
      {/* Back button */}
      <button
        onClick={() => navigate(-1)}
        className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-white mb-4"
      >
        <ArrowLeft className="w-3.5 h-3.5" />
        Back to queue
      </button>

      {/* Header */}
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-lg font-semibold text-white flex items-center gap-2">
            {c.customer_name ?? 'Unknown'}
            {c.high_risk_flag === 'true' && (
              <AlertTriangle className="w-4 h-4 text-red-500" />
            )}
          </h1>
          <p className="text-xs text-gray-500 font-mono">{id}</p>
        </div>
        <div className="text-right space-y-1.5 w-32">
          <ScoreBar score={fraudScore} />
          <StatusBadge status={c.review_status} />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left column — Case info */}
        <div className="lg:col-span-2 space-y-4">
          {/* Transaction Details */}
          <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <DollarSign className="w-3.5 h-3.5" />
              Transaction Details
            </h2>
            <dl className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <Field label="Type" value={c.transaction_type} />
              <Field label="Subtype" value={c.transaction_subtype} />
              <Field label="Amount" value={c.transaction_cost ? `$${parseFloat(c.transaction_cost).toFixed(2)}` : null} mono />
              <Field label="Exposure" value={c.case_exposure_usd ? `$${parseFloat(c.case_exposure_usd).toFixed(2)}` : null} mono />
              <Field label="Date" value={c.transaction_date ? new Date(c.transaction_date).toLocaleString() : null} />
              <Field label="Account ID" value={c.account_id} mono />
              <Field label="Services" value={c.account_services} />
              <Field label="User ID" value={c.customer_user_id} mono />
            </dl>
          </section>

          {/* Customer Transaction History */}
          {customerHistory.length > 1 && (
            <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
              <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                <TrendingUp className="w-3.5 h-3.5" />
                Customer Transaction History
              </h2>
              <CustomerSparkline history={customerHistory} currentTxnId={id!} />
            </section>
          )}

          {/* Risk Assessment */}
          <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <Shield className="w-3.5 h-3.5" />
              Risk Assessment
            </h2>
            <dl className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              <Field label="Fraud Score" value={c.fraud_score} mono />
              <Field label="Engine Label" value={c.fraud_label_engine === '1' ? 'Fraud' : 'Legit'} />
              <Field label="Risk Status" value={c.risk_status_engine} />
              <Field label="Root Cause" value={c.fraud_root_cause} />
              <Field label="High Risk" value={c.high_risk_flag === 'true' ? 'Yes' : 'No'} />
            </dl>
            <RiskIndicators
              riskReason={c.risk_reason_engine}
              fraudScore={fraudScore}
              highRisk={c.high_risk_flag === 'true'}
            />
          </section>

          {/* Location */}
          <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <MapPin className="w-3.5 h-3.5" />
              Location
            </h2>
            <dl className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-3">
              <Field label="Region" value={c.transaction_region} />
              <Field label="State" value={c.transaction_state} />
              <Field label="Latitude" value={c.subscriber_location_lat} mono />
              <Field label="Longitude" value={c.subscriber_location_long} mono />
            </dl>
            <LocationMap
              lat={c.subscriber_location_lat ? parseFloat(c.subscriber_location_lat) : null}
              lng={c.subscriber_location_long ? parseFloat(c.subscriber_location_long) : null}
              region={c.transaction_region}
              state={c.transaction_state}
            />
          </section>

          {/* Device Profile */}
          {d && (
            <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
              <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                <Smartphone className="w-3.5 h-3.5" />
                Device Profile
              </h2>
              <dl className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                <Field label="Device ID" value={d.device_id} mono />
                <Field label="Model" value={d.subscriber_device_model} />
                <Field label="Board" value={d.subscriber_device_board} />
                <Field label="OS Version" value={d.subscriber_os_version} />
                <Field label="RAM" value={d.subscriber_device_ram} />
                <Field label="Storage" value={d.subscriber_device_storage} />
                <Field label="VPN Active" value={d.subscriber_vpn_active} />
                <Field label="Encryption" value={d.subscriber_device_encryption} />
                <Field label="SELinux" value={d.subscriber_selinux_status} />
              </dl>
            </section>
          )}

          {/* Review History */}
          {(c.assigned_analyst || c.analyst_notes) && (
            <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
              <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                <Clock className="w-3.5 h-3.5" />
                Review History
              </h2>
              <dl className="grid grid-cols-2 gap-3">
                <Field label="Assigned Analyst" value={c.assigned_analyst} />
                <Field label="Last Review" value={c.last_review_date} />
                <div className="col-span-2">
                  <Field label="Analyst Notes" value={c.analyst_notes} />
                </div>
                <div className="col-span-2">
                  <Field label="Mitigation Steps" value={c.mitigation_steps} />
                </div>
              </dl>
            </section>
          )}

          {/* Similar Cases */}
          {similar && similar.similar_cases.length > 0 && (
            <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
              <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                <TrendingUp className="w-3.5 h-3.5" />
                Similar Resolved Cases
              </h2>
              <div className={`text-xs px-2 py-1.5 rounded mb-3 ${
                similar.recommendation.includes('cleared') ? 'bg-green-500/10 text-green-400' :
                similar.recommendation.includes('escalated') ? 'bg-purple-500/10 text-purple-400' :
                'bg-gray-800 text-gray-400'
              }`}>
                {similar.recommendation} ({similar.distribution.reviewed || 0} reviewed, {similar.distribution.escalated || 0} escalated)
              </div>
              <div className="space-y-2">
                {similar.similar_cases.map((sc, i) => (
                  <div key={i} className="flex items-center justify-between text-xs border-b border-gray-800/50 pb-1.5 cursor-pointer hover:bg-gray-800/30 px-1 rounded" onClick={() => navigate(`/cases/${sc.transaction_id}`)}>
                    <div>
                      <span className="text-white font-medium">{sc.customer_name}</span>
                      <span className="text-gray-500 ml-2 font-mono text-[10px]">{sc.transaction_id}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`px-1.5 py-0.5 rounded text-[10px] ${sc.review_status === 'reviewed' ? 'bg-blue-500/20 text-blue-400' : 'bg-purple-500/20 text-purple-400'}`}>{sc.review_status}</span>
                      <span className="text-gray-500 font-mono">{sc.fraud_score.toFixed(0)}</span>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* Decision History */}
          <section className="bg-[#161922] border border-gray-800 rounded-lg p-4">
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <Clock className="w-3.5 h-3.5" />
              Decision History
            </h2>
            <DecisionTimeline history={history} />
          </section>
        </div>

        {/* Right column — Action Form */}
        <div>
          <form
            onSubmit={handleSubmit}
            className="bg-[#161922] border border-gray-800 rounded-lg p-4 sticky top-6"
          >
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
              <User className="w-3.5 h-3.5" />
              Analyst Decision
            </h2>

            {/* Decision buttons */}
            <div className="grid grid-cols-2 gap-2 mb-3">
              {[
                { value: 'confirmed_fraud', label: 'Confirm Fraud', icon: AlertOctagon, active: 'bg-red-500/20 border-red-500/50 text-red-400' },
                { value: 'false_positive', label: 'False Positive', icon: XCircle, active: 'bg-green-500/20 border-green-500/50 text-green-400' },
                { value: 'escalated', label: 'Escalate', icon: AlertTriangle, active: 'bg-purple-500/20 border-purple-500/50 text-purple-400' },
                { value: 'reviewed', label: 'Reviewed', icon: CheckCircle2, active: 'bg-blue-500/20 border-blue-500/50 text-blue-400' },
              ].map(({ value, label, icon: Icon, active }) => (
                <button
                  key={value}
                  type="button"
                  onClick={() => setDecision(value)}
                  className={`flex items-center gap-1.5 px-2 py-2 rounded border text-xs font-medium transition ${
                    decision === value
                      ? active
                      : 'border-gray-700 text-gray-400 hover:border-gray-600 hover:text-gray-300'
                  }`}
                >
                  <Icon className="w-3.5 h-3.5" />
                  {label}
                </button>
              ))}
            </div>

            <label className="block mb-2">
              <span className="text-[10px] uppercase tracking-wider text-gray-500">Analyst Name</span>
              <input
                type="text"
                value={analystName}
                onChange={(e) => setAnalystName(e.target.value)}
                className="mt-0.5 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500"
                placeholder="your.name"
                required
              />
            </label>

            <label className="block mb-2">
              <span className="text-[10px] uppercase tracking-wider text-gray-500">Confidence</span>
              <select
                value={confidence}
                onChange={(e) => setConfidence(e.target.value)}
                className="mt-0.5 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500"
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
              </select>
            </label>

            <label className="block mb-2">
              <span className="text-[10px] uppercase tracking-wider text-gray-500">Notes</span>
              <textarea
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                rows={3}
                className="mt-0.5 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500 resize-none"
                placeholder="Investigation notes..."
              />
            </label>

            <label className="block mb-3">
              <span className="text-[10px] uppercase tracking-wider text-gray-500">Mitigation Action</span>
              <textarea
                value={mitigation}
                onChange={(e) => setMitigation(e.target.value)}
                rows={2}
                className="mt-0.5 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500 resize-none"
                placeholder="Recommended action..."
              />
            </label>

            <button
              type="submit"
              disabled={!decision || !analystName.trim() || submitting}
              className="w-full flex items-center justify-center gap-1.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-700 disabled:text-gray-500 text-white text-xs font-medium py-2 rounded transition"
            >
              {submitting ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <Send className="w-3.5 h-3.5" />
              )}
              {submitting ? 'Submitting...' : 'Submit Decision'}
            </button>

            {actionResult && (
              <div
                className={`mt-2 text-xs px-2 py-1.5 rounded ${
                  actionResult.startsWith('Error')
                    ? 'bg-red-500/10 text-red-400'
                    : 'bg-green-500/10 text-green-400'
                }`}
              >
                {actionResult}
              </div>
            )}
          </form>
        </div>
      </div>

      <ChatPanel transactionId={id} />
    </div>
  )
}
