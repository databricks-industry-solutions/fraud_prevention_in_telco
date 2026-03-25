const BASE = '/api'

function handleAuthRedirect(res: Response): void {
  if (res.redirected || res.status === 401 || res.status === 403) {
    window.location.reload()
  }
}

async function fetchJSON<T>(url: string): Promise<T> {
  const res = await fetch(url, { redirect: 'error' }).catch(() => {
    window.location.reload()
    throw new Error('Session expired — reloading')
  })
  handleAuthRedirect(res)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

async function postJSON<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    redirect: 'error',
  }).catch(() => {
    window.location.reload()
    throw new Error('Session expired — reloading')
  })
  handleAuthRedirect(res)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

// ── Types ─────────────────────────────────────────────────────

export interface CaseListItem {
  transaction_id: string
  customer_name: string | null
  transaction_type: string | null
  transaction_cost: string | null
  fraud_score: string | null
  risk_status_engine: string | null
  review_status: string | null
  transaction_date: string | null
  transaction_region: string | null
  high_risk_flag: string | null
}

export interface CasesResponse {
  cases: CaseListItem[]
  total: number
  page: number
  limit: number
  pages: number
}

export interface FilterOptions {
  statuses: string[]
  regions: string[]
  types: string[]
}

export interface CaseDetailData {
  case: Record<string, string | null>
  device: Record<string, string | null> | null
}

export interface DashboardStats {
  total_cases: number
  pending_count: number
  reviewed_count: number
  blocked_count: number
  escalated_count: number
  engine_pending_count: number
  avg_fraud_score: number
  total_exposure_usd: number
  high_risk_count: number
}

export interface TrendDay {
  day: string
  case_count: string
  fraud_count: string
  avg_score: string
  daily_exposure: string
}

export interface GeoRegion {
  transaction_region: string
  case_count: string
  total_exposure: string
  avg_score: string
  fraud_count: string
}

export interface TopRiskCase {
  transaction_id: string
  customer_name: string
  fraud_score: string
  case_exposure_usd: string
  risk_reason_engine: string
  transaction_region: string
  review_status: string
}

// ── Management View Types ────────────────────────────────────

export interface PeriodMetrics {
  period: string
  total_cases: number
  open_cases: number | null
  closed_cases: number
  escalated: number
  avg_score: number | null
  resolution_rate: number | null
  escalation_rate: number | null
  total_exposure: number | null
  open_exposure: number | null
  avg_exposure: number | null
}

export interface PipelineSummary {
  current: PeriodMetrics
  prior_month: PeriodMetrics
  prior_quarter: PeriodMetrics
  prior_year: PeriodMetrics
}

export interface MonthlyTrend {
  month: string
  total: number
  closed: number
  pending: number
  escalated: number
  resolution_rate: number
  exposure: number
  open_exposure: number
}

export interface ForecastPoint {
  day: string
  actual_cases: number | null
  forecast_cases: number | null
  forecast_cases_lower: number | null
  forecast_cases_upper: number | null
}

export interface ForecastSummary {
  predicted_cases_30d: number
  predicted_exposure_30d: number
  predicted_daily_avg_cases: number
}

export interface ForecastData {
  series: ForecastPoint[]
  summary: ForecastSummary
  error?: string
}

export interface EnvironmentalFactors {
  day_of_week: { day: string; cases: number; avg_score: number; exposure: number; critical_pct: number }[]
  transaction_types: { type: string; cases: number; avg_score: number; total_exposure: number; avg_exposure: number; escalation_rate: number }[]
  risk_reasons: { reason: string; cases: number; avg_score: number; exposure: number; escalation_rate: number }[]
  regions: { region: string; cases: number; avg_score: number; exposure: number; critical_cases: number; escalation_rate: number }[]
}

export interface MitigationStep {
  step: string
  total_cases: number
  resolved: number
  escalated: number
  pending: number
  success_rate: number
  avg_exposure: number
  total_exposure: number
  exposure_saved: number
  avg_score: number
  false_positives: number
  false_negatives: number
}

export interface EnginePeriod {
  autoblock_cases: number
  saved_by_autoblock: number
  false_positives: number
  lost_by_fp: number
  false_negatives: number
  lost_by_fn: number
  total_flagged: number
  total_exposure: number
}

export interface EnginePerformance {
  current: EnginePeriod
  prior: EnginePeriod
}

export interface AnalystPerformance {
  analyst: string
  total: number
  reviewed: number
  escalated: number
  pending: number
  resolution_rate: number
  escalation_rate: number
  total_exposure: number
  exposure_resolved: number
  exposure_escalated: number
}

export interface TeamPerformance {
  benchmarks: {
    target_resolution_rate: number
    target_escalation_rate: number
    target_cases_per_month: number
  }
  analysts: AnalystPerformance[]
}

export interface DecisionHistoryItem {
  transaction_id: string
  review_status: string | null
  assigned_analyst: string | null
  analyst_notes: string | null
  mitigation_steps: string | null
  last_review_date: string | null
  is_fp: boolean | null
  is_fn: boolean | null
}

// ── API calls ─────────────────────────────────────────────────

export interface CaseQueryParams {
  status?: string
  min_score?: number
  max_score?: number
  date_from?: string
  region?: string
  analyst?: string
  high_risk?: boolean
  sort_by?: string
  sort_dir?: string
  page?: number
  limit?: number
}

export function fetchCases(params: CaseQueryParams = {}): Promise<CasesResponse> {
  const qs = new URLSearchParams()
  if (params.status) qs.set('status', params.status)
  if (params.min_score !== undefined) qs.set('min_score', String(params.min_score))
  if (params.max_score !== undefined) qs.set('max_score', String(params.max_score))
  if (params.date_from) qs.set('date_from', params.date_from)
  if (params.region) qs.set('region', params.region)
  if (params.analyst) qs.set('analyst', params.analyst)
  if (params.high_risk) qs.set('high_risk', 'true')
  if (params.sort_by) qs.set('sort_by', params.sort_by)
  if (params.sort_dir) qs.set('sort_dir', params.sort_dir)
  if (params.page) qs.set('page', String(params.page))
  if (params.limit) qs.set('limit', String(params.limit))
  return fetchJSON(`${BASE}/cases?${qs}`)
}

export function fetchFilters(): Promise<FilterOptions> {
  return fetchJSON(`${BASE}/cases/filters`)
}

export function fetchCaseDetail(id: string): Promise<CaseDetailData> {
  return fetchJSON(`${BASE}/cases/${id}`)
}

export function fetchDecisionHistory(id: string): Promise<{ history: DecisionHistoryItem[] }> {
  return fetchJSON(`${BASE}/cases/${id}/history`)
}

export interface CustomerHistoryItem {
  transaction_id: string
  transaction_date: string | null
  transaction_cost: string | null
  fraud_score: string | null
  review_status: string | null
  transaction_type: string | null
}

export function fetchCustomerHistory(id: string): Promise<{ history: CustomerHistoryItem[]; account_id: string }> {
  return fetchJSON(`${BASE}/cases/${id}/customer-history`)
}

export function submitAction(id: string, body: {
  decision: string
  analyst_name: string
  notes?: string
  mitigation_action?: string
  confidence_level?: string
}): Promise<{ status: string; transaction_id: string; decision: string; analyst: string; timestamp: string }> {
  return postJSON(`${BASE}/cases/${id}/action`, body)
}

export function fetchDashboardStats(): Promise<DashboardStats> {
  return fetchJSON(`${BASE}/dashboard/stats`)
}

export function fetchTrends(): Promise<{ trends: TrendDay[] }> {
  return fetchJSON(`${BASE}/dashboard/trends`)
}

export function fetchGeo(): Promise<{ regions: GeoRegion[] }> {
  return fetchJSON(`${BASE}/dashboard/geo`)
}

export function fetchTopRisk(): Promise<{ cases: TopRiskCase[] }> {
  return fetchJSON(`${BASE}/dashboard/top-risk`)
}

// ── Management View API calls ────────────────────────────────

export function fetchPipelineSummary(period?: string): Promise<PipelineSummary> {
  const qs = period ? `?period=${period}` : ''
  return fetchJSON(`${BASE}/dashboard/pipeline-summary${qs}`)
}

export function fetchMonthlyTrend(): Promise<{ months: MonthlyTrend[] }> {
  return fetchJSON(`${BASE}/dashboard/monthly-trend`)
}

export function fetchEnginePerformance(period?: string): Promise<EnginePerformance> {
  const qs = period ? `?period=${period}` : ''
  return fetchJSON(`${BASE}/dashboard/engine-performance${qs}`)
}

export function fetchForecast(): Promise<ForecastData> {
  return fetchJSON(`${BASE}/dashboard/forecast`)
}

export function fetchEnvironmentalFactors(): Promise<EnvironmentalFactors> {
  return fetchJSON(`${BASE}/dashboard/environmental-factors`)
}

export function fetchMitigationEffectiveness(): Promise<{ mitigations: MitigationStep[] }> {
  return fetchJSON(`${BASE}/dashboard/mitigation-effectiveness`)
}

// ── Executive API ────────────────────────────────────────────

export interface FinancialPeriod {
  total_exposure: number; saved_by_autoblock: number; lost_by_fp: number; lost_by_fn: number
  net_savings: number; total_cases: number; autoblock_cases: number; fp_cases: number; fn_cases: number
  autoblock_accuracy: number; pending_cases: number; closed_cases: number; resolution_rate: number; fp_rate: number
}
export interface ExecTargets { target_cases: number; target_autoblock_rate: number; target_resolution_rate: number; target_max_fp_rate: number; target_max_exposure: number }
export interface FinancialSummary { current_month: FinancialPeriod; prior_month: FinancialPeriod; targets?: ExecTargets }
export interface MonthlyFinancial { month: string; total_cases: number; total_exposure: number; saved: number; lost_fp: number; lost_fn: number; net_savings: number; target_exposure: number }
export interface QuarterlyTrend { quarter: string; total_cases: number; total_exposure: number; saved: number; lost_fp: number; lost_fn: number; net_savings: number; qoq_growth: number | null }
export interface RegionalTeam { region: string; analysts: number; total_cases: number; total_exposure: number; saved: number; lost_fp: number; pending: number; closed: number; resolution_rate: number; autoblock_rate: number; fp_rate: number; net_savings: number; target_autoblock_rate: number; target_resolution_rate: number; target_max_fp_rate: number }
export interface PipelineChange { status: string; current_cases: number; current_exposure: number; prior_cases: number; prior_exposure: number; case_change: number; exposure_change: number }
export interface RiskBucket { category: string; case_count: number; total_exposure: number; avg_exposure: number; pct_of_total: number }
export interface TopExposureCase { transaction_id: string; customer_name: string; fraud_score: string; case_exposure_usd: string; risk_reason_engine: string; transaction_region: string; review_status: string; assigned_analyst: string }

export function fetchFinancialSummary(): Promise<FinancialSummary> { return fetchJSON(`${BASE}/executive/financial-summary`) }
export function fetchMonthlyFinancials(): Promise<{ months: MonthlyFinancial[] }> { return fetchJSON(`${BASE}/executive/monthly-financials`) }
export function fetchQuarterlyTrend(): Promise<{ quarters: QuarterlyTrend[] }> { return fetchJSON(`${BASE}/executive/quarterly-trend`) }
export function fetchRegionalTeams(): Promise<{ teams: RegionalTeam[] }> { return fetchJSON(`${BASE}/executive/regional-teams`) }
export function fetchPipelineChanges(): Promise<{ changes: PipelineChange[] }> { return fetchJSON(`${BASE}/executive/pipeline-changes`) }
export function fetchRiskDistribution(): Promise<{ buckets: RiskBucket[] }> { return fetchJSON(`${BASE}/executive/risk-distribution`) }
export function fetchTopExposureCases(): Promise<{ cases: TopExposureCase[] }> { return fetchJSON(`${BASE}/executive/top-exposure-cases`) }

// ── SLA, Alerts, Similar Cases ────────────────────────────────

export interface SLATracking {
  pending_over_24h: number; pending_over_72h: number; pending_over_7d: number
  total_pending: number; total_pending_exposure: number; avg_resolution_hours: number
  backlog_trend: { day: string; new_cases: number; resolved: number }[]
}
export interface Alert { severity: 'critical' | 'warning' | 'info'; title: string; message: string; metric: number; transaction_id?: string }
export interface SimilarCase { transaction_id: string; customer_name: string; fraud_score: number; case_exposure_usd: number; review_status: string; risk_reason_engine: string; assigned_analyst: string; mitigation_steps: string }
export interface SimilarCasesResponse { similar_cases: SimilarCase[]; distribution: Record<string, number>; recommendation: string }

export function fetchSLATracking(): Promise<SLATracking> { return fetchJSON(`${BASE}/dashboard/sla-tracking`) }
export function fetchAlerts(): Promise<{ alerts: Alert[] }> { return fetchJSON(`${BASE}/dashboard/alerts`) }
export function fetchSimilarCases(txnId: string): Promise<SimilarCasesResponse> { return fetchJSON(`${BASE}/dashboard/similar-cases/${txnId}`) }

export function fetchTeamPerformance(period?: string): Promise<TeamPerformance> {
  const qs = period ? `?period=${period}` : ''
  return fetchJSON(`${BASE}/dashboard/team-performance${qs}`)
}
