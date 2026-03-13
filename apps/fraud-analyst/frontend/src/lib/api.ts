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
  confirmed_fraud_count: number
  false_positive_count: number
  escalated_count: number
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

export interface DecisionHistoryItem {
  decision_id: string
  analyst_name: string | null
  decision: string | null
  notes: string | null
  mitigation_action: string | null
  confidence_level: string | null
  decided_at: string | null
}

// ── API calls ─────────────────────────────────────────────────

export interface CaseQueryParams {
  status?: string
  min_score?: number
  region?: string
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
  if (params.region) qs.set('region', params.region)
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
}): Promise<{ status: string; decision_id: string }> {
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
