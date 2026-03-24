import { useEffect, useState, useCallback } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  ChevronUp,
  ChevronDown,
  ChevronsLeft,
  ChevronsRight,
  ChevronLeft,
  ChevronRight,
  Filter,
  AlertTriangle,
  Loader2,
} from 'lucide-react'
import {
  fetchCases,
  fetchFilters,
  type CaseListItem,
  type FilterOptions,
  type CaseQueryParams,
} from '../lib/api'
import StatusBadge from '../components/StatusBadge'
import ScoreBar from '../components/ScoreBar'

function formatDate(d: string | null) {
  if (!d) return '-'
  return new Date(d).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

function formatCurrency(v: string | null) {
  if (!v) return '-'
  return `$${parseFloat(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

export default function CaseQueue() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const analystParam = searchParams.get('analyst') || undefined
  const minScoreParam = searchParams.get('min_score') ? Number(searchParams.get('min_score')) : undefined
  const maxScoreParam = searchParams.get('max_score') ? Number(searchParams.get('max_score')) : undefined
  const dateFromParam = searchParams.get('date_from') || undefined
  const highRiskParam = searchParams.get('high_risk') === 'true' || undefined
  const [cases, setCases] = useState<CaseListItem[]>([])
  const [total, setTotal] = useState(0)
  const [pages, setPages] = useState(0)
  const [loading, setLoading] = useState(true)
  const [filters, setFilters] = useState<FilterOptions | null>(null)

  const [params, setParams] = useState<CaseQueryParams>({
    sort_by: 'fraud_score',
    sort_dir: 'desc',
    page: 1,
    limit: 25,
    analyst: analystParam,
    min_score: minScoreParam,
    max_score: maxScoreParam,
    date_from: dateFromParam,
    high_risk: highRiskParam,
  })

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const data = await fetchCases(params)
      setCases(data.cases)
      setTotal(data.total)
      setPages(data.pages)
    } finally {
      setLoading(false)
    }
  }, [params])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    fetchFilters().then(setFilters)
  }, [])

  // Sync URL params with state
  useEffect(() => {
    if (analystParam !== params.analyst || minScoreParam !== params.min_score || maxScoreParam !== params.max_score || dateFromParam !== params.date_from || highRiskParam !== params.high_risk) {
      setParams((p) => ({ ...p, analyst: analystParam, min_score: minScoreParam, max_score: maxScoreParam, date_from: dateFromParam, high_risk: highRiskParam, page: 1 }))
    }
  }, [analystParam, minScoreParam, maxScoreParam, dateFromParam, highRiskParam])

  function toggleSort(col: string) {
    setParams((p) => ({
      ...p,
      sort_by: col,
      sort_dir: p.sort_by === col && p.sort_dir === 'desc' ? 'asc' : 'desc',
      page: 1,
    }))
  }

  function setFilter(key: string, value: string) {
    setParams((p) => ({
      ...p,
      [key]: value || undefined,
      page: 1,
    }))
  }

  function goPage(page: number) {
    setParams((p) => ({ ...p, page }))
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-lg font-semibold text-white">Case Queue</h1>
          <p className="text-xs text-gray-500">
            {total.toLocaleString()} cases total
          </p>
        </div>
      </div>

      {/* Analyst filter banner */}
      {params.analyst && (
        <div className="flex items-center gap-2 mb-3 px-3 py-2 rounded-lg bg-blue-500/10 border border-blue-500/20">
          <span className="text-xs text-blue-400">
            Showing {params.min_score ? `score ${params.min_score}–${params.max_score ?? '100'} ` : ''}cases assigned to <span className="font-semibold text-white">{params.analyst}</span>
          </span>
          <button
            onClick={() => {
              setSearchParams({})
              setParams((p) => ({ ...p, analyst: undefined, min_score: undefined, max_score: undefined, date_from: undefined, high_risk: undefined, page: 1 }))
            }}
            className="ml-auto text-[10px] px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 hover:bg-blue-500/30 transition"
          >
            Clear filter
          </button>
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-2 mb-4">
        <div className="flex items-center gap-1.5 text-xs text-gray-400">
          <Filter className="w-3.5 h-3.5" />
          Filters:
        </div>
        <select
          className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-300 focus:outline-none focus:border-blue-500"
          value={params.status || ''}
          onChange={(e) => setFilter('status', e.target.value)}
        >
          <option value="">All statuses</option>
          {filters?.statuses.map((s) => (
            <option key={s} value={s}>
              {s.replace(/_/g, ' ')}
            </option>
          ))}
        </select>
        <select
          className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs text-gray-300 focus:outline-none focus:border-blue-500"
          value={params.region || ''}
          onChange={(e) => setFilter('region', e.target.value)}
        >
          <option value="">All regions</option>
          {filters?.regions.map((r) => (
            <option key={r} value={r}>
              {r}
            </option>
          ))}
        </select>
        <label className="flex items-center gap-1.5 text-xs text-gray-400 cursor-pointer">
          <input
            type="checkbox"
            className="rounded border-gray-600 bg-gray-800 text-red-500 focus:ring-red-500/50"
            checked={!!params.high_risk}
            onChange={(e) =>
              setParams((p) => ({
                ...p,
                high_risk: e.target.checked || undefined,
                page: 1,
              }))
            }
          />
          <AlertTriangle className="w-3 h-3 text-red-500" />
          High risk only
        </label>
      </div>

      {/* Table */}
      <div className="bg-[#161922] border border-gray-800 rounded-lg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-gray-800">
                {[
                  ['customer_name', 'Customer'],
                  ['transaction_region', 'Region'],
                  ['fraud_score', 'Score'],
                  ['transaction_cost', 'Amount'],
                  ['review_status', 'Status'],
                  ['transaction_date', 'Date'],
                ].map(([col, label]) => (
                  <th
                    key={col}
                    className="px-3 py-2.5 text-left font-medium text-gray-400 cursor-pointer hover:text-white select-none"
                    onClick={() => toggleSort(col)}
                  >
                    <span className="flex items-center gap-1">
                      {label}
                      {params.sort_by === col ? (
                        params.sort_dir === 'desc' ? (
                          <ChevronDown className="w-3 h-3" />
                        ) : (
                          <ChevronUp className="w-3 h-3" />
                        )
                      ) : null}
                    </span>
                  </th>
                ))}
                <th className="px-3 py-2.5 text-left font-medium text-gray-400 w-8">
                  Risk
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={7} className="px-3 py-12 text-center text-gray-500">
                    <Loader2 className="w-5 h-5 animate-spin mx-auto mb-1" />
                    Loading cases...
                  </td>
                </tr>
              ) : cases.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-3 py-12 text-center text-gray-500">
                    No cases match your filters.
                  </td>
                </tr>
              ) : (
                cases.map((c) => (
                  <tr
                    key={c.transaction_id}
                    className="border-b border-gray-800/50 hover:bg-gray-800/40 cursor-pointer transition"
                    onClick={() => navigate(`/cases/${c.transaction_id}`)}
                  >
                    <td className="px-3 py-2.5">
                      <div className="text-white font-medium">
                        {c.customer_name ?? '-'}
                      </div>
                      <div className="text-gray-500 text-[10px]">
                        {c.transaction_id}
                      </div>
                    </td>
                    <td className="px-3 py-2.5 text-gray-400">
                      {c.transaction_region ?? '-'}
                    </td>
                    <td className="px-3 py-2.5">
                      <ScoreBar score={parseFloat(c.fraud_score ?? '0')} />
                    </td>
                    <td className="px-3 py-2.5 text-gray-300 font-mono">
                      {formatCurrency(c.transaction_cost)}
                    </td>
                    <td className="px-3 py-2.5">
                      <StatusBadge status={c.review_status} />
                    </td>
                    <td className="px-3 py-2.5 text-gray-400">
                      {formatDate(c.transaction_date)}
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      {(c.high_risk_flag === 'true' || c.high_risk_flag === 't' || c.high_risk_flag === '1') ? (
                        <AlertTriangle className="w-3.5 h-3.5 text-red-500 inline" />
                      ) : (
                        <span className="text-gray-600 text-[10px]">—</span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {pages > 1 && (
          <div className="flex items-center justify-between px-3 py-2.5 border-t border-gray-800">
            <span className="text-[11px] text-gray-500">
              Page {params.page} of {pages.toLocaleString()}
            </span>
            <div className="flex gap-1">
              <button
                className="p-1 rounded hover:bg-gray-700 disabled:opacity-30"
                disabled={params.page === 1}
                onClick={() => goPage(1)}
              >
                <ChevronsLeft className="w-3.5 h-3.5" />
              </button>
              <button
                className="p-1 rounded hover:bg-gray-700 disabled:opacity-30"
                disabled={params.page === 1}
                onClick={() => goPage((params.page ?? 1) - 1)}
              >
                <ChevronLeft className="w-3.5 h-3.5" />
              </button>
              <button
                className="p-1 rounded hover:bg-gray-700 disabled:opacity-30"
                disabled={params.page === pages}
                onClick={() => goPage((params.page ?? 1) + 1)}
              >
                <ChevronRight className="w-3.5 h-3.5" />
              </button>
              <button
                className="p-1 rounded hover:bg-gray-700 disabled:opacity-30"
                disabled={params.page === pages}
                onClick={() => goPage(pages)}
              >
                <ChevronsRight className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
