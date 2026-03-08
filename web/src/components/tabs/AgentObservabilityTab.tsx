import { useQuery } from '@tanstack/react-query'
import { getObservabilitySummary, getObservabilityTraces } from '../../api/endpoints'

interface TraceEntry {
  id: string
  timestamp: string
  agent_name: string
  task_name: string
  provider_id: string
  model: string
  input_tokens: number
  output_tokens: number
  total_tokens: number
  latency_ms: number
  success: boolean
  error: string | null
}

interface Summary {
  total_calls: number
  success_count: number
  error_count: number
  total_tokens: number
  avg_latency_ms: number
  by_provider: Record<string, { calls: number; tokens: number; errors: number }>
  by_agent: Record<string, { calls: number; tokens: number; avg_latency_ms: number; errors: number }>
  langfuse_enabled: boolean
}

export default function AgentObservabilityTab() {
  const { data: summaryData } = useQuery({
    queryKey: ['observability-summary'],
    queryFn: getObservabilitySummary,
    refetchInterval: 5000,
  })
  const { data: tracesData } = useQuery({
    queryKey: ['observability-traces'],
    queryFn: () => getObservabilityTraces(50),
    refetchInterval: 5000,
  })

  const summary: Summary | null = summaryData ?? null
  const traces: TraceEntry[] = tracesData?.traces ?? []

  return (
    <div className="space-y-6 animate-fade-in-up">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-text-primary">Agent Observability</h2>
          <p className="text-text-secondary text-sm mt-1">
            Real-time monitoring of AI agent executions, LLM calls, token usage, and latency
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold ${
            summary?.langfuse_enabled
              ? 'bg-success/20 text-success border border-success/30'
              : 'bg-warning/20 text-warning border border-warning/30'
          }`}>
            <span className={`w-2 h-2 rounded-full ${summary?.langfuse_enabled ? 'bg-success' : 'bg-warning'}`} />
            Langfuse {summary?.langfuse_enabled ? 'Connected' : 'Local Only'}
          </span>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        <KpiCard label="Total Calls" value={summary?.total_calls ?? 0} />
        <KpiCard label="Successful" value={summary?.success_count ?? 0} color="text-success" />
        <KpiCard label="Errors" value={summary?.error_count ?? 0} color="text-error" />
        <KpiCard label="Total Tokens" value={formatNumber(summary?.total_tokens ?? 0)} />
        <KpiCard label="Avg Latency" value={`${(summary?.avg_latency_ms ?? 0).toFixed(0)}ms`} />
      </div>

      {/* Provider and Agent breakdowns side by side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* By Provider */}
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-5">
          <h3 className="text-sm font-semibold text-text-secondary uppercase tracking-wide mb-4">By Provider</h3>
          {summary && Object.keys(summary.by_provider).length > 0 ? (
            <div className="space-y-3">
              {Object.entries(summary.by_provider).map(([provider, stats]) => (
                <div key={provider} className="flex items-center justify-between p-3 bg-bg-secondary/30 rounded-lg">
                  <div>
                    <p className="text-sm font-medium text-text-primary">{provider}</p>
                    <p className="text-xs text-text-muted">{stats.tokens.toLocaleString()} tokens</p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-semibold text-accent">{stats.calls} calls</p>
                    {stats.errors > 0 && (
                      <p className="text-xs text-error">{stats.errors} errors</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState message="No provider data yet. Run an agent task to see metrics." />
          )}
        </div>

        {/* By Agent */}
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-5">
          <h3 className="text-sm font-semibold text-text-secondary uppercase tracking-wide mb-4">By Agent</h3>
          {summary && Object.keys(summary.by_agent).length > 0 ? (
            <div className="space-y-3">
              {Object.entries(summary.by_agent).map(([agent, stats]) => (
                <div key={agent} className="flex items-center justify-between p-3 bg-bg-secondary/30 rounded-lg">
                  <div>
                    <p className="text-sm font-medium text-text-primary">{agent}</p>
                    <p className="text-xs text-text-muted">avg {stats.avg_latency_ms}ms</p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm font-semibold text-purple">{stats.calls} calls</p>
                    <p className="text-xs text-text-muted">{stats.tokens.toLocaleString()} tokens</p>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState message="No agent data yet. Run an agent task to see metrics." />
          )}
        </div>
      </div>

      {/* Trace Log Table */}
      <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-5">
        <h3 className="text-sm font-semibold text-text-secondary uppercase tracking-wide mb-4">
          Recent Traces
          {traces.length > 0 && <span className="ml-2 text-text-muted font-normal">({traces.length})</span>}
        </h3>

        {traces.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-3 text-text-muted font-medium">Time</th>
                  <th className="text-left py-3 px-3 text-text-muted font-medium">Agent</th>
                  <th className="text-left py-3 px-3 text-text-muted font-medium">Task</th>
                  <th className="text-left py-3 px-3 text-text-muted font-medium">Provider</th>
                  <th className="text-right py-3 px-3 text-text-muted font-medium">Tokens</th>
                  <th className="text-right py-3 px-3 text-text-muted font-medium">Latency</th>
                  <th className="text-center py-3 px-3 text-text-muted font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {traces.map((t) => (
                  <tr key={t.id} className="border-b border-border/50 hover:bg-bg-secondary/30 transition-colors">
                    <td className="py-2.5 px-3 text-text-muted text-xs font-mono">
                      {new Date(t.timestamp).toLocaleTimeString()}
                    </td>
                    <td className="py-2.5 px-3 text-text-primary font-medium">{t.agent_name}</td>
                    <td className="py-2.5 px-3 text-text-secondary">{t.task_name || '—'}</td>
                    <td className="py-2.5 px-3">
                      <span className="inline-flex items-center px-2 py-0.5 rounded-md bg-accent/10 text-accent text-xs font-medium">
                        {t.provider_id}
                      </span>
                    </td>
                    <td className="py-2.5 px-3 text-right text-text-secondary font-mono text-xs">
                      <span className="text-text-muted">{t.input_tokens.toLocaleString()}</span>
                      <span className="text-text-muted mx-1">/</span>
                      <span>{t.output_tokens.toLocaleString()}</span>
                    </td>
                    <td className="py-2.5 px-3 text-right text-text-secondary font-mono text-xs">
                      {t.latency_ms.toFixed(0)}ms
                    </td>
                    <td className="py-2.5 px-3 text-center">
                      {t.success ? (
                        <span className="inline-flex items-center px-2 py-0.5 rounded-full bg-success/20 text-success text-xs font-semibold">
                          OK
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2 py-0.5 rounded-full bg-error/20 text-error text-xs font-semibold" title={t.error ?? ''}>
                          ERR
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState message="No traces recorded yet. Execute an agent task (e.g. SQL transpilation) to see trace data here." />
        )}
      </div>
    </div>
  )
}

function KpiCard({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-4">
      <p className="text-xs text-text-muted uppercase tracking-wide mb-1">{label}</p>
      <p className={`text-2xl font-bold ${color ?? 'text-text-primary'}`}>{value}</p>
    </div>
  )
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-8 text-center">
      <svg className="w-12 h-12 text-text-muted/30 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
      </svg>
      <p className="text-sm text-text-muted">{message}</p>
    </div>
  )
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toString()
}
