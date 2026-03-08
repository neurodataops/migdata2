import { useCallback, useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useAuthStore } from '../../store/authStore'
import { useAppStore } from '../../store/appStore'
import { getSchemas, runPipeline, updatePlatform, getPipelineStatus, getLlmProviders, setLlmProvider } from '../../api/endpoints'
import { usePipelineWebSocket, type PipelineMessage } from '../../api/websocket'
import PipelineProgress from '../common/PipelineProgress'
import ProgressBar from '../common/ProgressBar'
import type { PipelineStep } from '../../types'

export default function Sidebar() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { username, logout } = useAuthStore()
  const {
    sourcePlatform, setSourcePlatform,
    targetPlatform,
    selectedSchemas, setSelectedSchemas,
    confidenceThreshold, setConfidenceThreshold,
    useMock, setUseMock,
    pipeline, setPipeline,
    pipelineSteps, setPipelineSteps,
    progressPercent, setProgressPercent,
    progressMetrics, setProgressMetrics,
    llmProvider, setLlmProvider,
  } = useAppStore()

  // Fetch LLM providers
  const { data: llmData } = useQuery({
    queryKey: ['llm-providers'],
    queryFn: getLlmProviders,
  })
  const llmProviders: { id: string; name: string; description: string }[] = llmData?.providers ?? []
  const activeLlm: string = llmData?.active ?? llmProvider

  const llmMutation = useMutation({
    mutationFn: (providerId: string) => setLlmProvider(providerId),
    onSuccess: (_data, providerId) => {
      setLlmProvider(providerId)
      queryClient.invalidateQueries({ queryKey: ['llm-providers'] })
    },
  })

  // Fetch schemas
  const { data: schemasData } = useQuery({
    queryKey: ['schemas'],
    queryFn: getSchemas,
    refetchInterval: 5000, // Refetch every 5 seconds to update after pipeline runs
  })
  const allSchemas: string[] = schemasData?.schemas ?? []

  // Initialize selected schemas when data loads (use useEffect to avoid setState during render)
  useEffect(() => {
    if (allSchemas.length > 0 && selectedSchemas.length === 0) {
      setSelectedSchemas(allSchemas)
    }
  }, [allSchemas.length, selectedSchemas.length, setSelectedSchemas])

  const noSchemasAvailable = allSchemas.length === 0

  // Platform change mutation
  const platformMutation = useMutation({
    mutationFn: (adapter: string) => updatePlatform({ source_adapter: adapter }),
    onSuccess: () => {
      queryClient.invalidateQueries()
    },
  })

  const handlePlatformChange = (platform: string) => {
    setSourcePlatform(platform)
    const adapter = useMock
      ? platform === 'snowflake' ? 'mock_snowflake' : 'mock_redshift'
      : platform
    platformMutation.mutate(adapter)
  }

  // Pipeline execution
  const pipelineMutation = useMutation({
    mutationFn: () => runPipeline(sourcePlatform, useMock, selectedSchemas),
    onSuccess: (data) => {
      setPipeline({ running: true, jobId: data.job_id, currentStep: 0 })
      const totalSteps = 5
      const stepLabels = useMock
        ? [
            `Generate mock ${sourcePlatform === 'snowflake' ? 'Snowflake' : 'Redshift'} source catalog`,
            `Convert SQL (${sourcePlatform === 'snowflake' ? 'Snowflake' : 'Redshift'} -> Spark)`,
            'Load data (Parquet)',
            'Run validation checks',
            'Execute test suite',
          ]
        : [
            `Connect to ${sourcePlatform === 'snowflake' ? 'Snowflake' : 'Redshift'} and extract schema`,
            `Convert SQL (${sourcePlatform === 'snowflake' ? 'Snowflake' : 'Redshift'} -> Spark)`,
            'Load data to target',
            'Run validation checks',
            'Execute test suite',
          ]

      setPipelineSteps(
        stepLabels.map((label, i) => ({
          step: i + 1,
          label,
          status: 'pending',
          total_steps: totalSteps,
        }))
      )
    },
  })

  // WebSocket for pipeline progress
  const handlePipelineMessage = useCallback((msg: PipelineMessage) => {
    setPipeline({
      currentStep: msg.step,
      totalSteps: msg.total_steps,
      label: msg.label,
    })

    // Update progress percentage and metrics if available
    if (msg.progress_percent !== undefined) {
      setProgressPercent(msg.progress_percent)
    }
    if (msg.metrics) {
      setProgressMetrics(msg.metrics)

      // Invalidate queries when specific milestones are reached to update tabs in real-time
      // This makes the UI feel more dynamic and engaging
      if (msg.metrics.tables || msg.metrics.columns || msg.metrics.tables_loaded) {
        // Invalidate catalog-related queries when tables/columns are processed
        queryClient.invalidateQueries({ queryKey: ['catalog'] })
      }
      if (msg.metrics.sql_objects_total || msg.metrics.auto !== undefined) {
        // Invalidate conversion queries when SQL conversion progresses
        queryClient.invalidateQueries({ queryKey: ['conversion'] })
      }
      if (msg.metrics.checks_passed !== undefined) {
        // Invalidate validation queries when checks are running
        queryClient.invalidateQueries({ queryKey: ['validation'] })
      }
    }

    setPipelineSteps((prev) =>
      prev.map((s) => {
        if (s.step === msg.step) {
          return { ...s, status: msg.status === 'done' ? 'done' : msg.status === 'error' ? 'error' : 'running', elapsed_ms: msg.elapsed_ms }
        }
        if (s.step < msg.step && s.status !== 'done' && s.status !== 'error') {
          return { ...s, status: 'done' }
        }
        return s
      })
    )
  }, [setPipeline, queryClient])

  const handlePipelineComplete = useCallback(async () => {
    // Set progress to 100% and mark pipeline as complete
    setProgressPercent(100)

    // Wait a bit to ensure backend has written all files
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Force refetch all queries by invalidating and refetching
    await queryClient.invalidateQueries()
    await queryClient.refetchQueries()

    // Mark pipeline as complete
    setPipeline({ running: false })

    console.log('[Pipeline Complete] All queries invalidated and refetched')
  }, [setProgressPercent, setPipeline, queryClient])

  usePipelineWebSocket(
    pipeline.running ? pipeline.jobId : null,
    handlePipelineMessage,
    handlePipelineComplete,
  )

  // Pipeline status polling (check if status file artifacts exist)
  useQuery({
    queryKey: ['pipeline-status'],
    queryFn: getPipelineStatus,
    refetchInterval: pipeline.running ? 2000 : false,
  })

  const handleLogout = () => {
    logout()
    navigate('/login')
  }

  const platformLabel = sourcePlatform === 'snowflake' ? 'Snowflake' : 'Redshift'
  const targetLabel = targetPlatform.charAt(0).toUpperCase() + targetPlatform.slice(1)

  return (
    <aside className="w-72 min-h-screen bg-gradient-to-b from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border-r border-border p-5 flex flex-col shadow-[4px_0_30px_rgba(0,212,255,0.1)]">
      {/* Logo */}
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-2">
          <img
            src="/logo.jpg"
            alt="MigData"
            className="h-10 w-auto rounded-lg shadow-[0_2px_10px_rgba(0,212,255,0.2)]"
          />
          <h1 className="text-2xl font-bold bg-gradient-to-r from-accent to-purple bg-clip-text text-transparent">MigData</h1>
        </div>
        <p className="text-xs text-text-secondary flex items-center gap-2">
          <span>{platformLabel}</span>
          <svg className="w-3 h-3 text-accent" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
          </svg>
          <span>{targetLabel}</span>
        </p>
      </div>
      <hr className="border-border-light mb-4" />

      {/* Source Platform */}
      <label className="text-xs text-text-secondary font-medium uppercase tracking-wide mb-2">
        Source platform
      </label>
      <div className="flex gap-2 mb-4">
        {['redshift', 'snowflake'].map((p) => (
          <button
            key={p}
            onClick={() => handlePlatformChange(p)}
            className={`flex-1 px-3 py-2 rounded-lg text-sm font-medium transition-all ${
              sourcePlatform === p
                ? 'bg-gradient-to-r from-accent to-accent-dark text-white shadow-[0_2px_10px_rgba(0,212,255,0.3)]'
                : 'bg-bg-secondary/50 text-text-secondary border border-border hover:border-accent/50'
            }`}
          >
            {p === 'snowflake' ? 'Snowflake' : 'Redshift'}
          </button>
        ))}
      </div>

      {/* Target Platform */}
      <label className="text-xs text-text-secondary font-medium uppercase tracking-wide mb-2">
        Target platform
      </label>
      <div className="flex gap-2 mb-4">
        <button
          className="flex-1 px-3 py-2 rounded-lg text-sm font-medium bg-gradient-to-r from-purple to-purple-dark text-white shadow-[0_2px_10px_rgba(139,92,246,0.3)]"
        >
          Databricks
        </button>
      </div>
      <hr className="border-border-light mb-4" />

      {/* AI Model Selection */}
      <label className="text-xs text-text-secondary font-medium uppercase tracking-wide mb-2">
        AI Model
      </label>
      <div className="mb-4 relative">
        <select
          value={activeLlm}
          onChange={(e) => llmMutation.mutate(e.target.value)}
          className="w-full px-3 py-2.5 rounded-lg text-sm font-medium bg-bg-secondary/50 border border-border text-text-primary appearance-none cursor-pointer hover:border-accent/50 focus:border-accent focus:outline-none transition-all"
        >
          {llmProviders.length > 0 ? (
            llmProviders.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))
          ) : (
            <>
              <option value="claude-opus-4">Claude Opus 4</option>
              <option value="gpt-4o-mini">GPT-4o Mini</option>
              <option value="deepseek-v3">DeepSeek-V3</option>
              <option value="llama-3.3-70b">Llama 3.3 70B</option>
            </>
          )}
        </select>
        <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none">
          <svg className="w-4 h-4 text-text-muted" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </div>
      {llmProviders.find((p) => p.id === activeLlm)?.description && (
        <p className="text-xs text-text-muted -mt-2 mb-4">
          {llmProviders.find((p) => p.id === activeLlm)?.description}
        </p>
      )}
      <hr className="border-border-light mb-4" />

      {/* Schema Filter */}
      <label className="text-xs text-text-secondary font-medium uppercase tracking-wide mb-2">
        Schema filter
      </label>
      <div className="mb-4 max-h-32 overflow-y-auto space-y-1">
        {noSchemasAvailable ? (
          <div className="text-xs text-text-muted italic py-2">
            {useMock
              ? 'Click "Run Demo Pipeline" to load sample schemas'
              : `Test connection on Connection page to see available schemas`}
          </div>
        ) : (
          <>
            {/* Select All checkbox */}
            <label className="flex items-center gap-2 text-sm text-text-primary cursor-pointer hover:text-accent font-medium border-b border-border-light pb-2 mb-2">
              <input
                type="checkbox"
                checked={selectedSchemas.length === allSchemas.length}
                onChange={(e) => {
                  if (e.target.checked) {
                    setSelectedSchemas(allSchemas)
                  } else {
                    setSelectedSchemas([])
                  }
                }}
                className="accent-accent"
              />
              Select All ({allSchemas.length})
            </label>

            {/* Individual schemas */}
            {allSchemas.map((s) => (
              <label key={s} className="flex items-center gap-2 text-sm text-text-secondary cursor-pointer hover:text-text-primary">
                <input
                  type="checkbox"
                  checked={selectedSchemas.includes(s)}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setSelectedSchemas([...selectedSchemas, s])
                    } else {
                      setSelectedSchemas(selectedSchemas.filter((x) => x !== s))
                    }
                  }}
                  className="accent-accent"
                />
                {s}
              </label>
            ))}
          </>
        )}
      </div>

      {/* Confidence Threshold */}
      <label className="text-xs text-text-secondary font-medium uppercase tracking-wide mb-2">
        Confidence threshold: {confidenceThreshold.toFixed(2)}
      </label>
      <input
        type="range"
        min={0}
        max={1}
        step={0.05}
        value={confidenceThreshold}
        onChange={(e) => setConfidenceThreshold(parseFloat(e.target.value))}
        className="w-full mb-4 accent-accent"
      />
      <hr className="border-border-light mb-4" />

      {/* Data Source Mode Toggle */}
      <div className="mb-4 p-3 bg-bg-secondary/50 border border-border rounded-xl">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-text-secondary font-medium">Data Source</span>
          <label className="relative inline-flex items-center cursor-pointer">
            <input
              type="checkbox"
              checked={useMock}
              onChange={(e) => setUseMock(e.target.checked)}
              className="sr-only peer"
            />
            <div className="w-12 h-6 bg-bg-card border border-border rounded-full peer peer-checked:bg-accent/30 peer-checked:border-accent transition-all">
              <div className="absolute top-0.5 left-0.5 bg-text-muted rounded-full h-5 w-5 peer-checked:translate-x-6 peer-checked:bg-accent transition-all"></div>
            </div>
          </label>
        </div>
        <p className="text-xs text-text-muted">
          {useMock ? 'Using mock data' : `Using real ${platformLabel} data`}
        </p>
      </div>

      {/* Run Pipeline */}
      <button
        onClick={() => pipelineMutation.mutate()}
        disabled={pipeline.running}
        className="w-full py-3 rounded-xl text-sm font-semibold bg-gradient-to-r from-accent via-purple to-accent-dark text-white shadow-[0_4px_20px_rgba(0,212,255,0.4)] hover:shadow-[0_6px_30px_rgba(0,212,255,0.6)] transition-all disabled:opacity-50 disabled:cursor-not-allowed mb-2 transform hover:scale-[1.02]"
      >
        {pipeline.running ? (
          <span className="flex items-center justify-center gap-2">
            <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            Pipeline Running...
          </span>
        ) : (
          `Run ${useMock ? 'Demo' : 'Real'} Pipeline`
        )}
      </button>

      {/* Refresh Data Button */}
      <button
        onClick={() => {
          queryClient.invalidateQueries()
          queryClient.refetchQueries()
        }}
        disabled={pipeline.running}
        className="w-full py-2 rounded-xl text-xs font-medium bg-bg-secondary/50 border border-border text-text-secondary hover:text-accent hover:border-accent/50 transition-all disabled:opacity-50 disabled:cursor-not-allowed mb-4"
      >
        <span className="flex items-center justify-center gap-2">
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Refresh Dashboard Data
        </span>
      </button>

      {/* Pipeline progress */}
      {pipeline.running && (
        <div className="mb-4 space-y-4">
          {/* Overall progress bar */}
          <ProgressBar
            percent={progressPercent}
            label={pipeline.label}
            metrics={progressMetrics}
            showMetrics={!useMock}
          />

          {/* Step-by-step progress */}
          {pipelineSteps.length > 0 && (
            <div className="pt-2 border-t border-border-light">
              <PipelineProgress steps={pipelineSteps} />
            </div>
          )}
        </div>
      )}

      <hr className="border-border-light mb-4" />

      {/* Pipeline Status */}
      <p className="text-xs text-text-muted uppercase tracking-wide mb-2">Pipeline Status</p>
      <div className="space-y-1 text-sm mb-4">
        <StatusIndicator label="Source catalog generated" check="catalog" />
        <StatusIndicator label="SQL conversion done" check="conversion" />
        <StatusIndicator label="Data loaded (Parquet)" check="load" />
        <StatusIndicator label="Validation complete" check="validation" />
        <StatusIndicator label="Tests executed" check="tests" />
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* User & Logout */}
      <hr className="border-border-light mb-3" />
      <p className="text-xs text-text-muted mb-2">
        Logged in as: <span className="font-semibold text-text-secondary">{username}</span>
      </p>
      <button
        onClick={handleLogout}
        className="w-full py-2.5 rounded-xl text-sm bg-bg-secondary/50 border border-border text-text-secondary hover:text-accent hover:border-accent/50 transition-all"
      >
        Logout
      </button>
    </aside>
  )
}

function StatusIndicator({ label }: { label: string; check: string }) {
  // Simple check based on query cache
  const queryClient = useQueryClient()
  const catalogData = queryClient.getQueryData(['catalog']) as Record<string, unknown> | undefined
  const conversionData = queryClient.getQueryData(['conversion']) as Record<string, unknown> | undefined
  const validationData = queryClient.getQueryData(['validation']) as Record<string, unknown> | undefined

  let exists = false
  if (label.includes('catalog') && catalogData && Object.keys(catalogData).length > 0) exists = true
  if (label.includes('conversion') && conversionData && Object.keys(conversionData).length > 0) exists = true
  if (label.includes('loaded') && conversionData) exists = true // load comes with conversion
  if (label.includes('Validation') && validationData && Object.keys(validationData).length > 0) exists = true
  if (label.includes('Tests') && validationData) exists = true

  return (
    <div className="text-text-secondary">
      <span>{exists ? '\u2705' : '\u2B1C'}</span>{' '}
      <span>{label}</span>
    </div>
  )
}
