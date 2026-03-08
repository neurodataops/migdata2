import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import { useAppStore } from '../store/appStore'
import {
  testSourceMock,
  testSourceRedshift,
  testSourceSnowflake,
  testTargetMock,
} from '../api/endpoints'
import StatusChip from '../components/common/StatusChip'

interface ConnectionFormData {
  host: string
  port: string
  database: string
  username: string
  password: string
  warehouse?: string
  account?: string
  role?: string
}

export default function ConnectionPage() {
  const navigate = useNavigate()
  const { sourcePlatform, setSourcePlatform, targetPlatform, setTargetPlatform, useMock, setUseMock } = useAppStore()

  // Load saved credentials from localStorage
  const loadSavedCredentials = () => {
    const saved = localStorage.getItem('migdata_connection_credentials')
    if (saved) {
      try {
        return JSON.parse(saved)
      } catch {
        return {}
      }
    }
    return {}
  }

  const savedCreds = loadSavedCredentials()

  const [formData, setFormData] = useState<ConnectionFormData>({
    host: savedCreds.host || '',
    port: savedCreds.port || '5439',
    database: savedCreds.database || '',
    username: savedCreds.username || '',
    password: '', // Never save password
    warehouse: savedCreds.warehouse || '',
    account: savedCreds.account || '',
    role: savedCreds.role || 'PUBLIC',
  })

  const [rememberConnection, setRememberConnection] = useState(!!savedCreds.username)

  const [sourceStatus, setSourceStatus] = useState<'idle' | 'success' | 'error'>('idle')
  const [targetStatus, setTargetStatus] = useState<'idle' | 'success' | 'error'>('idle')
  const [sourceMessage, setSourceMessage] = useState('')
  const [targetMessage, setTargetMessage] = useState('')

  const testSourceMutation = useMutation({
    mutationFn: () => {
      if (useMock) {
        return testSourceMock()
      } else if (sourcePlatform === 'snowflake') {
        // Map form data to match backend SnowflakeConnectionRequest
        return testSourceSnowflake({
          account: formData.account || '',
          warehouse: formData.warehouse || '',
          database: formData.database,
          role: formData.role || 'PUBLIC',
          user: formData.username,
          password: formData.password,
        })
      } else {
        // Map form data to match backend RedshiftConnectionRequest
        return testSourceRedshift({
          host: formData.host,
          port: parseInt(formData.port) || 5439,
          database: formData.database,
          user: formData.username,
          password: formData.password,
        })
      }
    },
    onSuccess: (data) => {
      if (data.success) {
        setSourceStatus('success')
        setSourceMessage(data.message || 'Connection successful!')
      } else {
        setSourceStatus('error')
        setSourceMessage(`${data.message}${data.hint ? ' ' + data.hint : ''}`)
      }
    },
    onError: (err: any) => {
      setSourceStatus('error')
      const errorMsg = err.response?.data?.detail || err.message || 'Connection test failed. Please check your credentials.'
      setSourceMessage(errorMsg)
    },
  })

  const testTargetMutation = useMutation({
    mutationFn: testTargetMock,
    onSuccess: (data) => {
      if (data.success) {
        setTargetStatus('success')
        setTargetMessage(data.message || 'Connection successful!')
      } else {
        setTargetStatus('error')
        setTargetMessage(`${data.message}${data.hint ? ' ' + data.hint : ''}`)
      }
    },
    onError: (err: any) => {
      setTargetStatus('error')
      const errorMsg = err.response?.data?.detail || err.message || 'Connection test failed. Please check your settings.'
      setTargetMessage(errorMsg)
    },
  })

  const handleTestSource = () => {
    setSourceStatus('idle')
    setSourceMessage('')

    // Save credentials if remember is checked (except password)
    if (rememberConnection) {
      const credsToSave = { ...formData }
      delete credsToSave.password
      localStorage.setItem('migdata_connection_credentials', JSON.stringify(credsToSave))
    } else {
      localStorage.removeItem('migdata_connection_credentials')
    }

    // If testing real connection (not mock), set useMock to false
    if (!useMock) {
      setUseMock(false)
    }

    testSourceMutation.mutate()
  }

  const handleTestTarget = () => {
    setTargetStatus('idle')
    setTargetMessage('')
    testTargetMutation.mutate()
  }

  const canProceed = useMock || (sourceStatus === 'success' && targetStatus === 'success')

  const platformLabel = sourcePlatform === 'snowflake' ? 'Snowflake' : 'Redshift'

  return (
    <div className="min-h-screen p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12 animate-fade-in-up">
          <h1 className="text-4xl font-bold text-text-primary mb-3">
            Configure Your <span className="bg-gradient-to-r from-accent to-purple bg-clip-text text-transparent">Data Connections</span>
          </h1>
          <p className="text-xl text-text-secondary">
            Connect your source and target platforms to begin migration
          </p>
          <div className="mt-4 inline-flex items-center gap-3 px-4 py-2 bg-bg-card/50 border border-border rounded-full">
            <span className="text-text-secondary">{platformLabel}</span>
            <svg className="w-5 h-5 text-accent" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
            </svg>
            <span className="text-text-secondary">{targetPlatform.charAt(0).toUpperCase() + targetPlatform.slice(1)}</span>
          </div>
        </div>

        {/* Platform Selectors */}
        <div className="grid md:grid-cols-2 gap-6 mb-8">
          {/* Source Platform Selector */}
          <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_8px_30px_rgba(0,212,255,0.1)] animate-fade-in-up" style={{ animationDelay: '0.1s' }}>
            <h3 className="text-lg font-semibold text-text-primary mb-4 flex items-center gap-2">
              <svg className="w-5 h-5 text-accent" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
              </svg>
              Select Source Platform
            </h3>
            <div className="grid grid-cols-1 gap-3">
              <button
                onClick={() => setSourcePlatform('snowflake')}
                className={`group relative overflow-hidden px-6 py-4 rounded-xl text-base font-semibold transition-all ${
                  sourcePlatform === 'snowflake'
                    ? 'bg-gradient-to-r from-accent to-accent-dark text-white shadow-[0_4px_20px_rgba(0,212,255,0.4)]'
                    : 'bg-bg-secondary/50 text-text-secondary border border-border hover:border-accent/50'
                }`}
              >
                <div className="relative z-10 flex items-center justify-center gap-2">
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
                  </svg>
                  Snowflake
                </div>
                {sourcePlatform !== 'snowflake' && (
                  <div className="absolute inset-0 bg-gradient-to-r from-accent/0 via-accent/5 to-accent/0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
                )}
              </button>
              <button
                onClick={() => setSourcePlatform('redshift')}
                className={`group relative overflow-hidden px-6 py-4 rounded-xl text-base font-semibold transition-all ${
                  sourcePlatform === 'redshift'
                    ? 'bg-gradient-to-r from-accent to-accent-dark text-white shadow-[0_4px_20px_rgba(0,212,255,0.4)]'
                    : 'bg-bg-secondary/50 text-text-secondary border border-border hover:border-accent/50'
                }`}
              >
                <div className="relative z-10 flex items-center justify-center gap-2">
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
                  </svg>
                  Redshift
                </div>
                {sourcePlatform !== 'redshift' && (
                  <div className="absolute inset-0 bg-gradient-to-r from-accent/0 via-accent/5 to-accent/0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
                )}
              </button>
            </div>
          </div>

          {/* Target Platform Selector */}
          <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_8px_30px_rgba(0,212,255,0.1)] animate-fade-in-up" style={{ animationDelay: '0.15s' }}>
            <h3 className="text-lg font-semibold text-text-primary mb-4 flex items-center gap-2">
              <svg className="w-5 h-5 text-purple" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M12 5l7 7-7 7" />
              </svg>
              Select Target Platform
            </h3>
            <div className="grid grid-cols-1 gap-3">
              <button
                onClick={() => setTargetPlatform('databricks')}
                className={`group relative overflow-hidden px-6 py-4 rounded-xl text-base font-semibold transition-all ${
                  targetPlatform === 'databricks'
                    ? 'bg-gradient-to-r from-purple to-purple-dark text-white shadow-[0_4px_20px_rgba(139,92,246,0.4)]'
                    : 'bg-bg-secondary/50 text-text-secondary border border-border hover:border-purple/50'
                }`}
              >
                <div className="relative z-10 flex items-center justify-center gap-2">
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
                  </svg>
                  Databricks
                </div>
                {targetPlatform !== 'databricks' && (
                  <div className="absolute inset-0 bg-gradient-to-r from-purple/0 via-purple/5 to-purple/0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
                )}
              </button>
              <button
                onClick={() => setTargetPlatform('bigquery')}
                className={`group relative overflow-hidden px-6 py-4 rounded-xl text-base font-semibold transition-all ${
                  targetPlatform === 'bigquery'
                    ? 'bg-gradient-to-r from-purple to-purple-dark text-white shadow-[0_4px_20px_rgba(139,92,246,0.4)]'
                    : 'bg-bg-secondary/50 text-text-secondary border border-border hover:border-purple/50 opacity-60'
                }`}
                disabled
                title="Coming soon"
              >
                <div className="relative z-10 flex items-center justify-center gap-2">
                  <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
                  </svg>
                  BigQuery
                  <span className="text-xs bg-warning/20 text-warning px-2 py-0.5 rounded-full">Soon</span>
                </div>
              </button>
            </div>
          </div>
        </div>

        {/* Demo Mode Toggle */}
        <div className="mb-8 bg-gradient-to-br from-purple/10 to-accent/10 border border-purple/30 rounded-2xl p-6 animate-fade-in-up" style={{ animationDelay: '0.2s' }}>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-4">
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-purple to-purple-dark flex items-center justify-center flex-shrink-0">
                <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <div>
                <h3 className="text-lg font-semibold text-text-primary mb-1">Demo Mode</h3>
                <p className="text-sm text-text-secondary">
                  Use mock data sources for quick testing and demos
                </p>
              </div>
            </div>
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={useMock}
                onChange={(e) => setUseMock(e.target.checked)}
                className="sr-only peer"
              />
              <div className="w-16 h-8 bg-bg-secondary border-2 border-border rounded-full peer peer-checked:bg-gradient-to-r peer-checked:from-accent peer-checked:to-purple peer-checked:border-accent transition-all shadow-inner">
                <div className="absolute top-1 left-1 bg-white rounded-full h-6 w-6 peer-checked:translate-x-8 transition-transform shadow-lg"></div>
              </div>
            </label>
          </div>
          <div className="flex items-start gap-2 mt-3 pt-3 border-t border-purple/20">
            <svg className="w-4 h-4 text-accent mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <p className="text-xs text-text-muted">
              <strong className="text-text-secondary">Note:</strong> This toggle takes precedence over config.yaml.
              When running the pipeline, your config.yaml will be automatically updated to match this selection.
            </p>
          </div>
        </div>

        <div className="grid md:grid-cols-2 gap-6">
          {/* Source Connection */}
          <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_8px_30px_rgba(0,212,255,0.1)] animate-fade-in-up" style={{ animationDelay: '0.3s' }}>
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-accent to-accent-dark flex items-center justify-center">
                  <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7" />
                  </svg>
                </div>
                <h2 className="text-xl font-semibold text-text-primary">
                  Source: {platformLabel}
                </h2>
              </div>
              {sourceStatus !== 'idle' && (
                <StatusChip
                  status={sourceStatus === 'success' ? 'success' : 'error'}
                  label={sourceStatus === 'success' ? 'Connected' : 'Failed'}
                />
              )}
            </div>

            {!useMock ? (
              <div className="space-y-4 mb-6">
                {sourcePlatform === 'snowflake' ? (
                  <>
                    <input
                      type="text"
                      placeholder="Account (e.g., xy12345.us-east-1)"
                      value={formData.account}
                      onChange={(e) => setFormData({ ...formData, account: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Warehouse"
                      value={formData.warehouse}
                      onChange={(e) => setFormData({ ...formData, warehouse: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Database"
                      value={formData.database}
                      onChange={(e) => setFormData({ ...formData, database: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Role (e.g., PUBLIC or ACCOUNTADMIN)"
                      value={formData.role}
                      onChange={(e) => setFormData({ ...formData, role: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Username"
                      value={formData.username}
                      onChange={(e) => setFormData({ ...formData, username: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="password"
                      placeholder="Password"
                      value={formData.password}
                      onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                  </>
                ) : (
                  <>
                    <input
                      type="text"
                      placeholder="Host (e.g., cluster.region.redshift.amazonaws.com)"
                      value={formData.host}
                      onChange={(e) => setFormData({ ...formData, host: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Port (default: 5439)"
                      value={formData.port}
                      onChange={(e) => setFormData({ ...formData, port: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Database"
                      value={formData.database}
                      onChange={(e) => setFormData({ ...formData, database: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="text"
                      placeholder="Username"
                      value={formData.username}
                      onChange={(e) => setFormData({ ...formData, username: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                    <input
                      type="password"
                      placeholder="Password"
                      value={formData.password}
                      onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                      className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                    />
                  </>
                )}
              </div>
            ) : (
              <div className="mb-6 p-4 bg-accent/5 border border-accent/20 rounded-xl">
                <p className="text-sm text-text-secondary">
                  ✓ Demo mode enabled. Mock {platformLabel} data will be used.
                </p>
              </div>
            )}

            {sourceMessage && (
              <div
                className={`mb-4 px-4 py-3 rounded-xl text-sm ${
                  sourceStatus === 'success'
                    ? 'bg-success/10 border border-success/30 text-success'
                    : 'bg-error/10 border border-error/30 text-error'
                }`}
              >
                {sourceMessage}
              </div>
            )}

            {/* Remember Connection */}
            {!useMock && (
              <label className="flex items-center gap-2 mb-4 text-sm text-text-secondary cursor-pointer hover:text-text-primary transition-colors">
                <input
                  type="checkbox"
                  checked={rememberConnection}
                  onChange={(e) => setRememberConnection(e.target.checked)}
                  className="w-4 h-4 rounded border-border bg-bg-secondary text-accent focus:ring-2 focus:ring-accent focus:ring-offset-0"
                />
                <span>Remember connection details (password not saved)</span>
              </label>
            )}

            <button
              onClick={handleTestSource}
              disabled={testSourceMutation.isPending}
              className="w-full py-3 rounded-xl text-sm font-semibold bg-gradient-to-r from-accent to-accent-dark text-white shadow-[0_4px_15px_rgba(0,212,255,0.3)] hover:shadow-[0_6px_25px_rgba(0,212,255,0.5)] transition-all disabled:opacity-50 transform hover:scale-[1.02]"
            >
              {testSourceMutation.isPending ? (
                <span className="flex items-center justify-center gap-2">
                  <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Testing Connection...
                </span>
              ) : (
                'Test Source Connection'
              )}
            </button>
          </div>

          {/* Target Connection */}
          <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_8px_30px_rgba(0,212,255,0.1)] animate-fade-in-up" style={{ animationDelay: '0.4s' }}>
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-purple to-purple-dark flex items-center justify-center">
                  <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M12 5l7 7-7 7" />
                  </svg>
                </div>
                <h2 className="text-xl font-semibold text-text-primary">
                  Target: {targetPlatform.charAt(0).toUpperCase() + targetPlatform.slice(1)}
                </h2>
              </div>
              {targetStatus !== 'idle' && (
                <StatusChip
                  status={targetStatus === 'success' ? 'success' : 'error'}
                  label={targetStatus === 'success' ? 'Connected' : 'Failed'}
                />
              )}
            </div>

            <div className="mb-6 p-6 bg-purple/5 border border-purple/20 rounded-xl">
              <p className="text-sm text-text-secondary mb-3">
                Target connection will be configured automatically based on your migration settings.
              </p>
              <div className="flex items-center gap-2 text-xs text-text-muted">
                <svg className="w-4 h-4 text-purple" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                Workspace, cluster, and authentication details required
              </div>
            </div>

            {targetMessage && (
              <div
                className={`mb-4 px-4 py-3 rounded-xl text-sm ${
                  targetStatus === 'success'
                    ? 'bg-success/10 border border-success/30 text-success'
                    : 'bg-error/10 border border-error/30 text-error'
                }`}
              >
                {targetMessage}
              </div>
            )}

            <button
              onClick={handleTestTarget}
              disabled={testTargetMutation.isPending}
              className="w-full py-3 rounded-xl text-sm font-semibold bg-gradient-to-r from-purple to-purple-dark text-white shadow-[0_4px_15px_rgba(139,92,246,0.3)] hover:shadow-[0_6px_25px_rgba(139,92,246,0.5)] transition-all disabled:opacity-50 transform hover:scale-[1.02]"
            >
              {testTargetMutation.isPending ? (
                <span className="flex items-center justify-center gap-2">
                  <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Testing Connection...
                </span>
              ) : (
                'Test Target Connection'
              )}
            </button>
          </div>
        </div>

        {/* Proceed Button */}
        <div className="mt-10 animate-fade-in-up" style={{ animationDelay: '0.5s' }}>
          {!canProceed && !useMock && (
            <div className="mb-4 text-center">
              <p className="text-sm text-text-secondary flex items-center justify-center gap-2">
                <svg className="w-4 h-4 text-accent" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                Test both source and target connections before proceeding
              </p>
            </div>
          )}
          <div className="flex justify-center">
            <button
              onClick={() => {
                // Ensure useMock state matches the connection mode
                if (!useMock && (sourceStatus === 'success' || targetStatus === 'success')) {
                  // Real connections were tested successfully, keep real mode
                  setUseMock(false)
                } else if (useMock) {
                  // Demo mode is on, keep mock mode
                  setUseMock(true)
                }
                navigate('/dashboard')
              }}
              disabled={!canProceed}
              className="group relative px-10 py-4 rounded-xl text-lg font-semibold bg-gradient-to-r from-accent via-purple to-accent-dark text-white shadow-[0_8px_30px_rgba(0,212,255,0.4)] hover:shadow-[0_12px_40px_rgba(0,212,255,0.6)] transition-all disabled:opacity-50 disabled:cursor-not-allowed transform hover:scale-105 disabled:hover:scale-100"
            >
              <span className="flex items-center gap-3">
                Proceed to Dashboard
                <svg className="w-5 h-5 group-hover:translate-x-1 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                </svg>
              </span>
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
