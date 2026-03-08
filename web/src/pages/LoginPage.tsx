import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import { useAuthStore } from '../store/authStore'
import { login, register } from '../api/endpoints'

export default function LoginPage() {
  const navigate = useNavigate()
  const { login: setAuth } = useAuthStore()
  const [isRegister, setIsRegister] = useState(false)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')

  const loginMutation = useMutation({
    mutationFn: () => login(username, password),
    onSuccess: (data) => {
      setAuth(data.token, username)
      navigate('/connection')
    },
    onError: (err: any) => {
      setError(err.response?.data?.detail || 'Login failed')
    },
  })

  const registerMutation = useMutation({
    mutationFn: () => register(username, password),
    onSuccess: () => {
      // After registration, auto-login
      loginMutation.mutate()
    },
    onError: (err: any) => {
      setError(err.response?.data?.detail || 'Registration failed')
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    if (!username || !password) {
      setError('Username and password are required')
      return
    }
    if (isRegister) {
      registerMutation.mutate()
    } else {
      loginMutation.mutate()
    }
  }

  return (
    <div className="min-h-screen flex">
      {/* Left Side - Hero Section */}
      <div className="hidden lg:flex lg:w-1/2 relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-accent/20 via-purple/20 to-transparent"></div>
        <div className="absolute top-10 left-10 w-72 h-72 bg-accent/10 rounded-full blur-3xl animate-pulse-glow"></div>
        <div className="absolute bottom-10 right-10 w-96 h-96 bg-purple/10 rounded-full blur-3xl animate-pulse-glow" style={{ animationDelay: '1s' }}></div>

        <div className="relative z-10 flex flex-col justify-center px-16 py-20">
          <div className="animate-fade-in-up">
            <img
              src="/logo.jpg"
              alt="MigData"
              className="h-16 w-auto mb-6 rounded-lg shadow-[0_4px_20px_rgba(0,212,255,0.3)]"
            />
            <h1 className="text-5xl font-bold text-text-primary mb-6 leading-tight">
              Accelerate Your<br />
              <span className="bg-gradient-to-r from-accent via-accent-light to-purple bg-clip-text text-transparent">
                Data Migration Journey
              </span>
            </h1>
            <p className="text-xl text-text-secondary mb-8 leading-relaxed">
              Intelligent migration platform powered by AI-driven insights,
              automated SQL conversion, and comprehensive validation.
            </p>
          </div>

          <div className="space-y-6 animate-fade-in-up" style={{ animationDelay: '0.2s' }}>
            <div className="flex items-start gap-4">
              <div className="mt-1 w-8 h-8 rounded-lg bg-gradient-to-br from-accent to-accent-dark flex items-center justify-center flex-shrink-0">
                <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <div>
                <h3 className="text-lg font-semibold text-text-primary mb-1">
                  90%+ Automation Rate
                </h3>
                <p className="text-text-secondary">
                  Automatically convert SQL with minimal manual intervention
                </p>
              </div>
            </div>

            <div className="flex items-start gap-4">
              <div className="mt-1 w-8 h-8 rounded-lg bg-gradient-to-br from-purple to-purple-dark flex items-center justify-center flex-shrink-0">
                <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
              <div>
                <h3 className="text-lg font-semibold text-text-primary mb-1">
                  Comprehensive Validation
                </h3>
                <p className="text-text-secondary">
                  Built-in data quality checks and confidence scoring
                </p>
              </div>
            </div>

            <div className="flex items-start gap-4">
              <div className="mt-1 w-8 h-8 rounded-lg bg-gradient-to-br from-success to-success/70 flex items-center justify-center flex-shrink-0">
                <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
                </svg>
              </div>
              <div>
                <h3 className="text-lg font-semibold text-text-primary mb-1">
                  Real-time Analytics
                </h3>
                <p className="text-text-secondary">
                  Track migration progress with interactive dashboards
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Right Side - Login Form */}
      <div className="w-full lg:w-1/2 flex items-center justify-center p-8">
        <div className="w-full max-w-md">
          {/* Logo */}
          <div className="text-center mb-8 animate-fade-in-up">
            <h1 className="text-5xl font-bold bg-gradient-to-r from-accent via-accent-light to-purple bg-clip-text text-transparent mb-4">
              MigData
            </h1>
            <p className="text-text-secondary text-lg">
              Data Migration Intelligence Platform
            </p>
          </div>

          {/* Login/Register Card */}
          <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-8 shadow-[0_8px_40px_rgba(0,212,255,0.15)] animate-fade-in-up" style={{ animationDelay: '0.1s' }}>
            <h2 className="text-2xl font-semibold text-text-primary mb-6">
              {isRegister ? 'Create Your Account' : 'Welcome Back'}
            </h2>

            <form onSubmit={handleSubmit} className="space-y-5">
              <div>
                <label className="block text-sm font-medium text-text-secondary mb-2">
                  Username
                </label>
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                  placeholder="Enter your username"
                  autoFocus
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-text-secondary mb-2">
                  Password
                </label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full px-4 py-3 bg-bg-secondary/50 border border-border rounded-xl text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent transition-all"
                  placeholder="Enter your password"
                />
              </div>

              {error && (
                <div className="bg-error/10 border border-error/30 rounded-xl px-4 py-3 text-error text-sm">
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={loginMutation.isPending || registerMutation.isPending}
                className="w-full py-3.5 rounded-xl text-base font-semibold bg-gradient-to-r from-accent to-purple text-white shadow-[0_4px_20px_rgba(0,212,255,0.4)] hover:shadow-[0_6px_30px_rgba(0,212,255,0.6)] transition-all disabled:opacity-50 disabled:cursor-not-allowed transform hover:scale-[1.02]"
              >
                {loginMutation.isPending || registerMutation.isPending
                  ? 'Please wait...'
                  : isRegister
                    ? 'Create Account'
                    : 'Sign In'}
              </button>
            </form>

            <div className="mt-6 text-center">
              <button
                onClick={() => {
                  setIsRegister(!isRegister)
                  setError('')
                }}
                className="text-sm text-accent hover:text-accent-light transition-colors"
              >
                {isRegister
                  ? 'Already have an account? Sign in'
                  : "Don't have an account? Create one"}
              </button>
            </div>

            {/* Demo Credentials Hint */}
            {!isRegister && (
              <div className="mt-6 pt-6 border-t border-border-light">
                <p className="text-xs text-text-muted text-center mb-2">Default Credentials:</p>
                <div className="flex items-center justify-center gap-4 text-sm">
                  <div className="px-3 py-1.5 bg-bg-secondary/50 rounded-lg border border-border">
                    <span className="text-text-muted">User:</span>{' '}
                    <span className="font-mono text-accent">admin</span>
                  </div>
                  <div className="px-3 py-1.5 bg-bg-secondary/50 rounded-lg border border-border">
                    <span className="text-text-muted">Pass:</span>{' '}
                    <span className="font-mono text-accent">admin@123</span>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Supported Platforms */}
          <div className="mt-8 text-center animate-fade-in-up" style={{ animationDelay: '0.2s' }}>
            <p className="text-xs text-text-muted mb-3">Supported Platforms</p>
            <div className="flex items-center justify-center gap-6 opacity-60">
              <div className="text-text-secondary text-sm font-medium">Snowflake</div>
              <div className="text-text-muted">•</div>
              <div className="text-text-secondary text-sm font-medium">Redshift</div>
              <div className="text-text-muted">•</div>
              <div className="text-text-secondary text-sm font-medium">Databricks</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
