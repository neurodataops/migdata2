import { useEffect, useState } from 'react'

interface ProgressBarProps {
  percent: number
  label?: string
  metrics?: Record<string, number>
  showMetrics?: boolean
}

export default function ProgressBar({ percent, label, metrics, showMetrics = true }: ProgressBarProps) {
  const [displayPercent, setDisplayPercent] = useState(0)

  // Smooth animation of progress
  useEffect(() => {
    const timer = setTimeout(() => {
      setDisplayPercent(percent)
    }, 50)
    return () => clearTimeout(timer)
  }, [percent])

  return (
    <div className="space-y-3">
      {/* Progress bar */}
      <div className="relative">
        <div className="h-3 bg-bg-secondary/50 rounded-full overflow-hidden border border-border">
          <div
            className="h-full bg-gradient-to-r from-accent via-purple to-accent-dark transition-all duration-500 ease-out relative overflow-hidden"
            style={{ width: `${displayPercent}%` }}
          >
            {/* Animated shimmer effect */}
            <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent animate-shimmer" />
          </div>
        </div>

        {/* Percentage label */}
        <div className="absolute right-0 -top-6 text-xs font-semibold text-accent">
          {displayPercent.toFixed(0)}%
        </div>
      </div>

      {/* Current label */}
      {label && (
        <div className="text-sm text-text-secondary flex items-center gap-2">
          <svg className="w-4 h-4 text-accent animate-pulse" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          {label}
        </div>
      )}

      {/* Metrics grid */}
      {showMetrics && metrics && Object.keys(metrics).length > 0 && (
        <div className="grid grid-cols-2 gap-2 pt-2">
          {Object.entries(metrics).map(([key, value]) => (
            <div key={key} className="bg-bg-secondary/30 rounded-lg px-3 py-2 border border-border-light">
              <div className="text-xs text-text-muted capitalize">{key.replace(/_/g, ' ')}</div>
              <div className="text-sm font-semibold text-text-primary">{typeof value === 'number' ? value.toLocaleString() : value}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
