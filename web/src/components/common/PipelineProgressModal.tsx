import { useEffect, useState } from 'react'
import ProgressBar from './ProgressBar'
import type { PipelineStep } from '../../types'

interface PipelineProgressModalProps {
  isOpen: boolean
  steps: PipelineStep[]
  progressPercent: number
  currentLabel: string
  metrics: Record<string, number>
  onClose?: () => void
}

export default function PipelineProgressModal({
  isOpen,
  steps,
  progressPercent,
  currentLabel,
  metrics,
  onClose,
}: PipelineProgressModalProps) {
  const [completedMilestones, setCompletedMilestones] = useState<string[]>([])

  // Track completed milestones based on progress
  useEffect(() => {
    const milestones: string[] = []

    if (progressPercent >= 15) milestones.push('Source catalog extracted')
    if (progressPercent >= 40) milestones.push('SQL conversion completed')
    if (progressPercent >= 65) milestones.push('Data loading finished')
    if (progressPercent >= 85) milestones.push('Validation checks passed')
    if (progressPercent >= 100) milestones.push('Pipeline completed successfully')

    setCompletedMilestones(milestones)
  }, [progressPercent])

  if (!isOpen) return null

  const isComplete = progressPercent >= 100

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Blur backdrop */}
      <div
        className="absolute inset-0 bg-bg-primary/80 backdrop-blur-md"
        onClick={isComplete ? onClose : undefined}
      />

      {/* Modal content */}
      <div className="relative w-full max-w-2xl mx-4 bg-gradient-to-br from-bg-card via-bg-card-alt to-bg-card rounded-2xl border-2 border-accent/30 shadow-[0_20px_80px_rgba(0,212,255,0.3)] p-8 animate-fade-in-up">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-gradient-to-r from-accent to-purple mb-4 animate-pulse-glow">
            {isComplete ? (
              <svg className="w-10 h-10 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            ) : (
              <svg className="w-10 h-10 text-white animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
              </svg>
            )}
          </div>

          <h2 className="text-2xl font-bold text-text-primary mb-2">
            {isComplete ? 'Pipeline Complete! 🎉' : 'Migration Pipeline Running'}
          </h2>

          <p className="text-text-secondary">
            {isComplete
              ? 'Your data migration has completed successfully'
              : 'Please wait while we process your data migration'
            }
          </p>
        </div>

        {/* Progress bar */}
        <div className="mb-8">
          <ProgressBar
            percent={progressPercent}
            label={currentLabel}
            metrics={metrics}
            showMetrics={true}
          />
        </div>

        {/* Completed milestones */}
        <div className="space-y-3 mb-8">
          <h3 className="text-sm font-semibold text-text-secondary uppercase tracking-wide mb-3">
            Completed Milestones
          </h3>

          {completedMilestones.length > 0 ? (
            <div className="space-y-2">
              {completedMilestones.map((milestone, idx) => (
                <div
                  key={milestone}
                  className="flex items-center gap-3 bg-success/10 border border-success/30 rounded-lg px-4 py-3 animate-fade-in-up"
                  style={{ animationDelay: `${idx * 100}ms` }}
                >
                  <svg className="w-5 h-5 text-success flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span className="text-text-primary font-medium">{milestone}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center text-text-muted italic py-4">
              Initializing pipeline...
            </div>
          )}
        </div>

        {/* Step progress */}
        <div className="space-y-2 mb-6">
          <h3 className="text-sm font-semibold text-text-secondary uppercase tracking-wide mb-3">
            Pipeline Steps
          </h3>

          {steps.map((step) => {
            const icon =
              step.status === 'done'
                ? '✅'
                : step.status === 'running'
                ? '⏳'
                : step.status === 'error'
                ? '❌'
                : '⬜'

            const textColor =
              step.status === 'done'
                ? 'text-success'
                : step.status === 'running'
                ? 'text-accent'
                : step.status === 'error'
                ? 'text-error'
                : 'text-text-muted'

            return (
              <div key={step.step} className="flex items-center gap-3 text-sm">
                <span className="text-lg">{icon}</span>
                <span className={`${textColor} flex-1`}>{step.label}</span>
                {step.elapsed_ms != null && step.status === 'done' && (
                  <span className="text-text-muted text-xs">
                    {(step.elapsed_ms / 1000).toFixed(1)}s
                  </span>
                )}
                {step.status === 'running' && (
                  <span className="inline-block w-4 h-4 border-2 border-border border-t-accent rounded-full animate-spin" />
                )}
              </div>
            )
          })}
        </div>

        {/* Close button (only show when complete) */}
        {isComplete && onClose && (
          <button
            onClick={onClose}
            className="w-full py-3 px-6 rounded-xl bg-gradient-to-r from-accent via-purple to-accent-dark text-white font-semibold shadow-[0_4px_20px_rgba(0,212,255,0.4)] hover:shadow-[0_6px_30px_rgba(0,212,255,0.6)] transition-all transform hover:scale-[1.02]"
          >
            View Results
          </button>
        )}

        {/* Loading indicator */}
        {!isComplete && (
          <div className="text-center text-text-muted text-sm">
            <p>Do not close this window</p>
          </div>
        )}
      </div>
    </div>
  )
}
