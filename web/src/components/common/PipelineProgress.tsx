import type { PipelineStep } from '../../types'

interface PipelineProgressProps {
  steps: PipelineStep[]
}

export default function PipelineProgress({ steps }: PipelineProgressProps) {
  // Guard against undefined or non-array steps
  if (!steps || !Array.isArray(steps) || steps.length === 0) {
    return (
      <div className="text-xs text-text-muted italic">
        Initializing pipeline...
      </div>
    )
  }

  return (
    <div className="space-y-2">
      {steps.map((step) => {
        const icon =
          step.status === 'done'
            ? '\u2705'
            : step.status === 'running'
            ? '\u23F3'
            : step.status === 'error'
            ? '\u274C'
            : '\u2B1C'
        const textColor =
          step.status === 'done'
            ? 'text-success'
            : step.status === 'running'
            ? 'text-indigo'
            : step.status === 'error'
            ? 'text-error'
            : 'text-text-muted'

        return (
          <div key={step.step} className="flex items-center gap-2 text-sm">
            <span>{icon}</span>
            <span className={textColor}>{step.label}</span>
            {step.elapsed_ms != null && step.status === 'done' && (
              <span className="text-text-muted text-xs ml-auto">
                {(step.elapsed_ms / 1000).toFixed(1)}s
              </span>
            )}
            {step.status === 'running' && (
              <span className="ml-auto">
                <span className="inline-block w-4 h-4 border-2 border-border border-t-indigo rounded-full animate-spin" />
              </span>
            )}
          </div>
        )
      })}
    </div>
  )
}
