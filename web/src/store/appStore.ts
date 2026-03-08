import { create } from 'zustand'

interface PipelineStatus {
  running: boolean
  currentStep: number
  totalSteps: number
  label: string
  jobId: string
}

interface PipelineStep {
  step: number
  label: string
  status: 'pending' | 'running' | 'done' | 'error'
  total_steps: number
  elapsed_ms?: number
}

interface AppState {
  sourcePlatform: string
  targetPlatform: string
  selectedSchemas: string[]
  confidenceThreshold: number
  activeTab: number
  useMock: boolean
  pipeline: PipelineStatus
  pipelineSteps: PipelineStep[]
  progressPercent: number
  progressMetrics: Record<string, number>
  llmProvider: string

  setSourcePlatform: (p: string) => void
  setTargetPlatform: (p: string) => void
  setSelectedSchemas: (s: string[]) => void
  setConfidenceThreshold: (t: number) => void
  setActiveTab: (t: number) => void
  setUseMock: (m: boolean) => void
  setPipeline: (p: Partial<PipelineStatus>) => void
  setPipelineSteps: (steps: PipelineStep[]) => void
  setProgressPercent: (percent: number) => void
  setProgressMetrics: (metrics: Record<string, number>) => void
  setLlmProvider: (id: string) => void
}

export const useAppStore = create<AppState>((set) => ({
  sourcePlatform: 'snowflake',
  targetPlatform: 'databricks',
  selectedSchemas: [],
  confidenceThreshold: 0.6,
  activeTab: 0,
  useMock: true,
  pipeline: {
    running: false,
    currentStep: 0,
    totalSteps: 0,
    label: '',
    jobId: '',
  },
  pipelineSteps: [],
  progressPercent: 0,
  progressMetrics: {},
  llmProvider: 'claude-opus-4',

  setSourcePlatform: (p) => set({ sourcePlatform: p }),
  setTargetPlatform: (p) => set({ targetPlatform: p }),
  setSelectedSchemas: (s) => set({ selectedSchemas: s }),
  setConfidenceThreshold: (t) => set({ confidenceThreshold: t }),
  setActiveTab: (t) => set({ activeTab: t }),
  setUseMock: (m) => set({ useMock: m }),
  setPipeline: (p) =>
    set((state) => ({ pipeline: { ...state.pipeline, ...p } })),
  setPipelineSteps: (steps) => set({ pipelineSteps: steps }),
  setProgressPercent: (percent) => set({ progressPercent: percent }),
  setProgressMetrics: (metrics) => set({ progressMetrics: metrics }),
  setLlmProvider: (id) => set({ llmProvider: id }),
}))
