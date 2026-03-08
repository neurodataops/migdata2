export interface ConversionObject {
  object: string
  schema: string
  type: string
  classification: string
  difficulty: number
  rules_applied: number
  warnings: number
  manual_flags: string[]
}

export interface ConversionSummary {
  total_objects: number
  classifications: Record<string, number>
  total_warnings?: number
  manual_rewrite_count?: number
}

export interface ValidationSummary {
  total_checks: number
  passed: number
  failed: number
  pass_rate: number
  avg_confidence: number
}

export interface ConfidenceScore {
  table: string
  confidence: number
  [key: string]: unknown
}

export interface CatalogTable {
  schema: string
  table: string
  rows_estimate: number
  size_mb: number
  diststyle?: string
  encoded?: string
  pct_used?: number
  cluster_by?: string[]
  auto_clustering?: boolean
  retention_time?: number
}

export interface QueryLog {
  query_id: string
  start_time: string
  end_time: string
  query_type: string
  schema: string
  table: string
  [key: string]: unknown
}

export interface RiskItem {
  category: string
  count: number
  impact: string
  action: string
  severity: 'High' | 'Medium' | 'Low'
}

export interface PipelineStep {
  step: number
  label: string
  status: 'pending' | 'running' | 'done' | 'error'
  elapsed_ms?: number
  error?: string
}

export interface BusinessItem {
  icon: string
  title: string
  default: string
}

export const TAB_LABELS = [
  'Executive Summary',
  'Overview',
  'Objects',
  'Schema Explorer',
  'Relationships',
  'Metadata',
  'Lineage',
  'SQL Comparison',
  'Validation',
  'Manual Work',
] as const
