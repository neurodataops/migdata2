import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import {
  getCatalog,
  getConversion,
  getValidation,
  getLoadSummary,
} from '../../api/endpoints'
import MetricCard from '../common/MetricCard'
import LoadingOverlay from '../common/LoadingOverlay'
import type { ConversionSummary, ValidationSummary } from '../../types'

export default function ExecutiveSummaryTab() {
  const { data: catalogData, isLoading: catalogLoading } = useQuery({
    queryKey: ['catalog-all'],
    queryFn: () => getCatalog(), // Get all catalog data without schema filter
  })

  const { data: conversionData, isLoading: conversionLoading } = useQuery({
    queryKey: ['conversion'],
    queryFn: getConversion,
  })

  const { data: validationData, isLoading: validationLoading } = useQuery({
    queryKey: ['validation'],
    queryFn: getValidation,
  })

  const { data: loadData, isLoading: loadLoading } = useQuery({
    queryKey: ['load-summary'],
    queryFn: getLoadSummary,
  })

  const isLoading = catalogLoading || conversionLoading || validationLoading || loadLoading

  const catalog = catalogData?.tables || []
  const conversion: ConversionSummary = conversionData?.summary || { total_objects: 0, classifications: {} }
  const validation: ValidationSummary = validationData?.summary || {
    total_checks: 0,
    passed: 0,
    failed: 0,
    pass_rate: 0,
    avg_confidence: 0,
  }
  const loadSummary = loadData?.summary || {}

  const totalObjects = catalog.length
  const totalSizeMB = catalog.reduce((sum: number, t: any) => sum + (t.size_mb || 0), 0)
  const totalRows = catalog.reduce((sum: number, t: any) => sum + (t.rows_estimate || 0), 0)

  const autoConvert = conversion.classifications?.['AUTO_CONVERT'] || 0
  const withWarnings = conversion.classifications?.['CONVERT_WITH_WARNINGS'] || 0
  const manualWork = conversion.classifications?.['MANUAL_REWRITE_REQUIRED'] || 0

  const automationRate = conversion.total_objects > 0
    ? ((autoConvert / conversion.total_objects) * 100).toFixed(1)
    : '0.0'
  const automationRateNum = parseFloat(automationRate)

  const totalLoaded = loadSummary.tables_loaded || 0
  const totalLoadedRows = loadSummary.total_rows_loaded || 0

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Executive Summary</h2>
        <p className="text-text-secondary">
          High-level overview of your data migration project
        </p>
      </div>

      {/* Key Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard
          label="Total Objects"
          value={totalObjects.toLocaleString()}
          delta={`${totalSizeMB.toFixed(1)} MB total size`}
        />
        <MetricCard
          label="Total Rows"
          value={totalRows.toLocaleString()}
          delta={`Across ${totalObjects} tables`}
        />
        <MetricCard
          label="Automation Rate"
          value={`${automationRate}%`}
          delta={`${autoConvert} auto-convertible`}
          deltaColor="green"
        />
        <MetricCard
          label="Validation Pass Rate"
          value={`${validation.pass_rate.toFixed(1)}%`}
          delta={`${validation.passed}/${validation.total_checks} checks passed`}
          deltaColor={validation.pass_rate >= 90 ? 'green' : validation.pass_rate >= 70 ? 'default' : 'red'}
        />
      </div>

      {/* Conversion Summary */}
      <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
        <h3 className="text-xl font-semibold text-text-primary mb-4">
          SQL Conversion Summary
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-success/10 border border-success/30 rounded-lg p-4">
            <p className="text-success text-2xl font-bold">{autoConvert}</p>
            <p className="text-text-secondary text-sm mt-1">Auto-convertible</p>
            <p className="text-text-muted text-xs mt-2">
              No manual intervention needed
            </p>
          </div>
          <div className="bg-warning/10 border border-warning/30 rounded-lg p-4">
            <p className="text-warning text-2xl font-bold">{withWarnings}</p>
            <p className="text-text-secondary text-sm mt-1">Convert with Warnings</p>
            <p className="text-text-muted text-xs mt-2">
              Minor review recommended
            </p>
          </div>
          <div className="bg-error/10 border border-error/30 rounded-lg p-4">
            <p className="text-error text-2xl font-bold">{manualWork}</p>
            <p className="text-text-secondary text-sm mt-1">Manual Rewrite Required</p>
            <p className="text-text-muted text-xs mt-2">
              Significant rework needed
            </p>
          </div>
        </div>
      </div>

      {/* Data Load Summary */}
      <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
        <h3 className="text-xl font-semibold text-text-primary mb-4">
          Data Load Status
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <MetricCard
            label="Tables Loaded"
            value={totalLoaded.toLocaleString()}
            delta={`Out of ${totalObjects} total tables`}
          />
          <MetricCard
            label="Rows Loaded"
            value={totalLoadedRows.toLocaleString()}
            delta="Parquet format"
          />
        </div>
      </div>

      {/* Confidence Metrics */}
      <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
        <h3 className="text-xl font-semibold text-text-primary mb-4">
          Quality & Confidence
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <MetricCard
            label="Average Confidence Score"
            value={`${(validation.avg_confidence * 100).toFixed(1)}%`}
            delta="Across all validated objects"
            deltaColor={validation.avg_confidence >= 0.8 ? 'green' : 'default'}
          />
          <MetricCard
            label="Validation Checks"
            value={validation.total_checks.toLocaleString()}
            delta={`${validation.failed} failed, ${validation.passed} passed`}
            deltaColor={validation.failed === 0 ? 'green' : 'red'}
          />
        </div>
      </div>

      {/* Migration Readiness */}
      <div className="bg-gradient-to-br from-accent/10 to-accent-dark/10 border border-accent/30 rounded-xl p-6">
        <h3 className="text-xl font-semibold text-text-primary mb-2">
          Migration Readiness
        </h3>
        <p className="text-text-secondary mb-4">
          Based on the analysis, your migration is{' '}
          <span className={`font-bold ${
            automationRateNum >= 90 ? 'text-success' : automationRateNum >= 70 ? 'text-warning' : 'text-error'
          }`}>
            {automationRateNum >= 90 ? 'highly ready' : automationRateNum >= 70 ? 'moderately ready' : 'requires significant work'}
          </span>
        </p>
        <div className="flex gap-2 text-sm">
          <span className="px-3 py-1 bg-success/20 text-success rounded-full border border-success/30">
            {autoConvert} ready to migrate
          </span>
          <span className="px-3 py-1 bg-warning/20 text-warning rounded-full border border-warning/30">
            {withWarnings} need review
          </span>
          <span className="px-3 py-1 bg-error/20 text-error rounded-full border border-error/30">
            {manualWork} need rework
          </span>
        </div>
      </div>
    </div>
  )
}
