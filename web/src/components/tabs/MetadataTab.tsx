import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getConfig, getCatalog, getConversionObjects } from '../../api/endpoints'
import LoadingOverlay from '../common/LoadingOverlay'

export default function MetadataTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data: configData, isLoading: configLoading } = useQuery({
    queryKey: ['config'],
    queryFn: getConfig,
  })

  const { data: catalogData, isLoading: catalogLoading } = useQuery({
    queryKey: ['catalog', schemasParam],
    queryFn: () => getCatalog(schemasParam),
  })

  const { data: conversionData, isLoading: conversionLoading } = useQuery({
    queryKey: ['conversion-objects', schemasParam],
    queryFn: () => getConversionObjects(schemasParam),
  })

  const isLoading = configLoading || catalogLoading || conversionLoading

  const config = configData || {}
  const catalog = catalogData || {}
  const conversion = conversionData || {}

  // Extract object types from catalog
  const views = catalog.views || []
  const procedures = catalog.procs || []
  const udfs = catalog.udfs || []
  const materializedViews = catalog.materialized_views || []

  // Calculate automation stats from conversion
  const objects = conversion.objects || []
  const autoConvertible = objects.filter((o: any) => o.classification === 'AUTO_CONVERT').length
  const manualRequired = objects.filter((o: any) => o.classification === 'MANUAL_REWRITE_REQUIRED').length
  const totalObjects = objects.length

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Metadata</h2>
        <p className="text-text-secondary">
          Configuration and metadata information
        </p>
      </div>

      {/* Database Objects Summary */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <div className="text-3xl font-bold text-accent mb-2">{views.length}</div>
          <div className="text-sm text-text-secondary">Views</div>
        </div>
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <div className="text-3xl font-bold text-purple mb-2">{procedures.length}</div>
          <div className="text-sm text-text-secondary">Stored Procedures</div>
        </div>
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <div className="text-3xl font-bold text-success mb-2">{udfs.length}</div>
          <div className="text-sm text-text-secondary">User Defined Functions</div>
        </div>
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <div className="text-3xl font-bold text-warning mb-2">{materializedViews.length}</div>
          <div className="text-sm text-text-secondary">Materialized Views</div>
        </div>
      </div>

      {/* Automation Possibility */}
      <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
        <h3 className="text-lg font-semibold text-text-primary mb-4">Migration Automation Analysis</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="bg-success/10 border border-success/30 rounded-lg p-4">
            <div className="text-2xl font-bold text-success mb-1">{autoConvertible}</div>
            <div className="text-sm text-text-secondary mb-2">Fully Automated</div>
            <div className="text-xs text-text-muted">
              {totalObjects > 0 ? ((autoConvertible / totalObjects) * 100).toFixed(1) : 0}% of total objects
            </div>
          </div>
          <div className="bg-warning/10 border border-warning/30 rounded-lg p-4">
            <div className="text-2xl font-bold text-warning mb-1">
              {totalObjects - autoConvertible - manualRequired}
            </div>
            <div className="text-sm text-text-secondary mb-2">Needs Review</div>
            <div className="text-xs text-text-muted">
              Minor manual verification required
            </div>
          </div>
          <div className="bg-error/10 border border-error/30 rounded-lg p-4">
            <div className="text-2xl font-bold text-error mb-1">{manualRequired}</div>
            <div className="text-sm text-text-secondary mb-2">Manual Intervention</div>
            <div className="text-xs text-text-muted">
              {totalObjects > 0 ? ((manualRequired / totalObjects) * 100).toFixed(1) : 0}% requires manual rewrite
            </div>
          </div>
        </div>
      </div>

      {/* Configuration Details */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <h3 className="text-lg font-semibold text-text-primary mb-4">Configuration</h3>
          <div className="space-y-3">
            <div className="flex justify-between py-2 border-b border-border-light">
              <span className="text-text-secondary">Source Adapter</span>
              <span className="text-text-primary font-medium">{config.source_adapter || 'N/A'}</span>
            </div>
            <div className="flex justify-between py-2 border-b border-border-light">
              <span className="text-text-secondary">Target Platform</span>
              <span className="text-text-primary font-medium">{config.target_platform || 'N/A'}</span>
            </div>
            <div className="flex justify-between py-2 border-b border-border-light">
              <span className="text-text-secondary">Confidence Threshold</span>
              <span className="text-text-primary font-medium">
                {config.confidence_threshold ? (config.confidence_threshold * 100).toFixed(0) + '%' : 'N/A'}
              </span>
            </div>
          </div>
        </div>

        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <h3 className="text-lg font-semibold text-text-primary mb-4">Project Info</h3>
          <div className="space-y-3">
            <div className="flex justify-between py-2 border-b border-border-light">
              <span className="text-text-secondary">Platform</span>
              <span className="text-text-primary font-medium">MigData</span>
            </div>
            <div className="flex justify-between py-2 border-b border-border-light">
              <span className="text-text-secondary">Version</span>
              <span className="text-text-primary font-medium">1.0.0</span>
            </div>
            <div className="flex justify-between py-2 border-b border-border-light">
              <span className="text-text-secondary">Environment</span>
              <span className="text-text-primary font-medium">Development</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
