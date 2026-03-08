import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getSqlComparison } from '../../api/endpoints'
import LoadingOverlay from '../common/LoadingOverlay'

export default function SQLComparisonTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data, isLoading } = useQuery({
    queryKey: ['sql-comparison', schemasParam],
    queryFn: () => getSqlComparison(schemasParam),
  })

  const comparisons = data?.comparisons || []
  const [selectedObject, setSelectedObject] = useState<any>(null)

  // Auto-select first object when data loads
  if (comparisons.length > 0 && !selectedObject) {
    setSelectedObject(comparisons[0])
  }

  const difficultyColor = (difficulty: number) => {
    if (difficulty >= 7) return 'text-error'
    if (difficulty >= 4) return 'text-warning'
    return 'text-success'
  }

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">SQL Comparison</h2>
        <p className="text-text-secondary">
          Side-by-side comparison of original and converted SQL
        </p>
      </div>

      {comparisons.length > 0 ? (
        <div className="space-y-6">
          {/* Object Selector Dropdown */}
          <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-4 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
            <label className="block text-sm font-medium text-text-secondary mb-2">
              Select Object to Compare
            </label>
            <select
              value={selectedObject?.object_name || ''}
              onChange={(e) => {
                const obj = comparisons.find((c: any) => c.object_name === e.target.value)
                setSelectedObject(obj)
              }}
              className="w-full px-4 py-3 bg-bg-secondary border border-border rounded-xl text-text-primary focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent"
            >
              <option value="">-- Select an object --</option>
              {comparisons.map((obj: any) => (
                <option key={obj.object_name} value={obj.object_name}>
                  {obj.object_name} (Difficulty: {obj.difficulty}/10
                  {obj.warnings?.length > 0 ? ` - ${obj.warnings.length} warnings` : ''})
                </option>
              ))}
            </select>
          </div>

          {/* SQL Comparison */}
          <div>
            {selectedObject ? (
              <div className="space-y-4">
                {/* Header */}
                <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-4 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-xl font-semibold text-text-primary">
                      {selectedObject.object_name}
                    </h3>
                    <div className="flex items-center gap-4">
                      <span className="text-sm text-text-secondary">
                        Type: <span className="font-medium">{selectedObject.type}</span>
                      </span>
                      <span className="text-sm text-text-secondary">
                        Classification:{' '}
                        <span className="font-medium">{selectedObject.classification}</span>
                      </span>
                    </div>
                  </div>
                  {selectedObject.warnings?.length > 0 && (
                    <div className="mt-3 space-y-1">
                      {selectedObject.warnings.map((warning: string, idx: number) => (
                        <div
                          key={idx}
                          className="text-sm text-warning bg-warning/10 border border-warning/30 rounded-lg px-3 py-2"
                        >
                          ⚠ {warning}
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Side-by-side SQL */}
                <div className="grid grid-cols-2 gap-4">
                  {/* Source SQL */}
                  <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-4 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="text-sm font-semibold text-text-primary">
                        Source SQL (Snowflake)
                      </h4>
                      <span className="text-xs text-text-muted bg-bg-alt px-2 py-1 rounded">
                        Original
                      </span>
                    </div>
                    <pre className="text-xs text-text-secondary bg-bg-alt rounded-lg p-4 overflow-x-auto max-h-[500px] overflow-y-auto">
                      <code>{selectedObject.source_sql || 'No source SQL available'}</code>
                    </pre>
                  </div>

                  {/* Target SQL */}
                  <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-4 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="text-sm font-semibold text-text-primary">
                        Target SQL (Databricks)
                      </h4>
                      <span className="text-xs text-accent bg-accent/10 px-2 py-1 rounded">
                        Converted
                      </span>
                    </div>
                    <pre className="text-xs text-text-secondary bg-bg-alt rounded-lg p-4 overflow-x-auto max-h-[500px] overflow-y-auto">
                      <code>{selectedObject.target_sql || 'No target SQL available'}</code>
                    </pre>
                  </div>
                </div>

                {/* Rules Applied */}
                {selectedObject.rules_applied?.length > 0 && (
                  <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-4 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
                    <h4 className="text-sm font-semibold text-text-primary mb-3">
                      Conversion Rules Applied ({selectedObject.rules_applied.length})
                    </h4>
                    <div className="flex flex-wrap gap-2">
                      {selectedObject.rules_applied.map((rule: string, idx: number) => (
                        <span
                          key={idx}
                          className="text-xs text-accent bg-accent/10 border border-accent/30 rounded-lg px-3 py-1"
                        >
                          {rule}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-12 text-center shadow-[0_8px_30px_rgba(0,212,255,0.1)]">
                <p className="text-text-secondary">Select an object to view SQL comparison</p>
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-12 text-center shadow-[0_8px_30px_rgba(0,212,255,0.1)]">
          <div className="max-w-md mx-auto">
            <div className="w-16 h-16 mx-auto mb-4 rounded-2xl bg-gradient-to-br from-purple/20 to-accent/20 border border-purple/30 flex items-center justify-center">
              <svg
                className="w-8 h-8 text-purple"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"
                />
              </svg>
            </div>
            <h3 className="text-xl font-semibold text-text-primary mb-2">No SQL Comparisons</h3>
            <p className="text-text-secondary">
              Run the pipeline to generate SQL conversion comparisons for your database objects.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}
