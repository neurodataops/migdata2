import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getValidation, getConfidence } from '../../api/endpoints'
import MetricCard from '../common/MetricCard'
import DataTable from '../common/DataTable'
import LoadingOverlay from '../common/LoadingOverlay'
import Plot from 'react-plotly.js'

export default function ValidationTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')
  const [schemaFilter, setSchemaFilter] = useState<string>('all')

  const { data: validationData, isLoading: validationLoading } = useQuery({
    queryKey: ['validation'],
    queryFn: getValidation,
  })

  const { data: confidenceData, isLoading: confidenceLoading } = useQuery({
    queryKey: ['confidence', schemasParam],
    queryFn: () => getConfidence(schemasParam),
  })

  const isLoading = validationLoading || confidenceLoading

  const summary = validationData?.summary || {
    total_checks: 0,
    passed: 0,
    failed: 0,
    pass_rate: 0,
    avg_confidence: 0,
  }

  const confidenceScores = confidenceData?.confidence_scores || []

  // Extract unique schemas from confidence scores
  const schemas = ['all', ...new Set(confidenceScores.map((c: any) => c.table.split('.')[0]))]

  // Filter confidence scores by schema
  const filteredScores = schemaFilter === 'all'
    ? confidenceScores
    : confidenceScores.filter((c: any) => c.table.split('.')[0] === schemaFilter)

  // Prepare heatmap data (group by schema)
  const schemaGroups = confidenceScores.reduce((acc: Record<string, any[]>, score: any) => {
    const schema = score.table.split('.')[0]
    if (!acc[schema]) acc[schema] = []
    acc[schema].push(score)
    return acc
  }, {})

  const heatmapSchemas = Object.keys(schemaGroups)
  const maxTablesPerSchema = Math.max(...heatmapSchemas.map(s => schemaGroups[s].length), 1)

  // Create heatmap data - each row is a schema, each column is a table
  const heatmapData = heatmapSchemas.map(schema => {
    const tables = schemaGroups[schema]
    return tables.map((t: any) => (t.confidence * 100).toFixed(1))
  })

  const heatmapText = heatmapSchemas.map(schema => {
    const tables = schemaGroups[schema]
    return tables.map((t: any) => `${t.table.split('.')[1]}<br>${(t.confidence * 100).toFixed(1)}%`)
  })

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Validation Results</h2>
        <p className="text-text-secondary">
          Data quality checks and confidence scores
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <MetricCard
          label="Total Checks"
          value={summary.total_checks.toLocaleString()}
          delta="Validation tests run"
        />
        <MetricCard
          label="Passed"
          value={summary.passed.toLocaleString()}
          delta={`${summary.pass_rate.toFixed(1)}% pass rate`}
          deltaColor="green"
        />
        <MetricCard
          label="Failed"
          value={summary.failed.toLocaleString()}
          delta="Need attention"
          deltaColor={summary.failed > 0 ? 'red' : 'default'}
        />
        <MetricCard
          label="Avg Confidence"
          value={`${(summary.avg_confidence * 100).toFixed(1)}%`}
          delta="Across all objects"
          deltaColor={summary.avg_confidence >= 0.8 ? 'green' : 'default'}
        />
      </div>

      {/* Confidence Heatmap */}
      {heatmapSchemas.length > 0 && (
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <h3 className="text-xl font-semibold text-text-primary mb-4">
            Confidence Score Heatmap
          </h3>
          <Plot
            data={[
              {
                type: 'heatmap' as any,
                z: heatmapData,
                text: heatmapText,
                hovertemplate: '%{text}<extra></extra>',
                colorscale: [
                  [0, '#f87171'],
                  [0.6, '#fbbf24'],
                  [0.8, '#34d399'],
                  [1, '#10b981'],
                ],
                showscale: true,
                colorbar: {
                  title: 'Confidence %',
                  titleside: 'right',
                  tickmode: 'linear',
                  tick0: 0,
                  dtick: 20,
                },
              },
            ]}
            layout={{
              paper_bgcolor: 'transparent',
              plot_bgcolor: 'transparent',
              font: { color: '#e2e8f0', family: 'Inter' },
              margin: { t: 40, b: 100, l: 150, r: 100 },
              height: Math.max(300, heatmapSchemas.length * 40),
              yaxis: {
                title: 'Schema',
                ticktext: heatmapSchemas,
                tickvals: heatmapSchemas.map((_, i) => i),
                automargin: true,
              },
              xaxis: {
                title: 'Tables',
                showticklabels: false,
              },
            }}
            config={{ displayModeBar: false, responsive: true }}
            style={{ width: '100%' }}
          />
        </div>
      )}

      {/* Confidence Scores Table with Filter */}
      <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-xl font-semibold text-text-primary">
            Confidence Scores by Table
          </h3>
          <select
            value={schemaFilter}
            onChange={(e) => setSchemaFilter(e.target.value)}
            className="px-4 py-2 bg-bg-secondary border border-border rounded-lg text-text-primary focus:outline-none focus:ring-2 focus:ring-accent"
          >
            {schemas.map(schema => (
              <option key={schema} value={schema}>
                {schema === 'all' ? 'All Schemas' : schema}
              </option>
            ))}
          </select>
        </div>
        {filteredScores.length > 0 ? (
          <DataTable
            data={filteredScores}
            columns={[
              { key: 'table', label: 'Table', sortable: true },
              {
                key: 'confidence',
                label: 'Confidence Score',
                render: (val: any) => {
                  const conf = Number(val)
                  return (
                    <span className={`font-semibold ${
                      conf >= 0.8 ? 'text-success' :
                      conf >= 0.6 ? 'text-warning' : 'text-error'
                    }`}>
                      {(conf * 100).toFixed(0)}%
                    </span>
                  )
                },
                sortable: true,
              },
            ]}
          />
        ) : (
          <p className="text-text-muted text-center py-8">
            No confidence scores available. Run the pipeline to generate validation data.
          </p>
        )}
      </div>
    </div>
  )
}
