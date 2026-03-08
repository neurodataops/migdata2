import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import {
  getCatalog,
  getConversion,
  getQueryTimeline,
  getConfidence,
} from '../../api/endpoints'
import MetricCard from '../common/MetricCard'
import LoadingOverlay from '../common/LoadingOverlay'
import Plot from 'react-plotly.js'

export default function OverviewTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data: catalogData, isLoading: catalogLoading } = useQuery({
    queryKey: ['catalog', schemasParam],
    queryFn: () => getCatalog(schemasParam),
  })

  const { data: conversionData, isLoading: conversionLoading } = useQuery({
    queryKey: ['conversion'],
    queryFn: getConversion,
  })

  const { data: queryData, isLoading: queryLoading } = useQuery({
    queryKey: ['query-timeline'],
    queryFn: getQueryTimeline,
  })

  const { data: confidenceData, isLoading: confidenceLoading } = useQuery({
    queryKey: ['confidence', schemasParam],
    queryFn: () => getConfidence(schemasParam),
  })

  const isLoading = catalogLoading || conversionLoading || queryLoading || confidenceLoading

  const catalog = catalogData?.tables || []
  const conversion = conversionData?.summary || { total: 0, classifications: {} }
  const queryTimeline = queryData?.timeline || []

  // Schema distribution
  const schemaGroups = catalog.reduce((acc: Record<string, number>, t: any) => {
    acc[t.schema] = (acc[t.schema] || 0) + 1
    return acc
  }, {})

  const schemaLabels = Object.keys(schemaGroups)
  const schemaValues = Object.values(schemaGroups) as number[]

  // Conversion classification distribution
  const classLabels = Object.keys(conversion.classifications || {})
  const classValues = Object.values(conversion.classifications || {}) as number[]

  // Confidence score distribution
  const confidenceScores = confidenceData?.confidence_scores || []
  const confidenceBySchema = confidenceScores.reduce((acc: Record<string, { total: number, count: number }>, item: any) => {
    const schema = item.table.split('.')[0]
    if (!acc[schema]) {
      acc[schema] = { total: 0, count: 0 }
    }
    acc[schema].total += item.confidence
    acc[schema].count += 1
    return acc
  }, {})

  const confidenceLabels = Object.keys(confidenceBySchema)
  const confidenceValues = confidenceLabels.map(schema =>
    (confidenceBySchema[schema].total / confidenceBySchema[schema].count) * 100
  )

  // Query activity by business category
  const businessCategories = queryTimeline.reduce((acc: Record<string, number>, q: any) => {
    const category = q.business_category || 'Other'
    acc[category] = (acc[category] || 0) + 1
    return acc
  }, {})

  const categoryLabels = Object.keys(businessCategories)
  const categoryValues = Object.values(businessCategories) as number[]

  const totalObjects = catalog.length
  const totalSchemas = schemaLabels.length
  const avgSizePerTable = totalObjects > 0
    ? (catalog.reduce((sum: number, t: any) => sum + (t.size_mb || 0), 0) / totalObjects).toFixed(2)
    : '0.00'

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Overview</h2>
        <p className="text-text-secondary">
          Detailed analytics and distribution charts
        </p>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Total Schemas"
          value={totalSchemas}
          delta={`${totalObjects} tables total`}
        />
        <MetricCard
          label="Avg Table Size"
          value={`${avgSizePerTable} MB`}
          delta="Per table"
        />
        <MetricCard
          label="Query Logs Analyzed"
          value={queryTimeline.length}
          delta={`${categoryLabels.length} business categories`}
        />
      </div>

      {/* Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Schema Distribution */}
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
          <h3 className="text-lg font-semibold text-text-primary mb-4">
            Objects by Schema
          </h3>
          {schemaLabels.length > 0 ? (
            <Plot
              data={[
                {
                  type: 'pie',
                  labels: schemaLabels,
                  values: schemaValues,
                  marker: {
                    colors: ['#7c3aed', '#6d28d9', '#818cf8', '#34d399', '#fbbf24'],
                  },
                  textinfo: 'label+percent',
                  textfont: { color: '#e2e8f0' },
                  hovertemplate: '<b>%{label}</b><br>%{value} tables<br>%{percent}<extra></extra>',
                },
              ]}
              layout={{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: { color: '#e2e8f0', family: 'Inter' },
                margin: { t: 20, b: 20, l: 20, r: 20 },
                height: 300,
                showlegend: true,
                legend: {
                  orientation: 'v',
                  x: 1,
                  y: 0.5,
                  font: { size: 10 },
                },
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          ) : (
            <p className="text-text-muted text-center py-8">No data available</p>
          )}
        </div>

        {/* Conversion Classification */}
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
          <h3 className="text-lg font-semibold text-text-primary mb-4">
            SQL Conversion Classification
          </h3>
          {classLabels.length > 0 ? (
            <Plot
              data={[
                {
                  type: 'bar',
                  x: classLabels.map(l => l.replace('_', ' ')),
                  y: classValues,
                  marker: {
                    color: classLabels.map(l =>
                      l === 'AUTO_CONVERT' ? '#34d399' :
                      l === 'CONVERT_WITH_WARNINGS' ? '#fbbf24' : '#f87171'
                    ),
                  },
                  text: classValues.map(String),
                  textposition: 'auto' as any,
                  textfont: { color: '#fff' },
                  hovertemplate: '<b>%{x}</b><br>%{y} objects<extra></extra>',
                },
              ]}
              layout={{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: { color: '#e2e8f0', family: 'Inter' },
                margin: { t: 20, b: 80, l: 50, r: 20 },
                height: 300,
                xaxis: {
                  gridcolor: 'rgba(124, 58, 237, 0.1)',
                  tickangle: -45,
                },
                yaxis: {
                  gridcolor: 'rgba(124, 58, 237, 0.1)',
                  title: { text: 'Count' },
                },
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          ) : (
            <p className="text-text-muted text-center py-8">No data available</p>
          )}
        </div>

        {/* Confidence Score by Schema */}
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
          <h3 className="text-lg font-semibold text-text-primary mb-4">
            Confidence Score by Schema (%)
          </h3>
          {confidenceLabels.length > 0 ? (
            <Plot
              data={[
                {
                  type: 'bar',
                  x: confidenceLabels,
                  y: confidenceValues,
                  marker: {
                    color: confidenceValues.map(v =>
                      v >= 80 ? '#34d399' : v >= 60 ? '#fbbf24' : '#f87171'
                    ),
                  },
                  text: confidenceValues.map((v) => `${v.toFixed(1)}%`),
                  textposition: 'auto' as any,
                  textfont: { color: '#fff' },
                  hovertemplate: '<b>%{x}</b><br>%{y:.1f}%<extra></extra>',
                },
              ]}
              layout={{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: { color: '#e2e8f0', family: 'Inter' },
                margin: { t: 20, b: 60, l: 60, r: 20 },
                height: 300,
                xaxis: { gridcolor: 'rgba(124, 58, 237, 0.1)' },
                yaxis: {
                  gridcolor: 'rgba(124, 58, 237, 0.1)',
                  title: { text: 'Confidence (%)' },
                  range: [0, 100],
                },
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          ) : (
            <p className="text-text-muted text-center py-8">No data available</p>
          )}
        </div>

        {/* Query Activity by Business Function */}
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6 shadow-[0_4px_15px_rgba(0,0,0,0.3)]">
          <h3 className="text-lg font-semibold text-text-primary mb-4">
            Query Activity by Business Function
          </h3>
          {categoryLabels.length > 0 ? (
            <Plot
              data={[
                {
                  type: 'pie',
                  labels: categoryLabels,
                  values: categoryValues,
                  marker: {
                    colors: ['#7c3aed', '#00d4ff', '#34d399', '#fbbf24', '#f87171', '#818cf8', '#6d28d9', '#10b981', '#fb923c'],
                  },
                  textinfo: 'label+percent',
                  textfont: { color: '#e2e8f0', size: 11 },
                  hovertemplate: '<b>%{label}</b><br>%{value} queries<br>%{percent}<extra></extra>',
                },
              ]}
              layout={{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: { color: '#e2e8f0', family: 'Inter' },
                margin: { t: 20, b: 20, l: 20, r: 20 },
                height: 300,
                showlegend: true,
                legend: {
                  orientation: 'v',
                  x: 1.05,
                  y: 0.5,
                  font: { size: 9 },
                },
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          ) : (
            <p className="text-text-muted text-center py-8">No data available</p>
          )}
        </div>
      </div>
    </div>
  )
}
