import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getConversionObjects, getCatalog, getConfidence } from '../../api/endpoints'
import DataTable from '../common/DataTable'
import StatusChip from '../common/StatusChip'
import LoadingOverlay from '../common/LoadingOverlay'
import type { ConversionObject, CatalogTable, ConfidenceScore } from '../../types'

export default function ObjectsTab() {
  const { selectedSchemas, confidenceThreshold } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const [searchTerm, setSearchTerm] = useState('')
  const [filterClassification, setFilterClassification] = useState<string>('all')

  const { data: conversionData, isLoading: conversionLoading } = useQuery({
    queryKey: ['conversion-objects', schemasParam],
    queryFn: () => getConversionObjects(schemasParam),
  })

  const { data: catalogData, isLoading: catalogLoading } = useQuery({
    queryKey: ['catalog', schemasParam],
    queryFn: () => getCatalog(schemasParam),
  })

  const { data: confidenceData, isLoading: confidenceLoading } = useQuery({
    queryKey: ['confidence', schemasParam],
    queryFn: () => getConfidence(schemasParam),
  })

  const isLoading = conversionLoading || catalogLoading || confidenceLoading

  const objects: ConversionObject[] = conversionData?.objects || []
  const catalog: CatalogTable[] = catalogData?.tables || []
  const confidence: ConfidenceScore[] = confidenceData?.confidence_scores || []

  // Create lookup maps
  const catalogMap = new Map(catalog.map((t) => [`${t.schema}.${t.table}`, t]))
  const confidenceMap = new Map(confidence.map((c) => [c.table, c.confidence]))

  // Enriched data
  const enriched = useMemo(() => {
    return objects.map((obj) => {
      const key = `${obj.schema}.${obj.object}`
      const catalogEntry = catalogMap.get(key)
      const conf = confidenceMap.get(key) || 0
      return {
        ...obj,
        rows_estimate: catalogEntry?.rows_estimate || 0,
        size_mb: catalogEntry?.size_mb || 0,
        confidence: conf,
      }
    })
  }, [objects, catalogMap, confidenceMap])

  // Filtering
  const filtered = useMemo(() => {
    return enriched.filter((obj) => {
      // Search filter
      const matchesSearch =
        searchTerm === '' ||
        obj.object.toLowerCase().includes(searchTerm.toLowerCase()) ||
        obj.schema.toLowerCase().includes(searchTerm.toLowerCase())

      // Classification filter
      const matchesClass =
        filterClassification === 'all' || obj.classification === filterClassification

      // Confidence threshold filter
      const meetsThreshold = obj.confidence >= confidenceThreshold

      return matchesSearch && matchesClass && meetsThreshold
    })
  }, [enriched, searchTerm, filterClassification, confidenceThreshold])

  const classifications = ['all', ...new Set(objects.map((o) => o.classification))]

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Database Objects</h2>
        <p className="text-text-secondary">
          Detailed view of all database objects with conversion status
        </p>
      </div>

      {/* Filters */}
      <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-4">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1">
            <label className="block text-xs text-text-secondary font-medium mb-2">
              Search
            </label>
            <input
              type="text"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="Search by schema or object name..."
              className="w-full px-4 py-2 bg-bg-secondary border border-border rounded-lg text-text-primary placeholder-text-muted focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent"
            />
          </div>
          <div className="md:w-64">
            <label className="block text-xs text-text-secondary font-medium mb-2">
              Classification
            </label>
            <select
              value={filterClassification}
              onChange={(e) => setFilterClassification(e.target.value)}
              className="w-full px-4 py-2 bg-bg-secondary border border-border rounded-lg text-text-primary focus:outline-none focus:ring-2 focus:ring-accent focus:border-transparent"
            >
              {classifications.map((c) => (
                <option key={c} value={c}>
                  {c === 'all' ? 'All Classifications' : c.replace(/_/g, ' ')}
                </option>
              ))}
            </select>
          </div>
        </div>
        <div className="mt-3 text-sm text-text-muted">
          Showing {filtered.length} of {objects.length} objects
          {confidenceThreshold > 0 && ` (confidence ≥ ${confidenceThreshold.toFixed(2)})`}
        </div>
      </div>

      {/* Objects Table */}
      <DataTable
        data={filtered}
        columns={[
          {
            key: 'schema',
            label: 'Schema',
            sortable: true,
          },
          {
            key: 'object',
            label: 'Object',
            sortable: true,
          },
          {
            key: 'type',
            label: 'Type',
            sortable: true,
          },
          {
            key: 'classification',
            label: 'Classification',
            render: (val) => <StatusChip status={String(val)} />,
            sortable: true,
          },
          {
            key: 'difficulty',
            label: 'Difficulty',
            render: (val) => (
              <span className={`font-medium ${
                Number(val) <= 3 ? 'text-success' :
                Number(val) <= 6 ? 'text-warning' : 'text-error'
              }`}>
                {val}/10
              </span>
            ),
            sortable: true,
          },
          {
            key: 'rules_applied',
            label: 'Rules Applied',
            sortable: true,
          },
          {
            key: 'warnings',
            label: 'Warnings',
            render: (val) => (
              <span className={Number(val) > 0 ? 'text-warning' : 'text-text-secondary'}>
                {val}
              </span>
            ),
            sortable: true,
          },
          {
            key: 'confidence',
            label: 'Confidence',
            render: (val) => {
              const conf = Number(val)
              return (
                <span className={`font-medium ${
                  conf >= 0.8 ? 'text-success' :
                  conf >= 0.6 ? 'text-warning' : 'text-error'
                }`}>
                  {(conf * 100).toFixed(0)}%
                </span>
              )
            },
            sortable: true,
          },
          {
            key: 'rows_estimate',
            label: 'Rows',
            render: (val) => Number(val).toLocaleString(),
            sortable: true,
          },
          {
            key: 'size_mb',
            label: 'Size (MB)',
            render: (val) => Number(val).toFixed(2),
            sortable: true,
          },
        ]}
      />

      {/* Manual Flags Summary */}
      {filtered.some((obj) => obj.manual_flags && obj.manual_flags.length > 0) && (
        <div className="bg-gradient-to-br from-bg-card to-bg-card-alt border border-border rounded-xl p-6">
          <h3 className="text-lg font-semibold text-text-primary mb-4">
            Manual Review Required
          </h3>
          <div className="space-y-2">
            {filtered
              .filter((obj) => obj.manual_flags && obj.manual_flags.length > 0)
              .map((obj, i) => (
                <div
                  key={i}
                  className="bg-bg-secondary border border-border-light rounded-lg p-3"
                >
                  <p className="text-text-primary font-medium mb-1">
                    {obj.schema}.{obj.object}
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {obj.manual_flags.map((flag, j) => (
                      <span
                        key={j}
                        className="px-2 py-1 bg-error/20 text-error text-xs rounded border border-error/30"
                      >
                        {flag}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  )
}
