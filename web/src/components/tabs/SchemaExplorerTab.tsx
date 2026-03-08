import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getCatalog } from '../../api/endpoints'
import LoadingOverlay from '../common/LoadingOverlay'

export default function SchemaExplorerTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data: catalogData, isLoading } = useQuery({
    queryKey: ['catalog', schemasParam],
    queryFn: () => getCatalog(schemasParam),
  })

  const catalog = catalogData?.tables || []

  // Group by schema
  const schemaGroups = catalog.reduce((acc: Record<string, any[]>, table: any) => {
    if (!acc[table.schema]) acc[table.schema] = []
    acc[table.schema].push(table)
    return acc
  }, {})

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Schema Explorer</h2>
        <p className="text-text-secondary">
          Browse database schemas and their tables
        </p>
      </div>

      <div className="space-y-4">
        {Object.entries(schemaGroups).map(([schema, tables]) => (
          <div
            key={schema}
            className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-xl font-semibold text-text-primary flex items-center gap-2">
                <svg className="w-5 h-5 text-accent" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
                </svg>
                {schema.toUpperCase()}
              </h3>
              <span className="px-3 py-1 bg-accent/20 text-accent rounded-full text-sm font-semibold">
                {tables.length} tables
              </span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
              {tables.map((table: any) => (
                <div
                  key={table.table}
                  className="bg-bg-secondary/50 border border-border-light rounded-xl p-4 hover:border-accent/50 transition-all"
                >
                  <h4 className="font-semibold text-text-primary mb-2">{table.table}</h4>
                  <div className="text-xs text-text-muted space-y-1">
                    <div>Rows: {table.rows_estimate?.toLocaleString() || 'N/A'}</div>
                    <div>Size: {table.size_mb?.toFixed(2) || '0'} MB</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
