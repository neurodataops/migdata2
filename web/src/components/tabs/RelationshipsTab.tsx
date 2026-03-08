import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getRelationships } from '../../api/endpoints'
import DataTable from '../common/DataTable'
import MetricCard from '../common/MetricCard'
import LoadingOverlay from '../common/LoadingOverlay'

export default function RelationshipsTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data, isLoading } = useQuery({
    queryKey: ['relationships', schemasParam],
    queryFn: () => getRelationships(schemasParam),
  })

  const relationships = data?.relationships || []
  const totalCount = data?.total_count || 0

  // Calculate stats
  const uniqueTables = new Set([
    ...relationships.map((r: any) => `${r.from_schema}.${r.from_table}`),
    ...relationships.map((r: any) => `${r.to_schema}.${r.to_table}`),
  ]).size

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Relationship Mapping</h2>
        <p className="text-text-secondary">
          Foreign key relationships and table dependencies
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard
          label="Total Relationships"
          value={totalCount.toLocaleString()}
          delta="Foreign key constraints"
        />
        <MetricCard
          label="Connected Tables"
          value={uniqueTables.toLocaleString()}
          delta="Tables with relationships"
        />
        <MetricCard
          label="Avg per Table"
          value={uniqueTables > 0 ? (totalCount / uniqueTables).toFixed(1) : '0'}
          delta="Relationships per table"
        />
      </div>

      <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
        <h3 className="text-xl font-semibold text-text-primary mb-4">
          Foreign Key Relationships
        </h3>
        {relationships.length > 0 ? (
          <DataTable
            data={relationships}
            columns={[
              {
                key: 'from_table',
                label: 'Source Table',
                render: (val: any, row: any) => (
                  <span className="font-medium text-accent">
                    {row.from_schema}.{val}
                  </span>
                ),
                sortable: true,
              },
              {
                key: 'from_column',
                label: 'Source Column',
                sortable: true,
              },
              {
                key: 'to_table',
                label: 'Target Table',
                render: (val: any, row: any) => (
                  <span className="font-medium text-purple">
                    {row.to_schema}.{val}
                  </span>
                ),
                sortable: true,
              },
              {
                key: 'to_column',
                label: 'Target Column',
                sortable: true,
              },
              {
                key: 'constraint_name',
                label: 'Constraint Name',
                render: (val: any) => (
                  <span className="text-text-muted text-sm">{val || 'N/A'}</span>
                ),
              },
            ]}
          />
        ) : (
          <div className="text-center py-12 text-text-secondary">
            <p>No foreign key relationships found in the selected schemas.</p>
            <p className="text-sm text-text-muted mt-2">
              Run the pipeline to discover relationships in your database.
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
