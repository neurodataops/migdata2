import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getConversionObjects } from '../../api/endpoints'
import DataTable from '../common/DataTable'
import StatusChip from '../common/StatusChip'
import LoadingOverlay from '../common/LoadingOverlay'

export default function ManualWorkTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data: conversionData, isLoading } = useQuery({
    queryKey: ['conversion-objects', schemasParam],
    queryFn: () => getConversionObjects(schemasParam),
  })

  const objects = conversionData?.objects || []

  // Filter only manual rewrite required
  const manualWork = objects.filter((obj: any) =>
    obj.classification === 'MANUAL_REWRITE_REQUIRED' ||
    (obj.manual_flags && obj.manual_flags.length > 0)
  )

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Manual Work Required</h2>
        <p className="text-text-secondary">
          Objects requiring manual intervention and review
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-gradient-to-br from-error/10 to-error/5 border border-error/30 rounded-xl p-6">
          <p className="text-error text-3xl font-bold">{manualWork.length}</p>
          <p className="text-text-secondary text-sm mt-1">Objects Need Manual Work</p>
        </div>
        <div className="bg-gradient-to-br from-warning/10 to-warning/5 border border-warning/30 rounded-xl p-6">
          <p className="text-warning text-3xl font-bold">
            {manualWork.filter((o: any) => o.difficulty >= 7).length}
          </p>
          <p className="text-text-secondary text-sm mt-1">High Difficulty (7+)</p>
        </div>
        <div className="bg-gradient-to-br from-accent/10 to-accent/5 border border-accent/30 rounded-xl p-6">
          <p className="text-accent text-3xl font-bold">
            {manualWork.filter((o: any) => o.warnings > 0).length}
          </p>
          <p className="text-text-secondary text-sm mt-1">With Warnings</p>
        </div>
      </div>

      <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
        <h3 className="text-xl font-semibold text-text-primary mb-4">
          Manual Work Items
        </h3>
        <DataTable
          data={manualWork}
          columns={[
            { key: 'schema', label: 'Schema', sortable: true },
            { key: 'object', label: 'Object', sortable: true },
            { key: 'type', label: 'Type', sortable: true },
            {
              key: 'classification',
              label: 'Status',
              render: (val: any) => <StatusChip status={String(val)} />,
              sortable: true,
            },
            {
              key: 'difficulty',
              label: 'Difficulty',
              render: (val: any) => (
                <span className={`font-semibold ${
                  Number(val) >= 7 ? 'text-error' :
                  Number(val) >= 5 ? 'text-warning' : 'text-success'
                }`}>
                  {val}/10
                </span>
              ),
              sortable: true,
            },
            { key: 'warnings', label: 'Warnings', sortable: true },
          ]}
        />
      </div>
    </div>
  )
}
