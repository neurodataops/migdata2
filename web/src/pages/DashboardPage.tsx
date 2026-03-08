import { useQueryClient } from '@tanstack/react-query'
import Sidebar from '../components/layout/Sidebar'
import TabNavigation from '../components/layout/TabNavigation'
import PipelineProgressModal from '../components/common/PipelineProgressModal'
import { useAppStore } from '../store/appStore'
import ExecutiveSummaryTab from '../components/tabs/ExecutiveSummaryTab'
import OverviewTab from '../components/tabs/OverviewTab'
import ObjectsTab from '../components/tabs/ObjectsTab'
import SchemaExplorerTab from '../components/tabs/SchemaExplorerTab'
import RelationshipsTab from '../components/tabs/RelationshipsTab'
import MetadataTab from '../components/tabs/MetadataTab'
import LineageTab from '../components/tabs/LineageTab'
import SQLComparisonTab from '../components/tabs/SQLComparisonTab'
import ValidationTab from '../components/tabs/ValidationTab'
import ManualWorkTab from '../components/tabs/ManualWorkTab'
import AgentObservabilityTab from '../components/tabs/AgentObservabilityTab'

export default function DashboardPage() {
  const queryClient = useQueryClient()
  const activeTab = useAppStore((s) => s.activeTab)
  const useMock = useAppStore((s) => s.useMock)
  const pipeline = useAppStore((s) => s.pipeline)
  const pipelineSteps = useAppStore((s) => s.pipelineSteps)
  const progressPercent = useAppStore((s) => s.progressPercent)
  const progressMetrics = useAppStore((s) => s.progressMetrics)
  const setPipeline = useAppStore((s) => s.setPipeline)

  const handleModalClose = () => {
    setPipeline({ running: false })
    // Refresh all data when modal closes to populate tabs
    queryClient.invalidateQueries()
  }

  return (
    <div className="flex min-h-screen">
      <Sidebar />

      {/* Pipeline Progress Modal - Disabled (progress shown in sidebar instead) */}
      {/* <PipelineProgressModal
        isOpen={pipeline.running}
        steps={pipelineSteps}
        progressPercent={progressPercent}
        currentLabel={pipeline.label}
        metrics={progressMetrics}
        onClose={handleModalClose}
      /> */}

      <main className="flex-1 p-8">
        <div className="max-w-[1600px] mx-auto">
          {/* Dashboard Header */}
          <div className="mb-8 animate-fade-in-up">
            <h1 className="text-3xl font-bold text-text-primary mb-2">
              Migration <span className="bg-gradient-to-r from-accent to-purple bg-clip-text text-transparent">Intelligence</span> Dashboard
            </h1>
            <p className="text-text-secondary">
              Real-time insights and analytics for your data migration project
            </p>
          </div>

          {/* Data Source Notice */}
          {useMock ? (
            <div className="mb-6 bg-gradient-to-r from-warning/10 to-warning/5 border border-warning/30 rounded-xl p-4 animate-fade-in-up">
              <div className="flex items-start gap-3">
                <svg className="w-5 h-5 text-warning mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <div>
                  <p className="text-warning font-semibold mb-1">Demo Mode Active</p>
                  <p className="text-text-secondary text-sm mb-2">
                    You're viewing sample demo data for demonstration purposes.
                  </p>
                  <p className="text-text-secondary text-sm">
                    <strong>To view your Snowflake data:</strong><br />
                    1. Toggle "Data Source" to OFF in the left sidebar<br />
                    2. Click "Run Real Pipeline"<br />
                    3. Wait for pipeline to complete
                  </p>
                </div>
              </div>
            </div>
          ) : (
            <div className="mb-6 bg-gradient-to-r from-accent/10 to-purple/5 border border-accent/30 rounded-xl p-4 animate-fade-in-up">
              <div className="flex items-start gap-3">
                <svg className="w-5 h-5 text-accent mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
                <div>
                  <p className="text-accent font-semibold mb-1">Real Data Mode Active</p>
                  <p className="text-text-secondary text-sm mb-2">
                    Your database connection is configured. Click <strong>"Run Real Pipeline"</strong> in the sidebar to:
                  </p>
                  <ul className="text-text-secondary text-sm space-y-1 list-disc list-inside">
                    <li>Extract schemas and tables from your connected database</li>
                    <li>Convert SQL to Spark (Databricks compatible)</li>
                    <li>Load and validate your data</li>
                    <li>Generate migration reports and confidence scores</li>
                  </ul>
                  <p className="text-text-muted text-xs mt-3 italic">
                    Note: The pipeline will use the credentials you tested on the connection page.
                  </p>
                </div>
              </div>
            </div>
          )}

          <TabNavigation />

          <div className="mt-8">
            {activeTab === 0 && <ExecutiveSummaryTab />}
            {activeTab === 1 && <OverviewTab />}
            {activeTab === 2 && <ObjectsTab />}
            {activeTab === 3 && <SchemaExplorerTab />}
            {activeTab === 4 && <RelationshipsTab />}
            {activeTab === 5 && <MetadataTab />}
            {activeTab === 6 && <LineageTab />}
            {activeTab === 7 && <SQLComparisonTab />}
            {activeTab === 8 && <ValidationTab />}
            {activeTab === 9 && <ManualWorkTab />}
            {activeTab === 10 && <AgentObservabilityTab />}
          </div>
        </div>
      </main>
    </div>
  )
}
