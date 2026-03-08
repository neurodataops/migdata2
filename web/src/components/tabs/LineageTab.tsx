import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../../store/appStore'
import { getLineage } from '../../api/endpoints'
import MetricCard from '../common/MetricCard'
import LoadingOverlay from '../common/LoadingOverlay'

export default function LineageTab() {
  const { selectedSchemas } = useAppStore()
  const schemasParam = selectedSchemas.join(',')

  const { data, isLoading } = useQuery({
    queryKey: ['lineage', schemasParam],
    queryFn: () => getLineage(schemasParam),
  })

  const nodes = data?.nodes || []
  const edges = data?.edges || []
  const stats = data?.stats || { total_tables: 0, total_relationships: 0 }

  // Build dependency map - corrected for ERD flow
  // In ERD: parent (PK) -> child (FK)
  // Backend gives: edge.from = child (FK), edge.to = parent (PK)
  // So we swap them for proper flow
  const dependencyMap: Record<string, { parents: string[], children: string[] }> = {}
  edges.forEach((edge: any) => {
    const parent = edge.to   // Has the PK
    const child = edge.from  // Has the FK

    if (!dependencyMap[parent]) {
      dependencyMap[parent] = { parents: [], children: [] }
    }
    if (!dependencyMap[child]) {
      dependencyMap[child] = { parents: [], children: [] }
    }

    // Parent has this child
    dependencyMap[parent].children.push(child)
    // Child has this parent
    dependencyMap[child].parents.push(parent)
  })

  // Find root tables (no parents = leftmost) and leaf tables (no children = rightmost)
  const rootTables = nodes.filter(
    (n: any) => !dependencyMap[n.id]?.parents?.length && dependencyMap[n.id]?.children?.length
  )
  const leafTables = nodes.filter(
    (n: any) => dependencyMap[n.id]?.parents?.length && !dependencyMap[n.id]?.children?.length
  )

  // Render SVG-based lineage graph
  const renderLineageGraph = () => {
    if (nodes.length === 0) return null

    // Assign levels (left to right): root tables at level 0
    const levels: Record<number, any[]> = {}
    const nodeLevel: Record<string, number> = {}
    const visited = new Set<string>()

    const assignLevels = (nodeId: string, level: number) => {
      if (visited.has(nodeId)) return
      visited.add(nodeId)

      if (!levels[level]) levels[level] = []
      const node = nodes.find((n: any) => n.id === nodeId)
      if (node) {
        levels[level].push(node)
        nodeLevel[nodeId] = level
      }

      // Process children (tables that reference this table's PK)
      const children = dependencyMap[nodeId]?.children || []
      children.forEach(childId => {
        if (!visited.has(childId)) {
          assignLevels(childId, level + 1)
        }
      })
    }

    // Start from root tables
    if (rootTables.length > 0) {
      rootTables.forEach(node => assignLevels(node.id, 0))
    } else if (nodes.length > 0) {
      // If no clear roots, start from first table
      assignLevels(nodes[0].id, 0)
    }

    // Handle unvisited nodes
    nodes.forEach((node: any) => {
      if (!visited.has(node.id)) {
        const maxLevel = Math.max(...Object.keys(levels).map(Number), -1)
        assignLevels(node.id, maxLevel + 1)
      }
    })

    // Calculate positions (left to right layout)
    const levelGap = 300 // Horizontal gap between levels
    const nodeHeight = 120 // Height of each node box
    const nodeWidth = 200 // Width of each node box
    const verticalGap = 40 // Vertical gap between nodes

    const nodePositions: Record<string, { x: number, y: number }> = {}
    const levelKeys = Object.keys(levels).map(Number).sort((a, b) => a - b)

    levelKeys.forEach(level => {
      const nodesInLevel = levels[level]
      const totalHeight = nodesInLevel.length * (nodeHeight + verticalGap) - verticalGap
      const startY = -totalHeight / 2

      nodesInLevel.forEach((node: any, idx: number) => {
        nodePositions[node.id] = {
          x: level * levelGap + 100,
          y: startY + idx * (nodeHeight + verticalGap),
        }
      })
    })

    // Calculate SVG dimensions
    const maxX = Math.max(...Object.values(nodePositions).map(p => p.x)) + nodeWidth + 100
    const minY = Math.min(...Object.values(nodePositions).map(p => p.y)) - 50
    const maxY = Math.max(...Object.values(nodePositions).map(p => p.y)) + nodeHeight + 50
    const svgHeight = maxY - minY
    const svgWidth = maxX

    return (
      <div className="overflow-auto bg-bg-secondary rounded-xl p-4" style={{ maxHeight: '800px' }}>
        <svg
          width={svgWidth}
          height={svgHeight}
          viewBox={`0 ${minY} ${svgWidth} ${svgHeight}`}
          xmlns="http://www.w3.org/2000/svg"
        >
          <defs>
            <marker
              id="arrowhead"
              markerWidth="10"
              markerHeight="10"
              refX="9"
              refY="3"
              orient="auto"
            >
              <polygon points="0 0, 10 3, 0 6" fill="#00d4ff" />
            </marker>
          </defs>

          {/* Draw edges (connections) first so they appear behind boxes */}
          {edges.map((edge: any, idx: number) => {
            // IMPORTANT: In ERD, arrow goes FROM parent (PK) TO child (FK)
            // Backend returns: from=child table (has FK), to=parent table (has PK)
            // So we need to reverse: draw from parent TO child
            const parentPos = nodePositions[edge.to]     // Parent table (has PK)
            const childPos = nodePositions[edge.from]    // Child table (has FK)
            if (!parentPos || !childPos) return null

            // Draw line from right side of parent to left side of child
            const x1 = parentPos.x + nodeWidth
            const y1 = parentPos.y + nodeHeight / 2
            const x2 = childPos.x
            const y2 = childPos.y + nodeHeight / 2

            // Create bezier curve for nicer look
            const midX = (x1 + x2) / 2

            return (
              <g key={`edge-${idx}`}>
                <path
                  d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`}
                  stroke="#00d4ff"
                  strokeWidth="2"
                  fill="none"
                  markerEnd="url(#arrowhead)"
                  opacity="0.6"
                />
                {/* Label showing PK → FK relationship */}
                <text
                  x={(x1 + x2) / 2}
                  y={(y1 + y2) / 2 - 5}
                  fill="#64748b"
                  fontSize="11"
                  textAnchor="middle"
                >
                  {edge.to_column} → {edge.from_column}
                </text>
              </g>
            )
          })}

          {/* Draw table boxes */}
          {nodes.map((node: any) => {
            const pos = nodePositions[node.id]
            if (!pos) return null

            const pks = node.primary_keys || []
            const fks = node.foreign_keys || []

            return (
              <g key={node.id}>
                {/* Main rectangle */}
                <rect
                  x={pos.x}
                  y={pos.y}
                  width={nodeWidth}
                  height={nodeHeight}
                  fill="#1e293b"
                  stroke="#7c3aed"
                  strokeWidth="2"
                  rx="8"
                />

                {/* Header bar */}
                <rect
                  x={pos.x}
                  y={pos.y}
                  width={nodeWidth}
                  height={30}
                  fill="#7c3aed"
                  fillOpacity="0.3"
                  rx="8"
                />

                {/* Table name */}
                <text
                  x={pos.x + nodeWidth / 2}
                  y={pos.y + 20}
                  fill="#00d4ff"
                  fontSize="13"
                  fontWeight="bold"
                  textAnchor="middle"
                >
                  {node.table}
                </text>

                {/* Schema name (smaller) */}
                <text
                  x={pos.x + nodeWidth / 2}
                  y={pos.y + 45}
                  fill="#94a3b8"
                  fontSize="10"
                  textAnchor="middle"
                >
                  {node.schema}
                </text>

                {/* Primary Keys */}
                {pks.length > 0 && (
                  <g>
                    <text
                      x={pos.x + 10}
                      y={pos.y + 65}
                      fill="#34d399"
                      fontSize="11"
                      fontWeight="600"
                    >
                      PK:
                    </text>
                    {pks.map((pk: string, idx: number) => (
                      <text
                        key={`pk-${idx}`}
                        x={pos.x + 35}
                        y={pos.y + 65 + idx * 15}
                        fill="#34d399"
                        fontSize="10"
                      >
                        {pk}
                      </text>
                    ))}
                  </g>
                )}

                {/* Foreign Keys */}
                {fks.length > 0 && (
                  <g>
                    <text
                      x={pos.x + 10}
                      y={pos.y + 65 + pks.length * 15 + 15}
                      fill="#fbbf24"
                      fontSize="11"
                      fontWeight="600"
                    >
                      FK:
                    </text>
                    {fks.slice(0, 2).map((fk: any, idx: number) => (
                      <text
                        key={`fk-${idx}`}
                        x={pos.x + 35}
                        y={pos.y + 65 + pks.length * 15 + 15 + idx * 15}
                        fill="#fbbf24"
                        fontSize="10"
                      >
                        {fk.column}
                      </text>
                    ))}
                    {fks.length > 2 && (
                      <text
                        x={pos.x + 35}
                        y={pos.y + 65 + pks.length * 15 + 15 + 2 * 15}
                        fill="#fbbf24"
                        fontSize="9"
                        fontStyle="italic"
                      >
                        +{fks.length - 2} more
                      </text>
                    )}
                  </g>
                )}
              </g>
            )
          })}
        </svg>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {isLoading && <LoadingOverlay />}

      <div>
        <h2 className="text-2xl font-bold text-text-primary mb-2">Data Lineage</h2>
        <p className="text-text-secondary">
          Track data flow and dependencies across your migration
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <MetricCard
          label="Total Tables"
          value={stats.total_tables.toLocaleString()}
          delta="In lineage graph"
        />
        <MetricCard
          label="Relationships"
          value={stats.total_relationships.toLocaleString()}
          delta="Foreign key links"
        />
        <MetricCard
          label="Root Tables"
          value={rootTables.length.toLocaleString()}
          delta="Parent tables"
        />
        <MetricCard
          label="Leaf Tables"
          value={leafTables.length.toLocaleString()}
          delta="Child tables"
        />
      </div>

      {/* Enterprise-style Lineage Graph */}
      {nodes.length > 0 ? (
        <div className="bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-6 shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-xl font-semibold text-text-primary">
              Table Lineage & Relationships
            </h3>
            <div className="flex gap-4 text-sm">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-[#34d399]"></div>
                <span className="text-text-secondary">Primary Key</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 bg-[#fbbf24]"></div>
                <span className="text-text-secondary">Foreign Key</span>
              </div>
              <div className="flex items-center gap-2">
                <svg width="20" height="12" className="inline-block">
                  <line x1="0" y1="6" x2="20" y2="6" stroke="#00d4ff" strokeWidth="2" />
                  <polygon points="20,6 15,3 15,9" fill="#00d4ff" />
                </svg>
                <span className="text-text-secondary">Relationship</span>
              </div>
            </div>
          </div>
          <p className="text-sm text-text-muted mb-4">
            Left to right flow: Parent tables → Child tables. Lines show foreign key relationships.
          </p>
          {renderLineageGraph()}
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
                  d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"
                />
              </svg>
            </div>
            <h3 className="text-xl font-semibold text-text-primary mb-2">No Lineage Data</h3>
            <p className="text-text-secondary">
              Run the pipeline to generate data lineage information from your database relationships.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}
