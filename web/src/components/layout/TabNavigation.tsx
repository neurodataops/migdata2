import { useState } from 'react'
import { useAppStore } from '../../store/appStore'

interface TabGroup {
  label: string
  icon: string
  tabs: { id: number; label: string }[]
}

const TAB_GROUPS: TabGroup[] = [
  {
    label: 'Overview',
    icon: 'M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z',
    tabs: [
      { id: 0, label: 'Executive Summary' },
      { id: 1, label: 'Overview' },
    ],
  },
  {
    label: 'Analysis',
    icon: 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2',
    tabs: [
      { id: 2, label: 'Objects' },
      { id: 3, label: 'Schema Explorer' },
      { id: 4, label: 'Relationships' },
      { id: 6, label: 'Lineage' },
    ],
  },
  {
    label: 'Details',
    icon: 'M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4',
    tabs: [
      { id: 5, label: 'Metadata' },
      { id: 7, label: 'SQL Comparison' },
    ],
  },
  {
    label: 'Validation',
    icon: 'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z',
    tabs: [
      { id: 8, label: 'Validation Results' },
      { id: 9, label: 'Manual Work' },
    ],
  },
  {
    label: 'AI Agents',
    icon: 'M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z',
    tabs: [
      { id: 10, label: 'Agent Observability' },
    ],
  },
]

export default function TabNavigation() {
  const { activeTab, setActiveTab } = useAppStore()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const getCurrentGroup = () => {
    for (const group of TAB_GROUPS) {
      if (group.tabs.some(t => t.id === activeTab)) {
        return group
      }
    }
    return TAB_GROUPS[0]
  }

  const currentGroup = getCurrentGroup()
  const currentTabLabel = TAB_GROUPS
    .flatMap(g => g.tabs)
    .find(t => t.id === activeTab)?.label || 'Executive Summary'

  return (
    <div className="flex gap-3 flex-wrap">
      {TAB_GROUPS.map((group) => {
        const isActive = group.tabs.some(t => t.id === activeTab)
        const isOpen = openDropdown === group.label

        return (
          <div key={group.label} className="relative">
            <button
              onClick={() => setOpenDropdown(isOpen ? null : group.label)}
              className={`flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-semibold transition-all ${
                isActive
                  ? 'bg-gradient-to-r from-accent to-purple text-white shadow-[0_4px_15px_rgba(0,212,255,0.4)]'
                  : 'bg-gradient-to-r from-bg-card/50 to-bg-card-alt/50 backdrop-blur-xl text-text-secondary border border-border hover:border-accent/50'
              }`}
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={group.icon} />
              </svg>
              <span>{isActive ? currentTabLabel : group.label}</span>
              <svg
                className={`w-4 h-4 transition-transform ${isOpen ? 'rotate-180' : ''}`}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {isOpen && (
              <>
                <div
                  className="fixed inset-0 z-10"
                  onClick={() => setOpenDropdown(null)}
                ></div>
                <div className="absolute top-full left-0 mt-2 w-64 bg-gradient-to-br from-bg-card to-bg-card-alt backdrop-blur-xl border border-border rounded-xl shadow-[0_8px_30px_rgba(0,212,255,0.2)] overflow-hidden z-20">
                  {group.tabs.map((tab) => (
                    <button
                      key={tab.id}
                      onClick={() => {
                        setActiveTab(tab.id)
                        setOpenDropdown(null)
                      }}
                      className={`w-full text-left px-4 py-3 text-sm transition-all ${
                        activeTab === tab.id
                          ? 'bg-accent/20 text-accent font-semibold'
                          : 'text-text-secondary hover:bg-bg-secondary/50 hover:text-text-primary'
                      }`}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>
              </>
            )}
          </div>
        )
      })}
    </div>
  )
}
