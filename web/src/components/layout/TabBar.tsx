import { TAB_LABELS } from '../../types'
import { useAppStore } from '../../store/appStore'

const ACTIVE_TABS = new Set([0, 1, 2]) // Phase 1 tabs

export default function TabBar() {
  const activeTab = useAppStore((s) => s.activeTab)
  const setActiveTab = useAppStore((s) => s.setActiveTab)

  return (
    <div className="flex gap-1 bg-gradient-to-r from-bg-card/50 to-bg-card-alt/50 backdrop-blur-xl rounded-2xl p-1.5 border border-border overflow-x-auto shadow-[0_4px_20px_rgba(0,212,255,0.1)]">
      {TAB_LABELS.map((label, i) => {
        const isActive = activeTab === i
        const isEnabled = ACTIVE_TABS.has(i)

        return (
          <button
            key={i}
            onClick={() => isEnabled && setActiveTab(i)}
            className={`
              whitespace-nowrap px-5 py-2.5 rounded-xl text-sm font-semibold transition-all
              ${isActive
                ? 'bg-gradient-to-r from-accent to-purple text-white shadow-[0_4px_15px_rgba(0,212,255,0.4)]'
                : isEnabled
                ? 'text-text-secondary hover:text-text-primary hover:bg-bg-secondary/50'
                : 'text-text-muted cursor-not-allowed opacity-40'
              }
            `}
            title={!isEnabled ? 'Coming in Phase 2' : undefined}
          >
            {label}
          </button>
        )
      })}
    </div>
  )
}
