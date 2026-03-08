interface MetricCardProps {
  label: string
  value: string | number
  delta?: string
  deltaColor?: 'green' | 'red' | 'default'
}

export default function MetricCard({ label, value, delta, deltaColor = 'default' }: MetricCardProps) {
  const deltaColors = {
    green: 'text-success',
    red: 'text-error',
    default: 'text-text-secondary',
  }

  return (
    <div className="group bg-gradient-to-br from-bg-card/80 to-bg-card-alt/80 backdrop-blur-xl border border-border rounded-2xl p-5 shadow-[0_4px_20px_rgba(0,212,255,0.1)] hover:-translate-y-1 hover:shadow-[0_8px_30px_rgba(0,212,255,0.2)] transition-all duration-300">
      <p className="text-text-muted text-xs font-semibold uppercase tracking-wider mb-2">
        {label}
      </p>
      <p className="text-text-primary text-3xl font-bold mb-1 group-hover:text-transparent group-hover:bg-gradient-to-r group-hover:from-accent group-hover:to-purple group-hover:bg-clip-text transition-all">{value}</p>
      {delta && (
        <p className={`text-xs font-medium ${deltaColors[deltaColor]}`}>{delta}</p>
      )}
    </div>
  )
}
