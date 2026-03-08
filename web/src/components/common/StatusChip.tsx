const STATUS_STYLES: Record<string, string> = {
  AUTO_CONVERT: 'bg-success/15 text-success border-success/40 shadow-[0_2px_10px_rgba(16,185,129,0.2)]',
  CONVERT_WITH_WARNINGS: 'bg-warning/15 text-warning border-warning/40 shadow-[0_2px_10px_rgba(245,158,11,0.2)]',
  MANUAL_REWRITE_REQUIRED: 'bg-error/15 text-error border-error/40 shadow-[0_2px_10px_rgba(239,68,68,0.2)]',
  success: 'bg-success/15 text-success border-success/40 shadow-[0_2px_10px_rgba(16,185,129,0.2)]',
  error: 'bg-error/15 text-error border-error/40 shadow-[0_2px_10px_rgba(239,68,68,0.2)]',
  validated: 'bg-success/15 text-success border-success/40 shadow-[0_2px_10px_rgba(16,185,129,0.2)]',
  skipped: 'bg-warning/15 text-warning border-warning/40 shadow-[0_2px_10px_rgba(245,158,11,0.2)]',
  High: 'bg-gradient-to-r from-error to-error/80 text-white border-error/50 shadow-[0_2px_10px_rgba(239,68,68,0.3)]',
  Medium: 'bg-gradient-to-r from-warning to-warning/80 text-white border-warning/50 shadow-[0_2px_10px_rgba(245,158,11,0.3)]',
  Low: 'bg-gradient-to-r from-success to-success/80 text-white border-success/50 shadow-[0_2px_10px_rgba(16,185,129,0.3)]',
}

interface StatusChipProps {
  status: string
  label?: string
  className?: string
}

export default function StatusChip({ status, label, className = '' }: StatusChipProps) {
  const style = STATUS_STYLES[status] || 'bg-bg-card text-text-secondary border-border'

  return (
    <span
      className={`inline-block px-3 py-1.5 rounded-full text-xs font-semibold border ${style} ${className}`}
    >
      {label || status}
    </span>
  )
}
