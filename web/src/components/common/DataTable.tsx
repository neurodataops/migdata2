import { useState, useMemo } from 'react'

interface Column<T> {
  key: keyof T & string
  label: string
  render?: (value: T[keyof T], row: T) => React.ReactNode
  sortable?: boolean
}

interface DataTableProps<T> {
  data: T[]
  columns: Column<T>[]
  className?: string
}

export default function DataTable<T extends Record<string, unknown>>({
  data,
  columns,
  className = '',
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null)
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')

  const sorted = useMemo(() => {
    if (!sortKey) return data
    return [...data].sort((a, b) => {
      const av = a[sortKey]
      const bv = b[sortKey]
      if (av == null) return 1
      if (bv == null) return -1
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [data, sortKey, sortDir])

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  return (
    <div className={`overflow-x-auto border border-border rounded-2xl shadow-[0_4px_20px_rgba(0,212,255,0.1)] ${className}`}>
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gradient-to-r from-accent via-purple to-accent-dark text-white">
            {columns.map((col) => (
              <th
                key={col.key}
                className={`px-4 py-3 text-left font-semibold ${
                  col.sortable !== false ? 'cursor-pointer hover:bg-white/10 select-none' : ''
                }`}
                onClick={() => col.sortable !== false && handleSort(col.key)}
              >
                {col.label}
                {sortKey === col.key && (
                  <span className="ml-1">{sortDir === 'asc' ? '\u25B2' : '\u25BC'}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr
              key={i}
              className={`${
                i % 2 === 0 ? 'bg-bg-card/50' : 'bg-bg-card-alt/50'
              } text-text-secondary hover:bg-accent/10 transition-colors`}
            >
              {columns.map((col) => (
                <td key={col.key} className="px-4 py-2.5 border-b border-border-light">
                  {col.render
                    ? col.render(row[col.key], row)
                    : String(row[col.key] ?? '')}
                </td>
              ))}
            </tr>
          ))}
          {sorted.length === 0 && (
            <tr>
              <td colSpan={columns.length} className="px-4 py-8 text-center text-text-muted">
                No data available
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
