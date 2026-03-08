interface LoadingOverlayProps {
  message?: string
}

export default function LoadingOverlay({ message = 'Loading...' }: LoadingOverlayProps) {
  return (
    <div className="flex flex-col items-center justify-center py-20">
      <div className="w-10 h-10 border-4 border-border border-t-accent rounded-full animate-spin mb-4" />
      <p className="text-text-secondary text-sm">{message}</p>
    </div>
  )
}
