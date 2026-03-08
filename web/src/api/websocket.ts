import { useEffect, useRef, useCallback } from 'react'

export interface PipelineMessage {
  step: number
  total_steps: number
  label: string
  status: 'running' | 'done' | 'error' | 'complete'
  elapsed_ms: number
  error?: string
  progress_percent?: number
  metrics?: Record<string, number>
}

export function usePipelineWebSocket(
  jobId: string | null,
  onMessage: (msg: PipelineMessage) => void,
  onComplete?: () => void,
) {
  const wsRef = useRef<WebSocket | null>(null)
  const onMessageRef = useRef(onMessage)
  const onCompleteRef = useRef(onComplete)

  onMessageRef.current = onMessage
  onCompleteRef.current = onComplete

  const connect = useCallback(() => {
    if (!jobId) return

    // Connect via Vite proxy (same host as frontend, proxied to backend)
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host  // Use frontend host (proxy will forward to backend)
    const ws = new WebSocket(`${protocol}//${host}/api/pipeline/ws/${jobId}`)

    ws.onmessage = (event) => {
      const msg: PipelineMessage = JSON.parse(event.data)
      onMessageRef.current(msg)
      if (msg.status === 'complete' || msg.status === 'error') {
        onCompleteRef.current?.()
      }
    }

    ws.onerror = () => ws.close()
    wsRef.current = ws
  }, [jobId])

  useEffect(() => {
    connect()
    return () => {
      wsRef.current?.close()
    }
  }, [connect])
}
