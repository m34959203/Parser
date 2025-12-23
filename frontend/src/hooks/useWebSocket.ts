import { useEffect, useRef, useCallback, useState } from 'react'

interface UseWebSocketOptions {
  url: string
  onMessage?: (data: unknown) => void
  onOpen?: () => void
  onClose?: () => void
  onError?: (error: Event) => void
  reconnect?: boolean
  reconnectInterval?: number
  maxReconnectAttempts?: number
}

interface UseWebSocketReturn {
  sendMessage: (data: unknown) => void
  isConnected: boolean
  reconnect: () => void
}

export function useWebSocket({
  url,
  onMessage,
  onOpen,
  onClose,
  onError,
  reconnect = true,
  reconnectInterval = 3000,
  maxReconnectAttempts = 5,
}: UseWebSocketOptions): UseWebSocketReturn {
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectAttempts = useRef(0)
  const reconnectTimeout = useRef<NodeJS.Timeout>()
  const [isConnected, setIsConnected] = useState(false)

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return
    }

    try {
      const ws = new WebSocket(url)

      ws.onopen = () => {
        setIsConnected(true)
        reconnectAttempts.current = 0
        onOpen?.()
      }

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          onMessage?.(data)
        } catch {
          onMessage?.(event.data)
        }
      }

      ws.onclose = () => {
        setIsConnected(false)
        onClose?.()

        if (reconnect && reconnectAttempts.current < maxReconnectAttempts) {
          reconnectTimeout.current = setTimeout(() => {
            reconnectAttempts.current++
            connect()
          }, reconnectInterval)
        }
      }

      ws.onerror = (error) => {
        onError?.(error)
      }

      wsRef.current = ws
    } catch (error) {
      console.error('WebSocket connection error:', error)
    }
  }, [url, onMessage, onOpen, onClose, onError, reconnect, reconnectInterval, maxReconnectAttempts])

  const sendMessage = useCallback((data: unknown) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(data))
    } else {
      console.warn('WebSocket is not connected')
    }
  }, [])

  const manualReconnect = useCallback(() => {
    reconnectAttempts.current = 0
    connect()
  }, [connect])

  useEffect(() => {
    connect()

    return () => {
      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current)
      }
      wsRef.current?.close()
    }
  }, [connect])

  return {
    sendMessage,
    isConnected,
    reconnect: manualReconnect,
  }
}
