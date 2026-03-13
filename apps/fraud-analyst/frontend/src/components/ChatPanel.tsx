import { useState, useRef, useEffect } from 'react'
import { MessageCircle, Send, Loader2, X, Minimize2, Maximize2, Wrench } from 'lucide-react'

interface ToolCall {
  tool: string
  args: Record<string, unknown>
}

interface Message {
  role: 'user' | 'assistant'
  content: string
  toolCalls?: ToolCall[]
}

export default function ChatPanel({ transactionId }: { transactionId?: string }) {
  const [open, setOpen] = useState(false)
  const [minimized, setMinimized] = useState(false)
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages])

  async function sendMessage(e: React.FormEvent) {
    e.preventDefault()
    if (!input.trim() || loading) return

    const userMsg: Message = { role: 'user', content: input.trim() }
    const newMessages = [...messages, userMsg]
    setMessages(newMessages)
    setInput('')
    setLoading(true)

    const payload = JSON.stringify({
      messages: newMessages.map(({ role, content }) => ({ role, content })),
      transaction_id: transactionId,
    })

    try {
      // Try streaming endpoint first
      const response = await fetch('/api/chat/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload,
      })

      if (!response.ok || !response.body) {
        throw new Error('Stream failed')
      }

      // Add placeholder assistant message
      const assistantMsg: Message = { role: 'assistant', content: '' }
      setMessages(prev => [...prev, assistantMsg])

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let fullText = ''
      let toolCalls: ToolCall[] | undefined
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        // Keep the last (possibly incomplete) line in the buffer
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          try {
            const event = JSON.parse(line.slice(6))
            if (event.type === 'tools') {
              toolCalls = event.tool_calls
            } else if (event.type === 'text') {
              fullText += event.content
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = { role: 'assistant', content: fullText, toolCalls }
                return updated
              })
            } else if (event.type === 'error') {
              fullText = event.content
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = { role: 'assistant', content: fullText }
                return updated
              })
            }
          } catch {
            // ignore malformed SSE lines
          }
        }
      }
    } catch {
      // Fallback to non-streaming endpoint
      try {
        const res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: payload,
        })
        const data = await res.json()
        setMessages(prev => {
          // Remove placeholder if it was added during a failed stream attempt
          const base = prev[prev.length - 1]?.role === 'assistant' && prev[prev.length - 1]?.content === ''
            ? prev.slice(0, -1)
            : prev
          return [
            ...base,
            {
              role: 'assistant',
              content: data.reply,
              toolCalls: data.tool_calls && data.tool_calls.length > 0 ? data.tool_calls : undefined,
            },
          ]
        })
      } catch {
        setMessages(prev => {
          const base = prev[prev.length - 1]?.role === 'assistant' && prev[prev.length - 1]?.content === ''
            ? prev.slice(0, -1)
            : prev
          return [...base, { role: 'assistant', content: 'Connection error. Please try again.' }]
        })
      }
    } finally {
      setLoading(false)
    }
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="fixed bottom-5 right-5 bg-blue-600 hover:bg-blue-500 text-white rounded-full p-3 shadow-lg transition z-50"
      >
        <MessageCircle className="w-5 h-5" />
      </button>
    )
  }

  return (
    <div
      className={`fixed bottom-5 right-5 bg-[#161922] border border-gray-700 rounded-lg shadow-2xl z-50 flex flex-col transition-all ${
        minimized ? 'w-72 h-10' : 'w-96 h-[500px]'
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-gray-800 shrink-0">
        <span className="text-xs font-semibold text-gray-300 flex items-center gap-1.5">
          <MessageCircle className="w-3.5 h-3.5 text-blue-400" />
          AI Fraud Analyst
          {transactionId && (
            <span className="text-gray-500 font-mono text-[10px]">
              ({transactionId})
            </span>
          )}
        </span>
        <div className="flex gap-1">
          <button
            onClick={() => setMinimized(!minimized)}
            className="p-1 text-gray-500 hover:text-white"
          >
            {minimized ? (
              <Maximize2 className="w-3 h-3" />
            ) : (
              <Minimize2 className="w-3 h-3" />
            )}
          </button>
          <button
            onClick={() => {
              setOpen(false)
              setMinimized(false)
            }}
            className="p-1 text-gray-500 hover:text-white"
          >
            <X className="w-3 h-3" />
          </button>
        </div>
      </div>

      {!minimized && (
        <>
          {/* Messages */}
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-3 py-2 space-y-3">
            {messages.length === 0 && (
              <div className="text-center text-gray-500 text-xs py-8">
                <p>Ask about this case, fraud patterns, or investigation steps.</p>
                {transactionId && (
                  <p className="mt-1 text-[10px]">
                    Case context is loaded automatically.
                  </p>
                )}
              </div>
            )}
            {messages.map((m, i) => (
              <div
                key={i}
                className={`text-xs leading-relaxed ${
                  m.role === 'user'
                    ? 'bg-blue-600/20 text-blue-200 rounded-lg px-3 py-2 ml-8'
                    : 'text-gray-300 pr-8'
                }`}
              >
                <div className="whitespace-pre-wrap">{m.content}</div>
                {m.toolCalls && m.toolCalls.length > 0 && (
                  <div className="flex flex-wrap gap-1 mt-1.5">
                    {m.toolCalls.map((tc: ToolCall, i: number) => (
                      <span key={i} className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-400 text-[10px]">
                        <Wrench className="w-2.5 h-2.5" />
                        {tc.tool}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
            {loading && (
              <div className="flex items-center gap-1.5 text-xs text-gray-500">
                <Loader2 className="w-3 h-3 animate-spin" />
                Thinking...
              </div>
            )}
          </div>

          {/* Input */}
          <form
            onSubmit={sendMessage}
            className="border-t border-gray-800 px-3 py-2 shrink-0"
          >
            <div className="flex gap-2">
              <input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Ask about this case..."
                className="flex-1 bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:border-blue-500"
                disabled={loading}
              />
              <button
                type="submit"
                disabled={!input.trim() || loading}
                className="bg-blue-600 hover:bg-blue-500 disabled:bg-gray-700 text-white rounded p-1.5 transition"
              >
                <Send className="w-3.5 h-3.5" />
              </button>
            </div>
          </form>
        </>
      )}
    </div>
  )
}
