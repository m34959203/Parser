import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { Sparkles, Loader2, CheckCircle, AlertCircle, ArrowRight } from 'lucide-react'
import { aiApi, schemasApi } from '../../api/client'

export function AIGenerator() {
  const navigate = useNavigate()
  const [url, setUrl] = useState('')
  const [goal, setGoal] = useState('')
  const [exampleFields, setExampleFields] = useState('')
  const [result, setResult] = useState<any>(null)
  const [polling, setPolling] = useState(false)

  const generateMutation = useMutation({
    mutationFn: async () => {
      const response = await aiApi.generate({
        url,
        goal_description: goal,
        example_fields: exampleFields ? exampleFields.split(',').map(s => s.trim()) : undefined,
      })
      return response.data
    },
    onSuccess: async (data) => {
      // Poll for results
      setPolling(true)
      const pollResult = async () => {
        const result = await aiApi.getResult(data.task_id)
        if (result.data.status === 'completed') {
          setResult(result.data)
          setPolling(false)
        } else if (result.data.status === 'failed') {
          setResult({ error: result.data.error })
          setPolling(false)
        } else {
          setTimeout(pollResult, 2000)
        }
      }
      pollResult()
    },
  })

  const saveMutation = useMutation({
    mutationFn: (schema: any) => schemasApi.create(schema),
    onSuccess: (response) => {
      navigate(`/schemas/${response.data.schema_id}`)
    },
  })

  const handleGenerate = (e: React.FormEvent) => {
    e.preventDefault()
    setResult(null)
    generateMutation.mutate()
  }

  const handleSave = () => {
    if (result?.schema) {
      saveMutation.mutate(result.schema)
    }
  }

  return (
    <div className="max-w-4xl mx-auto">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
          <Sparkles className="h-7 w-7 text-primary" />
          AI Schema Generator
        </h1>
        <p className="text-gray-600 mt-2">
          Automatically generate parsing schemas using AI. Just provide a URL and describe what you want to extract.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Form */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <form onSubmit={handleGenerate} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Website URL
              </label>
              <input
                type="url"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://example.com/products"
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                What do you want to extract?
              </label>
              <textarea
                value={goal}
                onChange={(e) => setGoal(e.target.value)}
                placeholder="I want to extract product names, prices, and images from the catalog page..."
                rows={4}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Example field names (optional)
              </label>
              <input
                type="text"
                value={exampleFields}
                onChange={(e) => setExampleFields(e.target.value)}
                placeholder="title, price, image_url, description"
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
              />
              <p className="text-xs text-gray-500 mt-1">Comma-separated list</p>
            </div>

            <button
              type="submit"
              disabled={generateMutation.isPending || polling}
              className="w-full flex items-center justify-center gap-2 px-4 py-3 bg-primary text-white rounded-lg hover:bg-primary/90 disabled:opacity-50"
            >
              {generateMutation.isPending || polling ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  {polling ? 'Analyzing...' : 'Starting...'}
                </>
              ) : (
                <>
                  <Sparkles className="h-4 w-4" />
                  Generate Schema
                </>
              )}
            </button>
          </form>
        </div>

        {/* Results */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h2 className="text-lg font-semibold mb-4">Generated Schema</h2>

          {!result && !generateMutation.isPending && !polling && (
            <div className="text-center py-12 text-gray-500">
              <Sparkles className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>Enter a URL and description to generate a schema</p>
            </div>
          )}

          {(generateMutation.isPending || polling) && (
            <div className="text-center py-12">
              <Loader2 className="h-12 w-12 mx-auto mb-4 text-primary animate-spin" />
              <p className="text-gray-600">
                {polling ? 'Analyzing page structure...' : 'Starting generation...'}
              </p>
              <p className="text-sm text-gray-500 mt-2">This may take 15-30 seconds</p>
            </div>
          )}

          {result?.error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <div className="flex items-start gap-3">
                <AlertCircle className="h-5 w-5 text-red-600 mt-0.5" />
                <div>
                  <p className="font-medium text-red-800">Generation Failed</p>
                  <p className="text-sm text-red-700">{result.error}</p>
                </div>
              </div>
            </div>
          )}

          {result?.schema && (
            <div className="space-y-4">
              {/* Confidence */}
              <div className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                <span className="text-sm font-medium text-gray-700">Confidence</span>
                <div className="flex items-center gap-2">
                  <div className="w-24 h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary rounded-full"
                      style={{ width: `${result.confidence * 100}%` }}
                    />
                  </div>
                  <span className="text-sm font-medium">{Math.round(result.confidence * 100)}%</span>
                </div>
              </div>

              {/* Warnings */}
              {result.warnings?.length > 0 && (
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-3">
                  <p className="text-sm font-medium text-yellow-800 mb-1">Warnings</p>
                  <ul className="text-sm text-yellow-700 list-disc list-inside">
                    {result.warnings.map((w: string, i: number) => (
                      <li key={i}>{w}</li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Fields */}
              <div>
                <p className="text-sm font-medium text-gray-700 mb-2">
                  Detected Fields ({result.schema.fields?.length || 0})
                </p>
                <div className="space-y-2">
                  {result.schema.fields?.map((field: any, i: number) => (
                    <div key={i} className="flex items-center justify-between p-2 bg-gray-50 rounded-lg">
                      <div className="flex items-center gap-2">
                        <CheckCircle className="h-4 w-4 text-green-600" />
                        <span className="font-mono text-sm">{field.name}</span>
                      </div>
                      <span className="text-xs text-gray-500">{field.type}</span>
                    </div>
                  ))}
                </div>
              </div>

              {/* Save Button */}
              <button
                onClick={handleSave}
                disabled={saveMutation.isPending}
                className="w-full flex items-center justify-center gap-2 px-4 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700"
              >
                {saveMutation.isPending ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <>
                    Accept & Save Schema
                    <ArrowRight className="h-4 w-4" />
                  </>
                )}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
