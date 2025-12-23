import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Save, Play, Plus, Trash2, GripVertical } from 'lucide-react'
import { schemasApi, tasksApi } from '../../api/client'

interface FieldDefinition {
  name: string
  type: string
  method: string
  selector: string
  attribute?: string
  required: boolean
  transformations: string[]
}

export function SchemaEditor() {
  const { schemaId } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const isNew = !schemaId

  const [formData, setFormData] = useState({
    source_id: '',
    description: '',
    start_url: '',
    item_container: '',
    mode: 'http',
    fields: [] as FieldDefinition[],
  })

  const { data: schema, isLoading } = useQuery({
    queryKey: ['schema', schemaId],
    queryFn: () => schemasApi.get(schemaId!).then(r => r.data),
    enabled: !!schemaId,
    onSuccess: (data) => {
      setFormData({
        source_id: data.source_id,
        description: data.description || '',
        start_url: data.start_url,
        item_container: data.item_container || '',
        mode: data.mode,
        fields: data.fields || [],
      })
    },
  })

  const saveMutation = useMutation({
    mutationFn: (data: any) =>
      isNew ? schemasApi.create(data) : schemasApi.update(schemaId!, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schemas'] })
      navigate('/schemas')
    },
  })

  const testMutation = useMutation({
    mutationFn: async () => {
      // Create a test task
      const task = await tasksApi.create({
        source_id: formData.source_id,
        target_url: formData.start_url,
        schema_id: schemaId,
        mode: formData.mode,
        max_attempts: 1,
      })
      return task.data
    },
  })

  const addField = () => {
    setFormData(prev => ({
      ...prev,
      fields: [...prev.fields, {
        name: '',
        type: 'string',
        method: 'css',
        selector: '',
        required: true,
        transformations: [],
      }],
    }))
  }

  const updateField = (index: number, updates: Partial<FieldDefinition>) => {
    setFormData(prev => ({
      ...prev,
      fields: prev.fields.map((f, i) => i === index ? { ...f, ...updates } : f),
    }))
  }

  const removeField = (index: number) => {
    setFormData(prev => ({
      ...prev,
      fields: prev.fields.filter((_, i) => i !== index),
    }))
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    saveMutation.mutate(formData)
  }

  if (isLoading && schemaId) {
    return <div className="animate-pulse">Loading...</div>
  }

  return (
    <div className="max-w-4xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">
            {isNew ? 'Create Schema' : `Edit: ${schema?.schema_id}`}
          </h1>
          {!isNew && (
            <p className="text-gray-600">Version {schema?.version}</p>
          )}
        </div>
        <div className="flex gap-3">
          {!isNew && (
            <button
              onClick={() => testMutation.mutate()}
              disabled={testMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50"
            >
              <Play className="h-4 w-4" />
              Test Run
            </button>
          )}
          <button
            onClick={handleSubmit}
            disabled={saveMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90"
          >
            <Save className="h-4 w-4" />
            {saveMutation.isPending ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="space-y-6">
        {/* Basic Info */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h2 className="text-lg font-semibold mb-4">Basic Information</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Source ID
              </label>
              <input
                type="text"
                value={formData.source_id}
                onChange={(e) => setFormData(prev => ({ ...prev, source_id: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                placeholder="example.com/products"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Mode
              </label>
              <select
                value={formData.mode}
                onChange={(e) => setFormData(prev => ({ ...prev, mode: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
              >
                <option value="http">HTTP (Fast)</option>
                <option value="browser">Browser (JavaScript)</option>
              </select>
            </div>
            <div className="md:col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Start URL
              </label>
              <input
                type="url"
                value={formData.start_url}
                onChange={(e) => setFormData(prev => ({ ...prev, start_url: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                placeholder="https://example.com/catalog"
                required
              />
            </div>
            <div className="md:col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Description
              </label>
              <textarea
                value={formData.description}
                onChange={(e) => setFormData(prev => ({ ...prev, description: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                rows={2}
                placeholder="What does this schema extract?"
              />
            </div>
            <div className="md:col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Item Container (for lists)
              </label>
              <input
                type="text"
                value={formData.item_container}
                onChange={(e) => setFormData(prev => ({ ...prev, item_container: e.target.value }))}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 font-mono text-sm"
                placeholder="div.product-card"
              />
            </div>
          </div>
        </div>

        {/* Fields */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold">Fields</h2>
            <button
              type="button"
              onClick={addField}
              className="flex items-center gap-2 px-3 py-1.5 text-sm text-primary border border-primary rounded-lg hover:bg-primary/5"
            >
              <Plus className="h-4 w-4" />
              Add Field
            </button>
          </div>

          <div className="space-y-4">
            {formData.fields.map((field, index) => (
              <div key={index} className="flex gap-4 p-4 bg-gray-50 rounded-lg">
                <GripVertical className="h-5 w-5 text-gray-400 mt-2 cursor-grab" />
                <div className="flex-1 grid grid-cols-1 md:grid-cols-4 gap-3">
                  <input
                    type="text"
                    value={field.name}
                    onChange={(e) => updateField(index, { name: e.target.value })}
                    placeholder="Field name"
                    className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                  />
                  <select
                    value={field.type}
                    onChange={(e) => updateField(index, { type: e.target.value })}
                    className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                  >
                    <option value="string">String</option>
                    <option value="integer">Integer</option>
                    <option value="float">Float</option>
                    <option value="boolean">Boolean</option>
                    <option value="url">URL</option>
                    <option value="datetime">DateTime</option>
                  </select>
                  <select
                    value={field.method}
                    onChange={(e) => updateField(index, { method: e.target.value })}
                    className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
                  >
                    <option value="css">CSS</option>
                    <option value="xpath">XPath</option>
                    <option value="regex">Regex</option>
                  </select>
                  <input
                    type="text"
                    value={field.selector}
                    onChange={(e) => updateField(index, { selector: e.target.value })}
                    placeholder="Selector"
                    className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50 font-mono text-sm"
                  />
                </div>
                <button
                  type="button"
                  onClick={() => removeField(index)}
                  className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </div>
            ))}

            {formData.fields.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                No fields defined. Click "Add Field" to start.
              </div>
            )}
          </div>
        </div>
      </form>
    </div>
  )
}
