import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { Plus, Search, FileCode, MoreVertical, Sparkles } from 'lucide-react'
import { schemasApi } from '../../api/client'
import { useState } from 'react'

export function SchemaList() {
  const [search, setSearch] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['schemas'],
    queryFn: () => schemasApi.list({ limit: 50 }).then(r => r.data),
  })

  const schemas = data?.items || []
  const filteredSchemas = schemas.filter((s: any) =>
    s.schema_id.toLowerCase().includes(search.toLowerCase()) ||
    s.source_id.toLowerCase().includes(search.toLowerCase()) ||
    s.description?.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Parsing Schemas</h1>
          <p className="text-gray-600">Manage your data extraction configurations</p>
        </div>
        <div className="flex gap-3">
          <Link
            to="/schemas/generate"
            className="flex items-center gap-2 px-4 py-2 text-primary bg-primary/10 rounded-lg hover:bg-primary/20 transition-colors"
          >
            <Sparkles className="h-4 w-4" />
            AI Generate
          </Link>
          <Link
            to="/schemas/new"
            className="flex items-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4" />
            New Schema
          </Link>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
        <input
          type="text"
          placeholder="Search schemas..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
        />
      </div>

      {/* Schema Grid */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="bg-white rounded-lg border border-gray-200 p-6 h-48 animate-pulse" />
          ))}
        </div>
      ) : filteredSchemas.length === 0 ? (
        <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
          <FileCode className="h-12 w-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">No schemas found</h3>
          <p className="text-gray-600 mb-4">
            {search ? 'Try a different search term' : 'Get started by creating your first schema'}
          </p>
          {!search && (
            <Link
              to="/schemas/new"
              className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90"
            >
              <Plus className="h-4 w-4" />
              Create Schema
            </Link>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filteredSchemas.map((schema: any) => (
            <Link
              key={schema.schema_id}
              to={`/schemas/${schema.schema_id}`}
              className="bg-white rounded-lg border border-gray-200 p-6 hover:border-primary/50 hover:shadow-md transition-all"
            >
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <FileCode className="h-5 w-5 text-blue-600" />
                  </div>
                  <div>
                    <h3 className="font-medium text-gray-900">{schema.schema_id}</h3>
                    <p className="text-sm text-gray-500">v{schema.version}</p>
                  </div>
                </div>
                <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                  schema.is_active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'
                }`}>
                  {schema.is_active ? 'Active' : 'Inactive'}
                </span>
              </div>

              <p className="text-sm text-gray-600 mb-4 line-clamp-2">
                {schema.description || 'No description'}
              </p>

              <div className="flex items-center justify-between text-sm">
                <span className="text-gray-500">{schema.fields?.length || 0} fields</span>
                <span className={`px-2 py-0.5 rounded ${
                  schema.mode === 'browser' ? 'bg-purple-100 text-purple-700' : 'bg-blue-100 text-blue-700'
                }`}>
                  {schema.mode}
                </span>
              </div>

              {schema.confidence && (
                <div className="mt-3 flex items-center gap-2">
                  <div className="flex-1 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary rounded-full"
                      style={{ width: `${schema.confidence * 100}%` }}
                    />
                  </div>
                  <span className="text-xs text-gray-500">{Math.round(schema.confidence * 100)}%</span>
                </div>
              )}
            </Link>
          ))}
        </div>
      )}
    </div>
  )
}
