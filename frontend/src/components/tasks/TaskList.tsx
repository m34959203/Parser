import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { Plus, RefreshCw, Filter, RotateCcw } from 'lucide-react'
import { tasksApi } from '../../api/client'
import { useState } from 'react'
import { formatDistanceToNow } from 'date-fns'

const statusColors: Record<string, string> = {
  pending: 'bg-gray-100 text-gray-800',
  queued: 'bg-blue-100 text-blue-800',
  running: 'bg-yellow-100 text-yellow-800',
  success: 'bg-green-100 text-green-800',
  partial: 'bg-orange-100 text-orange-800',
  failed: 'bg-red-100 text-red-800',
  dlq: 'bg-red-100 text-red-800',
  cancelled: 'bg-gray-100 text-gray-800',
}

export function TaskList() {
  const queryClient = useQueryClient()
  const [statusFilter, setStatusFilter] = useState<string>('')

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['tasks', statusFilter],
    queryFn: () => tasksApi.list({ status: statusFilter || undefined, limit: 50 }).then(r => r.data),
    refetchInterval: 10000,
  })

  const retryMutation = useMutation({
    mutationFn: (taskId: string) => tasksApi.retry(taskId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tasks'] })
    },
  })

  const tasks = data?.items || []

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Tasks</h1>
          <p className="text-gray-600">Monitor and manage parsing tasks</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => refetch()}
            className="flex items-center gap-2 px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <Filter className="h-4 w-4 text-gray-400" />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="px-3 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary/50"
          >
            <option value="">All Statuses</option>
            <option value="pending">Pending</option>
            <option value="queued">Queued</option>
            <option value="running">Running</option>
            <option value="success">Success</option>
            <option value="failed">Failed</option>
            <option value="dlq">DLQ</option>
          </select>
        </div>
        <span className="text-sm text-gray-500">
          {data?.total || 0} tasks total
        </span>
      </div>

      {/* Task Table */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Source
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Records
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Created
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {isLoading ? (
              [...Array(5)].map((_, i) => (
                <tr key={i}>
                  <td colSpan={5} className="px-6 py-4">
                    <div className="h-4 bg-gray-200 rounded animate-pulse" />
                  </td>
                </tr>
              ))
            ) : tasks.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-6 py-12 text-center text-gray-500">
                  No tasks found
                </td>
              </tr>
            ) : (
              tasks.map((task: any) => (
                <tr key={task.task_id} className="hover:bg-gray-50">
                  <td className="px-6 py-4">
                    <Link
                      to={`/tasks/${task.task_id}`}
                      className="font-medium text-gray-900 hover:text-primary"
                    >
                      {task.source_id}
                    </Link>
                    <p className="text-sm text-gray-500 truncate max-w-xs">
                      {task.target_url}
                    </p>
                  </td>
                  <td className="px-6 py-4">
                    <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${statusColors[task.status] || 'bg-gray-100'}`}>
                      {task.status}
                    </span>
                    {task.attempt > 1 && (
                      <span className="ml-2 text-xs text-gray-500">
                        Attempt {task.attempt}/{task.max_attempts}
                      </span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-900">
                    {task.records_extracted}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {formatDistanceToNow(new Date(task.created_at), { addSuffix: true })}
                  </td>
                  <td className="px-6 py-4">
                    {(task.status === 'failed' || task.status === 'dlq') && (
                      <button
                        onClick={() => retryMutation.mutate(task.task_id)}
                        disabled={retryMutation.isPending}
                        className="flex items-center gap-1 text-sm text-primary hover:underline"
                      >
                        <RotateCcw className="h-3 w-3" />
                        Retry
                      </button>
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
