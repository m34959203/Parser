import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link } from 'react-router-dom'
import {
  ArrowLeft,
  RotateCcw,
  XCircle,
  Clock,
  CheckCircle,
  AlertTriangle,
  ExternalLink
} from 'lucide-react'
import { tasksApi } from '../../api/client'
import { formatDistanceToNow, format } from 'date-fns'

export function TaskDetail() {
  const { taskId } = useParams()
  const queryClient = useQueryClient()

  const { data: task, isLoading } = useQuery({
    queryKey: ['task', taskId],
    queryFn: () => tasksApi.get(taskId!).then(r => r.data),
    enabled: !!taskId,
    refetchInterval: (data) =>
      data?.status === 'running' || data?.status === 'queued' ? 5000 : false,
  })

  const retryMutation = useMutation({
    mutationFn: () => tasksApi.retry(taskId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['task', taskId] })
    },
  })

  const cancelMutation = useMutation({
    mutationFn: () => tasksApi.cancel(taskId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['task', taskId] })
    },
  })

  if (isLoading) {
    return <div className="animate-pulse">Loading...</div>
  }

  if (!task) {
    return <div>Task not found</div>
  }

  const statusIcon = {
    success: <CheckCircle className="h-6 w-6 text-green-600" />,
    partial: <AlertTriangle className="h-6 w-6 text-orange-600" />,
    failed: <XCircle className="h-6 w-6 text-red-600" />,
    running: <Clock className="h-6 w-6 text-blue-600 animate-spin" />,
    queued: <Clock className="h-6 w-6 text-gray-600" />,
    pending: <Clock className="h-6 w-6 text-gray-400" />,
    dlq: <AlertTriangle className="h-6 w-6 text-red-600" />,
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link
          to="/tasks"
          className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
        >
          <ArrowLeft className="h-5 w-5" />
        </Link>
        <div className="flex-1">
          <h1 className="text-2xl font-bold text-gray-900">Task Details</h1>
          <p className="text-gray-600 font-mono text-sm">{task.task_id}</p>
        </div>
        <div className="flex gap-3">
          {(task.status === 'failed' || task.status === 'dlq') && (
            <button
              onClick={() => retryMutation.mutate()}
              disabled={retryMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary/90"
            >
              <RotateCcw className="h-4 w-4" />
              Retry
            </button>
          )}
          {(task.status === 'pending' || task.status === 'queued') && (
            <button
              onClick={() => cancelMutation.mutate()}
              disabled={cancelMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 border border-red-200 text-red-600 rounded-lg hover:bg-red-50"
            >
              <XCircle className="h-4 w-4" />
              Cancel
            </button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Main Info */}
        <div className="lg:col-span-2 space-y-6">
          {/* Status Card */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <div className="flex items-center gap-4 mb-6">
              {statusIcon[task.status as keyof typeof statusIcon]}
              <div>
                <p className="text-lg font-semibold capitalize">{task.status}</p>
                <p className="text-sm text-gray-500">
                  Attempt {task.attempt} of {task.max_attempts}
                </p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-sm text-gray-500">Source</p>
                <p className="font-medium">{task.source_id}</p>
              </div>
              <div>
                <p className="text-sm text-gray-500">Schema</p>
                <Link
                  to={`/schemas/${task.schema_id}`}
                  className="font-medium text-primary hover:underline"
                >
                  {task.schema_id}
                </Link>
              </div>
              <div className="col-span-2">
                <p className="text-sm text-gray-500">Target URL</p>
                <a
                  href={task.target_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="font-medium text-primary hover:underline flex items-center gap-1"
                >
                  {task.target_url}
                  <ExternalLink className="h-3 w-3" />
                </a>
              </div>
            </div>
          </div>

          {/* Results */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-semibold mb-4">Results</h2>
            <div className="grid grid-cols-3 gap-4">
              <div className="p-4 bg-gray-50 rounded-lg text-center">
                <p className="text-2xl font-bold text-gray-900">{task.records_extracted}</p>
                <p className="text-sm text-gray-500">Records Extracted</p>
              </div>
              <div className="p-4 bg-green-50 rounded-lg text-center">
                <p className="text-2xl font-bold text-green-700">
                  {task.records_extracted > 0 ? Math.round((task.records_extracted / task.records_extracted) * 100) : 0}%
                </p>
                <p className="text-sm text-gray-500">Success Rate</p>
              </div>
              <div className="p-4 bg-blue-50 rounded-lg text-center">
                <p className="text-2xl font-bold text-blue-700">{task.mode}</p>
                <p className="text-sm text-gray-500">Mode</p>
              </div>
            </div>
          </div>

          {/* Errors */}
          {task.errors && task.errors.length > 0 && (
            <div className="bg-white rounded-lg border border-red-200 p-6">
              <h2 className="text-lg font-semibold text-red-700 mb-4">Errors</h2>
              <div className="space-y-2">
                {task.errors.map((error: string, i: number) => (
                  <div key={i} className="p-3 bg-red-50 rounded-lg">
                    <p className="text-sm text-red-700">{error}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Sidebar */}
        <div className="space-y-6">
          {/* Timestamps */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-semibold mb-4">Timeline</h2>
            <div className="space-y-4">
              <div>
                <p className="text-sm text-gray-500">Created</p>
                <p className="font-medium">
                  {format(new Date(task.created_at), 'PPpp')}
                </p>
                <p className="text-xs text-gray-400">
                  {formatDistanceToNow(new Date(task.created_at), { addSuffix: true })}
                </p>
              </div>
              {task.started_at && (
                <div>
                  <p className="text-sm text-gray-500">Started</p>
                  <p className="font-medium">
                    {format(new Date(task.started_at), 'PPpp')}
                  </p>
                </div>
              )}
              {task.completed_at && (
                <div>
                  <p className="text-sm text-gray-500">Completed</p>
                  <p className="font-medium">
                    {format(new Date(task.completed_at), 'PPpp')}
                  </p>
                </div>
              )}
            </div>
          </div>

          {/* Config */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-semibold mb-4">Configuration</h2>
            <div className="space-y-3 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-500">Priority</span>
                <span className="font-medium">{task.priority}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Mode</span>
                <span className="font-medium">{task.mode}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Max Attempts</span>
                <span className="font-medium">{task.max_attempts}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
