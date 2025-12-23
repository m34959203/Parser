import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import {
  Activity,
  CheckCircle,
  Clock,
  AlertTriangle,
  ArrowUpRight,
  ArrowDownRight
} from 'lucide-react'
import { api } from '../../api/client'

interface StatCardProps {
  title: string
  value: string | number
  change?: number
  icon: React.ElementType
  trend?: 'up' | 'down'
  status?: 'success' | 'warning' | 'error'
}

function StatCard({ title, value, change, icon: Icon, trend, status }: StatCardProps) {
  const statusColors = {
    success: 'text-green-600 bg-green-100',
    warning: 'text-yellow-600 bg-yellow-100',
    error: 'text-red-600 bg-red-100',
    default: 'text-blue-600 bg-blue-100',
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-6">
      <div className="flex items-center justify-between">
        <div className={`p-2 rounded-lg ${statusColors[status || 'default']}`}>
          <Icon className="h-5 w-5" />
        </div>
        {change !== undefined && (
          <div className={`flex items-center text-sm ${trend === 'up' ? 'text-green-600' : 'text-red-600'}`}>
            {trend === 'up' ? <ArrowUpRight className="h-4 w-4" /> : <ArrowDownRight className="h-4 w-4" />}
            {Math.abs(change)}%
          </div>
        )}
      </div>
      <div className="mt-4">
        <p className="text-2xl font-bold text-gray-900">{value}</p>
        <p className="text-sm text-gray-600">{title}</p>
      </div>
    </div>
  )
}

export function Dashboard() {
  const { data: stats, isLoading } = useQuery({
    queryKey: ['stats', 'overview'],
    queryFn: () => api.get('/stats/overview').then(r => r.data),
    refetchInterval: 30000, // Refresh every 30 seconds
  })

  const { data: recentTasks } = useQuery({
    queryKey: ['tasks', 'recent'],
    queryFn: () => api.get('/tasks', { params: { limit: 5 } }).then(r => r.data),
  })

  if (isLoading) {
    return (
      <div className="animate-pulse">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="bg-white rounded-lg border border-gray-200 p-6 h-32" />
          ))}
        </div>
      </div>
    )
  }

  const taskStats = stats?.tasks || {}
  const queueStats = stats?.queues || {}

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-gray-600">Overview of your parsing operations</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          title="Tasks Today"
          value={taskStats.today_total || 0}
          icon={Activity}
          change={12}
          trend="up"
        />
        <StatCard
          title="Success Rate"
          value={`${taskStats.success_rate || 0}%`}
          icon={CheckCircle}
          status="success"
          change={2.1}
          trend="up"
        />
        <StatCard
          title="Queued Tasks"
          value={(queueStats['tasks.http']?.message_count || 0) + (queueStats['tasks.browser']?.message_count || 0)}
          icon={Clock}
        />
        <StatCard
          title="In DLQ"
          value={taskStats.by_status?.dlq || 0}
          icon={AlertTriangle}
          status={taskStats.by_status?.dlq > 0 ? 'warning' : 'success'}
        />
      </div>

      {/* Recent Tasks */}
      <div className="bg-white rounded-lg border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">Recent Tasks</h2>
          <Link to="/tasks" className="text-sm text-primary hover:underline">
            View all
          </Link>
        </div>
        <div className="divide-y divide-gray-200">
          {recentTasks?.items?.map((task: any) => (
            <div key={task.task_id} className="px-6 py-4 flex items-center justify-between">
              <div>
                <Link
                  to={`/tasks/${task.task_id}`}
                  className="font-medium text-gray-900 hover:text-primary"
                >
                  {task.source_id}
                </Link>
                <p className="text-sm text-gray-500 truncate max-w-md">{task.target_url}</p>
              </div>
              <div className="flex items-center gap-4">
                <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                  task.status === 'success' ? 'bg-green-100 text-green-800' :
                  task.status === 'failed' ? 'bg-red-100 text-red-800' :
                  task.status === 'running' ? 'bg-blue-100 text-blue-800' :
                  'bg-gray-100 text-gray-800'
                }`}>
                  {task.status}
                </span>
                <span className="text-sm text-gray-500">
                  {task.records_extracted} records
                </span>
              </div>
            </div>
          ))}
          {(!recentTasks?.items || recentTasks.items.length === 0) && (
            <div className="px-6 py-8 text-center text-gray-500">
              No tasks yet. Create a schema and start parsing!
            </div>
          )}
        </div>
      </div>

      {/* Queue Status */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">HTTP Queue</h3>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-600">Messages</span>
              <span className="font-medium">{queueStats['tasks.http']?.message_count || 0}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Consumers</span>
              <span className="font-medium">{queueStats['tasks.http']?.consumer_count || 0}</span>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Browser Queue</h3>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-600">Messages</span>
              <span className="font-medium">{queueStats['tasks.browser']?.message_count || 0}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Consumers</span>
              <span className="font-medium">{queueStats['tasks.browser']?.consumer_count || 0}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
