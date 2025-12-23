import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export const api = axios.create({
  baseURL: `${API_URL}/api/v1`,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor
api.interceptors.request.use(
  (config) => {
    // Add auth token if available
    const token = localStorage.getItem('token')
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// Response interceptor
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // Handle common errors
    if (error.response?.status === 401) {
      // Redirect to login or refresh token
      localStorage.removeItem('token')
    }
    return Promise.reject(error)
  }
)

// API methods
export const schemasApi = {
  list: (params?: { limit?: number; offset?: number; source_id?: string }) =>
    api.get('/schemas', { params }),
  get: (schemaId: string, version?: string) =>
    api.get(`/schemas/${schemaId}`, { params: { version } }),
  create: (data: any) =>
    api.post('/schemas', data),
  update: (schemaId: string, data: any) =>
    api.put(`/schemas/${schemaId}`, data),
  delete: (schemaId: string) =>
    api.delete(`/schemas/${schemaId}`),
  validate: (schemaId: string, testUrls: string[]) =>
    api.post(`/schemas/${schemaId}/validate`, testUrls),
  versions: (schemaId: string) =>
    api.get(`/schemas/${schemaId}/versions`),
}

export const tasksApi = {
  list: (params?: { status?: string; source_id?: string; limit?: number; offset?: number }) =>
    api.get('/tasks', { params }),
  get: (taskId: string) =>
    api.get(`/tasks/${taskId}`),
  create: (data: any) =>
    api.post('/tasks', data),
  retry: (taskId: string) =>
    api.post(`/tasks/${taskId}/retry`),
  cancel: (taskId: string) =>
    api.post(`/tasks/${taskId}/cancel`),
  dlq: () =>
    api.get('/tasks/dlq'),
}

export const aiApi = {
  generate: (data: { url: string; goal_description: string; example_fields?: string[] }) =>
    api.post('/ai/generate', data),
  getResult: (taskId: string) =>
    api.get(`/ai/generate/${taskId}`),
  analyze: (url: string, goal: string) =>
    api.post('/ai/analyze', null, { params: { url, goal } }),
  validate: (schema: any, testUrl: string) =>
    api.post('/ai/validate', { schema, test_url: testUrl }),
}

export const statsApi = {
  overview: () =>
    api.get('/stats/overview'),
  tasks: () =>
    api.get('/stats/tasks'),
  queues: () =>
    api.get('/stats/queues'),
  health: () =>
    api.get('/stats/health'),
}
