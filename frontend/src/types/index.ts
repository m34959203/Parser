// Schema Types
export interface FieldDefinition {
  name: string
  selector: string
  selector_type: 'css' | 'xpath' | 'regex'
  attribute?: string
  default_value?: string
  required: boolean
  multiple: boolean
  transformations: string[]
  nested_fields?: FieldDefinition[]
}

export interface NavigationStep {
  action: 'click' | 'scroll' | 'wait' | 'input' | 'hover'
  selector?: string
  value?: string
  wait_after?: number
}

export interface PaginationRule {
  type: 'next_button' | 'infinite_scroll' | 'page_number' | 'load_more'
  selector?: string
  max_pages?: number
  wait_between?: number
}

export interface ParsingSchema {
  id?: string
  name: string
  version: number
  source_id: string
  base_url: string
  url_patterns: string[]
  container_selector?: string
  fields: FieldDefinition[]
  navigation?: NavigationStep[]
  pagination?: PaginationRule
  wait_for?: string
  wait_timeout?: number
  requires_javascript: boolean
  rate_limit_delay?: number
  headers?: Record<string, string>
  cookies?: Record<string, string>
  metadata?: Record<string, unknown>
  created_at?: string
  updated_at?: string
  is_active?: boolean
}

export interface SchemaCreateRequest {
  name: string
  source_id: string
  base_url: string
  url_patterns?: string[]
  container_selector?: string
  fields: FieldDefinition[]
  navigation?: NavigationStep[]
  pagination?: PaginationRule
  wait_for?: string
  wait_timeout?: number
  requires_javascript?: boolean
  rate_limit_delay?: number
  headers?: Record<string, string>
  cookies?: Record<string, string>
  metadata?: Record<string, unknown>
}

export interface SchemaUpdateRequest extends Partial<SchemaCreateRequest> {}

// Task Types
export type TaskStatus = 'pending' | 'queued' | 'running' | 'success' | 'partial' | 'failed' | 'dlq'
export type TaskMode = 'http' | 'browser'
export type TaskPriority = 1 | 2 | 3 | 4 | 5

export interface TaskCreate {
  source_id: string
  schema_id: string
  target_url: string
  mode?: TaskMode
  priority?: TaskPriority
  max_attempts?: number
  callback_url?: string
  metadata?: Record<string, unknown>
}

export interface Task {
  task_id: string
  source_id: string
  schema_id: string
  target_url: string
  mode: TaskMode
  priority: TaskPriority
  status: TaskStatus
  attempt: number
  max_attempts: number
  records_extracted: number
  errors?: string[]
  callback_url?: string
  metadata?: Record<string, unknown>
  created_at: string
  started_at?: string
  completed_at?: string
  scheduled_for?: string
}

export interface TaskListResponse {
  items: Task[]
  total: number
  page: number
  page_size: number
  pages: number
}

// Stats Types
export interface DashboardStats {
  tasks: {
    total: number
    pending: number
    queued: number
    running: number
    success: number
    partial: number
    failed: number
    dlq: number
  }
  schemas: {
    total: number
    active: number
  }
  records: {
    total_extracted: number
    today: number
  }
  performance: {
    avg_duration_ms: number
    success_rate: number
  }
}

export interface TimeSeriesPoint {
  timestamp: string
  value: number
}

export interface TaskTimeSeries {
  success: TimeSeriesPoint[]
  failed: TimeSeriesPoint[]
  pending: TimeSeriesPoint[]
}

// AI Module Types
export interface AIAnalysisRequest {
  url: string
  hint?: string
}

export interface AIAnalysisResponse {
  analysis: {
    page_type: string
    data_patterns: string[]
    suggested_fields: string[]
    complexity: 'low' | 'medium' | 'high'
    requires_javascript: boolean
  }
}

export interface AISchemaGenerateRequest {
  url: string
  analysis?: AIAnalysisResponse['analysis']
  hint?: string
}

export interface AISchemaGenerateResponse {
  schema: ParsingSchema
  confidence: number
  warnings?: string[]
}

export interface AIValidationRequest {
  schema: ParsingSchema
  test_url?: string
}

export interface AIValidationResponse {
  valid: boolean
  errors: string[]
  warnings: string[]
  sample_data?: Record<string, unknown>[]
}

// API Response Types
export interface ApiError {
  detail: string
  code?: string
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  pages: number
}

// Form Types
export interface SchemaFormData extends SchemaCreateRequest {}

export interface TaskFormData extends TaskCreate {}

// Filter Types
export interface TaskFilters {
  status?: TaskStatus
  source_id?: string
  schema_id?: string
  from_date?: string
  to_date?: string
}

export interface SchemaFilters {
  source_id?: string
  is_active?: boolean
  search?: string
}
