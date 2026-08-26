export interface OverviewResponse {
  metric_rows: number
  airline_count: number
  successful_response_records: number
  success_token_count: number
  start_date: string
  end_date: string
  publication_id: string
}

export interface AirlineSummary {
  airline_code: string
  successful_response_records: number
  success_token_count: number
}


export interface AirlinesResponse {
  total_airlines: number
  items: AirlineSummary[]
}

export interface TimelinePoint {
  stat_date: string
  stat_hour: number
  successful_response_records: number
  success_token_count: number
}

export interface TimelineResponse {
  items: TimelinePoint[]
}

export interface AirlineTimelineResponse {
  airline_code: string
  items: TimelinePoint[]
}

export interface HeatmapCell {
  airline_code: string
  stat_hour: number
  successful_response_records: number
  success_token_count: number
}

export interface HourlyHeatmapResponse {
  airlines: string[]
  hours: number[]
  cells: HeatmapCell[]
}

export interface HealthResponse {
  status: 'ok'
  service: 'gds-analytics-api'
}

export interface PublicationResponse {
  publication_id: string
  source_hdfs_root: string
  output_version: string
  source_row_count: number
  successful_response_records: number
  success_token_count: number
  metrics_sha256: string
  status: 'published'
  completed_at: string
}
