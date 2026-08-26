import type {
  AirlinesResponse,
  AirlineTimelineResponse,
  OverviewResponse,
  TimelineResponse,
  HourlyHeatmapResponse,
} from './types'

export async function getOverview(): Promise<OverviewResponse> {
  const response = await fetch('/api/v1/overview', {
    headers: {
      Accept: 'application/json',
    },
  })

  if (!response.ok) {
    throw new Error(
      `Overview request failed with status ${response.status}`,
    )
  }

  return (await response.json()) as OverviewResponse
}

export async function getAirlines(
  limit: number,
): Promise<AirlinesResponse> {
  const response = await fetch(
    `/api/v1/airlines?limit=${limit}`,
    {
      headers: {
        Accept: 'application/json',
      },
    },
  )

  if (!response.ok) {
    throw new Error(
      `Airlines request failed with status ${response.status}`,
    )
  }

  return (await response.json()) as AirlinesResponse
}

export async function getAirlineTimeline(
  airlineCode: string,
): Promise<AirlineTimelineResponse> {
  const normalizedCode = encodeURIComponent(
    airlineCode.trim().toUpperCase(),
  )

  const response = await fetch(
    `/api/v1/airlines/${normalizedCode}/timeline`,
    {
      headers: {
        Accept: 'application/json',
      },
    },
  )

  if (!response.ok) {
    throw new Error(
      `Airline timeline request failed with status ${response.status}`,
    )
  }

  return (await response.json()) as AirlineTimelineResponse
}

export async function getTimeline(): Promise<TimelineResponse> {
  const response = await fetch('/api/v1/timeline', {
    headers: {
      Accept: 'application/json',
    },
  })

  if (!response.ok) {
    throw new Error(
      `Timeline request failed with status ${response.status}`,
    )
  }

  return (await response.json()) as TimelineResponse
}

export async function getHourlyHeatmap(
  limit: number,
): Promise<HourlyHeatmapResponse> {
  const response = await fetch(
    `/api/v1/hourly-heatmap?limit=${limit}`,
    {
      headers: {
        Accept: 'application/json',
      },
    },
  )

  if (!response.ok) {
    throw new Error(
      `Hourly heatmap request failed with status ${response.status}`,
    )
  }

  return (await response.json()) as HourlyHeatmapResponse
}
