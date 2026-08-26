import type {
  AirlinesResponse,
  AirlineTimelineResponse,
  OverviewResponse,
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
