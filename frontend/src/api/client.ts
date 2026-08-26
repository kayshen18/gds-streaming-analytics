import type { OverviewResponse } from './types'


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
