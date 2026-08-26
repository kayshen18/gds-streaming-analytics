import { afterEach, describe, expect, it, vi } from 'vitest'

import { getOverview } from './client'


const overviewPayload = {
  metric_rows: 3203,
  airline_count: 198,
  successful_response_records: 1310068,
  success_token_count: 2145511,
  start_date: '2018-08-30',
  end_date: '2018-08-30',
  publication_id: '126fa842-3721-4233-991f-8fd3b9e22929',
}


describe('getOverview', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('requests and returns the overview payload', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: vi.fn().mockResolvedValue(overviewPayload),
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(getOverview()).resolves.toEqual(
      overviewPayload,
    )

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/overview',
      {
        headers: {
          Accept: 'application/json',
        },
      },
    )
  })

  it('rejects when the overview request fails', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(getOverview()).rejects.toThrow(
      'Overview request failed with status 503',
    )
  })
})
