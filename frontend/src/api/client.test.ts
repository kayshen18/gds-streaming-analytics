import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  getAirlines,
  getAirlineTimeline,
  getOverview,
  getTimeline,
} from './client'

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


describe('getAirlines', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('requests ranked airlines with the selected limit', async () => {
    const airlinesPayload = {
      total_airlines: 198,
      items: [
        {
          airline_code: 'CZ',
          successful_response_records: 167610,
          success_token_count: 291028,
        },
      ],
    }

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: vi.fn().mockResolvedValue(airlinesPayload),
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(getAirlines(10)).resolves.toEqual(
      airlinesPayload,
    )

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/airlines?limit=10',
      {
        headers: {
          Accept: 'application/json',
        },
      },
    )
  })

  it('rejects when the airlines request fails', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 422,
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(getAirlines(0)).rejects.toThrow(
      'Airlines request failed with status 422',
    )
  })
})

describe('getAirlineTimeline', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('normalizes the code and requests its timeline', async () => {
    const timelinePayload = {
      airline_code: 'CA',
      items: [
        {
          stat_date: '2018-08-30',
          stat_hour: 0,
          successful_response_records: 4174,
          success_token_count: 8622,
        },
      ],
    }

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: vi.fn().mockResolvedValue(timelinePayload),
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(
      getAirlineTimeline(' ca '),
    ).resolves.toEqual(timelinePayload)

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/airlines/CA/timeline',
      {
        headers: {
          Accept: 'application/json',
        },
      },
    )
  })

  it('rejects when the airline timeline is unavailable', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(
      getAirlineTimeline('NOTREAL'),
    ).rejects.toThrow(
      'Airline timeline request failed with status 404',
    )
  })
})

describe('getTimeline', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('requests and returns the global timeline', async () => {
    const timelinePayload = {
      items: [
        {
          stat_date: '2018-08-30',
          stat_hour: 0,
          successful_response_records: 25521,
          success_token_count: 46434,
        },
        {
          stat_date: '2018-08-30',
          stat_hour: 23,
          successful_response_records: 43103,
          success_token_count: 77030,
        },
      ],
    }

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: vi.fn().mockResolvedValue(timelinePayload),
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(getTimeline()).resolves.toEqual(
      timelinePayload,
    )

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/timeline',
      {
        headers: {
          Accept: 'application/json',
        },
      },
    )
  })

  it('rejects when the timeline request fails', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
    })

    vi.stubGlobal('fetch', fetchMock)

    await expect(getTimeline()).rejects.toThrow(
      'Timeline request failed with status 503',
    )
  })
})
