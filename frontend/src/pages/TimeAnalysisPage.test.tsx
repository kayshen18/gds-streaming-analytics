import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getTimeline } from '../api/client'
import TimeAnalysisPage from './TimeAnalysisPage'


vi.mock('../api/client', () => ({
  getTimeline: vi.fn(),
}))

vi.mock('../charts/AirlineTimelineChart', () => ({
  default: ({
    airlineCode,
    points,
  }: {
    airlineCode: string
    points: unknown[]
  }) => (
    <div
      role="img"
      aria-label={`${airlineCode} hourly timeline`}
    >
      {points.length} hourly points
    </div>
  ),
}))


const getTimelineMock = vi.mocked(getTimeline)

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


describe('TimeAnalysisPage', () => {
  beforeEach(() => {
    getTimelineMock.mockReset()
  })

  it('loads and displays the global timeline', async () => {
    getTimelineMock.mockResolvedValue(timelinePayload)

    render(<TimeAnalysisPage />)

    expect(
      screen.getByText('Loading timeline...'),
    ).toBeInTheDocument()

    expect(
      await screen.findByRole('img', {
        name: 'System-wide hourly timeline',
      }),
    ).toHaveTextContent('2 hourly points')

    expect(getTimelineMock).toHaveBeenCalledTimes(1)
  })

  it('shows a safe message when loading fails', async () => {
    getTimelineMock.mockRejectedValue(
      new Error('database connection details'),
    )

    render(<TimeAnalysisPage />)

    expect(
      await screen.findByRole('alert'),
    ).toHaveTextContent('Unable to load timeline.')

    expect(
      screen.queryByText('database connection details'),
    ).not.toBeInTheDocument()
  })
})
