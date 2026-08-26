import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getHourlyHeatmap } from '../api/client'
import HourlyHeatmapSection from './HourlyHeatmapSection'


vi.mock('../api/client', () => ({
  getHourlyHeatmap: vi.fn(),
}))

vi.mock('../charts/HourlyHeatmapChart', () => ({
  default: ({
    heatmap,
  }: {
    heatmap: {
      airlines: string[]
      hours: number[]
    }
  }) => (
    <div role="img" aria-label="Airline hourly heatmap">
      {heatmap.airlines.length} airlines and{' '}
      {heatmap.hours.length} hours
    </div>
  ),
}))


const getHourlyHeatmapMock = vi.mocked(getHourlyHeatmap)

const heatmapPayload = {
  airlines: ['CZ', 'MU'],
  hours: [0, 1],
  cells: [
    {
      airline_code: 'CZ',
      stat_hour: 0,
      successful_response_records: 1200,
      success_token_count: 2400,
    },
  ],
}


describe('HourlyHeatmapSection', () => {
  beforeEach(() => {
    getHourlyHeatmapMock.mockReset()
  })

  it('loads the heatmap and reloads when the limit changes', async () => {
    getHourlyHeatmapMock.mockResolvedValue(heatmapPayload)

    const user = userEvent.setup()

    render(<HourlyHeatmapSection />)

    expect(
      screen.getByText('Loading heatmap...'),
    ).toBeInTheDocument()

    expect(
      await screen.findByRole('img', {
        name: 'Airline hourly heatmap',
      }),
    ).toHaveTextContent('2 airlines and 2 hours')

    expect(getHourlyHeatmapMock).toHaveBeenCalledWith(10)

    await user.selectOptions(
      screen.getByRole('combobox', {
        name: 'Number of airlines in heatmap',
      }),
      '5',
    )

    expect(getHourlyHeatmapMock).toHaveBeenLastCalledWith(5)
    expect(getHourlyHeatmapMock).toHaveBeenCalledTimes(2)
  })

  it('shows a safe message when loading fails', async () => {
    getHourlyHeatmapMock.mockRejectedValue(
      new Error('database details'),
    )

    render(<HourlyHeatmapSection />)

    expect(
      await screen.findByRole('alert'),
    ).toHaveTextContent('Unable to load heatmap.')

    expect(
      screen.queryByText('database details'),
    ).not.toBeInTheDocument()
  })
})
