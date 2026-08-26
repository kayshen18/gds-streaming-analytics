import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import HourlyHeatmapChart from './HourlyHeatmapChart'


const chartMocks = vi.hoisted(() => ({
  init: vi.fn(),
  setOption: vi.fn(),
  resize: vi.fn(),
  dispose: vi.fn(),
}))


vi.mock('./echarts', () => ({
  init: chartMocks.init,
}))


describe('HourlyHeatmapChart', () => {
  beforeEach(() => {
    chartMocks.init.mockReset()
    chartMocks.setOption.mockReset()
    chartMocks.resize.mockReset()
    chartMocks.dispose.mockReset()

    chartMocks.init.mockReturnValue({
      setOption: chartMocks.setOption,
      resize: chartMocks.resize,
      dispose: chartMocks.dispose,
    })
  })

  it('renders, configures, and disposes the heatmap', () => {
    const { unmount } = render(
      <HourlyHeatmapChart
        heatmap={{
          airlines: ['CZ'],
          hours: [0],
          cells: [
            {
              airline_code: 'CZ',
              stat_hour: 0,
              successful_response_records: 1200,
              success_token_count: 2400,
            },
          ],
        }}
      />,
    )

    expect(
      screen.getByRole('img', {
        name: 'Airline hourly heatmap',
      }),
    ).toBeInTheDocument()

    expect(chartMocks.init).toHaveBeenCalledTimes(1)
    expect(chartMocks.setOption).toHaveBeenCalledTimes(1)

    unmount()

    expect(chartMocks.dispose).toHaveBeenCalledTimes(1)
  })
})
