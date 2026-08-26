import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import AirlineTimelineChart from './AirlineTimelineChart'


const chartMocks = vi.hoisted(() => ({
  init: vi.fn(),
  setOption: vi.fn(),
  resize: vi.fn(),
  dispose: vi.fn(),
}))


vi.mock('./echarts', () => ({
  init: chartMocks.init,
}))


describe('AirlineTimelineChart', () => {
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

  it('renders, configures, and disposes the chart', () => {
    const { unmount } = render(
      <AirlineTimelineChart
        airlineCode="CA"
        points={[
          {
            stat_date: '2018-08-30',
            stat_hour: 0,
            successful_response_records: 4174,
            success_token_count: 8622,
          },
        ]}
      />,
    )

    expect(
      screen.getByRole('img', {
        name: 'CA hourly timeline',
      }),
    ).toBeInTheDocument()

    expect(chartMocks.init).toHaveBeenCalledTimes(1)
    expect(chartMocks.setOption).toHaveBeenCalledTimes(1)

    unmount()

    expect(chartMocks.dispose).toHaveBeenCalledTimes(1)
  })
})
