import { describe, expect, it } from 'vitest'

import { buildHourlyHeatmapOptions } from './hourlyHeatmapOptions'


describe('buildHourlyHeatmapOptions', () => {
  it('maps airlines, hours, and response counts to heatmap data', () => {
    const options = buildHourlyHeatmapOptions({
      airlines: ['CZ', 'MU'],
      hours: [0, 1],
      cells: [
        {
          airline_code: 'CZ',
          stat_hour: 0,
          successful_response_records: 1200,
          success_token_count: 2400,
        },
        {
          airline_code: 'MU',
          stat_hour: 1,
          successful_response_records: 900,
          success_token_count: 1800,
        },
      ],
    })

    expect(options.xAxis.data).toEqual([
      '00:00',
      '01:00',
    ])
    expect(options.xAxis.axisLabel).toEqual({
      interval: 0,
      rotate: 45,
    })
    expect(options.yAxis.data).toEqual([
      'CZ',
      'MU',
    ])
    expect(options.series[0].data).toEqual([
      [0, 0, 1200],
      [1, 1, 900],
    ])
    expect(options.visualMap.max).toBe(1200)
  })
})
