import { describe, expect, it } from 'vitest'

import {
  buildAirlineTimelineOptions,
} from './airlineTimelineOptions'

describe('buildAirlineTimelineOptions', () => {
  it('maps hourly points to chart axes and series', () => {
    const options = buildAirlineTimelineOptions([
      {
        stat_date: '2018-08-30',
        stat_hour: 0,
        successful_response_records: 4174,
        success_token_count: 8622,
      },
      {
        stat_date: '2018-08-30',
        stat_hour: 1,
        successful_response_records: 3890,
        success_token_count: 8104,
      },
    ])

    expect(options.xAxis.data).toEqual([
      '00:00',
      '01:00',
    ])

    expect(options.series[0]).toMatchObject({
      name: 'Successful responses',
      data: [4174, 3890],
    })

    expect(options.series[1]).toMatchObject({
      name: 'Successful tokens',
      data: [8622, 8104],
    })
  })
})
