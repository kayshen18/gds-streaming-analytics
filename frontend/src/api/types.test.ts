import { expectTypeOf, test } from 'vitest'

import type {
  AirlineSummary,
  AirlinesResponse,
  AirlineTimelineResponse,
  OverviewResponse,
  TimelinePoint,
} from './types'


test('OverviewResponse describes the overview API payload', () => {
  const overview: OverviewResponse = {
    metric_rows: 3203,
    airline_count: 198,
    successful_response_records: 1310068,
    success_token_count: 2145511,
    start_date: '2018-08-30',
    end_date: '2018-08-30',
    publication_id: '126fa842-3721-4233-991f-8fd3b9e22929',
  }

  expectTypeOf(overview.metric_rows).toEqualTypeOf<number>()
  expectTypeOf(overview.airline_count).toEqualTypeOf<number>()
  expectTypeOf(overview.start_date).toEqualTypeOf<string>()
  expectTypeOf(overview.publication_id).toEqualTypeOf<string>()
})

test('AirlinesResponse describes ranked airline results', () => {
  const airline: AirlineSummary = {
    airline_code: 'CZ',
    successful_response_records: 167610,
    success_token_count: 291028,
  }

  const response: AirlinesResponse = {
    total_airlines: 198,
    items: [airline],
  }

  expectTypeOf(airline.airline_code).toEqualTypeOf<string>()
  expectTypeOf(
    airline.successful_response_records,
  ).toEqualTypeOf<number>()
  expectTypeOf(response.total_airlines).toEqualTypeOf<number>()
  expectTypeOf(response.items).toEqualTypeOf<AirlineSummary[]>()
})

test('AirlineTimelineResponse describes hourly airline data', () => {
  const point: TimelinePoint = {
    stat_date: '2018-08-30',
    stat_hour: 0,
    successful_response_records: 4174,
    success_token_count: 8622,
  }

  const response: AirlineTimelineResponse = {
    airline_code: 'CA',
    items: [point],
  }

  expectTypeOf(point.stat_date).toEqualTypeOf<string>()
  expectTypeOf(point.stat_hour).toEqualTypeOf<number>()
  expectTypeOf(
    point.successful_response_records,
  ).toEqualTypeOf<number>()
  expectTypeOf(response.airline_code).toEqualTypeOf<string>()
  expectTypeOf(response.items).toEqualTypeOf<TimelinePoint[]>()
})
