import { expectTypeOf, test } from 'vitest'

import type { OverviewResponse } from './types'


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
