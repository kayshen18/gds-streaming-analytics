import type { TimelinePoint } from '../api/types'


export function buildAirlineTimelineOptions(
  points: TimelinePoint[],
) {
  return {
    color: [
      '#5e606c',
      '#e4babe',
    ],
    tooltip: {
      trigger: 'axis',
    },
    legend: {
      data: [
        'Successful responses',
        'Successful tokens',
      ],
      bottom: 4,
    },
    grid: {
      left: 64,
      right: 64,
      top: 72,
      bottom: 88,
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: points.map(
        (point) =>
          `${String(point.stat_hour).padStart(2, '0')}:00`,
      ),
    },
    yAxis: [
      {
        type: 'value',
        name: 'Responses',
      },
      {
        type: 'value',
        name: 'Tokens',
      },
    ],
    series: [
      {
        name: 'Successful responses',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: points.map(
          (point) => point.successful_response_records,
        ),
      },
      {
        name: 'Successful tokens',
        type: 'line',
        yAxisIndex: 1,
        smooth: true,
        showSymbol: false,
        data: points.map(
          (point) => point.success_token_count,
        ),
      },
    ],
  }
}
