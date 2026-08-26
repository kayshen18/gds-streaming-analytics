import type {
  HourlyHeatmapResponse,
} from '../api/types'


export function buildHourlyHeatmapOptions(
  heatmap: HourlyHeatmapResponse,
) {
  const hourIndexes = new Map(
    heatmap.hours.map((hour, index) => [hour, index]),
  )
  const airlineIndexes = new Map(
    heatmap.airlines.map(
      (airlineCode, index) => [airlineCode, index],
    ),
  )

  const data = heatmap.cells.map((cell) => [
    hourIndexes.get(cell.stat_hour) ?? 0,
    airlineIndexes.get(cell.airline_code) ?? 0,
    cell.successful_response_records,
  ])

  const maximumValue = Math.max(
    0,
    ...heatmap.cells.map(
      (cell) => cell.successful_response_records,
    ),
  )

  return {
    tooltip: {
      position: 'top',
    },
    grid: {
      left: 72,
      right: 32,
      top: 32,
      bottom: 150,
    },
    xAxis: {
      type: 'category',
      data: heatmap.hours.map(
        (hour) => `${String(hour).padStart(2, '0')}:00`,
      ),
      axisLabel: {
        interval: 0,
        rotate: 45,
      },
      splitArea: {
        show: true,
      },
    },
    yAxis: {
      type: 'category',
      data: heatmap.airlines,
      splitArea: {
        show: true,
      },
    },
    visualMap: {
      min: 0,
      max: maximumValue,
      calculable: true,
      orient: 'horizontal',
      left: 'center',
      bottom: 0,
      inRange: {
        color: [
          '#fcf1ef',
          '#e4babe',
          '#5e606c',
        ],
      },
    },
    series: [
      {
        name: 'Successful responses',
        type: 'heatmap',
        data,
        label: {
          show: false,
        },
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowColor: 'rgba(0, 0, 0, 0.35)',
          },
        },
      },
    ],
  }
}
