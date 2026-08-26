import { useEffect, useRef } from 'react'
import { init } from './echarts'

import type {
  HourlyHeatmapResponse,
} from '../api/types'
import {
  buildHourlyHeatmapOptions,
} from './hourlyHeatmapOptions'


interface HourlyHeatmapChartProps {
  heatmap: HourlyHeatmapResponse
}


function HourlyHeatmapChart({
  heatmap,
}: HourlyHeatmapChartProps) {
  const chartElementRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const chartElement = chartElementRef.current

    if (chartElement === null) {
      return
    }

    const chart = init(chartElement)

    chart.setOption(
      buildHourlyHeatmapOptions(heatmap),
    )

    function handleResize() {
      chart.resize()
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.dispose()
    }
  }, [heatmap])

  return (
    <section
      className="chart-panel"
      aria-labelledby="hourly-heatmap-title"
    >
      <h3 id="hourly-heatmap-title">
        Airline hourly heatmap
      </h3>

      <div
        ref={chartElementRef}
        className="heatmap-canvas"
        role="img"
        aria-label="Airline hourly heatmap"
      />
    </section>
  )
}

export default HourlyHeatmapChart
