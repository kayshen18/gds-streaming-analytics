import { useEffect, useRef } from 'react'
import { init } from './echarts'

import type { TimelinePoint } from '../api/types'
import {
  buildAirlineTimelineOptions,
} from './airlineTimelineOptions'


interface AirlineTimelineChartProps {
  airlineCode: string
  points: TimelinePoint[]
}


function AirlineTimelineChart({
  airlineCode,
  points,
}: AirlineTimelineChartProps) {
  const chartElementRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const chartElement = chartElementRef.current

    if (chartElement === null) {
      return
    }

    const chart = init(chartElement)

    chart.setOption(
      buildAirlineTimelineOptions(points),
    )

    function handleResize() {
      chart.resize()
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.dispose()
    }
  }, [points])

  return (
    <section
      className="chart-panel"
      aria-labelledby="airline-timeline-title"
    >
      <h3 id="airline-timeline-title">
        {airlineCode} hourly timeline
      </h3>

      <div
        ref={chartElementRef}
        className="chart-canvas"
        role="img"
        aria-label={`${airlineCode} hourly timeline`}
      />
    </section>
  )
}

export default AirlineTimelineChart
