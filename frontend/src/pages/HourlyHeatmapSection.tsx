import { useEffect, useState } from 'react'

import { getHourlyHeatmap } from '../api/client'
import type {
  HourlyHeatmapResponse,
} from '../api/types'
import HourlyHeatmapChart from '../charts/HourlyHeatmapChart'


const heatmapLimits = [5, 10, 20]


function HourlyHeatmapSection() {
  const [limit, setLimit] = useState(10)
  const [heatmap, setHeatmap] =
    useState<HourlyHeatmapResponse | null>(null)
  const [hasError, setHasError] = useState(false)

  useEffect(() => {
    let isActive = true

    async function loadHeatmap() {
      try {
        const response = await getHourlyHeatmap(limit)

        if (isActive) {
          setHeatmap(response)
        }
      } catch {
        if (isActive) {
          setHasError(true)
        }
      }
    }

    void loadHeatmap()

    return () => {
      isActive = false
    }
  }, [limit])

  return (
    <section
      className="timeline-section"
      aria-labelledby="heatmap-section-title"
    >
      <div className="section-heading">
        <div>
          <h3 id="heatmap-section-title">
            Airline activity by hour
          </h3>
          <p>
            Successful responses across ranked airlines and hours.
          </p>
        </div>

        <label className="limit-control">
          <span>Number of airlines in heatmap</span>
          <select
            aria-label="Number of airlines in heatmap"
            value={limit}
            onChange={(event) => {
              setHeatmap(null)
              setHasError(false)
              setLimit(Number(event.target.value))
            }}
          >
            {heatmapLimits.map((option) => (
              <option key={option} value={option}>
                Top {option}
              </option>
            ))}
          </select>
        </label>
      </div>

      {hasError ? (
        <p role="alert">Unable to load heatmap.</p>
      ) : heatmap === null ? (
        <p>Loading heatmap...</p>
      ) : (
        <HourlyHeatmapChart heatmap={heatmap} />
      )}
    </section>
  )
}

export default HourlyHeatmapSection
