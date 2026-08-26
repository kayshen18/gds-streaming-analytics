import { useEffect, useState } from 'react'

import { getTimeline } from '../api/client'
import type { TimelineResponse } from '../api/types'
import AirlineTimelineChart from '../charts/AirlineTimelineChart'


function TimeAnalysisPage() {
  const [timeline, setTimeline] =
    useState<TimelineResponse | null>(null)
  const [hasError, setHasError] = useState(false)

  useEffect(() => {
    let isActive = true

    async function loadTimeline() {
      try {
        const response = await getTimeline()

        if (isActive) {
          setTimeline(response)
          setHasError(false)
        }
      } catch {
        if (isActive) {
          setHasError(true)
        }
      }
    }

    void loadTimeline()

    return () => {
      isActive = false
    }
  }, [])

  return (
    <>
      <h2>Time Analysis</h2>

      {hasError ? (
        <p role="alert">Unable to load timeline.</p>
      ) : timeline === null ? (
        <p>Loading timeline...</p>
      ) : (
        <AirlineTimelineChart
          airlineCode="System-wide"
          points={timeline.items}
        />
      )}
    </>
  )
}

export default TimeAnalysisPage
