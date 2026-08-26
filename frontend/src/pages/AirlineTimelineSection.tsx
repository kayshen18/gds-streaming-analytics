import {
  type ChangeEvent,
  useEffect,
  useState,
} from 'react'
import { getAirlineTimeline } from '../api/client'
import type {
  AirlineSummary,
  AirlineTimelineResponse,
} from '../api/types'
import AirlineTimelineChart from '../charts/AirlineTimelineChart'

interface AirlineTimelineSectionProps {
  airlines: AirlineSummary[]
}


function AirlineTimelineSection({
  airlines,
}: AirlineTimelineSectionProps) {
  const [selectedAirline, setSelectedAirline] = useState(
    airlines[0]?.airline_code ?? '',
  )
  const [timeline, setTimeline] =
    useState<AirlineTimelineResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (selectedAirline === '') {
      return
    }

    let isActive = true

    getAirlineTimeline(selectedAirline)
      .then((payload) => {
        if (isActive) {
          setTimeline(payload)
          setError(null)
        }
      })
      .catch(() => {
        if (isActive) {
          setError('Unable to load airline timeline.')
        }
      })

    return () => {
      isActive = false
    }
  }, [selectedAirline])

  function handleAirlineChange(
    event: ChangeEvent<HTMLSelectElement>,
  ) {
    setTimeline(null)
    setError(null)
    setSelectedAirline(event.target.value)
  }

  const isLoading = timeline === null && error === null

  return (
    <section
      className="timeline-section"
      aria-labelledby="airline-timeline-section-title"
    >
      <div className="section-heading">
        <h3 id="airline-timeline-section-title">
          Airline hourly timeline
        </h3>

        <div className="limit-control">
          <label htmlFor="timeline-airline">
            Airline timeline
          </label>
          <select
            id="timeline-airline"
            value={selectedAirline}
            onChange={handleAirlineChange}
            disabled={isLoading}
          >
            {airlines.map((airline) => (
              <option
                key={airline.airline_code}
                value={airline.airline_code}
              >
                {airline.airline_code}
              </option>
            ))}
          </select>
        </div>
      </div>

      {error !== null ? (
        <p role="alert">{error}</p>
      ) : timeline === null ? (
        <p>Loading airline timeline...</p>
      ) : (
        <AirlineTimelineChart
          airlineCode={timeline.airline_code}
          points={timeline.items}
        />
      )}
    </section>
  )
}

export default AirlineTimelineSection
