import { useEffect, useState } from 'react'

import { getAirlines } from '../api/client'
import type { AirlinesResponse } from '../api/types'


const DEFAULT_LIMIT = 10
const numberFormatter = new Intl.NumberFormat('en-US')


function AirlineAnalysisPage() {
  const [airlines, setAirlines] =
    useState<AirlinesResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let isActive = true

    getAirlines(DEFAULT_LIMIT)
      .then((payload) => {
        if (isActive) {
          setAirlines(payload)
        }
      })
      .catch(() => {
        if (isActive) {
          setError('Unable to load airline rankings.')
        }
      })

    return () => {
      isActive = false
    }
  }, [])

  if (error !== null) {
    return (
      <>
        <h2>Airline Analysis</h2>
        <p role="alert">{error}</p>
      </>
    )
  }

  if (airlines === null) {
    return (
      <>
        <h2>Airline Analysis</h2>
        <p>Loading airline rankings...</p>
      </>
    )
  }

  return (
    <>
      <h2>Airline Analysis</h2>
      <p>{airlines.total_airlines} total airlines</p>

      <div className="table-container">
        <table aria-label="Airline rankings">
          <thead>
            <tr>
              <th scope="col">Airline</th>
              <th scope="col">Successful responses</th>
              <th scope="col">Successful tokens</th>
            </tr>
          </thead>

          <tbody>
            {airlines.items.map((airline) => (
              <tr key={airline.airline_code}>
                <td>{airline.airline_code}</td>
                <td className="numeric-cell">
                  {numberFormatter.format(
                    airline.successful_response_records,
                  )}
                </td>
                <td className="numeric-cell">
                  {numberFormatter.format(
                    airline.success_token_count,
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

export default AirlineAnalysisPage
