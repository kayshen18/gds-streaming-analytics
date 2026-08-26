import {
  type ChangeEvent,
  useEffect,
  useState,
} from 'react'

import { getAirlines } from '../api/client'
import type { AirlinesResponse } from '../api/types'


const DEFAULT_LIMIT = 10
const numberFormatter = new Intl.NumberFormat('en-US')


function AirlineAnalysisPage() {
  const [limit, setLimit] = useState(DEFAULT_LIMIT)
  const [airlines, setAirlines] =
    useState<AirlinesResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let isActive = true

    getAirlines(limit)
      .then((payload) => {
        if (isActive) {
          setAirlines(payload)
          setError(null)
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
  }, [limit])

  function handleLimitChange(
    event: ChangeEvent<HTMLSelectElement>,
  ) {
    setAirlines(null)
    setError(null)
    setLimit(Number(event.target.value))
  }

  const isLoading = airlines === null && error === null

  return (
    <>
      <div className="page-heading">
        <h2>Airline Analysis</h2>

        <div className="limit-control">
          <label htmlFor="airline-limit">
            Number of airlines
          </label>
          <select
            id="airline-limit"
            value={limit}
            onChange={handleLimitChange}
            disabled={isLoading}
          >
            <option value={5}>Top 5</option>
            <option value={10}>Top 10</option>
            <option value={20}>Top 20</option>
          </select>
        </div>
      </div>

      {error !== null ? (
        <p role="alert">{error}</p>
      ) : airlines === null ? (
        <p>Loading airline rankings...</p>
      ) : (
        <>
          <p>{airlines.total_airlines} total airlines</p>

          <div className="table-container">
            <table aria-label="Airline rankings">
              <thead>
                <tr>
                  <th scope="col">Airline</th>
                  <th scope="col">
                    Successful responses
                  </th>
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
      )}
    </>
  )
}

export default AirlineAnalysisPage
