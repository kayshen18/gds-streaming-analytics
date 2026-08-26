import { useEffect, useState } from 'react'

import { getOverview } from '../api/client'
import type { OverviewResponse } from '../api/types'


const numberFormatter = new Intl.NumberFormat('en-US')


function OverviewPage() {
  const [overview, setOverview] =
    useState<OverviewResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let isActive = true

    getOverview()
      .then((payload) => {
        if (isActive) {
          setOverview(payload)
        }
      })
      .catch(() => {
        if (isActive) {
          setError('Unable to load overview.')
        }
      })

    return () => {
      isActive = false
    }
  }, [])

  if (error !== null) {
    return (
      <>
        <h2>Overview</h2>
        <p role="alert">{error}</p>
      </>
    )
  }

  if (overview === null) {
    return (
      <>
        <h2>Overview</h2>
        <p>Loading overview...</p>
      </>
    )
  }

  return (
    <>
      <h2>Overview</h2>

      <dl className="metrics-grid">
        <div className="metric-card">
          <dt>Metric rows</dt>
          <dd>{numberFormatter.format(overview.metric_rows)}</dd>
        </div>

        <div className="metric-card">
          <dt>Airlines</dt>
          <dd>{numberFormatter.format(overview.airline_count)}</dd>
        </div>

        <div className="metric-card">
          <dt>Successful responses</dt>
          <dd>
            {numberFormatter.format(
              overview.successful_response_records,
            )}
          </dd>
        </div>

        <div className="metric-card">
          <dt>Successful tokens</dt>
          <dd>
            {numberFormatter.format(
              overview.success_token_count,
            )}
          </dd>
        </div>
      </dl>
    </>
  )
}

export default OverviewPage
