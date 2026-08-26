import { useEffect, useState } from 'react'

import { getOverview } from '../api/client'
import type { OverviewResponse } from '../api/types'


const numberFormatter = new Intl.NumberFormat('en-US')


function OverviewPage() {
  const [overview, setOverview] =
    useState<OverviewResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [requestVersion, setRequestVersion] = useState(0)

  useEffect(() => {
    let isActive = true

    getOverview()
      .then((payload) => {
        if (isActive) {
          setOverview(payload)
          setError(null)
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
  }, [requestVersion])

  function handleRefresh() {
    setOverview(null)
    setError(null)
    setRequestVersion((currentVersion) => currentVersion + 1)
  }

  const isLoading = overview === null && error === null

  return (
    <>
      <div className="page-heading">
        <h2>Overview</h2>
        <button
          type="button"
          onClick={handleRefresh}
          disabled={isLoading}
        >
          Refresh overview
        </button>
      </div>

      {error !== null ? (
        <p role="alert">{error}</p>
      ) : overview === null ? (
        <p>Loading overview...</p>
      ) : (
        <>
          <dl className="metrics-grid">
            <div className="metric-card">
              <dt>Metric rows</dt>
              <dd>
                {numberFormatter.format(overview.metric_rows)}
              </dd>
            </div>

            <div className="metric-card">
              <dt>Airlines</dt>
              <dd>
                {numberFormatter.format(overview.airline_count)}
              </dd>
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

          <section
            className="snapshot-details"
            aria-labelledby="snapshot-details-title"
          >
            <h3 id="snapshot-details-title">
              Snapshot details
            </h3>

            <dl className="metadata-list">
              <div>
                <dt>Data range</dt>
                <dd>
                  {overview.start_date} to {overview.end_date}
                </dd>
              </div>

              <div>
                <dt>Publication ID</dt>
                <dd>{overview.publication_id}</dd>
              </div>
            </dl>
          </section>
        </>
      )}
    </>
  )
}

export default OverviewPage
