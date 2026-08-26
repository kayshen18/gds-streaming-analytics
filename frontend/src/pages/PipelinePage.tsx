import { useEffect, useState } from 'react'

import {
  getHealth,
  getPublication,
} from '../api/client'
import type {
  HealthResponse,
  PublicationResponse,
} from '../api/types'


const numberFormatter = new Intl.NumberFormat('en-US')


function PipelinePage() {
  const [health, setHealth] =
    useState<HealthResponse | null>(null)
  const [publication, setPublication] =
    useState<PublicationResponse | null>(null)
  const [hasError, setHasError] = useState(false)

  useEffect(() => {
    let isActive = true

    async function loadPipelineStatus() {
      try {
        const [healthResponse, publicationResponse] =
          await Promise.all([
            getHealth(),
            getPublication(),
          ])

        if (isActive) {
          setHealth(healthResponse)
          setPublication(publicationResponse)
        }
      } catch {
        if (isActive) {
          setHasError(true)
        }
      }
    }

    void loadPipelineStatus()

    return () => {
      isActive = false
    }
  }, [])

  const isLoading =
    health === null &&
    publication === null &&
    !hasError

  return (
    <>
      <h2>Pipeline &amp; Data Quality</h2>

      {hasError ? (
        <p role="alert">
          Unable to load pipeline status.
        </p>
      ) : isLoading ? (
        <p>Loading pipeline status...</p>
      ) : health !== null && publication !== null ? (
        <>
          <section
            className="status-card"
            aria-labelledby="service-status-title"
          >
            <div>
              <h3 id="service-status-title">
                API and database ready
              </h3>
              <p>{health.service}</p>
            </div>

            <span className="status-badge">
              {health.status}
            </span>
          </section>

          <section
            className="publication-panel"
            aria-labelledby="publication-title"
          >
            <div className="section-heading">
              <div>
                <h3 id="publication-title">
                  Published snapshot
                </h3>
                <p>
                  Current analytics serving snapshot and
                  integrity metadata.
                </p>
              </div>

              <span className="status-badge">
                {publication.status}
              </span>
            </div>

            <dl className="publication-grid">
              <div>
                <dt>Publication ID</dt>
                <dd>{publication.publication_id}</dd>
              </div>
              <div>
                <dt>Output version</dt>
                <dd>{publication.output_version}</dd>
              </div>
              <div>
                <dt>Source rows</dt>
                <dd>
                  {numberFormatter.format(
                    publication.source_row_count,
                  )}
                </dd>
              </div>
              <div>
                <dt>Successful responses</dt>
                <dd>
                  {numberFormatter.format(
                    publication.successful_response_records,
                  )}
                </dd>
              </div>
              <div>
                <dt>Successful tokens</dt>
                <dd>
                  {numberFormatter.format(
                    publication.success_token_count,
                  )}
                </dd>
              </div>
              <div>
                <dt>Completed at</dt>
                <dd>{publication.completed_at}</dd>
              </div>
              <div className="publication-wide">
                <dt>HDFS source</dt>
                <dd>{publication.source_hdfs_root}</dd>
              </div>
              <div className="publication-wide">
                <dt>Metrics SHA-256</dt>
                <dd>{publication.metrics_sha256}</dd>
              </div>
            </dl>
          </section>
        </>
      ) : null}
    </>
  )
}

export default PipelinePage
