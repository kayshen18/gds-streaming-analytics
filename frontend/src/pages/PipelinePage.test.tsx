import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getHealth,
  getPublication,
} from '../api/client'
import PipelinePage from './PipelinePage'

import type {
  PublicationResponse,
} from '../api/types'


vi.mock('../api/client', () => ({
  getHealth: vi.fn(),
  getPublication: vi.fn(),
}))


const getHealthMock = vi.mocked(getHealth)
const getPublicationMock = vi.mocked(getPublication)

const publicationPayload: PublicationResponse = {
  publication_id: '126fa842-3721-4233-991f-8fd3b9e22929',
  source_hdfs_root:
    'hdfs://hdfs-namenode:8020/data/gds-full/v1-full-20260813-123126',
  output_version: 'v1',
  source_row_count: 3203,
  successful_response_records: 1310068,
  success_token_count: 2145511,
  metrics_sha256:
    '9b0f4a3afc33e73461414ff2d60a2653e32a5fdbcfe8a810b8b2b42525fcc0be',
  status: 'published',
  completed_at: '2026-08-13T14:15:19.385388',
}


describe('PipelinePage', () => {
  beforeEach(() => {
    getHealthMock.mockReset()
    getPublicationMock.mockReset()
  })

  it('displays service health and publication metadata', async () => {
    getHealthMock.mockResolvedValue({
      status: 'ok',
      service: 'gds-analytics-api',
    })
    getPublicationMock.mockResolvedValue(publicationPayload)

    render(<PipelinePage />)

    expect(
      screen.getByText('Loading pipeline status...'),
    ).toBeInTheDocument()

    expect(
      await screen.findByText('API and database ready'),
    ).toBeInTheDocument()

    expect(
      screen.getByText('gds-analytics-api'),
    ).toBeInTheDocument()
    expect(screen.getByText('published')).toBeInTheDocument()
    expect(screen.getByText('3,203')).toBeInTheDocument()
    expect(screen.getByText('1,310,068')).toBeInTheDocument()
    expect(screen.getByText('2,145,511')).toBeInTheDocument()
    expect(
      screen.getByText(publicationPayload.source_hdfs_root),
    ).toBeInTheDocument()
    expect(
      screen.getByText(publicationPayload.metrics_sha256),
    ).toBeInTheDocument()

    expect(getHealthMock).toHaveBeenCalledTimes(1)
    expect(getPublicationMock).toHaveBeenCalledTimes(1)
  })

  it('shows a safe message when status loading fails', async () => {
    getHealthMock.mockRejectedValue(
      new Error('database password was rejected'),
    )
    getPublicationMock.mockResolvedValue(publicationPayload)

    render(<PipelinePage />)

    expect(
      await screen.findByRole('alert'),
    ).toHaveTextContent('Unable to load pipeline status.')

    expect(
      screen.queryByText('database password was rejected'),
    ).not.toBeInTheDocument()
  })
})
