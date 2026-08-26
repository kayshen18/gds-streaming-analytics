import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getOverview } from '../api/client'
import OverviewPage from './OverviewPage'


vi.mock('../api/client', () => ({
  getOverview: vi.fn(),
}))


const getOverviewMock = vi.mocked(getOverview)

const overviewPayload = {
  metric_rows: 3203,
  airline_count: 198,
  successful_response_records: 1310068,
  success_token_count: 2145511,
  start_date: '2018-08-30',
  end_date: '2018-08-30',
  publication_id: '126fa842-3721-4233-991f-8fd3b9e22929',
}


describe('OverviewPage', () => {
  beforeEach(() => {
    getOverviewMock.mockReset()
  })

  it('loads and displays the overview metrics', async () => {
    getOverviewMock.mockResolvedValue(overviewPayload)

    render(<OverviewPage />)

    expect(
      screen.getByText('Loading overview...'),
    ).toBeInTheDocument()

    expect(
      await screen.findByText('3,203'),
    ).toBeInTheDocument()

    expect(screen.getByText('Metric rows')).toBeInTheDocument()
    expect(screen.getByText('198')).toBeInTheDocument()
    expect(screen.getByText('Airlines')).toBeInTheDocument()
    expect(screen.getByText('1,310,068')).toBeInTheDocument()
    expect(
      screen.getByText('Successful responses'),
    ).toBeInTheDocument()
    expect(screen.getByText('2,145,511')).toBeInTheDocument()
    expect(
      screen.getByText('Successful tokens'),
    ).toBeInTheDocument()
  })

  it('shows a safe message when loading fails', async () => {
    getOverviewMock.mockRejectedValue(
      new Error('database password was rejected'),
    )

    render(<OverviewPage />)

    expect(
      await screen.findByRole('alert'),
    ).toHaveTextContent('Unable to load overview.')

    expect(
      screen.queryByText('database password was rejected'),
    ).not.toBeInTheDocument()
  })
})
