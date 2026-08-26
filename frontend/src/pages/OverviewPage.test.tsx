import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest'

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

  afterEach(() => {
    vi.useRealTimers()
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
    expect(
      screen.getByText('Data range'),
    ).toBeInTheDocument()
    expect(
      screen.getByText('2018-08-30 to 2018-08-30'),
    ).toBeInTheDocument()

    expect(
      screen.getByText('Publication ID'),
    ).toBeInTheDocument()
    expect(
      screen.getByText(
        '126fa842-3721-4233-991f-8fd3b9e22929',
      ),
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

  it('reloads the overview when refresh is clicked', async () => {
    const refreshedPayload = {
      ...overviewPayload,
      metric_rows: 4000,
    }

    getOverviewMock
      .mockResolvedValueOnce(overviewPayload)
      .mockResolvedValueOnce(refreshedPayload)

    const user = userEvent.setup()

    render(<OverviewPage />)

    expect(
      await screen.findByText('3,203'),
    ).toBeInTheDocument()

    await user.click(
      screen.getByRole('button', {
        name: 'Refresh overview',
      }),
    )

    expect(
      await screen.findByText('4,000'),
    ).toBeInTheDocument()

    expect(getOverviewMock).toHaveBeenCalledTimes(2)
  })

  it('refreshes the overview automatically every 30 seconds', async () => {
    vi.useFakeTimers()

    const refreshedPayload = {
      ...overviewPayload,
      metric_rows: 12,
      successful_response_records: 100,
      success_token_count: 100,
      publication_id: 'bf65b507-0884-494b-8e2d-f8c89c81040a',
    }

    getOverviewMock
      .mockResolvedValueOnce(overviewPayload)
      .mockResolvedValueOnce(refreshedPayload)

    render(<OverviewPage />)

    await act(async () => {
      await Promise.resolve()
    })

    expect(screen.getByText('3,203')).toBeInTheDocument()
    expect(getOverviewMock).toHaveBeenCalledTimes(1)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000)
    })

    expect(screen.getByText('12')).toBeInTheDocument()
    expect(screen.getAllByText('100')).toHaveLength(2)
    expect(
      screen.getByText('bf65b507-0884-494b-8e2d-f8c89c81040a'),
    ).toBeInTheDocument()

    expect(getOverviewMock).toHaveBeenCalledTimes(2)
  })
})
