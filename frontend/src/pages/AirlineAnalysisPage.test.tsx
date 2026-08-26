import {
  render,
  screen,
  waitFor,
} from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getAirlines } from '../api/client'
import AirlineAnalysisPage from './AirlineAnalysisPage'


vi.mock('../api/client', () => ({
  getAirlines: vi.fn(),
}))


const getAirlinesMock = vi.mocked(getAirlines)

const airlinesPayload = {
  total_airlines: 198,
  items: [
    {
      airline_code: 'CZ',
      successful_response_records: 167610,
      success_token_count: 291028,
    },
    {
      airline_code: 'MU',
      successful_response_records: 165093,
      success_token_count: 296145,
    },
  ],
}


describe('AirlineAnalysisPage', () => {
  beforeEach(() => {
    getAirlinesMock.mockReset()
  })

  it('loads and displays ranked airlines', async () => {
    getAirlinesMock.mockResolvedValue(airlinesPayload)

    render(<AirlineAnalysisPage />)

    expect(
      screen.getByText('Loading airline rankings...'),
    ).toBeInTheDocument()

    expect(
      await screen.findByRole('table', {
        name: 'Airline rankings',
      }),
    ).toBeInTheDocument()

    expect(getAirlinesMock).toHaveBeenCalledWith(10)

    expect(
      screen.getByText('198 total airlines'),
    ).toBeInTheDocument()

    expect(
      screen.getByRole('columnheader', {
        name: 'Airline',
      }),
    ).toBeInTheDocument()
    expect(
      screen.getByRole('columnheader', {
        name: 'Successful responses',
      }),
    ).toBeInTheDocument()
    expect(
      screen.getByRole('columnheader', {
        name: 'Successful tokens',
      }),
    ).toBeInTheDocument()

    expect(
      screen.getByRole('cell', {
        name: 'CZ',
      }),
    ).toBeInTheDocument()
    expect(screen.getByText('167,610')).toBeInTheDocument()
    expect(screen.getByText('291,028')).toBeInTheDocument()

    expect(
      screen.getByRole('cell', {
        name: 'MU',
      }),
    ).toBeInTheDocument()
    expect(screen.getByText('165,093')).toBeInTheDocument()
    expect(screen.getByText('296,145')).toBeInTheDocument()
  })

  it('shows a safe message when loading fails', async () => {
    getAirlinesMock.mockRejectedValue(
      new Error('database connection was refused'),
    )

    render(<AirlineAnalysisPage />)

    expect(
      await screen.findByRole('alert'),
    ).toHaveTextContent('Unable to load airline rankings.')

    expect(
      screen.queryByText('database connection was refused'),
    ).not.toBeInTheDocument()
  })
  it('reloads rankings when the limit changes', async () => {
    getAirlinesMock.mockResolvedValue(airlinesPayload)

    const user = userEvent.setup()

    render(<AirlineAnalysisPage />)

    await screen.findByRole('table', {
      name: 'Airline rankings',
    })

    await user.selectOptions(
      screen.getByRole('combobox', {
        name: 'Number of airlines',
      }),
      '20',
    )

    await waitFor(() => {
      expect(getAirlinesMock).toHaveBeenLastCalledWith(20)
    })

    expect(getAirlinesMock).toHaveBeenCalledTimes(2)
  })
})
