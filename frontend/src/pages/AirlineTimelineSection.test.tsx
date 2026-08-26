import {
  render,
  screen,
  waitFor,
} from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getAirlineTimeline } from '../api/client'
import AirlineTimelineSection from './AirlineTimelineSection'


vi.mock('../api/client', () => ({
  getAirlineTimeline: vi.fn(),
}))


vi.mock('../charts/AirlineTimelineChart', () => ({
  default: ({
    airlineCode,
  }: {
    airlineCode: string
  }) => (
    <div
      role="img"
      aria-label={`${airlineCode} hourly timeline`}
    />
  ),
}))


const getAirlineTimelineMock = vi.mocked(
  getAirlineTimeline,
)

const airlines = [
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
]


describe('AirlineTimelineSection', () => {
  beforeEach(() => {
    getAirlineTimelineMock.mockReset()

    getAirlineTimelineMock.mockImplementation(
      async (airlineCode) => ({
        airline_code: airlineCode.trim().toUpperCase(),
        items: [],
      }),
    )
  })

  it('loads the default airline and changes selection', async () => {
    const user = userEvent.setup()

    render(
      <AirlineTimelineSection airlines={airlines} />,
    )

    expect(
      await screen.findByRole('img', {
        name: 'CZ hourly timeline',
      }),
    ).toBeInTheDocument()

    expect(getAirlineTimelineMock).toHaveBeenCalledWith('CZ')

    await user.selectOptions(
      screen.getByRole('combobox', {
        name: 'Airline timeline',
      }),
      'MU',
    )

    await waitFor(() => {
      expect(
        getAirlineTimelineMock,
      ).toHaveBeenLastCalledWith('MU')
    })

    expect(
      await screen.findByRole('img', {
        name: 'MU hourly timeline',
      }),
    ).toBeInTheDocument()
  })
})
