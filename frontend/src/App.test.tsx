import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'

import App from './App'


function renderApp(path = '/') {
  render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  )
}


describe('App', () => {
  it('renders the dashboard title and primary navigation', () => {
    renderApp()

    expect(
      screen.getByRole('heading', {
        name: 'GDS Analytics Dashboard',
        level: 1,
      }),
    ).toBeInTheDocument()

    const navigation = screen.getByRole('navigation', {
      name: 'Primary navigation',
    })

    expect(
      within(navigation).getByRole('link', {
        name: 'Overview',
      }),
    ).toBeInTheDocument()

    expect(
      within(navigation).getByRole('link', {
        name: 'Airline Analysis',
      }),
    ).toBeInTheDocument()

    expect(
      within(navigation).getByRole('link', {
        name: 'Time Analysis',
      }),
    ).toBeInTheDocument()

    expect(
      within(navigation).getByRole('link', {
        name: 'Pipeline & Data Quality',
      }),
    ).toBeInTheDocument()
  })

  it.each([
    ['/', 'Overview'],
    ['/airlines', 'Airline Analysis'],
    ['/time', 'Time Analysis'],
    ['/pipeline', 'Pipeline & Data Quality'],
  ])(
    'renders route %s with heading %s',
    (path, heading) => {
      renderApp(path)

      expect(
        screen.getByRole('heading', {
          name: heading,
          level: 2,
        }),
      ).toBeInTheDocument()
    },
  )

})
