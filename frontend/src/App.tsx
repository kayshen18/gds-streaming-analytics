import { NavLink, Route, Routes } from 'react-router-dom'

import './App.css'
import AirlineAnalysisPage from './pages/AirlineAnalysisPage'
import OverviewPage from './pages/OverviewPage'
import TimeAnalysisPage from './pages/TimeAnalysisPage'
import PipelinePage from './pages/PipelinePage'




function App() {
  return (
    <div className="app-shell">
      <header>
        <h1>GDS Analytics Dashboard</h1>

        <nav aria-label="Primary navigation">
          <NavLink to="/" end>
            Overview
          </NavLink>
          <NavLink to="/airlines">
            Airline Analysis
          </NavLink>
          <NavLink to="/time">
            Time Analysis
          </NavLink>
          <NavLink to="/pipeline">
            Pipeline &amp; Data Quality
          </NavLink>
        </nav>
      </header>

      <main>
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route
            path="/airlines"
            element={<AirlineAnalysisPage />}
          />
          <Route
            path="/time"
            element={<TimeAnalysisPage />}
          />
          <Route
            path="/pipeline"
            element={<PipelinePage />}
          />
        </Routes>
      </main>
    </div>
  )
}

export default App
