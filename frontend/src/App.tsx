import { NavLink, Route, Routes } from 'react-router-dom'

import './App.css'
import AirlineAnalysisPage from './pages/AirlineAnalysisPage'
import OverviewPage from './pages/OverviewPage'
import TimeAnalysisPage from './pages/TimeAnalysisPage'



function PipelinePage() {
  return (
    <>
      <h2>Pipeline &amp; Data Quality</h2>
      <p>Publication and pipeline status will appear here.</p>
    </>
  )
}


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
