import { Routes, Route, NavLink } from 'react-router-dom'
import { Shield, Briefcase, Gauge, UserSearch, Zap } from 'lucide-react'
import CaseQueue from './pages/CaseQueue'
import CaseDetail from './pages/CaseDetail'
import Dashboard from './pages/Dashboard'
import Analytics from './pages/Analytics'
import FraudEngineLive from './pages/FraudEngineLive'

function App() {
  return (
    <div className="min-h-screen bg-[#0f1117]">
      {/* Top nav */}
      <nav className="border-b border-gray-800 bg-[#161922]">
        <div className="px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-14">
            <div className="flex items-center gap-2">
              <Shield className="w-5 h-5 text-red-500" />
              <span className="text-sm font-semibold text-white tracking-wide">
                Telecom Fraud Operations
              </span>
            </div>
            <div className="flex gap-1">
              <NavLink
                to="/engine"
                className={({ isActive }) =>
                  `flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition ${
                    isActive
                      ? 'bg-gray-700/60 text-white'
                      : 'text-gray-400 hover:text-white hover:bg-gray-800'
                  }`
                }
              >
                <Zap className="w-3.5 h-3.5" />
                Fraud Engine
              </NavLink>
              <NavLink
                to="/"
                end
                className={({ isActive }) =>
                  `flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition ${
                    isActive
                      ? 'bg-gray-700/60 text-white'
                      : 'text-gray-400 hover:text-white hover:bg-gray-800'
                  }`
                }
              >
                <Briefcase className="w-3.5 h-3.5" />
                Executive
              </NavLink>
              <NavLink
                to="/management"
                className={({ isActive }) =>
                  `flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition ${
                    isActive
                      ? 'bg-gray-700/60 text-white'
                      : 'text-gray-400 hover:text-white hover:bg-gray-800'
                  }`
                }
              >
                <Gauge className="w-3.5 h-3.5" />
                Management
              </NavLink>
              <NavLink
                to="/analyst"
                className={({ isActive }) =>
                  `flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition ${
                    isActive
                      ? 'bg-gray-700/60 text-white'
                      : 'text-gray-400 hover:text-white hover:bg-gray-800'
                  }`
                }
              >
                <UserSearch className="w-3.5 h-3.5" />
                Analyst
              </NavLink>
            </div>
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main className="px-4 sm:px-6 lg:px-8 py-6">
        <Routes>
          <Route path="/" element={<Analytics />} />
          <Route path="/engine" element={<FraudEngineLive />} />
          <Route path="/management" element={<Dashboard />} />
          <Route path="/analyst" element={<CaseQueue />} />
          <Route path="/cases/:id" element={<CaseDetail />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
