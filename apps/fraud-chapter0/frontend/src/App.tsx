import { Shield } from 'lucide-react'
import FraudEngineLive from './pages/FraudEngineLive'

function App() {
  return (
    <div className="min-h-screen bg-[#0f1117]">
      <nav className="border-b border-gray-800 bg-[#161922]">
        <div className="px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-14">
            <div className="flex items-center gap-2">
              <Shield className="w-5 h-5 text-red-500" />
              <span className="text-sm font-semibold text-white tracking-wide">
                Telecom Fraud Operations — Fraud Engine
              </span>
            </div>
          </div>
        </div>
      </nav>
      <main className="px-4 sm:px-6 lg:px-8 py-6">
        <FraudEngineLive />
      </main>
    </div>
  )
}

export default App
