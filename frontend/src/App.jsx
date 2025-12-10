import { useState, useEffect, useRef } from 'react'
import FootballPitch from './components/FootballPitch'
import tactixLogo from './assets/tactix-live.png'

// --- COMPONENTES UI (Sin cambios) ---
const EventFeed = () => (
  <div className="bg-cyber-panel/90 border-l-2 border-cyber-neon h-full p-4 overflow-y-auto rounded-r-lg shadow-lg relative backdrop-blur-md">
    <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-cyber-neon to-transparent opacity-50"></div>
    <h3 className="text-cyber-neon font-bold mb-4 tracking-widest text-sm">EVENTOS DEL PARTIDO</h3>
    <ul className="space-y-3 text-sm">
      <li className="flex items-center gap-2"><span className="text-cyber-green">⚽ GOAL</span> <span className="text-gray-400">12'</span> Mbappe</li>
      <li className="flex items-center gap-2"><span className="text-yellow-400">🟨 TARJETA</span> <span className="text-gray-400">24'</span> Vinicius</li>
      <li className="flex items-center gap-2"><span className="text-cyber-red">⬇️ CAMBIO</span> <span className="text-gray-400">45'</span> Modric</li>
    </ul>
  </div>
)

const GhostMateTicker = () => (
  <div className="bg-cyber-panel/90 border-l-2 border-cyber-red h-full p-4 rounded-r-lg shadow-lg backdrop-blur-md">
    <h3 className="text-cyber-text font-bold mb-4 tracking-widest text-sm uppercase">Rendimiento (Ghost Mate)</h3>
    <div className="space-y-4">
      <div className="flex justify-between items-center border-b border-gray-700 pb-2">
        <span className="font-bold">J. ÁLVAREZ</span>
        <div className="text-right"><div className="text-cyber-green font-bold">▲ +3.5</div></div>
      </div>
      <div className="flex justify-between items-center border-b border-gray-700 pb-2">
        <span className="font-bold">RODRYGO</span>
        <div className="text-right"><div className="text-cyber-red font-bold">▼ -1.2</div></div>
      </div>
    </div>
  </div>
)

const PlaybackControls = ({ timestamp }) => (
  <div className="bg-cyber-panel/90 border-t border-cyber-neon p-4 flex items-center gap-6 rounded-t-xl mx-4 backdrop-blur-md">
    <div className="font-mono text-2xl text-cyber-neon w-32 text-center">
      {timestamp ? timestamp.split('.')[0] : "00:00:00"}
    </div>
    <div className="flex gap-4 text-cyber-text text-2xl">
      <button>⏮</button><button className="text-cyber-neon">⏸</button><button>⏭</button>
    </div>
    <div className="flex-1 h-2 bg-gray-700 rounded-full relative">
      <div className="h-full bg-cyber-green w-[70%] rounded-full relative shadow-neon">
         <div className="absolute right-0 top-1/2 -translate-y-1/2 w-4 h-4 bg-cyber-neon rounded-full shadow-lg"></div>
      </div>
    </div>
  </div>
)

// --- APP PRINCIPAL ---
function App() {
  const [matchState, setMatchState] = useState(null)
  const [status, setStatus] = useState("OFFLINE")
  const ws = useRef(null)

  useEffect(() => {
    ws.current = new WebSocket("ws://127.0.0.1:8000/ws/match/test_match")
    ws.current.onopen = () => setStatus("ONLINE")
    ws.current.onmessage = (e) => { try { setMatchState(JSON.parse(e.data)) } catch (err) {} }
    ws.current.onclose = () => setStatus("OFFLINE")
    return () => ws.current?.close()
  }, [])

  return (
    // bg-transparent vital para ver el fondo limpio
    <div className="bg-transparent text-cyber-text min-h-screen w-screen overflow-hidden flex flex-col font-sans selection:bg-cyber-neon selection:text-black relative">

      <header className="h-36 flex items-center justify-between px-16 border-b border-gray-800/30 z-20 relative">
        <div className="flex items-center">
          <img 
            src={tactixLogo} 
            alt="Tactix Live Logo" 
            className="h-44 w-auto object-contain drop-shadow-[0_0_20px_rgba(0,242,255,0.5)]" 
          />
        
        </div>
        
        {/* Tabs (Alineados abajo para compensar la altura del logo) */}
        <div className="flex gap-2 h-14 items-end pb-4">
          {['ANÁLISIS', 'ESTADÍSTICAS', 'JUGADORES', 'ALERTAS'].map((tab, i) => (
            <button key={tab} 
              className={`px-10 py-3 skew-x-[-20deg] border-b-4 font-bold text-xl transition-all flex items-center
              ${i === 0 
                ? 'bg-cyber-panel/60 border-cyber-neon text-cyber-neon shadow-[0_0_20px_rgba(0,242,255,0.3)]' 
                : 'border-transparent text-gray-500 hover:text-white hover:bg-gray-800/40'
              }`}>
              <span className="skew-x-[20deg] block">{tab}</span>
            </button>
          ))}
        </div>

        {/* Status y Reloj */}
        <div className="flex flex-col items-end justify-center h-full gap-2">
          <div className="text-5xl font-mono text-cyber-neon font-bold drop-shadow-neon tracking-widest">
            68:45
          </div>
          <span className={`text-sm px-4 py-1 rounded-full font-bold tracking-widest uppercase ${status === 'ONLINE' ? 'bg-cyber-green text-black shadow-neon' : 'bg-red-500/80 text-white'}`}>
            {status}
          </span>
        </div>
      </header>

      {/* MAIN CONTENT (Igual que antes) */}
      <div className="flex-1 grid grid-cols-12 gap-6 p-6 relative z-10">
        <div className="col-span-3 flex flex-col gap-6">
          <div className="flex-1 min-h-0"><EventFeed /></div>
          <div className="h-72"><GhostMateTicker /></div>
        </div>

        <div className="col-span-9 flex flex-col relative bg-cyber-panel/30 rounded-xl border border-cyber-neon/30 shadow-2xl overflow-hidden backdrop-blur-sm">
            <div className="absolute top-0 left-0 w-20 h-20 border-t-4 border-l-4 border-cyber-neon/50 rounded-tl-xl"></div>
            <div className="absolute top-0 right-0 w-20 h-20 border-t-4 border-r-4 border-cyber-neon/50 rounded-tr-xl"></div>
            
            <div className="flex-1 flex items-center justify-center p-8">
              <FootballPitch matchState={matchState} width={1100} height={700} />
            </div>

            <div className="mt-auto relative z-20">
              <PlaybackControls timestamp={matchState?.timestamp} />
            </div>
        </div>
      </div>
    </div>
  )
}

export default App