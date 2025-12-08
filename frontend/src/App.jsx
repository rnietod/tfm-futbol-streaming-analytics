import { useState, useEffect, useRef } from 'react'
import FootballPitch from './components/FootballPitch'

function App() {
  const [matchState, setMatchState] = useState(null)
  const [status, setStatus] = useState("Desconectado 🔴")
  const ws = useRef(null)

  useEffect(() => {
    ws.current = new WebSocket("ws://127.0.0.1:8000/ws/match/test_match")

    ws.current.onopen = () => {
      setStatus("🟢 LIVE DATA")
    }

    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        setMatchState(data)
      } catch (err) {
        console.error(err)
      }
    }

    ws.current.onclose = () => setStatus("🔴 OFF AIR")

    return () => {
      if (ws.current) ws.current.close()
    }
  }, [])

  // Formatear periodo
  const getPeriodLabel = (p) => {
    if (!p) return "PRE";
    if (p === 1) return "1T";
    if (p === 2) return "2T";
    if (p === 3) return "ET"; // Tiempo extra
    return p;
  }

  // Estilo del marcador
  const scoreboardStyle = {
    background: '#222',
    border: '2px solid #444',
    borderRadius: '10px',
    padding: '10px 30px',
    display: 'flex',
    gap: '40px',
    alignItems: 'center',
    marginBottom: '20px',
    boxShadow: '0 4px 10px rgba(0,0,0,0.5)'
  }

  const timeStyle = {
    fontSize: '2.5rem',
    fontWeight: 'bold',
    fontFamily: 'monospace',
    color: '#ffeb3b', // Amarillo reloj digital
    letterSpacing: '2px'
  }

  return (
    <div style={{ 
      backgroundColor: "#121212", 
      color: "#eee", 
      minHeight: "100vh", 
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      paddingTop: '2rem'
    }}>
      
      {/* --- MARCADOR SUPERIOR --- */}
      <div style={scoreboardStyle}>
        
        {/* Equipo Local */}
        <div style={{textAlign: 'center'}}>
          <h2 style={{margin:0, color: '#d32f2f'}}>HOME</h2>
          <span style={{fontSize: '2rem', fontWeight: 'bold'}}>3</span>
        </div>

        {/* Reloj Central */}
        <div style={{textAlign: 'center'}}>
          <div style={{color: '#888', fontSize: '0.9rem', marginBottom: '5px'}}>
            {status} | {getPeriodLabel(matchState?.period)}
          </div>
          <div style={timeStyle}>
            {matchState?.timestamp ? matchState.timestamp.split('.')[0] : "00:00:00"}
          </div>
          <div style={{fontSize: '0.8rem', color: '#666', marginTop: '5px'}}>
            FRAME: {matchState?.frame || 0}
          </div>
        </div>

        {/* Equipo Visitante */}
        <div style={{textAlign: 'center'}}>
          <h2 style={{margin:0, color: '#1976d2'}}>AWAY</h2>
          <span style={{fontSize: '2rem', fontWeight: 'bold'}}>1</span>
        </div>

      </div>

      {/* --- CANCHA --- */}
      <main style={{ width: '100%', maxWidth: '1000px', padding: '0 20px' }}>
        <FootballPitch matchState={matchState} />
      </main>

    </div>
  )
}

export default App