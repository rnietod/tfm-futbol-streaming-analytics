import { useState, useEffect, useRef } from 'react'
import FootballPitch from './components/FootballPitch'

function App() {
  console.log("📢 El componente App se está renderizando...")
  const [matchState, setMatchState] = useState(null)
  const [status, setStatus] = useState("Desconectado 🔴")
  
  // Usamos useRef para que la conexión persista entre renderizados
  const ws = useRef(null)

  useEffect(() => {
    console.log("🔌 Intentando conectar al WebSocket...")
    // 1. Conectar al WebSocket de tu API Python
    ws.current = new WebSocket("ws://127.0.0.1:8000/ws/match/test_match")

    ws.current.onopen = () => {
      setStatus("🟢 EN VIVO")
      console.log("✅ ¡Conexión WebSocket ABIERTA!") // <--- SI NO SALE ESTO, NO CONECTÓ
    }
    
    ws.current.onerror = (error) => {
      console.error("❌ Error en WebSocket:", error) // <--- ESTO NOS DIRÁ QUÉ PASA
    }

    ws.current.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          setMatchState(data)
        } catch (err) {
          console.error("Error parseando JSON:", err)
        }
      }

    ws.current.onclose = () => setStatus("Desconectado 🔴")

    // Limpieza al cerrar el componente
    return () => {
      if (ws.current) ws.current.close()
    }
  }, [])

  return (
      <div style={{ 
        backgroundColor: "#121212", 
        color: "#eee", 
        minHeight: "100vh", 
        fontFamily: "Segoe UI, sans-serif",
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center'
      }}>
        {/* Header */}
        <header style={{ 
          width: '100%', 
          padding: '1rem 2rem', 
          backgroundColor: '#1e1e1e', 
          borderBottom: '1px solid #333',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '2rem'
        }}>
          <h1 style={{ margin: 0, fontSize: '1.5rem', display: 'flex', alignItems: 'center', gap: '10px' }}>
            ⚽ TFM Analytics Engine <span style={{ fontSize: '0.8rem', padding: '2px 8px', borderRadius: '4px', background: '#333', color: '#888' }}>BETA</span>
          </h1>
          <div style={{ display: 'flex', gap: '20px', alignItems: 'center' }}>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: '0.8rem', color: '#888' }}>SIGNAL STATUS</div>
              <div style={{ fontWeight: 'bold', color: status.includes("🟢") ? '#4caf50' : '#f44336' }}>{status}</div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: '0.8rem', color: '#888' }}>FRAME ID</div>
              <div style={{ fontFamily: 'monospace' }}>{matchState?.frame || '---'}</div>
            </div>
          </div>
        </header>
  
        {/* Main Content */}
        <main style={{ width: '100%', maxWidth: '1000px', padding: '0 20px' }}>
          
          {/* El componente de la cancha */}
          <FootballPitch matchState={matchState} />
  
          {/* Panel de Estadísticas (Placeholder para el futuro) */}
          <div style={{ 
            marginTop: '2rem', 
            display: 'grid', 
            gridTemplateColumns: '1fr 1fr 1fr', 
            gap: '1rem' 
          }}>
            <div style={{ background: '#1e1e1e', padding: '1rem', borderRadius: '8px', border: '1px solid #333' }}>
              <h3 style={{ margin: '0 0 10px 0', color: '#888', fontSize: '0.9rem' }}>POSESIÓN (LIVE)</h3>
              <div style={{ fontSize: '1.5rem', fontWeight: 'bold' }}>52% vs 48%</div>
            </div>
            <div style={{ background: '#1e1e1e', padding: '1rem', borderRadius: '8px', border: '1px solid #333' }}>
              <h3 style={{ margin: '0 0 10px 0', color: '#888', fontSize: '0.9rem' }}>VELOCIDAD BALÓN</h3>
              <div style={{ fontSize: '1.5rem', fontWeight: 'bold' }}>12 m/s</div>
            </div>
            <div style={{ background: '#1e1e1e', padding: '1rem', borderRadius: '8px', border: '1px solid #333' }}>
              <h3 style={{ margin: '0 0 10px 0', color: '#888', fontSize: '0.9rem' }}>XG ACTUAL</h3>
              <div style={{ fontSize: '1.5rem', fontWeight: 'bold' }}>0.45</div>
            </div>
          </div>
        </main>
      </div>
  )
}

export default App