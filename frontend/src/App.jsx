import { useState, useEffect, useRef } from 'react'
import FootballPitch from './components/FootballPitch'
import tactixLogo from './assets/tactix-live.png'

// --- UTILIDADES ---
const formatTime = (fullTimestamp) => {
  if (!fullTimestamp) return "00:00:00";
  try {
    const timePart = fullTimestamp.split(' ')[1]; 
    return timePart ? timePart.split('.')[0] : "00:00:00";
  } catch (e) { return "00:00:00"; }
};

// --- COMPONENTE: EVENT FEED (Blindado contra nulos) ---
const EventFeed = ({ events = [] }) => {

  const formatMatchTime = (minute) => {
    // Seguridad: Si viene null, undefined o texto raro, se convierte a 0
    const minVal = parseInt(minute) || 0;
    const minStr = String(minVal).padStart(2, '0');
    return `('${minStr})`;
  };

  const getEventStyle = (typeName) => {
    // Seguridad: Convertir a string para evitar crash si es null/object
    const typeLower = String(typeName || "evento").toLowerCase();
    
    if (typeLower.includes("goal")) return { icon: "⚽", color: "text-cyber-green font-bold text-base" };
    if (typeLower.includes("yellow")) return { icon: "🟨", color: "text-yellow-400" };
    if (typeLower.includes("red")) return { icon: "🟥", color: "text-red-600 font-bold" };
    if (typeLower.includes("sub")) return { icon: "🔄", color: "text-cyber-neon" };
    if (typeLower.includes("foul")) return { icon: "❌", color: "text-orange-400" };
    if (typeLower.includes("shot")) return { icon: "🎯", color: "text-blue-400" };
    if (typeLower.includes("corner")) return { icon: "🚩", color: "text-purple-400" };
    
    return { icon: "🔹", color: "text-cyber-text" };
  };

  return (
    <div className="bg-cyber-panel/90 border-l-2 border-cyber-neon h-full flex flex-col p-4 rounded-r-lg shadow-lg relative backdrop-blur-md overflow-hidden">
      {/* Decoración superior */}
      <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-cyber-neon to-transparent opacity-50"></div>
      {/* HEADER FIJO: No debe scrollear */}
      <div className="flex justify-between items-center mb-4 shrink-0 z-10">
        <h3 className="text-cyber-neon font-bold tracking-widest text-sm">EVENT FEED</h3>
        <span className="text-[10px] text-gray-500 bg-black/50 px-2 py-1 rounded border border-gray-700">
          Total: {events.length}
        </span>
      </div>
      
      {/* LISTA SCROLLEABLE */}
      {/* Agregamos 'cyber-scrollbar' y aseguramos que ocupe el espacio restante */}
      <ul className="space-y-3 text-sm overflow-y-auto flex-1 pr-2 cyber-scrollbar">
        {events.map((evt, idx) => {
           // ... (toda tu lógica de renderizado de items se mantiene igual) ...
           // Solo asegúrate de que el return del map esté aquí dentro
           const rawType = evt.event_type_name || evt.type_name || evt.Type || "EVENTO";
           const rawPlayer = evt.player_name || evt.Player || "";
           const rawMinute = evt.minute || evt.Time || 0;
           const { icon, color } = getEventStyle(rawType);
           const timeFormatted = formatMatchTime(rawMinute);

           return (
            <li key={idx} className="flex items-center gap-3 border-b border-gray-800 pb-2 hover:bg-white/5 p-1 rounded transition group">
              <span className="text-lg leading-none w-6 text-center group-hover:scale-110 transition-transform" role="img" aria-label={rawType}>
                {icon}
              </span>
              <span className="font-mono text-gray-500 text-xs font-bold min-w-[32px]">
                {timeFormatted}
              </span>
              <div className="flex flex-col flex-1 leading-tight overflow-hidden">
                <div className="flex items-center gap-2">
                  <span className={`uppercase text-xs font-bold tracking-wide truncate ${color}`}>
                    {rawType}
                  </span>
                </div>
                {rawPlayer && (
                  <span className="text-gray-300 text-xs font-semibold mt-0.5 truncate">
                    {rawPlayer}
                  </span>
                )}
              </div>
            </li>
          )
        })}
        
        {/* Mensaje de estado vacío */}
        {events.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full text-gray-600 gap-2 min-h-[100px]">
            <span className="text-2xl opacity-20">📭</span>
            <span className="text-xs italic">Esperando datos del partido...</span>
          </div>
        )}
      </ul>
    </div>
  )
}

// --- COMPONENTE: TICKER ---
const GhostMateTicker = () => (
  <div className="bg-cyber-panel/90 border-l-2 border-cyber-red h-full p-4 rounded-r-lg shadow-lg backdrop-blur-md">
    <h3 className="text-cyber-text font-bold mb-4 tracking-widest text-sm uppercase">GHOST MATE (LIVE)</h3>
    <div className="space-y-4 opacity-50">
      <div className="flex justify-between items-center border-b border-gray-700 pb-2">
        <span className="font-bold">J. ÁLVAREZ</span>
        <div className="text-right"><div className="text-cyber-green font-bold">▲ +3.5</div></div>
      </div>
    </div>
  </div>
)

// --- COMPONENTE: CONTROLES ---
const PlaybackControls = ({ currentTime, isLive, onSeek, onToggleLive }) => (
  <div className="bg-cyber-panel/90 border-t border-cyber-neon p-4 flex items-center gap-6 rounded-t-xl mx-4 backdrop-blur-md">
    <div className="font-mono text-2xl text-cyber-neon w-32 text-center tabular-nums">
      {currentTime}
    </div>
    <div className="flex gap-4 text-cyber-text text-2xl">
      <button className="hover:text-white transition active:scale-95">⏮</button>
      <button 
        onClick={onToggleLive}
        className={`text-xs font-bold px-3 py-1 rounded border flex items-center ${isLive ? 'bg-red-500 border-red-500 text-white' : 'border-cyber-neon text-cyber-neon hover:bg-cyber-neon hover:text-black'} transition`}
      >
        {isLive ? 'EN VIVO' : 'VOLVER AL VIVO'}
      </button>
    </div>
    <div className="flex-1 relative group flex items-center">
      <input 
        type="range" 
        min="0" 
        max="100" 
        defaultValue={100} 
        onChange={(e) => onSeek(e.target.value)}
        className="w-full h-2 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-cyber-neon hover:accent-cyber-green"
      />
    </div>
  </div>
)

// --- APP PRINCIPAL ---
function App() {
  const [history, setHistory] = useState([]); 
  const [playbackIndex, setPlaybackIndex] = useState(-1);
  const [isLive, setIsLive] = useState(true);
  
  const [eventsList, setEventsList] = useState([]);
  const [latestEvent, setLatestEvent] = useState(null);
  const [status, setStatus] = useState("OFFLINE");
  const [playerMap, setPlayerMap] = useState({});

  const ws = useRef(null);
  const MAX_BUFFER = 60000; 

  // 1. Cargar Alineación
  useEffect(() => {
    fetch('http://127.0.0.1:8000/match/test_match/metadata')
      .then(res => res.json())
      .then(data => {
        if (data && !data.error) {
          const map = {};
          const players = Array.isArray(data) ? data : (data.players || Object.values(data));
          if (Array.isArray(players)) {
            players.forEach(p => {
              const pid = String(p.id || p.player_id);
              map[pid] = {
                number: p.number || p.jersey_number || '?',
                name: p.short_name || p.name || 'Unknown',
                team_id: p.team_id
              };
            });
            setPlayerMap(map);
            console.log("✅ Alineación cargada");
          }
        }
      })
      .catch(err => console.warn("Esperando metadata...", err));
  }, []);

  // 2. WebSocket (Con Diagnóstico)
  useEffect(() => {
    console.log("🔌 Conectando WebSocket...");
    ws.current = new WebSocket("ws://127.0.0.1:8000/ws/match/test_match");
    
    ws.current.onopen = () => {
      console.log("✅ WebSocket Conectado");
      setStatus("ONLINE");
    };
    
    ws.current.onclose = () => setStatus("OFFLINE");

    ws.current.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);

        if (msg.type === "tracking") {
          const newFrame = msg.payload;
          setHistory(prev => {
            const newHistory = [...prev, newFrame];
            if (newHistory.length > MAX_BUFFER) newHistory.shift();
            return newHistory;
          });
        } 
        else if (msg.type === "event") {
          const newEvent = msg.payload;
  
          if (newEvent) {
            console.log("📩 Evento Recibido:", newEvent.event_type_name);
    
            setLatestEvent(newEvent);
            setEventsList(prev => {
                const updatedList = [newEvent, ...prev]; 
                return updatedList;
            });
            
            setTimeout(() => setLatestEvent(null), 2000);
          }
        }
      } catch (err) { console.error("Error WS:", err); }
    };

    return () => ws.current?.close();
  }, []);

  // 3. Loop de Visualización
  useEffect(() => {
    if (isLive) {
      setPlaybackIndex(history.length - 1);
    }
  }, [history.length, isLive]);

  const handleSeek = (val) => {
    setIsLive(false);
    const newIndex = Math.floor((val / 100) * (history.length - 1));
    setPlaybackIndex(newIndex);
  };

  const currentFrame = history[playbackIndex] || null;
  const liveFrame = history[history.length - 1] || null;

  return (
    <div className="bg-transparent text-cyber-text h-screen w-screen overflow-hidden flex flex-col font-sans selection:bg-cyber-neon selection:text-black relative">
      
      {/* HEADER */}
      <header className="h-36 flex items-center justify-between px-16 border-b border-gray-800/30 z-20 relative">
        <div className="flex items-center">
          <img src={tactixLogo} alt="Tactix" className="h-44 w-auto object-contain drop-shadow-[0_0_20px_rgba(0,242,255,0.5)]" />
        </div>
        
        <div className="absolute left-1/2 -translate-x-1/2 bottom-4 flex gap-2 h-12 items-end">
          {['ANÁLISIS', 'ESTADÍSTICAS', 'JUGADORES', 'ALERTAS'].map((tab, i) => (
            <button key={tab} className={`px-8 py-2 skew-x-[-20deg] border-b-4 font-bold text-lg transition-all flex items-center hover:scale-105 hover:shadow-neon ${i === 0 ? 'bg-cyber-panel/60 border-cyber-neon text-cyber-neon shadow-[0_0_20px_rgba(0,242,255,0.3)]' : 'border-transparent text-gray-500 hover:text-white hover:bg-gray-800/40'}`}>
              <span className="skew-x-[20deg] block">{tab}</span>
            </button>
          ))}
        </div>

        <div className="flex flex-col items-end justify-center h-full gap-1 z-10">
          <div className="text-5xl font-mono text-cyber-neon font-bold drop-shadow-neon tracking-widest">
            {formatTime(liveFrame?.timestamp)}
          </div>
          <span className={`text-xs px-3 py-0.5 rounded-full font-bold tracking-widest uppercase ${status === 'ONLINE' ? 'bg-cyber-green text-black shadow-neon' : 'bg-red-500 text-white'}`}>
            {status}
          </span>
        </div>
      </header>

      {/* MAIN */}
      <div className="flex-1 grid grid-cols-12 gap-6 p-6 relative z-10 min-h-0 overflow-hidden">
        <div className="col-span-3 flex flex-col gap-6 h-full overflow-hidden">
          <div className="flex-1 min-h-0 flex flex-col relative"> 
            <EventFeed events={eventsList} />
          </div>
          <div className="h-72 shrink-0"><GhostMateTicker /></div>
        </div>

        <div className="col-span-9 flex flex-col relative bg-cyber-panel/30 rounded-xl border border-cyber-neon/30 shadow-2xl overflow-hidden backdrop-blur-sm">
            <div className="absolute top-0 left-0 w-16 h-16 border-t-4 border-l-4 border-cyber-neon/50 rounded-tl-xl pointer-events-none"></div>
            <div className="absolute top-0 right-0 w-16 h-16 border-t-4 border-r-4 border-cyber-neon/50 rounded-tr-xl pointer-events-none"></div>
            
            <div className="flex-1 flex items-center justify-center p-8">
              <FootballPitch 
                matchState={currentFrame} 
                latestEvent={latestEvent}
                playerMap={playerMap} 
                width={1100} 
                height={700} 
              />
            </div>

            <div className="mt-auto relative z-20">
              <PlaybackControls 
                currentTime={formatTime(currentFrame?.timestamp)} 
                isLive={isLive}
                onSeek={handleSeek}
                onToggleLive={() => setIsLive(true)}
              />
            </div>
        </div>
      </div>
    </div>
  )
}

export default App