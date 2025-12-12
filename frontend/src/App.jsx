import { useState, useEffect, useRef, useMemo } from 'react'
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

// --- COMPONENTES UI ---
const EventFeed = ({ events = [] }) => {
  // Función auxiliar para encontrar valores sin importar el nombre de la columna en el CSV
  const getSafeValue = (obj, keys, defaultVal = "") => {
    if (!obj) return defaultVal;
    for (const key of keys) {
      if (obj[key] !== undefined && obj[key] !== null) return obj[key];
    }
    return defaultVal;
  };

  return (
    <div className="bg-cyber-panel/90 border-l-2 border-cyber-neon h-full p-4 overflow-y-auto rounded-r-lg shadow-lg relative backdrop-blur-md">
      <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-cyber-neon to-transparent opacity-50"></div>
      
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-cyber-neon font-bold tracking-widest text-sm">EVENT FEED</h3>
        <span className="text-[10px] text-gray-500 bg-black/50 px-2 py-1 rounded">
          {events.length}
        </span>
      </div>
      
      <ul className="space-y-3 text-sm">
        {events.map((evt, idx) => {
          // 1. INTENTAR CAZAR EL TIPO DE EVENTO
          // Buscamos: 'type_name', 'Type', 'Event Type', 'Event', 'type'
          const rawType = getSafeValue(evt, ['type_name', 'Type', 'type', 'Event Type', 'Event', 'event_type'], "EVENTO");
          
          // 2. INTENTAR CAZAR EL JUGADOR
          // Buscamos: 'player_name', 'Player', 'Player Name', 'player', 'From', 'Actor'
          const rawPlayer = getSafeValue(evt, ['player_name', 'Player', 'Player Name', 'player', 'From'], "");
          
          // 3. INTENTAR CAZAR EL EQUIPO
          const rawTeam = getSafeValue(evt, ['team_name', 'Team', 'Team Name', 'team'], "");
          
          // 4. TIEMPO
          const rawTime = getSafeValue(evt, ['display_time', 'timestamp', 'Time', 'time', 'game_time'], "00:00");

          // Iconos y colores según el texto encontrado
          const typeLower = String(rawType).toLowerCase();
          let icon = "🔹";
          let colorClass = "text-cyber-text";

          if (typeLower.includes("goal")) { icon = "⚽"; colorClass = "text-cyber-green font-bold text-base"; }
          else if (typeLower.includes("yellow")) { icon = "🟨"; colorClass = "text-yellow-400"; }
          else if (typeLower.includes("red")) { icon = "🟥"; colorClass = "text-red-600 font-bold"; }
          else if (typeLower.includes("sub")) { icon = "🔄"; colorClass = "text-cyber-neon"; }
          else if (typeLower.includes("foul")) { icon = "❌"; colorClass = "text-orange-400"; }
          else if (typeLower.includes("shot")) { icon = "🎯"; colorClass = "text-blue-400"; }
          else if (typeLower.includes("corner")) { icon = "🚩"; colorClass = "text-purple-400"; }

          return (
            <li key={idx} className="flex items-start gap-3 border-b border-gray-800 pb-2 animate-pulse-once hover:bg-white/5 p-1 rounded transition">
              <span className="font-mono text-gray-500 text-xs mt-1 min-w-[40px]">
                {rawTime.toString().split('.')[0]}
              </span>
              <div className="flex-1">
                <div className={`flex items-center gap-2 ${colorClass}`}>
                  <span>{icon}</span>
                  <span className="uppercase text-xs font-bold">{rawType}</span>
                </div>
                {rawPlayer && (
                  <div className="text-gray-300 text-xs font-bold mt-0.5">{rawPlayer}</div>
                )}
                {rawTeam && (
                  <div className="text-gray-500 text-[10px]">{rawTeam}</div>
                )}
              </div>
            </li>
          )
        })}
        
        {events.length === 0 && (
          <div className="flex flex-col items-center justify-center h-32 text-gray-600 gap-2">
            <span className="text-2xl opacity-20">📭</span>
            <span className="text-xs italic">Esperando datos del partido...</span>
          </div>
        )}
      </ul>
    </div>
  )
}

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
        // Si no estamos en vivo, el valor debería reflejar la posición relativa
        // Nota: Para un slider real perfecto necesitamos saber el "total" de frames esperados, 
        // pero para DVR live, 100% es "ahora".
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
  
  // NUEVO: Mapa de Jugadores (ID -> Datos)
  const [playerMap, setPlayerMap] = useState({});

  const ws = useRef(null);

  // AUMENTADO: 60,000 frames (aprox 100 min de partido a 10Hz)
  const MAX_BUFFER = 60000; 

  // 1. Cargar Alineación al inicio
  useEffect(() => {
    fetch('http://127.0.0.1:8000/match/test_match/metadata')
      .then(res => res.json())
      .then(data => {
        if (data && !data.error) {
          // Convertir lista o objeto raw en un Mapa rápido por ID
          const map = {};
          // Detectar estructura (lista plana, objeto 'players', o diccionario de IDs)
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
            console.log("✅ Alineación cargada:", Object.keys(map).length, "jugadores");
          }
        }
      })
      .catch(err => console.warn("Esperando metadata...", err));
  }, []);

  // 2. WebSocket
  useEffect(() => {
    ws.current = new WebSocket("ws://127.0.0.1:8000/ws/match/test_match");
    ws.current.onopen = () => setStatus("ONLINE");
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
            setLatestEvent(newEvent);
            setEventsList(prev => [newEvent, ...prev].slice(0, 50));
            setTimeout(() => setLatestEvent(null), 2000);
          }
        }
      } catch (err) { console.error(err); }
    };

    return () => ws.current?.close();
  }, []);

  // 3. Loop de visualización (Sincroniza UI con el buffer)
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
    <div className="bg-transparent text-cyber-text min-h-screen w-screen overflow-hidden flex flex-col font-sans selection:bg-cyber-neon selection:text-black relative">
      
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
      <div className="flex-1 grid grid-cols-12 gap-6 p-6 relative z-10">
        <div className="col-span-3 flex flex-col gap-6">
          <div className="flex-1 min-h-0">
             <EventFeed events={eventsList} />
          </div>
          <div className="h-72"><GhostMateTicker /></div>
        </div>

        <div className="col-span-9 flex flex-col relative bg-cyber-panel/30 rounded-xl border border-cyber-neon/30 shadow-2xl overflow-hidden backdrop-blur-sm">
            <div className="absolute top-0 left-0 w-16 h-16 border-t-4 border-l-4 border-cyber-neon/50 rounded-tl-xl pointer-events-none"></div>
            <div className="absolute top-0 right-0 w-16 h-16 border-t-4 border-r-4 border-cyber-neon/50 rounded-tr-xl pointer-events-none"></div>
            
            <div className="flex-1 flex items-center justify-center p-8">
              <FootballPitch 
                matchState={currentFrame} 
                latestEvent={latestEvent}
                playerMap={playerMap} // <--- PASAMOS EL MAPA AL PINTOR
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