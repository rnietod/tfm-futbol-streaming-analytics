import { useState, useEffect, useRef, useMemo } from 'react'
import FootballPitch from './components/FootballPitch'
import { useMatchHistory } from './hooks/useMatchHistory';
import tactixLogo from './assets/tactix-live.png'

// --- UTILIDADES ---
const formatTime = (fullTimestamp) => {
  if (!fullTimestamp) return "00:00:00";
  try {
    const timePart = fullTimestamp.split(' ')[1]; 
    return timePart ? timePart.split('.')[0] : "00:00:00";
  } catch (e) { return "00:00:00"; }
};

// --- COMPONENTE: EVENT FEED (Con Lógica de Doble Visualización) ---
const EventFeed = ({ events = [] }) => {

  // IDs permitidos (Whitelist)
  const ALLOWED_IDS = [6, 16, 18, 19, 21, 22, 23, 34, 40];

  // --- PROCESAMIENTO DE DATOS (The Magic Step) ---
  const feedItems = useMemo(() => {
    // 1. Primero filtramos por la whitelist general
    const relevantEvents = events.filter(evt => 
      evt.event_type_id && ALLOWED_IDS.includes(evt.event_type_id)
    );

    // 2. APLANAMIENTO Y EXPANSIÓN
    return relevantEvents.flatMap((evt) => {
      const typeId = evt.event_type_id;   // Ej: 16 (Shot)
      const subTypeId = evt.type_id;      // Ej: 88 (Penalty), 26 (Goal Conceded)
      const outcomeId = evt.outcome_id;   // Ej: 97 (Goal)

      // REGLA 1: Ocultar 'Goal Conceded' del Portero
      // (Para evitar duplicidad visual o confusión de quién marcó)
      if (typeId === 23 && subTypeId === 26) {
        return []; 
      }

      const itemsToRender = [];

      // REGLA 2: El evento original (El Tiro, la Falta, la Parada...)
      itemsToRender.push({
        ...evt,
        isVirtualGoal: false, // Marca: Es el evento de la acción
        uniqueKey: `${evt.id}_action` // Key única para React
      });

      // REGLA 3: Si es Tiro y terminó en Gol -> Generar evento Virtual de Gol
      if (typeId === 16 && outcomeId === 97) {
        itemsToRender.push({
          ...evt,
          isVirtualGoal: true, // Marca: Es la celebración del gol
          event_type_name: 'GOAL !!!', // Forzamos el nombre
          uniqueKey: `${evt.id}_goal_celebration`
        });
      }

      return itemsToRender;
    });
  }, [events]);

  const formatMatchTime = (minute) => {
    const minVal = parseInt(minute) || 0;
    const minStr = String(minVal).padStart(2, '0');
    return `('${minStr})`;
  };

  const getEventStyle = (item) => {
    // Si es nuestro item "Virtual" de gol, tiene prioridad absoluta
    if (item.isVirtualGoal) {
      return { icon: "⚽", color: "text-cyber-green font-extrabold text-lg tracking-widest glow-text drop-shadow-[0_0_5px_rgba(0,255,0,0.5)]" };
    }

    const typeId = item.event_type_id;
    const subTypeId = item.type_id;
    const typeLower = String(item.event_type_name || "").toLowerCase();

    switch (typeId) {
      // --- SHOTS (16) ---
      case 16:
        if (subTypeId === 88) return { icon: "👮", color: "text-orange-400 font-bold" }; // Penalty
        if (subTypeId === 62) return { icon: "👟", color: "text-blue-300" };   // Free Kick
        if (subTypeId === 87) return { icon: "💥", color: "text-gray-300" };   // Regular Shot
        return { icon: "🎯", color: "text-blue-400" }; // Fallback Shot

      // --- GOAL KEEPER (23) ---
      case 23:
        if (subTypeId === 32) return { icon: "✋", color: "text-yellow-200" }; // Shot Faced
        if (subTypeId === 33) return { icon: "🧤", color: "text-green-400 font-bold" }; // Shot Saved
        if (subTypeId === 27) return { icon: "🏃", color: "text-purple-300" }; // Keeper Sweeper
        return { icon: "🛡️", color: "text-gray-400" }; // Fallback GK

      // --- OTROS ---
      case 6:  return { icon: "🛡️", color: "text-purple-400" }; // Block
      case 19: return { icon: "🔄", color: "text-cyber-neon" }; // Sub
      
      case 21: // Foul Won
      case 22: // Foul Committed
        if (typeLower.includes("yellow")) return { icon: "🟨", color: "text-yellow-400" };
        if (typeLower.includes("red")) return { icon: "🟥", color: "text-red-600 font-bold" };
        return { icon: "❌", color: "text-orange-400" };

      case 18: // Half Start
      case 34: // Half End
        return { icon: "⏱️", color: "text-gray-500 font-mono" };

      case 40: return { icon: "🚑", color: "text-red-500" }; // Injury
        
      default: return { icon: "🔹", color: "text-cyber-text" };
    }
  };

  return (
    <div className="bg-cyber-panel/90 border-l-2 border-cyber-neon h-full flex flex-col p-4 rounded-r-lg shadow-lg relative backdrop-blur-md overflow-hidden">
      <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-cyber-neon to-transparent opacity-50"></div>
      
      <div className="flex justify-between items-center mb-4 shrink-0 z-10">
        <h3 className="text-cyber-neon font-bold tracking-widest text-sm">EVENT FEED</h3>
        <span className="text-[10px] text-gray-500 bg-black/50 px-2 py-1 rounded border border-gray-700">
          Items: {feedItems.length}
        </span>
      </div>
      
      <ul className="space-y-3 text-sm overflow-y-auto flex-1 pr-2 cyber-scrollbar">
        {feedItems.map((item) => {
           // Usamos el item procesado (que puede ser virtual o real)
           const rawType = item.isVirtualGoal ? "GOAL !!!" : (item.event_type_name || item.type_name || "EVENTO");
           const rawPlayer = item.player_name || item.Player || "";
           const rawMinute = item.minute || item.Time || 0;
           
           const { icon, color } = getEventStyle(item);
           const timeFormatted = formatMatchTime(rawMinute);

           return (
            <li key={item.uniqueKey} className={`flex items-center gap-3 border-b border-gray-800 pb-2 p-1 rounded transition group ${item.isVirtualGoal ? 'bg-cyber-green/5 hover:bg-cyber-green/10' : 'hover:bg-white/5'}`}>
              <span className={`text-lg leading-none w-6 text-center ${item.isVirtualGoal ? 'animate-bounce' : 'group-hover:scale-110'} transition-transform`} role="img">
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
                  {/* Etiqueta extra para Penales si no es el gol virtual */}
                  {!item.isVirtualGoal && item.type_id === 88 && item.event_type_id === 16 && (
                    <span className="text-[9px] border border-orange-500 text-orange-400 px-1 rounded ml-1 opacity-70">PENALTY</span>
                  )}
                </div>
                {rawPlayer && (
                  <span className={`text-xs font-semibold mt-0.5 truncate ${item.isVirtualGoal ? 'text-white' : 'text-gray-300'}`}>
                    {rawPlayer}
                  </span>
                )}
              </div>
            </li>
          )
        })}
        
        {feedItems.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full text-gray-600 gap-2 min-h-[100px]">
            <span className="text-2xl opacity-20">📭</span>
            <span className="text-xs italic">Esperando acciones...</span>
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
const PlaybackControls = ({ currentTime, isLive, isPlaying, currentFrame, maxFrame, onSeek, onToggleLive, onTogglePlay }) => (
  <div className="bg-cyber-panel/90 border-t border-cyber-neon p-4 flex items-center gap-6 rounded-t-xl mx-4 backdrop-blur-md">
    <div className="font-mono text-2xl text-cyber-neon w-32 text-center tabular-nums">
      {currentTime}
    </div>
    <div className="flex gap-4 text-cyber-text text-2xl items-center">
      {/* Botón de Play/Pause solo visible si NO estamos en vivo */}
      {!isLive && (
        <button 
          onClick={onTogglePlay}
          className="hover:text-cyber-green transition active:scale-95 text-3xl flex items-center"
          title={isPlaying ? "Pausar" : "Reproducir"}
        >
          {isPlaying ? "⏸" : "▶️"}
        </button>
      )}

      <button className="hover:text-white transition active:scale-95 text-xl">⏮</button>

      <button 
        onClick={onToggleLive}
        className={`text-xs font-bold px-3 py-1 rounded border flex items-center h-8 ${isLive ? 'bg-red-500 border-red-500 text-white' : 'border-cyber-neon text-cyber-neon hover:bg-cyber-neon hover:text-black'} transition`}
      >
        {isLive ? 'EN VIVO' : 'VOLVER AL VIVO'}
      </button>
    </div>
    <div className="flex-1 relative group flex items-center">
      <input 
        type="range" 
        min="0" 
        max={maxFrame || 100} 
        value={currentFrame || 0}
        onChange={(e) => onSeek(parseInt(e.target.value))}
        className="w-full h-2 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-cyber-neon hover:accent-cyber-green"
      />
      <div className="absolute top-0 right-0 -mt-6 text-xs text-gray-500 font-mono">
        Frame: {currentFrame} / {maxFrame}
      </div>
    </div>
  </div>
)

// --- APP PRINCIPAL ---
function App() {
  // -----------------------------------------------------------
  // 1. ESTADO MÍNIMO NECESARIO
  // -----------------------------------------------------------
  const [isLive, setIsLive] = useState(true);
  const [isPlaying, setIsPlaying] = useState(false);

  const [sliderValue, setSliderValue] = useState(0); 
  const [maxFrame, setMaxFrame] = useState(0);
  
  const [latestTracking, setLatestTracking] = useState(null);

  const [eventsList, setEventsList] = useState([]);
  const [latestEvent, setLatestEvent] = useState(null);
  const [status, setStatus] = useState("OFFLINE");
  const [playerMap, setPlayerMap] = useState({});

  const ws = useRef(null);

  // -----------------------------------------------------------
  // 2. EL CEREBRO: Hook de Historial
  // -----------------------------------------------------------
  const { 
    displayData,   // La data lista para pintar (posiciones, balón...)
    events: historyEvents, // La lista de eventos históricos
    isLoadingHistory,
    history: fullHistory
  } = useMatchHistory("test_match", isLive, sliderValue, latestTracking);

  // -----------------------------------------------------------
  // 3. SINCRONIZACIÓN DE EVENTOS
  // -----------------------------------------------------------
  useEffect(() => {
    if (historyEvents && historyEvents.length > 0) {
      // Fusionamos con precaución para no borrar los que ya tengamos
      setEventsList(prev => {
        const existingIds = new Set(prev.map(e => e.id));
        const newEvents = historyEvents.filter(e => !existingIds.has(e.id));
        return [...newEvents, ...prev].sort((a,b) => (a.minute || 0) - (b.minute || 0));
      });
    }
  }, [historyEvents]);

  // -----------------------------------------------------------
  // 4. WEBSOCKET
  // -----------------------------------------------------------
  useEffect(() => {
    console.log("🔌 Conectando WebSocket...");
    ws.current = new WebSocket("ws://127.0.0.1:8000/ws/match/test_match");
    
    ws.current.onopen = () => setStatus("ONLINE");
    ws.current.onclose = () => setStatus("OFFLINE");

    ws.current.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);

        if (msg.type === "tracking") {
          setLatestTracking(msg.payload);
        } 
        else if (msg.type === "event") {
          const newEvent = msg.payload;
          if (newEvent) {
            console.log("📩 Evento Recibido:", newEvent.event_type_name);
            setLatestEvent(newEvent);
            setEventsList(prev => [newEvent, ...prev]); // Eventos sí los acumulamos visualmente
            setTimeout(() => setLatestEvent(null), 2000);
          }
        }
      } catch (err) { console.error("Error WS:", err); }
    };

    return () => ws.current?.close();
  }, []);

  // -----------------------------------------------------------
  // 5. GESTIÓN DE FRAMES Y SINCRONIZACIÓN
  // -----------------------------------------------------------
  useEffect(() => {
    if (latestTracking?.frame) {
      setMaxFrame(latestTracking.frame);
    }
  }, [latestTracking]);

  // Sync automático en modo Live
  useEffect(() => {
    if (isLive && displayData?.frame) {
      setSliderValue(displayData.frame);
    }
  }, [displayData, isLive]);

  // -----------------------------------------------------------
  // 6. MOTOR DE REPRODUCCIÓN (GAME LOOP)
  // -----------------------------------------------------------
  useEffect(() => {
    let timeoutId;

    if (!isLive && isPlaying) {
      let delay = 40; // Default fallback (25fps)

      if (fullHistory && fullHistory[sliderValue] && fullHistory[sliderValue + 1]) {
        try {
          const rawCurrent = fullHistory[sliderValue].timestamp;
          const rawNext = fullHistory[sliderValue + 1].timestamp;

          if (rawCurrent && rawNext) {
              const tCurrent = new Date(rawCurrent.replace(' ', 'T')).getTime();
              const tNext = new Date(rawNext.replace(' ', 'T')).getTime();
              const diff = tNext - tCurrent;

              // Validamos que la diferencia sea lógica (entre 0ms y 1000ms)
              if (!isNaN(diff) && diff > 0) {
                 delay = Math.min(diff, 1000); 
              }
          }
        } catch (e) {
          console.warn("Error calculando delay de frames:", e);
        }
      }

      timeoutId = setTimeout(() => {
        setSliderValue(prev => {
          if (prev >= maxFrame) return prev;
          return prev + 1;
        });
      }, delay);
    }

    return () => clearTimeout(timeoutId);
  }, [sliderValue, isPlaying, isLive, maxFrame, fullHistory]);

  // -----------------------------------------------------------
  // 7. DETECTOR DE FIN DE REPRODUCCIÓN (Catch-up)
  // -----------------------------------------------------------
  useEffect(() => {
    // Si estamos reproduciendo y alcanzamos al vivo, nos enganchamos
    if (!isLive && isPlaying && sliderValue >= maxFrame) {
      setIsLive(true);
      setIsPlaying(false);
    }
  }, [sliderValue, maxFrame, isLive, isPlaying]);

  // Handlers
  const handleSeek = (val) => {
    setIsLive(false);
    setIsPlaying(false); // Detener reproducción al arrastrar
    setSliderValue(val);
  };

  const handleTogglePlay = () => {
    setIsPlaying(!isPlaying);
  };

  const handleGoLive = () => {
    setIsLive(true);
    setIsPlaying(false);
  };

  // -----------------------------------------------------------
  // 8. Carga de Alineación (Sin cambios)
  // -----------------------------------------------------------
  useEffect(() => {
    fetch('http://127.0.0.1:8000/match/test_match/metadata')
      .then(res => res.json())
      .then(data => {
        if (data && !data.error) {
          const map = {};
          const players = Array.isArray(data) ? data : (data.players || []);
          players.forEach(p => {
             map[String(p.id)] = { 
               number: p.number, name: p.short_name, team_id: p.team_id 
             };
          });
          setPlayerMap(map);
        }
      })
      .catch(e => console.warn(e));
  }, []);

  // -----------------------------------------------------------
  // RENDER
  // -----------------------------------------------------------
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
            {formatTime(displayData?.timestamp)}
          </div>
          <div className="flex gap-2">
            {isLoadingHistory && <span className="text-xs px-2 py-0.5 bg-yellow-500 text-black font-bold rounded animate-pulse">BUFFERING</span>}
            <span className={`text-xs px-3 py-0.5 rounded-full font-bold tracking-widest uppercase ${status === 'ONLINE' ? 'bg-cyber-green text-black shadow-neon' : 'bg-red-500 text-white'}`}>
              {status}
            </span>
          </div>
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
                matchState={displayData} 
                latestEvent={latestEvent}
                playerMap={playerMap} 
                width={1100} 
                height={700} 
              />
            </div>

            <div className="mt-auto relative z-20">
              <PlaybackControls 
                currentTime={formatTime(displayData?.timestamp)} 
                isLive={isLive}
                isPlaying={isPlaying} // Prop Nueva
                currentFrame={sliderValue} 
                maxFrame={maxFrame} 
                onSeek={handleSeek}
                onToggleLive={handleGoLive}
                onTogglePlay={handleTogglePlay} // Prop Nueva
              />
            </div>
        </div>
      </div>
    </div>
  )
}

export default App