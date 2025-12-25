import React, { useState, useEffect, useRef, useMemo } from 'react';
import { Chip, Slider, Button } from "@nextui-org/react";
import { 
  Wifi, WifiOff, Activity, Play, Pause, SkipBack, 
  FastForward, Radio, BrainCircuit, TrendingUp, Users // Iconos
} from 'lucide-react';
import FootballPitch from './components/FootballPitch';
import { useMatchHistory } from './hooks/useMatchHistory';
import tactixLogo from './assets/tactix-live.png';

// --- UTILIDADES ---
const formatTime = (fullTimestamp) => {
  if (!fullTimestamp) return "00:00:00";
  try {
    const timePart = fullTimestamp.split(' ')[1]; 
    return timePart ? timePart.split('.')[0] : "00:00:00";
  } catch (e) { return "00:00:00"; }
};

// --- COMPONENTE: EVENT FEED ---
const EventFeed = ({ events = [] }) => {
  const ALLOWED_IDS = [6, 16, 18, 19, 21, 22, 23, 34, 40];

  const feedItems = useMemo(() => {
    const relevantEvents = events.filter(evt => 
      evt.event_type_id && ALLOWED_IDS.includes(evt.event_type_id)
    );
    relevantEvents.sort((a, b) => (b.index || 0) - (a.index || 0));

    return relevantEvents.flatMap((evt) => {
      const typeId = evt.event_type_id;
      const subTypeId = evt.type_id;
      const outcomeId = evt.outcome_id;

      if (typeId === 23 && subTypeId === 26) return []; 

      const itemsToRender = [];
      itemsToRender.push({ ...evt, isVirtualGoal: false, uniqueKey: evt.id });

      if (typeId === 16 && outcomeId === 97) {
        itemsToRender.push({
          ...evt,
          isVirtualGoal: true,
          event_type_name: 'GOAL !!!',
          uniqueKey: `${evt.id}_goal`
        });
      }
      return itemsToRender;
    });
  }, [events]);

  const formatMatchTime = (minute) => {
    const minVal = parseInt(minute) || 0;
    const minStr = String(minVal).padStart(2, '0');
    return `${minStr}'`;
  };

  const getEventStyle = (item) => {
    if (item.isVirtualGoal) {
      return { icon: "⚽", color: "text-success font-extrabold text-lg animate-pulse" };
    }

    const typeId = item.event_type_id;
    const subTypeId = item.type_id;
    const typeLower = String(item.event_type_name || "").toLowerCase();

    switch (typeId) {
      case 16: // Shots
        if (subTypeId === 88) return { icon: "👮", color: "text-warning font-bold" };
        if (subTypeId === 62) return { icon: "👟", color: "text-primary-300" };
        if (subTypeId === 87) return { icon: "💥", color: "text-foreground-300" };
        return { icon: "🎯", color: "text-primary" };

      case 23: // GK
        if (subTypeId === 32) return { icon: "✋", color: "text-warning-200" };
        if (subTypeId === 33) return { icon: "🧤", color: "text-success font-bold" };
        if (subTypeId === 27) return { icon: "🏃", color: "text-secondary" };
        return { icon: "🛡️", color: "text-zinc-500" };

      case 6: return { icon: "🛡️", color: "text-secondary" };
      case 19: return { icon: "🔄", color: "text-primary" };
      
      case 21: 
      case 22: // Fouls
        if (typeLower.includes("yellow")) return { icon: "🟨", color: "text-yellow-500" };
        if (typeLower.includes("red")) return { icon: "🟥", color: "text-danger font-bold" };
        return { icon: "❌", color: "text-orange-400" };

      case 18:
      case 34: return { icon: "⏱️", color: "text-zinc-400 font-mono" };
      case 40: return { icon: "🚑", color: "text-danger" };
        
      default: return { icon: "🔹", color: "text-zinc-500" };
    }
  };

  return (
    <div className="h-full flex flex-col overflow-hidden pointer-events-auto pl-2">
      {/* HEADER INTEGRADO: Texto simple, sin bordes ni fondos */}
      <div className="flex items-center gap-2 mb-2 opacity-80">
          <Activity size={12} className="text-primary animate-pulse" />
          <h3 className="text-[10px] font-bold tracking-[0.2em] uppercase text-zinc-400">
            Live Feed
          </h3>
      </div>

      <ul className="flex-1 overflow-y-auto space-y-2 no-scrollbar pr-2 fade-mask">
        {feedItems.map((item) => {
           const rawType = item.isVirtualGoal ? "GOAL !!!" : (item.event_type_name || item.type_name || "EVENTO");
           const rawPlayer = item.player_name || item.Player || "";
           const rawMinute = item.minute || item.Time || 0;
           const { icon, color } = getEventStyle(item);
           const timeFormatted = formatMatchTime(rawMinute);

           return (
            <li key={item.uniqueKey} className="flex items-start gap-3 group">
              <div className={`mt-0.5 text-base leading-none drop-shadow-md ${item.isVirtualGoal ? 'animate-bounce' : 'group-hover:scale-110 transition-transform'}`}>
                {icon}
              </div>
              
              <div className="flex flex-col flex-1 min-w-0">
                <div className="flex justify-between items-baseline">
                    <span className={`text-[10px] font-bold uppercase tracking-wider truncate ${color}`}>
                        {rawType}
                    </span>
                    <span className="font-mono text-zinc-500 text-[10px]">
                        {timeFormatted}
                    </span>
                </div>
                
                <div className="flex items-center gap-2">
                    {rawPlayer && (
                    <span className={`text-xs font-medium truncate ${item.isVirtualGoal ? 'text-white' : 'text-zinc-300'}`}>
                        {rawPlayer}
                    </span>
                    )}
                    {!item.isVirtualGoal && item.type_id === 88 && item.event_type_id === 16 && (
                        <Chip size="sm" color="warning" variant="flat" classNames={{base: "h-4 px-0", content: "text-[9px] px-1"}}>PENALTY</Chip>
                    )}
                </div>
              </div>
            </li>
          )
        })}
        
        {feedItems.length === 0 && (
          <div className="flex flex-col items-center justify-center h-full text-zinc-600 gap-2">
            <Activity size={32} className="opacity-20" />
            <span className="text-xs">Waiting for data...</span>
          </div>
        )}
      </ul>
    </div>
  )
}

// --- COMPONENTE: GHOST MATE (Restaurado) ---
// Este es el ticker de predicciones/apuestas, separado de la IA.
const GhostMateTicker = () => (
  <div className="tactix-clean p-2 pointer-events-auto h-full flex flex-col justify-center relative group">
    
    {/* Header Integrado */}
    <div className="flex items-center justify-end gap-2 mb-4 z-10 opacity-90">
        <h3 className="text-[10px] font-bold tracking-[0.2em] uppercase text-zinc-400 text-right">
          GHOST MATE <span className="text-danger">🧬 AI</span>
        </h3>
        <div className="w-1.5 h-1.5 rounded-full bg-danger animate-pulse shadow-[0_0_8px_rgba(243,18,96,0.8)]" />
    </div>
    
    <div className="space-y-6 z-10">
      {/* Métrica 1: Julián Álvarez */}
      <div className="flex flex-col items-end gap-1">
        <div className="flex justify-between w-full items-end">
             {/* Sparkline SVG: Area Chart (Positivo Verde / Negativo Rojo) */}
             <div className="w-24 h-10 relative">
                <svg viewBox="0 0 100 40" className="w-full h-full overflow-visible drop-shadow-lg">
                    {/* Eje Central */}
                    <line x1="0" y1="20" x2="100" y2="20" stroke="#52525b" strokeWidth="0.5" strokeDasharray="2 2" />
                    
                    {/* Area Verde (Positiva - Arriba del eje 20) */}
                    <path d="M0 20 C10 15, 20 5, 30 8 C35 10, 40 20, 40 20 Z" fill="rgba(23, 201, 100, 0.5)" stroke="none" />
                    <path d="M60 20 C70 15, 80 2, 90 10 C95 15, 100 20, 100 20 Z" fill="rgba(23, 201, 100, 0.5)" stroke="none" />

                    {/* Area Roja (Negativa - Abajo del eje 20) */}
                    <path d="M40 20 C45 25, 50 35, 55 30 C58 25, 60 20, 60 20 Z" fill="rgba(243, 18, 96, 0.5)" stroke="none" />

                    {/* Línea de Tendencia (Blanca suave) */}
                    <path d="M0 20 C10 15, 20 5, 30 8 C35 10, 40 20, 40 20 C45 25, 50 35, 55 30 C58 25, 60 20, 60 20 C70 15, 80 2, 90 10 C95 15, 100 20, 100 20" 
                          fill="none" stroke="white" strokeWidth="1.5" strokeLinecap="round" opacity="0.8" />
                </svg>
             </div>

             <div className="text-right">
                <div className="text-xs font-bold text-zinc-100">J. ÁLVAREZ</div>
                <div className="text-[9px] text-zinc-500 uppercase tracking-wider">Perf. Deviation</div>
             </div>
        </div>
        <span className="text-success font-mono font-bold text-lg leading-none drop-shadow-md">▲ +3.5</span>
      </div>
      
    </div>
  </div>
)

// --- COMPONENTE: CONTROLES ---
const PlaybackControls = ({ currentTime, isLive, isPlaying, currentFrame, maxFrame, onSeek, onToggleLive, onTogglePlay }) => (
  <div className="tactix-glass px-6 py-3 rounded-xl flex items-center gap-6 w-full max-w-4xl mx-auto pointer-events-auto backdrop-blur-2xl border-t border-white/10">
    
    {/* Time Display */}
    <div className="flex flex-col items-center min-w-[80px]">
        <span className="font-mono text-xl font-bold text-white tabular-nums tracking-wider">{currentTime}</span>
        <span className="text-[9px] text-zinc-500 uppercase tracking-widest">Match Time</span>
    </div>

    {/* Controls */}
    <div className="flex items-center gap-2">
        <Button 
            isIconOnly variant="light" size="sm" 
            className="text-zinc-400 hover:text-white"
        >
            <SkipBack size={18} />
        </Button>
        
        {!isLive && (
            <Button 
                isIconOnly radius="full" 
                className={isPlaying ? "bg-zinc-800 text-warning" : "bg-primary text-white shadow-[0_0_15px_rgba(0,111,238,0.4)]"}
                onPress={onTogglePlay}
            >
                {isPlaying ? <Pause size={18} fill="currentColor" /> : <Play size={18} fill="currentColor" />}
            </Button>
        )}

        <Button 
            size="sm" variant={isLive ? "solid" : "bordered"} 
            color={isLive ? "danger" : "default"}
            className={`font-bold min-w-[100px] ${isLive ? 'shadow-[0_0_15px_rgba(243,18,96,0.4)]' : 'text-zinc-400 border-zinc-700'}`}
            startContent={isLive ? <Radio size={14} className="animate-pulse" /> : <FastForward size={14} />}
            onPress={onToggleLive}
        >
            {isLive ? 'LIVE' : 'GO LIVE'}
        </Button>
    </div>

    {/* Timeline Slider */}
    <div className="flex-1 flex flex-col justify-center gap-1">
        <Slider 
            size="sm"
            color="primary"
            step={1} 
            minValue={0} 
            maxValue={maxFrame || 100} 
            value={currentFrame || 0} 
            onChange={(v) => onSeek(Array.isArray(v) ? v[0] : v)}
            aria-label="Timeline"
            classNames={{
                track: "bg-zinc-800 border border-white/5",
                thumb: "bg-white shadow-lg w-4 h-4 after:bg-primary"
            }}
        />
        <div className="flex justify-between text-[10px] text-zinc-600 font-mono px-1">
            <span>START</span>
            <span>END</span>
        </div>
    </div>
  </div>
)

// --- APP PRINCIPAL ---
function App() {
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
  const lastEventIndex = useRef(-1);

  // Hook de Historial
  const { 
    displayData,   
    events: historyEvents, 
    isLoadingHistory,
    historyRef
  } = useMatchHistory("test_match", isLive, sliderValue, latestTracking);

  // Sincronización Eventos
  useEffect(() => {
    if (historyEvents && historyEvents.length > 0) {
      setEventsList(prev => {
        const existingIds = new Set(prev.map(e => e.id));
        const newEvents = historyEvents.filter(e => !existingIds.has(e.id));
        return [...newEvents, ...prev].sort((a,b) => (a.minute || 0) - (b.minute || 0));
      });
    }
  }, [historyEvents]);

  // WebSocket
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
            if (lastEventIndex.current !== -1 && newEvent.index !== lastEventIndex.current + 1) {
              console.warn(`⚠️ PACKET LOSS: Esperado: ${lastEventIndex.current + 1}, Recibido: ${newEvent.index}`);
            }
            lastEventIndex.current = newEvent.index;
            setLatestEvent(newEvent);
            setEventsList(prev => [newEvent, ...prev]); 
            setTimeout(() => setLatestEvent(null), 2000);
          }
        }
      } catch (err) { console.error("Error WS:", err); }
    };
    return () => ws.current?.close();
  }, []);

  // Sync Frames
  useEffect(() => {
    if (latestTracking?.frame) setMaxFrame(latestTracking.frame);
  }, [latestTracking]);

  useEffect(() => {
    if (isLive && displayData?.frame !== undefined) {
       setSliderValue(prev => (prev !== displayData.frame ? displayData.frame : prev));
    }
  }, [displayData, isLive]);

  // Game Loop
  useEffect(() => {
    let timeoutId;
    if (!isLive && isPlaying) {
      let delay = 40; 
      const buffer = historyRef.current || {}; 
      if (buffer[sliderValue] && buffer[sliderValue + 1]) {
        try {
          const tCurrent = new Date(buffer[sliderValue].timestamp.replace(' ', 'T')).getTime();
          const tNext = new Date(buffer[sliderValue + 1].timestamp.replace(' ', 'T')).getTime();
          const diff = tNext - tCurrent;
          if (!isNaN(diff) && diff > 0) delay = Math.min(diff, 1000); 
        } catch (e) {}
      }
      timeoutId = setTimeout(() => {
        setSliderValue(prev => (prev >= maxFrame ? prev : prev + 1));
      }, delay);
    }
    return () => clearTimeout(timeoutId);;
  }, [sliderValue, isPlaying, isLive, maxFrame, historyRef]);

  // Catch-up
  useEffect(() => {
    if (!isLive && isPlaying && sliderValue >= maxFrame) {
      setIsLive(true);
      setIsPlaying(false);
    }
  }, [sliderValue, maxFrame, isLive, isPlaying]);

  // Handlers
  const handleSeek = (val) => {
    setIsLive(false);
    setIsPlaying(false);
    setSliderValue(val);
  };
  const handleTogglePlay = () => setIsPlaying(!isPlaying);
  const handleGoLive = () => { setIsLive(true); setIsPlaying(false); };

  // Metadata
  useEffect(() => {
    const fetchMetadata = () => {
      fetch('http://127.0.0.1:8000/match/test_match/metadata')
        .then(res => res.json())
        .then(data => {
            if (!data || data.error) return; 
            const players = Array.isArray(data) ? data : (data.players || []);
            const map = {};
            players.forEach(p => map[String(p.player_id)] = { number: p.number, name: p.short_name, team_id: p.team_id });
            setPlayerMap(map);
          })
        .catch(e => console.warn("Error meta:",e));
    };
    fetchMetadata();
    const intervalId = setInterval(fetchMetadata, 60000);
    return () => clearInterval(intervalId);
  }, []);

  // --- RENDER FINAL ---
  return (
    <div className="relative w-full h-screen overflow-hidden bg-transparent font-sans selection:bg-primary/30 text-foreground">
      
      {/* LAYER 0: EL CANVAS (Centrado + Escalado Inteligente) */}
      <div className="absolute inset-0 z-0 flex items-center justify-center bg-black/20 overflow-hidden">
         <div className="flex items-center justify-center w-full h-full transition-transform duration-500 ease-out scale-[0.65] md:scale-[0.85] lg:scale-100 xl:scale-105">
              <div className="relative shadow-2xl border border-white/5 rounded-xl overflow-hidden">
                  <FootballPitch 
                    matchState={displayData} 
                    latestEvent={latestEvent}
                    playerMap={playerMap} 
                    width={1280}   
                    height={720}   
                  />
              </div>
         </div>
      </div>

      {/* LAYER 1: EL OVERLAY (Interfaz) */}
      <div className="absolute inset-0 z-10 flex flex-col p-4 pointer-events-none gap-2">
        
        {/* TOP BAR */}
        <header className="flex justify-between items-center pointer-events-auto h-12">
          {/* Logo & Match Info */}
          <div className="flex items-center gap-3 pl-2">
             <img src={tactixLogo} alt="Tactix" className="h-6 w-auto opacity-80" />
             <div className="h-4 w-[1px] bg-white/10" />
             <span className="text-[10px] text-zinc-500 font-mono uppercase">PRO LEAGUE</span>
          </div>

          {/* NAV TABS (Actualizado: 'GEMINI AI' sustituye a 'Heatmaps') */}
          <div className="flex gap-1 bg-black/20 backdrop-blur-md p-1 rounded-full border border-white/5">
             <button className="px-5 py-1 rounded-full text-[10px] font-bold tracking-wide bg-white/10 text-white shadow-sm flex items-center gap-2">
                <Activity size={12} /> OVERVIEW
             </button>
             <button className="px-5 py-1 rounded-full text-[10px] font-bold tracking-wide text-zinc-400 hover:text-white hover:bg-white/5 transition-colors">
                ANALITYCS
             </button>
             <button className="px-5 py-1 rounded-full text-[10px] font-bold tracking-wide text-zinc-400 hover:text-white hover:bg-white/5 transition-colors">
                PLAYERS
             </button>
             <button className="px-5 py-1 rounded-full text-[10px] font-bold tracking-wide text-zinc-400 hover:text-white hover:bg-white/5 transition-colors group">
                <BrainCircuit size={12} className="group-hover:text-primary" /> GEMINI AI
             </button>
          </div>

          {/* Status */}
          <div className="flex gap-3">
             {isLoadingHistory && (
                 <Chip color="warning" variant="dot" className="tactix-glass border-none text-warning">BUFFERING</Chip>
             )}
             <div className={`tactix-glass px-4 py-2 rounded-xl flex items-center gap-2 border ${status === 'ONLINE' ? 'border-success/20' : 'border-danger/20'}`}>
                {status === 'ONLINE' ? <Wifi size={16} className="text-success" /> : <WifiOff size={16} className="text-danger" />}
                <span className={`font-mono text-xs font-bold ${status === 'ONLINE' ? 'text-success' : 'text-danger'}`}>{status}</span>
             </div>
          </div>
        </header>

        {/* SCOREBOARD FLOTANTE (Bajado de la barra) */}
        {/* Lo colocamos fuera del header, centrado, con mt-2 para separarlo */}
        <div className="flex justify-center pointer-events-none z-20 -mt-2">
            <div className="bg-zinc-950/80 backdrop-blur-xl px-6 py-2 rounded-2xl border border-white/10 shadow-2xl flex items-center gap-6 pointer-events-auto transform scale-90">
                <span className="text-lg font-bold text-zinc-300">HOME</span>
                <div className="text-3xl font-mono font-bold text-white tracking-widest drop-shadow-[0_0_10px_rgba(255,255,255,0.2)]">
                  2<span className="text-zinc-600 mx-2 text-2xl">:</span>1
                </div>
                <span className="text-lg font-bold text-zinc-300">AWAY</span>
            </div>
        </div>

        {/* MAIN GRID (Cuerpo) */}
        <main className="flex-1 grid grid-cols-12 gap-4 min-h-0 pt-2">
          
          {/* IZQUIERDA: Event Feed (Transparente y estrecho) */}
          <aside className="col-span-2 flex flex-col min-h-0">
             <EventFeed events={eventsList} />
          </aside>

          {/* CENTRO: Vacío para el campo */}
          <div className="col-span-8 pointer-events-none"></div>

          {/* DERECHA: Ghost Mate (A la derecha, transparente) */}
          <aside className="col-span-2 flex flex-col min-h-0">
             <GhostMateTicker />
          </aside>

        </main>

        {/* FOOTER CONTROLS */}
        <footer className="flex justify-center items-end pb-2 pointer-events-none">
          <PlaybackControls 
                currentTime={formatTime(displayData?.timestamp)} 
                isLive={isLive}
                isPlaying={isPlaying} 
                currentFrame={sliderValue} 
                maxFrame={maxFrame} 
                onSeek={handleSeek}
                onToggleLive={handleGoLive}
                onTogglePlay={handleTogglePlay}
          />
        </footer>

      </div>
    </div>
  )
}

export default App;