import React, { useState, useEffect, useRef, useMemo } from 'react';
import { Chip, Slider, Button, Dropdown, DropdownTrigger, DropdownMenu, DropdownItem, Switch } from "@nextui-org/react";
import {
  Wifi, WifiOff, Activity, Play, Pause, SkipBack,
  FastForward, Radio, BrainCircuit, TrendingUp, Users, LayoutGrid, BarChart3, Swords
} from 'lucide-react';
import FootballPitch from './components/FootballPitch';
import PitchLayerMenu from './components/pitch/PitchLayerMenu';
import GhostTicker from './components/GhostTicker';
import DynamicBackground from './components/DynamicBackground';
import PlayerGlassCard from './components/PlayerGlassCard';
import TactixLogo from './components/TactixLogo';
import MatchStatsTab from './components/matchstats/MatchStatsTab';
import DofaTab from './components/dofa/DofaTab';
import { useMatchHistory } from './hooks/useMatchHistory';
import { nextPopulatedFrame } from './lib/replayBuffer';
import { eventReplayTarget } from './lib/eventReplay';

// --- UTILIDADES ---
const formatTime = (fullTimestamp) => {
  if (!fullTimestamp) return "00:00:00";
  try {
    const timePart = fullTimestamp.split(' ')[1]; 
    return timePart ? timePart.split('.')[0] : "00:00:00";
  } catch (e) { return "00:00:00"; }
};

// --- COMPONENTE: EVENT FEED ---
const EventFeed = ({ events = [], onEventClick }) => {
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
    <div className="h-full flex flex-col bg-zinc-900/40 backdrop-blur-xl border-r border-white/5">
      {/* HEADER FEED */}
      <div className="p-3 border-b border-white/5 flex items-center justify-between bg-zinc-900/50">
          <div className="flex items-center gap-2">
            <Activity size={14} className="text-primary" />
            <span className="text-[10px] font-bold tracking-widest text-zinc-300 uppercase">Live Events</span>
          </div>
          <span className="text-[9px] bg-zinc-800 px-1.5 py-0.5 rounded text-zinc-400 font-mono">{feedItems.length}</span>
      </div>

      {/* LISTA CON SCROLL */}
      <ul className="flex-1 overflow-y-auto p-2 space-y-0 relative [&::-webkit-scrollbar]:hidden [-ms-overflow-style:none] [scrollbar-width:none]">
        {feedItems.map((item) => {
           const rawType = item.isVirtualGoal ? "GOAL !!!" : (item.event_type_name || item.type_name || "EVENTO");
           const rawPlayer = item.player_name || item.Player || "";
           const rawMinute = item.minute || item.Time || 0;
           const { icon, color } = getEventStyle(item);
           const timeFormatted = formatMatchTime(rawMinute);

           return (
            <li key={item.uniqueKey}
                onClick={() => onEventClick && onEventClick(item)}
                title="Ver la jugada — salta el replay a 15s antes (30s si es gol)"
                className="relative flex items-start gap-3 p-2 rounded-lg transition-all duration-200 hover:bg-white/5 border border-transparent hover:border-white/5 group cursor-pointer"
            >
              {/* Timeline Line (Visual Candy) */}
              <div className="absolute left-[19px] top-8 bottom-[-8px] w-[1px] bg-zinc-800 group-last:hidden" />

              <div className={`mt-0.5 relative z-10 text-base leading-none drop-shadow-md ${item.isVirtualGoal ? 'animate-bounce' : 'group-hover:scale-110 transition-transform'}`}>
                {icon}
              </div>
              
              <div className="flex flex-col flex-1 min-w-0">
                <div className="flex justify-between items-baseline">
                    <span className={`text-[10px] font-bold uppercase tracking-wider truncate ${color}`}>
                        {rawType}
                    </span>
                    <span className="font-mono text-zinc-500 text-[9px] bg-black/30 px-1 rounded flex items-center gap-1">
                        <Play size={8} className="text-primary opacity-0 group-hover:opacity-100 transition-opacity" fill="currentColor" />
                        {timeFormatted}
                    </span>
                </div>
                
                {rawPlayer && (
                    <span className={`text-xs font-medium truncate mt-0.5 ${item.isVirtualGoal ? 'text-white' : 'text-zinc-400 group-hover:text-zinc-200'}`}>
                        {rawPlayer}
                    </span>
                )}
                {!item.isVirtualGoal && item.type_id === 88 && item.event_type_id === 16 && (
                    <div className="mt-1">
                        <Chip size="sm" color="warning" variant="flat" classNames={{base: "h-3 px-0", content: "text-[8px] px-1 uppercase font-bold"}}>Penalty</Chip>
                    </div>
                )}
              </div>
            </li>
          )
        })}
        
        {feedItems.length === 0 && (
          <div className="flex flex-col items-center justify-center h-32 text-zinc-600 gap-2 opacity-50">
            <Activity size={24} />
            <span className="text-[10px] uppercase tracking-wider">Waiting for events...</span>
          </div>
        )}
      </ul>
    </div>
  )
}

// --- COMPONENTE: CONTROLES ---
const PlaybackControls = ({ currentTime, isLive, isPlaying, currentFrame, maxFrame, onSeek, onToggleLive, onTogglePlay, layers, onToggleLayer }) => (
  // CAMBIO: Estilos ajustados para vivir dentro del grid (ancho relativo, borde completo redondeado)
  <div className="w-full max-w-2xl mx-auto flex items-center gap-4 px-6 py-3 mt-4 rounded-2xl border border-white/10 bg-zinc-950/80 backdrop-blur-xl shadow-2xl">
    
    {/* Botones */}
    <div className="flex items-center gap-2">
        {!isLive && (
            <Button 
                isIconOnly radius="full" size="sm" variant="flat"
                className={isPlaying ? "bg-zinc-800 text-warning" : "bg-primary/20 text-primary hover:bg-primary hover:text-white"}
                onPress={onTogglePlay}
            >
                {isPlaying ? <Pause size={16} fill="currentColor" /> : <Play size={16} fill="currentColor" />}
            </Button>
        )}
        <Button 
            size="sm" variant={isLive ? "solid" : "bordered"} 
            color={isLive ? "danger" : "default"}
            className={`font-bold h-8 text-[10px] tracking-wider ${isLive ? 'shadow-[0_0_10px_rgba(243,18,96,0.4)]' : 'text-zinc-500 border-zinc-800'}`}
            startContent={isLive ? <Radio size={12} className="animate-pulse" /> : <FastForward size={12} />}
            onPress={onToggleLive}
        >
            {isLive ? 'LIVE' : 'SYNC'}
        </Button>
    </div>

    {/* Slider y Tiempo */}
    <div className="flex-1 flex flex-col justify-center gap-1">
        <div className="flex justify-between items-end px-1">
             <span className="text-[10px] text-zinc-500 font-mono uppercase tracking-wider">MATCH TIME</span>
             <span className="font-mono text-sm font-bold text-white tabular-nums tracking-widest">{currentTime}</span>
        </div>
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
                track: "bg-zinc-800 border border-white/5 h-1.5",
                thumb: "bg-white shadow-lg w-3 h-3 after:bg-primary"
            }}
        />
        {/* CAMBIO: Se quitaron los textos START / END */}
    </div>

    {/* Menú de capas tácticas (desplegable hacia arriba) */}
    <PitchLayerMenu layers={layers} onToggle={onToggleLayer} />
  </div>
)

// --- APP PRINCIPAL ---
function App() {
  const [activeTab, setActiveTab] = useState('dashboard'); // 'dashboard' | 'matchstats' | 'vision'
  const [isLive, setIsLive] = useState(true);
  const [isPlaying, setIsPlaying] = useState(false);
  const [sliderValue, setSliderValue] = useState(0); 
  const [maxFrame, setMaxFrame] = useState(0);
  const [latestTracking, setLatestTracking] = useState(null);
  const [eventsList, setEventsList] = useState([]);
  const [latestEvent, setLatestEvent] = useState(null);
  const [status, setStatus] = useState("OFFLINE");
  const [playerMap, setPlayerMap] = useState({});
  const [homeTeam, setHomeTeam] = useState('');
  const [awayTeam, setAwayTeam] = useState('');
  const [teamsMap, setTeamsMap] = useState({});
  const ws = useRef(null);
  const lastEventIndex = useRef(-1);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [servicesStatus, setServicesStatus] = useState({});
  const [statsTargetPlayer, setStatsTargetPlayer] = useState(null);

  // Capas tácticas del campo (Voronoi / Pitch Control). Estado en App para compartirlo
  // entre el campo (FootballPitch) y el menú desplegable de la barra (PlaybackControls).
  const [layers, setLayers] = useState({ voronoi: false, pitchControl: false });
  const toggleLayer = (key) => setLayers((prev) => ({ ...prev, [key]: !prev[key] }));

  const handleViewPlayerStats = (player) => {
      setSelectedPlayer(null); // Cerrar modal
      setStatsTargetPlayer({ name: player.name, number: player.number }); 
      setActiveTab('matchstats');
  };

  // --- POLL SERVICES STATUS ---
  useEffect(() => {
      const fetchServices = async () => {
          try {
              const res = await fetch('http://127.0.0.1:8000/admin/services');
              if (res.ok) {
                  const data = await res.json();
                  setServicesStatus(data);
              }
          } catch (e) {
              console.error("Error fetching services:", e);
          }
      };
      fetchServices();
      const interval = setInterval(fetchServices, 5000);
      return () => clearInterval(interval);
  }, []);

  const handleToggleService = async (serviceId, currentStatus) => {
      const action = currentStatus === 'running' ? 'stop' : 'start';
      try {
          // Optimistic update
          setServicesStatus(prev => ({
              ...prev,
              [serviceId]: { ...prev[serviceId], status: action === 'start' ? 'running' : 'stopped' }
          }));
          await fetch(`http://127.0.0.1:8000/admin/services/${serviceId}/${action}`, { method: 'POST' });
      } catch (e) {
          console.error(`Error toggling service ${serviceId}:`, e);
      }
  };

  const handleSelectPlayer = (playerData) => {
      setSelectedPlayer(playerData);
  };

  // Hook de Historial
  const {
    displayData,
    setDisplayData,
    events: historyEvents,
    isLoadingHistory,
    historyRef
  } = useMatchHistory("test_match", isLive, sliderValue, latestTracking, isPlaying);

  // maxFrame y el frame de replay se leen por ref dentro del bucle rAF para no
  // reiniciarlo en cada frame del live ni depender de sliderValue.
  const maxFrameRef = useRef(0);
  const replayFrameRef = useRef(0);
  const isLiveRef = useRef(isLive);
  useEffect(() => { maxFrameRef.current = maxFrame; }, [maxFrame]);
  useEffect(() => { isLiveRef.current = isLive; }, [isLive]);

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
          // maxFrame y (en live) el slider se fijan aquí, batched con setLatestTracking,
          // en vez de en useEffects separados: evita setStates anidados que hacían trepar
          // el contador de updates de React (renders abortados -> tirón "robótico").
          const f = msg.payload?.frame;
          if (f) {
            setMaxFrame(prev => (f > prev ? f : prev));
            if (isLiveRef.current) setSliderValue(f);
          }
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

  // Game Loop (replay): cadencia de reloj real con rAF + acumulador.
  // Avanza un frame poblado cada 100ms (10Hz, igual que el live) saltando los
  // frames vacíos (53%). Un único rAF persistente por sesión de play elimina el
  // jitter del setTimeout encadenado y el churn de recrear el efecto cada frame.
  // Actualiza el estado ~10 veces/seg (como el live) -> sin tormenta de renders;
  // la transición CSS de 0.1s suaviza entre frames.
  useEffect(() => {
    if (isLive || !isPlaying) return;
    const FRAME_MS = 100; // 10Hz
    let raf;
    let last = performance.now();
    let acc = 0;
    replayFrameRef.current = sliderValue; // arranca en el frame donde está el slider

    const step = (now) => {
      const buffer = historyRef.current || {};
      const maxF = maxFrameRef.current;
      acc += now - last;
      last = now;
      if (acc > 500) acc = FRAME_MS; // tras un parón (pestaña oculta) no spamear avances

      if (acc >= FRAME_MS) {
        acc -= FRAME_MS;
        const cur = replayFrameRef.current;
        const next = cur >= maxF ? cur : nextPopulatedFrame(buffer, cur + 1, maxF);
        if (next !== cur) {
          replayFrameRef.current = next;
          const frameData = buffer[next];
          // slider + frame en el MISMO tick -> un solo render batched, sin que
          // el efecto del hook fije el frame en fase passive (lo que anidaba updates).
          setSliderValue(next);
          if (frameData) setDisplayData(frameData);
        }
      }
      raf = requestAnimationFrame(step);
    };

    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLive, isPlaying]);

  // Catch-up
  useEffect(() => {
    if (!isLive && isPlaying && sliderValue >= maxFrame) {
      setIsLive(true);
      setIsPlaying(false);
    }
  }, [sliderValue, maxFrame, isLive, isPlaying]);

  // Handlers
  // isLiveRef se actualiza SÍNCRONO (no solo por efecto) para que el onmessage del
  // WS respete al instante el cambio de modo y no reescriba el slider (carrera que
  // revertía los saltos a replay).
  const handleSeek = (val) => {
    isLiveRef.current = false;
    setIsLive(false);
    setIsPlaying(false);
    setSliderValue(val);
  };
  const handleTogglePlay = () => setIsPlaying(!isPlaying);
  const handleGoLive = () => { isLiveRef.current = true; setIsLive(true); setIsPlaying(false); };

  // Click en un evento del feed -> salta el replay a 15s antes de la jugada
  // (30s si es gol) y reproduce. El mapeo tiempo->frame lo resuelve el backend
  // (frame_at) por el reloj de juego, no por frame_idx (evita asumir fps).
  const handleJumpToEvent = async (evt) => {
    if (!evt) return;
    const { period, seconds } = eventReplayTarget(evt);
    try {
      const res = await fetch(`http://127.0.0.1:8000/match/test_match/frame_at?period=${period}&seconds=${seconds}`);
      const data = await res.json();
      if (data && data.frame_idx != null) {
        isLiveRef.current = false;
        setIsLive(false);
        setSliderValue(data.frame_idx);
        setIsPlaying(true);
      }
    } catch (e) {
      console.warn('Error frame_at:', e);
    }
  };

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
            // Extract team names from API response
            if (data.homeName) setHomeTeam(data.homeName);
            if (data.awayName) setAwayTeam(data.awayName);
            if (data.teams) setTeamsMap(data.teams);
          })
        .catch(e => console.warn("Error meta:",e));
    };
    fetchMetadata();
    const intervalId = setInterval(fetchMetadata, 60000);
    return () => clearInterval(intervalId);
  }, []);

  // Compute live score from events (goal = event_type_id 16 + outcome_id 97)
  const { homeGoals, awayGoals } = useMemo(() => {
    let home = 0, away = 0;
    if (!homeTeam && !awayTeam) return { homeGoals: 0, awayGoals: 0 };

    eventsList.forEach(evt => {
      if (evt.event_type_id === 16 && evt.outcome_id === 97) {
        const evtTeam = evt.team_name || '';
        const hl = homeTeam.toLowerCase();
        const al = awayTeam.toLowerCase();
        const el = evtTeam.toLowerCase();
        if (hl && (el.includes(hl) || hl.includes(el) || el.startsWith(hl.substring(0, 3)))) {
          home++;
        } else if (al && (el.includes(al) || al.includes(el) || el.startsWith(al.substring(0, 3)))) {
          away++;
        }
      }
    });
    return { homeGoals: home, awayGoals: away };
  }, [eventsList, homeTeam, awayTeam]);

  // Lista de Jugadores para el Ticker (Derivada del mapa)
  const tickerPlayers = useMemo(() => {
    return Object.keys(playerMap).map((pid, i) => {
        const p = playerMap[pid];
        return {
            id: pid,
            name: p.name,
            number: p.number,
            team_id: String(p.team_id),
            team_name: teamsMap[String(p.team_id)] || 'UNKNOWN',
            deviation: (Math.random() * 5 - 2).toFixed(1) // Placeholder dinámico
        };
    });
  }, [playerMap, teamsMap]);

  const homeTeamId = useMemo(() => {
    if (!teamsMap || !homeTeam) return null;
    return Object.keys(teamsMap).find(id => teamsMap[id] === homeTeam) || Object.keys(teamsMap)[0];
  }, [teamsMap, homeTeam]);


  // --- RENDER FINAL ---
return (
    <div className="flex flex-col h-screen w-full relative overflow-hidden font-sans selection:bg-primary/30 text-foreground">
      <DynamicBackground />
        {/* HEADER */}
        <header className="h-14 flex items-center justify-between px-4 border-b border-white/5 bg-zinc-950/50 relative z-50">
          <div className="flex items-center gap-3">
            {/* Logo SVG Animado y Vectorial */}
            <TactixLogo size={32} /> 
            <div className="h-4 w-[1px] bg-white/10" />
            {/* Texto de Marca (Tipografía limpia) */}
            <span className="text-lg font-bold tracking-tight text-white">
              TACTIX <span className="text-primary font-mono text-xs align-top">LIVE</span>
            </span>
          </div>
          <div className="flex gap-1 bg-zinc-900/60 rounded-full p-0.5 border border-white/5">
             <button
                onClick={() => setActiveTab('dashboard')}
                className={`px-4 py-1.5 rounded-full text-[10px] font-bold tracking-wide transition-all duration-300 flex items-center gap-2 ${
                  activeTab === 'dashboard'
                    ? 'bg-white/10 text-white border border-white/10 shadow-lg'
                    : 'text-zinc-500 hover:text-zinc-300 border border-transparent'
                }`}
             >
                <LayoutGrid size={12} /> DASHBOARD
             </button>
             <button
                onClick={() => setActiveTab('matchstats')}
                className={`px-4 py-1.5 rounded-full text-[10px] font-bold tracking-wide transition-all duration-300 flex items-center gap-2 ${
                  activeTab === 'matchstats'
                    ? 'bg-primary/20 text-primary border border-primary/20 shadow-[0_0_15px_rgba(0,111,238,0.15)]'
                    : 'text-zinc-500 hover:text-zinc-300 border border-transparent'
                }`}
             >
                <BarChart3 size={12} /> MATCH STATS
             </button>
             <button
                onClick={() => setActiveTab('dofa')}
                className={`px-4 py-1.5 rounded-full text-[10px] font-bold tracking-wide transition-all duration-300 flex items-center gap-2 ${
                  activeTab === 'dofa'
                    ? 'bg-primary/20 text-primary border border-primary/20 shadow-[0_0_15px_rgba(0,111,238,0.15)]'
                    : 'text-zinc-500 hover:text-zinc-300 border border-transparent'
                }`}
             >
                <Swords size={12} /> DOFA
             </button>
             <button
                onClick={() => setActiveTab('vision')}
                className={`px-4 py-1.5 rounded-full text-[10px] font-bold tracking-wide transition-all duration-300 flex items-center gap-2 ${
                  activeTab === 'vision'
                    ? 'bg-white/10 text-white border border-white/10 shadow-lg'
                    : 'text-zinc-500 hover:text-zinc-300 border border-transparent'
                }`}
             >
                <BrainCircuit size={12} /> GEMINI VISION
             </button>
          </div>
          <div className="flex items-center gap-3">
             {isLoadingHistory && <Chip color="warning" variant="dot" size="sm" className="bg-transparent border-none text-warning">BUFFERING</Chip>}
             <Dropdown placement="bottom-end">
                <DropdownTrigger>
                   <div className={`flex items-center gap-2 px-3 py-1 rounded-full bg-black/40 border cursor-pointer hover:bg-black/60 transition-colors ${status === 'ONLINE' ? 'border-success/20' : 'border-danger/20'}`}>
                      {status === 'ONLINE' ? <Wifi size={12} className="text-success" /> : <WifiOff size={12} className="text-danger" />}
                      <span className={`font-mono text-[10px] font-bold ${status === 'ONLINE' ? 'text-success' : 'text-danger'}`}>{status}</span>
                   </div>
                </DropdownTrigger>
                <DropdownMenu aria-label="Dev Services" className="bg-zinc-950 border border-white/10 rounded-xl w-64 shadow-2xl">
                   <DropdownItem key="header" className="h-10 gap-2 pointer-events-none" textValue="Dev Services Header">
                       <p className="font-bold text-[10px] text-zinc-400 uppercase tracking-widest">Microservices Control</p>
                   </DropdownItem>
                   {Object.entries(servicesStatus).map(([id, service]) => (
                       <DropdownItem key={id} textValue={service.name} className="py-3" closeOnSelect={false}>
                           <div className="flex items-center justify-between w-full">
                               <div className="flex items-center gap-3">
                                   <div className={`w-2 h-2 rounded-full ${service.status === 'running' ? 'bg-success shadow-[0_0_8px_rgba(23,201,100,0.8)]' : 'bg-zinc-600'}`} />
                                   <span className="text-xs font-semibold text-white/90">{service.name}</span>
                               </div>
                               <Switch 
                                   size="sm" 
                                   color="success" 
                                   isSelected={service.status === 'running'} 
                                   onValueChange={() => handleToggleService(id, service.status)}
                               />
                           </div>
                       </DropdownItem>
                   ))}
                </DropdownMenu>
             </Dropdown>
          </div>
        </header>

        {/* TAB: DASHBOARD */}
        {activeTab === 'dashboard' && (
          <>
            {/* GHOST TICKER */}
            <GhostTicker 
                players={tickerPlayers} 
                onPlayerClick={handleSelectPlayer} 
                homeTeam={homeTeam}
                awayTeam={awayTeam}
                homeTeamId={homeTeamId}
            />

            {/* MAIN LAYOUT */}
            <main className="flex-1 min-h-0 grid grid-cols-12 gap-0 relative bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-zinc-900/10 via-zinc-950/30 to-zinc-950/50">
                {/* IZQUIERDA: FEED */}
                <aside className="col-span-3 lg:col-span-2 min-h-0 flex flex-col z-20 shadow-[5px_0_30px_rgba(0,0,0,0.3)]">
                    <EventFeed events={eventsList} onEventClick={handleJumpToEvent} />
                </aside>

                {/* DERECHA: CAMPO + CONTROLES */}
                <section className="col-span-9 lg:col-span-10 relative flex flex-col p-4 overflow-hidden">
                    
                    {/* Scoreboard */}
                    <div className="absolute top-4 left-0 right-0 z-30 flex justify-center w-full pointer-events-none">
                        <div className="bg-black/40 backdrop-blur-md px-8 py-2 rounded-2xl border border-white/5 shadow-2xl flex items-center gap-8">
                            <span className="text-sm font-bold text-zinc-400 tracking-wider">{homeTeam ? homeTeam.substring(0, 3).toUpperCase() : 'HOME'}</span>
                            <div className="text-3xl font-mono font-bold text-white tracking-widest drop-shadow-lg">
                            {homeGoals}<span className="text-zinc-600 mx-3 text-2xl">:</span>{awayGoals}
                            </div>
                            <span className="text-sm font-bold text-zinc-400 tracking-wider">{awayTeam ? awayTeam.substring(0, 3).toUpperCase() : 'AWAY'}</span>
                        </div>
                    </div>

                    {/* Área del Campo */}
                    <div className="flex-1 flex items-center justify-center relative min-h-0">
                         <div className="scale-[0.8] lg:scale-[0.9] xl:scale-100 transition-transform duration-500">
                            <FootballPitch
                                matchState={displayData}
                                latestEvent={latestEvent}
                                playerMap={playerMap}
                                width={1150}
                                height={720}
                                onPlayerClick={handleSelectPlayer}
                                layers={layers}
                            />
                         </div>
                    </div>

                    {/* FOOTER CONTROLS */}
                    <div className="w-full relative z-20 pb-2">
                        <PlaybackControls
                            currentTime={formatTime(displayData?.timestamp)}
                            isLive={isLive}
                            isPlaying={isPlaying}
                            currentFrame={sliderValue}
                            maxFrame={maxFrame}
                            onSeek={handleSeek}
                            onToggleLive={handleGoLive}
                            onTogglePlay={handleTogglePlay}
                            layers={layers}
                            onToggleLayer={toggleLayer}
                        />
                    </div>
                </section>
            </main>
          </>
        )}

        {/* TAB: MATCH STATS HUB */}
        {activeTab === 'matchstats' && (
          <MatchStatsTab targetPlayer={statsTargetPlayer} />
        )}

        {/* TAB: DOFA (análisis pre-partido) */}
        {activeTab === 'dofa' && (
          <DofaTab />
        )}

        {/* TAB: GEMINI VISION (placeholder) */}
        {activeTab === 'vision' && (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-center space-y-3">
              <BrainCircuit size={48} className="text-zinc-700 mx-auto" />
              <p className="text-zinc-600 text-sm font-medium">Gemini Vision — Coming Soon</p>
            </div>
          </div>
        )}

        {/* LAYER 2: MODAL OVERLAY (Player Card) */}
        {selectedPlayer && (
            <div className="absolute inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm animate-in fade-in duration-200">
                {/* Click en el fondo para cerrar */}
                <div className="absolute inset-0" onClick={() => setSelectedPlayer(null)} />
                
                {/* La Tarjeta Centrada */}
                <div className="z-10 animate-in zoom-in-95 duration-300 relative">
                    <PlayerGlassCard 
                        player={selectedPlayer} 
                        homeGoals={homeGoals}
                        awayGoals={awayGoals}
                        homeTeamId={homeTeamId}
                        onClose={() => setSelectedPlayer(null)} 
                        onViewStats={() => handleViewPlayerStats(selectedPlayer)}
                    />
                </div>
            </div>
        )}
    </div>
  )
}

export default App;