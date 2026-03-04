import React, { useState, useEffect, useRef, useMemo } from 'react';
import { Chip, Slider, Button } from "@nextui-org/react";
import {
  Wifi, WifiOff, Activity, Play, Pause, SkipBack,
  FastForward, Radio, BrainCircuit, TrendingUp, Users, LayoutGrid
} from 'lucide-react';
import FootballPitch from './components/FootballPitch';
import GhostTicker from './components/GhostTicker';
import DynamicBackground from './components/DynamicBackground';
import PlayerGlassCard from './components/PlayerGlassCard';
import TactixLogo from './components/TactixLogo';
import { useMatchHistory } from './hooks/useMatchHistory';

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
              className="relative flex items-start gap-3 p-2 rounded-lg transition-all duration-200 hover:bg-white/5 border border-transparent hover:border-white/5 group"
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
                  <span className="font-mono text-zinc-500 text-[9px] bg-black/30 px-1 rounded">
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
                    <Chip size="sm" color="warning" variant="flat" classNames={{ base: "h-3 px-0", content: "text-[8px] px-1 uppercase font-bold" }}>Penalty</Chip>
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
const PlaybackControls = ({ currentTime, isLive, isPlaying, currentFrame, maxFrame, onSeek, onToggleLive, onTogglePlay }) => (
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
  const [matchInfo, setMatchInfo] = useState(null);
  const ws = useRef(null);
  const lastEventIndex = useRef(-1);
  const [selectedPlayer, setSelectedPlayer] = useState(null);

  const handleSelectPlayer = (playerData) => {
    setSelectedPlayer(playerData);
  };

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
        return [...newEvents, ...prev].sort((a, b) => (a.minute || 0) - (b.minute || 0));
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
        } catch (e) { }
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

          if (data.match) {
            setMatchInfo(data.match);
          }

          const players = Array.isArray(data) ? data : (data.players || []);
          const map = {};

          const getTeamName = (tid) => {
            if (!data.match) return "";
            if (tid === data.match.home_team_id) return data.match.home_team_name;
            if (tid === data.match.away_team_id) return data.match.away_team_name;
            return "";
          };

          players.forEach(p => map[String(p.player_id)] = {
            player_id: p.player_id,
            number: p.number,
            name: p.short_name,
            team_id: p.team_id,
            team_name: getTeamName(p.team_id)
          });
          setPlayerMap(map);
        })
        .catch(e => console.warn("Error meta:", e));
    };
    fetchMetadata();
    const intervalId = setInterval(fetchMetadata, 60000);
    return () => clearInterval(intervalId);
  }, []);

  // Lista de Jugadores para el Ticker (Derivada del mapa)
  const tickerPlayers = useMemo(() => {
    return Object.values(playerMap).map((p, i) => ({
      id: p.player_id || i,
      name: p.name,
      number: p.number,
      team_id: p.team_id,
      team_name: p.team_name,
      deviation: (Math.random() * 5 - 2).toFixed(1) // Placeholder dinámico
    }));
  }, [playerMap]);

  // Cálculo Dinámico del Marcador (GOAL = event_type_id 16 + outcome_id 97)
  const matchScore = useMemo(() => {
    let home = 0;
    let away = 0;
    if (!matchInfo) return { home, away };

    eventsList.forEach(evt => {
      if (evt.event_type_id === 16 && evt.outcome_id === 97) {
        if (evt.team_name === matchInfo.home_team_name) {
          home += 1;
        } else if (evt.team_name === matchInfo.away_team_name) {
          away += 1;
        }
      }
    });
    return { home, away };
  }, [eventsList, matchInfo]);


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
        <div className="flex gap-2">
          <button className="px-4 py-1.5 rounded-full text-[10px] font-bold tracking-wide bg-white/5 text-white border border-white/5 hover:bg-white/10 transition-colors flex items-center gap-2">
            <LayoutGrid size={12} /> DASHBOARD
          </button>
          <button className="px-4 py-1.5 rounded-full text-[10px] font-bold tracking-wide text-zinc-400 hover:text-white transition-colors flex items-center gap-2">
            <BrainCircuit size={12} /> GEMINI VISION
          </button>
        </div>
        <div className="flex items-center gap-3">
          {isLoadingHistory && <Chip color="warning" variant="dot" size="sm" className="bg-transparent border-none text-warning">BUFFERING</Chip>}
          <div className={`flex items-center gap-2 px-3 py-1 rounded-full bg-black/40 border ${status === 'ONLINE' ? 'border-success/20' : 'border-danger/20'}`}>
            {status === 'ONLINE' ? <Wifi size={12} className="text-success" /> : <WifiOff size={12} className="text-danger" />}
            <span className={`font-mono text-[10px] font-bold ${status === 'ONLINE' ? 'text-success' : 'text-danger'}`}>{status}</span>
          </div>
        </div>
      </header>

      {/* GHOST TICKER */}
      <GhostTicker
        players={tickerPlayers}
        matchInfo={matchInfo}
        onPlayerClick={handleSelectPlayer}
      />

      {/* MAIN LAYOUT */}
      <main className="flex-1 min-h-0 grid grid-cols-12 gap-0 relative bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-zinc-900/10 via-zinc-950/30 to-zinc-950/50">
        {/* IZQUIERDA: FEED */}
        <aside className="col-span-3 lg:col-span-2 min-h-0 flex flex-col z-20 shadow-[5px_0_30px_rgba(0,0,0,0.3)]">
          <EventFeed events={eventsList} />
        </aside>

        {/* DERECHA: CAMPO + CONTROLES */}
        <section className="col-span-9 lg:col-span-10 relative flex flex-col p-4 overflow-hidden">

          {/* Scoreboard */}
          <div className="absolute top-4 left-0 right-0 z-30 flex justify-center w-full pointer-events-none">
            <div className="bg-black/40 backdrop-blur-md px-8 py-2 rounded-2xl border border-white/5 shadow-2xl flex items-center gap-8">
              <span className="text-sm font-bold text-zinc-400 tracking-wider">
                {matchInfo ? matchInfo.home_team_acronym : 'HOME'}
              </span>
              <div className="text-3xl font-mono font-bold text-white tracking-widest drop-shadow-lg">
                {matchScore.home}<span className="text-zinc-600 mx-3 text-2xl">:</span>{matchScore.away}
              </div>
              <span className="text-sm font-bold text-zinc-400 tracking-wider">
                {matchInfo ? matchInfo.away_team_acronym : 'AWAY'}
              </span>
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
              />
            </div>
          </div>

          {/* FOOTER CONTROLS (Ahora vive dentro de esta columna) */}
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
            />
          </div>
        </section>
      </main>

      {/* LAYER 2: MODAL OVERLAY (Player Card) */}
      {selectedPlayer && (
        <div className="absolute inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm animate-in fade-in duration-200">
          {/* Click en el fondo para cerrar */}
          <div className="absolute inset-0" onClick={() => setSelectedPlayer(null)} />

          {/* La Tarjeta Centrada */}
          <div className="z-10 animate-in zoom-in-95 duration-300 relative">
            <PlayerGlassCard
              player={selectedPlayer}
              onClose={() => setSelectedPlayer(null)}
            />
          </div>
        </div>
      )}
    </div>
  )
}

export default App;