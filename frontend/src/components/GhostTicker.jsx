import React, { useState, useMemo } from 'react';
import { TrendingUp, TrendingDown, Minus, Users, Shield } from 'lucide-react';

const GhostTicker = ({ players = [] }) => {
  const [filter, setFilter] = useState('all'); // 'all', 'home', 'away'
  const [isPaused, setIsPaused] = useState(false);

  // 1. Filtrado de jugadores
  const filteredPlayers = useMemo(() => {
    if (filter === 'all') return players;
    // Ajusta la lógica de team_id según tus datos reales (ej: 275 vs 262)
    return players.filter(p => {
        if (filter === 'home') return p.team_id === 275; 
        if (filter === 'away') return p.team_id !== 275; 
        return true;
    });
  }, [players, filter]);

  // 2. Lista infinita
  // Duplicamos x10 para asegurar que cubra pantallas grandes
  const marqueeList = useMemo(() => {
      const base = filteredPlayers.length > 0 ? filteredPlayers : [{ id: 'loading', name: 'ESPERANDO DATOS...', number: '--', deviation: 0 }];
      return [...base, ...base, ...base, ...base, ...base, ...base, ...base, ...base];
  }, [filteredPlayers]);

  const getTrendIcon = (val) => {
    if (val > 0.5) return <TrendingUp size={14} className="text-success" />;
    if (val < -0.5) return <TrendingDown size={14} className="text-danger" />;
    return <Minus size={14} className="text-zinc-500" />;
  };

  return (
    // CAMBIO: Opacidad bajada a /70 para que se intuya el fondo del estadio
    <div className="w-full h-12 bg-zinc-950/70 border-y border-white/5 flex items-center overflow-hidden relative backdrop-blur-md z-30 shadow-lg">
      
      {/* Definición de Animación CSS en línea para no depender de index.css externo */}
      <style>{`
        @keyframes ticker-scroll {
          0% { transform: translateX(0); }
          100% { transform: translateX(-50%); }
        }
      `}</style>

      {/* SECCIÓN IZQUIERDA: CONTROLES DE EQUIPO */}
      <div className="absolute left-0 h-full px-4 bg-zinc-950/90 z-20 flex items-center gap-4 border-r border-white/10 shadow-[5px_0_30px_rgba(0,0,0,0.8)]">
        <div className="flex items-center gap-2">
            <div className="w-1.5 h-1.5 rounded-full bg-cyber-neon animate-pulse" />
            <span className="text-[10px] font-bold tracking-[0.2em] text-zinc-400 hidden md:block">
            GHOST<span className="text-cyber-neon">AI</span>
            </span>
        </div>

        <div className="flex bg-zinc-900 rounded-lg p-0.5 border border-white/5">
            <button 
                onClick={() => setFilter('home')}
                className={`px-3 py-1 rounded-md text-[10px] font-bold transition-all flex items-center gap-1 ${filter === 'home' ? 'bg-zinc-700 text-white shadow-sm' : 'text-zinc-500 hover:text-zinc-300'}`}
            >
                <Shield size={10} /> HOME
            </button>
            <button 
                onClick={() => setFilter('away')}
                className={`px-3 py-1 rounded-md text-[10px] font-bold transition-all flex items-center gap-1 ${filter === 'away' ? 'bg-zinc-700 text-white shadow-sm' : 'text-zinc-500 hover:text-zinc-300'}`}
            >
                <Users size={10} /> AWAY
            </button>
        </div>
      </div>

      {/* MARQUEE ANIMADO (CSS PURO) */}
      <div 
        className="flex w-full items-center pl-60 mask-linear-fade"
        onMouseEnter={() => setIsPaused(true)}
        onMouseLeave={() => setIsPaused(false)}
      >
        <div 
          className="flex gap-12 whitespace-nowrap"
          style={{
            animationName: 'ticker-scroll',
            animationDuration: `${Math.max(60, marqueeList.length * 8)}s`, // Velocidad dinámica
            animationTimingFunction: 'linear',
            animationIterationCount: 'infinite',
            animationPlayState: isPaused ? 'paused' : 'running', // <--- PAUSA SUAVE AQUÍ
            willChange: 'transform'
          }}
        >
          {marqueeList.map((player, idx) => {
             const deviation = player.deviation !== undefined ? player.deviation : 0;
             const isPositive = deviation > 0;

             return (
              <div key={`${player.id}-${idx}`} className="flex items-center gap-3 group cursor-pointer select-none hover:opacity-100 opacity-80 transition-opacity">
                <div className="flex flex-col items-end leading-none">
                    <span className="text-[9px] font-mono text-zinc-600">#{player.number || '00'}</span>
                    <span className="text-xs font-bold text-zinc-400 group-hover:text-white transition-colors">
                        {player.name ? player.name.toUpperCase() : "JUGADOR"}
                    </span>
                </div>
                
                <div className={`flex items-center gap-1.5 px-2 py-1 rounded bg-black/40 border border-white/5 group-hover:border-white/20 transition-colors`}>
                    {getTrendIcon(deviation)}
                    <span className={`text-[10px] font-mono font-bold ${isPositive ? 'text-success' : 'text-danger'}`}>
                        {deviation > 0 ? '+' : ''}{deviation}%
                    </span>
                </div>
                
                <div className="w-1 h-1 rounded-full bg-zinc-800 ml-6 opacity-50" />
              </div>
            )
          })}
        </div>
      </div>

      <div className="absolute right-0 top-0 h-full w-32 bg-gradient-to-l from-zinc-950 to-transparent pointer-events-none z-10" />
    </div>
  );
};

export default GhostTicker;