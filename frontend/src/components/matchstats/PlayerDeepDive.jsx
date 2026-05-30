import React from 'react';
import {
  Target, Zap, Navigation, Shield, Footprints,
  Timer, Ruler, Award, Loader2
} from 'lucide-react';
import PlayerPitchAnalytics from './PlayerPitchAnalytics';

// ============================================================
// PLAYER DEEP DIVE — Pitch + Stats Panel
// ============================================================

// --- STAT TILE ---
const StatTile = ({ icon: Icon, label, value, unit, highlight = false }) => (
  <div
    className={`
      flex flex-col items-center justify-center p-3 rounded-lg border transition-all duration-300
      ${highlight
        ? 'bg-primary/10 border-primary/20 shadow-[0_0_15px_rgba(0,111,238,0.1)]'
        : 'bg-zinc-900/50 border-white/5 hover:border-white/10'
      }
    `}
  >
    <Icon size={12} className={highlight ? 'text-primary' : 'text-zinc-600'} />
    <span className={`font-mono text-lg font-bold mt-1 tabular-nums ${highlight ? 'text-white' : 'text-zinc-300'}`}>
      {value}
      {unit && <span className="text-[9px] text-zinc-500 font-normal ml-0.5">{unit}</span>}
    </span>
    <span className="text-[8px] text-zinc-600 uppercase tracking-wider font-medium mt-0.5">{label}</span>
  </div>
);

// --- CROSSHAIR ICON (inline to avoid import conflicts) ---
const CrosshairIcon = ({ size = 24, className = '' }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
    <circle cx="12" cy="12" r="10" />
    <line x1="22" y1="12" x2="18" y2="12" />
    <line x1="6" y1="12" x2="2" y2="12" />
    <line x1="12" y1="6" x2="12" y2="2" />
    <line x1="12" y1="22" x2="12" y2="18" />
  </svg>
);

// --- PLAYER HEADER ---
const PlayerHeader = ({ player }) => (
  <div className="flex items-center gap-4 mb-4">
    {/* Jersey Number Badge */}
    <div className="relative">
      <div className="w-14 h-14 rounded-xl bg-gradient-to-br from-primary/30 to-primary/5 border border-primary/20 flex items-center justify-center shadow-lg">
        <span className="text-2xl font-mono font-black text-white">{player.info.number}</span>
      </div>
      <div className="absolute -bottom-1 -right-1 px-1.5 py-0.5 bg-primary rounded text-[7px] font-bold text-white uppercase">
        {player.info.position}
      </div>
    </div>
    <div>
      <h3 className="text-lg font-bold text-white leading-tight">{player.info.name}</h3>
      <p className="text-[10px] text-zinc-500 font-medium tracking-wider uppercase mt-0.5">
        {player.stats.minutesPlayed} min · {player.stats.touches} touches
        {player.stats.distanceCovered > 0 && ` · ${player.stats.distanceCovered} km`}
      </p>
    </div>
  </div>
);

// ============================================================
// MAIN COMPONENT
// ============================================================
const PlayerDeepDive = ({ playerData, pitchLoading = false }) => {
  if (!playerData) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-600">
        <p className="text-sm">Select a player to see their analysis</p>
      </div>
    );
  }

  const { stats, pitchData } = playerData;

  return (
    <div className="w-full max-w-5xl mx-auto space-y-4">
      {/* Player Header */}
      <PlayerHeader player={playerData} />

      <div className="grid grid-cols-12 gap-4">
        {/* LEFT: Stats Panel */}
        <div className="col-span-4 space-y-3">
          {/* Attacking */}
          <div className="bg-zinc-900/40 backdrop-blur-sm rounded-xl border border-white/5 p-3">
            <div className="flex items-center gap-2 mb-3 pb-2 border-b border-white/5">
              <Target size={11} className="text-orange-400" />
              <span className="text-[9px] font-bold tracking-widest text-zinc-500 uppercase">Attacking</span>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <StatTile icon={Award} label="Goals" value={stats.goals} highlight={stats.goals > 0} />
              <StatTile icon={Zap} label="Assists" value={stats.assists} highlight={stats.assists > 0} />
              <StatTile icon={Target} label="Shots" value={stats.shots} />
              <StatTile icon={Target} label="On Target" value={stats.shotsOnTarget} />
              {stats.xG > 0 && <StatTile icon={CrosshairIcon} label="xG" value={stats.xG.toFixed(2)} highlight />}
            </div>
          </div>

          {/* Passing */}
          <div className="bg-zinc-900/40 backdrop-blur-sm rounded-xl border border-white/5 p-3">
            <div className="flex items-center gap-2 mb-3 pb-2 border-b border-white/5">
              <Navigation size={11} className="text-blue-400" />
              <span className="text-[9px] font-bold tracking-widest text-zinc-500 uppercase">Passing</span>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <StatTile icon={Navigation} label="Completed" value={stats.passesCompleted} />
              <StatTile icon={Ruler} label="Accuracy" value={`${stats.passAccuracy}%`} />
              <StatTile icon={Zap} label="Key Passes" value={stats.keyPasses} highlight={stats.keyPasses > 2} />
              <StatTile icon={Navigation} label="Progressive" value={stats.progressivePasses} />
            </div>
          </div>

          {/* Defending */}
          <div className="bg-zinc-900/40 backdrop-blur-sm rounded-xl border border-white/5 p-3">
            <div className="flex items-center gap-2 mb-3 pb-2 border-b border-white/5">
              <Shield size={11} className="text-green-400" />
              <span className="text-[9px] font-bold tracking-widest text-zinc-500 uppercase">Defending</span>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <StatTile icon={Shield} label="Tackles" value={stats.tackles} />
              <StatTile icon={Shield} label="Interceptions" value={stats.interceptions} />
              <StatTile icon={Footprints} label="Duels Won" value={stats.duelsWon} />
              <StatTile icon={Footprints} label="Aerial Won" value={stats.aerialDuelsWon} />
            </div>
          </div>
        </div>

        {/* RIGHT: Pitch Visualization */}
        <div className="col-span-8 relative">
          {pitchLoading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-zinc-950/50 backdrop-blur-sm rounded-xl">
              <Loader2 size={24} className="text-primary animate-spin" />
            </div>
          )}
          <PlayerPitchAnalytics pitchData={pitchData} />
        </div>
      </div>
    </div>
  );
};

export default PlayerDeepDive;
