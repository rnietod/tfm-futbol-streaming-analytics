import React, { useState, useEffect } from 'react';
import { Network, Target, Thermometer, X, Award, Navigation, Shield, HelpCircle, Flame, Star } from 'lucide-react';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, ResponsiveContainer } from 'recharts';
import PassingNetworkPitch from './PassingNetworkPitch';
import ShotMapViz from './ShotMapViz';
import MomentumChart from './MomentumChart';
import AccuratePassesViz from './AccuratePassesViz';
import { useShotMap, useMomentum } from '../../hooks/useMatchStats';

// ============================================================
// TEAM COMPARISON — Desktop-first layout
//
//  ┌─────────────────────────────────────────────────────┐
//  │  Team Toggle (full width, top)                      │
//  ├──────┬──────────────┬──────────────────────────────┤
//  │ Btn  │  Pitch 40%   │  Stats panel 60%             │
//  │ col  │  (vertical)  │  [Section tabs + stat rows]  │
//  └──────┴──────────────┴──────────────────────────────┘
// ============================================================

// ── ANALYSIS MODES ──────────────────────────────────────────
const ANALYSES = [
  { id: 'passing',  label: 'Pass Net',  icon: Network     },
  { id: 'shots',    label: 'Shots',     icon: Target      },
  { id: 'pressure', label: 'Pressure',  icon: Thermometer },
];

// Vertical icon+label buttons on the left of the pitch
const AnalysisSidebar = ({ active, onChange, selectedTeam }) => {
  const teamColor = selectedTeam === 'teamA' ? '#006FEE' : '#f31260';
  return (
    <div className="w-16 flex-shrink-0 flex flex-col gap-1.5 p-1 bg-zinc-900/80 backdrop-blur-md rounded-xl border border-white/5 items-center z-10 shadow-lg">
      {ANALYSES.map(({ id, label, icon: Icon }) => {
        const isActive = active === id;
        return (
          <button
            key={id}
            onClick={() => onChange(id)}
            title={label}
            className={`
              w-full flex flex-col items-center gap-1 py-2.5 px-1 rounded-lg border
              transition-all duration-300 text-[9px] font-bold uppercase tracking-wider
              ${isActive
                ? 'text-white shadow-lg'
                : 'text-zinc-400 hover:text-zinc-200 hover:bg-white/5 border-transparent'
              }
            `}
            style={isActive ? {
              backgroundColor: `color-mix(in srgb, ${teamColor} 20%, #18181b)`,
              borderColor: `color-mix(in srgb, ${teamColor} 40%, transparent)`,
              boxShadow: `0 0 15px color-mix(in srgb, ${teamColor} 20%, transparent)`,
            } : {}}
          >
            <Icon size={14} />
            <span style={{ writingMode: 'horizontal-tb' }}>{label}</span>
          </button>
        );
      })}
    </div>
  );
};



// ── POSSESSION BAR ───────────────────────────────────────────
const PossessionBar = ({ valueA, valueB }) => {
  const a    = parseFloat(valueA) || 0;
  const b    = parseFloat(valueB) || 0;
  const tot  = a + b || 1;
  const pctA = (a / tot) * 100;
  const pctB = (b / tot) * 100;
  return (
    <div className="flex flex-col gap-1.5 py-1.5 mb-1">
      <div className="flex justify-between items-center text-[11px] font-semibold text-zinc-500 uppercase tracking-wide">
        <div
          className="min-w-[44px] h-[26px] flex items-center justify-center rounded-lg bg-zinc-950/80 border border-white/5 text-[11px] font-medium tabular-nums"
          style={{ fontFamily: "'JetBrains Mono', 'Fira Mono', monospace", color: '#006FEE' }}
        >
          {Math.round(pctA)}%
        </div>
        <span className="text-[11px] font-semibold tracking-wide text-zinc-400 uppercase">Possession</span>
        <div
          className="min-w-[44px] h-[26px] flex items-center justify-center rounded-lg bg-zinc-950/80 border border-white/5 text-[11px] font-medium tabular-nums"
          style={{ fontFamily: "'JetBrains Mono', 'Fira Mono', monospace", color: '#f31260' }}
        >
          {Math.round(pctB)}%
        </div>
      </div>
      <div className="w-full h-5 rounded-lg overflow-hidden flex border border-white/5 bg-zinc-950/60">
        <div className="h-full bg-[#006FEE] transition-all duration-1000" style={{ width: `${pctA}%` }} />
        <div className="h-full bg-[#f31260] transition-all duration-1000" style={{ width: `${pctB}%` }} />
      </div>
    </div>
  );
};

// ── STAT ROW ─────────────────────────────────────────────────
const StatRow = ({ label, valueA, valueB, format = 'number' }) => {
  const numA  = parseFloat(valueA) || 0;
  const numB  = parseFloat(valueB) || 0;
  const maxVal = Math.max(numA, numB, 1);

  const fmt = (v) => {
    if (format === 'decimal') return parseFloat(v || 0).toFixed(2);
    if (format === 'pct')     return `${v ?? 0}%`;
    return v ?? 0;
  };

  const ValuePill = ({ value, color }) => (
    <div
      className="min-w-[44px] h-[26px] flex items-center justify-center rounded-lg bg-zinc-950/80 border border-white/5 text-[11px] font-medium tabular-nums shadow-sm"
      style={{ fontFamily: "'JetBrains Mono', 'Fira Mono', monospace", color: '#ECEDEE' }}
    >
      {fmt(value)}
    </div>
  );

  return (
    <div className="flex flex-col gap-1 py-1.5 border-b border-white/[0.04] last:border-0">
      {/* Values + Label */}
      <div className="flex justify-between items-center">
        <ValuePill value={valueA} color="#006FEE" />
        <span className="flex-1 text-center text-[11px] font-semibold tracking-wide text-zinc-400 uppercase px-2">{label}</span>
        <ValuePill value={valueB} color="#f31260" />
      </div>
      {/* Dual mirrored bars */}
      <div className="flex items-center justify-center gap-1.5 px-1">
        {/* Left bar (team A) — fills from right */}
        <div className="flex-1 h-1.5 bg-zinc-950/60 border border-white/5 rounded-full flex justify-end overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700"
            style={{ width: `${(numA / maxVal) * 100}%`, backgroundColor: '#006FEE' }}
          />
        </div>
        <div className="w-1 h-1 rounded-full bg-zinc-600 flex-shrink-0" />
        {/* Right bar (team B) — fills from left */}
        <div className="flex-1 h-1.5 bg-zinc-950/60 border border-white/5 rounded-full flex justify-start overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700"
            style={{ width: `${(numB / maxVal) * 100}%`, backgroundColor: '#f31260' }}
          />
        </div>
      </div>
    </div>
  );
};

// ── STATS SECTION TABS ───────────────────────────────────────
const STAT_SECTIONS = ['Top Stats', 'Attacking', 'Defending', 'Passing', 'Fouls'];

const buildSections = (teamA, teamB) => ({
  'Top Stats': [
    { label: 'Possession',       valueA: teamA.topStats?.possession,     valueB: teamB.topStats?.possession,     format: 'pct'     },
    { label: 'Expected Goals',   valueA: teamA.topStats?.xG,             valueB: teamB.topStats?.xG,             format: 'decimal' },
    { label: 'Total Shots',      valueA: teamA.topStats?.totalShots,     valueB: teamB.topStats?.totalShots      },
    { label: 'Shots on Target',  valueA: teamA.topStats?.shotsOnTarget,  valueB: teamB.topStats?.shotsOnTarget   },
    { label: 'Big Chances',      valueA: teamA.topStats?.bigChances,     valueB: teamB.topStats?.bigChances      },
  ],
  'Attacking': [
    { label: 'Total Shots',        valueA: teamA.topStats?.totalShots,       valueB: teamB.topStats?.totalShots       },
    { label: 'Shots on Target',    valueA: teamA.topStats?.shotsOnTarget,     valueB: teamB.topStats?.shotsOnTarget    },
    { label: 'Big Chances',        valueA: teamA.topStats?.bigChances,        valueB: teamB.topStats?.bigChances       },
    { label: 'Big Chances Missed', valueA: teamA.topStats?.bigChancesMissed,  valueB: teamB.topStats?.bigChancesMissed },
    { label: 'xG',                 valueA: teamA.topStats?.xG,                valueB: teamB.topStats?.xG,               format: 'decimal' },
  ],
  'Defending': [
    { label: 'Interceptions', valueA: teamA.defense?.interceptions, valueB: teamB.defense?.interceptions },
    { label: 'Tackles Won',   valueA: teamA.defense?.tacklesWon,    valueB: teamB.defense?.tacklesWon    },
    { label: 'Clearances',    valueA: teamA.defense?.clearances,    valueB: teamB.defense?.clearances    },
  ],
  'Passing': [
    { label: 'Total Passes',        valueA: teamA.topStats?.totalPasses,      valueB: teamB.topStats?.totalPasses      },
    { label: 'Accurate Passes',     valueA: teamA.topStats?.accuratePasses,   valueB: teamB.topStats?.accuratePasses   },
    { label: 'Pass Accuracy',       valueA: teamA.topStats?.passAccuracy,     valueB: teamB.topStats?.passAccuracy,     format: 'pct' },
    { label: 'Progressive Passes',  valueA: teamA.passing?.progressivePasses, valueB: teamB.passing?.progressivePasses },
  ],
  'Fouls': [
    { label: 'Fouls',        valueA: teamA.topStats?.fouls,   valueB: teamB.topStats?.fouls   },
    { label: 'Corner Kicks', valueA: teamA.topStats?.corners, valueB: teamB.topStats?.corners },
  ],
});

// ============================================================
// MAIN COMPONENT
// ============================================================
// ── HELPER COMPONENTS FOR ATTACKING TAB ───────────────────────────
const AttackingRow = ({ label, valA, valB, isDecimal = false }) => {
  const numA = parseFloat(valA) || 0;
  const numB = parseFloat(valB) || 0;

  const formatVal = (val) => {
    if (isDecimal) return parseFloat(val || 0).toFixed(2);
    return parseInt(val || 0, 10);
  };

  const highlightA = numA > numB;
  const highlightB = numB > numA;

  return (
    <div className="flex items-center justify-between py-1.5 border-b border-white/[0.04] last:border-0">
      {/* Left Value */}
      <div className="min-w-[44px] flex justify-start">
        {highlightA ? (
          <span className="px-2 py-0.5 bg-white text-zinc-950 font-mono font-black rounded text-[10px] shadow-sm select-none">
            {formatVal(valA)}
          </span>
        ) : (
          <span className="text-zinc-400 font-mono font-bold text-[10px] pl-1 select-none">
            {formatVal(valA)}
          </span>
        )}
      </div>

      {/* Middle Label */}
      <span className="flex-1 text-center text-[10px] font-bold text-zinc-400 uppercase tracking-wider px-2 select-none">
        {label}
      </span>

      {/* Right Value */}
      <div className="min-w-[44px] flex justify-end">
        {highlightB ? (
          <span className="px-2 py-0.5 bg-white text-zinc-950 font-mono font-black rounded text-[10px] shadow-sm select-none">
            {formatVal(valB)}
          </span>
        ) : (
          <span className="text-zinc-400 font-mono font-bold text-[10px] pr-1 select-none">
            {formatVal(valB)}
          </span>
        )}
      </div>
    </div>
  );
};

const AttackingGoalMouth = ({ shots, teamAName, teamBName, onShotSelect }) => {
  const [hoveredIdx, setHoveredIdx] = useState(null);

  const GW = 200;
  const GH = 100;
  const PAD = 16;
  const POST_W = 3;

  return (
    <svg viewBox={`0 0 ${GW + PAD * 2} ${GH + PAD * 2 + 10}`} className="w-full h-full">
      {/* Net pattern */}
      <defs>
        <pattern id="netPatternAttacking" x="0" y="0" width="10" height="10" patternUnits="userSpaceOnUse">
          <line x1="0" y1="0" x2="10" y2="10" stroke="rgba(255,255,255,0.06)" strokeWidth="0.3" />
          <line x1="10" y1="0" x2="0" y2="10" stroke="rgba(255,255,255,0.06)" strokeWidth="0.3" />
        </pattern>
      </defs>

      {/* Goal frame */}
      <rect x={PAD} y={PAD} width={GW} height={GH} rx="1"
        fill="url(#netPatternAttacking)" stroke="rgba(255,255,255,0.4)" strokeWidth="1.5" />

      {/* Crossbar */}
      <rect x={PAD - POST_W / 2} y={PAD - POST_W} width={GW + POST_W} height={POST_W}
        fill="rgba(255,255,255,0.5)" rx="1" />

      {/* Posts */}
      <rect x={PAD - POST_W} y={PAD - POST_W} width={POST_W} height={GH + POST_W + 2}
        fill="rgba(255,255,255,0.4)" rx="1" />
      <rect x={PAD + GW} y={PAD - POST_W} width={POST_W} height={GH + POST_W + 2}
        fill="rgba(255,255,255,0.4)" rx="1" />

      {/* Ground line */}
      <line x1={PAD - 10} y1={PAD + GH} x2={PAD + GW + 10} y2={PAD + GH}
        stroke="rgba(255,255,255,0.15)" strokeWidth="1" />

      {/* Grid lines */}
      <line x1={PAD + GW / 2} y1={PAD} x2={PAD + GW / 2} y2={PAD + GH}
        stroke="rgba(255,255,255,0.06)" strokeWidth="0.5" strokeDasharray="3,3" />
      <line x1={PAD} y1={PAD + GH / 2} x2={PAD + GW} y2={PAD + GH / 2}
        stroke="rgba(255,255,255,0.06)" strokeWidth="0.5" strokeDasharray="3,3" />

      {/* Shots */}
      {shots.map((shot, i) => {
        const sx = PAD + (shot.goalX / 100) * GW;
        const sy = PAD + (shot.goalY / 100) * GH;
        const isTeamA = shot.team === teamAName;
        const baseColor = isTeamA ? '#006FEE' : '#f31260';
        const color = shot.isGoal ? '#00E676' : shot.onTarget ? baseColor : '#555555';
        const r = shot.isGoal ? 7 : shot.onTarget ? 5.5 : 4;
        const isHovered = hoveredIdx === i;

        const clampedX = shot.onTarget ? sx : Math.max(PAD - 8, Math.min(PAD + GW + 8, sx));
        const clampedY = shot.onTarget ? sy : Math.max(PAD - 8, Math.min(PAD + GH + 8, sy));

        return (
          <g key={i}
            onMouseEnter={() => setHoveredIdx(i)}
            onMouseLeave={() => setHoveredIdx(null)}
            onClick={() => onShotSelect && onShotSelect(shot)}
            style={{ cursor: 'pointer' }}
          >
            {(shot.isGoal || isHovered) && (
              <circle cx={clampedX} cy={clampedY} r={r + 5}
                fill={color} fillOpacity={0.15} />
            )}
            <circle cx={clampedX} cy={clampedY} r={r}
              fill={shot.isGoal ? '#00E676' : 'rgba(0,0,0,0.85)'}
              stroke={color}
              strokeWidth={isHovered ? 2.5 : 1.2}
              fillOpacity={shot.isGoal ? 0.95 : shot.onTarget ? 0.85 : 0.4}
              strokeDasharray={shot.onTarget ? 'none' : '2,2'}
            />
            {shot.isGoal && (
              <text x={clampedX} y={clampedY + 1.5} textAnchor="middle" dominantBaseline="middle"
                fontSize="5" fill="black" fontWeight="900" style={{ fontFamily: 'Inter, sans-serif' }}>
                G
              </text>
            )}
            {isHovered && (
              <g pointerEvents="none">
                <rect x={clampedX + 8} y={clampedY - 20} width="65" height="18" rx="4"
                  fill="rgba(10,10,12,0.95)" stroke="rgba(255,255,255,0.15)" strokeWidth="0.8" />
                <text x={clampedX + 12} y={clampedY - 12} fontSize="4" fill="white" fontWeight="bold"
                  style={{ fontFamily: 'Inter, sans-serif' }}>
                  {shot.player?.split(' ').pop()} {shot.minute}'
                </text>
                <text x={clampedX + 12} y={clampedY - 6} fontSize="3.5" fill={color} fontWeight="bold"
                  style={{ fontFamily: 'Inter, sans-serif' }}>
                  xG: {shot.xg} · {shot.outcome}
                </text>
              </g>
            )}
          </g>
        );
      })}
    </svg>
  );
};

const AnimatePlayerCard = ({ playerId, players, teamA, teamB, statsData, loading, onClose }) => {
  if (!playerId || !players?.[playerId]) {
    return (
      <div className="w-full flex-1 flex flex-col items-center justify-center p-6 border border-dashed border-white/10 rounded-xl bg-zinc-950/20 min-h-[220px]">
        <div className="w-10 h-10 rounded-full border border-white/5 flex items-center justify-center mb-3 bg-zinc-900/40">
          <Star size={14} className="text-zinc-600 animate-pulse" />
        </div>
        <p className="text-[10px] text-zinc-500 font-bold uppercase tracking-wider text-center">
          No player selected
        </p>
        <p className="text-[9px] text-zinc-600 text-center mt-1 max-w-[180px]">
          Click any shot in the goal mouth to view the player's performance radar and match stats!
        </p>
      </div>
    );
  }

  const player = players[playerId];
  const pName = player.info.name || player.info.shortName || "Unknown Player";
  const pNum = player.info.number || "00";
  const pPos = player.info.position || "?";
  const pTeam = player.info.teamName || "";
  
  const isHome = pTeam === teamA.teamName;
  const playerTeamColor = isHome ? '#006FEE' : '#f31260';

  // Calculate percentage accuracy/wins
  const passAcc = player.stats.passAccuracy || 0;
  const duelWinPct = player.stats.tackles > 0 
    ? Math.round((player.stats.duelsWon / player.stats.tackles) * 100) 
    : 0;

  // Render dicebear avatar with seed of player name
  const avatarUrl = `https://api.dicebear.com/7.x/adventurer/svg?seed=${encodeURIComponent(pName)}`;

  // Compute Overall rating (OVR) for the center of the radar
  const averageOvr = statsData 
    ? Math.round(statsData.reduce((acc, curr) => acc + curr.value, 0) / statsData.length)
    : 75;

  return (
    <div className="w-full bg-zinc-950/85 backdrop-blur-2xl border border-white/10 rounded-2xl p-4 shadow-2xl relative flex flex-col justify-between overflow-hidden group">
      {/* Background glow matching team color */}
      <div 
        className="absolute -inset-1 rounded-2xl blur-3xl opacity-20 group-hover:opacity-30 transition duration-1000 pointer-events-none"
        style={{
          background: `radial-gradient(circle, ${playerTeamColor}44 0%, transparent 70%)`
        }}
      />

      {/* Card Header */}
      <div className="flex items-start justify-between relative z-10">
        <div className="flex items-center gap-3">
          {/* Avatar */}
          <div className="relative">
            <div 
              className="w-12 h-12 rounded-xl overflow-hidden border flex items-center justify-center bg-zinc-900/80 shadow-md"
              style={{ borderColor: `${playerTeamColor}44` }}
            >
              <img src={avatarUrl} alt={pName} className="w-10 h-10 object-contain" />
            </div>
            <span 
              className="absolute -bottom-1 -right-1 w-5 h-5 rounded-full flex items-center justify-center text-[8px] font-black text-white shadow"
              style={{ backgroundColor: playerTeamColor }}
            >
              {pNum}
            </span>
          </div>
          <div>
            <h4 className="text-xs font-black text-white leading-snug">{pName}</h4>
            <span className="text-[8px] font-bold text-zinc-500 uppercase tracking-widest">
              {pTeam} · {pPos}
            </span>
          </div>
        </div>

        {/* Close Button */}
        <button 
          onClick={onClose}
          className="p-1 rounded-full text-zinc-500 hover:text-white hover:bg-white/5 transition-all"
        >
          <X size={12} />
        </button>
      </div>

      {/* Radar Chart (Center) */}
      <div className="w-full h-[150px] relative z-10 flex items-center justify-center my-2">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <div className="w-4 h-4 border-2 border-primary border-t-transparent rounded-full animate-spin" />
          </div>
        ) : statsData ? (
          <div className="w-full h-full relative">
            <ResponsiveContainer width="100%" height="100%">
              <RadarChart cx="50%" cy="50%" outerRadius="70%" data={statsData}>
                <PolarGrid stroke="rgba(255,255,255,0.06)" />
                <PolarAngleAxis 
                  dataKey="metric" 
                  tick={{ fill: 'rgba(255,255,255,0.4)', fontSize: 7, fontWeight: 'bold' }} 
                />
                <Radar
                  name={pName}
                  dataKey="value"
                  stroke={playerTeamColor}
                  strokeWidth={1.5}
                  fill={playerTeamColor}
                  fillOpacity={0.35}
                />
              </RadarChart>
            </ResponsiveContainer>
            
            {/* OVR display in the center of the radar */}
            <div className="absolute top-[48%] left-1/2 -translate-x-1/2 -translate-y-1/2 text-center pointer-events-none">
              <span className="text-lg font-black text-white drop-shadow-md">
                {averageOvr}
              </span>
              <div className="text-[6px] text-zinc-500 uppercase font-black tracking-widest -mt-0.5">OVR</div>
            </div>
          </div>
        ) : (
          <div className="text-zinc-600 text-[8px] font-bold uppercase tracking-wider">
            Could not load radar
          </div>
        )}
      </div>

      {/* Compiled Match Actions */}
      <div className="grid grid-cols-4 gap-2 relative z-10 pt-2 border-t border-white/5 bg-zinc-950/40 p-2 rounded-xl border border-white/5 shadow-inner">
        {/* Goals */}
        <div className="flex flex-col items-center justify-center">
          <Award size={10} className={player.stats.goals > 0 ? "text-success animate-pulse" : "text-zinc-600"} />
          <span className="font-mono text-[10px] font-black text-white mt-0.5 tabular-nums">
            {player.stats.goals}
          </span>
          <span className="text-[7px] text-zinc-500 font-bold uppercase tracking-wider">Goals</span>
        </div>

        {/* Passes */}
        <div className="flex flex-col items-center justify-center">
          <Navigation size={10} className="text-blue-400" />
          <span className="font-mono text-[10px] font-black text-white mt-0.5 tabular-nums">
            {player.stats.passesCompleted}
            <span className="text-[8px] text-zinc-500 font-normal">/{player.stats.totalPasses || player.stats.passesCompleted || 0}</span>
          </span>
          <span className="text-[7px] text-zinc-500 font-bold uppercase tracking-wider">
            Pass {passAcc}%
          </span>
        </div>

        {/* Assists / Key Passes */}
        <div className="flex flex-col items-center justify-center">
          <Flame size={10} className={player.stats.keyPasses > 0 ? "text-warning animate-pulse" : "text-zinc-600"} />
          <span className="font-mono text-[10px] font-black text-white mt-0.5 tabular-nums">
            {player.stats.assists || player.stats.keyPasses || 0}
          </span>
          <span className="text-[7px] text-zinc-500 font-bold uppercase tracking-wider">Assists</span>
        </div>

        {/* Duels Won / Total */}
        <div className="flex flex-col items-center justify-center">
          <Shield size={10} className="text-green-400" />
          <span className="font-mono text-[10px] font-black text-white mt-0.5 tabular-nums">
            {player.stats.duelsWon}
            <span className="text-[8px] text-zinc-500 font-normal">/{player.stats.tackles || player.stats.duelsWon || 0}</span>
          </span>
          <span className="text-[7px] text-zinc-500 font-bold uppercase tracking-wider">
            Duel {duelWinPct}%
          </span>
        </div>
      </div>
    </div>
  );
};

const MATCH_ID = 'test_match';

const TeamComparison = ({ teamA, teamB, selectedTeam, players }) => {
  const [analysis,     setAnalysis]     = useState('passing');
  const [activeSection, setActiveSection] = useState('Top Stats');
  const [dataType,     setDataType]     = useState('eventing'); // 'eventing' | 'tracking'
  const [selectedPlayerId, setSelectedPlayerId] = useState(null);
  const [playerProfileStats, setPlayerProfileStats] = useState(null);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [showXgHelper, setShowXgHelper] = useState(false);

  // Fetch shot map & momentum data
  const { shotData } = useShotMap(MATCH_ID);
  const { momentumData } = useMomentum(MATCH_ID);

  // Fetch player profile stats when selectedPlayerId changes
  useEffect(() => {
    if (!selectedPlayerId) {
      setPlayerProfileStats(null);
      return;
    }
    setLoadingProfile(true);
    const player = players?.[selectedPlayerId];
    let gameState = 'Drawing';
    if (player && teamA && teamB) {
      const isHome = player.info.teamName === teamA.teamName;
      if (isHome) {
        if (teamA.goals > teamB.goals) gameState = 'Winning';
        else if (teamA.goals < teamB.goals) gameState = 'Losing';
      } else {
        if (teamB.goals > teamA.goals) gameState = 'Winning';
        else if (teamB.goals < teamA.goals) gameState = 'Losing';
      }
    }

    fetch(`http://127.0.0.1:8000/match/test_match/player/${selectedPlayerId}/profile?state=${gameState}`)
      .then(res => res.json())
      .then(data => {
        if (data && data.stats) {
          setPlayerProfileStats(data.stats);
        } else {
          setPlayerProfileStats([
            { metric: "GOALS", value: 65, fullMark: 100 },
            { metric: "SHOTS", value: 75, fullMark: 100 },
            { metric: "xG", value: 70, fullMark: 100 },
            { metric: "CREA", value: 80, fullMark: 100 },
            { metric: "PROG", value: 85, fullMark: 100 },
            { metric: "DEF", value: 50, fullMark: 100 }
          ]);
        }
      })
      .catch(err => {
        console.error("Error fetching player profile:", err);
        setPlayerProfileStats([
          { metric: "GOALS", value: 60, fullMark: 100 },
          { metric: "SHOTS", value: 70, fullMark: 100 },
          { metric: "xG", value: 65, fullMark: 100 },
          { metric: "CREA", value: 75, fullMark: 100 },
          { metric: "PROG", value: 80, fullMark: 100 },
          { metric: "DEF", value: 45, fullMark: 100 }
        ]);
      })
      .finally(() => setLoadingProfile(false));
  }, [selectedPlayerId, players, teamA?.goals, teamB?.goals]);

  const handleShotClick = (shot) => {
    console.log("🎯 Attacking Shot Node Clicked:", shot);
    if (shot.player_id && players?.[shot.player_id]) {
      console.log("✅ Resolved via backend player_id:", shot.player_id);
      setSelectedPlayerId(shot.player_id);
    } else {
      // Fallback lookup by accent-insensitive player name matching
      const norm = (txt) => (txt || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
      const target = norm(shot.player);
      
      const found = Object.values(players || {}).find(p => {
        const pName = norm(p.info.name);
        const pShort = norm(p.info.shortName);
        return pName.includes(target) || target.includes(pName) || pShort.includes(target) || target.includes(pShort);
      });

      if (found) {
        console.log("🔍 Resolved via frontend fallback matching:", found.info.name, "(ID:", found.info.id, ")");
        setSelectedPlayerId(found.info.id);
      } else {
        console.warn("⚠️ Player mismatch for shot:", shot.player, "among players:", Object.values(players || {}).map(p => p.info.name));
      }
    }
  };

  if (!teamA || !teamB) return null;

  const sections   = buildSections(teamA, teamB);
  const activeRows = sections[activeSection] || [];

  return (
    <div className="w-full h-full flex flex-col gap-3 overflow-hidden">

      {/* ── ROW 2: [Sidebar | Pitch 40%] | [Stats 60%] ──────── */}
      <div className="flex-1 min-h-0 flex gap-3 overflow-hidden">

        {/* ── PITCH COLUMN ≈ 40% (sidebar buttons + pitch) ──── */}
        <div className="flex gap-2 overflow-hidden" style={{ flex: '0 0 40%', minWidth: 0 }}>

          {/* Vertical analysis sidebar */}
          <AnalysisSidebar active={analysis} onChange={setAnalysis} selectedTeam={selectedTeam} />

          {/* Pitch fills remaining width */}
          <div className="flex-1 min-w-0 min-h-0 relative">
            {analysis === 'passing' && (
              <div className="absolute top-2 right-2 z-20 flex gap-1 bg-zinc-950/80 backdrop-blur-md p-0.5 rounded-lg border border-white/5 shadow-lg">
                {[
                  { value: 'eventing', label: 'Eventing Data' },
                  { value: 'tracking', label: 'Tracking Data' }
                ].map(opt => {
                  const isActive = dataType === opt.value;
                  return (
                    <button
                      key={opt.value}
                      onClick={() => setDataType(opt.value)}
                      className={`
                        px-2 py-1 rounded text-[8px] font-bold uppercase tracking-wider transition-all duration-200 border
                        ${isActive
                          ? 'bg-white/10 text-white border-white/10 shadow-sm'
                          : 'text-zinc-500 hover:text-zinc-300 border-transparent hover:bg-white/5'
                        }
                      `}
                    >
                      {opt.label}
                    </button>
                  );
                })}
              </div>
            )}

            <PassingNetworkPitch
              teamA={teamA}
              teamB={teamB}
              selectedTeam={selectedTeam}
              analysisMode={analysis}
              dataType={dataType}
            />
          </div>
        </div>

        {/* ── STATS COLUMN ≈ 60% ───────────────────────────── */}
        <div className="flex-1 min-w-0 flex flex-col gap-3 overflow-hidden">

          {/* Scoreboard */}
          <div className="bg-zinc-900/60 backdrop-blur-sm rounded-xl border border-white/5 px-4 py-2.5 flex-shrink-0">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 rounded-lg flex items-center justify-center text-[9px] font-black"
                  style={{ backgroundColor: '#006FEE22', border: '1px solid #006FEE44', color: '#006FEE' }}>
                  {(teamA.teamName || 'A').substring(0, 3).toUpperCase()}
                </div>
                <span className="text-sm font-bold text-white">{teamA.teamName}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-4xl font-black text-white tabular-nums">{teamA.goals ?? 0}</span>
                <span className="text-xl font-light text-zinc-700">–</span>
                <span className="text-4xl font-black text-white tabular-nums">{teamB.goals ?? 0}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-sm font-bold text-white text-right">{teamB.teamName}</span>
                <div className="w-7 h-7 rounded-lg flex items-center justify-center text-[9px] font-black"
                  style={{ backgroundColor: '#f3126022', border: '1px solid #f3126044', color: '#f31260' }}>
                  {(teamB.teamName || 'B').substring(0, 3).toUpperCase()}
                </div>
              </div>
            </div>
          </div>

          {/* Stats card */}
          <div className="flex-1 min-h-0 bg-zinc-900/60 backdrop-blur-sm rounded-xl border border-white/5 flex flex-col overflow-hidden">

            {/* Section tabs — pill style (TFM design) */}
            <div className="flex items-center px-2 pt-2 pb-1.5 gap-1 flex-shrink-0 overflow-x-auto no-scrollbar">
              {STAT_SECTIONS.map(sec => {
                const isActive = activeSection === sec;
                return (
                  <button
                    key={sec}
                    onClick={() => setActiveSection(sec)}
                    className={`
                      px-3 py-1.5 text-[10px] font-bold tracking-wider uppercase whitespace-nowrap
                      rounded-lg transition-all duration-200
                      ${isActive
                        ? 'bg-white/10 text-white'
                        : 'text-zinc-500 hover:text-white hover:bg-white/5'
                      }
                    `}
                  >
                    {sec}
                  </button>
                );
              })}
            </div>
            <div className="h-px bg-white/5 flex-shrink-0" />

            {/* Team name headers for stat rows */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-white/5 flex-shrink-0">
              <span className="text-[11px] font-bold tracking-wider" style={{ color: '#006FEE' }}>
                {(teamA.teamName || '').split(' ').slice(-1)[0]}
              </span>
              <span className="text-[9px] font-bold text-zinc-600 uppercase tracking-widest">
                {activeSection}
              </span>
              <span className="text-[11px] font-bold tracking-wider" style={{ color: '#f31260' }}>
                {(teamB.teamName || '').split(' ').slice(-1)[0]}
              </span>
            </div>

            {/* Scrollable rows */}
            <div className="flex-1 overflow-y-auto no-scrollbar px-4 py-2">
              {activeSection === 'Top Stats' && (
                <>
                  <PossessionBar
                    valueA={teamA.topStats?.possession}
                    valueB={teamB.topStats?.possession}
                  />

                  {/* Stat rows first */}
                  {activeRows
                    .filter(row => row.label !== 'Possession')
                    .map((row, i) => (
                      <StatRow key={i} {...row} />
                    ))
                  }

                  {/* Shot Map — TFM card style */}
                  <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg mt-3">
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase">
                        Shot Map
                      </h3>
                    </div>
                    <ShotMapViz shotData={shotData} teamAShort={teamA?.teamShort} teamBShort={teamB?.teamShort} />
                  </div>

                  {/* Momentum + Accurate Passes — side by side (TFM grid layout) */}
                  <div className="grid grid-cols-2 gap-3 mt-3 mb-2">

                    {/* Momentum */}
                    <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg flex flex-col">
                      <div className="flex items-center justify-between mb-2">
                        <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase">
                          Momentum
                        </h3>
                      </div>
                      <MomentumChart momentumData={momentumData} shotData={shotData} teamAShort={teamA?.teamShort} teamBShort={teamB?.teamShort} />
                    </div>

                    {/* Accurate Passes */}
                    <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg flex flex-col justify-between">
                      <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase mb-4">
                        Accurate Passes
                      </h3>
                      <div className="flex-1 flex items-center justify-center">
                        <AccuratePassesViz teamA={teamA} teamB={teamB} />
                      </div>
                    </div>

                  </div>
                </>
              )}

              {activeSection !== 'Top Stats' && activeSection !== 'Attacking' && (
                activeRows.map((row, i) => (
                  <StatRow key={i} {...row} />
                ))
              )}

              {activeSection === 'Attacking' && (
                <div className="flex flex-col gap-4">
                  {/* ATTACKING DASHBOARD */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {/* Expected Goals Card */}
                    <div className="bg-zinc-950/60 backdrop-blur-md rounded-xl border border-white/5 p-4 shadow-lg flex flex-col justify-between">
                      <div>
                        <h3 className="text-center text-[11px] font-extrabold uppercase tracking-widest text-[#ECEDEE] mb-4 select-none">
                          Expected goals (xG)
                        </h3>
                        <div className="flex flex-col gap-1">
                          <AttackingRow
                            label="Expected goals (xG)"
                            valA={teamA.topStats?.xG}
                            valB={teamB.topStats?.xG}
                            isDecimal={true}
                          />
                          <AttackingRow
                            label="xG open play"
                            valA={teamA.topStats?.xgOpenPlay}
                            valB={teamB.topStats?.xgOpenPlay}
                            isDecimal={true}
                          />
                          <AttackingRow
                            label="xG set play"
                            valA={teamA.topStats?.xgSetPlay}
                            valB={teamB.topStats?.xgSetPlay}
                            isDecimal={true}
                          />
                          <AttackingRow
                            label="Non-penalty xG"
                            valA={teamA.topStats?.xgNonPenalty}
                            valB={teamB.topStats?.xgNonPenalty}
                            isDecimal={true}
                          />
                          <AttackingRow
                            label="xG on target (xGOT)"
                            valA={teamA.topStats?.xgOnTarget}
                            valB={teamB.topStats?.xgOnTarget}
                            isDecimal={true}
                          />
                        </div>
                      </div>
                      
                      {/* Explainer Link */}
                      <div className="mt-4 pt-3 border-t border-white/[0.04]">
                        <button
                          onClick={() => setShowXgHelper(!showXgHelper)}
                          className="w-full flex items-center justify-center gap-1.5 text-[10px] text-zinc-500 hover:text-zinc-300 font-bold uppercase transition-colors"
                        >
                          <HelpCircle size={12} />
                          What does Expected Goals (xG) mean?
                        </button>
                        {showXgHelper && (
                          <p className="text-[10px] text-zinc-500 leading-relaxed text-center mt-2 px-1 select-none">
                            Expected Goals (xG) measures shot quality. A shot with 0.40 xG means a player scores that chance 40% of the time. xG on target (xGOT) measures execution after the shot was taken (on-target shots only).
                          </p>
                        )}
                      </div>
                    </div>

                    {/* Shots Card */}
                    <div className="bg-zinc-950/60 backdrop-blur-md rounded-xl border border-white/5 p-4 shadow-lg flex flex-col justify-between">
                      <div>
                        <h3 className="text-center text-[11px] font-extrabold uppercase tracking-widest text-[#ECEDEE] mb-4 select-none">
                          Shots
                        </h3>
                        <div className="flex flex-col gap-1">
                          <AttackingRow
                            label="Total shots"
                            valA={teamA.topStats?.totalShots}
                            valB={teamB.topStats?.totalShots}
                          />
                          <AttackingRow
                            label="Shots off target"
                            valA={teamA.topStats?.shotsOffTarget}
                            valB={teamB.topStats?.shotsOffTarget}
                          />
                          <AttackingRow
                            label="Shots on target"
                            valA={teamA.topStats?.shotsOnTarget}
                            valB={teamB.topStats?.shotsOnTarget}
                          />
                          <AttackingRow
                            label="Blocked shots"
                            valA={teamA.topStats?.shotsBlocked}
                            valB={teamB.topStats?.shotsBlocked}
                          />
                          <AttackingRow
                            label="Hit woodwork"
                            valA={teamA.topStats?.shotsWoodwork}
                            valB={teamB.topStats?.shotsWoodwork}
                          />
                          <AttackingRow
                            label="Shots inside box"
                            valA={teamA.topStats?.shotsInsideBox}
                            valB={teamB.topStats?.shotsInsideBox}
                          />
                          <AttackingRow
                            label="Shots outside box"
                            valA={teamA.topStats?.shotsOutsideBox}
                            valB={teamB.topStats?.shotsOutsideBox}
                          />
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* INTERACTIVE GOAL MOUTH AND INTERACTIVE PLAYER DETAIL CARD */}
                  <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 mt-2">
                    {/* Goal Mouth Column */}
                    <div className="lg:col-span-7 bg-zinc-900/60 backdrop-blur-sm border border-white/5 rounded-xl p-4 shadow-lg flex flex-col items-center justify-between">
                      <div className="w-full flex items-center justify-between mb-2">
                        <h3 className="text-[10px] font-extrabold uppercase tracking-widest text-zinc-400 select-none">
                          Interactive Goal Mouth
                        </h3>
                        <span className="text-[9px] text-zinc-500 font-bold uppercase select-none">
                          Click any shot to analyze player
                        </span>
                      </div>
                      <div className="w-full max-w-[340px] aspect-[1.7] flex items-center justify-center py-2">
                        <AttackingGoalMouth
                          shots={shotData?.shots || []}
                          teamAName={teamA.teamName}
                          teamBName={teamB.teamName}
                          onShotSelect={handleShotClick}
                        />
                      </div>
                      <div className="w-full flex justify-center mt-2">
                        <div className="flex flex-wrap justify-center gap-4 text-[8px] text-zinc-500 font-bold uppercase select-none">
                          <div className="flex items-center gap-1.5">
                            <div className="w-2 h-2 rounded bg-[#00E676]" />
                            Goal
                          </div>
                          <div className="flex items-center gap-1.5">
                            <div className="w-2 h-2 rounded bg-[#006FEE]" />
                            {teamA.teamShort || 'Home'}
                          </div>
                          <div className="flex items-center gap-1.5">
                            <div className="w-2 h-2 rounded bg-[#f31260]" />
                            {teamB.teamShort || 'Away'}
                          </div>
                          <div className="flex items-center gap-1.5">
                            <div className="w-2 h-2 rounded border border-white/40" />
                            Off target
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Interactive Player Detail Card Column */}
                    <div className="lg:col-span-5 flex">
                      <AnimatePlayerCard
                        playerId={selectedPlayerId}
                        players={players}
                        teamA={teamA}
                        teamB={teamB}
                        statsData={playerProfileStats}
                        loading={loadingProfile}
                        onClose={() => setSelectedPlayerId(null)}
                      />
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TeamComparison;
