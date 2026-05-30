import React, { useState } from 'react';
import { Network, Target, Thermometer } from 'lucide-react';
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
const AnalysisSidebar = ({ active, onChange }) => (
  <div className="flex flex-col gap-1.5 p-1 bg-zinc-950/70 rounded-xl border border-white/5 items-center">
    {ANALYSES.map(({ id, label, icon: Icon }) => {
      const isActive = active === id;
      return (
        <button
          key={id}
          onClick={() => onChange(id)}
          title={label}
          className={`
            w-full flex flex-col items-center gap-1 py-2.5 px-1 rounded-lg
            transition-all duration-200 text-[9px] font-bold uppercase tracking-wider
            ${isActive
              ? 'bg-white/10 text-white border border-white/15'
              : 'text-zinc-600 hover:text-zinc-400 border border-transparent hover:bg-white/5'
            }
          `}
        >
          <Icon size={14} />
          <span style={{ writingMode: 'horizontal-tb' }}>{label}</span>
        </button>
      );
    })}
  </div>
);



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
const MATCH_ID = 'test_match';

const TeamComparison = ({ teamA, teamB, selectedTeam }) => {
  const [analysis,     setAnalysis]     = useState('passing');
  const [activeSection, setActiveSection] = useState('Top Stats');

  // Fetch shot map & momentum data
  const { shotData } = useShotMap(MATCH_ID);
  const { momentumData } = useMomentum(MATCH_ID);

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
          <AnalysisSidebar active={analysis} onChange={setAnalysis} />

          {/* Pitch fills remaining width */}
          <div className="flex-1 min-w-0 min-h-0">
            <PassingNetworkPitch
              teamA={teamA}
              teamB={teamB}
              selectedTeam={selectedTeam}
              analysisMode={analysis}
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

              {activeSection !== 'Top Stats' && (
                activeRows.map((row, i) => (
                  <StatRow key={i} {...row} />
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TeamComparison;
