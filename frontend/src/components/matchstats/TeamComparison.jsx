import React from 'react';
import { Crosshair, Navigation, Shield } from 'lucide-react';

// ============================================================
// TEAM COMPARISON — FotMob-Inspired Stat Bars
// ============================================================

// --- STAT BAR ROW ---
const StatBar = ({ label, valueA, valueB, format = 'number', teamAColor = '#006FEE', teamBColor = '#f31260' }) => {
  const numA = parseFloat(valueA) || 0;
  const numB = parseFloat(valueB) || 0;
  const total = numA + numB || 1;
  const pctA = (numA / total) * 100;
  const pctB = (numB / total) * 100;

  // Determine who "wins"
  const aWins = numA > numB;
  const bWins = numB > numA;

  const formatVal = (v) => {
    if (format === 'pct') return `${v}%`;
    if (format === 'decimal') return parseFloat(v).toFixed(2);
    return v;
  };

  return (
    <div className="flex items-center gap-3 py-2 group">
      {/* Value A */}
      <span
        className={`w-12 text-right font-mono text-sm tabular-nums transition-colors duration-300 ${
          aWins ? 'text-white font-bold' : 'text-zinc-500'
        }`}
      >
        {formatVal(valueA)}
      </span>

      {/* Bar Container */}
      <div className="flex-1 flex items-center gap-0.5 h-5 relative">
        {/* Bar A (grows right-to-left) */}
        <div className="flex-1 flex justify-end h-full">
          <div
            className="h-full rounded-l-sm transition-all duration-700 ease-out relative overflow-hidden"
            style={{
              width: `${pctA}%`,
              backgroundColor: teamAColor,
              opacity: aWins ? 0.9 : 0.4,
            }}
          >
            {/* Shimmer */}
            <div
              className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500"
              style={{
                background: `linear-gradient(90deg, transparent, rgba(255,255,255,0.15), transparent)`,
              }}
            />
          </div>
        </div>

        {/* Center divider */}
        <div className="w-[1px] h-full bg-zinc-700 flex-shrink-0" />

        {/* Bar B (grows left-to-right) */}
        <div className="flex-1 flex justify-start h-full">
          <div
            className="h-full rounded-r-sm transition-all duration-700 ease-out relative overflow-hidden"
            style={{
              width: `${pctB}%`,
              backgroundColor: teamBColor,
              opacity: bWins ? 0.9 : 0.4,
            }}
          >
            <div
              className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500"
              style={{
                background: `linear-gradient(90deg, transparent, rgba(255,255,255,0.15), transparent)`,
              }}
            />
          </div>
        </div>
      </div>

      {/* Value B */}
      <span
        className={`w-12 text-left font-mono text-sm tabular-nums transition-colors duration-300 ${
          bWins ? 'text-white font-bold' : 'text-zinc-500'
        }`}
      >
        {formatVal(valueB)}
      </span>
    </div>
  );
};

// --- STAT GROUP SECTION ---
const StatGroup = ({ title, icon: Icon, stats }) => (
  <div className="bg-zinc-900/50 backdrop-blur-sm rounded-xl border border-white/5 p-4 space-y-1">
    {/* Group Header */}
    <div className="flex items-center gap-2 mb-3 pb-2 border-b border-white/5">
      <Icon size={13} className="text-primary" />
      <span className="text-[10px] font-bold tracking-widest text-zinc-400 uppercase">{title}</span>
    </div>

    {/* Stat Rows */}
    {stats.map((stat) => (
      <div key={stat.label}>
        {/* Label centered */}
        <p className="text-[10px] text-zinc-500 text-center font-medium tracking-wider uppercase mb-0.5">
          {stat.label}
        </p>
        <StatBar {...stat} />
      </div>
    ))}
  </div>
);

// ============================================================
// MAIN COMPONENT
// ============================================================
const TeamComparison = ({ teamA, teamB }) => {
  if (!teamA || !teamB) return null;

  const statGroups = [
    {
      title: 'Top Stats',
      icon: Crosshair,
      stats: [
        { label: 'Expected Goals (xG)', valueA: teamA.topStats.xG, valueB: teamB.topStats.xG, format: 'decimal' },
        { label: 'Total Shots', valueA: teamA.topStats.totalShots, valueB: teamB.topStats.totalShots },
        { label: 'Shots on Target', valueA: teamA.topStats.shotsOnTarget, valueB: teamB.topStats.shotsOnTarget },
        { label: 'Possession', valueA: teamA.topStats.possession, valueB: teamB.topStats.possession, format: 'pct' },
      ],
    },
    {
      title: 'Passing',
      icon: Navigation,
      stats: [
        { label: 'Accurate Passes', valueA: teamA.passing.accuratePasses, valueB: teamB.passing.accuratePasses },
        { label: 'Pass Accuracy', valueA: teamA.passing.passAccuracy, valueB: teamB.passing.passAccuracy, format: 'pct' },
        { label: 'Progressive Passes', valueA: teamA.passing.progressivePasses, valueB: teamB.passing.progressivePasses },
      ],
    },
    {
      title: 'Defense',
      icon: Shield,
      stats: [
        { label: 'Interceptions', valueA: teamA.defense.interceptions, valueB: teamB.defense.interceptions },
        { label: 'Tackles Won', valueA: teamA.defense.tacklesWon, valueB: teamB.defense.tacklesWon },
        { label: 'Clearances', valueA: teamA.defense.clearances, valueB: teamB.defense.clearances },
      ],
    },
  ];

  return (
    <div className="space-y-4 max-w-2xl mx-auto w-full">
      {/* Team Header Bar */}
      <div className="flex items-center justify-between px-4 py-3 bg-zinc-900/50 rounded-xl border border-white/5">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-full bg-[#006FEE]/20 border border-[#006FEE]/30 flex items-center justify-center">
            <span className="text-[10px] font-bold text-[#006FEE]">{teamA.teamShort}</span>
          </div>
          <span className="text-sm font-bold text-white">{teamA.teamName}</span>
        </div>
        <span className="text-[9px] text-zinc-600 font-bold tracking-widest uppercase">vs</span>
        <div className="flex items-center gap-3">
          <span className="text-sm font-bold text-white">{teamB.teamName}</span>
          <div className="w-8 h-8 rounded-full bg-[#f31260]/20 border border-[#f31260]/30 flex items-center justify-center">
            <span className="text-[10px] font-bold text-[#f31260]">{teamB.teamShort}</span>
          </div>
        </div>
      </div>

      {/* Stat Groups */}
      {statGroups.map((group) => (
        <StatGroup key={group.title} {...group} />
      ))}
    </div>
  );
};

export default TeamComparison;
