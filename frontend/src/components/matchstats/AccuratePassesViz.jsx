import React from 'react';

// ============================================================
// ACCURATE PASSES VIZ — Double donut chart showing
// pass accuracy for both teams side by side
// Design: TFM PassAccuracy.tsx style
// ============================================================

const TEAM_A_COLOR = '#006FEE';
const TEAM_B_COLOR = '#f31260';

const DonutRing = ({ total, accurate, color, teamShort }) => {
  const percentage = total > 0 ? Math.round((accurate / total) * 100) : 0;
  const fraction = `${accurate}/${total}`;
  const radius = 38;
  const circumference = 2 * Math.PI * radius;
  const strokeDashoffset = circumference - (percentage / 100) * circumference;

  return (
    <div className="flex flex-col items-center gap-3">
      <div className="relative w-24 h-24 flex items-center justify-center">
        {/* SVG rotated -90° so arc starts at top */}
        <svg className="absolute inset-0 w-full h-full transform -rotate-90">
          {/* Track */}
          <circle
            cx="48"
            cy="48"
            r={radius}
            stroke="rgba(255,255,255,0.05)"
            strokeWidth="6"
            fill="transparent"
          />
          {/* Glow layer */}
          <circle
            cx="48"
            cy="48"
            r={radius}
            stroke={color}
            strokeWidth="9"
            fill="transparent"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            strokeLinecap="round"
            opacity="0.12"
          />
          {/* Filled arc */}
          <circle
            cx="48"
            cy="48"
            r={radius}
            stroke={color}
            strokeWidth="6"
            fill="transparent"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            strokeLinecap="round"
            style={{ transition: 'stroke-dashoffset 0.8s ease' }}
          />
        </svg>
        {/* Center text — not rotated */}
        <div className="flex flex-col items-center justify-center relative z-10">
          <span
            className="text-xl font-bold text-[#ECEDEE] tabular-nums"
            style={{ fontFamily: "'JetBrains Mono', 'Fira Mono', monospace" }}
          >
            {percentage}%
          </span>
          <span
            className="text-[9px] text-[#71717A] tabular-nums"
            style={{ fontFamily: "'JetBrains Mono', 'Fira Mono', monospace" }}
          >
            {fraction}
          </span>
        </div>
      </div>
      <span className="text-[11px] font-semibold" style={{ color }}>
        {teamShort}
      </span>
    </div>
  );
};

const AccuratePassesViz = ({ teamA, teamB }) => {
  const totalA = teamA?.topStats?.totalPasses || 0;
  const accA   = teamA?.topStats?.accuratePasses || 0;
  const totalB = teamB?.topStats?.totalPasses || 0;
  const accB   = teamB?.topStats?.accuratePasses || 0;

  const shortA = teamA?.teamShort || teamA?.teamName?.substring(0, 3).toUpperCase() || 'HME';
  const shortB = teamB?.teamShort || teamB?.teamName?.substring(0, 3).toUpperCase() || 'AWY';

  return (
    <div className="w-full flex flex-col gap-1">
      {/* Donuts */}
      <div className="flex items-center justify-around py-2">
        <DonutRing
          total={totalA}
          accurate={accA}
          color={TEAM_A_COLOR}
          teamShort={shortA}
        />

        <div className="text-[10px] font-bold text-[#71717A] tracking-widest uppercase">VS</div>

        <DonutRing
          total={totalB}
          accurate={accB}
          color={TEAM_B_COLOR}
          teamShort={shortB}
        />
      </div>
    </div>
  );
};

export default AccuratePassesViz;
