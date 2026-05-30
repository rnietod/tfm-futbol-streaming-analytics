import React, { useMemo } from 'react';

// ============================================================
// SHOT MAP VISUALIZATION
// Left: Half-pitch top-down showing shot origins
// Right: Goal mouth front view showing where shots ended
// ============================================================

const TEAM_A_COLOR = '#006FEE';
const TEAM_B_COLOR = '#f31260';
const GOAL_COLOR = '#00E676';
const MISS_COLOR = '#666';

// --- HALF PITCH (only attacking half, top-down view) ---
const HalfPitch = ({ shots, teamAName, teamBName, hoveredIdx, setHoveredIdx }) => {
  // ViewBox: we show only x=50..100, y=0..100 (attacking half)
  // Map to SVG coords: svgX = (y/100)*pitchW, svgY = (1 - (x-50)/50)*pitchH
  const PW = 200;
  const PH = 160;
  const PAD = 12;

  const toSvg = (pitchX, pitchY) => ({
    sx: PAD + (pitchY / 100) * PW,
    sy: PAD + (1 - (pitchX - 50) / 50) * PH,
  });

  // Penalty area proportions (relative to pitch)
  const paW = PW * (40.32 / 68);
  const paH = PH * (16.5 / 52.5);
  const gaW = PW * (18.32 / 68);
  const gaH = PH * (5.5 / 52.5);

  return (
    <svg viewBox={`0 0 ${PW + PAD * 2} ${PH + PAD * 2}`} className="w-full h-full">
      {/* Background */}
      <rect x={PAD} y={PAD} width={PW} height={PH} rx="2"
        fill="rgba(20,60,30,0.35)" stroke="rgba(255,255,255,0.25)" strokeWidth="0.8" />

      {/* Halfway line (bottom) */}
      <line x1={PAD} y1={PAD + PH} x2={PAD + PW} y2={PAD + PH}
        stroke="rgba(255,255,255,0.2)" strokeWidth="0.6" />

      {/* Penalty area */}
      <rect x={PAD + (PW - paW) / 2} y={PAD} width={paW} height={paH}
        fill="none" stroke="rgba(255,255,255,0.2)" strokeWidth="0.6" />

      {/* 6-yard box */}
      <rect x={PAD + (PW - gaW) / 2} y={PAD} width={gaW} height={gaH}
        fill="none" stroke="rgba(255,255,255,0.2)" strokeWidth="0.6" />

      {/* Goal line (top) */}
      <rect x={PAD + (PW - gaW) / 2} y={PAD - 4} width={gaW} height={4}
        fill="none" stroke="rgba(255,255,255,0.3)" strokeWidth="0.5" />

      {/* Penalty spot */}
      <circle cx={PAD + PW / 2} cy={PAD + PH * (11 / 52.5)} r="1.2"
        fill="rgba(255,255,255,0.2)" />

      {/* Center circle arc */}
      <path
        d={`M ${PAD + PW / 2 - PH * (9.15 / 52.5)} ${PAD + PH}
            A ${PH * (9.15 / 52.5)} ${PH * (9.15 / 52.5)} 0 0 1
            ${PAD + PW / 2 + PH * (9.15 / 52.5)} ${PAD + PH}`}
        fill="none" stroke="rgba(255,255,255,0.15)" strokeWidth="0.6"
      />

      {/* Shots */}
      {shots.map((shot, i) => {
        if (shot.pitchX < 50) return null; // only attacking half
        const { sx, sy } = toSvg(shot.pitchX, shot.pitchY);
        const isTeamA = shot.team === teamAName;
        const baseColor = isTeamA ? TEAM_A_COLOR : TEAM_B_COLOR;
        const color = shot.isGoal ? GOAL_COLOR : shot.onTarget ? baseColor : MISS_COLOR;
        const r = shot.isGoal ? 5.5 : shot.onTarget ? 4 : 3.2;
        const isHovered = hoveredIdx === i;

        return (
          <g key={i}
            onMouseEnter={() => setHoveredIdx(i)}
            onMouseLeave={() => setHoveredIdx(null)}
            style={{ cursor: 'pointer' }}
          >
            {/* Glow */}
            {(shot.isGoal || isHovered) && (
              <circle cx={sx} cy={sy} r={r + 4}
                fill={color} fillOpacity={0.15} />
            )}
            <circle cx={sx} cy={sy} r={r}
              fill={shot.isGoal ? GOAL_COLOR : 'rgba(0,0,0,0.7)'}
              stroke={color} strokeWidth={isHovered ? 1.5 : 0.8}
              fillOpacity={shot.isGoal ? 0.9 : 0.8}
            />
            {shot.isGoal && (
              <text x={sx} y={sy + 1.2} textAnchor="middle" dominantBaseline="middle"
                fontSize="4" fill="black" fontWeight="900" style={{ fontFamily: 'Inter, sans-serif' }}>
                G
              </text>
            )}
            {/* Tooltip on hover */}
            {isHovered && (
              <g>
                <rect x={sx + 6} y={sy - 18} width="52" height="16" rx="3"
                  fill="rgba(0,0,0,0.9)" stroke={color} strokeWidth="0.5" />
                <text x={sx + 8} y={sy - 10} fontSize="3.5" fill="white"
                  style={{ fontFamily: 'Inter, sans-serif' }}>
                  {shot.player?.split(' ').pop()} {shot.minute}'
                </text>
                <text x={sx + 8} y={sy - 5} fontSize="3" fill={color}
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

// --- GOAL MOUTH (front view) ---
const GoalMouth = ({ shots, teamAName, teamBName, hoveredIdx, setHoveredIdx }) => {
  const GW = 200;
  const GH = 100;
  const PAD = 16;
  const POST_W = 3;

  return (
    <svg viewBox={`0 0 ${GW + PAD * 2} ${GH + PAD * 2 + 10}`} className="w-full h-full">
      {/* Net background pattern */}
      <defs>
        <pattern id="netPattern" x="0" y="0" width="10" height="10" patternUnits="userSpaceOnUse">
          <line x1="0" y1="0" x2="10" y2="10" stroke="rgba(255,255,255,0.06)" strokeWidth="0.3" />
          <line x1="10" y1="0" x2="0" y2="10" stroke="rgba(255,255,255,0.06)" strokeWidth="0.3" />
        </pattern>
      </defs>

      {/* Goal frame */}
      <rect x={PAD} y={PAD} width={GW} height={GH} rx="1"
        fill="url(#netPattern)" stroke="rgba(255,255,255,0.4)" strokeWidth="1.5" />

      {/* Crossbar */}
      <rect x={PAD - POST_W / 2} y={PAD - POST_W} width={GW + POST_W} height={POST_W}
        fill="rgba(255,255,255,0.5)" rx="1" />

      {/* Left post */}
      <rect x={PAD - POST_W} y={PAD - POST_W} width={POST_W} height={GH + POST_W + 2}
        fill="rgba(255,255,255,0.4)" rx="1" />

      {/* Right post */}
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

      {/* Shots on/near goal */}
      {shots.map((shot, i) => {
        const sx = PAD + (shot.goalX / 100) * GW;
        const sy = PAD + (shot.goalY / 100) * GH;
        const isTeamA = shot.team === teamAName;
        const baseColor = isTeamA ? TEAM_A_COLOR : TEAM_B_COLOR;
        const color = shot.isGoal ? GOAL_COLOR : shot.onTarget ? baseColor : MISS_COLOR;
        const r = shot.isGoal ? 7 : shot.onTarget ? 5.5 : 4;
        const isHovered = hoveredIdx === i;

        // Off-target shots appear outside the frame
        const clampedX = shot.onTarget ? sx : Math.max(PAD - 8, Math.min(PAD + GW + 8, sx));
        const clampedY = shot.onTarget ? sy : Math.max(PAD - 8, Math.min(PAD + GH + 8, sy));

        return (
          <g key={i}
            onMouseEnter={() => setHoveredIdx(i)}
            onMouseLeave={() => setHoveredIdx(null)}
            style={{ cursor: 'pointer' }}
          >
            {(shot.isGoal || isHovered) && (
              <circle cx={clampedX} cy={clampedY} r={r + 5}
                fill={color} fillOpacity={0.12} />
            )}
            <circle cx={clampedX} cy={clampedY} r={r}
              fill={shot.isGoal ? GOAL_COLOR : 'rgba(0,0,0,0.75)'}
              stroke={color}
              strokeWidth={isHovered ? 2 : 1}
              fillOpacity={shot.isGoal ? 0.9 : shot.onTarget ? 0.7 : 0.4}
              strokeDasharray={shot.onTarget ? 'none' : '2,2'}
            />
            {shot.isGoal && (
              <text x={clampedX} y={clampedY + 1.5} textAnchor="middle" dominantBaseline="middle"
                fontSize="5" fill="black" fontWeight="900" style={{ fontFamily: 'Inter, sans-serif' }}>
                G
              </text>
            )}
          </g>
        );
      })}

      {/* Label */}
      <text x={PAD + GW / 2} y={PAD + GH + 14} textAnchor="middle"
        fontSize="5" fill="rgba(255,255,255,0.3)" fontWeight="600"
        style={{ fontFamily: 'Inter, sans-serif', letterSpacing: '2px' }}>
        GOAL MOUTH VIEW
      </text>
    </svg>
  );
};

// --- LEGEND ---
const ShotLegend = ({ teamAShort, teamBShort }) => (
  <div className="flex flex-wrap justify-center gap-6 text-[11px] text-[#71717A] mt-2 font-medium">
    <div className="flex items-center gap-1.5">
      <div className="w-2 h-2 rounded-full" style={{ backgroundColor: GOAL_COLOR }} />
      Goal
    </div>
    <div className="flex items-center gap-1.5">
      <div className="w-2 h-2 rounded-full" style={{ backgroundColor: TEAM_A_COLOR }} />
      <span style={{ color: TEAM_A_COLOR }}>{teamAShort}</span>
    </div>
    <div className="flex items-center gap-1.5">
      <div className="w-2 h-2 rounded-full" style={{ backgroundColor: TEAM_B_COLOR }} />
      <span style={{ color: TEAM_B_COLOR }}>{teamBShort}</span>
    </div>
    <div className="flex items-center gap-1.5">
      <div className="w-2 h-2 rounded-full border border-white/40" />
      Off target
    </div>
  </div>
);


// ============================================================
// MAIN EXPORT
// ============================================================
const ShotMapViz = ({ shotData, teamAShort, teamBShort }) => {
  const [hoveredIdx, setHoveredIdx] = React.useState(null);

  const shots = shotData?.shots || [];
  const teamAName = shotData?.home || '';
  const teamBName = shotData?.away || '';

  const teamAShots = useMemo(() => shots.filter(s => s.team === teamAName), [shots, teamAName]);
  const teamBShots = useMemo(() => shots.filter(s => s.team !== teamAName), [shots, teamAName]);

  const displayA = teamAShort || teamAName?.substring(0, 3).toUpperCase();
  const displayB = teamBShort || teamBName?.substring(0, 3).toUpperCase();

  if (!shots.length) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-[10px] text-zinc-600 uppercase tracking-widest font-bold">
          No shots yet
        </span>
      </div>
    );
  }

  return (
    <div className="w-full flex flex-col gap-2">
      {/* Shot counts */}
      <div className="flex items-center justify-end gap-3 px-1">
        <span className="text-[10px] font-bold" style={{ color: TEAM_A_COLOR }}>
          {teamAShots.length} shots
        </span>
        <span className="text-[10px] text-zinc-600">vs</span>
        <span className="text-[10px] font-bold" style={{ color: TEAM_B_COLOR }}>
          {teamBShots.length} shots
        </span>
      </div>

      {/* Viz grid — matches TFM grid grid-cols-1 md:grid-cols-2 */}
      <div className="grid grid-cols-2 gap-3">
        {/* Field View */}
        <div
          className="relative w-full rounded-xl border border-white/5 bg-[#18181B] overflow-hidden flex items-center justify-center shadow-inner"
          style={{ aspectRatio: '1.5' }}
        >
          <HalfPitch
            shots={shots} teamAName={teamAName} teamBName={teamBName}
            hoveredIdx={hoveredIdx} setHoveredIdx={setHoveredIdx}
          />
        </div>

        {/* Goal View */}
        <div
          className="relative w-full rounded-xl bg-[#18181B] border border-white/5 overflow-hidden flex flex-col items-center justify-end shadow-inner"
          style={{ aspectRatio: '1.5' }}
        >
          <GoalMouth
            shots={shots} teamAName={teamAName} teamBName={teamBName}
            hoveredIdx={hoveredIdx} setHoveredIdx={setHoveredIdx}
          />
        </div>
      </div>

      <ShotLegend teamAShort={displayA} teamBShort={displayB} />
    </div>
  );
};

export default ShotMapViz;
