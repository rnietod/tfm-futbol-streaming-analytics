import React, { useMemo } from 'react';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
} from 'recharts';

// ============================================================
// MOMENTUM CHART — Net dominance per minute
//
// value = away_passes_rival_half - home_passes_rival_half
//   > 0  → Real Madrid dominates  (RED  fills above zero)
//   < 0  → Atlético dominates     (BLUE fills below zero)
//
// Goals are marked with a small coloured ball:
//   • Madrid goal → ball ABOVE the chart
//   • Atlético goal → ball BELOW the chart
// ============================================================

const TEAM_A_COLOR = '#006FEE'; // Atlético  (home)  — negative zone
const TEAM_B_COLOR = '#f31260'; // Real Madrid (away) — positive zone

// ── Position of zero on the 0→1 fill gradient ───────────────
const calcGradientOffset = (data) => {
  if (!data.length) return 0.5;
  const maxVal = Math.max(...data.map((d) => d.value));
  const minVal = Math.min(...data.map((d) => d.value));
  if (maxVal <= 0) return 0;
  if (minVal >= 0) return 1;
  return maxVal / (maxVal - minVal);
};

// ── Custom SVG goal marker rendered inside a ReferenceLine ──
//    Recharts injects `viewBox` automatically.
//    isAway = true  → Real Madrid goal → ball ABOVE (top of chart)
//    isAway = false → Atlético goal   → ball BELOW (bottom of chart)
const GoalMarker = ({ viewBox, isAway, color, player, minute }) => {
  if (!viewBox) return null;
  const { x, y, height } = viewBox;

  // Ball position: above top edge for Madrid, below bottom edge for Atlético
  const BALL_R   = 5.5;
  const OFFSET   = 11;                                           // px from chart edge
  const ballY    = isAway ? y - OFFSET : y + height + OFFSET;

  // Subtle vertical dashed line at goal minute
  const lineColor = `${color}55`;

  return (
    <g>
      {/* Reference line */}
      <line
        x1={x} y1={y}
        x2={x} y2={y + height}
        stroke={lineColor}
        strokeWidth={1}
        strokeDasharray="3 3"
      />

      {/* Glow halo */}
      <circle
        cx={x} cy={ballY} r={BALL_R + 4}
        fill={color} fillOpacity={0.18}
      />

      {/* Ball body */}
      <circle
        cx={x} cy={ballY} r={BALL_R}
        fill={color} fillOpacity={0.95}
      />

      {/* Tiny ⚽ pentagon detail */}
      <circle
        cx={x} cy={ballY} r={BALL_R}
        fill="none" stroke="rgba(0,0,0,0.35)" strokeWidth={0.8}
      />
      <circle
        cx={x} cy={ballY} r={BALL_R * 0.38}
        fill="rgba(0,0,0,0.25)"
      />

      {/* Minute label beside the ball */}
      <text
        x={x + BALL_R + 3}
        y={ballY + 3.5}
        fontSize="7"
        fill={color}
        fontWeight="700"
        fontFamily="'JetBrains Mono', monospace"
        opacity={0.85}
      >
        {minute}'
      </text>
    </g>
  );
};

// ── Main component ───────────────────────────────────────────
const MomentumChart = ({ momentumData, shotData, teamAShort, teamBShort }) => {
  const homeName = momentumData?.home || '';   // Atlético
  const awayName = momentumData?.away || '';   // Real Madrid
  const rawData  = momentumData?.data  || [];

  const nameA = teamAShort || homeName?.substring(0, 3).toUpperCase() || 'HME';
  const nameB = teamBShort || awayName?.substring(0, 3).toUpperCase() || 'AWY';

  // ── Smoothed net value (3-min rolling avg) ─────────────────
  // net = away − home  →  positive = Madrid dominant
  const data = useMemo(() => {
    if (!rawData.length) return [];
    const W = 3;
    return rawData.map((d, i) => {
      let sum = 0, count = 0;
      for (let j = Math.max(0, i - W + 1); j <= i; j++) {
        sum += (rawData[j].away || 0) - (rawData[j].home || 0);
        count++;
      }
      return {
        time:  `${d.minute}'`,
        value: parseFloat((sum / count).toFixed(2)),
        minute: d.minute,
      };
    });
  }, [rawData]);

  // ── Extract goals from shotData ────────────────────────────
  const goals = useMemo(() => {
    const shots = shotData?.shots || [];
    const sHome = shotData?.home || homeName;   // home team name from shots API
    return shots
      .filter((s) => s.isGoal)
      .map((s) => {
        // Away = Real Madrid (teamB) when team !== home team
        const isAway = !sHome || !s.team?.toLowerCase().includes(sHome.toLowerCase().split(' ')[0]);
        return {
          minute: s.minute,
          timeKey: `${s.minute}'`,
          player: s.player,
          isAway,                              // true = Madrid, false = Atlético
          color: isAway ? TEAM_B_COLOR : TEAM_A_COLOR,
        };
      });
  }, [shotData, homeName]);

  const off = calcGradientOffset(data);

  // ── Empty state ────────────────────────────────────────────
  if (!rawData.length) {
    return (
      <div className="flex items-center justify-center h-44">
        <span className="text-[10px] text-zinc-600 uppercase tracking-widest font-bold">
          Waiting for data...
        </span>
      </div>
    );
  }

  return (
    <div className="w-full flex flex-col gap-1">

      {/* Team legend */}
      <div className="flex items-center justify-end gap-3 text-[10px] font-medium px-1 mb-1">
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 rounded-[4px]" style={{ backgroundColor: TEAM_A_COLOR }} />
          <span style={{ color: TEAM_A_COLOR }}>{nameA}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 rounded-[4px]" style={{ backgroundColor: TEAM_B_COLOR }} />
          <span style={{ color: TEAM_B_COLOR }}>{nameB}</span>
        </div>
      </div>

      {/* Chart — extra top/bottom margin for goal ball overflow */}
      <div className="w-full" style={{ height: '176px' }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={data}
            margin={{ top: 20, right: 6, left: -20, bottom: 20 }}
          >
            <defs>
              {/*
                TOP  (positive, above zero) → RED   = Real Madrid
                BOTTOM (negative, below 0)  → BLUE  = Atlético
              */}
              <linearGradient id="momentumFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%"  stopColor={TEAM_B_COLOR} stopOpacity={0.65} />
                <stop offset={off} stopColor={TEAM_B_COLOR} stopOpacity={0.05} />
                <stop offset={off} stopColor={TEAM_A_COLOR} stopOpacity={0.05} />
                <stop offset="100%" stopColor={TEAM_A_COLOR} stopOpacity={0.65} />
              </linearGradient>
              <linearGradient id="momentumStroke" x1="0" y1="0" x2="0" y2="1">
                <stop offset={off} stopColor={TEAM_B_COLOR} stopOpacity={1} />
                <stop offset={off} stopColor={TEAM_A_COLOR} stopOpacity={1} />
              </linearGradient>
            </defs>

            <XAxis
              dataKey="time"
              axisLine={false}
              tickLine={false}
              tick={{
                fill: '#71717A',
                fontSize: 9,
                fontFamily: "'JetBrains Mono', 'Fira Mono', monospace",
              }}
              dy={6}
              minTickGap={12}
            />
            <YAxis hide />

            <Tooltip
              contentStyle={{
                backgroundColor: '#18181B',
                border: '1px solid rgba(255,255,255,0.05)',
                borderRadius: '8px',
                color: '#ECEDEE',
                fontSize: '11px',
              }}
              itemStyle={{
                color: '#ECEDEE',
                fontFamily: "'JetBrains Mono', monospace",
              }}
              labelStyle={{ color: '#71717A', fontSize: '9px' }}
              cursor={{
                stroke: 'rgba(255,255,255,0.1)',
                strokeWidth: 1,
                strokeDasharray: '4 4',
              }}
              formatter={(value) => {
                const abs  = Math.abs(value).toFixed(1);
                const team = value >= 0 ? nameB : nameA;
                const col  = value >= 0 ? TEAM_B_COLOR : TEAM_A_COLOR;
                return [<span style={{ color: col }}>{abs}</span>, team];
              }}
            />

            {/* Zero baseline */}
            <ReferenceLine
              y={0}
              stroke="rgba(255,255,255,0.12)"
              strokeDasharray="3 3"
            />

            <Area
              type="monotone"
              dataKey="value"
              stroke="url(#momentumStroke)"
              strokeWidth={2.5}
              fill="url(#momentumFill)"
              isAnimationActive={false}
              dot={false}
            />

            {/* ── Goal markers (rendered above the Area) ──────── */}
            {goals.map((goal, i) => (
              <ReferenceLine
                key={`goal-${i}`}
                x={goal.timeKey}
                stroke="transparent"
                label={
                  <GoalMarker
                    isAway={goal.isAway}
                    color={goal.color}
                    player={goal.player}
                    minute={goal.minute}
                  />
                }
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Caption */}
      <p className="text-[8px] text-zinc-600 text-center italic">
        Net passes opp. half · +{nameB} / −{nameA} · ⚽ = goal (3-min avg)
      </p>
    </div>
  );
};

export default MomentumChart;
