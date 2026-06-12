import React, { useMemo, useState, useRef } from 'react';
import { Zap } from 'lucide-react';

// ============================================================
// PHYSICAL METRICS — Player load breakdown + top speed board
// Data sources:
//   GET /match/{id}/tracking/metrics                  (compact, all players)
//   GET /match/{id}/player/{pid}/tracking/metrics     (detail on hover: speed_per_second)
// ============================================================

const API_BASE = 'http://127.0.0.1:8000';

const COLOR_A = '#006FEE';
const COLOR_B = '#f31260';
const WALK_COLOR = '#3f3f46';   // zinc-700
const SPRINT_COLOR = '#FACC15'; // yellow — keeps tiny sprint segments visible

const km = (m) => (m / 1000).toFixed(1);
const distFmt = (m) => (m >= 1000 ? `${km(m)} km` : `${Math.round(m)} m`);
const clock = (s) => `${Math.floor(s / 60)}:${String(Math.round(s) % 60).padStart(2, '0')}`;

const MONO = { fontFamily: "'JetBrains Mono', 'Fira Mono', monospace" };

// ── Speed timeseries sparkline (bucketed by max to preserve peaks) ──
// Exported: reused by PlayerDeepDive's Physical section
export const SpeedSparkline = ({ speedSeries, vmaxMs, color }) => {
  const W = 248;
  const H = 44;
  const { path, sprintY, peak } = useMemo(() => {
    const n = speedSeries.length;
    const bucket = Math.max(1, Math.ceil(n / 124));
    const pts = [];
    for (let i = 0; i < n; i += bucket) {
      let mx = 0;
      for (let j = i; j < Math.min(i + bucket, n); j++) mx = Math.max(mx, speedSeries[j].speed_ms || 0);
      pts.push(mx);
    }
    const maxV = Math.max(vmaxMs || 0, ...pts) || 1;
    const x = (i) => (i / Math.max(1, pts.length - 1)) * W;
    const y = (v) => H - (v / maxV) * (H - 6) - 3;
    const path = pts.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
    const peakIdx = pts.indexOf(Math.max(...pts));
    return {
      path,
      sprintY: y(0.75 * (vmaxMs || maxV)),
      peak: { x: x(peakIdx), y: y(pts[peakIdx]) },
    };
  }, [speedSeries, vmaxMs]);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: H }}>
      {/* Sprint threshold (75% of personal v_max) */}
      <line x1="0" y1={sprintY} x2={W} y2={sprintY}
        stroke={SPRINT_COLOR} strokeOpacity="0.35" strokeWidth="0.8" strokeDasharray="3,3" />
      <polyline points={path} fill="none" stroke={color} strokeWidth="1.2"
        strokeLinejoin="round" strokeLinecap="round" />
      {/* Peak speed marker */}
      <circle cx={peak.x} cy={peak.y} r="2.2" fill={SPRINT_COLOR} stroke="rgba(0,0,0,0.6)" strokeWidth="0.8" />
    </svg>
  );
};

// ── One zone line inside the hover card: dot + label + distance + time ──
const ZoneRow = ({ color, label, dist, secs, extra }) => (
  <div className="flex items-center gap-2 py-[5px] border-b border-white/[0.04] last:border-0">
    <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
    <span className="w-[38px] text-[8px] font-bold text-zinc-400 uppercase tracking-wider flex-shrink-0">{label}</span>
    <span className="w-[54px] text-[10px] font-medium text-white tabular-nums flex-shrink-0" style={MONO}>
      {distFmt(dist)}
    </span>
    <span className="w-[44px] text-[9px] text-zinc-500 tabular-nums flex-shrink-0" style={MONO}>
      {secs != null ? clock(secs) : '--:--'}
    </span>
    {extra && (
      <span className="ml-auto flex items-center gap-1 text-[9px] font-bold tabular-nums" style={{ ...MONO, color: SPRINT_COLOR }}>
        <Zap size={9} />
        {extra}
      </span>
    )}
  </div>
);

// ── Floating detail card shown while hovering a player row ──
const PlayerDetailCard = ({ row, teamLabel, detail }) => {
  const color = row.team === 'A' ? COLOR_A : COLOR_B;

  // Seconds spent in each speed zone, same thresholds as the backend engine
  // (walk < 25% v_max · run 25–75% · sprint > 75% of personal v_max)
  const zoneTimes = useMemo(() => {
    const series = detail?.speed_per_second;
    const vmax = detail?.max_speed_ms;
    if (!series?.length || !vmax) return null;
    let walk = 0, run = 0, sprint = 0;
    series.forEach(s => {
      const v = s.speed_ms || 0;
      if (v > 0.75 * vmax) sprint += 1;
      else if (v >= 0.25 * vmax) run += 1;
      else walk += 1;
    });
    return { walk, run, sprint, total: series.length };
  }, [detail]);

  return (
    <div className="bg-zinc-950/95 backdrop-blur-2xl border border-white/10 rounded-xl p-3 shadow-2xl">
      {/* Header: player identity + total distance/time */}
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span
            className="w-6 h-6 rounded-md flex items-center justify-center text-[9px] font-black flex-shrink-0"
            style={{ backgroundColor: `${color}22`, border: `1px solid ${color}44`, color }}
          >
            {row.number ?? '–'}
          </span>
          <div>
            <div className="text-[10px] font-black text-white leading-tight">{row.name}</div>
            <div className="text-[7px] font-bold text-zinc-500 uppercase tracking-widest">{teamLabel}</div>
          </div>
        </div>
        <div className="text-right">
          <div className="text-sm font-bold text-white tabular-nums" style={MONO}>{km(row.dist)} km</div>
          <div className="text-[7px] text-zinc-500 uppercase font-bold tracking-widest">
            Total · {zoneTimes ? clock(zoneTimes.total) : '--:--'} min
          </div>
        </div>
      </div>

      {/* Speed profile sparkline */}
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[7px] font-bold text-zinc-500 uppercase tracking-widest">Speed Profile</span>
        <span className="text-[7px] font-bold uppercase tracking-widest" style={{ color: SPRINT_COLOR, opacity: 0.7 }}>
          - - 75% Vmax
        </span>
      </div>
      {detail?.speed_per_second?.length ? (
        <SpeedSparkline speedSeries={detail.speed_per_second} vmaxMs={detail.max_speed_ms} color={color} />
      ) : (
        <div className="h-[44px] flex items-center justify-center">
          <div className="w-3.5 h-3.5 border-2 border-zinc-600 border-t-transparent rounded-full animate-spin" />
        </div>
      )}

      {/* Zone breakdown: distance + time per zone, Vmax on the sprint row */}
      <div className="mt-2 pt-1 border-t border-white/5">
        <div className="flex items-center gap-2 pb-0.5">
          <div className="w-2 flex-shrink-0" />
          <span className="w-[38px] text-[6px] font-bold text-zinc-600 uppercase tracking-widest flex-shrink-0">Zone</span>
          <span className="w-[54px] text-[6px] font-bold text-zinc-600 uppercase tracking-widest flex-shrink-0">Dist</span>
          <span className="w-[44px] text-[6px] font-bold text-zinc-600 uppercase tracking-widest flex-shrink-0">Time</span>
        </div>
        <ZoneRow color={WALK_COLOR}   label="Walk"   dist={row.walk}   secs={zoneTimes?.walk}   />
        <ZoneRow color={color}        label="Run"    dist={row.run}    secs={zoneTimes?.run}    />
        <ZoneRow color={SPRINT_COLOR} label="Sprint" dist={row.sprint} secs={zoneTimes?.sprint}
          extra={`${row.topSpeed.toFixed(1)} km/h · ${Math.floor((row.topSpeedSecond || 0) / 60)}'`} />
      </div>
    </div>
  );
};

// ── Single player row: number chip + name + stacked walk/run/sprint bar ──
const PlayerLoadRow = ({ row, maxDist, color, isHovered, onHover }) => {
  const pct = (v) => `${(v / maxDist) * 100}%`;
  return (
    <div
      className={`flex items-center gap-2 py-[3px] rounded-md px-1 -mx-1 transition-colors duration-150 cursor-default ${isHovered ? 'bg-white/[0.06]' : ''}`}
      onMouseEnter={() => onHover(row)}
    >
      <span
        className="w-5 h-5 rounded-md flex items-center justify-center text-[8px] font-black flex-shrink-0"
        style={{ backgroundColor: `${color}22`, border: `1px solid ${color}44`, color }}
      >
        {row.number ?? '–'}
      </span>
      <span className="w-[70px] flex-shrink-0 text-[9px] font-bold text-zinc-400 truncate uppercase tracking-wide">
        {row.name}
      </span>
      <div className="flex-1 h-2.5 bg-zinc-950/60 border border-white/5 rounded-full overflow-hidden flex">
        <div className="h-full" style={{ width: pct(row.walk), backgroundColor: WALK_COLOR }} />
        <div className="h-full" style={{ width: pct(row.run), backgroundColor: color, opacity: 0.85 }} />
        <div className="h-full" style={{ width: pct(row.sprint), backgroundColor: SPRINT_COLOR }} />
      </div>
      <span
        className="w-[52px] text-right text-[9px] font-medium text-zinc-300 tabular-nums flex-shrink-0"
        style={MONO}
      >
        {km(row.dist)} km
      </span>
    </div>
  );
};

const LegendDot = ({ style, label }) => (
  <div className="flex items-center gap-1.5">
    <div className="w-2 h-2 rounded-full" style={style} />
    {label}
  </div>
);

const PhysicalMetricsViz = ({ trackingMetrics, players, teamA, teamB, matchId }) => {
  const [hovered, setHovered] = useState(null);
  const [detail, setDetail] = useState(null);
  const hoveredIdRef = useRef(null);
  const detailCacheRef = useRef({});

  const { rowsA, rowsB, maxDist, topSpeeds } = useMemo(() => {
    const rowsA = [];
    const rowsB = [];
    (trackingMetrics?.players || []).forEach(m => {
      const info = players?.[m.player_id]?.info;
      if (!info) return; // jugador de tracking sin mapear al roster
      const isA = info.teamName === teamA?.teamName;
      const isB = info.teamName === teamB?.teamName;
      if (!isA && !isB) return;
      (isA ? rowsA : rowsB).push({
        id: m.player_id,
        name: info.shortName || info.name || `#${m.player_id}`,
        number: info.number,
        dist: m.total_distance_m || 0,
        walk: m.distance_walk_m || 0,
        run: m.distance_run_m || 0,
        sprint: m.distance_sprint_m || 0,
        topSpeed: m.max_speed_kmh || 0,
        topSpeedSecond: m.max_speed_at_second,
        team: isA ? 'A' : 'B',
      });
    });
    rowsA.sort((a, b) => b.dist - a.dist);
    rowsB.sort((a, b) => b.dist - a.dist);
    const maxDist = Math.max(1, ...rowsA.map(r => r.dist), ...rowsB.map(r => r.dist));
    const topSpeeds = [...rowsA, ...rowsB]
      .sort((a, b) => b.topSpeed - a.topSpeed)
      .slice(0, 5);
    return { rowsA, rowsB, maxDist, topSpeeds };
  }, [trackingMetrics, players, teamA?.teamName, teamB?.teamName]);

  // Hover → fetch full metrics (speed_per_second) on demand, with per-player cache
  const handleHover = (row) => {
    setHovered(row);
    hoveredIdRef.current = row.id;
    const cached = detailCacheRef.current[row.id];
    if (cached) {
      setDetail(cached);
      return;
    }
    setDetail(null);
    fetch(`${API_BASE}/match/${matchId}/player/${row.id}/tracking/metrics`)
      .then(res => res.json())
      .then(data => {
        if (data && !data.error) {
          detailCacheRef.current[row.id] = data;
          if (hoveredIdRef.current === row.id) setDetail(data);
        }
      })
      .catch(err => console.error('❌ player tracking detail error:', err));
  };

  const handleLeave = () => {
    setHovered(null);
    setDetail(null);
    hoveredIdRef.current = null;
  };

  if (!trackingMetrics) {
    return (
      <div className="flex items-center justify-center py-10">
        <div className="w-5 h-5 border-2 border-zinc-600 border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (!rowsA.length && !rowsB.length) {
    return (
      <div className="flex items-center justify-center py-10 text-zinc-600 text-[10px] font-bold uppercase tracking-wider">
        No tracking data available
      </div>
    );
  }

  const maxTopSpeed = topSpeeds[0]?.topSpeed || 1;

  return (
    <>
      {/* ── PLAYER LOAD — stacked distance bars per player ── */}
      <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg mt-3 relative">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase">Player Load</h3>
          <div className="flex gap-3 text-[8px] text-zinc-500 font-bold uppercase select-none">
            <LegendDot style={{ backgroundColor: WALK_COLOR }} label="Walk" />
            <LegendDot style={{ background: `linear-gradient(90deg, ${COLOR_A} 50%, ${COLOR_B} 50%)` }} label="Run" />
            <LegendDot style={{ backgroundColor: SPRINT_COLOR }} label="Sprint" />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4" onMouseLeave={handleLeave}>
          <div>
            <div className="text-[9px] font-bold tracking-wider uppercase mb-1.5" style={{ color: COLOR_A }}>
              {teamA?.teamShort || teamA?.teamName}
            </div>
            {rowsA.map(row => (
              <PlayerLoadRow key={row.id} row={row} maxDist={maxDist} color={COLOR_A}
                isHovered={hovered?.id === row.id} onHover={handleHover} />
            ))}
          </div>
          <div>
            <div className="text-[9px] font-bold tracking-wider uppercase mb-1.5" style={{ color: COLOR_B }}>
              {teamB?.teamShort || teamB?.teamName}
            </div>
            {rowsB.map(row => (
              <PlayerLoadRow key={row.id} row={row} maxDist={maxDist} color={COLOR_B}
                isHovered={hovered?.id === row.id} onHover={handleHover} />
            ))}
          </div>
        </div>

        {/* Floating detail card — over the opposite column so the hovered row stays visible */}
        {hovered && (
          <div
            className={`absolute top-9 z-30 w-[300px] pointer-events-none ${hovered.team === 'A' ? 'right-3' : 'left-3'}`}
          >
            <PlayerDetailCard
              row={hovered}
              teamLabel={hovered.team === 'A' ? (teamA?.teamName || '') : (teamB?.teamName || '')}
              detail={detail}
            />
          </div>
        )}
      </div>

      {/* ── FASTEST PLAYERS — top 5 max speeds ── */}
      <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg mt-3 mb-2">
        <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase mb-2">Fastest Players</h3>
        {topSpeeds.map((p, i) => {
          const color = p.team === 'A' ? COLOR_A : COLOR_B;
          return (
            <div key={p.id} className="flex items-center gap-2.5 py-1.5 border-b border-white/[0.04] last:border-0">
              <span className="w-4 text-center text-[10px] font-black text-zinc-600 tabular-nums flex-shrink-0">
                {i === 0 ? <Zap size={11} className="text-yellow-400 inline" /> : i + 1}
              </span>
              <span
                className="w-5 h-5 rounded-md flex items-center justify-center text-[8px] font-black flex-shrink-0"
                style={{ backgroundColor: `${color}22`, border: `1px solid ${color}44`, color }}
              >
                {p.number ?? '–'}
              </span>
              <span className="w-[110px] flex-shrink-0 text-[10px] font-bold text-zinc-300 truncate">
                {p.name}
              </span>
              <div className="flex-1 h-1.5 bg-zinc-950/60 border border-white/5 rounded-full overflow-hidden">
                <div
                  className="h-full rounded-full transition-all duration-700"
                  style={{ width: `${(p.topSpeed / maxTopSpeed) * 100}%`, backgroundColor: color }}
                />
              </div>
              <span className="text-[9px] text-zinc-500 font-bold tabular-nums flex-shrink-0">
                {Math.floor((p.topSpeedSecond || 0) / 60)}'
              </span>
              <span
                className="w-[64px] text-right text-[11px] font-medium text-white tabular-nums flex-shrink-0"
                style={MONO}
              >
                {p.topSpeed.toFixed(1)} km/h
              </span>
            </div>
          );
        })}
      </div>
    </>
  );
};

export default PhysicalMetricsViz;
