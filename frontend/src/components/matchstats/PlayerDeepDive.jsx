import React, { useState, useMemo } from 'react';
import { Loader2, Zap } from 'lucide-react';
import PlayerPitchAnalytics from './PlayerPitchAnalytics';
import { SpeedSparkline } from './PhysicalMetricsViz';
import {
  usePlayerTrackingHeatmap,
  usePlayerTrackingDetail,
  useTrackingMetrics,
  useShotMap,
} from '../../hooks/useMatchStats';

// ============================================================
// PLAYER DEEP DIVE — mirrors the Team Overview design system:
//   · same cards (#18181B/80 + blur + border-white/5)
//   · same pill section tabs (Attacking / Passing / Defending / Physical)
//   · same StatRow rhythm, but player vs squad-best instead of A vs B
//   · pitch panel: tracking heatmap + eventing pass/touch layers
// ============================================================

const MONO = { fontFamily: "'JetBrains Mono', 'Fira Mono', monospace" };
const WALK_COLOR = '#3f3f46';
const SPRINT_COLOR = '#FACC15';

const km = (m) => (m == null ? null : m / 1000);
const clock = (s) => `${Math.floor(s / 60)}:${String(Math.round(s) % 60).padStart(2, '0')}`;

const fmtValue = (v, format) => {
  if (v == null) return '–';
  if (format === 'decimal') return parseFloat(v || 0).toFixed(2);
  if (format === 'pct') return `${v ?? 0}%`;
  if (format === 'km') return `${parseFloat(v || 0).toFixed(1)} km`;
  if (format === 'kmh') return `${parseFloat(v || 0).toFixed(1)} km/h`;
  return v ?? 0;
};

// ── VALUE PILL — identical to Team Overview's StatRow pill ──
const ValuePill = ({ value, format, dim = false }) => (
  <div
    className="min-w-[44px] h-[26px] px-1.5 flex items-center justify-center rounded-lg bg-zinc-950/80 border border-white/5 text-[11px] font-medium tabular-nums shadow-sm flex-shrink-0"
    style={{ ...MONO, color: dim ? '#71717a' : '#ECEDEE' }}
  >
    {fmtValue(value, format)}
  </div>
);

// ── PLAYER STAT ROW — same dual-bar layout as StatRow:
//    left = player (team color) · right = squad best (reference, zinc) ──
const PlayerStatRow = ({ label, value, best, format = 'number', rank, squadSize, teamColor }) => {
  const numP = parseFloat(value) || 0;
  const numB = parseFloat(best) || 0;
  const maxVal = Math.max(numP, numB, 1);

  return (
    <div className="flex flex-col gap-1 py-1.5 border-b border-white/[0.04] last:border-0">
      <div className="flex justify-between items-center">
        <ValuePill value={value} format={format} />
        <span className="flex-1 text-center text-[11px] font-semibold tracking-wide text-zinc-400 uppercase px-2">
          {label}
          {rank != null && squadSize > 0 && (
            <span
              className="ml-1.5 text-[8px] px-1 py-0.5 rounded font-bold align-middle"
              style={{
                backgroundColor: rank === 1 ? `color-mix(in srgb, ${teamColor} 25%, transparent)` : 'rgba(255,255,255,0.05)',
                color: rank === 1 ? '#fff' : '#71717a',
              }}
            >
              #{rank}
            </span>
          )}
        </span>
        <ValuePill value={best} format={format} dim />
      </div>
      <div className="flex items-center justify-center gap-1.5 px-1">
        {/* Player bar — fills from center outward (left) */}
        <div className="flex-1 h-1.5 bg-zinc-950/60 border border-white/5 rounded-full flex justify-end overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700"
            style={{ width: `${(numP / maxVal) * 100}%`, backgroundColor: teamColor }}
          />
        </div>
        <div className="w-1 h-1 rounded-full bg-zinc-600 flex-shrink-0" />
        {/* Squad-best reference bar */}
        <div className="flex-1 h-1.5 bg-zinc-950/60 border border-white/5 rounded-full flex justify-start overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700 bg-zinc-600"
            style={{ width: `${(numB / maxVal) * 100}%` }}
          />
        </div>
      </div>
    </div>
  );
};

// ── HEADER CARD — scoreboard-style identity + key totals ──
const HeaderPill = ({ label, value, format, accent }) => (
  <div className="flex flex-col items-center gap-1">
    <div
      className="min-w-[64px] h-[26px] px-2 flex items-center justify-center rounded-lg bg-zinc-950/80 border border-white/5 text-[11px] font-medium tabular-nums"
      style={{ ...MONO, color: accent || '#ECEDEE' }}
    >
      {fmtValue(value, format)}
    </div>
    <span className="text-[8px] font-bold text-zinc-500 uppercase tracking-widest">{label}</span>
  </div>
);

const PlayerHeaderCard = ({ player, teamShort, teamColor, physRow }) => {
  const { info, stats } = player;
  return (
    <div className="bg-zinc-900/60 backdrop-blur-sm rounded-xl border border-white/5 px-4 py-2.5 flex-shrink-0 flex items-center justify-between gap-4">
      {/* Identity */}
      <div className="flex items-center gap-3 min-w-0">
        <div
          className="w-11 h-11 rounded-lg flex items-center justify-center text-lg font-black flex-shrink-0"
          style={{
            backgroundColor: `color-mix(in srgb, ${teamColor} 15%, transparent)`,
            border: `1px solid color-mix(in srgb, ${teamColor} 35%, transparent)`,
            color: teamColor,
          }}
        >
          {info.number || '–'}
        </div>
        <div className="min-w-0">
          <h3 className="text-base font-bold text-white leading-tight truncate">{info.name}</h3>
          <p className="text-[10px] text-zinc-400 font-bold tracking-widest uppercase mt-0.5">
            {teamShort} · {info.position} · {stats.minutesPlayed}' played
          </p>
        </div>
      </div>

      {/* Key totals */}
      <div className="flex items-center gap-3 flex-shrink-0">
        <HeaderPill label="Touches" value={stats.touches} />
        <HeaderPill label="Pass Acc" value={stats.passAccuracy} format="pct" />
        <HeaderPill label="Distance" value={km(physRow?.total_distance_m)} format="km" />
        <HeaderPill label="Top Speed" value={physRow?.max_speed_kmh} format="kmh" accent={SPRINT_COLOR} />
      </div>
    </div>
  );
};

// ── SHOT BREAKDOWN — player's shots from the team Shot Map data ──
const ShotBreakdown = ({ shots, teamColor }) => {
  if (!shots.length) {
    return (
      <div className="py-3 text-center text-[10px] font-bold text-zinc-600 uppercase tracking-wider">
        No shots taken yet
      </div>
    );
  }
  return (
    <>
      {shots.map((s, i) => {
        const color = s.isGoal ? '#00E676' : s.onTarget ? teamColor : '#71717a';
        return (
          <div key={i} className="flex items-center gap-2.5 py-1.5 border-b border-white/[0.04] last:border-0">
            <span className="w-7 text-[10px] text-zinc-500 font-bold tabular-nums flex-shrink-0" style={MONO}>
              {s.minute}'
            </span>
            <div
              className="w-2.5 h-2.5 rounded-full flex-shrink-0"
              style={{
                backgroundColor: s.isGoal ? color : 'transparent',
                border: `1.5px solid ${color}`,
              }}
            />
            <span className="flex-1 text-[10px] font-bold text-zinc-300 uppercase tracking-wide truncate">
              {s.isGoal ? 'Goal' : s.outcome}
            </span>
            <span className="text-[9px] text-zinc-500 font-bold uppercase tracking-wider flex-shrink-0">xG</span>
            <span className="w-[44px] text-right text-[11px] font-medium tabular-nums flex-shrink-0" style={{ ...MONO, color: '#ECEDEE' }}>
              {(s.xg ?? 0).toFixed(2)}
            </span>
          </div>
        );
      })}
    </>
  );
};

// ── PHYSICAL DETAIL — zone breakdown + speed profile (tracking) ──
const ZoneLine = ({ color, label, distM, secs }) => (
  <div className="flex items-center gap-2.5 py-1.5 border-b border-white/[0.04] last:border-0">
    <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
    <span className="w-[52px] text-[10px] font-bold text-zinc-400 uppercase tracking-wider flex-shrink-0">{label}</span>
    <div className="flex-1" />
    <span className="text-[11px] font-medium text-white tabular-nums" style={MONO}>
      {distM != null ? `${(distM / 1000).toFixed(1)} km` : '–'}
    </span>
    <span className="w-[48px] text-right text-[10px] text-zinc-500 tabular-nums" style={MONO}>
      {secs != null ? clock(secs) : '--:--'}
    </span>
  </div>
);

const PhysicalDetail = ({ physRow, detail, teamColor }) => {
  // Time per speed zone — same thresholds as the backend engine
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
    return { walk, run, sprint };
  }, [detail]);

  if (!physRow) {
    return (
      <div className="py-6 text-center text-[10px] font-bold text-zinc-600 uppercase tracking-wider">
        No tracking data for this player
      </div>
    );
  }

  const total = Math.max(1, physRow.total_distance_m || 0);

  return (
    <>
      {/* Stacked walk/run/sprint distance bar */}
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-[9px] font-bold text-zinc-500 uppercase tracking-widest">Distance by zone</span>
        <span className="text-[11px] font-medium text-white tabular-nums" style={MONO}>
          {(total / 1000).toFixed(1)} km
        </span>
      </div>
      <div className="h-2.5 bg-zinc-950/60 border border-white/5 rounded-full overflow-hidden flex mb-2">
        <div className="h-full" style={{ width: `${((physRow.distance_walk_m || 0) / total) * 100}%`, backgroundColor: WALK_COLOR }} />
        <div className="h-full" style={{ width: `${((physRow.distance_run_m || 0) / total) * 100}%`, backgroundColor: teamColor, opacity: 0.85 }} />
        <div className="h-full" style={{ width: `${((physRow.distance_sprint_m || 0) / total) * 100}%`, backgroundColor: SPRINT_COLOR }} />
      </div>

      <ZoneLine color={WALK_COLOR} label="Walk" distM={physRow.distance_walk_m} secs={zoneTimes?.walk} />
      <ZoneLine color={teamColor} label="Run" distM={physRow.distance_run_m} secs={zoneTimes?.run} />
      <ZoneLine color={SPRINT_COLOR} label="Sprint" distM={physRow.distance_sprint_m} secs={zoneTimes?.sprint} />

      {/* Top speed callout */}
      <div className="flex items-center gap-2 mt-2 pt-2 border-t border-white/5">
        <Zap size={11} className="text-yellow-400 flex-shrink-0" />
        <span className="text-[10px] font-bold text-zinc-400 uppercase tracking-wider">Top speed</span>
        <div className="flex-1" />
        <span className="text-[12px] font-bold tabular-nums" style={{ ...MONO, color: SPRINT_COLOR }}>
          {(physRow.max_speed_kmh || 0).toFixed(1)} km/h
        </span>
        <span className="text-[10px] text-zinc-500 font-bold tabular-nums" style={MONO}>
          {Math.floor((physRow.max_speed_at_second || 0) / 60)}'
        </span>
      </div>

      {/* Speed profile sparkline */}
      <div className="flex items-center justify-between mt-3 mb-1">
        <span className="text-[9px] font-bold text-zinc-500 uppercase tracking-widest">Speed profile</span>
        <span className="text-[8px] font-bold uppercase tracking-widest" style={{ color: SPRINT_COLOR, opacity: 0.7 }}>
          - - 75% Vmax
        </span>
      </div>
      {detail?.speed_per_second?.length ? (
        <SpeedSparkline speedSeries={detail.speed_per_second} vmaxMs={detail.max_speed_ms} color={teamColor} />
      ) : (
        <div className="h-[44px] flex items-center justify-center">
          <Loader2 size={14} className="text-zinc-600 animate-spin" />
        </div>
      )}
    </>
  );
};

// ============================================================
// MAIN COMPONENT
// ============================================================
const STAT_SECTIONS = ['Attacking', 'Passing', 'Defending', 'Physical'];

const PlayerDeepDive = ({
  playerData,
  roster = [],
  teamColor = '#006FEE',
  teamShort = '',
  matchId,
  pitchLoading = false,
}) => {
  const [activeSection, setActiveSection] = useState('Attacking');

  const playerId = playerData?.info?.id || null;
  const { heatmap, isLoading: heatmapLoading } = usePlayerTrackingHeatmap(matchId, playerId);
  const { detail } = usePlayerTrackingDetail(matchId, playerId);
  const { trackingMetrics } = useTrackingMetrics(matchId);
  const { shotData } = useShotMap(matchId);

  // Physical metrics row for this player + squad context
  const { physRow, physBest } = useMemo(() => {
    const all = trackingMetrics?.players || [];
    const rosterIds = new Set(roster.map(p => String(p.info.id)));
    const squad = all.filter(m => rosterIds.has(String(m.player_id)));
    const physRow = all.find(m => String(m.player_id) === String(playerId)) || null;
    const best = (key) => Math.max(0, ...squad.map(m => m[key] || 0));
    return {
      physRow,
      physBest: {
        dist: best('total_distance_m'),
        sprint: best('distance_sprint_m'),
        run: best('distance_run_m'),
        walk: best('distance_walk_m'),
        topSpeed: best('max_speed_kmh'),
      },
    };
  }, [trackingMetrics, roster, playerId]);

  // Player shots + per-player xG from the shot map data
  const { playerShots, playerXg, xgBest } = useMemo(() => {
    const shots = shotData?.shots || [];
    const mine = shots.filter(s => String(s.player_id) === String(playerId));
    const xgByPlayer = {};
    shots.forEach(s => {
      const pid = String(s.player_id);
      xgByPlayer[pid] = (xgByPlayer[pid] || 0) + (s.xg || 0);
    });
    const rosterIds = new Set(roster.map(p => String(p.info.id)));
    const xgBest = Math.max(0, ...Object.entries(xgByPlayer)
      .filter(([pid]) => rosterIds.has(pid))
      .map(([, v]) => v));
    return {
      playerShots: mine,
      playerXg: xgByPlayer[String(playerId)] || 0,
      xgBest,
    };
  }, [shotData, playerId, roster]);

  // Squad-best + rank helpers over eventing stats
  const squadContext = useMemo(() => {
    const get = (p, key) => p?.stats?.[key] || 0;
    const best = (key) => Math.max(0, ...roster.map(p => get(p, key)));
    const rank = (key, val) => roster.filter(p => get(p, key) > val).length + 1;
    return { best, rank, size: roster.length };
  }, [roster]);

  if (!playerData) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-500">
        <p className="text-sm">Select a player to see their analysis</p>
      </div>
    );
  }

  const { stats } = playerData;
  const duelWinPct = stats.tackles > 0 ? Math.round((stats.duelsWon / stats.tackles) * 100) : 0;

  const row = (label, key, format) => (
    <PlayerStatRow
      key={label}
      label={label}
      value={stats[key]}
      best={squadContext.best(key)}
      format={format}
      rank={squadContext.rank(key, stats[key] || 0)}
      squadSize={squadContext.size}
      teamColor={teamColor}
    />
  );

  const physRowItem = (label, valueM, bestM, format = 'km') => (
    <PlayerStatRow
      key={label}
      label={label}
      value={format === 'km' ? km(valueM) : valueM}
      best={format === 'km' ? km(bestM) : bestM}
      format={format}
      squadSize={squadContext.size}
      teamColor={teamColor}
    />
  );

  return (
    <div className="w-full h-full flex flex-col gap-3 overflow-hidden">
      {/* ── HEADER — identity + key totals (scoreboard style) ── */}
      <PlayerHeaderCard player={playerData} teamShort={teamShort} teamColor={teamColor} physRow={physRow} />

      {/* ── ROW: [Pitch 44%] | [Stats 56%] — same split rhythm as Team Overview ── */}
      <div className="flex-1 min-h-0 flex gap-3 overflow-hidden">

        {/* PITCH COLUMN */}
        <div className="flex flex-col overflow-hidden" style={{ flex: '0 0 44%', minWidth: 0 }}>
          <div className="relative flex-1 min-h-0 overflow-y-auto no-scrollbar">
            {pitchLoading && (
              <div className="absolute inset-0 z-10 flex items-center justify-center bg-zinc-950/50 backdrop-blur-sm rounded-xl">
                <Loader2 size={24} className="text-primary animate-spin" />
              </div>
            )}
            <PlayerPitchAnalytics
              pitchData={playerData.pitchData}
              heatmap={heatmap}
              heatmapLoading={heatmapLoading}
              attackDirection="right"
            />
          </div>
        </div>

        {/* STATS COLUMN */}
        <div className="flex-1 min-w-0 bg-zinc-900/60 backdrop-blur-sm rounded-xl border border-white/5 flex flex-col overflow-hidden">

          {/* Section tabs — same pill style as Team Overview */}
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
                    ${isActive ? 'bg-white/10 text-white' : 'text-zinc-500 hover:text-white hover:bg-white/5'}
                  `}
                >
                  {sec}
                </button>
              );
            })}
          </div>
          <div className="h-px bg-white/5 flex-shrink-0" />

          {/* Column headers: player vs squad best */}
          <div className="flex items-center justify-between px-4 py-2 border-b border-white/5 flex-shrink-0">
            <span className="text-[11px] font-bold tracking-wider" style={{ color: teamColor }}>
              {playerData.info.shortName || playerData.info.name}
            </span>
            <span className="text-[9px] font-bold text-zinc-600 uppercase tracking-widest">
              {activeSection}
            </span>
            <span className="text-[11px] font-bold tracking-wider text-zinc-500">
              Squad Best
            </span>
          </div>

          {/* Scrollable rows */}
          <div className="flex-1 overflow-y-auto no-scrollbar px-4 py-2">

            {activeSection === 'Attacking' && (
              <>
                {row('Goals', 'goals')}
                <PlayerStatRow
                  label="Expected Goals (xG)"
                  value={playerXg}
                  best={xgBest}
                  format="decimal"
                  squadSize={squadContext.size}
                  teamColor={teamColor}
                />
                {row('Total Shots', 'shots')}
                {row('Shots on Target', 'shotsOnTarget')}
                {row('Touches', 'touches')}

                <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg mt-3 mb-2">
                  <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase mb-2">Shot Breakdown</h3>
                  <ShotBreakdown shots={playerShots} teamColor={teamColor} />
                </div>
              </>
            )}

            {activeSection === 'Passing' && (
              <>
                {row('Passes Completed', 'passesCompleted')}
                {row('Pass Accuracy', 'passAccuracy', 'pct')}
                {row('Progressive Passes', 'progressivePasses')}
                {row('Touches', 'touches')}
              </>
            )}

            {activeSection === 'Defending' && (
              <>
                {row('Duels', 'tackles')}
                {row('Duels Won', 'duelsWon')}
                <PlayerStatRow
                  label="Duel Win %"
                  value={duelWinPct}
                  best={100}
                  format="pct"
                  squadSize={squadContext.size}
                  teamColor={teamColor}
                />
                {row('Interceptions', 'interceptions')}
              </>
            )}

            {activeSection === 'Physical' && (
              <>
                {physRowItem('Distance Covered', physRow?.total_distance_m, physBest.dist)}
                {physRowItem('Sprinting', physRow?.distance_sprint_m, physBest.sprint)}
                {physRowItem('Running', physRow?.distance_run_m, physBest.run)}
                {physRowItem('Walking', physRow?.distance_walk_m, physBest.walk)}
                {physRowItem('Top Speed', physRow?.max_speed_kmh, physBest.topSpeed, 'kmh')}

                <div className="bg-[#18181B]/80 backdrop-blur-[16px] border border-white/5 rounded-xl p-3 shadow-lg mt-3 mb-2">
                  <h3 className="text-[11px] font-bold tracking-[0.1em] text-[#ECEDEE] uppercase mb-2">Physical Profile</h3>
                  <PhysicalDetail physRow={physRow} detail={detail} teamColor={teamColor} />
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default PlayerDeepDive;
