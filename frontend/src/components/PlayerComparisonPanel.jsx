import React, { useState, useEffect, useMemo } from 'react';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, ResponsiveContainer, Legend } from 'recharts';
import { X, TrendingUp, TrendingDown, Minus } from 'lucide-react';
import PlayerAvatar from './PlayerAvatar';

const API_BASE = (import.meta?.env?.VITE_API_BASE) || "http://127.0.0.1:8000";

const km = (m) => (m === null || m === undefined ? '–' : `${(m / 1000).toFixed(2)} km`);

/** Celda de métrica del partido */
const Stat = ({ label, value, sub }) => (
  <div className="bg-white/[0.03] rounded-lg px-2.5 py-1.5 border border-white/5">
    <div className="text-[8px] uppercase tracking-wider text-zinc-500 font-bold truncate">{label}</div>
    <div className="text-sm font-bold font-mono text-white leading-tight">
      {value}{sub && <span className="text-[9px] text-zinc-500 ml-0.5">{sub}</span>}
    </div>
  </div>
);

/**
 * Panel de comparación (columna derecha en flujo). Muestra:
 *  - Cabecera + desviación compuesta (σ)
 *  - Radar de dos áreas: esperado (azul) vs real (verde/rojo)
 *  - Comparativa 1:1 por métrica (esperado / real / z)
 *  - Datos del partido (eventos, pases, precisión, tiros, etc.)
 *  - Físico (velocidad máxima + distribución de distancia)
 */
const PlayerComparisonPanel = ({ player, homeGoals, awayGoals, homeTeamId, onClose }) => {
  const [profile, setProfile] = useState(null);
  const [phys, setPhys] = useState(null);
  const [loading, setLoading] = useState(true);

  const isHome = player && String(player.team_id) === String(homeTeamId);
  const teamColor = isHome ? '#006FEE' : '#f31260';
  const pid = player && (player.id ?? player.player_id);

  const gameState = useMemo(() => {
    if (!player) return "Overall";
    if (isHome) {
      if (homeGoals > awayGoals) return "Winning";
      if (homeGoals < awayGoals) return "Losing";
    } else {
      if (awayGoals > homeGoals) return "Winning";
      if (awayGoals < homeGoals) return "Losing";
    }
    return "Drawing";
  }, [player, isHome, homeGoals, awayGoals]);

  useEffect(() => {
    if (!pid) return;
    setLoading(true);
    fetch(`${API_BASE}/match/test_match/player/${pid}/profile?state=${gameState}`)
      .then(r => r.json())
      .then(d => setProfile(d && d.metrics ? d : null))
      .catch(() => setProfile(null))
      .finally(() => setLoading(false));
  }, [pid, gameState]);

  // Físico (velocidad + distancias) desde tracking metrics
  useEffect(() => {
    if (!pid) return;
    setPhys(null);
    fetch(`${API_BASE}/match/test_match/tracking/metrics`)
      .then(r => r.json())
      .then(d => {
        const arr = d?.players || [];
        setPhys(arr.find(p => String(p.player_id) === String(pid)) || null);
      })
      .catch(() => setPhys(null));
  }, [pid]);

  if (!player) return null;

  const dev = profile?.deviation_sigma ?? null;
  const devColor = dev === null ? '#a1a1aa' : (dev >= 0 ? '#17c964' : '#f31260');
  const actualColor = dev !== null && dev < 0 ? '#f31260' : '#17c964';
  const TrendIcon = dev === null ? Minus : (dev > 0.15 ? TrendingUp : (dev < -0.15 ? TrendingDown : Minus));
  const m = profile?.match;

  // distribución de distancia (walk/run/sprint)
  const distTotal = phys?.total_distance_m || 0;
  const seg = [
    { k: 'Caminata', v: phys?.distance_walk_m || 0, c: '#3f3f46' },
    { k: 'Carrera', v: phys?.distance_run_m || 0, c: '#006FEE' },
    { k: 'Sprint', v: phys?.distance_sprint_m || 0, c: '#f5a524' },
  ];

  return (
    <div className="h-full w-full flex flex-col bg-zinc-950/90 backdrop-blur-2xl border-l border-white/10
                    shadow-[-8px_0_40px_rgba(0,0,0,0.5)] animate-in slide-in-from-right duration-300">

      {/* Header */}
      <div className="flex items-center gap-3 p-4 border-b border-white/5 flex-shrink-0">
        <PlayerAvatar trackingId={pid} name={player.name} size={48} ring={teamColor} />
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <h3 className="text-base font-bold text-white truncate">{player.name}</h3>
            <span className="text-[10px] font-mono px-1 rounded border"
                  style={{ color: teamColor, borderColor: `${teamColor}55` }}>#{player.number ?? '–'}</span>
          </div>
          <span className="text-[10px] text-zinc-400 uppercase tracking-widest font-bold">
            {player.team_name || ''}
          </span>
        </div>
        <button onClick={onClose}
                className="text-zinc-400 hover:text-white bg-white/5 hover:bg-white/10 rounded-full p-1.5">
          <X size={16} />
        </button>
      </div>

      {/* Headline deviation */}
      <div className="px-4 py-3 flex items-center justify-between border-b border-white/5 flex-shrink-0">
        <div>
          <div className="text-[9px] text-zinc-500 uppercase tracking-widest font-bold">Desviación</div>
          <div className="flex items-center gap-1.5">
            <TrendIcon size={18} style={{ color: devColor }} />
            <span className="text-2xl font-bold font-mono" style={{ color: devColor }}>
              {dev === null ? '—' : `${dev > 0 ? '+' : ''}${dev.toFixed(2)}σ`}
            </span>
          </div>
        </div>
        <div className="text-right">
          <div className="text-[9px] text-zinc-500 uppercase tracking-widest font-bold">Estado · {gameState}</div>
          <div className="text-xs font-bold text-zinc-300">
            {profile?.status === 'calibrating' ? '⏳ Calibrando'
              : profile?.status === 'active' ? '● Activo' : '—'}
            {profile?.n_matches ? <span className="text-zinc-600 ml-2">{profile.n_matches} part.</span> : null}
          </div>
        </div>
      </div>

      {loading && <div className="flex-1 flex items-center justify-center text-zinc-600 text-xs">Cargando…</div>}
      {!loading && !profile && (
        <div className="flex-1 flex items-center justify-center text-zinc-600 text-xs px-6 text-center">
          Sin histórico suficiente para este jugador.
        </div>
      )}

      {!loading && profile && (
        <div className="flex-1 overflow-y-auto no-scrollbar">
          {/* Radar de dos áreas */}
          <div className="h-[210px] px-2 pt-3">
            <ResponsiveContainer width="100%" height="100%">
              <RadarChart cx="50%" cy="50%" outerRadius="70%" data={profile.metrics}>
                <PolarGrid stroke="#3f3f46" />
                <PolarAngleAxis dataKey="metric" tick={{ fill: '#a1a1aa', fontSize: 10, fontWeight: 'bold' }} />
                <Radar name="Esperado" dataKey="expected_norm" stroke="#006FEE"
                       strokeWidth={2} fill="#006FEE" fillOpacity={0.18} isAnimationActive={false} />
                <Radar name="Real" dataKey="actual_norm" stroke={actualColor}
                       strokeWidth={2} fill={actualColor} fillOpacity={0.35} isAnimationActive={false} />
                <Legend wrapperStyle={{ fontSize: 10 }} />
              </RadarChart>
            </ResponsiveContainer>
          </div>

          {/* Comparativa 1:1 esperado vs real */}
          <div className="px-4 py-2">
            <div className="text-[9px] text-zinc-500 uppercase tracking-widest font-bold mb-1.5">Rendimiento vs esperado (p30)</div>
            <div className="grid grid-cols-[1fr_auto_auto_auto] gap-x-3 gap-y-1 text-[11px] items-center">
              <span className="text-[9px] text-zinc-600 uppercase font-bold">Métrica</span>
              <span className="text-[9px] text-zinc-600 uppercase font-bold text-right">Esper.</span>
              <span className="text-[9px] text-zinc-600 uppercase font-bold text-right">Real</span>
              <span className="text-[9px] text-zinc-600 uppercase font-bold text-right">z</span>
              {profile.metrics.map(mm => {
                const zc = mm.z > 0.15 ? 'text-success' : (mm.z < -0.15 ? 'text-danger' : 'text-zinc-400');
                return (
                  <React.Fragment key={mm.key}>
                    <span className="text-zinc-300 font-medium">{mm.metric}</span>
                    <span className="font-mono text-zinc-500 text-right">{mm.expected.toFixed(2)}</span>
                    <span className="font-mono text-white text-right">{mm.actual.toFixed(2)}</span>
                    <span className={`font-mono font-bold text-right ${zc}`}>{mm.z > 0 ? '+' : ''}{mm.z.toFixed(2)}</span>
                  </React.Fragment>
                );
              })}
            </div>
          </div>

          {/* Datos del partido */}
          {m && (
            <div className="px-4 py-2 border-t border-white/5">
              <div className="text-[9px] text-zinc-500 uppercase tracking-widest font-bold mb-1.5">Datos del partido</div>
              <div className="grid grid-cols-3 gap-1.5">
                <Stat label="Eventos" value={m.events} />
                <Stat label="Pases" value={m.passes} />
                <Stat label="Precisión" value={m.pass_accuracy} sub="%" />
                <Stat label="Tiros" value={m.shots} />
                <Stat label="A puerta" value={m.shots_on_target} />
                <Stat label="Goles" value={m.goals} />
                <Stat label="Pases clave" value={m.key_passes} />
                <Stat label="Centros" value={m.crosses} />
                <Stat label="P. largos" value={m.long_passes} />
                <Stat label="P. cortos" value={m.short_passes} />
                <Stat label="Recuper." value={m.recoveries} />
                <Stat label="Intercep." value={m.interceptions} />
              </div>
            </div>
          )}

          {/* Físico */}
          <div className="px-4 py-2 border-t border-white/5">
            <div className="text-[9px] text-zinc-500 uppercase tracking-widest font-bold mb-1.5">Físico</div>
            <div className="grid grid-cols-2 gap-1.5 mb-2">
              <Stat label="Vel. máxima" value={phys?.max_speed_kmh != null ? phys.max_speed_kmh.toFixed(1) : '–'} sub=" km/h" />
              <Stat label="Distancia" value={km(distTotal)} />
            </div>
            {/* Distribución de distancia */}
            <div className="text-[8px] uppercase tracking-wider text-zinc-500 font-bold mb-1">Distribución de distancia</div>
            <div className="h-2.5 w-full rounded-full overflow-hidden flex bg-white/5">
              {distTotal > 0 ? seg.map(s => (
                <div key={s.k} style={{ width: `${(s.v / distTotal) * 100}%`, backgroundColor: s.c }} title={`${s.k}: ${km(s.v)}`} />
              )) : <div className="w-full" />}
            </div>
            <div className="flex justify-between mt-1 text-[8px] text-zinc-500">
              {seg.map(s => (
                <span key={s.k} className="flex items-center gap-1">
                  <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: s.c }} />{s.k}
                </span>
              ))}
            </div>
          </div>

          <p className="px-4 py-3 text-[9px] text-zinc-600 leading-relaxed border-t border-white/5">
            Esperado = media histórica por 30′ (BigQuery). Real = ritmo proyectado a 30′ en este partido.
            z = desviaciones estándar sobre su propia norma.
          </p>
        </div>
      )}
    </div>
  );
};

export default PlayerComparisonPanel;
