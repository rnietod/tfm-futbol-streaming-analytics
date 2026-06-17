import React, { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, ResponsiveContainer } from 'recharts';
import PlayerAvatar from './PlayerAvatar';

const API_BASE = (import.meta?.env?.VITE_API_BASE) || "http://127.0.0.1:8000";

// Caché de perfiles para no refetchear en cada hover.
const _cache = {};

/**
 * Mini-pestaña que aparece al pasar el ratón por un jugador del ticker:
 * foto + z-score + radar (esperado vs real). Posición fija (escapa overflow).
 */
const PlayerHoverCard = ({ player, x, y }) => {
  const [profile, setProfile] = useState(() => _cache[player?.id] || null);

  useEffect(() => {
    if (!player?.id) return;
    if (_cache[player.id]) { setProfile(_cache[player.id]); return; }
    let alive = true;
    fetch(`${API_BASE}/match/test_match/player/${player.id}/profile?state=Overall`)
      .then(r => r.json())
      .then(d => {
        if (!alive || !d?.metrics) return;
        _cache[player.id] = d;
        setProfile(d);
      })
      .catch(() => {});
    return () => { alive = false; };
  }, [player?.id]);

  if (!player) return null;

  const dev = (player.deviation === null || player.deviation === undefined)
    ? (profile?.deviation_sigma ?? null) : Number(player.deviation);
  const devColor = dev === null ? '#a1a1aa' : (dev >= 0 ? '#17c964' : '#f31260');
  const actualColor = dev !== null && dev < 0 ? '#f31260' : '#17c964';

  return createPortal((
    <div
      className="fixed z-[200] w-56 rounded-xl bg-zinc-950/95 backdrop-blur-xl border border-white/10
                 shadow-2xl p-3 pointer-events-none animate-in fade-in zoom-in-95 duration-150"
      style={{ left: Math.min(x, window.innerWidth - 236), top: y + 6 }}
    >
      <div className="flex items-center gap-2 mb-1">
        <PlayerAvatar trackingId={player.id} name={player.name} size={36} />
        <div className="min-w-0 flex-1 leading-tight">
          <div className="text-xs font-bold text-white truncate">{player.name}</div>
          <div className="text-[9px] font-mono text-zinc-500">#{player.number || '00'}</div>
        </div>
        <div className="text-right">
          <div className="text-[7px] uppercase tracking-wider text-zinc-500 font-bold">Desv.</div>
          <div className="text-base font-bold font-mono leading-none" style={{ color: devColor }}>
            {dev === null ? '—' : `${dev > 0 ? '+' : ''}${dev.toFixed(2)}σ`}
          </div>
        </div>
      </div>

      {profile?.metrics ? (
        <div className="h-[140px] -mx-1">
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart cx="50%" cy="50%" outerRadius="68%" data={profile.metrics}>
              <PolarGrid stroke="#3f3f46" />
              <PolarAngleAxis dataKey="metric" tick={{ fill: '#71717a', fontSize: 8 }} />
              <Radar dataKey="expected_norm" stroke="#006FEE" strokeWidth={1.5}
                     fill="#006FEE" fillOpacity={0.15} isAnimationActive={false} />
              <Radar dataKey="actual_norm" stroke={actualColor} strokeWidth={1.5}
                     fill={actualColor} fillOpacity={0.35} isAnimationActive={false} />
            </RadarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="h-[60px] flex items-center justify-center text-[9px] text-zinc-600">
          {player.status === 'calibrating' ? 'Calibrando…' : 'Cargando radar…'}
        </div>
      )}

      <div className="text-[8px] text-zinc-600 uppercase tracking-wider font-bold text-center mt-1">
        Click para comparar →
      </div>
    </div>
  ), document.body);
};

export default PlayerHoverCard;
