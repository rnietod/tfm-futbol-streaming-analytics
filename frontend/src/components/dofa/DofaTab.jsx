import React, { useState } from 'react';
import {
  Swords, Shield, TrendingUp, TrendingDown, Target, Crosshair,
  Users, Flame, Clock, Award, Footprints, Loader2, Database,
} from 'lucide-react';
import { useDofaTeams, useDofaOverview, useIdealXI } from '../../hooks/useDofa';

// ============================================================
// DOFA TAB — análisis pre-partido (BigQuery, ponderado por recencia)
//   Cabecera: toggle Rival / Real Madrid
//   Fila SWOT: 4 cuadrantes
//   Paneles: 11 previsto · 11 ideal · goles/min · top goleadores/asistidores
//            · zonas de tiro · mapa de calor del equipo
// ============================================================

const RIVAL_COLOR = '#f31260';
const OURS_COLOR = '#006FEE';

// ── Escala térmica (igual que PlayerPitchAnalytics) ─────────
const HEAT_STOPS = [
  { v: 0.0, c: [10, 60, 150] }, { v: 0.25, c: [0, 111, 238] },
  { v: 0.5, c: [34, 211, 238] }, { v: 0.7, c: [250, 204, 21] },
  { v: 0.85, c: [251, 146, 60] }, { v: 1.0, c: [239, 68, 68] },
];
const heatColor = (v) => {
  const t = Math.max(0, Math.min(1, v));
  let lo = HEAT_STOPS[0], hi = HEAT_STOPS[HEAT_STOPS.length - 1];
  for (let i = 0; i < HEAT_STOPS.length - 1; i++) {
    if (t >= HEAT_STOPS[i].v && t <= HEAT_STOPS[i + 1].v) { lo = HEAT_STOPS[i]; hi = HEAT_STOPS[i + 1]; break; }
  }
  const f = hi.v === lo.v ? 0 : (t - lo.v) / (hi.v - lo.v);
  const mix = lo.c.map((c, i) => Math.round(c + (hi.c[i] - c) * f));
  return `rgb(${mix[0]},${mix[1]},${mix[2]})`;
};

const surname = (name) => (name || '').trim().split(' ').pop();

// Coordenadas Opta: y=100 es la banda IZQUIERDA. Para que la izquierda quede arriba
// en pantalla (convención de scouting), el eje Y se espeja al pasar a viewBox (64 alto).
const toCy = (y) => ((100 - y) / 100) * 64;

// Etiqueta de posición (español) → código corto, para el rótulo bajo cada jugador.
const LABEL_TO_CODE = {
  'POR': 'POR', 'Lateral Dcho': 'LD', 'Lateral Izdo': 'LI', 'Defensa Central': 'DFC',
  'Carrilero Dcho': 'CAR', 'Carrilero Izdo': 'CAR', 'Medio Centro Def': 'MCD',
  'Medio Centro': 'MC', 'Medio Dcho': 'MD', 'Medio Izdo': 'MI',
  'Extremo Dcho': 'ED', 'Extremo Izdo': 'EI', 'Delantero Centro': 'DC',
};
const codeOf = (label) => LABEL_TO_CODE[label] || (label || '').slice(0, 3).toUpperCase();

// Código de formación Opta (Q130) → nombre legible (las comunes del dataset).
const FORMATION_NAMES = {
  '2': '4-4-2', '3': '4-1-2-1-2', '4': '4-3-3', '5': '4-5-1', '6': '4-4-1-1',
  '7': '4-2-3-1', '8': '4-2-3-1', '9': '4-3-2-1', '10': '5-3-2', '11': '5-4-1',
  '12': '3-5-2', '13': '3-4-3', '15': '4-3-3', '16': '3-4-3', '17': '3-4-3', '18': '3-4-3',
};
const formationName = (f) => (f == null ? '' : (FORMATION_NAMES[String(f)] || String(f)));

// ── CARD (glassmorphism) ────────────────────────────────────
const Card = ({ title, icon: Icon, accent, children, className = '', span = '' }) => (
  <div className={`rounded-xl border border-white/5 bg-zinc-900/60 backdrop-blur-xl shadow-lg p-3 flex flex-col ${span} ${className}`}>
    {title && (
      <div className="flex items-center gap-2 mb-2">
        {Icon && <Icon size={13} style={{ color: accent }} />}
        <h3 className="text-[10px] font-bold uppercase tracking-widest text-[#ECEDEE]">{title}</h3>
      </div>
    )}
    <div className="flex-1 min-h-0">{children}</div>
  </div>
);

// ── PITCH LINES (horizontal, viewBox 100×64) ────────────────
const PitchLines = () => (
  <g stroke="rgba(255,255,255,0.16)" strokeWidth="0.3" fill="none">
    <rect x="1" y="1" width="98" height="62" rx="1" />
    <line x1="50" y1="1" x2="50" y2="63" />
    <circle cx="50" cy="32" r="8" />
    <circle cx="50" cy="32" r="0.6" fill="rgba(255,255,255,0.3)" />
    <rect x="1" y="18" width="12" height="28" />
    <rect x="87" y="18" width="12" height="28" />
    <rect x="1" y="25" width="5" height="14" />
    <rect x="94" y="25" width="5" height="14" />
  </g>
);

// ── FORMATION PITCH (alineación estándar + hover de perfiles) ─
const FormationPitch = ({ players = [], accent }) => {
  const [hover, setHover] = useState(null);
  const hp = hover != null ? players[hover] : null;
  return (
    <div className="relative w-full">
      <svg viewBox="0 0 100 64" className="w-full h-auto"
        style={{ background: 'linear-gradient(180deg,#0e1f38 0%,#122a18 50%,#0e1f38 100%)', borderRadius: 8 }}>
        <PitchLines />
        {players.map((p, i) => {
          const cx = p.x, cy = toCy(p.y);
          const active = hover === i;
          return (
            <g key={p.playerId || i} style={{ cursor: 'pointer' }}
              onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(null)}>
              <circle cx={cx} cy={cy} r={active ? 5.6 : 4.6} fill={accent}
                fillOpacity={active ? 0.28 : 0.12} />
              <circle cx={cx} cy={cy} r="3.4" fill="rgba(0,0,0,0.82)" stroke={accent}
                strokeWidth={active ? 1 : 0.6} />
              <text x={cx} y={cy + 0.9} textAnchor="middle" dominantBaseline="middle"
                fontSize="2" fill="white" fontWeight="bold">{surname(p.name).slice(0, 7)}</text>
              <text x={cx} y={cy + 6} textAnchor="middle" fontSize="1.8" fontWeight="700"
                fill={accent} style={{ letterSpacing: '0.3px' }}>{codeOf(p.primaryLabel)}</text>
            </g>
          );
        })}
        <text x="50" y="62" textAnchor="middle" fontSize="2.3" fill="rgba(255,255,255,0.35)"
          fontWeight="700" style={{ letterSpacing: '0.4px' }}>ATACA →</text>
      </svg>

      {/* Tooltip de perfiles al pasar por encima */}
      {hp && (
        <div className="absolute z-30 pointer-events-none"
          style={{ left: `${hp.x}%`, top: `${(toCy(hp.y) / 64) * 100}%`,
                   transform: 'translate(-50%, -116%)' }}>
          <div className="rounded-lg bg-zinc-950/95 backdrop-blur-md border px-2.5 py-2 shadow-2xl min-w-[140px]"
            style={{ borderColor: `color-mix(in srgb, ${accent} 55%, transparent)` }}>
            <div className="text-[11px] font-bold text-white leading-tight">{hp.name}</div>
            <div className="text-[8px] font-bold uppercase tracking-widest mb-1.5" style={{ color: accent }}>
              {hp.primaryLabel}
            </div>
            <div className="space-y-1 border-t border-white/10 pt-1.5">
              <div className="text-[7px] uppercase tracking-widest text-zinc-600 font-bold">Perfiles (partidos)</div>
              {(hp.positions || []).map((q, j) => (
                <div key={j} className="flex items-center justify-between gap-4 text-[9px]">
                  <span className={j === 0 ? 'text-white font-semibold' : 'text-zinc-400'}>{q.label}</span>
                  <span className="font-mono text-zinc-500 tabular-nums">{q.matches}</span>
                </div>
              ))}
              {(hp.positions || []).length === 0 && (
                <div className="text-[8px] text-zinc-600 italic">sin histórico de posiciones</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// ── HEAT PITCH (team heatmap) ───────────────────────────────
const HeatPitch = ({ heatmap }) => {
  const cells = heatmap?.cells || [];
  const grid = heatmap?.grid || { x: 20, y: 10 };
  const r = Math.max(100 / grid.x, 64 / grid.y) * 0.9;
  return (
    <svg viewBox="0 0 100 64" className="w-full h-auto"
      style={{ background: 'linear-gradient(180deg,#0e1f38 0%,#122a18 50%,#0e1f38 100%)', borderRadius: 8 }}>
      <defs>
        <filter id="dofaHeatBlur" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="2.2" />
        </filter>
      </defs>
      <g filter="url(#dofaHeatBlur)" style={{ mixBlendMode: 'screen' }}>
        {cells.map((c, i) => {
          // sqrt: sin ella, una celda extrema (p.ej. el córner desde el que más se saca)
          // domina la normalización y deja el resto del campo plano.
          const v = Math.sqrt(c.v);
          return (
            <circle key={i} cx={c.x} cy={toCy(c.y)} r={r}
              fill={heatColor(v)} opacity={0.10 + v * 0.72} />
          );
        })}
      </g>
      <PitchLines />
      <text x="50" y="62" textAnchor="middle" fontSize="2.3" fill="rgba(255,255,255,0.35)"
        fontWeight="700" style={{ letterSpacing: '0.4px' }}>ATACA →</text>
    </svg>
  );
};

// ── SHOT ZONE PITCH ─────────────────────────────────────────
const ShotZonePitch = ({ zones = [], accent }) => (
  <svg viewBox="0 0 100 64" className="w-full h-auto"
    style={{ background: 'linear-gradient(180deg,#0e1f38 0%,#122a18 50%,#0e1f38 100%)', borderRadius: 8 }}>
    {zones.map((z, i) => {
      const w = 10, h = (10 / 100) * 64;
      const cy = toCy(z.y);
      return (
        <g key={i}>
          <title>{`${z.shots} tiros · ${z.goals} goles`}</title>
          <rect x={z.x - 5} y={cy - h / 2} width={w} height={h} fill={accent} fillOpacity={z.intensity * 0.8} />
          {z.goals > 0 && (
            <text x={z.x} y={cy + 1} textAnchor="middle" dominantBaseline="middle"
              fontSize="2.6" fill="#00E676" fontWeight="900">{z.goals}</text>
          )}
        </g>
      );
    })}
    <PitchLines />
    <text x="50" y="62" textAnchor="middle" fontSize="2.3" fill="rgba(255,255,255,0.35)"
      fontWeight="700" style={{ letterSpacing: '0.4px' }}>ATACA → · nº = goles</text>
  </svg>
);

// ── GOALS BY MINUTE ─────────────────────────────────────────
const GoalsByMinute = ({ data = [], accent }) => {
  const max = Math.max(...data.map(d => d.weightedShare), 1);
  return (
    <div className="flex items-end justify-between gap-1.5 h-full min-h-[120px] pt-2">
      {data.map((d, i) => (
        <div key={i} className="flex-1 flex flex-col items-center gap-1 h-full justify-end">
          <span className="text-[8px] font-mono text-zinc-400">{d.weightedShare}%</span>
          <div className="w-full rounded-t transition-all duration-700"
            style={{ height: `${(d.weightedShare / max) * 100}%`,
                     backgroundColor: `color-mix(in srgb, ${accent} 70%, transparent)`,
                     border: `1px solid ${accent}` }}
            title={`${d.goals} goles`} />
          <span className="text-[7px] text-zinc-500 font-bold">{d.range}</span>
        </div>
      ))}
    </div>
  );
};

// ── TOP PLAYERS ─────────────────────────────────────────────
const TopPlayers = ({ rows = [], accent, valueKey, valueLabel }) => (
  <ol className="space-y-2">
    {rows.map((p, i) => (
      <li key={p.playerId || i} className="flex items-center gap-2">
        <span className="w-5 h-5 rounded-md flex items-center justify-center text-[9px] font-black text-white"
          style={{ backgroundColor: `color-mix(in srgb, ${accent} ${40 - i * 10}%, #18181b)` }}>{i + 1}</span>
        <div className="flex-1 min-w-0">
          <div className="text-[11px] font-semibold text-[#ECEDEE] truncate">{p.name}</div>
          <div className="text-[8px] text-zinc-500 uppercase tracking-wider">{p.position || '—'} · {p.apps} part.</div>
        </div>
        <span className="text-base font-mono font-black tabular-nums" style={{ color: accent }}>{p[valueKey]}</span>
        <span className="text-[8px] text-zinc-600 uppercase">{valueLabel}</span>
      </li>
    ))}
    {rows.length === 0 && <li className="text-[10px] text-zinc-600 italic">— sin datos —</li>}
  </ol>
);

// ── SWOT QUADRANT ───────────────────────────────────────────
const SWOT_META = {
  fortalezas:    { label: 'Fortalezas',    icon: Shield,       color: '#17c964', hint: 'Rival · interno +' },
  debilidades:   { label: 'Debilidades',   icon: TrendingDown, color: '#f31260', hint: 'Rival · interno −' },
  oportunidades: { label: 'Oportunidades', icon: TrendingUp,   color: '#006FEE', hint: 'Nuestro · externo +' },
  amenazas:      { label: 'Amenazas',      icon: Crosshair,    color: '#f5a524', hint: 'Nuestro · externo −' },
};
const SwotQuadrant = ({ kind, items = [] }) => {
  const m = SWOT_META[kind];
  return (
    <div className="rounded-xl border bg-zinc-900/60 backdrop-blur-xl p-3 flex flex-col shadow-lg"
      style={{ borderColor: `color-mix(in srgb, ${m.color} 30%, transparent)` }}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2" style={{ color: m.color }}>
          <m.icon size={14} />
          <span className="text-[11px] font-bold uppercase tracking-widest">{m.label}</span>
        </div>
        <span className="text-[8px] text-zinc-600 uppercase tracking-wider">{m.hint}</span>
      </div>
      <ul className="space-y-1.5">
        {items.map((it, i) => (
          <li key={i} className="text-[10px] leading-snug">
            <span className="font-bold text-[#ECEDEE]">{it.title}.</span>{' '}
            <span className="text-zinc-500">{it.detail}</span>
          </li>
        ))}
        {items.length === 0 && <li className="text-[10px] text-zinc-600 italic">— sin señales —</li>}
      </ul>
    </div>
  );
};

// ── TEAM TOGGLE ─────────────────────────────────────────────
const TeamToggle = ({ value, onChange, ourTeam, rival }) => (
  <div className="inline-flex rounded-xl bg-zinc-900/80 border border-white/5 p-1 gap-0.5">
    {[{ v: 'rival', l: rival, c: RIVAL_COLOR }, { v: 'ours', l: ourTeam, c: OURS_COLOR }].map(opt => {
      const active = value === opt.v;
      return (
        <button key={opt.v} onClick={() => onChange(opt.v)}
          className={`px-4 py-1.5 rounded-lg text-[11px] font-bold uppercase tracking-wider transition-all duration-300
            ${active ? 'text-white shadow-lg' : 'text-zinc-500 hover:text-zinc-300'}`}
          style={active ? { backgroundColor: `color-mix(in srgb, ${opt.c} 25%, #18181b)`,
                            boxShadow: `0 0 20px color-mix(in srgb, ${opt.c} 20%, transparent)` } : {}}>
          {opt.l}
        </button>
      );
    })}
  </div>
);

// ── STATES ──────────────────────────────────────────────────
const Loading = ({ msg }) => (
  <div className="flex flex-col items-center justify-center h-full min-h-[120px] gap-2 text-zinc-500">
    <Loader2 size={20} className="animate-spin text-primary" />
    <span className="text-[10px] uppercase tracking-wider font-bold">{msg}</span>
  </div>
);

// ============================================================
// MAIN
// ============================================================
const DofaTab = () => {
  const { ourTeam, rival } = useDofaTeams();
  const [team, setTeam] = useState('rival');
  const focusName = team === 'rival' ? rival : ourTeam;
  const accent = team === 'rival' ? RIVAL_COLOR : OURS_COLOR;

  const { data, isLoading, error } = useDofaOverview(focusName);
  const { data: ideal } = useIdealXI(ourTeam, rival);

  const swot = data?.swot || {};
  const dateRange = data?.dateRange;

  return (
    <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
      {/* CONTROLS BAR */}
      <div className="flex items-center justify-between px-6 py-3 border-b border-white/5 bg-zinc-950/60 backdrop-blur-xl">
        <div className="flex items-center gap-3">
          <Swords size={16} style={{ color: accent }} />
          <span className="text-xs font-bold uppercase tracking-widest text-[#ECEDEE]">
            Análisis DOFA · Pre-partido
          </span>
          {data && (
            <span className="flex items-center gap-1.5 text-[9px] px-2 py-0.5 rounded bg-zinc-800/80 text-zinc-400 font-bold uppercase tracking-wider">
              <Database size={9} /> {data.windowMatches} part.
              {dateRange && <span className="text-zinc-600 normal-case font-medium ml-1">{dateRange.from} → {dateRange.to}</span>}
            </span>
          )}
        </div>
        <TeamToggle value={team} onChange={setTeam} ourTeam={ourTeam} rival={rival} />
      </div>

      {/* BODY */}
      <div className="flex-1 min-h-0 overflow-y-auto p-4 space-y-4">
        {error && (
          <div className="rounded-xl border border-danger/30 bg-danger/10 p-4 text-center">
            <p className="text-danger text-sm font-bold">No se pudo cargar el DOFA</p>
            <p className="text-zinc-500 text-xs font-mono mt-1">{error}</p>
          </div>
        )}

        {/* SWOT ROW */}
        <div>
          <div className="flex items-center gap-2 mb-2">
            <span className="text-[10px] font-bold uppercase tracking-widest" style={{ color: accent }}>
              DOFA — {focusName}
            </span>
            <div className="flex-1 h-px bg-white/5" />
          </div>
          {isLoading ? (
            <div className="rounded-xl border border-white/5 bg-zinc-900/60 p-4"><Loading msg="Consultando BigQuery…" /></div>
          ) : (
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              <SwotQuadrant kind="fortalezas" items={swot.fortalezas} />
              <SwotQuadrant kind="debilidades" items={swot.debilidades} />
              <SwotQuadrant kind="oportunidades" items={swot.oportunidades} />
              <SwotQuadrant kind="amenazas" items={swot.amenazas} />
            </div>
          )}
        </div>

        {/* PANELS GRID */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-3">
          {/* CAMPO ÚNICO según equipo: rival → 11 previsto · Real Madrid → 11 ideal */}
          {(() => {
            const isRival = team === 'rival';
            const xi = isRival ? (data?.expectedXI || []) : (ideal?.xi || []);
            const formation = isRival ? data?.formation : ideal?.formation;
            const xiLoading = isRival ? isLoading : !ideal;
            return (
              <Card
                title={isRival ? `11 Previsto · ${focusName}` : `11 Ideal · ${ourTeam}`}
                icon={isRival ? Users : Award}
                accent={accent}
                span="lg:col-span-12"
              >
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-[8px] text-zinc-500 uppercase tracking-widest">
                    {isRival ? 'Alineación más probable (ventana reciente)' : 'Mejor XI por rendimiento'}
                  </span>
                  {formation && (
                    <span className="text-[9px] font-mono font-bold px-2 py-0.5 rounded bg-zinc-800/80"
                      style={{ color: accent }}>
                      {formationName(formation)}
                    </span>
                  )}
                </div>
                {xiLoading
                  ? <Loading msg="Cargando 11…" />
                  : <div className="mx-auto w-full" style={{ maxWidth: 680 }}>
                      <FormationPitch players={xi} accent={accent} />
                    </div>}
                <div className="mt-1.5 text-[8px] text-zinc-600 text-center">
                  Pasa el cursor sobre un jugador para ver sus posiciones y partidos
                </div>
              </Card>
            );
          })()}

          <Card title="Goles por minuto" icon={Clock} accent={accent} span="lg:col-span-6">
            {isLoading ? <Loading msg="…" /> : <GoalsByMinute data={data?.goalsByMinute || []} accent={accent} />}
          </Card>
          <Card title="Top 3 Goleadores" icon={Target} accent={accent} span="lg:col-span-3">
            {isLoading ? <Loading msg="…" /> : <TopPlayers rows={data?.topScorers || []} accent={accent} valueKey="goals" valueLabel="g" />}
          </Card>
          <Card title="Top 3 Asistidores" icon={Footprints} accent={accent} span="lg:col-span-3">
            {isLoading ? <Loading msg="…" /> : <TopPlayers rows={data?.topAssisters || []} accent={accent} valueKey="assists" valueLabel="a" />}
          </Card>

          <Card title="Zonas de tiro" icon={Crosshair} accent={accent} span="lg:col-span-6">
            {isLoading ? <Loading msg="…" /> : <ShotZonePitch zones={data?.shotZones || []} accent={accent} />}
          </Card>
          <Card title="Mapa de calor del equipo" icon={Flame} accent={accent} span="lg:col-span-6">
            {isLoading ? <Loading msg="…" /> : <HeatPitch heatmap={data?.teamHeatmap} />}
          </Card>
        </div>

        <p className="text-[9px] text-zinc-600 text-center pt-1">
          Datos de BigQuery ponderados por recencia (últimos 5 partidos 50% · siguientes 10 30% · resto 20%) ·
          {data ? ` ${(data.bytesBilled / 1e9).toFixed(1)} GB facturados` : ' —'}
        </p>
      </div>
    </div>
  );
};

export default DofaTab;
