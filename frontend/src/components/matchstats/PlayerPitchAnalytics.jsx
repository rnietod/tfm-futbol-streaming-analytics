import React, { useState } from 'react';
import { Flame, GitBranch, CircleDot, Loader2 } from 'lucide-react';

// ============================================================
// PLAYER PITCH ANALYTICS — SVG Pitch with Layer Overlays
//   · Heatmap     → datos de TRACKING (match_tracking, binned 0-100)
//   · Pass Map    → datos de eventing (match_events)
//   · Touch Map   → datos de eventing (match_events)
// ============================================================

// Pitch dimensions in viewBox coordinates
const PITCH_W = 120;
const PITCH_H = 80;
const PAD = 6; // padding around the pitch lines
const VB_W = PITCH_W + PAD * 2;
const VB_H = PITCH_H + PAD * 2;

// Convert normalized data coords (0-100) to viewBox coords
const toVB = (xNorm, yNorm) => ({
  x: PAD + (xNorm / 100) * PITCH_W,
  y: PAD + (yNorm / 100) * PITCH_H,
});

// ── Thermal color scale for the tracking heatmap ──
// v 0→1 : deep blue → cyan → yellow → orange → red
const HEAT_STOPS = [
  { v: 0.0,  c: [10, 60, 150] },
  { v: 0.25, c: [0, 111, 238] },
  { v: 0.5,  c: [34, 211, 238] },
  { v: 0.7,  c: [250, 204, 21] },
  { v: 0.85, c: [251, 146, 60] },
  { v: 1.0,  c: [239, 68, 68] },
];

const heatColor = (v) => {
  const t = Math.max(0, Math.min(1, v));
  let lo = HEAT_STOPS[0];
  let hi = HEAT_STOPS[HEAT_STOPS.length - 1];
  for (let i = 0; i < HEAT_STOPS.length - 1; i++) {
    if (t >= HEAT_STOPS[i].v && t <= HEAT_STOPS[i + 1].v) {
      lo = HEAT_STOPS[i];
      hi = HEAT_STOPS[i + 1];
      break;
    }
  }
  const f = hi.v === lo.v ? 0 : (t - lo.v) / (hi.v - lo.v);
  const mix = lo.c.map((c, i) => Math.round(c + (hi.c[i] - c) * f));
  return `rgb(${mix[0]},${mix[1]},${mix[2]})`;
};

// --- SVG PITCH MARKINGS ---
const PitchSVG = () => (
  <g stroke="rgba(255,255,255,0.45)" strokeWidth="0.4" fill="none">
    {/* Outline */}
    <rect x={PAD} y={PAD} width={PITCH_W} height={PITCH_H} rx="0.5" />
    {/* Center line */}
    <line x1={PAD + PITCH_W / 2} y1={PAD} x2={PAD + PITCH_W / 2} y2={PAD + PITCH_H} />
    {/* Center circle */}
    <circle cx={PAD + PITCH_W / 2} cy={PAD + PITCH_H / 2} r={9.15} />
    <circle cx={PAD + PITCH_W / 2} cy={PAD + PITCH_H / 2} r={0.6} fill="rgba(255,255,255,0.45)" />
    {/* Left penalty area */}
    <rect x={PAD} y={PAD + PITCH_H / 2 - 20.16} width={16.5} height={40.32} />
    <rect x={PAD} y={PAD + PITCH_H / 2 - 9.16} width={5.5} height={18.32} />
    <circle cx={PAD + 11} cy={PAD + PITCH_H / 2} r={0.5} fill="rgba(255,255,255,0.35)" />
    {/* Right penalty area */}
    <rect x={PAD + PITCH_W - 16.5} y={PAD + PITCH_H / 2 - 20.16} width={16.5} height={40.32} />
    <rect x={PAD + PITCH_W - 5.5} y={PAD + PITCH_H / 2 - 9.16} width={5.5} height={18.32} />
    <circle cx={PAD + PITCH_W - 11} cy={PAD + PITCH_H / 2} r={0.5} fill="rgba(255,255,255,0.35)" />
    {/* Corner arcs */}
    <path d={`M ${PAD + 2} ${PAD} A 2 2 0 0 0 ${PAD} ${PAD + 2}`} />
    <path d={`M ${PAD + PITCH_W} ${PAD + 2} A 2 2 0 0 0 ${PAD + PITCH_W - 2} ${PAD}`} />
    <path d={`M ${PAD} ${PAD + PITCH_H - 2} A 2 2 0 0 0 ${PAD + 2} ${PAD + PITCH_H}`} />
    <path d={`M ${PAD + PITCH_W - 2} ${PAD + PITCH_H} A 2 2 0 0 0 ${PAD + PITCH_W} ${PAD + PITCH_H - 2}`} />
  </g>
);

// --- TRACKING HEATMAP LAYER ---
// Blurred thermal cells from binned tracking positions (grid 0-100 → viewBox)
const TrackingHeatmapLayer = ({ cells = [], grid }) => {
  const cellW = PITCH_W / (grid?.x || 24);
  const cellH = PITCH_H / (grid?.y || 16);
  const r = Math.max(cellW, cellH) * 0.85;

  return (
    <g filter="url(#heatBlur)" style={{ mixBlendMode: 'screen' }}>
      {cells.map((cell, i) => {
        const { x, y } = toVB(cell.x, cell.y);
        return (
          <circle
            key={`heat-${i}`}
            cx={x}
            cy={y}
            r={r}
            fill={heatColor(cell.v)}
            opacity={0.12 + cell.v * 0.78}
          />
        );
      })}
    </g>
  );
};

// --- PASS MAP LAYER ---
const PassNetworkLayer = ({ links = [] }) => (
  <g>
    <defs>
      <marker id="arrowhead" markerWidth="4" markerHeight="3" refX="4" refY="1.5" orient="auto">
        <polygon points="0 0, 4 1.5, 0 3" fill="rgba(0,111,238,0.8)" />
      </marker>
    </defs>
    {links.map((link, i) => {
      const from = toVB(link.from_x, link.from_y);
      const to = toVB(link.to_x, link.to_y);
      const strokeW = Math.min(0.35 + link.count * 0.15, 1.5);
      const opacity = Math.min(0.5 + link.count * 0.1, 0.95);
      return (
        <line
          key={`pass-${i}`}
          x1={from.x}
          y1={from.y}
          x2={to.x}
          y2={to.y}
          stroke={link.successful ? '#3b9bff' : '#f31260'}
          strokeWidth={strokeW}
          opacity={opacity}
          markerEnd="url(#arrowhead)"
          strokeLinecap="round"
        />
      );
    })}
    {links.map((link, i) => {
      const pos = toVB(link.from_x, link.from_y);
      return (
        <circle
          key={`pnode-${i}`}
          cx={pos.x}
          cy={pos.y}
          r={0.9}
          fill="#3b9bff"
          opacity={0.85}
        />
      );
    })}
  </g>
);

// --- TOUCH MAP LAYER ---
const TOUCH_COLORS = {
  pass: '#60a5fa',
  shot: '#f97316',
  dribble: '#a78bfa',
  cross: '#34d399',
  tackle: '#f43f5e',
  reception: '#94a3b8',
};

const TouchMapLayer = ({ points = [] }) => (
  <g>
    {points.map((pt, i) => {
      const { x, y } = toVB(pt.x, pt.y);
      const color = TOUCH_COLORS[pt.eventType] || '#94a3b8';
      return (
        <g key={`touch-${i}`}>
          <circle cx={x} cy={y} r={1.2} fill={color} opacity={0.95} />
          <circle cx={x} cy={y} r={2.2} fill={color} opacity={0.2} />
        </g>
      );
    })}
  </g>
);

// --- LAYER TOGGLE BUTTON ---
const LAYER_COLORS = { orange: '#f97316', blue: '#006FEE', purple: '#a78bfa' };

const LayerButton = ({ icon: Icon, label, sub, active, onClick, color }) => {
  const hex = LAYER_COLORS[color] || '#006FEE';
  return (
    <button
      onClick={onClick}
      className={`
        flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[10px] font-bold uppercase tracking-wider
        transition-all duration-300 border whitespace-nowrap
        ${active
          ? 'text-white shadow-lg'
          : 'bg-zinc-900/60 border-white/5 text-zinc-400 hover:text-zinc-200 hover:border-white/10'
        }
      `}
      style={active ? {
        backgroundColor: `color-mix(in srgb, ${hex} 18%, #18181b)`,
        borderColor: `color-mix(in srgb, ${hex} 45%, transparent)`,
        boxShadow: `0 0 18px color-mix(in srgb, ${hex} 22%, transparent)`,
      } : {}}
    >
      <Icon size={12} style={active ? { color: hex } : {}} />
      {label}
      {sub && (
        <span className={`text-[7px] font-bold px-1 py-0.5 rounded ${active ? 'bg-white/10 text-white/80' : 'bg-white/5 text-zinc-500'}`}>
          {sub}
        </span>
      )}
    </button>
  );
};

// ============================================================
// MAIN COMPONENT
// ============================================================
const PlayerPitchAnalytics = ({
  pitchData = {},
  heatmap = null,
  heatmapLoading = false,
  attackDirection = 'right',
}) => {
  const [layers, setLayers] = useState({
    heatmap: true,
    passNetwork: false,
    touchMap: false,
  });

  const toggleLayer = (key) => {
    setLayers(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const heatCells = heatmap?.cells || [];
  const passNetwork = pitchData?.passNetwork || [];
  const touchMap = pitchData?.touchMap || [];

  const heatmapEmpty = layers.heatmap && !heatmapLoading && heatCells.length === 0;

  return (
    <div className="flex flex-col gap-2 h-full min-h-0">
      {/* Layer Controls */}
      <div className="flex items-center gap-1.5 flex-shrink-0 flex-wrap">
        <span className="text-[9px] font-bold tracking-widest text-zinc-500 uppercase mr-1">Layers</span>
        <LayerButton
          icon={Flame}
          label="Heatmap"
          sub="Tracking"
          active={layers.heatmap}
          onClick={() => toggleLayer('heatmap')}
          color="orange"
        />
        <LayerButton
          icon={GitBranch}
          label="Pass Map"
          sub="Events"
          active={layers.passNetwork}
          onClick={() => toggleLayer('passNetwork')}
          color="blue"
        />
        <LayerButton
          icon={CircleDot}
          label="Touch Map"
          sub="Events"
          active={layers.touchMap}
          onClick={() => toggleLayer('touchMap')}
          color="purple"
        />
      </div>

      {/* SVG Pitch */}
      <div className="relative w-full rounded-xl overflow-hidden border border-white/10 bg-[#0d1117] shadow-2xl">
        <svg
          viewBox={`0 0 ${VB_W} ${VB_H}`}
          className="w-full h-auto"
          style={{ background: 'linear-gradient(180deg, #0e1f38 0%, #122a18 50%, #0e1f38 100%)' }}
        >
          <defs>
            {/* Grass stripes */}
            <pattern id="grass" width="12" height="100" patternUnits="userSpaceOnUse">
              <rect width="6" height="100" fill="rgba(34,197,94,0.07)" />
              <rect x="6" width="6" height="100" fill="rgba(34,197,94,0.035)" />
            </pattern>
            {/* Gaussian blur that fuses heat cells into a continuous surface */}
            <filter id="heatBlur" x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur stdDeviation="2.4" />
            </filter>
          </defs>
          <rect x={PAD} y={PAD} width={PITCH_W} height={PITCH_H} fill="url(#grass)" />

          {/* Data layers — heatmap below the lines so markings stay legible */}
          {layers.heatmap && heatCells.length > 0 && (
            <TrackingHeatmapLayer cells={heatCells} grid={heatmap?.grid} />
          )}

          {/* Pitch lines */}
          <PitchSVG />

          {layers.passNetwork && <PassNetworkLayer links={passNetwork} />}
          {layers.touchMap && <TouchMapLayer points={touchMap} />}

          {/* Attack direction hint */}
          <g opacity="0.5">
            <text
              x={VB_W / 2}
              y={VB_H - 1.2}
              textAnchor="middle"
              fontSize="2.6"
              fill="rgba(255,255,255,0.55)"
              fontWeight="700"
              style={{ fontFamily: 'Inter, sans-serif', letterSpacing: '0.4px', textTransform: 'uppercase' }}
            >
              {attackDirection === 'right' ? 'Attacking →' : '← Attacking'}
            </text>
          </g>
        </svg>

        {/* Heatmap loading overlay */}
        {layers.heatmap && heatmapLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/40 backdrop-blur-[2px]">
            <Loader2 size={20} className="text-primary animate-spin" />
          </div>
        )}

        {/* Heatmap empty state — player without tracking samples */}
        {heatmapEmpty && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <span className="px-3 py-1.5 rounded-lg bg-zinc-950/85 border border-white/10 text-[10px] font-bold text-zinc-400 uppercase tracking-wider">
              No tracking data for this player
            </span>
          </div>
        )}
      </div>

      {/* Legends */}
      <div className="flex items-center justify-between gap-3 px-1 flex-shrink-0 min-h-[18px]">
        {layers.heatmap && heatCells.length > 0 ? (
          <div className="flex items-center gap-2">
            <span className="text-[8px] text-zinc-500 uppercase tracking-wider font-bold">Low</span>
            <div
              className="w-24 h-1.5 rounded-full"
              style={{
                background: `linear-gradient(90deg, ${HEAT_STOPS.map(s => heatColor(s.v)).join(', ')})`,
              }}
            />
            <span className="text-[8px] text-zinc-500 uppercase tracking-wider font-bold">High</span>
            <span className="text-[8px] text-zinc-600 uppercase tracking-wider font-medium ml-2">
              Tracking · {heatmap?.samples?.toLocaleString?.() || 0} samples
            </span>
          </div>
        ) : <div />}

        {layers.touchMap && (
          <div className="flex items-center gap-3 animate-in fade-in duration-300">
            {Object.entries(TOUCH_COLORS).map(([type, color]) => (
              <div key={type} className="flex items-center gap-1">
                <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color }} />
                <span className="text-[8px] text-zinc-500 uppercase tracking-wider font-medium">{type}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default PlayerPitchAnalytics;
