import React, { useState } from 'react';
import { Flame, GitBranch, CircleDot } from 'lucide-react';

// ============================================================
// PLAYER PITCH ANALYTICS — SVG Pitch with Layer Overlays
// ============================================================

// Pitch dimensions in viewBox coordinates
const PITCH_W = 120;
const PITCH_H = 80;
const PAD = 10; // padding around the pitch lines
const VB_W = PITCH_W + PAD * 2;  // 140
const VB_H = PITCH_H + PAD * 2;  // 100

// Convert normalized data coords (0-100) to viewBox coords
const toVB = (xNorm, yNorm) => ({
  x: PAD + (xNorm / 100) * PITCH_W,
  y: PAD + (yNorm / 100) * PITCH_H,
});

// --- SVG PITCH MARKINGS ---
const PitchSVG = () => (
  <g stroke="rgba(255,255,255,0.25)" strokeWidth="0.4" fill="none">
    {/* Outline */}
    <rect x={PAD} y={PAD} width={PITCH_W} height={PITCH_H} rx="0.5" />
    {/* Center line */}
    <line x1={PAD + PITCH_W / 2} y1={PAD} x2={PAD + PITCH_W / 2} y2={PAD + PITCH_H} />
    {/* Center circle */}
    <circle cx={PAD + PITCH_W / 2} cy={PAD + PITCH_H / 2} r={9.15} />
    <circle cx={PAD + PITCH_W / 2} cy={PAD + PITCH_H / 2} r={0.6} fill="rgba(255,255,255,0.25)" />
    {/* Left penalty area */}
    <rect x={PAD} y={PAD + PITCH_H / 2 - 20.16} width={16.5} height={40.32} />
    <rect x={PAD} y={PAD + PITCH_H / 2 - 9.16} width={5.5} height={18.32} />
    <circle cx={PAD + 11} cy={PAD + PITCH_H / 2} r={0.5} fill="rgba(255,255,255,0.2)" />
    {/* Right penalty area */}
    <rect x={PAD + PITCH_W - 16.5} y={PAD + PITCH_H / 2 - 20.16} width={16.5} height={40.32} />
    <rect x={PAD + PITCH_W - 5.5} y={PAD + PITCH_H / 2 - 9.16} width={5.5} height={18.32} />
    <circle cx={PAD + PITCH_W - 11} cy={PAD + PITCH_H / 2} r={0.5} fill="rgba(255,255,255,0.2)" />
    {/* Corner arcs */}
    <path d={`M ${PAD + 2} ${PAD} A 2 2 0 0 0 ${PAD} ${PAD + 2}`} />
    <path d={`M ${PAD + PITCH_W} ${PAD + 2} A 2 2 0 0 0 ${PAD + PITCH_W - 2} ${PAD}`} />
    <path d={`M ${PAD} ${PAD + PITCH_H - 2} A 2 2 0 0 0 ${PAD + 2} ${PAD + PITCH_H}`} />
    <path d={`M ${PAD + PITCH_W - 2} ${PAD + PITCH_H} A 2 2 0 0 0 ${PAD + PITCH_W} ${PAD + PITCH_H - 2}`} />
  </g>
);

// --- HEATMAP LAYER ---
const HeatmapLayer = ({ points = [] }) => (
  <g>
    <defs>
      <radialGradient id="heatGrad">
        <stop offset="0%" stopColor="#f97316" stopOpacity="0.8" />
        <stop offset="40%" stopColor="#ef4444" stopOpacity="0.4" />
        <stop offset="100%" stopColor="#ef4444" stopOpacity="0" />
      </radialGradient>
    </defs>
    {points.map((pt, i) => {
      const { x, y } = toVB(pt.x, pt.y);
      return (
        <circle
          key={`heat-${i}`}
          cx={x}
          cy={y}
          r={4 + pt.intensity * 6}
          fill="url(#heatGrad)"
          opacity={0.3 + pt.intensity * 0.5}
          style={{ mixBlendMode: 'screen' }}
        />
      );
    })}
  </g>
);

// --- PASS NETWORK LAYER ---
const PassNetworkLayer = ({ links = [] }) => (
  <g>
    <defs>
      <marker id="arrowhead" markerWidth="4" markerHeight="3" refX="4" refY="1.5" orient="auto">
        <polygon points="0 0, 4 1.5, 0 3" fill="rgba(0,111,238,0.6)" />
      </marker>
    </defs>
    {links.map((link, i) => {
      const from = toVB(link.from_x, link.from_y);
      const to = toVB(link.to_x, link.to_y);
      const strokeW = Math.min(0.3 + link.count * 0.15, 1.5);
      const opacity = Math.min(0.3 + link.count * 0.1, 0.9);
      return (
        <line
          key={`pass-${i}`}
          x1={from.x}
          y1={from.y}
          x2={to.x}
          y2={to.y}
          stroke={link.successful ? '#006FEE' : '#f31260'}
          strokeWidth={strokeW}
          opacity={opacity}
          markerEnd="url(#arrowhead)"
          strokeLinecap="round"
        />
      );
    })}
    {/* Draw nodes at unique pass origins */}
    {links.map((link, i) => {
      const pos = toVB(link.from_x, link.from_y);
      return (
        <circle
          key={`pnode-${i}`}
          cx={pos.x}
          cy={pos.y}
          r={0.8 + link.count * 0.2}
          fill="#006FEE"
          opacity={0.6}
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
          <circle
            cx={x}
            cy={y}
            r={1.2}
            fill={color}
            opacity={0.85}
          />
          <circle
            cx={x}
            cy={y}
            r={2.2}
            fill={color}
            opacity={0.15}
          />
        </g>
      );
    })}
  </g>
);

// --- LAYER TOGGLE BUTTON ---
const LayerButton = ({ icon: Icon, label, active, onClick, color }) => (
  <button
    onClick={onClick}
    className={`
      flex items-center gap-2 px-3 py-2 rounded-lg text-[11px] font-bold uppercase tracking-wider
      transition-all duration-300 border
      ${active
        ? `bg-${color}/20 border-${color}/40 text-white shadow-lg`
        : 'bg-zinc-900/60 border-white/5 text-zinc-500 hover:text-zinc-300 hover:border-white/10'
      }
    `}
    style={active ? {
      backgroundColor: `color-mix(in srgb, ${color === 'orange' ? '#f97316' : color === 'blue' ? '#006FEE' : '#a78bfa'} 15%, transparent)`,
      borderColor: `color-mix(in srgb, ${color === 'orange' ? '#f97316' : color === 'blue' ? '#006FEE' : '#a78bfa'} 40%, transparent)`,
      boxShadow: `0 0 20px color-mix(in srgb, ${color === 'orange' ? '#f97316' : color === 'blue' ? '#006FEE' : '#a78bfa'} 20%, transparent)`,
    } : {}}
  >
    <Icon size={14} />
    {label}
  </button>
);

// ============================================================
// MAIN COMPONENT
// ============================================================
const PlayerPitchAnalytics = ({ pitchData = {} }) => {
  const [layers, setLayers] = useState({
    heatmap: true,
    passNetwork: false,
    touchMap: false,
  });

  const toggleLayer = (key) => {
    setLayers(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const heatmap = pitchData.heatmap || [];
  const passNetwork = pitchData.passNetwork || [];
  const touchMap = pitchData.touchMap || [];

  return (
    <div className="flex flex-col gap-3">
      {/* Layer Controls */}
      <div className="flex items-center gap-2">
        <span className="text-[9px] font-bold tracking-widest text-zinc-600 uppercase mr-1">Layers</span>
        <LayerButton
          icon={Flame}
          label="Heatmap"
          active={layers.heatmap}
          onClick={() => toggleLayer('heatmap')}
          color="orange"
        />
        <LayerButton
          icon={GitBranch}
          label="Pass Network"
          active={layers.passNetwork}
          onClick={() => toggleLayer('passNetwork')}
          color="blue"
        />
        <LayerButton
          icon={CircleDot}
          label="Touch Map"
          active={layers.touchMap}
          onClick={() => toggleLayer('touchMap')}
          color="purple"
        />
      </div>

      {/* SVG Pitch */}
      <div className="relative w-full rounded-xl overflow-hidden border border-white/5 bg-[#0d1117] shadow-2xl">
        <svg
          viewBox={`0 0 ${VB_W} ${VB_H}`}
          className="w-full h-auto"
          style={{ background: 'linear-gradient(180deg, #0a1628 0%, #0d1a0d 50%, #0a1628 100%)' }}
        >
          {/* Pitch grass texture (subtle stripes) */}
          <defs>
            <pattern id="grass" width="6" height="100" patternUnits="userSpaceOnUse">
              <rect width="3" height="100" fill="rgba(34,197,94,0.03)" />
              <rect x="3" width="3" height="100" fill="rgba(34,197,94,0.015)" />
            </pattern>
          </defs>
          <rect x={PAD} y={PAD} width={PITCH_W} height={PITCH_H} fill="url(#grass)" />

          {/* Pitch lines */}
          <PitchSVG />

          {/* Data layers — order matters for visibility */}
          {layers.heatmap && <HeatmapLayer points={heatmap} />}
          {layers.passNetwork && <PassNetworkLayer links={passNetwork} />}
          {layers.touchMap && <TouchMapLayer points={touchMap} />}
        </svg>

        {/* Subtle vignette overlay */}
        <div
          className="absolute inset-0 pointer-events-none"
          style={{
            background: 'radial-gradient(ellipse at center, transparent 50%, rgba(0,0,0,0.4) 100%)',
          }}
        />
      </div>

      {/* Touch Map Legend */}
      {layers.touchMap && (
        <div className="flex items-center gap-4 px-2 animate-in fade-in duration-300">
          {Object.entries(TOUCH_COLORS).map(([type, color]) => (
            <div key={type} className="flex items-center gap-1.5">
              <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
              <span className="text-[9px] text-zinc-500 uppercase tracking-wider font-medium">{type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default PlayerPitchAnalytics;
