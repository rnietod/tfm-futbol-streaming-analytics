import React from 'react';

// Botón-toggle de capa con estética glassmorphism del design-system.
// Reutilizado por Match Stats (PlayerPitchAnalytics) y por las capas del campo.
export const LAYER_COLORS = {
  orange: '#f97316',
  blue: '#006FEE',
  purple: '#a78bfa',
  red: '#f31260',
  cyan: '#22d3ee',
  green: '#17c964',
};

const LayerButton = ({ icon: Icon, label, sub, active, onClick, color }) => {
  const hex = LAYER_COLORS[color] || LAYER_COLORS.blue;
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
      {Icon && <Icon size={12} style={active ? { color: hex } : {}} />}
      {label}
      {sub && (
        <span className={`text-[7px] font-bold px-1 py-0.5 rounded ${active ? 'bg-white/10 text-white/80' : 'bg-white/5 text-zinc-500'}`}>
          {sub}
        </span>
      )}
    </button>
  );
};

export default LayerButton;
