import React from 'react';
import { Hexagon } from 'lucide-react';
import LayerButton from '../ui/LayerButton';

// Barra de toggles de capas tácticas sobre el campo (esquina superior izquierda;
// el marcador va centrado). Cada capa se activa de forma independiente.
const PitchLayerToggles = ({ layers, onToggle }) => (
  <div className="absolute top-3 left-3 z-40 inline-flex items-center gap-1.5 rounded-xl bg-zinc-900/80 backdrop-blur border border-white/5 p-1.5 shadow-lg">
    <span className="text-[9px] font-bold tracking-widest text-zinc-500 uppercase px-1 select-none">Capas</span>
    <LayerButton
      icon={Hexagon}
      label="Voronoi"
      active={!!layers.voronoi}
      onClick={() => onToggle('voronoi')}
      color="blue"
    />
  </div>
);

export default PitchLayerToggles;
