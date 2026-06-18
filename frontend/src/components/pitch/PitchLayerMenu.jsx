import React, { useState, useRef, useEffect } from 'react';
import { Layers, Hexagon, Gauge } from 'lucide-react';
import LayerButton from '../ui/LayerButton';

// Menú desplegable de capas tácticas, pensado para vivir dentro de la barra de
// reproducción (abajo). Se abre hacia arriba; cada capa se activa de forma
// independiente y al activarse se colorea (estilo LayerButton del design-system).
const PitchLayerMenu = ({ layers = {}, onToggle }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const anyActive = !!(layers.voronoi || layers.pitchControl);

  useEffect(() => {
    if (!open) return undefined;
    const onDoc = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [open]);

  return (
    <div className="relative flex-shrink-0" ref={ref}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={`flex items-center gap-1.5 px-3 h-8 rounded-lg text-[10px] font-bold uppercase tracking-wider transition-all duration-300 border
          ${anyActive ? 'text-white' : 'bg-zinc-900/60 border-white/5 text-zinc-400 hover:text-zinc-200 hover:border-white/10'}`}
        style={anyActive ? {
          backgroundColor: 'color-mix(in srgb, #006FEE 16%, #18181b)',
          borderColor: 'color-mix(in srgb, #006FEE 45%, transparent)',
          boxShadow: '0 0 14px color-mix(in srgb, #006FEE 18%, transparent)',
        } : {}}
      >
        <Layers size={13} style={anyActive ? { color: '#006FEE' } : {}} />
        Capas
        {anyActive && <span className="ml-0.5 w-1.5 h-1.5 rounded-full bg-[#006FEE]" />}
      </button>

      {open && (
        <div className="absolute bottom-full right-0 mb-2 z-50 flex flex-col gap-1.5 p-2 rounded-xl bg-zinc-900/95 backdrop-blur-xl border border-white/10 shadow-2xl min-w-[190px]">
          <span className="text-[8px] font-bold tracking-widest text-zinc-500 uppercase px-1 select-none">Capas tácticas</span>
          <LayerButton
            icon={Hexagon}
            label="Voronoi"
            active={!!layers.voronoi}
            onClick={() => onToggle('voronoi')}
            color="blue"
          />
          <LayerButton
            icon={Gauge}
            label="Pitch Control"
            active={!!layers.pitchControl}
            onClick={() => onToggle('pitchControl')}
            color="red"
          />
          {layers.pitchControl && (
            <div className="flex items-center gap-2 px-1 pt-1">
              <span className="text-[8px] font-bold text-[#f31260] uppercase tracking-wider">Visit.</span>
              <div
                className="flex-1 h-1.5 rounded-full"
                style={{ background: 'linear-gradient(90deg, #f31260, rgba(255,255,255,0.12), #006FEE)' }}
              />
              <span className="text-[8px] font-bold text-[#006FEE] uppercase tracking-wider">Local</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default PitchLayerMenu;
