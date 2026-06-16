import React, { useRef, useEffect } from 'react';
import { pitchControlField, estimateVelocities, controlColor } from '../../lib/pitchModels';

// ============================================================
// PITCH CONTROL OVERLAY — mapa de control de campo por equipo,
// velocity-aware (tiempo-de-llegada del jugador más cercano),
// pintado en CANVAS. La velocidad se estima de frames consecutivos;
// el rectángulo de juego `bounds` mapea la rejilla métrica al mismo
// espacio de píxeles que los marcadores.
//
// Anti-parpadeo: en los ~53% de frames vacíos NO se limpia el lienzo,
// se mantiene el último render -> el mapa no parpadea.
// ============================================================
const NX = 84;
const NY = 56;
const FPS = 25; // los frame_idx vienen a 25 fps (origen)

const PitchControlOverlay = ({ points = [], frame, bounds, width, height, active = false }) => {
  const canvasRef = useRef(null);
  const prevRef = useRef({ frame: null, byId: null, vel: null });
  const drawnRef = useRef(null);

  useEffect(() => {
    if (!active) { drawnRef.current = null; return; }
    const canvas = canvasRef.current;
    if (!canvas) return;
    // Frame vacío: mantener el último render (sin parpadeo). NO limpiamos.
    if (points.length < 2) return;
    // Recalcular solo cuando cambia el frame (evita recomputar la rejilla en cada render).
    if (frame != null && frame === drawnRef.current) return;
    drawnRef.current = frame;

    // Velocidad a partir del frame previo (dt en segundos; reset en salto/seek).
    const prev = prevRef.current;
    let dt = 0;
    if (prev.frame != null && frame != null) {
      const df = frame - prev.frame;
      if (df > 0 && df < FPS) dt = df / FPS;
    }
    const vel = estimateVelocities(points, prev.byId, dt, prev.vel);
    prevRef.current = { frame, byId: new Map(points.map((p) => [p.id, p])), vel };

    const field = pitchControlField(points, vel, null, NX, NY);

    // Rejilla pintada en un canvas offscreen pequeño y escalada (suavizada).
    const off = document.createElement('canvas');
    off.width = NX;
    off.height = NY;
    const octx = off.getContext('2d');
    const img = octx.createImageData(NX, NY);
    for (let i = 0; i < NX * NY; i++) {
      const [r, g, b, a] = controlColor(field[i]);
      const o = i * 4;
      img.data[o] = r;
      img.data[o + 1] = g;
      img.data[o + 2] = b;
      img.data[o + 3] = a;
    }
    octx.putImageData(img, 0, 0);

    const ctx = canvas.getContext('2d');
    const [x0, y0, x1, y1] = bounds;
    ctx.clearRect(0, 0, width, height);
    ctx.save();
    ctx.globalAlpha = 0.85;
    ctx.imageSmoothingEnabled = true;
    ctx.drawImage(off, x0, y0, x1 - x0, y1 - y0);
    ctx.restore();
  }, [active, points, frame, bounds, width, height]);

  if (!active) return null;
  return (
    <canvas
      ref={canvasRef}
      width={width}
      height={height}
      className="absolute top-0 left-0 z-[1] pointer-events-none"
      style={{ width: '100%', height: '100%' }}
    />
  );
};

export default PitchControlOverlay;
