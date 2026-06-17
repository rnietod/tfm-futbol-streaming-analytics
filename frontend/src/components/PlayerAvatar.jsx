import React, { useState, useEffect } from 'react';
import { playerImageUrl, playerInitials } from '../utils/playerImage';

/**
 * Foto del jugador con fallback a iniciales.
 *
 * Props:
 *  - trackingId: tracking_player_id (clave de la imagen en GCS).
 *  - name: para las iniciales del fallback.
 *  - size: diámetro en px (default 40).
 *  - ring: color del borde (hex) — opcional.
 *  - className: clases extra para el contenedor.
 */
export default function PlayerAvatar({ trackingId, name, size = 40, ring, className = '' }) {
  const url = playerImageUrl(trackingId);
  const [failed, setFailed] = useState(!url);

  // Reset al cambiar de jugador.
  useEffect(() => { setFailed(!url); }, [trackingId]); // eslint-disable-line react-hooks/exhaustive-deps

  const style = {
    width: size,
    height: size,
    ...(ring ? { boxShadow: `0 0 0 2px ${ring}` } : {}),
  };

  if (failed) {
    return (
      <div
        className={`rounded-full bg-zinc-800/80 flex items-center justify-center text-zinc-300 font-bold select-none ${className}`}
        style={{ ...style, fontSize: Math.round(size * 0.36) }}
        aria-label={name}
      >
        {playerInitials(name)}
      </div>
    );
  }

  return (
    <img
      src={url}
      alt={name || 'player'}
      onError={() => setFailed(true)}
      className={`rounded-full object-cover bg-zinc-900 ${className}`}
      style={style}
      loading="lazy"
    />
  );
}
