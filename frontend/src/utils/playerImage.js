// Resolución de la foto del jugador.
//
// Las imágenes viven en un bucket PRIVADO de GCS y las sirve un proxy del
// backend (GET /player-images/{tracking_id}.png). La clave del objeto es el
// tracking_player_id, que es el id que usa el frontend en todas partes.
//
// En dev apunta al backend local; en prod (Vercel) se puede sobreescribir con
// VITE_API_BASE. Coincide con el host que usa el resto del frontend.
const API_BASE = (import.meta?.env?.VITE_API_BASE) || "http://127.0.0.1:8000";

/** URL de la foto del jugador, o null si no hay id. */
export function playerImageUrl(trackingId) {
  if (trackingId === undefined || trackingId === null || trackingId === "") return null;
  return `${API_BASE}/player-images/${trackingId}.png`;
}

/** Iniciales para el fallback cuando no hay foto. */
export function playerInitials(name) {
  if (!name) return "?";
  const parts = String(name).replace(/\./g, " ").trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}
