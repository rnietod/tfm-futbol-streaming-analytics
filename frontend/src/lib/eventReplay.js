// ============================================================
// Lógica para saltar el replay a una jugada al clicar un evento.
// Pura y testeable (sin React ni fetch).
// ============================================================

// ¿El evento es un gol? (mismo criterio que el marcador y el feed)
export const isGoalEvent = (evt) =>
  !!(evt && ((evt.event_type_id === 16 && evt.outcome_id === 97) || evt.isVirtualGoal));

// Segundos de juego (dentro del periodo) a partir del reloj "HH:MM:SS.sss".
export const parseClockSeconds = (ts) => {
  if (!ts) return 0;
  const parts = String(ts).split(':');
  if (parts.length < 3) return 0;
  const h = parseInt(parts[0], 10) || 0;
  const m = parseInt(parts[1], 10) || 0;
  const s = parseFloat(parts[2]) || 0;
  return h * 3600 + m * 60 + s;
};

// Antesala: 30 s antes si es gol, 15 s en el resto.
export const eventLookbackSeconds = (evt) => (isGoalEvent(evt) ? 30 : 15);

// Parámetros para pedir el frame al backend (frame_at): periodo + segundo objetivo.
export const eventReplayTarget = (evt) => {
  const lookback = eventLookbackSeconds(evt);
  const seconds = Math.max(0, parseClockSeconds(evt && evt.timestamp) - lookback);
  return { period: (evt && evt.period) || 1, seconds, lookback };
};
