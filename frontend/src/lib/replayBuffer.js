// ============================================================
// replayBuffer — helpers puros de buffering y cadencia del replay
// ------------------------------------------------------------
// Sin React: lógica testeable (Vitest). La usan useMatchHistory (fetch/buffer)
// y el game loop de App.jsx (avance de frames).
//
// Contexto de datos (ver inspección de match_tracking):
//   · frame_idx es ~contiguo (delta=1 en el 99,9%).
//   · ~53% de los frames están VACÍOS (player_data: []): pre-saque, descanso,
//     post-partido. Hay que saltarlos para no dejar el campo en blanco.
//   · Los frames con juego van espaciados 100 ms (10 Hz); el reloj de juego está
//     en frame.timestamp ('YYYY-MM-DD HH:MM:SS.mmm', fecha basura + hora real).
// ============================================================

export const BUFFER_SIZE = 500;
export const FPS = 10; // frames con juego espaciados 100 ms
export const PREFETCH_THRESHOLD = 150; // frames (~15 s) antes del fin de chunk → prefetch

// Alinea un frame cualquiera al inicio de su chunk fijo (0, 500, 1000, …).
export const alignChunkStart = (frame, size = BUFFER_SIZE) =>
  Math.floor(Math.max(0, frame) / size) * size;

// Inicio del siguiente chunk fijo.
export const nextChunkStart = (frame, size = BUFFER_SIZE) =>
  alignChunkStart(frame, size) + size;

// Un frame está "vacío" cuando no hay jugadores detectados.
export const isEmptyFrame = (frameData) =>
  !frameData ||
  !Array.isArray(frameData.player_data) ||
  frameData.player_data.length === 0;

// Primer frame con juego en/ tras `from`, escaneando SOLO lo que hay en buffer.
// - Si encuentra un frame desconocido (no cargado) se detiene ahí para que el
//   fetch lo rellene (no nos saltamos zonas aún sin cargar).
// - Si encuentra uno poblado, lo devuelve.
export const nextPopulatedFrame = (buffer, from, maxFrame) => {
  for (let f = from; f <= maxFrame; f++) {
    const data = buffer[f];
    if (data === undefined) return f; // desconocido → parar, dejar que cargue
    if (!isEmptyFrame(data)) return f; // poblado → encontrado
  }
  return Math.min(from, maxFrame);
};

// Delay (ms) hasta el siguiente frame, a partir de timestamps de juego
// consecutivos. Fallback cuando faltan (frames vacíos sin timestamp).
export const computeFrameDelay = (buffer, frame, fallback = 1000 / FPS) => {
  const a = buffer[frame];
  const b = buffer[frame + 1];
  if (!a || !b || !a.timestamp || !b.timestamp) return fallback;
  const ta = new Date(String(a.timestamp).replace(' ', 'T')).getTime();
  const tb = new Date(String(b.timestamp).replace(' ', 'T')).getTime();
  const diff = tb - ta;
  if (Number.isNaN(diff) || diff <= 0) return fallback;
  return Math.min(diff, 1000); // tope de 1 s por si hay saltos
};

// ¿Conviene prefetch del siguiente chunk dado el frame que mostramos ahora?
export const shouldPrefetchNext = (
  frame,
  threshold = PREFETCH_THRESHOLD,
  size = BUFFER_SIZE
) => {
  const chunkStart = alignChunkStart(frame, size);
  return frame >= chunkStart + size - threshold;
};

// ¿Está el chunk que contiene `frame` ya cargado en el buffer?
// (basta con que exista su primer frame; los chunks se cargan completos.)
export const isChunkLoaded = (buffer, chunkStart) =>
  buffer[chunkStart] !== undefined;
