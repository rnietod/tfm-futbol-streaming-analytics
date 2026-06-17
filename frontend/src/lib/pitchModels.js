import { Delaunay } from 'd3-delaunay';

// ============================================================
// Modelos tácticos sobre el campo (capas del Dashboard).
// Lógica pura (sin React) -> testeable con Vitest.
// ============================================================

// Paleta design-system (igual que Match Stats): local azul, visitante rojo.
export const TEAM_COLORS = { home: '#006FEE', away: '#f31260' };

// Equipo de un jugador. Mismo criterio que FootballPitch: team_id 275 = local.
export const playerTeam = (player, playerMap = {}) => {
  const meta = playerMap[String(player.player_id)] || {};
  return meta.team_id === 275 || player.team_id === 275 ? 'home' : 'away';
};

// Construye los puntos (en píxeles) de los jugadores detectados.
// `toPixels(xMeters, yMeters)` es la transformación de FootballPitch (mismo espacio
// que los marcadores), de modo que las celdas encajan exactas con ellos.
export const buildPoints = (players = [], playerMap = {}, toPixels) => {
  const points = [];
  for (const p of players) {
    if (p == null || p.x == null || p.y == null) continue;
    const { x, y } = toPixels(p.x, p.y);
    points.push({ id: p.player_id, team: playerTeam(p, playerMap), x, y });
  }
  return points;
};

// Teselación de Voronoi recortada al rectángulo de juego `bounds` = [x0,y0,x1,y1].
// Devuelve una celda por punto: { id, team, polygon:[[x,y],...] }.
export const computeVoronoiCells = (points = [], bounds) => {
  if (!points || points.length < 2 || !bounds) return [];
  const delaunay = Delaunay.from(points, (p) => p.x, (p) => p.y);
  const voronoi = delaunay.voronoi(bounds);
  const cells = [];
  points.forEach((p, i) => {
    const polygon = voronoi.cellPolygon(i);
    if (polygon && polygon.length) cells.push({ id: p.id, team: p.team, polygon });
  });
  return cells;
};

// ============================================================
// PITCH CONTROL — modelo de influencia velocity-aware (Fernández & Bornn 2018).
// Es la aproximación en forma cerrada del pitch control de Spearman; se documenta
// honestamente que NO es el Spearman íntegro (integración temporal de tiempos de
// intercepción), inviable en tiempo real en el navegador.
// ============================================================

// Puntos en coordenadas MÉTRICAS (m, origen en el centro del campo) + equipo.
export const buildMetricPoints = (players = [], playerMap = {}) => {
  const out = [];
  for (const p of players) {
    if (p == null || p.x == null || p.y == null) continue;
    out.push({ id: p.player_id, team: playerTeam(p, playerMap), x: p.x, y: p.y });
  }
  return out;
};

// Estima velocidades (m/s) a partir de dos frames consecutivos, con suavizado EMA
// y clamp a vMax. dt en segundos; si dt<=0 o no hay frame previo -> velocidad 0
// (protege contra el salto del slider en replay).
export const estimateVelocities = (curPoints = [], prevById, dt, prevVelById, opts = {}) => {
  const { vMax = 12, alpha = 0.35 } = opts;
  const vel = new Map();
  const usable = !!prevById && dt > 0;
  for (const p of curPoints) {
    if (!usable) { vel.set(p.id, { vx: 0, vy: 0 }); continue; }
    const prev = prevById.get(p.id);
    if (!prev) { vel.set(p.id, { vx: 0, vy: 0 }); continue; }
    let vx = (p.x - prev.x) / dt;
    let vy = (p.y - prev.y) / dt;
    const pv = prevVelById && prevVelById.get(p.id);
    if (pv) { vx = alpha * vx + (1 - alpha) * pv.vx; vy = alpha * vy + (1 - alpha) * pv.vy; }
    const s = Math.hypot(vx, vy);
    if (s > vMax) { vx = (vx / s) * vMax; vy = (vy / s) * vMax; }
    vel.set(p.id, { vx, vy });
  }
  return vel;
};

const PC_PITCH_L = 105;
const PC_PITCH_W = 68;

// Campo de pitch control: para cada celda, probabilidad de control del LOCAL ∈ [0,1].
// Modelo de tiempo-de-llegada (Spearman simplificado): la posición de cada jugador se
// adelanta por su velocidad (tiempo de reacción) y cada celda la controla el equipo cuyo
// jugador más cercano "llega" antes; sigmoide de la diferencia para una transición suave.
// Produce un mapa de control de campo COMPLETO y continuo (no "manchas" locales).
export const pitchControlField = (points = [], velById, _ball, nx = 84, ny = 56, opts = {}) => {
  const { reactionTime = 0.7, maxPlayerSpeed = 5.2, scale = 1.5 } = opts;
  const field = new Float32Array(nx * ny);

  const home = [];
  const away = [];
  for (const p of points) {
    const v = (velById && velById.get(p.id)) || { vx: 0, vy: 0 };
    const rx = Math.max(-PC_PITCH_L / 2, Math.min(PC_PITCH_L / 2, p.x + v.vx * reactionTime));
    const ry = Math.max(-PC_PITCH_W / 2, Math.min(PC_PITCH_W / 2, p.y + v.vy * reactionTime));
    (p.team === 'home' ? home : away).push({ rx, ry });
  }

  const k = scale / maxPlayerSpeed; // pendiente de la sigmoide por metro de ventaja
  for (let gy = 0; gy < ny; gy++) {
    const ty = PC_PITCH_W / 2 - ((gy + 0.5) / ny) * PC_PITCH_W; // fila 0 = arriba (+34)
    for (let gx = 0; gx < nx; gx++) {
      const tx = -PC_PITCH_L / 2 + ((gx + 0.5) / nx) * PC_PITCH_L;
      let pHome;
      if (!home.length) pHome = away.length ? 0 : 0.5;
      else if (!away.length) pHome = 1;
      else {
        let dH = Infinity;
        let dA = Infinity;
        for (let i = 0; i < home.length; i++) {
          const d = Math.hypot(tx - home[i].rx, ty - home[i].ry);
          if (d < dH) dH = d;
        }
        for (let i = 0; i < away.length; i++) {
          const d = Math.hypot(tx - away[i].rx, ty - away[i].ry);
          if (d < dA) dA = d;
        }
        pHome = 1 / (1 + Math.exp(-(dA - dH) * k)); // sigmoide(ventaja de distancia del local)
      }
      field[gy * nx + gx] = pHome;
    }
  }
  return field;
};

// Colormap divergente: mezcla local↔visitante según el grado de control; alpha por
// dominancia (zona disputada ~0.5 = casi transparente). Devuelve [r, g, b, a] (alpha 0-255).
export const controlColor = (pHome) => {
  const HOME = [0, 111, 238];   // #006FEE local
  const AWAY = [243, 18, 96];   // #f31260 visitante
  const r = Math.round(HOME[0] * pHome + AWAY[0] * (1 - pHome));
  const g = Math.round(HOME[1] * pHome + AWAY[1] * (1 - pHome));
  const b = Math.round(HOME[2] * pHome + AWAY[2] * (1 - pHome));
  const dominance = Math.abs(pHome - 0.5) * 2;
  const a = Math.round((0.05 + dominance * (0.48 - 0.05)) * 255);
  return [r, g, b, a];
};
