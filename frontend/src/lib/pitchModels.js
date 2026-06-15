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
