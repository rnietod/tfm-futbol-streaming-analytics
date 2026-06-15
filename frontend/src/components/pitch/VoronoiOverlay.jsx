import React from 'react';
import { computeVoronoiCells, TEAM_COLORS } from '../../lib/pitchModels';

// ============================================================
// VORONOI OVERLAY — región del campo dominada por cada jugador
// (vecino más próximo), coloreada por equipo. SVG vectorial con
// viewBox en el mismo espacio de píxeles que los marcadores, por
// lo que las celdas encajan exactas con ellos.
//   · `combined`: cuando el Pitch Control también está activo, se
//     dibuja solo el borde (sin relleno) para que compongan bien.
// ============================================================
const VoronoiOverlay = ({ points = [], bounds, width, height, active = false, combined = false }) => {
  if (!active) return null;
  const cells = computeVoronoiCells(points, bounds);
  if (cells.length === 0) return null;

  return (
    <svg
      width="100%"
      height="100%"
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      className="absolute top-0 left-0 z-[2] pointer-events-none"
    >
      {cells.map((cell) => {
        const color = TEAM_COLORS[cell.team] || TEAM_COLORS.home;
        const pts = cell.polygon.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
        return (
          <polygon
            key={cell.id}
            points={pts}
            fill={combined ? 'none' : color}
            fillOpacity={combined ? 0 : 0.14}
            stroke={color}
            strokeOpacity={combined ? 0.9 : 0.5}
            strokeWidth={combined ? 1.4 : 1}
            strokeLinejoin="round"
          />
        );
      })}
    </svg>
  );
};

export default VoronoiOverlay;
