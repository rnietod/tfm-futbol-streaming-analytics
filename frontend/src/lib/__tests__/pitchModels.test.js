import { describe, it, expect } from 'vitest';
import { playerTeam, buildPoints, computeVoronoiCells } from '../pitchModels';

describe('playerTeam', () => {
  it('team_id 275 es local, el resto visitante', () => {
    expect(playerTeam({ player_id: 1, team_id: 275 })).toBe('home');
    expect(playerTeam({ player_id: 2, team_id: 999 })).toBe('away');
  });

  it('usa playerMap si el jugador no trae team_id', () => {
    expect(playerTeam({ player_id: 5 }, { 5: { team_id: 275 } })).toBe('home');
    expect(playerTeam({ player_id: 5 }, { 5: { team_id: 1 } })).toBe('away');
  });
});

describe('buildPoints', () => {
  const toPixels = (x, y) => ({ x: x * 2, y: y * 2 }); // transform de prueba

  it('mapea equipo y píxeles, descarta posiciones nulas', () => {
    const players = [
      { player_id: 1, team_id: 275, x: 10, y: 5 },
      { player_id: 2, team_id: 99, x: 20, y: 8 },
      { player_id: 3, team_id: 275, x: null, y: 3 }, // descartado
    ];
    const pts = buildPoints(players, {}, toPixels);
    expect(pts).toHaveLength(2);
    expect(pts[0]).toMatchObject({ id: 1, team: 'home', x: 20, y: 10 });
    expect(pts[1].team).toBe('away');
  });
});

describe('computeVoronoiCells', () => {
  const bounds = [0, 0, 100, 100];

  it('genera una celda por punto, etiquetada por equipo y con polígono', () => {
    const pts = [
      { id: 1, team: 'home', x: 25, y: 25 },
      { id: 2, team: 'away', x: 75, y: 75 },
      { id: 3, team: 'home', x: 25, y: 75 },
    ];
    const cells = computeVoronoiCells(pts, bounds);
    expect(cells).toHaveLength(3);
    cells.forEach((c) => {
      expect(c.polygon.length).toBeGreaterThan(2);
      expect(['home', 'away']).toContain(c.team);
    });
  });

  it('devuelve [] con menos de 2 puntos o sin bounds', () => {
    expect(computeVoronoiCells([{ id: 1, team: 'home', x: 5, y: 5 }], bounds)).toEqual([]);
    expect(computeVoronoiCells([], bounds)).toEqual([]);
    expect(computeVoronoiCells([{ id: 1, team: 'home', x: 5, y: 5 }, { id: 2, team: 'away', x: 9, y: 9 }], null)).toEqual([]);
  });
});
