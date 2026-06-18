import { describe, it, expect } from 'vitest';
import {
  playerTeam,
  buildPoints,
  computeVoronoiCells,
  estimateVelocities,
  pitchControlField,
  controlColor,
} from '../pitchModels';

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

describe('estimateVelocities', () => {
  it('calcula velocidad = (cur - prev) / dt', () => {
    const cur = [{ id: 1, x: 1, y: 0 }];
    const prev = new Map([[1, { x: 0, y: 0 }]]);
    const vel = estimateVelocities(cur, prev, 1); // dt=1s -> 1 m/s (bajo vMax)
    expect(vel.get(1).vx).toBeCloseTo(1);
    expect(vel.get(1).vy).toBeCloseTo(0);
  });

  it('clampa el módulo a vMax', () => {
    const cur = [{ id: 1, x: 100, y: 0 }];
    const prev = new Map([[1, { x: 0, y: 0 }]]);
    const vel = estimateVelocities(cur, prev, 0.1, null, { vMax: 12 });
    expect(Math.hypot(vel.get(1).vx, vel.get(1).vy)).toBeCloseTo(12);
  });

  it('sin frame previo o dt<=0 -> velocidad 0 (protege el seek del replay)', () => {
    const cur = [{ id: 1, x: 5, y: 5 }];
    expect(estimateVelocities(cur, null, 0.1).get(1)).toEqual({ vx: 0, vy: 0 });
    expect(estimateVelocities(cur, new Map([[1, { x: 0, y: 0 }]]), 0).get(1)).toEqual({ vx: 0, vy: 0 });
  });
});

describe('pitchControlField', () => {
  it('un único equipo controla todo el campo (=1 el local, =0 el visitante)', () => {
    const home = pitchControlField([{ id: 1, team: 'home', x: 0, y: 0 }], null, null, 10, 8);
    expect(home).toHaveLength(80);
    home.forEach((v) => expect(v).toBe(1));
    const away = pitchControlField([{ id: 1, team: 'away', x: 0, y: 0 }], null, null, 10, 8);
    away.forEach((v) => expect(v).toBe(0));
  });

  it('mapa completo en [0,1]; cada equipo domina su lado', () => {
    const NX = 84;
    const NY = 56;
    const pts = [
      { id: 1, team: 'home', x: -20, y: 0 },
      { id: 2, team: 'away', x: 20, y: 0 },
    ];
    const f = pitchControlField(pts, null, null, NX, NY);
    f.forEach((v) => {
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(1);
    });
    const gy = Math.floor(NY / 2);
    expect(f[gy * NX + 8]).toBeGreaterThan(0.5);          // izquierda = local
    expect(f[gy * NX + (NX - 9)]).toBeLessThan(0.5);      // derecha = visitante
  });

  it('la velocidad inclina el control hacia donde se mueve el jugador', () => {
    const NX = 84;
    const NY = 56;
    const pts = [
      { id: 1, team: 'home', x: 0, y: 0 },
      { id: 2, team: 'away', x: 10, y: 0 },
    ];
    // local moviéndose hacia el visitante (a la derecha)
    const vel = new Map([[1, { vx: 8, vy: 0 }], [2, { vx: 0, vy: 0 }]]);
    const still = pitchControlField(pts, null, null, NX, NY);
    const moving = pitchControlField(pts, vel, null, NX, NY);
    const gy = Math.floor(NY / 2);
    const cellRight = gy * NX + Math.floor(NX * 0.6); // celda a la derecha del centro
    expect(moving[cellRight]).toBeGreaterThan(still[cellRight]); // gana control el local
  });
});

describe('controlColor', () => {
  it('local azul, visitante rojo, disputado más transparente', () => {
    expect(controlColor(1).slice(0, 3)).toEqual([0, 111, 238]);
    expect(controlColor(0).slice(0, 3)).toEqual([243, 18, 96]);
    expect(controlColor(0.5)[3]).toBeLessThan(controlColor(1)[3]);
  });
});
