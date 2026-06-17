import { describe, it, expect } from 'vitest';
import {
  alignChunkStart,
  nextChunkStart,
  isEmptyFrame,
  nextPopulatedFrame,
  computeFrameDelay,
  shouldPrefetchNext,
  isChunkLoaded,
  BUFFER_SIZE,
} from '../replayBuffer';

const populated = (ts) => ({ player_data: [{ x: 1, y: 2 }], timestamp: ts });
const empty = (ts) => ({ player_data: [], timestamp: ts ?? null });

describe('alignChunkStart / nextChunkStart', () => {
  it('alinea a límites fijos de chunk', () => {
    expect(alignChunkStart(0)).toBe(0);
    expect(alignChunkStart(499)).toBe(0);
    expect(alignChunkStart(500)).toBe(500);
    expect(alignChunkStart(1340)).toBe(1000);
    expect(alignChunkStart(-5)).toBe(0); // no negativos
  });
  it('nextChunkStart salta al siguiente bloque', () => {
    expect(nextChunkStart(1340)).toBe(1500);
    expect(nextChunkStart(0)).toBe(BUFFER_SIZE);
  });
});

describe('isEmptyFrame', () => {
  it('detecta frames sin jugadores', () => {
    expect(isEmptyFrame(undefined)).toBe(true);
    expect(isEmptyFrame(empty())).toBe(true);
    expect(isEmptyFrame({ player_data: null })).toBe(true);
    expect(isEmptyFrame(populated('2026-06-11 00:00:00.000'))).toBe(false);
  });
});

describe('nextPopulatedFrame', () => {
  it('salta frames vacíos hasta el primero poblado', () => {
    const buf = {
      10: empty(),
      11: empty(),
      12: populated('2026-06-11 00:00:01.000'),
    };
    expect(nextPopulatedFrame(buf, 10, 100)).toBe(12);
  });
  it('se detiene en un frame desconocido (aún sin cargar)', () => {
    const buf = { 10: empty() }; // 11 no está en el buffer
    expect(nextPopulatedFrame(buf, 10, 100)).toBe(11);
  });
  it('devuelve el mismo frame si ya es poblado', () => {
    const buf = { 5: populated('2026-06-11 00:00:00.500') };
    expect(nextPopulatedFrame(buf, 5, 100)).toBe(5);
  });
});

describe('computeFrameDelay', () => {
  it('calcula 100 ms entre frames de juego consecutivos', () => {
    const buf = {
      1340: populated('2026-06-11 00:00:00.000'),
      1341: populated('2026-06-11 00:00:00.100'),
    };
    expect(computeFrameDelay(buf, 1340)).toBe(100);
  });
  it('usa el fallback cuando falta timestamp', () => {
    const buf = { 1: empty(), 2: empty() };
    expect(computeFrameDelay(buf, 1, 40)).toBe(40);
  });
  it('usa el fallback ante delta no positivo o NaN', () => {
    const buf = {
      1: populated('2026-06-11 00:00:01.000'),
      2: populated('2026-06-11 00:00:00.000'), // hacia atrás
    };
    expect(computeFrameDelay(buf, 1, 100)).toBe(100);
  });
});

describe('shouldPrefetchNext / isChunkLoaded', () => {
  it('dispara prefetch cerca del final del chunk', () => {
    expect(shouldPrefetchNext(700)).toBe(false); // chunk 500..1000, lejos del fin (umbral 850)
    expect(shouldPrefetchNext(870)).toBe(true); // dentro del umbral de 150
    expect(shouldPrefetchNext(999)).toBe(true);
  });
  it('isChunkLoaded comprueba presencia del primer frame del chunk', () => {
    expect(isChunkLoaded({ 500: empty() }, 500)).toBe(true);
    expect(isChunkLoaded({}, 500)).toBe(false);
  });
});
