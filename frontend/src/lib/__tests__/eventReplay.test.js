import { describe, it, expect } from 'vitest';
import {
  isGoalEvent,
  parseClockSeconds,
  eventLookbackSeconds,
  eventReplayTarget,
} from '../eventReplay';

describe('parseClockSeconds', () => {
  it('parsea HH:MM:SS.sss a segundos', () => {
    expect(parseClockSeconds('00:13:38.735')).toBeCloseTo(818.735);
    expect(parseClockSeconds('01:00:00')).toBe(3600);
    expect(parseClockSeconds('00:06:06')).toBe(366);
  });
  it('tolera vacío o malformado', () => {
    expect(parseClockSeconds(null)).toBe(0);
    expect(parseClockSeconds('12')).toBe(0);
  });
});

describe('isGoalEvent y lookback', () => {
  it('gol = tiro (16) con outcome 97, o item virtual de gol', () => {
    expect(isGoalEvent({ event_type_id: 16, outcome_id: 97 })).toBe(true);
    expect(isGoalEvent({ isVirtualGoal: true })).toBe(true);
    expect(isGoalEvent({ event_type_id: 16, outcome_id: 1 })).toBe(false);
    expect(isGoalEvent({ event_type_id: 30 })).toBe(false);
  });
  it('lookback: 30s para gol, 15s para el resto', () => {
    expect(eventLookbackSeconds({ event_type_id: 16, outcome_id: 97 })).toBe(30);
    expect(eventLookbackSeconds({ event_type_id: 30 })).toBe(15);
  });
});

describe('eventReplayTarget', () => {
  it('gol: 30s antes', () => {
    const t = eventReplayTarget({ period: 1, timestamp: '00:13:38.735', event_type_id: 16, outcome_id: 97 });
    expect(t.period).toBe(1);
    expect(t.seconds).toBeCloseTo(788.735);
    expect(t.lookback).toBe(30);
  });
  it('evento normal: 15s antes', () => {
    const t = eventReplayTarget({ period: 1, timestamp: '00:06:06', event_type_id: 30 });
    expect(t.seconds).toBe(351); // 366 - 15
    expect(t.lookback).toBe(15);
  });
  it('clamp a 0 si la jugada es muy temprana; periodo por defecto 1', () => {
    const t = eventReplayTarget({ timestamp: '00:00:05', event_type_id: 30 });
    expect(t.seconds).toBe(0);
    expect(t.period).toBe(1);
  });
});
