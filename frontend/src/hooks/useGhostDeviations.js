import { useState, useEffect } from 'react';

const API_BASE = (import.meta?.env?.VITE_API_BASE) || "http://127.0.0.1:8000";

/**
 * Polling del ticker Ghost. Devuelve un mapa { tracking_id(string): {deviation, status, trend, overall_score} }
 * con la desviación REAL en σ calculada por el backend (sustituye el mock Math.random).
 *
 * @param {string} matchId
 * @param {number} pollMs intervalo de polling (ms)
 */
export function useGhostDeviations(matchId, pollMs = 5000) {
  const [map, setMap] = useState({});

  useEffect(() => {
    if (!matchId) return;
    let alive = true;

    const fetchOnce = async () => {
      try {
        const res = await fetch(`${API_BASE}/match/${matchId}/ghost/ticker`);
        const data = await res.json();
        if (!alive || !data?.players) return;
        const next = {};
        for (const p of data.players) {
          next[String(p.player_id)] = {
            deviation: p.deviation,
            status: p.status,
            trend: p.trend,
            overall_score: p.overall_score,
          };
        }
        setMap(next);
      } catch (e) {
        // silencioso: el ticker cae a "sin dato" si el backend no responde
      }
    };

    fetchOnce();
    const id = setInterval(fetchOnce, pollMs);
    return () => { alive = false; clearInterval(id); };
  }, [matchId, pollMs]);

  return map;
}
