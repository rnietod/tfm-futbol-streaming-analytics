import { useState, useEffect, useCallback, useRef } from 'react';

// ============================================================
// useMatchStats — Polling hook for Match Stats Hub
// ============================================================

const API_BASE = 'http://127.0.0.1:8000';
const POLL_INTERVAL = 5000; // 5 seconds

/**
 * Hook principal: datos de equipo + jugadores (se refresca cada 5s)
 */
export const useMatchStats = (matchId) => {
  const [statsData, setStatsData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const intervalRef = useRef(null);

  const fetchStats = useCallback(async () => {
    if (!matchId) return;

    try {
      const res = await fetch(`${API_BASE}/match/${matchId}/stats`);
      const data = await res.json();

      if (data.error) {
        throw new Error(data.error);
      }

      setStatsData(data);
      setError(null);
    } catch (err) {
      console.error('❌ useMatchStats fetch error:', err);
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  }, [matchId]);

  // Initial fetch + periodic polling
  useEffect(() => {
    fetchStats();
    intervalRef.current = setInterval(fetchStats, POLL_INTERVAL);
    return () => clearInterval(intervalRef.current);
  }, [fetchStats]);

  // Refresh when browser tab regains focus
  useEffect(() => {
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        fetchStats();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, [fetchStats]);

  return { statsData, isLoading, error, refetch: fetchStats };
};


/**
 * Hook secundario: datos de pitch para un jugador específico (on-demand)
 */
export const usePlayerPitchData = (matchId, playerId) => {
  const [pitchData, setPitchData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!matchId || !playerId) {
      setPitchData(null);
      return;
    }

    let cancelled = false;
    setIsLoading(true);

    fetch(`${API_BASE}/match/${matchId}/player/${playerId}/pitch`)
      .then(res => res.json())
      .then(data => {
        if (!cancelled && !data.error) {
          setPitchData(data);
        }
      })
      .catch(err => console.error('❌ usePlayerPitchData error:', err))
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [matchId, playerId]);

  return { pitchData, isLoading };
};
