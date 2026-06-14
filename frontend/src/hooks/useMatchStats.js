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


/**
 * Hook para obtener los disparos del partido (Shot Map)
 */
export const useShotMap = (matchId) => {
  const [shotData, setShotData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!matchId) return;

    let cancelled = false;
    fetch(`${API_BASE}/match/${matchId}/shots`)
      .then(res => res.json())
      .then(data => {
        if (!cancelled && !data.error) {
          setShotData(data);
        }
      })
      .catch(err => console.error('❌ useShotMap error:', err))
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [matchId]);

  return { shotData, isLoading };
};


/**
 * Hook para métricas físicas de tracking (distancias y velocidades).
 * El backend cachea en Redis con TTL 60s, así que el polling usa el mismo intervalo.
 */
export const useTrackingMetrics = (matchId) => {
  const [trackingMetrics, setTrackingMetrics] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!matchId) return;

    let cancelled = false;
    const fetchMetrics = () => {
      fetch(`${API_BASE}/match/${matchId}/tracking/metrics`)
        .then(res => res.json())
        .then(data => {
          if (!cancelled && !data.error) {
            setTrackingMetrics(data);
          }
        })
        .catch(err => console.error('❌ useTrackingMetrics error:', err))
        .finally(() => {
          if (!cancelled) setIsLoading(false);
        });
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 60000);
    return () => { cancelled = true; clearInterval(interval); };
  }, [matchId]);

  return { trackingMetrics, isLoading };
};


/**
 * Hook para el heatmap posicional de un jugador basado en datos de TRACKING
 * (match_tracking), no de eventing. Devuelve celdas 0-100 con intensidad 0-1.
 */
export const usePlayerTrackingHeatmap = (matchId, playerId) => {
  const [heatmap, setHeatmap] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!matchId || !playerId) {
      setHeatmap(null);
      return;
    }

    let cancelled = false;
    setIsLoading(true);
    setHeatmap(null);

    fetch(`${API_BASE}/match/${matchId}/player/${playerId}/tracking/heatmap`)
      .then(res => res.json())
      .then(data => {
        if (!cancelled && !data.error) {
          setHeatmap(data);
        }
      })
      .catch(err => console.error('❌ usePlayerTrackingHeatmap error:', err))
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [matchId, playerId]);

  return { heatmap, isLoading };
};


/**
 * Hook para las métricas físicas detalladas de UN jugador
 * (incluye speed_per_second para la gráfica de velocidad).
 */
export const usePlayerTrackingDetail = (matchId, playerId) => {
  const [detail, setDetail] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!matchId || !playerId) {
      setDetail(null);
      return;
    }

    let cancelled = false;
    setIsLoading(true);
    setDetail(null);

    fetch(`${API_BASE}/match/${matchId}/player/${playerId}/tracking/metrics`)
      .then(res => res.json())
      .then(data => {
        if (!cancelled && !data.error && !data.detail) {
          setDetail(data);
        }
      })
      .catch(err => console.error('❌ usePlayerTrackingDetail error:', err))
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [matchId, playerId]);

  return { detail, isLoading };
};


/**
 * Hook para obtener el momentum del partido minuto a minuto
 */
export const useMomentum = (matchId) => {
  const [momentumData, setMomentumData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!matchId) return;

    let cancelled = false;
    fetch(`${API_BASE}/match/${matchId}/momentum`)
      .then(res => res.json())
      .then(data => {
        if (!cancelled && !data.error) {
          setMomentumData(data);
        }
      })
      .catch(err => console.error('❌ useMomentum error:', err))
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => { cancelled = true; };
  }, [matchId]);

  return { momentumData, isLoading };
};

