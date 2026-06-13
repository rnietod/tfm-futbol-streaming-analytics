import { useState, useEffect } from 'react';

// ============================================================
// useDofa — hooks para el análisis DOFA (BigQuery, ponderado por recencia)
// Las consultas a BigQuery son lentas en frío (~10-15s) y se cachean en el
// backend (Redis, 24h), así que las posteriores son instantáneas.
// ============================================================

const API_BASE = 'http://127.0.0.1:8000';

/** Equipos del DOFA: nuestro equipo (Real Madrid) y el rival (oponente de test_match). */
export const useDofaTeams = () => {
  const [teams, setTeams] = useState({ ourTeam: 'Real Madrid', rival: 'Atlético de Madrid' });

  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/dofa/teams`)
      .then(res => res.json())
      .then(data => { if (!cancelled && !data.error) setTeams(data); })
      .catch(err => console.error('❌ useDofaTeams error:', err));
    return () => { cancelled = true; };
  }, []);

  return teams;
};

/** Paquete DOFA completo de un equipo (summary, SWOT, goles/min, tops, XI, tiros, heatmap). */
export const useDofaOverview = (team) => {
  const [data, setData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!team) return;
    let cancelled = false;
    setIsLoading(true);
    setError(null);
    setData(null);

    fetch(`${API_BASE}/dofa/overview?team=${encodeURIComponent(team)}`)
      .then(res => res.json())
      .then(d => {
        if (cancelled) return;
        if (d.error) setError(d.error);
        else setData(d);
      })
      .catch(err => { if (!cancelled) setError(err.message); })
      .finally(() => { if (!cancelled) setIsLoading(false); });

    return () => { cancelled = true; };
  }, [team]);

  return { data, isLoading, error };
};

/** 11 ideal heurístico de nuestro equipo (Real Madrid) frente al rival. */
export const useIdealXI = (team = 'Real Madrid', rival = null) => {
  const [data, setData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!team) return;
    let cancelled = false;
    setIsLoading(true);
    setData(null);

    const rivalQs = rival ? `&rival=${encodeURIComponent(rival)}` : '';
    fetch(`${API_BASE}/dofa/ideal-xi?team=${encodeURIComponent(team)}${rivalQs}`)
      .then(res => res.json())
      .then(d => { if (!cancelled && !d.error) setData(d); })
      .catch(err => console.error('❌ useIdealXI error:', err))
      .finally(() => { if (!cancelled) setIsLoading(false); });

    return () => { cancelled = true; };
  }, [team, rival]);

  return { data, isLoading };
};
