# Frontend — Hooks de datos

Hooks en `frontend/src/hooks/`. Encapsulan el acceso a la API (polling REST +
WebSocket) y la gestión de caché/replay. Todos limpian sus efectos (clearInterval,
flag `cancelled`/`alive`, AbortController) al desmontar. Base de API:
`VITE_API_BASE` (por defecto `http://127.0.0.1:8000`).

---

## `useMatchHistory.js`

### `useMatchHistory(matchId, isLive, requestedFrame, liveData, isPlaying)`
Núcleo del modo **vivo + replay**. Devuelve `{ displayData, setDisplayData, events,
isLoadingHistory, historyRef }`.

- **Eventos:** carga `GET /match/{id}/events/history` una vez (guard `eventsLoaded`).
- **Vivo:** cuando `isLive`, refleja `liveData` (del WebSocket) y lo cachea en el buffer.
- **Replay:** mantiene un **buffer de frames en `ref`** (no re-renderiza al rellenar).
  Pide chunks **alineados** (0, 500, 1000…) a `GET /match/{id}/tracking/history`
  con **prefetch** del siguiente chunk y *debounce* (300 ms) en los *seek*. Usa
  `AbortController` para cancelar fetches urgentes anteriores.
- `historyRef` (el buffer) se expone para que el game-loop de `App.jsx` (rAF) fije
  el frame directamente, evitando setStates anidados (causa del antiguo replay
  "robótico"). Helpers en `lib/replayBuffer.js`.

---

## `useMatchStats.js` (Match Stats Hub)

| Hook | Endpoint | Notas |
| :--- | :--- | :--- |
| `useMatchStats(matchId)` | `GET /match/{id}/stats` | Polling cada **5s** + refetch al volver el foco de la pestaña. Devuelve `{statsData, isLoading, error, refetch}`. |
| `usePlayerPitchData(matchId, playerId)` | `GET /match/{id}/player/{pid}/pitch` | On-demand (heatmap/red de pases/touch map). |
| `useShotMap(matchId)` | `GET /match/{id}/shots` | Shot map. |
| `useTrackingMetrics(matchId)` | `GET /match/{id}/tracking/metrics` | Polling **60s** (alineado al TTL de caché del backend). |
| `usePlayerTrackingHeatmap(matchId, playerId)` | `GET /match/{id}/player/{pid}/tracking/heatmap` | Heatmap de tracking de un jugador. |
| `usePlayerTrackingDetail(matchId, playerId)` | `GET /match/{id}/player/{pid}/tracking/metrics` | Métricas físicas detalladas (incl. `speed_per_second`). |
| `useMomentum(matchId)` | `GET /match/{id}/momentum` | Momentum minuto a minuto. |

---

## `useGhostDeviations.js`

### `useGhostDeviations(matchId, pollMs = 5000)`
Polling de `GET /match/{id}/ghost/ticker`. Devuelve un **mapa**
`{ tracking_id(string): { deviation, status, trend, overall_score } }` con la
desviación real en **σ** (sustituye el antiguo mock `Math.random`). Falla en
silencio (el ticker cae a "sin dato" si el backend no responde).

---

## `useDofa.js` (análisis pre-partido)

| Hook | Endpoint | Notas |
| :--- | :--- | :--- |
| `useDofaTeams()` | `GET /dofa/teams` | Nuestro equipo + rival. Default: Real Madrid / Atlético de Madrid. |
| `useDofaOverview(team)` | `GET /dofa/overview?team=` | Paquete DOFA completo. `{data, isLoading, error}`. BigQuery lento en frío (~10–15s), cacheado 24h en backend. |
| `useIdealXI(team, rival)` | `GET /dofa/ideal-xi?team=&rival=` | XI ideal heurístico frente al rival. |

---

## Relacionado

- `PlayerHoverCard.jsx` hace su propio fetch (con caché de módulo) a
  `GET /match/test_match/player/{id}/profile` para el radar del hover del ticker.
- El WebSocket (`/ws/match/test_match`) se gestiona directamente en `App.jsx`
  (no en un hook), batcheando `setLatestTracking` + `setMaxFrame` + `setSliderValue`.
