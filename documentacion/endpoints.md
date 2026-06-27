# API — Endpoints (REST + WebSocket)

Backend **FastAPI** en `src/api/main.py`. Base local: `http://127.0.0.1:8000`.
CORS configurable por `CORS_ORIGINS` (por defecto `127.0.0.1:5173` y `localhost:5173`).

> Recordatorio de modelo de datos: `match_events` usa **opta_player_id**; el resto
> (frontend, `match_players`, tracking) usa **tracking_player_id**. Los endpoints
> traducen con `dim_player_mapping.csv` antes de cruzar datos. Coordenadas de
> eventos normalizadas a 0–100 vía `_sb_x_to_100` / `_sb_y_to_100`.

## Generales / salud

| Método | Ruta | Descripción |
| :--- | :--- | :--- |
| `GET` | `/` | Health check. Devuelve `{"status": "TACTIX API Online"}`. |
| `GET` | `/player-images/{tracking_id}.png` | Proxy de la foto del jugador desde el bucket **privado** de GCS (`tfm-datalake-raw-futbol`), con caché en memoria. La clave es el `tracking_player_id`. |

## Administración

| Método | Ruta | Descripción |
| :--- | :--- | :--- |
| `DELETE` | `/admin/reset-all` | `TRUNCATE` de `matches`, `match_events`, `match_tracking`, `match_players`. |
| `GET` | `/admin/services` | Estado de los microservicios gestionados (`streamlit_dashboard`, `worker_persist`). |
| `POST` | `/admin/services/{service_id}/start` | Arranca el proceso del servicio. |
| `POST` | `/admin/services/{service_id}/stop` | Detiene el proceso del servicio. |

## Partido — metadata y vivo

| Método | Ruta | Params | Descripción |
| :--- | :--- | :--- | :--- |
| `GET` | `/match/{match_id}/metadata` | — | Alineación (jugadores: id, dorsal, nombre, posición, team_id) + mapa `teams` (resuelto por `matches.home_team_id`/`away_team_id`) + `homeName`/`awayName`. |
| `WS` | `/ws/match/{match_id}` | — | Stream en vivo. Mensajes `{type:"tracking", payload:{frame, players_data...}}` y `{type:"event", payload:{...}}` publicados vía Redis pubsub. |
| `GET` | `/match/{match_id}/tracking/history` | `start_frame`, `end_frame` | Chunk de frames de tracking para el replay (el frontend pide chunks alineados y hace prefetch). |
| `GET` | `/match/{match_id}/frame_at` | `period`, `seconds` | Devuelve `{frame_idx}` correspondiente a un instante del reloj de juego (para saltar el replay a una jugada). |
| `GET` | `/match/{match_id}/events/history` | — | Lista completa de eventos del partido (feed + contexto). |

## Estadísticas

| Método | Ruta | Descripción |
| :--- | :--- | :--- |
| `GET` | `/match/{match_id}/stats` | Hub completo: por equipo (posesión, pases, xG, big chances, red de pases, posiciones medias…) y por jugador (goles, tiros, xG, pases, duelos, touches…). |
| `GET` | `/match/{match_id}/shots` | Shot map: tiros con coordenadas, `xg`, resultado y jugador (traduce opta→tracking). |
| `GET` | `/match/{match_id}/momentum` | Momentum minuto a minuto del partido. |
| `GET` | `/match/{match_id}/player/{player_id}/pitch` | Datos de campo de un jugador: heatmap, red de pases y touch map (coords 0–100). |

## Tracking físico

| Método | Ruta | Params | Descripción |
| :--- | :--- | :--- | :--- |
| `GET` | `/match/{match_id}/tracking/metrics` | — | Métricas físicas (distancias/velocidades) de todos los jugadores. Cacheado en Redis (TTL ~60s). |
| `GET` | `/match/{match_id}/player/{player_id}/tracking/metrics` | — | Métricas físicas detalladas de un jugador (incluye `speed_per_second`). |
| `GET` | `/match/{match_id}/player/{player_id}/tracking/heatmap` | — | Heatmap posicional basado en **tracking** (celdas 0–100, intensidad 0–1). |

## Ghost (desviación de rendimiento)

| Método | Ruta | Params | Descripción |
| :--- | :--- | :--- | :--- |
| `GET` | `/match/{match_id}/ghost/ticker` | `up_to_minute?` | Por jugador: desviación real en **σ** (z-score), estado, tendencia y `overall_score`. Alimenta el Ghost Ticker. |
| `GET` | `/match/{match_id}/player/{player_id}/profile` | `state=Overall` | Perfil del jugador para el radar **esperado vs real** (métricas normalizadas + `deviation_sigma`). |

## DOFA (pre-partido, BigQuery)

| Método | Ruta | Params | Descripción |
| :--- | :--- | :--- | :--- |
| `GET` | `/dofa/teams` | `match_id=test_match` | Nuestro equipo (Real Madrid) y el rival (oponente del partido). |
| `GET` | `/dofa/overview` | `team`, `n_matches=90`, `force_recalc=false` | Paquete DOFA completo ponderado por recencia (SWOT, goles/min, tops, XI, tiros, heatmap). Cacheado 24h (Redis + tabla Postgres). |
| `GET` | `/dofa/ideal-xi` | `team`, `rival?`, `n_matches=90`, `force_recalc=false` | XI ideal heurístico frente al rival. |

> Las consultas DOFA escanean ~5–6 GB en BigQuery; por eso hay caché en dos capas
> (`_dofa_cached`) para consultar BigQuery **una sola vez** por dato.
