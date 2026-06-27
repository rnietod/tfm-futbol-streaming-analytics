# TactixData (TACTIX) ⚽ — Análisis táctico y estadístico de fútbol en vivo

Plataforma web **full-stack** de análisis táctico y estadístico de fútbol
profesional, orientada a visualización de datos en tiempo (casi) real. Es el
**Trabajo de Fin de Máster (TFM)** en la Universidad Europea de Madrid (EURM).

Combina dos flujos:

- **Pre-partido (batch):** análisis DOFA, perfiles de jugador y modelo de xG
  sobre un *warehouse* analítico en **BigQuery**.
- **En vivo (streaming):** simulación de un partido reproducido frame a frame
  (tracking 10 Hz + eventing) servida por **WebSocket**, con replay, ticker
  "Ghost" de desviación de rendimiento (σ), y estadísticas en directo.

> Hoy existe un único partido cargado: `test_match` (Real Madrid vs Atlético de Madrid).

---

## ✨ Funcionalidades (pestañas)

| Pestaña | Qué muestra |
| :--- | :--- |
| **Dashboard** | Campo en vivo con los 22 jugadores (tracking), marcador, feed de eventos clickable (salta el replay a la jugada), controles de reproducción (LIVE / replay con slider), capas tácticas (Voronoi / Pitch Control) y **Ghost Ticker** (desviación real en σ por jugador con foto y radar esperado vs real). |
| **Match Stats** | Hub de estadísticas por equipo y jugador: posesión, pases, xG, tiros, duelos, red de pases, posiciones medias, heatmaps de tracking, métricas físicas y momentum. |
| **DOFA** | Análisis **pre-partido** (BigQuery): matriz DAFO/SWOT, XI previsto sobre el campo, goles por minuto, top goleadores/asistentes, zonas de tiro y heatmap del equipo, con selector de equipo. |
| **Gemini Vision** | *Placeholder* (próximamente). |

---

## 🏗️ Arquitectura

```
                 ┌─────────────────────────────────────────────┐
                 │  Frontend (React + Vite)  :5173             │
                 │  App.jsx → tabs · hooks/ (REST + WebSocket) │
                 └───────────────┬─────────────────────────────┘
                      REST / WS  │
                 ┌───────────────▼─────────────────────────────┐
                 │  Backend FastAPI  :8000  (src/api/main.py)  │
                 │  endpoints REST + WS · services/ (motores)  │
                 └───┬───────────────┬───────────────┬─────────┘
                     │               │               │
            ┌────────▼──────┐ ┌──────▼──────┐ ┌──────▼─────────────┐
            │ PostgreSQL    │ │  Redis      │ │  BigQuery          │
            │ (datos vivos) │ │ caché/pubsub│ │ marts_football +   │
            │ match_*       │ │  + WS push  │ │ modelo xG (BQML)   │
            └───────▲───────┘ └──────▲──────┘ └────────────────────┘
                    │  persiste      │ publica
            ┌───────┴────────────────┴──────────────┐
            │  Streaming engine (src/streaming)      │
            │  + worker_persist (src/data)           │
            │  reproduce tracking+eventing → Redis   │
            └────────────────────────────────────────┘
```

- **Frontend** (`frontend/src/`): `App.jsx` gestiona las pestañas vía `activeTab`.
  Hooks de datos en `hooks/` (polling REST + WebSocket). Viz en `components/`
  (SVG a mano + recharts; D3 en el stack, poco usado aún).
- **Backend** (`src/`): `api/main.py` (REST + WS), `services/` (ghost, dofa,
  tracking_metrics), `data/` (clientes Postgres/Redis/BigQuery, modelos),
  `streaming/` (engine de ingesta + dashboard Streamlit de control).
- **Datos en vivo:** tablas `matches`, `match_players`, `match_events`,
  `match_tracking` en Postgres.

---

## 🧰 Stack

- **Frontend:** React 18, NextUI/HeroUI, Tailwind CSS, recharts, Vite.
- **Backend:** FastAPI, SQLAlchemy + pg8000, WebSocket.
- **Infra de datos:** PostgreSQL (Docker), Redis (caché + pubsub).
- **Warehouse:** BigQuery `tfm-master-futbol` (Dataform en `definitions/`), modelo xG BQML.
- **Fotos de jugador:** bucket privado GCS servido por un proxy del backend.
- **Fuentes:** StatsBomb Open Data, FBref/WhoScored, Opta/Wyscout.

### Design system
- Fondo `zinc-950`; acentos **`#006FEE`** (azul, local) y **`#f31260`** (rojo/magenta, visitante).
- Tipografía **JetBrains Mono** (datos) + **Inter** (UI). Estilo *glassmorphism*.

---

## 📁 Estructura

```
src/
  api/main.py            # FastAPI: endpoints REST + WebSocket
  services/              # ghost_engine, dofa_engine, tracking_metrics, match_setup
  data/                  # postgres_client, redis_client, bigquery_client,
                         # worker_persist, init_db, models, generate_player_mapping
  streaming/             # engine (simulación), dashboard (Streamlit)
frontend/
  src/App.jsx            # raíz + tabs
  src/components/        # viz (matchstats/, dofa/, pitch/, GhostTicker, FootballPitch…)
  src/hooks/             # useMatchHistory, useMatchStats, useGhostDeviations, useDofa
  src/lib/               # replayBuffer, eventReplay, pitchModels (+ tests)
  dist/                  # build versionado (trackeado en git)
definitions/             # modelos Dataform (BigQuery)
scripts/                 # build_ghost_baseline, qa_ghost_*
documentacion/           # docs detalladas (endpoints, hooks, tools, frontend)
configs/dev.json         # config local de conexión (Postgres)
data/                    # eventing/tracking de test_match + dim_player_mapping.csv
```

---

## 🚀 Puesta en marcha (local)

### Requisitos
- Python 3.11+, Node 18+, Docker (PostgreSQL), Redis, y `gcloud` autenticado
  (ADC) para las funciones de BigQuery/GCS.

### 1) Backend
```bash
python -m venv venvfutbol
# Windows PowerShell:  venvfutbol\Scripts\Activate.ps1
. venvfutbol/Scripts/activate
pip install -r requirements.txt        # (o requirements-lock.txt para el entorno exacto)
```

Levanta **PostgreSQL** (Docker) y **Redis**, e inicializa el esquema:
```bash
python src/data/init_db.py             # crea tablas matches/match_players/match_events/...
```

Arranca la API (Windows: **sin `--reload`** y con `PYTHONUTF8=1` para evitar
errores de encoding):
```bash
# PowerShell
$env:PYTHONUTF8=1; uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

### 2) Frontend
```bash
cd frontend
npm install
npm run dev                            # http://127.0.0.1:5173
```

### 3) Simular el partido en vivo
El motor de simulación y el worker de persistencia se controlan desde el panel
de *Microservices* del header (dropdown de estado) o ejecutando el dashboard de
Streamlit (`src/streaming/dashboard.py`) y `src/data/worker_persist.py`.

---

## 🔧 Variables de entorno

| Variable | Por defecto | Para qué |
| :--- | :--- | :--- |
| `CORS_ORIGINS` | `http://127.0.0.1:5173,http://localhost:5173` | Orígenes permitidos por la API (lista separada por comas). |
| `BQ_PROJECT_ID` / `BQ_LOCATION` | (de `.df-credentials.json`) | Proyecto/región de BigQuery. La auth es por **ADC**. |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Ruta a la key de servicio si no se usa `gcloud auth`. |
| `DB_USER` / `DB_PASS` / `DB_HOST` / `DB_NAME` / `DB_PORT` | de `configs/dev.json` (`tactix_db`, etc.) | Conexión PostgreSQL. |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `localhost` / `6379` / `0` | Conexión Redis. |
| `VITE_API_BASE` (frontend) | `http://127.0.0.1:8000` | Base de la API para el cliente. |

---

## 🧠 Notas del modelo de datos (importante)

- **IDs de jugador:** `match_events` usa **opta_player_id**; el resto
  (`match_players`, tracking, frontend) usa **tracking_player_id**. Se traduce con
  `data/dim_player_mapping.csv` (`_OPTA_TO_TRACKING` / `_TRACKING_TO_OPTA`).
- **Coordenadas:** `match_events` en StatsBomb 0–120 × 0–80 → se normaliza a
  0–100 en la API (`_sb_x_to_100` / `_sb_y_to_100`). BigQuery `fct_events_enriched`
  viene en Opta 0–100 × 0–100. Tracking en metros centrados en el origen.
- **xG:** el engine mapea `statsbomb_xg → xg` y `worker_persist` lo guarda en
  `match_events.xg` (re-ingestar el partido para poblar eventos antiguos).
- **Home/Away:** se resuelve por `matches.home_team_id` / `away_team_id`.

---

## ✅ Tests

```bash
cd frontend && npx vitest run          # tests de replayBuffer / pitchModels / eventReplay
```

---

## 📚 Documentación

Documentación detallada en [`documentacion/`](documentacion/):
- [`endpoints.md`](documentacion/endpoints.md) — API REST + WebSocket.
- [`hooks.md`](documentacion/hooks.md) — hooks de datos del frontend.
- [`tools.md`](documentacion/tools.md) — servicios/motores del backend y scripts.
- [`frontend.md`](documentacion/frontend.md) — estructura y sistema de diseño del frontend.

Guía interna de arquitectura y convenciones: [`CLAUDE.md`](CLAUDE.md).

---

## 🎓 Contexto académico

Cada tarea sobre este repo es a la vez **producto** (app de calidad de
producción) y **evidencia académica** (memoria + póster científico EURM A0).
