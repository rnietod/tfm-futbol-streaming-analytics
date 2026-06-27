# Backend — Servicios, motores y herramientas

> Interpretación de "tools": los **servicios/motores del backend** (`src/services`,
> `src/streaming`, `src/data`) y los **scripts** del proyecto que sostienen los
> endpoints y el pipeline de datos.

## `src/services/` — motores de cálculo

| Módulo | Responsabilidad | Consumido por |
| :--- | :--- | :--- |
| `ghost_engine.py` (`GhostEngine`) | Calcula la desviación de rendimiento del jugador frente a su baseline (z-score en **σ**), estado y `overall_score`. | `/match/{id}/ghost/ticker`, perfil del jugador. |
| `dofa_engine.py` (`DofaEngine`) | Construye el paquete **DOFA** (SWOT, goles/min, tops, XI, tiros, heatmap) consultando BigQuery (`fct_events_enriched`, perfiles), ponderado por recencia. Usa `BigQueryClient`. | `/dofa/*`. |
| `tracking_metrics.py` | Métricas físicas a partir de `match_tracking` (distancias, velocidades, heatmaps de tracking). | `/match/{id}/tracking/metrics`, heatmap de tracking. |
| `match_setup.py` | Prepara/siembra metadatos del partido desde BigQuery hacia Postgres (alineación, perfiles activos). | Setup inicial. |

## `src/data/` — acceso a datos

| Módulo | Responsabilidad |
| :--- | :--- |
| `postgres_client.py` | Engine **SQLAlchemy singleton** (pool reutilizado por proceso; `pool_pre_ping`, `pool_recycle`). Config desde `configs/dev.json` o env `DB_*`. Driver `pg8000`. |
| `redis_client.py` | Conexión Redis + helpers `set_match_state` / `get_match_state` (clave `match:{id}:live`). Config por env `REDIS_*`. |
| `bigquery_client.py` (`BigQueryClient`) | Cliente BigQuery (auth por **ADC**). `projectId`/`location` desde env `BQ_PROJECT_ID`/`BQ_LOCATION` con fallback a `.df-credentials.json`. Tope de bytes facturados como red de seguridad de coste. |
| `worker_persist.py` | **Worker de persistencia**: consume de Redis y escribe `matches`, `match_players`, `match_events` (incl. `xg`), `match_tracking` y los perfiles ghost. Calcula proyecciones en vivo. |
| `init_db.py` | DDL idempotente: crea todas las tablas (`matches`, `match_players`, `match_events`, `match_tracking`, perfiles…). |
| `models.py` | Modelos SQLAlchemy (`MatchActiveProfile`, agregados por minuto, etc.). |
| `generate_player_mapping.py` | Genera `dim_player_mapping.csv` (opta ↔ tracking) por *fuzzy matching* de nombres (`thefuzz` + `unidecode`) contra BigQuery. |

## `src/streaming/` — ingesta en vivo

| Módulo | Responsabilidad |
| :--- | :--- |
| `engine.py` (`SimulationEngine`) | Lee tracking (`*.jsonl`) y eventing (`eventing_file.csv`), los estructura (incl. mapeo `statsbomb_xg → xg`) y los **publica en Redis** frame a frame (tracking 10 Hz + eventos) simulando el directo. |
| `dashboard.py` | Dashboard **Streamlit** de control de la simulación (arranque/seguimiento). |

## `src/api/` — capa HTTP

| Pieza | Responsabilidad |
| :--- | :--- |
| `main.py` | FastAPI: todos los endpoints REST + WebSocket (ver [`endpoints.md`](endpoints.md)). Carga el mapping opta↔tracking al arrancar; helpers de normalización de coordenadas (`_sb_x_to_100`/`_sb_y_to_100`) y de matching por apellido (`_match_by_last_name`); proxy de fotos de GCS; caché DOFA en dos capas. |

## `scripts/` — utilidades / QA

| Script | Para qué |
| :--- | :--- |
| `build_ghost_baseline.py` | Construye el baseline del Ghost (referencia de rendimiento) desde BigQuery → Postgres. |
| `qa_ghost_sim.py` | QA de la simulación del Ghost Engine. |
| `qa_ghost_eventfile.py` | QA del fichero de eventos para el Ghost. |

## Infra externa

- **PostgreSQL** (Docker local): datos del partido en vivo.
- **Redis**: caché + pubsub (puente engine → WebSocket).
- **BigQuery** `tfm-master-futbol` (`marts_football`): warehouse analítico +
  modelo xG (BQML). Modelos en `definitions/` (Dataform).
- **GCS** `tfm-datalake-raw-futbol`: bucket privado con las fotos de jugador.
