"""
Construye la línea base "Ghost" (media + desviación estándar por jugador) y la
materializa en Postgres -> tabla `player_ghost_baseline`.

Por qué
-------
El motor Ghost mide el rendimiento en vivo del jugador contra SU PROPIA norma
histórica, en unidades de desviación estándar:

    z = (valor_real_p30 - media_p30) / max(std_p30, σ_floor)

`mart_ghost_profile` (BigQuery) ya da las MEDIAS recencia-ponderadas, pero NO la
desviación estándar. La σ requiere granularidad por partido, que solo existe en
los eventos (`fct_events_enriched`). Como la app tiene un único partido (~50
jugadores), en vez de una mart league-wide (cara) lanzamos UNA consulta
filtrada a los master_player_id de la alineación (~8.5 GB ≈ $0.04) que calcula,
por (jugador, game_state):

    n_matches, {metric}_p30_mean, {metric}_p30_std

para xg, shots, key_passes, progressive, recoveries, interceptions, goals.

La lógica de xG (ML.PREDICT sobre model_xg) y de pase vertical se replica de
`definitions/models/marts/fct_player_season_profile.sqlx` para mantener
coherencia con las medias del mart.

Uso
---
  python scripts/build_ghost_baseline.py            # ejecuta y materializa
  python scripts/build_ghost_baseline.py --dry-run  # solo estima bytes
  python scripts/build_ghost_baseline.py --player "Mbappé"  # QA de un jugador
"""

import argparse
import csv
import os
import sys

from google.cloud import bigquery
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.postgres_client import get_db_engine  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPING_CSV = os.path.join(BASE_DIR, "data", "dim_player_mapping.csv")

PROJECT = "tfm-master-futbol"
MIN_STATE_MINUTES = 20      # segmentos (jugador,partido,estado) más cortos se descartan (p30 volátil)
MAX_BYTES = 20_000_000_000  # ~20 GB de tope de coste (la consulta escanea ~14 GB)

# Métricas del baseline (clave live <-> conteo de eventos)
METRICS = ["xg", "shots", "key_passes", "progressive", "recoveries", "interceptions", "goals"]

# Consulta: replica el feature-engineering de xG y verticalidad del season profile,
# pero a grano de PARTIDO y filtrada a los jugadores de la alineación.
QUERY = """
WITH
  base AS (
    SELECT *
    FROM `tfm-master-futbol.marts_football.fct_events_enriched`
    WHERE player_id IN UNNEST(@masters)
      AND period_id IN (1, 2)
  ),

  shots_with_xg AS (
    SELECT event_key, prob.prob AS predicted_xg
    FROM ML.PREDICT(MODEL `tfm-master-futbol.ml_football.model_xg`, (
      SELECT
        event_key,
        IF(type_id = 16, 1, 0) AS is_goal,
        ROUND(SQRT(POW(100 - location_x, 2) + POW(50 - location_y, 2)), 2) AS distance_to_goal,
        ROUND(ACOS(SAFE_DIVIDE(((100 - location_x) * (100 - location_x) + (46 - location_y) * (54 - location_y)),
              (SQRT(POW(100 - location_x, 2) + POW(46 - location_y, 2)) * SQRT(POW(100 - location_x, 2) + POW(54 - location_y, 2))))) * 180 / ACOS(-1), 2) AS angle_visible,
        CASE WHEN body_head THEN 'Head' WHEN body_right_foot OR body_left_foot THEN 'Foot' ELSE 'Other' END AS body_part,
        CASE WHEN is_penalty THEN 'Penalty' WHEN is_from_corner THEN 'From Corner' WHEN is_fast_break THEN 'Fast Break' ELSE 'Regular Play' END AS play_pattern
      FROM base
      WHERE type_id IN (13, 14, 15, 16)
    )), UNNEST(predicted_is_goal_probs) prob
    WHERE prob.label = 1
  ),

  pass_details AS (
    SELECT
      event_key,
      (pass_end_x - location_x) > 10 AND ABS(pass_end_y - location_y) < (pass_end_x - location_x) AS is_vertical
    FROM base
    WHERE type_id = 1
  ),

  -- Agregado por (jugador, partido, estado de juego)
  per_match AS (
    SELECT
      e.player_id,
      ANY_VALUE(e.player_name) AS player_name,
      e.match_id,
      COALESCE(e.game_state_label, 'Drawing') AS game_state,
      (MAX(e.minute) - MIN(e.minute)) + 1 AS minutes_state,
      COUNTIF(e.type_id = 16) AS goals,
      COUNTIF(e.type_id IN (13, 14, 15, 16)) AS shots,
      COUNTIF(COALESCE(e.is_key_pass, FALSE)) AS key_passes,
      COUNTIF(COALESCE(pd.is_vertical, FALSE)) AS progressive,
      COUNTIF(e.type_id = 49) AS recoveries,
      COUNTIF(e.type_id = 8) AS interceptions,
      SUM(COALESCE(x.predicted_xg, 0)) AS xg
    FROM base e
    LEFT JOIN shots_with_xg x USING (event_key)
    LEFT JOIN pass_details pd USING (event_key)
    GROUP BY e.player_id, e.match_id, game_state
  ),

  -- p30 por partido (descartando segmentos demasiado cortos)
  p30 AS (
    SELECT
      player_id, player_name, match_id, game_state,
      goals      * 30.0 / minutes_state AS goals_p30,
      shots      * 30.0 / minutes_state AS shots_p30,
      key_passes * 30.0 / minutes_state AS key_passes_p30,
      progressive* 30.0 / minutes_state AS progressive_p30,
      recoveries * 30.0 / minutes_state AS recoveries_p30,
      interceptions * 30.0 / minutes_state AS interceptions_p30,
      xg         * 30.0 / minutes_state AS xg_p30
    FROM per_match
    WHERE minutes_state >= @min_minutes
  )

SELECT
  player_id,
  ANY_VALUE(player_name) AS player_name,
  COALESCE(game_state, 'Overall') AS game_state,
  COUNT(DISTINCT match_id) AS n_matches,
  AVG(xg_p30) AS xg_mean,                 STDDEV_SAMP(xg_p30) AS xg_std,
  AVG(shots_p30) AS shots_mean,           STDDEV_SAMP(shots_p30) AS shots_std,
  AVG(key_passes_p30) AS key_passes_mean, STDDEV_SAMP(key_passes_p30) AS key_passes_std,
  AVG(progressive_p30) AS progressive_mean, STDDEV_SAMP(progressive_p30) AS progressive_std,
  AVG(recoveries_p30) AS recoveries_mean, STDDEV_SAMP(recoveries_p30) AS recoveries_std,
  AVG(interceptions_p30) AS interceptions_mean, STDDEV_SAMP(interceptions_p30) AS interceptions_std,
  AVG(goals_p30) AS goals_mean,           STDDEV_SAMP(goals_p30) AS goals_std
FROM p30
GROUP BY GROUPING SETS ((player_id, game_state), (player_id))
"""

DDL = """
CREATE TABLE IF NOT EXISTS player_ghost_baseline (
    tracking_player_id INTEGER,
    master_player_id   VARCHAR(50),
    player_name        VARCHAR(120),
    game_state         VARCHAR(20),
    n_matches          INTEGER,
    xg_p30_mean FLOAT, xg_p30_std FLOAT,
    shots_p30_mean FLOAT, shots_p30_std FLOAT,
    key_passes_p30_mean FLOAT, key_passes_p30_std FLOAT,
    progressive_p30_mean FLOAT, progressive_p30_std FLOAT,
    recoveries_p30_mean FLOAT, recoveries_p30_std FLOAT,
    interceptions_p30_mean FLOAT, interceptions_p30_std FLOAT,
    goals_p30_mean FLOAT, goals_p30_std FLOAT,
    PRIMARY KEY (tracking_player_id, game_state)
);
"""


def load_mapping():
    """master_player_id -> {tracking, opta, name}."""
    out = {}
    with open(MAPPING_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            m = r.get("master_player_id")
            if m:
                out[m] = {
                    "tracking": (r.get("tracking_player_id") or "").split(".")[0],
                    "name": r.get("player_name_normalized") or r.get("player_name_opta"),
                }
    return out


def run_query(client, masters, dry_run=False):
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("masters", "STRING", masters),
            bigquery.ScalarQueryParameter("min_minutes", "INT64", MIN_STATE_MINUTES),
        ],
        maximum_bytes_billed=MAX_BYTES,
        dry_run=dry_run,
        use_query_cache=not dry_run,
    )
    job = client.query(QUERY, job_config=cfg)
    if dry_run:
        return None, job.total_bytes_processed
    rows = [dict(r) for r in job.result()]
    return rows, job.total_bytes_billed


def persist(rows, mapping):
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text(DDL))
        conn.execute(text("DELETE FROM player_ghost_baseline"))
        inserted = 0
        for r in rows:
            info = mapping.get(r["player_id"])
            if not info or not info["tracking"]:
                continue
            try:
                tid = int(info["tracking"])
            except ValueError:
                continue
            params = {
                "tid": tid, "master": r["player_id"],
                "name": info["name"], "gs": r["game_state"] or "Overall", "n": r["n_matches"],
            }
            for m in METRICS:
                params[f"{m}_mean"] = r.get(f"{m}_mean")
                params[f"{m}_std"] = r.get(f"{m}_std")
            cols = ", ".join(f"{m}_p30_mean, {m}_p30_std" for m in METRICS)
            vals = ", ".join(f":{m}_mean, :{m}_std" for m in METRICS)
            updates = ", ".join(
                f"{m}_p30_mean = EXCLUDED.{m}_p30_mean, {m}_p30_std = EXCLUDED.{m}_p30_std"
                for m in METRICS
            )
            conn.execute(text(f"""
                INSERT INTO player_ghost_baseline
                    (tracking_player_id, master_player_id, player_name, game_state, n_matches, {cols})
                VALUES (:tid, :master, :name, :gs, :n, {vals})
                ON CONFLICT (tracking_player_id, game_state) DO UPDATE SET
                    master_player_id = EXCLUDED.master_player_id,
                    player_name = EXCLUDED.player_name,
                    n_matches = EXCLUDED.n_matches,
                    {updates}
            """), params)
            inserted += 1
    return inserted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--player", help="Filtra el reporte QA por nombre (substring)")
    args = ap.parse_args()

    mapping = load_mapping()
    masters = list(mapping.keys())
    print(f"Jugadores (master_id) en la alineación: {len(masters)}")

    client = bigquery.Client(project=PROJECT)

    if args.dry_run:
        _, b = run_query(client, masters, dry_run=True)
        print(f"Dry-run: {b / 1e9:.2f} GB (~${b / 1e12 * 5:.3f})")
        return 0

    rows, billed = run_query(client, masters)
    print(f"Filas BQ: {len(rows)} | facturado: {billed / 1e9:.2f} GB (~${billed / 1e12 * 5:.3f})")

    n = persist(rows, mapping)
    print(f"✅ player_ghost_baseline: {n} filas en Postgres")

    # Reporte QA
    qa = rows
    if args.player:
        qa = [r for r in rows if args.player.lower() in (r.get("player_name") or "").lower()]
    print("\n--- QA (game_state=Overall) ---")
    print(f"{'player':<22}{'n':>3} | {'xg':>14} {'shots':>14} {'key_p':>14} {'recov':>14}")
    for r in sorted(qa, key=lambda x: x.get("player_name") or ""):
        if (r["game_state"] or "Overall") != "Overall":
            continue
        def fmt(m):
            mean, std = r.get(f"{m}_mean"), r.get(f"{m}_std")
            return f"{(mean or 0):.2f}±{(std or 0):.2f}"
        print(f"{(r.get('player_name') or '')[:21]:<22}{r['n_matches']:>3} | "
              f"{fmt('xg'):>14} {fmt('shots'):>14} {fmt('key_passes'):>14} {fmt('recoveries'):>14}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
