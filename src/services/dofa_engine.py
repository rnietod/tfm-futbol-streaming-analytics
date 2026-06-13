"""
DOFA Engine — análisis pre-partido (scouting) ponderado por recencia.

Lee `tfm-master-futbol.marts_football.fct_events_enriched` (BigQuery) y produce, para un
equipo dado, el paquete de análisis del DOFA:

  - summary            : KPIs (totales últimos N partidos + media ponderada por partido)
  - goals_by_minute    : distribución de goles por franjas de 15' (ponderada)
  - top_scorers        : top goleadores (ponderado por recencia)
  - top_assisters      : top asistidores (ponderado por recencia)
  - expected_xi        : "11 previsto" (jugadores con más presencia reciente + posición media)
  - shot_zones         : rejilla 10×10 de origen de tiros (intensidad ponderada + goles)
  - team_heatmap       : rejilla 20×10 de toques (intensidad ponderada)
  - swot               : 4 cuadrantes (Fortalezas/Debilidades/Oportunidades/Amenazas)

PONDERACIÓN POR RECENCIA (a nivel de partido, ordenado por match_date DESC):
  - últimos 5 partidos  → 50 % del peso
  - siguientes 10       → 30 %
  - resto hasta 90      → 20 %
  Se normaliza por la suma de pesos disponibles (equipos con < 90 partidos suman 1).

ESTRATEGIA DE COSTE (ver memoria dofa-bigquery-notes):
  Filtrar por `team_name` escanea ~5 GB y la poda por `match_id` NO reduce el escaneo. Por eso
  materializamos UNA sola vez los eventos del equipo en una TEMP TABLE (sesión de BigQuery) y
  ejecutamos todos los análisis contra esa tabla pequeña (~MB). Un escaneo grande + N baratos.
  El resultado se cachea en la capa API. (Optimización futura: mart de Dataform pre-agregado.)

NOTAS:
  - Recencia por `match_date` (string 'YYYY-MM-DD'); `event_timestamp` tiene valores corruptos.
  - `formation_id` viene vacío → el XI se deriva de `player_position_desc` + presencia + posición media.
"""
import logging
import re

from google.cloud import bigquery

from src.data.bigquery_client import BigQueryClient

logger = logging.getLogger(__name__)

TABLE = "`tfm-master-futbol.marts_football.fct_events_enriched`"
# Tope por consulta (red de seguridad). La materialización de la TEMP TABLE escanea ~11 GB
# (lee 12 columnas del equipo en una sola pasada); el resto de consultas van contra esa tabla
# temporal y facturan ~0. Es ~2× más barato que repetir el escaneo por análisis.
MAX_BYTES = 14_000_000_000

RECENCY_BUCKETS = [(5, 0.50), (10, 0.30), (75, 0.20)]  # (nº partidos, peso del bucket)
SHOT_TYPES = (13, 14, 15, 16)  # Opta: fuera, palo, parado, gol
MINUTE_LABELS = ["0-15", "16-30", "31-45", "46-60", "61-75", "76-90+"]


def _recency_weights(n):
    """Lista de pesos por partido (índice 0 = más reciente), normalizada a suma 1."""
    raw = []
    for i in range(n):
        cursor, weight = 0, 0.0
        for count, bucket_w in RECENCY_BUCKETS:
            if i < cursor + count:
                weight = bucket_w / count
                break
            cursor += count
        raw.append(weight)
    total = sum(raw) or 1.0
    return [r / total for r in raw]


def _safe_id(mid):
    """Sanitiza un match_id para incrustarlo como literal SQL (alfanumérico)."""
    return re.sub(r"[^a-zA-Z0-9]", "", str(mid))


class DofaEngine:
    def __init__(self):
        self._bq = None
        self._bytes_billed = 0
        self._session_id = None

    @property
    def bq(self):
        if self._bq is None:
            self._bq = BigQueryClient()
        return self._bq

    # ── Infra de consultas ───────────────────────────────────────────────────
    def _run(self, sql, params=None):
        """Consulta independiente (sin sesión). Usada para el listado de partidos."""
        rows, billed = self.bq.query(sql, params=params, max_bytes=MAX_BYTES)
        self._bytes_billed += billed
        logger.info(f"[DofaEngine] query · {billed/1e9:.2f} GB")
        return rows

    def _qs(self, sql):
        """Consulta dentro de la sesión activa (contra la TEMP TABLE `ev`, barata)."""
        cfg = bigquery.QueryJobConfig(
            connection_properties=[bigquery.ConnectionProperty("session_id", self._session_id)],
            create_session=False,
        )
        job = self.bq._client.query(sql, job_config=cfg)
        rows = [dict(r) for r in job.result()]
        self._bytes_billed += job.total_bytes_billed or 0
        return rows

    def list_team_names(self):
        """Nombres de equipo distintos en el warehouse (para resolver alias Postgres↔BQ)."""
        rows = self._run(
            f"SELECT DISTINCT team_name FROM {TABLE} WHERE team_name IS NOT NULL"
        )
        return [r["team_name"] for r in rows]

    def _recent_matches(self, team_name, n_matches=90):
        sql = f"""
            SELECT match_id, ANY_VALUE(match_date) AS match_date
            FROM {TABLE}
            WHERE team_name = @team AND match_date IS NOT NULL
            GROUP BY match_id
            ORDER BY match_date DESC
            LIMIT {int(n_matches)}
        """
        params = [bigquery.ScalarQueryParameter("team", "STRING", team_name)]
        rows = self._run(sql, params)
        for r, w in zip(rows, _recency_weights(len(rows))):
            r["weight"] = w
        return rows

    def _create_ev(self, team_name, matches):
        """Crea la sesión + TEMP TABLE `ev` con los eventos del equipo (un solo escaneo grande)."""
        structs = ",\n            ".join(
            f"STRUCT('{_safe_id(m['match_id'])}' AS match_id, {m['weight']:.8f} AS mw)"
            for m in matches
        )
        id_list = ",".join(f"'{_safe_id(m['match_id'])}'" for m in matches)
        create_sql = f"""
        CREATE TEMP TABLE ev AS
        WITH w AS (
          SELECT match_id, mw FROM UNNEST([
            {structs}
          ])
        )
        SELECT
          e.match_id, e.player_id, e.player_name, e.type_id, e.minute, e.outcome,
          -- (0,0) es el "sin posición" de los eventos administrativos (tarjetas, cambios…):
          -- se anula para no crear un punto caliente artificial idéntico en todos los equipos
          -- ni sesgar las posiciones medias.
          IF(e.location_x = 0 AND e.location_y = 0, NULL, e.location_x) AS location_x,
          IF(e.location_x = 0 AND e.location_y = 0, NULL, e.location_y) AS location_y,
          e.is_assist, e.is_corner_taken, e.player_position_desc, w.mw
        FROM {TABLE} e
        JOIN w USING (match_id)
        WHERE e.team_name = @team AND e.match_id IN ({id_list})
        """
        cfg = bigquery.QueryJobConfig(
            create_session=True,
            query_parameters=[bigquery.ScalarQueryParameter("team", "STRING", team_name)],
            maximum_bytes_billed=MAX_BYTES,
        )
        job = self.bq._client.query(create_sql, job_config=cfg)
        job.result()
        self._session_id = job.session_info.session_id
        billed = job.total_bytes_billed or 0
        self._bytes_billed += billed
        logger.info(f"[DofaEngine] TEMP ev creada · {billed/1e9:.2f} GB · session {self._session_id}")

    # ── Análisis (contra la TEMP TABLE `ev`) ─────────────────────────────────
    def _summary(self):
        r = self._qs("""
            SELECT
              COUNT(DISTINCT match_id) AS matches,
              COUNTIF(type_id = 16) AS goals,
              SUM(CASE WHEN type_id = 16 THEN mw ELSE 0 END) AS wpm_goals,
              COUNTIF(type_id IN (13,14,15,16)) AS shots,
              SUM(CASE WHEN type_id IN (13,14,15,16) THEN mw ELSE 0 END) AS wpm_shots,
              COUNTIF(type_id IN (15,16)) AS shots_on_target,
              COUNTIF(is_assist) AS assists,
              COUNTIF(is_corner_taken) AS corners,
              SUM(CASE WHEN is_corner_taken THEN mw ELSE 0 END) AS wpm_corners,
              COUNTIF(type_id = 1) AS passes,
              COUNTIF(type_id = 1 AND outcome = 1) AS passes_ok
            FROM ev
        """)[0]
        passes = r["passes"] or 0
        shots = r["shots"] or 0
        return {
            "matches": r["matches"],
            "goals": r["goals"],
            "goalsPerMatch": round(r["wpm_goals"] or 0, 2),
            "shots": shots,
            "shotsPerMatch": round(r["wpm_shots"] or 0, 1),
            "shotsOnTarget": r["shots_on_target"],
            "shotConversion": round((r["goals"] or 0) / shots * 100, 1) if shots else 0,
            "assists": r["assists"],
            "corners": r["corners"],
            "cornersPerMatch": round(r["wpm_corners"] or 0, 1),
            "passAccuracy": round((r["passes_ok"] or 0) / passes * 100, 1) if passes else 0,
        }

    def _goals_by_minute(self):
        rows = self._qs("""
            SELECT bucket, SUM(mw) AS w_goals, COUNT(*) AS goals
            FROM (SELECT LEAST(DIV(minute, 15), 5) AS bucket, mw FROM ev WHERE type_id = 16)
            GROUP BY bucket ORDER BY bucket
        """)
        by_bucket = {int(r["bucket"]): r for r in rows}
        total_w = sum((r["w_goals"] or 0) for r in by_bucket.values()) or 1.0
        out = []
        for i, label in enumerate(MINUTE_LABELS):
            r = by_bucket.get(i)
            out.append({
                "range": label,
                "goals": (r["goals"] if r else 0) or 0,
                "weightedShare": round(((r["w_goals"] if r else 0) or 0) / total_w * 100, 1),
            })
        return out

    def _players(self):
        return self._qs("""
            SELECT
              player_id,
              ANY_VALUE(player_name) AS name,
              -- La mayoría de eventos no llevan posición (modal = NULL); pedimos varios
              -- candidatos y en Python tomamos el primero no nulo.
              APPROX_TOP_COUNT(player_position_desc, 4) AS pos_candidates,
              COUNT(DISTINCT match_id) AS apps,
              SUM(mw) AS w_presence,
              COUNTIF(type_id = 16) AS goals,
              SUM(CASE WHEN type_id = 16 THEN mw ELSE 0 END) AS w_goals,
              COUNTIF(is_assist) AS assists,
              SUM(CASE WHEN is_assist THEN mw ELSE 0 END) AS w_assists,
              COUNTIF(type_id IN (13,14,15,16)) AS shots,
              SAFE_DIVIDE(SUM(location_x * mw), SUM(IF(location_x IS NULL, 0, mw))) AS avg_x,
              SAFE_DIVIDE(SUM(location_y * mw), SUM(IF(location_y IS NULL, 0, mw))) AS avg_y
            FROM ev
            WHERE player_id IS NOT NULL
            GROUP BY player_id
        """)

    def _shot_zones(self):
        rows = self._qs("""
            SELECT
              GREATEST(LEAST(CAST(FLOOR(location_x / 10) AS INT64), 9), 0) AS gx,
              GREATEST(LEAST(CAST(FLOOR(location_y / 10) AS INT64), 9), 0) AS gy,
              SUM(mw) AS w, COUNT(*) AS shots, COUNTIF(type_id = 16) AS goals
            FROM ev
            WHERE type_id IN (13,14,15,16) AND location_x IS NOT NULL AND location_y IS NOT NULL
            GROUP BY gx, gy
        """)
        max_w = max((r["w"] or 0 for r in rows), default=0) or 1.0
        return [{
            "x": (r["gx"] + 0.5) * 10, "y": (r["gy"] + 0.5) * 10,
            "intensity": round((r["w"] or 0) / max_w, 4),
            "shots": r["shots"], "goals": r["goals"],
        } for r in rows]

    def _team_heatmap(self, gx=20, gy=10):
        rows = self._qs(f"""
            SELECT
              GREATEST(LEAST(CAST(FLOOR(location_x / {100/gx}) AS INT64), {gx-1}), 0) AS cx,
              GREATEST(LEAST(CAST(FLOOR(location_y / {100/gy}) AS INT64), {gy-1}), 0) AS cy,
              SUM(mw) AS w
            FROM ev
            WHERE location_x IS NOT NULL AND location_y IS NOT NULL
            GROUP BY cx, cy
        """)
        max_w = max((r["w"] or 0 for r in rows), default=0) or 1.0
        cells = [{
            "x": round((r["cx"] + 0.5) / gx * 100, 2),
            "y": round((r["cy"] + 0.5) / gy * 100, 2),
            "v": round((r["w"] or 0) / max_w, 4),
        } for r in rows]
        return {"grid": {"x": gx, "y": gy}, "cells": cells}

    # ── Derivados en Python ───────────────────────────────────────────────────
    @staticmethod
    def _resolve_position(candidates):
        """Primer valor no nulo de APPROX_TOP_COUNT(player_position_desc) (lista de {value,count})."""
        for c in (candidates or []):
            val = c.get("value") if isinstance(c, dict) else getattr(c, "value", None)
            if val:
                return val
        return None

    @staticmethod
    def _bucket_position(desc):
        d = (desc or "").lower()
        if "keeper" in d:
            return "GK"
        if "back" in d or "defend" in d:
            return "DEF"
        if "midfield" in d:
            return "MID"
        if any(k in d for k in ("forward", "striker", "wing", "attack")):
            return "FW"
        return "MID"

    def _gk_ids(self):
        """Set de player_id de porteros según fct_goalkeeper_season_profile (fuente de
        verdad del warehouse). Cubre a porteros sin player_position_desc en los eventos
        recientes (p.ej. Oblak). Tabla pequeña: el lookup factura ~0.01 GB."""
        if not hasattr(self, "_gk_ids_cache"):
            rows = self._run(
                "SELECT DISTINCT player_id "
                "FROM `tfm-master-futbol.marts_football.fct_goalkeeper_season_profile` "
                "WHERE player_id IS NOT NULL"
            )
            self._gk_ids_cache = {r["player_id"] for r in rows}
        return self._gk_ids_cache

    def _player_bucket(self, p):
        """Bucket de posición; el GK se identifica por el mart de porteros o por la
        descripción de posición (lo que llegue primero)."""
        if p.get("player_id") in self._gk_ids():
            if not p.get("position"):
                p["position"] = "Goalkeeper"
            return "GK"
        return self._bucket_position(p.get("position"))

    def _expected_xi(self, players):
        # El portero genera muchos menos eventos que un jugador de campo, así que si
        # ordenáramos solo por presencia podría quedar fuera: reservamos 1 plaza GK
        # y completamos con los 10 de campo más presentes.
        pool = [p for p in players if p["avg_x"] is not None]
        for p in pool:
            p["_bucket"] = self._player_bucket(p)
        by_presence = lambda p: p["w_presence"] or 0  # noqa: E731
        gks = sorted([p for p in pool if p["_bucket"] == "GK"], key=by_presence, reverse=True)
        outfield = sorted([p for p in pool if p["_bucket"] != "GK"], key=by_presence, reverse=True)
        chosen = gks[:1] + outfield[:11 - min(1, len(gks))]
        return [{
            "playerId": p["player_id"], "name": p["name"], "position": p["position"],
            "bucket": p["_bucket"], "apps": p["apps"],
            "goals": p["goals"], "assists": p["assists"],
            "x": round(p["avg_x"], 1), "y": round(p["avg_y"], 1),
        } for p in chosen]

    @staticmethod
    def _top(players, key, n=3):
        out = []
        for p in sorted(players, key=lambda p: p[key] or 0, reverse=True)[:n]:
            if (p[key] or 0) <= 0:
                break
            out.append({
                "playerId": p["player_id"], "name": p["name"], "position": p["position"],
                "goals": p["goals"], "assists": p["assists"],
                "shots": p["shots"], "apps": p["apps"],
            })
        return out

    @staticmethod
    def _build_swot(summary, goals_by_minute, top_scorers):
        """SWOT ligero basado en reglas sobre datos ofensivos. La capa defensiva (goles
        encajados) requiere datos del rival y queda para Fase 2 — se indica con honestidad."""
        strengths, weaknesses, opportunities, threats = [], [], [], []

        if summary["goalsPerMatch"] >= 1.5:
            strengths.append({"title": "Ataque prolífico",
                              "detail": f"{summary['goalsPerMatch']} goles/partido (ponderado)."})
        if summary["shotConversion"] >= 12:
            strengths.append({"title": "Eficacia de tiro alta",
                              "detail": f"{summary['shotConversion']}% de conversión."})
        if top_scorers:
            ts = top_scorers[0]
            strengths.append({"title": "Referente ofensivo",
                              "detail": f"{ts['name']} ({ts['goals']} goles) concentra la amenaza."})

        if 0 < summary["shotConversion"] < 9:
            weaknesses.append({"title": "Baja conversión",
                               "detail": f"Solo {summary['shotConversion']}% de los tiros acaban en gol."})
        if 0 < summary["passAccuracy"] < 80:
            weaknesses.append({"title": "Imprecisión en pase",
                               "detail": f"Precisión de pase del {summary['passAccuracy']}%."})

        if goals_by_minute:
            peak = max(goals_by_minute, key=lambda b: b["weightedShare"])
            low = min(goals_by_minute, key=lambda b: b["weightedShare"])
            threats.append({"title": f"Pico de goles en el {peak['range']}'",
                            "detail": f"{peak['weightedShare']}% de sus goles llegan en este tramo."})
            opportunities.append({"title": f"Flojo en el tramo {low['range']}'",
                                  "detail": f"Apenas {low['weightedShare']}% de sus goles; momento para apretar."})

        if summary["cornersPerMatch"] >= 5:
            threats.append({"title": "Peligro a balón parado",
                            "detail": f"{summary['cornersPerMatch']} córners/partido."})

        return {
            "fortalezas": strengths, "debilidades": weaknesses,
            "oportunidades": opportunities, "amenazas": threats,
        }

    # ── Orquestación ──────────────────────────────────────────────────────────
    def compute_team_dofa(self, team_name, n_matches=90):
        self._bytes_billed = 0
        matches = self._recent_matches(team_name, n_matches)
        if not matches:
            return {"error": f"Sin partidos para '{team_name}' en BigQuery"}

        self._create_ev(team_name, matches)
        summary = self._summary()
        goals_by_minute = self._goals_by_minute()
        players = self._players()
        for p in players:
            p["position"] = self._resolve_position(p.get("pos_candidates"))
        shot_zones = self._shot_zones()
        heatmap = self._team_heatmap()

        top_scorers = self._top(players, "w_goals")
        top_assisters = self._top(players, "w_assists")
        swot = self._build_swot(summary, goals_by_minute, top_scorers)

        logger.info(f"[DofaEngine] {team_name}: total facturado {self._bytes_billed/1e9:.2f} GB")
        return {
            "team": team_name,
            "windowMatches": len(matches),
            "dateRange": {"from": matches[-1]["match_date"], "to": matches[0]["match_date"]},
            "bytesBilled": self._bytes_billed,
            "summary": summary,
            "goalsByMinute": goals_by_minute,
            "topScorers": top_scorers,
            "topAssisters": top_assisters,
            "expectedXI": self._expected_xi(players),
            "shotZones": shot_zones,
            "teamHeatmap": heatmap,
            "swot": swot,
        }

    def compute_ideal_xi(self, team_name="Real Madrid", rival=None, n_matches=90):
        """11 ideal (heurística): mejor jugador por bucket de posición según rating ponderado
        de contribución ofensiva/presencia. El matiz 'contra este rival' queda para Fase 2."""
        self._bytes_billed = 0
        matches = self._recent_matches(team_name, n_matches)
        if not matches:
            return {"error": f"Sin partidos para '{team_name}' en BigQuery"}

        self._create_ev(team_name, matches)
        players = self._players()
        for p in players:
            p["position"] = self._resolve_position(p.get("pos_candidates"))
            p["_bucket"] = self._player_bucket(p)
            p["_rating"] = round(
                (p["w_goals"] or 0) * 3 + (p["w_assists"] or 0) * 2 + (p["w_presence"] or 0), 3
            )

        formation = {"GK": 1, "DEF": 4, "MID": 3, "FW": 3}
        xi = []
        for bucket, count in formation.items():
            pool = sorted(
                [p for p in players if p["_bucket"] == bucket and p["avg_x"] is not None],
                key=lambda p: p["_rating"], reverse=True,
            )
            for p in pool[:count]:
                xi.append({
                    "playerId": p["player_id"], "name": p["name"], "position": p["position"],
                    "bucket": bucket, "rating": p["_rating"], "goals": p["goals"],
                    "assists": p["assists"], "x": round(p["avg_x"], 1), "y": round(p["avg_y"], 1),
                })
        return {"team": team_name, "rival": rival, "formation": "1-4-3-3", "xi": xi}
