"""
Scraper de fotos de jugadores FotMob -> Google Cloud Storage.

Objetivo
--------
Resolver el `fotmob_id` de cada jugador del partido (data/ids_tracking.json,
equipos Real Madrid=262 y Atlético=275), descargar su foto del CDN de FotMob
y subirla al bucket privado del datalake:

    gs://tfm-datalake-raw-futbol/images/players/{tracking_id}.png

El frontend NO accede al bucket directamente: lo sirve un proxy del backend
(GET /player-images/{tracking_id}.png). Aquí solo poblamos el bucket y dejamos
un CSV de mapeo (data/fotmob_player_mapping.csv) que sí se commitea.

Estrategia de resolución de IDs (híbrida y robusta)
---------------------------------------------------
El partido es de septiembre 2025, pero FotMob refleja el club ACTUAL del
jugador (tras los traspasos de enero), así que NO se puede filtrar por equipo.
El `fotmob_id` del jugador sí es estable. Por eso combinamos dos fuentes:

  1. Plantilla actual (squad page __NEXT_DATA__): da candidatos con dorsal,
     ideal para los que siguen en el club (cross-check por número de camiseta).
  2. Search suggest API (apigw.fotmob.com): resuelve por nombre completo y
     devuelve el id estable aunque el jugador haya cambiado de club.

Se elige el match de mayor confianza; los dudosos quedan como REVIEW en el CSV.

Reutiliza el patrón GCS de tools/extractor/mass_extractor.py (storage.Client +
ADC) y el fuzzy-match de src/data/generate_player_mapping.py (thefuzz/unidecode).

Uso
---
  python tools/extractor/fotmob_player_photos.py            # resuelve + sube
  python tools/extractor/fotmob_player_photos.py --dry-run  # solo resuelve/reporta
  python tools/extractor/fotmob_player_photos.py --force    # re-sube aunque exista
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import time

import requests
from thefuzz import fuzz, process
from unidecode import unidecode

try:
    from PIL import Image  # validación de PNG
except Exception:  # pragma: no cover
    Image = None

try:
    from google.cloud import storage
except Exception:  # pragma: no cover
    storage = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - FOTMOB - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TRACKING_JSON = os.path.join(BASE_DIR, "data", "ids_tracking.json")
MAPPING_CSV = os.path.join(BASE_DIR, "data", "fotmob_player_mapping.csv")

PROJECT_ID = "tfm-master-futbol"
BUCKET_NAME = "tfm-datalake-raw-futbol"
GCS_PREFIX = "images/players"

# Equipo interno (tracking) -> (fotmob_team_id, slug de la squad page)
TEAM_MAP = {
    262: (8633, "real-madrid"),
    275: (9906, "atletico-madrid"),
}

CONFIDENCE_THRESHOLD = 78  # >= se considera MATCHED

# Imagen del jugador en el CDN de FotMob
IMG_URL = "https://images.fotmob.com/image_resources/playerimages/{fid}.png"

# Apodos / nombres legales que no se parecen al nombre futbolístico.
ALIAS_DB = {
    "Jorge Resurrección Merodio": "Koke",
    "Vinícius José Paixão de Oliveira Júnior": "Vinicius Junior",
    "Robin Aime Robert Robin Le Normand": "Robin Le Normand",
    "Éder Gabriel Militão": "Eder Militao",
    "Endrick Felipe Moreira de Sousa": "Endrick",
}

# Overrides manuales tracking_id -> fotmob_id (solo para casos irresolubles
# automáticamente; se rellena tras revisar el --dry-run si hiciera falta).
OVERRIDES = {}

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    # Evita respuestas brotli que el decoder local no soporta bien.
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9",
})


def normalize(text):
    return unidecode(text.lower().strip()) if isinstance(text, str) else ""


# ----------------------------------------------------------------------------
# Fuentes FotMob
# ----------------------------------------------------------------------------
def fetch_squad(fotmob_team_id, slug):
    """Devuelve {fotmob_id: {name, num}} desde el __NEXT_DATA__ de la squad page."""
    url = f"https://www.fotmob.com/teams/{fotmob_team_id}/squad/{slug}"
    try:
        r = _SESSION.get(url, timeout=25)
        m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', r.text, re.S)
        if not m:
            logger.warning("Sin __NEXT_DATA__ en %s", url)
            return {}
        node = json.loads(m.group(1))["props"]["pageProps"]["fallback"][f"team-{fotmob_team_id}"]
    except Exception as e:
        logger.error("Error squad %s: %r", fotmob_team_id, e)
        return {}

    out = {}

    def walk(o):
        if isinstance(o, dict):
            if {"id", "name", "shirtNumber", "positionId"} <= set(o.keys()):
                out[o["id"]] = {"name": o["name"], "num": o.get("shirtNumber")}
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(node)
    return out


def suggest(term):
    """Search suggest API -> lista de (name, fotmob_id, teamId, isCoach)."""
    url = f"https://apigw.fotmob.com/searchapi/suggest?term={requests.utils.quote(term)}&lang=en"
    try:
        j = _SESSION.get(url, timeout=20).json()
    except Exception as e:
        logger.debug("suggest fallo '%s': %r", term, e)
        return []
    opts = []
    for blk in j.get("squadMemberSuggest", []):
        for o in blk.get("options", []):
            name, _, fid = o.get("text", "").rpartition("|")
            pl = o.get("payload", {})
            if fid.isdigit():
                opts.append((name, int(fid), pl.get("teamId"), bool(pl.get("isCoach"))))
    return opts


# ----------------------------------------------------------------------------
# Resolución de identidad
# ----------------------------------------------------------------------------
def resolve(player, squad, expected_fid):
    """
    player: {tracking_id, short, full, num}
    squad:  {fotmob_id: {name, num}}  (plantilla actual del club)
    -> dict con fotmob_id, fotmob_name, confidence, source, status
    """
    if player["tracking_id"] in OVERRIDES:
        fid = OVERRIDES[player["tracking_id"]]
        return {"fotmob_id": fid, "fotmob_name": "(override)", "confidence": 100,
                "source": "override", "status": "MATCHED"}

    search_full = ALIAS_DB.get(player["full"], player["full"])
    search_short = player["short"]

    # 1) Match contra la plantilla actual (con cross-check de dorsal)
    cand = {fid: v["name"] for fid, v in squad.items()}
    best_sq = (None, 0, None)  # (name, score, fid)
    if cand:
        for q in (search_full, search_short):
            m = process.extractOne(q, cand, scorer=fuzz.token_set_ratio)
            if m and m[1] > best_sq[1]:
                best_sq = m
    # Dorsal coincidente => señal fuerte
    num_fid = None
    try:
        pnum = int(player["num"]) if player["num"] is not None else None
        for fid, v in squad.items():
            try:
                if pnum is not None and int(v["num"]) == pnum:
                    num_fid = fid
                    break
            except (TypeError, ValueError):
                pass
    except (TypeError, ValueError):
        pnum = None
    if num_fid is not None and best_sq[2] == num_fid:
        # nombre y dorsal coinciden: confianza máxima
        return {"fotmob_id": num_fid, "fotmob_name": cand[num_fid], "confidence": 100,
                "source": "squad+num", "status": "MATCHED"}

    # 2) Suggest API (id estable aunque haya cambiado de club).
    #    Ojo: el endpoint responde al nombre CORTO/común, no al legal completo,
    #    así que probamos varias variantes y puntuamos contra ambos nombres.
    best_sg = (None, 0, None)
    queries, seen_q = [], set()
    for q in (search_short, search_full, player["short"]):
        nq = normalize(q)
        if nq and nq not in seen_q:
            seen_q.add(nq)
            queries.append(q)
    for q in queries:
        for opt_name, fid, team_id, is_coach in suggest(q):
            if is_coach:
                continue
            score = max(
                fuzz.token_set_ratio(normalize(search_full), normalize(opt_name)),
                fuzz.token_set_ratio(normalize(player["short"]), normalize(opt_name)),
            )
            if team_id == expected_fid:
                score = min(100, score + 10)  # bonus suave si sigue en el club
            if score > best_sg[1]:
                best_sg = (opt_name, score, fid)

    # Elegimos la mejor de las dos fuentes
    if best_sq[1] >= best_sg[1] and best_sq[2] is not None:
        name, score, fid, src = best_sq[0], best_sq[1], best_sq[2], "squad"
    else:
        name, score, fid, src = best_sg[0], best_sg[1], best_sg[2], "suggest"

    status = "MATCHED" if score >= CONFIDENCE_THRESHOLD and fid else "REVIEW"
    return {"fotmob_id": fid, "fotmob_name": name, "confidence": int(score),
            "source": src, "status": status}


# ----------------------------------------------------------------------------
# Imágenes / GCS
# ----------------------------------------------------------------------------
def download_image(fotmob_id):
    """Descarga y valida el PNG. Devuelve bytes o None."""
    try:
        r = _SESSION.get(IMG_URL.format(fid=fotmob_id), timeout=25)
        if r.status_code != 200 or not r.content:
            return None
        if "image" not in r.headers.get("content-type", ""):
            return None
        if len(r.content) < 800:  # silueta/placeholder vacío
            return None
        if Image is not None:
            Image.open(io.BytesIO(r.content)).verify()
        return r.content
    except Exception as e:
        logger.debug("img %s fallo: %r", fotmob_id, e)
        return None


def get_bucket():
    if storage is None:
        raise RuntimeError("google-cloud-storage no disponible")
    return storage.Client(project=PROJECT_ID).bucket(BUCKET_NAME)


def load_tracking_players():
    with open(TRACKING_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for p in data.get("players", []):
        if p.get("team_id") not in TEAM_MAP:
            continue
        full = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        rows.append({
            "tracking_id": p.get("id"),
            "short": p.get("short_name") or full,
            "full": full,
            "num": p.get("number"),
            "team": p.get("team_id"),
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Resolver y reportar; sin descargar ni subir")
    ap.add_argument("--force", action="store_true", help="Re-subir aunque el blob ya exista")
    args = ap.parse_args()

    players = load_tracking_players()
    logger.info("Jugadores a resolver: %d", len(players))

    # Cache de plantillas por equipo
    squads = {}
    for team, (fid, slug) in TEAM_MAP.items():
        squads[team] = fetch_squad(fid, slug)
        logger.info("Plantilla FotMob %s (%s): %d jugadores", team, fid, len(squads[team]))

    bucket = None
    if not args.dry_run:
        bucket = get_bucket()
        logger.info("☁️  Conectado a gs://%s/%s", BUCKET_NAME, GCS_PREFIX)

    results = []
    uploaded = skipped = missing = review = 0

    for p in sorted(players, key=lambda x: (x["team"], x["short"])):
        expected_fid = TEAM_MAP[p["team"]][0]
        res = resolve(p, squads[p["team"]], expected_fid)
        row = {
            "tracking_player_id": p["tracking_id"],
            "fotmob_id": res["fotmob_id"] or "",
            "player_name": p["short"],
            "fotmob_name": res["fotmob_name"] or "",
            "shirt_number": p["num"],
            "team_id": p["team"],
            "confidence": res["confidence"],
            "source": res["source"],
            "status": res["status"],
            "has_image": False,
        }

        if res["status"] == "REVIEW":
            review += 1

        if not args.dry_run and res["fotmob_id"]:
            blob = bucket.blob(f"{GCS_PREFIX}/{p['tracking_id']}.png")
            if blob.exists() and not args.force:
                row["has_image"] = True
                skipped += 1
            else:
                img = download_image(res["fotmob_id"])
                if img:
                    blob.upload_from_string(img, content_type="image/png")
                    row["has_image"] = True
                    uploaded += 1
                else:
                    missing += 1
            time.sleep(0.4)  # cortesía anti-rate-limit

        tag = res["status"]
        logger.info("  [%s %3d via %-10s] %-22s -> %s (%s)",
                    tag, res["confidence"], res["source"], p["short"],
                    res["fotmob_name"], res["fotmob_id"])
        results.append(row)

    # Escribir CSV (sin pandas para no exigir dependencia)
    import csv
    cols = ["tracking_player_id", "fotmob_id", "player_name", "fotmob_name",
            "shirt_number", "team_id", "confidence", "source", "status", "has_image"]
    with open(MAPPING_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(results)

    logger.info("=" * 60)
    logger.info("Resueltos: %d | REVIEW: %d", len(results), review)
    if not args.dry_run:
        logger.info("Subidas: %d | Ya existían: %d | Sin imagen: %d", uploaded, skipped, missing)
    logger.info("CSV -> %s", MAPPING_CSV)
    if review:
        logger.info("Revisar manualmente las filas status=REVIEW (rellenar OVERRIDES si hace falta).")


if __name__ == "__main__":
    sys.exit(main())
