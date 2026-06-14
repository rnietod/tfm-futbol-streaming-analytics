"""
Construye en BigQuery, a partir de `marts_football.int_match_lineups` (alineaciones type-34 ya
materializadas), dos tablas para el "11 con alineación estándar":

  - `ref_formation_layout`  : (formation_id, slot) -> (position_code, label_es, x, y)
        Diccionario de posiciones por formación. Coordenadas 0-100 (x: portería propia 0 -> ataque
        100; y: 0-100, lado derecho = y bajo, izquierdo = y alto — el frontend espeja Y).
  - `mart_player_positions` : player_id -> posición principal (label,x,y) + top-5 perfiles
        (label, nº de partidos) ordenados de más a menos jugado. Carrera completa.

El layout se deriva del tier (Opta Q44: 1=GK,2=Def,3=Mid,4=Fwd) modal por (formación, slot) — que
se lee de int_match_lineups — repartiendo cada línea simétricamente por la cancha. Cubre las 23
formaciones del dataset. Reproducible (idempotente: CREATE OR REPLACE).

Uso:  python tools\build_dofa_positions.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.bigquery_client import BigQueryClient  # noqa: E402

DATASET = "tfm-master-futbol.marts_football"
LINEUPS = f"`{DATASET}.int_match_lineups`"
REF = f"`{DATASET}.ref_formation_layout`"
MART = f"`{DATASET}.mart_player_positions`"

# Orden lateral canónico de cada slot (derecha -> izquierda). Derivado de la convención de
# numeración de Opta validada con datos (PSG 4-3-3: slot2=lat.dcho, slot3=lat.izdo, 5/6=centrales).
LATERAL_RANK = {1: 2, 2: 0, 3: 4, 4: 2, 5: 1, 6: 3, 7: 1, 8: 3, 9: 2, 10: 0, 11: 4}


def _ys(n, lo=12, hi=88):
    """Reparto simétrico de y (ancho) en el rango [lo,hi]. Ascendente = dcha->izda."""
    if n <= 0:
        return []
    if n == 1:
        return [round((lo + hi) / 2)]
    step = (hi - lo) / (n - 1)
    return [round(lo + i * step) for i in range(n)]


def _spread_group(slots, x, lo=12, hi=88):
    """Asigna (x, y) a los slots de una línea (ancho [lo,hi]), ordenados derecha->izquierda."""
    if not slots:
        return {}
    ordered = sorted(slots, key=lambda s: LATERAL_RANK[s])
    ys = _ys(len(ordered), lo, hi)
    return {s: (x, ys[i]) for i, s in enumerate(ordered)}


def _label_for(slot, role, n_in_line, idx_in_line):
    """Etiqueta en español según rol y posición lateral dentro de la línea."""
    if role == "GK":
        return "POR", "POR"
    if role == "WB":
        return ("CARD", "Carrilero Dcho") if LATERAL_RANK[slot] < 2 else ("CARI", "Carrilero Izdo")
    if role == "DEF":
        if n_in_line >= 4 and idx_in_line == 0:
            return "LD", "Lateral Dcho"
        if n_in_line >= 4 and idx_in_line == n_in_line - 1:
            return "LI", "Lateral Izdo"
        return "DFC", "Defensa Central"
    if role == "DM":
        return "MCD", "Medio Centro Def"
    if role == "CM":
        return "MC", "Medio Centro"
    if role == "WM":
        return ("MD", "Medio Dcho") if LATERAL_RANK[slot] < 2 else ("MI", "Medio Izdo")
    if role == "FW":
        if n_in_line >= 3 and idx_in_line == 0:
            return "ED", "Extremo Dcho"
        if n_in_line >= 3 and idx_in_line == n_in_line - 1:
            return "EI", "Extremo Izdo"
        if n_in_line == 1:
            return "DC", "Delantero Centro"
        return "DC", "Delantero Centro"
    return "MC", "Medio Centro"


def build_layout(tiers):
    """tiers: dict {slot:int -> tier:str '1'..'4'}. Devuelve {slot -> (code,label,x,y)}."""
    backthree = tiers.get(2) == "3" and tiers.get(4) == "2"
    defenders = [s for s in range(2, 12) if tiers.get(s) == "2"]
    fwds = [s for s in range(2, 12) if tiers.get(s) == "4"]
    mids_all = [s for s in range(2, 12) if tiers.get(s) == "3"]

    wingbacks = [s for s in (2, 3) if s in mids_all] if backthree else []
    widemids = [s for s in (10, 11) if s in mids_all]
    mids = [s for s in mids_all if s not in wingbacks and s not in widemids]
    dms = [s for s in mids if s == 4]
    cms = [s for s in mids if s != 4]

    coords = {1: (8, 50)}
    # Defensas: línea de 4/5 abre todo el ancho; trío de centrales (línea de 3) más estrecho.
    if backthree:
        coords.update(_spread_group(defenders, 22, lo=28, hi=72))
    else:
        coords.update(_spread_group(defenders, 23, lo=12, hi=88))
    coords.update(_spread_group(wingbacks, 45, lo=8, hi=92))   # carrileros, bien abiertos
    coords.update(_spread_group(dms, 40, lo=38, hi=62))         # pivote(s), central
    coords.update(_spread_group(cms, 56, lo=30, hi=70))         # medios centrales, estrechos
    coords.update(_spread_group(widemids, 64, lo=14, hi=86))    # medios de banda, abiertos
    # Delanteros: tridente abre el ancho; pareja/punta más centrada.
    fw_lo, fw_hi = (12, 88) if len(fwds) >= 3 else (34, 66)
    coords.update(_spread_group(fwds, 84, lo=fw_lo, hi=fw_hi))

    # roles por slot, para la etiqueta
    role = {1: "GK"}
    for s in defenders:
        role[s] = "DEF"
    for s in wingbacks:
        role[s] = "WB"
    for s in dms:
        role[s] = "DM"
    for s in cms:
        role[s] = "CM"
    for s in widemids:
        role[s] = "WM"
    for s in fwds:
        role[s] = "FW"

    # índices laterales dentro de cada línea (para LD/LI, ED/EI)
    line_members = {}
    for s, r in role.items():
        line_members.setdefault(r, []).append(s)
    for r in line_members:
        line_members[r] = sorted(line_members[r], key=lambda s: LATERAL_RANK[s])

    out = {}
    for s in range(1, 12):
        if s not in role:
            continue
        r = role[s]
        members = line_members[r]
        idx = members.index(s)
        code, label = _label_for(s, r, len(members), idx)
        x, y = coords[s]
        out[s] = (code, label, x, y)
    return out


def main():
    bq = BigQueryClient()
    cli = bq._client

    # 1. Tier modal por (formación, slot) desde la tabla materializada (barato)
    rows = bq.query(
        "SELECT formation_id, formation_place, "
        "APPROX_TOP_COUNT(coarse_pos,1)[OFFSET(0)].value coarse "
        "FROM " + LINEUPS + " WHERE formation_place BETWEEN 1 AND 11 "
        "GROUP BY formation_id, formation_place"
    )[0]
    tiers_by_f = {}
    for r in rows:
        tiers_by_f.setdefault(r["formation_id"], {})[int(r["formation_place"])] = r["coarse"]

    # 2. Generar filas del diccionario
    structs = []
    for fid, tiers in tiers_by_f.items():
        layout = build_layout(tiers)
        for slot, (code, label, x, y) in layout.items():
            lab = label.replace("'", "")
            structs.append(
                f"STRUCT('{fid}' AS formation_id, {slot} AS slot, '{code}' AS position_code, "
                f"'{lab}' AS label, {x} AS x, {y} AS y)"
            )
    ref_sql = (
        "CREATE OR REPLACE TABLE " + REF + " AS SELECT formation_id, slot, position_code, label, "
        "x, y FROM UNNEST([" + ",".join(structs) + "])"
    )
    cli.query(ref_sql).result()
    print("ref_formation_layout creada:", len(structs), "filas")

    # 3. mart_player_positions: por jugador, principal + top-5 perfiles (sin subconsultas correladas)
    mart_sql = (
        "CREATE OR REPLACE TABLE " + MART + " AS "
        "WITH lp AS ("
        "  SELECT l.player_id, r.position_code AS code, r.label, r.x, r.y, l.match_id "
        "  FROM " + LINEUPS + " l JOIN " + REF + " r "
        "  ON l.formation_id=r.formation_id AND l.formation_place=r.slot "
        "  WHERE l.formation_place BETWEEN 1 AND 11 AND l.player_id IS NOT NULL"
        "), "
        "by_label AS ("
        "  SELECT player_id, label, ANY_VALUE(code) code, COUNT(DISTINCT match_id) matches "
        "  FROM lp GROUP BY player_id, label"
        "), "
        "by_slot AS ("
        "  SELECT player_id, label, x, y, COUNT(DISTINCT match_id) matches "
        "  FROM lp GROUP BY player_id, label, x, y"
        "), "
        "ranked AS ("
        "  SELECT player_id, label, x, y, "
        "    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY matches DESC, label) rn "
        "  FROM by_slot"
        "), "
        "primary_pos AS (SELECT player_id, label AS primary_label, x AS primary_x, y AS primary_y "
        "                FROM ranked WHERE rn=1), "
        "pos_arr AS (SELECT player_id, "
        "  ARRAY_AGG(STRUCT(label, code, matches) ORDER BY matches DESC, label LIMIT 5) AS positions "
        "  FROM by_label GROUP BY player_id) "
        "SELECT pa.player_id, pp.primary_label, pp.primary_x, pp.primary_y, pa.positions "
        "FROM pos_arr pa JOIN primary_pos pp USING(player_id)"
    )
    cli.query(mart_sql).result()
    cnt = bq.query("SELECT COUNT(1) c FROM " + MART)[0][0]["c"]
    print("mart_player_positions creada:", cnt, "jugadores")


if __name__ == "__main__":
    main()
