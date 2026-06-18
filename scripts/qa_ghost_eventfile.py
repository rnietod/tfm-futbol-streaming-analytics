"""
QA del motor Ghost con datos REALES de partido completo (data/eventing_file.csv).

Agrega las métricas del partido por jugador (StatsBomb), las proyecta a p30 y las
cruza contra el baseline histórico (player_ghost_baseline) vía GhostEngine, para
validar que el z-score refleja el rendimiento real (p.ej. Julián Álvarez, 2 goles,
debe salir claramente positivo).

  python scripts/qa_ghost_eventfile.py
"""
import csv
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.ghost_engine import GhostEngine  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS = os.path.join(BASE, "data", "eventing_file.csv")
MAPPING = os.path.join(BASE, "data", "dim_player_mapping.csv")
MATCH = "test_match"
STATE = "Overall"

# Jugadores destacados a validar (opta_player_id -> etiqueta)
WATCH = {
    29560: "Julián Álvarez (2 goles)",
    3009:  "K. Mbappé",
    5199:  "Koke",
}


def load_opta_to_tracking():
    m = {}
    with open(MAPPING, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            opta = (r.get("opta_player_id") or "").split(".")[0]
            trk = (r.get("tracking_player_id") or "").split(".")[0]
            if opta and trk:
                m[opta] = trk
    return m


def aggregate(df_p):
    """Métricas del partido para un jugador (StatsBomb) -> dict 'live' del engine."""
    shots = df_p[df_p.event_type_id == 16]
    passes = df_p[df_p.event_type_id == 30]
    completed = passes[passes.outcome_name.isna()]  # en StatsBomb, pase OK = sin outcome
    progressive = completed[(completed.end_location_x - completed.location_x) > 10]
    mins = df_p.minute
    minutes = int(mins.max() - mins.min() + 1) if len(mins) else 0
    return {
        "minutes": minutes,
        "xg": float(shots.statsbomb_xg.fillna(0).sum()),
        "shots": int(len(shots)),
        "goals": int((shots.outcome_name == "Goal").sum()),
        "key_passes": int(df_p.key_pass_id.notna().sum()),
        "progressive": int(len(progressive)),
        "recoveries": int((df_p.event_type_id == 2).sum()),
        "interceptions": int((df_p.event_type_id == 10).sum()),
    }


def main():
    df = pd.read_csv(EVENTS, encoding="utf-8")
    o2t = load_opta_to_tracking()

    print(f"Partido: match_id={df.match_id.iloc[0]} | eventos={len(df)} | "
          f"equipos={', '.join(df.team_name.unique())}\n")

    for opta, label in WATCH.items():
        df_p = df[df.player_id == float(opta)]
        if df_p.empty:
            print(f"⚠️  {label}: sin eventos en el archivo")
            continue
        tid = o2t.get(str(opta))
        if not tid:
            print(f"⚠️  {label}: sin mapeo a tracking")
            continue
        profile = GhostEngine.get_profile(MATCH, STATE, int(tid))
        if not profile:
            print(f"⚠️  {label}: sin baseline histórico")
            continue

        live = aggregate(df_p)
        bd = GhostEngine.player_breakdown(profile, live)

        print(f"=== {label}  (tracking {tid}, rol {profile['role']}->{bd['position_group']}, "
              f"min={live['minutes']}, baseline n={profile['n_matches']}) ===")
        print(f"    DEV COMPUESTA: {bd['deviation_sigma']:+.2f}σ   score={bd['overall_score']}   ({bd['status']})")
        print(f"    {'métrica':<14}{'real':>7}{'real_p30':>10}{'media':>9}{'σ':>8}{'z':>8}")
        for k, mm in bd["metrics"].items():
            print(f"    {k:<14}{live.get(k, 0):>7.2f}{mm['live_p30']:>10.2f}"
                  f"{mm['expected']:>9.2f}{mm['std']:>8.2f}{mm['z']:>+8.2f}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
