"""
QA del motor Ghost: valida el cálculo de z-score contra la norma histórica.

Simula la línea de stats acumuladas de un jugador a varios minutos y comprueba:
  1. El cold-start damper (z≈0 hasta el min 5, rampa hasta el 20).
  2. El signo del z (sobre-rendimiento -> z>0; bajo-rendimiento -> z<0).
  3. La proyección p30 y el z compuesto ponderado por posición.

Usa el baseline real (player_ghost_baseline) vía GhostEngine.get_profile.
Por defecto: K. Mbappé (tracking 6028) en test_match. Se puede cambiar con --tid.

  python scripts/qa_ghost_sim.py
  python scripts/qa_ghost_sim.py --tid 24955   # Julián Álvarez
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.ghost_engine import GhostEngine  # noqa: E402


def show(profile, label, live):
    bd = GhostEngine.player_breakdown(profile, live)
    mins = live["minutes"]
    print(f"\n[{label}]  min={mins:>3}  grupo={bd['position_group']}  "
          f"DEV={bd['deviation_sigma']:+.2f}σ  score={bd['overall_score']}  ({bd['status']})")
    print(f"    {'métrica':<14}{'real_p30':>9}{'media':>9}{'σ':>8}{'z':>8}")
    for k, m in bd["metrics"].items():
        print(f"    {k:<14}{m['live_p30']:>9.2f}{m['expected']:>9.2f}{m['std']:>8.2f}{m['z']:>+8.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", default="test_match")
    ap.add_argument("--tid", type=int, default=6028)  # K. Mbappé
    ap.add_argument("--state", default="Overall")
    args = ap.parse_args()

    profile = GhostEngine.get_profile(args.match, args.state, args.tid)
    if not profile:
        print(f"❌ Sin baseline para tracking_id={args.tid} en {args.match}. "
              f"¿Corriste scripts/build_ghost_baseline.py?")
        return 1

    print(f"Jugador tracking={args.tid} | rol={profile['role']} | "
          f"n_matches(baseline)={profile['n_matches']} | game_state={profile['game_state_used']}")
    print("Medias p30 esperadas:",
          {k: round(v['mean'], 3) for k, v in profile['metrics'].items() if v['mean'] is not None})

    # --- Escenario A: ARRANQUE (damper) — partido normal a su nivel ---
    print("\n=== A. Damper: rendimiento ~en su media (z debería ≈0, creciendo poco) ===")
    show(profile, "min 3 ", {"minutes": 3, "xg": 0.05, "shots": 0, "recoveries": 1, "progressive": 0})
    show(profile, "min 15", {"minutes": 15, "xg": 0.07, "shots": 1, "recoveries": 1, "progressive": 1})

    # --- Escenario B: SOBRE-RENDIMIENTO (partidazo) ---
    print("\n=== B. Sobre-rendimiento (debería dar z>0 claro tras el min 20) ===")
    show(profile, "min 30", {"minutes": 30, "xg": 0.6, "shots": 4, "goals": 1, "recoveries": 2, "progressive": 2})
    show(profile, "min 60", {"minutes": 60, "xg": 1.2, "shots": 7, "goals": 2, "recoveries": 4, "progressive": 4})

    # --- Escenario C: BAJO-RENDIMIENTO (partido gris) ---
    print("\n=== C. Bajo-rendimiento (debería dar z<0 tras el min 20) ===")
    show(profile, "min 75", {"minutes": 75, "xg": 0.0, "shots": 0, "recoveries": 0, "progressive": 0})

    return 0


if __name__ == "__main__":
    sys.exit(main())
