import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from src.api.main import get_match_stats

data = get_match_stats('test_match')

teamA = data.get("teamA", {})
teamB = data.get("teamB", {})

print(f"=== TEAM A: {teamA.get('teamName')} (Short: {teamA.get('teamShort')}) ===")
print(f"{'Player Name':<25} | {'Num':<3} | {'Ev X':<5} | {'Ev Y':<5} | {'Track X':<7} | {'Track Y':<7}")
print("-" * 75)

# Match tracking positions by number
track_pos_a = {t["number"]: t for t in teamA.get("averagePositionsTracking", []) if t.get("number") is not None}
for ep in teamA.get("averagePositions", []):
    num = ep["number"]
    num_str = str(num) if num is not None else "None"
    tp = track_pos_a.get(num, {}) if num is not None else {}
    tx = str(tp.get("x", "None"))
    ty = str(tp.get("y", "None"))
    print(f"{ep['name']:<25} | {num_str:<3} | {ep['x']:<5} | {ep['y']:<5} | {tx:<7} | {ty:<7}")

print(f"\n=== TEAM B: {teamB.get('teamName')} (Short: {teamB.get('teamShort')}) ===")
print(f"{'Player Name':<25} | {'Num':<3} | {'Ev X':<5} | {'Ev Y':<5} | {'Track X':<7} | {'Track Y':<7}")
print("-" * 75)

track_pos_b = {t["number"]: t for t in teamB.get("averagePositionsTracking", []) if t.get("number") is not None}
for ep in teamB.get("averagePositions", []):
    num = ep["number"]
    num_str = str(num) if num is not None else "None"
    tp = track_pos_b.get(num, {}) if num is not None else {}
    tx = str(tp.get("x", "None"))
    ty = str(tp.get("y", "None"))
    print(f"{ep['name']:<25} | {num_str:<3} | {ep['x']:<5} | {ep['y']:<5} | {tx:<7} | {ty:<7}")
