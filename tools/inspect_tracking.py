import pandas as pd
import json
import os

# Ajustar ruta
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(BASE_DIR, "data", "tracking_file.jsonl")


def _time_to_seconds(time_val):
    if pd.isna(time_val) or time_val is None:
        return None
    if isinstance(time_val, (int, float)):
        return float(time_val)
    time_str = str(time_val).strip()
    try:
        parts = time_str.split(':')
        if len(parts) == 3:
            return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        else:
            return float(time_str)
    except Exception:
        return None


def inspect_schema():
    print(f"🔍 Inspeccionando archivo: {DATA_FILE}")

    if not os.path.exists(DATA_FILE):
        print(f"❌ Error: No encuentro el archivo en {DATA_FILE}")
        return

    try:
        # 1. Carga
        t_df = pd.read_json(DATA_FILE, lines=True)

        # 2. Transformaciones
        t_df['timestamp'] = t_df['timestamp'].astype(str)
        t_df['game_time'] = t_df['timestamp'].apply(_time_to_seconds)
        if 'period' not in t_df.columns:
            t_df['period'] = 1

        # --- AQUÍ ESTÁ EL CAMBIO ---
        # Saltamos al frame 1340 donde dices que empieza la acción
        TARGET_FRAME = 1341

        print(f"\n⏩ Saltando al frame {TARGET_FRAME} para ver datos reales...")

        # Extraemos 1 solo registro representativo
        valid_row = t_df.iloc[TARGET_FRAME]
        valid_dict = valid_row.to_dict()

        # 3. Análisis de Tipos
        print("\n📊 TIPOS DE DATOS (En este frame):")
        print(valid_row.map(type))

        # 4. JSON Puto
        print(f"\n📋 ESTRUCTURA REAL DEL TRACKING (Frame {TARGET_FRAME}):")
        print("="*60)
        # default=str ayuda a imprimir fechas y objetos raros sin error
        print(json.dumps(valid_dict, indent=2, default=str))
        print("="*60)

        # 5. Verificación de anidamiento
        # Buscamos si hay listas dentro de las columnas
        print("\n🕵️‍♂️ ANÁLISIS DE ESTRUCTURA:")
        for key, value in valid_dict.items():
            if isinstance(value, list):
                print(f"   👉 '{key}' es una LISTA de {len(value)} elementos (Probablemente aquí están los jugadores).")
                if len(value) > 0:
                    print(f"      Ejemplo de elemento en '{key}': {value[0]}")
            elif isinstance(value, dict):
                print(f"   👉 '{key}' es un DICCIONARIO.")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    inspect_schema()
