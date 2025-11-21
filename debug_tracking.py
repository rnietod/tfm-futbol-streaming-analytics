import pandas as pd
import json
import os

# Ruta al archivo
FILE_PATH = "data/tracking_file.jsonl"

print(f"🔍 Analizando archivo: {FILE_PATH}")

if not os.path.exists(FILE_PATH):
    print("❌ Error: No encuentro el archivo.")
    exit()

try:
    # 1. Leemos el archivo como texto puro primero para ver la linea cruda
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    raw_line_1340 = lines[1340]  # El frame 1340 (ajustado por índice 0)
    print("\n--- [1] LÍNEA CRUDA DEL FRAME 1340 (Texto) ---")
    print(raw_line_1340)

    # 2. Parseamos esa línea específica con JSON
    json_obj = json.loads(raw_line_1340)
    print("\n--- [2] OBJETO JSON DEL FRAME 1340 ---")
    print(f"Keys encontradas: {list(json_obj.keys())}")

    ts_value = json_obj.get('timestamp')
    print(f"Valor de 'timestamp': '{ts_value}'")
    print(f"Tipo de datos de 'timestamp': {type(ts_value)}")

    # 3. Ahora probamos cómo lo ve Pandas
    print("\n--- [3] CÓMO LO VE PANDAS ---")
    df = pd.read_json(FILE_PATH, lines=True)

    # Vamos a la fila donde el frame es 1340 (por si no coincide con la linea)
    row = df[df['frame'] == 1340]

    if not row.empty:
        val_pandas = row.iloc[0]['timestamp']
        print(f"Valor en Pandas: '{val_pandas}'")
        print(f"Tipo en Pandas: {type(val_pandas)}")

        # Prueba de conversión in-situ
        print("\n--- [4] PRUEBA DE CONVERSIÓN ---")
        from simulator.engine import SimulationEngine
        resultado = SimulationEngine._time_to_seconds(val_pandas)
        print(f"Resultado de tu función actual: {resultado}")
    else:
        print("❌ Pandas no encontró una fila con 'frame': 1340")

except Exception as e:
    print(f"❌ ERROR FATAL EN DIAGNÓSTICO: {e}")
