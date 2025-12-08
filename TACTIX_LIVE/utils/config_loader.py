# TACTIX_LIVE/utils/config_loader.py
import json
import os
import sys


def load_config(environment: str) -> dict:
    """Carga el archivo de configuración JSON usando rutas absolutas."""

    config_file_name = f"{environment.lower()}.json"

    # 1. Encontrar la raíz del proyecto dinámicamente
    # (Estamos en TACTIX_LIVE/utils, subimos 2 niveles)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

    config_path = os.path.join(project_root, "configs", config_file_name)

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"❌ Error Crítico: No se encuentra '{config_file_name}' en: {config_path}\n"
            f"⚠️ Recuerda crear la carpeta 'configs' y el archivo json manualmente (no están en git)."
        )

    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"❌ Error: El archivo '{config_file_name}' no es un JSON válido.", file=sys.stderr)
        sys.exit(1)
