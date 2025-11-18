# TACTIX_LIVE/utils/config_loader.py
import json
import os
import sys

def load_config(environment: str) -> dict:
    """Carga el archivo de configuración JSON desde la carpeta 'configs/'."""
    
    config_file_name = f"{environment.lower()}.json"
    
    # Asume que el script se ejecuta desde la raíz (un nivel arriba de 'TACTIX_LIVE')
    config_path = os.path.join("configs", config_file_name)
        
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"❌ Error: Archivo de configuración '{config_file_name}' no encontrado. "
            f"Asegúrate de que el archivo esté en la carpeta 'configs/'."
        )
        
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            
        print(f"✅ Configuración '{environment.upper()}' cargada exitosamente desde: {config_path}")
        return config_data
    except json.JSONDecodeError:
        print(f"❌ Error: El archivo '{config_file_name}' no es un JSON válido.", file=sys.stderr)
        sys.exit(1)