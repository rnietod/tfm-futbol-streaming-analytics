import requests

BASE_URL = "http://127.0.0.1:8000"

def test_api():
    print("1️⃣ Probando API General...")
    try:
        r = requests.get(f"{BASE_URL}/")
        if r.status_code == 200:
            print(f"   ✅ API Online: {r.json()}")
        else:
            print(f"   ❌ API devolvió error: {r.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ API INACCESIBLE: {e}")
        print("      (Asegúrate de que uvicorn/docker esté corriendo en puerto 8000)")
        return False
    return True

def test_history():
    print("\n2️⃣ Probando Endpoint de Historial (DB)...")
    url = f"{BASE_URL}/match/test_match/events/history"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"   ✅ Conexión a DB Exitosa. Eventos encontrados: {len(data)}")
                if len(data) == 0:
                    print("      ⚠️ AVISO: Array vacío. La DB está conectada pero no tiene datos.")
                    print("      ¿Corriste el worker_persist.py?")
            else:
                print(f"   ❌ Error en formato: {data}")
        elif r.status_code == 404:
             print("   ❌ Error 404: El endpoint no existe.")
             print("      (Tu main.py no se ha actualizado o no se reinició el servidor)")
        else:
            print(f"   ❌ Error del Servidor: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"   ❌ Error de conexión: {e}")

if __name__ == "__main__":
    if test_api():
        test_history()