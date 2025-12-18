import time
import json
import random
import os
import pandas as pd
# IMPORTANTE: Usamos seleniumwire para interceptar tráfico
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import storage

# --- CONFIGURACIÓN ---
BUCKET_NAME = "tfm-datalake-raw-futbol"
PROJECT_ID = "tfm-master-futbol"
DATA_ROOT = "data"


def create_driver():
    print("🔧 Configurando Driver Masivo (Selenium Wire)...")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # OBLIGATORIO para la VM
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")  # Crucial para VM
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())

    # Configuración para interceptar respuestas descomprimidas
    driver = webdriver.Chrome(
        service=service,
        options=chrome_options,
        seleniumwire_options={'disable_encoding': True}
    )
    return driver


def handle_cookies(driver):
    """Intenta cerrar el banner de cookies rápidamente."""
    try:
        btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        btn.click()
        time.sleep(1)
    except BaseException:
        pass


def file_exists_in_gcs(bucket, blob_name):
    if not bucket:
        return False
    blob = bucket.blob(blob_name)
    return blob.exists()


def upload_to_gcs(bucket, json_data, destination_blob_name):
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(json_data), content_type='application/json')
        return True
    except Exception as e:
        print(f"      ❌ Error GCS: {e}")
        return False


def extract_match_data(driver, match_url):
    """Navega a Player Stats e intercepta el JSON."""
    try:
        # 🟢 CAMBIO CLAVE: Forzar la URL de estadísticas
        target_url = match_url
        if not target_url.endswith("/player-stats"):
            if target_url.endswith("/"):
                target_url += "player-stats"
            else:
                target_url += "/player-stats"

        # Limpiar memoria de peticiones anteriores
        del driver.requests

        driver.get(target_url)
        handle_cookies(driver)

        # Espera aleatoria para simular humano y dar tiempo a la carga de red
        time.sleep(random.uniform(1.0, 3.0))

        # Buscar en el tráfico capturado
        for request in driver.requests:
            if request.response and "api.performfeeds.com/soccerdata/matchevent" in request.url:
                # ¡ENCONTRADO!
                body = request.response.body
                try:
                    content = body.decode('utf-8')

                    # Limpieza JSONP (callback(...))
                    if content.strip():
                        start = content.find('{')
                        end = content.rfind('}') + 1
                        if start != -1 and end != -1:
                            return json.loads(content[start:end])
                except BaseException:
                    continue  # Si falla la decodificación, probamos siguiente petición

    except Exception as e:
        print(f"      ⚠️ Error scraping: {e}")

    return None


def run_mass_scraper():
    # 1. Setup GCS
    bucket = None
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        print(f"☁️ Conectado a Bucket: {BUCKET_NAME}")
    except BaseException:
        print("⚠️ Modo Local (Sin conexión a GCS)")

    # 2. Setup Driver
    driver = create_driver()

    print("🚀 Iniciando Scraping Masivo...")

    try:
        # 3. Recorrer carpetas (Ligas)
        # Ordenamos para tener un orden determinista
        ligas = sorted(os.listdir(DATA_ROOT))

        for league_folder in ligas:
            league_path = os.path.join(DATA_ROOT, league_folder)

            if os.path.isdir(league_path):
                # Recorrer archivos CSV (Temporadas)
                csvs = sorted([f for f in os.listdir(league_path) if f.endswith(".csv")], reverse=True)

                for csv_file in csvs:
                    csv_path = os.path.join(league_path, csv_file)
                    season_name = csv_file.replace(".csv", "")

                    print(f"\n📂 Procesando: {league_folder} -> {season_name}")

                    try:
                        df = pd.read_csv(csv_path)
                    except BaseException:
                        print("   ❌ Error leyendo CSV, saltando...")
                        continue

                    total_matches = len(df)

                    for i, row in df.iterrows():
                        match_id = str(row['match_id'])
                        match_url = row['url']

                        # Ruta destino: eventing/liga/temporada/id.json
                        blob_path = f"eventing/{league_folder}/{season_name}/{match_id}.json"

                        # Chequeo de existencia (Idempotencia)
                        if bucket and file_exists_in_gcs(bucket, blob_path):
                            print(f"   ⏩ [{i+1}/{total_matches}] {match_id} existe. Saltando.")
                            continue

                        print(f"   🕷️ [{i+1}/{total_matches}] Scrapeando {match_id}...")

                        # EXTRACT
                        data = extract_match_data(driver, match_url)

                        if data:
                            # LOAD
                            if bucket:
                                upload_to_gcs(bucket, data, blob_path)
                                print("      ✅ Guardado en GCS")
                            else:
                                print("      ✅ Extraído (Modo Dry-Run)")
                        else:
                            print("      ❌ Falló extracción (No JSON)")

                        # Sleep entre partidos (Anti-Ban)
                        time.sleep(random.uniform(2.0, 4.0))

    except Exception as e:
        print(f"🔥 Error Global: {e}")
    finally:
        driver.quit()
        print("🏁 Proceso finalizado.")


if __name__ == "__main__":
    run_mass_scraper()
