import time
import json
import random
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import storage

# --- CONFIGURACIÓN ---
BUCKET_NAME = "tfm-datalake-raw-futbol"  # Tu bucket real
PROJECT_ID = "tfm-master-futbol"
DATA_ROOT = "data"  # Carpeta donde están los CSVs de los índices


def create_driver():
    chrome_options = Options()
    # EN LA NUBE ES OBLIGATORIO EL HEADLESS
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def file_exists_in_gcs(bucket, blob_name):
    """Verifica si el archivo ya existe para no repetirlo."""
    blob = bucket.blob(blob_name)
    return blob.exists()


def upload_to_gcs(bucket, json_data, destination_blob_name):
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(json_data), content_type='application/json')
        return True
    except Exception as e:
        print(f"   ❌ Error GCS: {e}")
        return False


def clean_jsonp(content):
    try:
        start = content.index('{')
        end = content.rindex('}') + 1
        return json.loads(content[start:end])
    except BaseException:
        return None


def extract_match_data(driver, match_url):
    """Navega e inyecta JS para sacar el JSON."""
    try:
        driver.get(match_url)

        # Espera inteligente con random
        time.sleep(random.uniform(2.0, 4.0))  # Espera inicial de carga

        # Bypass de Cookies (Simplificado para velocidad)
        try:
            btn = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            btn.click()
            time.sleep(1)
        except BaseException:
            pass

        # Buscar URL en logs
        logs = driver.get_log("performance")
        target_url = None
        for entry in logs:
            msg = json.loads(entry["message"])["message"]
            if msg["method"] == "Network.responseReceived":
                if "api.performfeeds.com/soccerdata/matchevent" in msg["params"]["response"]["url"]:
                    target_url = msg["params"]["response"]["url"]
                    break

        if target_url:
            # Inyección JS
            js = """
            var callback = arguments[arguments.length - 1];
            fetch(arguments[0]).then(r => r.text()).then(t => callback(t)).catch(e => callback("ERR:"+e));
            """
            content = driver.execute_async_script(js, target_url)
            if content and not content.startswith("ERR:"):
                return clean_jsonp(content)

    except Exception as e:
        print(f"   ⚠️ Error scraping: {e}")

    return None


def run_mass_scraper():
    # 1. Setup GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    # 2. Setup Driver
    driver = create_driver()

    print("🚀 Iniciando Scraping Masivo...")

    # 3. Recorrer carpetas (Ligas)
    for league_folder in os.listdir(DATA_ROOT):
        league_path = os.path.join(DATA_ROOT, league_folder)

        if os.path.isdir(league_path):
            # Recorrer archivos (Temporadas)
            for csv_file in os.listdir(league_path):
                if csv_file.endswith(".csv"):
                    csv_path = os.path.join(league_path, csv_file)
                    season_name = csv_file.replace(".csv", "")

                    print(f"\n📂 Procesando: {league_folder} -> {season_name}")

                    # Leer CSV
                    df = pd.read_csv(csv_path)
                    total_matches = len(df)

                    for i, row in df.iterrows():
                        match_id = row['match_id']
                        match_url = row['url']

                        # Construir ruta de destino
                        # Estructura: eventing / liga / temporada / id.json
                        blob_path = f"eventing/{league_folder}/{season_name}/{match_id}.json"

                        # Chequeo de existencia (Idempotencia)
                        if file_exists_in_gcs(bucket, blob_path):
                            print(f"   ⏩ [{i+1}/{total_matches}] Salta {match_id} (Ya existe)")
                            continue

                        print(f"   🕷️ [{i+1}/{total_matches}] Scrapeando {match_id}...")

                        # SCRAPING REAL
                        data = extract_match_data(driver, match_url)

                        if data:
                            upload_to_gcs(bucket, data, blob_path)
                            print(f"      ✅ Guardado")
                        else:
                            print(f"      ❌ Falló extracción")

                        # 💤 SLEEP RANDOM (Anti-Ban)
                        sleep_time = random.uniform(1.5, 3.5)
                        time.sleep(sleep_time)

    driver.quit()
    print("🏁 Todo procesado.")


if __name__ == "__main__":
    run_mass_scraper()
