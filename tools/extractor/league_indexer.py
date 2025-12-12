import time
import pandas as pd
import os
# Borramos 'import sys' que no se usa
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURACIÓN MAESTRA ---
LEAGUES_CONFIG = [
    {
        "name": "la_liga",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/primera-divisi%C3%B3n-2025-2026/"
            "80zg2v1cuqcfhphn56u4qpyqc/results"
        )
    },
    {
        "name": "premier_league",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/premier-league-2025-2026/"
            "51r6ph2woavlbbpk8f29nynf8/results"
        )
    },
    {
        "name": "ligue_1",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/ligue-1-2025-2026/"
            "dbxs75cag7zyip5re0ppsanmc/results"
        )
    },
    {
        "name": "bundesliga",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/bundesliga-2025-2026/"
            "2bchmrj23l9u42d68ntcekob8/results"
        )
    },
    {
        "name": "serie_a",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/serie-a-2025-2026/"
            "emdmtfr1v8rey2qru3xzfwges/results"
        )
    },
    {
        "name": "champions_league",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/uefa-champions-league-2025-2026/"
            "2mr0u0l78k2gdsm79q56tb2fo/results"
        )
    },
    {
        "name": "liga_argentina",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/liga-profesional-argentina-2025/"
            "3l4bzc8syz1ea2dnv453kp89g/results"
        )
    },
    {
        "name": "brasileirao",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/serie-a-2025/"
            "9pqtmpr3w8jm73y0eb8hmum8k/results"
        )
    },
    {
        "name": "efl_championship",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/championship-2025-2026/"
            "bmmk637l2a33h90zlu36kx8no/results"
        )
    },
    {
        "name": "primeira_liga_portugal",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/primeira-liga-2025-2026/"
            "7l4z25o4hc0fmj9rdspcisdn8/results"
        )
    },
    {
        "name": "eredivisie",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/eredivisie-2025-2026/"
            "aouykkl1rt7zo06sg0kbzkbh0/results"
        )
    },
    {
        "name": "segunda_division_es",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/segunda-divisi%C3%B3n-2025-2026/"
            "dko0hzifl1xv9c51s3ai017v8/results"
        )
    },
    {
        "name": "super_lig_turkey",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/s%C3%BCper-lig-2025-2026/"
            "97zghcaoec1isvvdkh9ud50d0/results"
        )
    },
    {
        "name": "bundesliga_austria",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/bundesliga-2025-2026/"
            "4jr49qim3gv5bjx3wjkf6n0us/results"
        )
    },
    {
        "name": "superliga_dinamarca",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/superliga-2025-2026/"
            "3o8l1yf2irp018eaa2far455g/results"
        )
    },
    {
        "name": "liga_uruguay",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/liga-auf-2025/"
            "1sy3nz08f7za3zwe6pvfdm710/results"
        )
    },
    {
        "name": "europa_league",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/uefa-europa-league-2025-2026/"
            "7ttpe5jzya3vjhjadiemjy7mc/results"
        )
    },
    {
        "name": "copa_libertadores",
        "url": (
            "https://www.scoresway.com/en_GB/soccer/conmebol-libertadores-2025/"
            "168tc8nsaknvru654r6qwupsk/results"
        )
    }
]

MAX_SEASONS = 10


def create_driver():
    print("🔧 Configurando Crawler...")
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def handle_cookies(driver):
    try:
        btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        btn.click()
        time.sleep(1)
    except BaseException:
        pass


def get_last_10_seasons(driver):
    """Extrae temporadas del select."""
    print("📅 Buscando temporadas...")
    seasons_data = []
    try:
        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "season-select"))
        )
        select = Select(select_elem)

        for option in select.options:
            name = option.text.strip()
            value = option.get_attribute("value")
            if value:
                url = f"https://www.scoresway.com{value}" if value.startswith("/") else value

                # Lógica para forzar la vista de RESULTADOS y no Fixtures
                # (Esto asegura que veamos partidos pasados, no futuros)
                if "/fixtures" in url:
                    url = url.replace("/fixtures", "/results")
                elif "/results" not in url:
                    base_parts = url.split("/")
                    url = "/".join(base_parts[:-1]) + "/results"

                seasons_data.append({'name': name, 'url': url})

        # Tomamos las últimas 10
        limited = seasons_data[:MAX_SEASONS]
        print(f"   ✅ Detectadas {len(seasons_data)} temporadas. Procesando últimas {len(limited)}.")
        return limited
    except Exception as e:
        print(f"   ⚠️ Error en selector de temporadas: {e}")
        return []


def extract_matches(driver):
    """Extrae partidos de la vista actual."""
    matches = []
    try:
        # Esperar enlaces de partido
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/match/view/')]"))
        )

        elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/match/view/')]")

        for el in elements:
            try:
                url = el.get_attribute("href")
                if url and "/match/view/" in url:
                    match_id = url.split("/match/view/")[1].split("/")[0]
                    matches.append({
                        "match_id": match_id,
                        "url": url
                    })
            except BaseException:
                continue
    except BaseException:
        pass

    unique = {v['match_id']: v for v in matches}.values()
    return list(unique)


def save_season_csv(matches, league_name, season_name):
    """Guarda el CSV en data/{league_name}/{season}.csv"""
    if not matches:
        return

    # 1. Crear carpeta dinámica
    folder_path = f"data/{league_name}"
    os.makedirs(folder_path, exist_ok=True)

    # 2. Limpiar nombre
    safe_name = season_name.replace("/", "-").replace(" ", "_")
    file_path = f"{folder_path}/{safe_name}.csv"

    # 3. Guardar
    df = pd.DataFrame(matches)
    df.to_csv(file_path, index=False)
    print(f"      💾 Guardado: {file_path} ({len(df)} partidos)")


def run_indexer():
    driver = create_driver()

    try:
        print(f"🚀 Iniciando Indexador Multi-Liga ({len(LEAGUES_CONFIG)} ligas)")

        for idx, league in enumerate(LEAGUES_CONFIG):
            l_name = league['name']
            l_url = league['url']

            print(f"\n🏆 [{idx+1}/{len(LEAGUES_CONFIG)}] PROCESANDO LIGA: {l_name.upper()}")
            print(f"   🌍 URL Base: {l_url}")

            try:
                driver.get(l_url)
                # Solo manejamos cookies en la primera liga
                if idx == 0:
                    handle_cookies(driver)
                time.sleep(3)

                seasons = get_last_10_seasons(driver)

                if not seasons:
                    print(f"   ❌ No se encontraron temporadas para {l_name}. Saltando.")
                    continue

                # Iterar Temporadas
                for i, season in enumerate(seasons):
                    s_name = season['name']
                    s_url = season['url']

                    print(f"   🔄 Temporada [{i+1}/{len(seasons)}]: {s_name}")

                    if driver.current_url != s_url:
                        driver.get(s_url)
                        time.sleep(4)

                    matches = extract_matches(driver)

                    if len(matches) > 0:
                        print(f"      ✅ Encontrados: {len(matches)}")
                        save_season_csv(matches, l_name, s_name)
                    else:
                        print("      ⚠️ 0 Partidos. Intentando scroll...")
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)
                        matches = extract_matches(driver)
                        if matches:
                            print(f"      ✅ Recuperados: {len(matches)}")
                            save_season_csv(matches, l_name, s_name)
                        else:
                            print("      ❌ Vacío.")

            except Exception as e:
                print(f"   🔥 Error procesando {l_name}: {e}")
                continue  # Si falla una liga, seguimos con la siguiente

        print("\n✨ TODO EL TRABAJO TERMINADO ✨")

    except Exception as e:
        print(f"🔥 Error Fatal Global: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    run_indexer()
