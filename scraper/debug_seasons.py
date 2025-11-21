import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.scoresway.com/en_GB/soccer/primera-divisi%C3%B3n-2025-2026/80zg2v1cuqcfhphn56u4qpyqc/results"


def run_debug():
    print("🔧 Configurando Driver...")
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        print(f"🌍 Entrando a: {URL}")
        driver.get(URL)
        time.sleep(5)  # Espera generosa

        # 1. BUSCAR EL SELECT POR ID
        print("\n--- DIAGNÓSTICO DEL SELECT ---")
        try:
            select = driver.find_element(By.ID, "season-select")
            print(f"✅ Elemento encontrado: <select id='season-select'>")
            print(f"   ¿Es visible?: {select.is_displayed()}")
            print(f"   ¿Está habilitado?: {select.is_enabled()}")
            print(f"   Clases CSS: {select.get_attribute('class')}")

            # Imprimir las primeras 3 opciones que tiene dentro
            options = select.find_elements(By.TAG_NAME, "option")
            print(f"   Opciones encontradas: {len(options)}")
            for i, op in enumerate(options[:3]):
                print(f"     Opción {i}: Texto='{op.text}' | Value='{op.get_attribute('value')}'")

        except Exception as e:
            print(f"❌ No se encontró el ID 'season-select': {e}")

        # 2. GUARDAR HTML PARA ANÁLISIS
        print("\n💾 Guardando HTML de la página...")
        with open("debug_page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("✅ Guardado: debug_page_source.html")

        # 3. GUARDAR CAPTURA (Para ver si hay un banner tapando)
        driver.save_screenshot("debug_screenshot.png")
        print("✅ Guardado: debug_screenshot.png")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_debug()
