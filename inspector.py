# inspector.py (V1.1 - Range View)
import streamlit as st
import pandas as pd
import json
import os

# Configuración de la página
st.set_page_config(
    page_title="TACTIX Inspector v1.1",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS para Dark Mode
st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    .block-container { padding-top: 2rem; }
    div.stButton > button:first-child {
        background-color: #262730;
        color: #ffffff;
        border: 1px solid #4c4c4c;
    }
    code { color: #00ff00; }
</style>
""", unsafe_allow_html=True)

# --- BARRA LATERAL: SELECCIÓN DE ARCHIVO ---
with st.sidebar:
    st.title("🔍 Inspector V1.1")
    st.caption("Exploración por Rangos")

    data_folder = "data"
    if not os.path.exists(data_folder):
        st.error(f"⚠️ Carpeta '{data_folder}' no encontrada.")
        st.stop()

    files = [f for f in os.listdir(data_folder) if f.endswith(('.jsonl', '.csv', '.json'))]

    if not files:
        st.warning("No hay archivos en 'data/'")
        st.stop()

    selected_file = st.radio("Selecciona Archivo:", files)
    file_path = os.path.join(data_folder, selected_file)

    st.divider()
    st.info(f"📂 Cargando: {selected_file}")

# --- LÓGICA PRINCIPAL ---

st.header(f"Analizando: `{selected_file}`")

try:
    # === MODO JSONL (TRACKING) ===
    if selected_file.endswith('.jsonl'):

        @st.cache_data
        def load_jsonl(path):
            data = []
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line))
            return data

        with st.spinner("Cargando archivo completo en memoria..."):
            records = load_jsonl(file_path)

        total_records = len(records)
        st.success(f"✅ Archivo cargado. Total Objetos: **{total_records:,}**")

        st.divider()

        # --- CONTROLES DE RANGO ---
        c1, c2, c3 = st.columns([1, 1, 2])

        with c1:
            start_idx = st.number_input("Desde (Índice)", min_value=0, max_value=total_records - 1, value=0)

        with c2:
            # Por defecto mostramos 10 registros
            default_end = min(start_idx + 10, total_records)
            end_idx = st.number_input(
                "Hasta (Índice - No incluido)",
                min_value=start_idx + 1,
                max_value=total_records,
                value=default_end)

        range_size = end_idx - start_idx

        with c3:
            st.write("###")  # Espaciador
            if range_size > 1000:
                st.error(f"⚠️ El rango seleccionado ({range_size} objetos) supera el límite de 1000.")
                st.stop()
            else:
                st.info(f"Visualizando **{range_size}** objetos")

        # --- VISUALIZACIÓN ---
        subset = records[start_idx:end_idx]

        tab1, tab2 = st.tabs(["📄 Vista JSON (Árbol)", "📊 Vista Tabla (Resumen)"])

        with tab1:
            st.caption("Expande los objetos para ver los detalles profundos.")
            st.json(subset, expanded=False)  # expanded=False para que salgan contraídos

        with tab2:
            st.caption("Vista aplanada de primer nivel para detectar patrones.")
            # Convertimos a DF para ver tabla fácil
            df_preview = pd.json_normalize(subset, max_level=0)
            st.dataframe(df_preview, use_container_width=True)

    # === MODO CSV (EVENTING) ===
    elif selected_file.endswith('.csv'):
        df = pd.read_csv(file_path)
        st.write(f"Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")

        # Filtro de rango para CSV también
        c1, c2 = st.columns(2)
        with c1:
            start_row = st.number_input("Fila Inicial", 0, len(df) - 1, 0)
        with c2:
            end_row = st.number_input("Fila Final", start_row + 1, len(df), min(start_row + 100, len(df)))

        if end_row - start_row > 1000:
            st.warning("Mostrando solo los primeros 1000 del rango seleccionado por rendimiento.")
            end_row = start_row + 1000

        st.subheader("Vista de Tabla")
        st.dataframe(df.iloc[start_row:end_row], use_container_width=True)

    # === MODO JSON (IDS) ===
    elif selected_file.endswith('.json'):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        st.json(data)

except Exception as e:
    st.error(f"❌ Error al leer el archivo: {e}")
