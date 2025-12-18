# src/streaming/dashboard.py
import streamlit as st
import pandas as pd
import time
import os
import sys
import requests

# Asegurar path para imports
sys.path.append(os.getcwd())

from src.streaming.engine import SimulationEngine  # noqa: E402

# CONFIGURACIÓN UI
st.set_page_config(page_title="TACTIX Command Center", page_icon="🎛️", layout="wide")
st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    .status-live { color: #00FF00; font-weight: bold; border: 1px solid #00FF00; padding: 5px; border-radius: 5px;}
    .status-off { color: #FF0000; font-weight: bold; border: 1px solid #FF0000; padding: 5px; border-radius: 5px;}
    div.stButton > button { width: 100%; border-radius: 5px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# GESTIÓN DE ESTADO (Singleton Pattern)
if 'engine' not in st.session_state:
    st.session_state.engine = SimulationEngine()

engine = st.session_state.engine

API_URL = "http://localhost:8000"

# --- SIDEBAR ---
with st.sidebar:
    st.title("🎛️ Controles")
    if st.button("♻️ Reiniciar Motor"):
        engine.stop_stream()
        st.session_state.engine = SimulationEngine()
        st.rerun()

    st.divider()

    st.markdown("### 🧨 Zona de Peligro")
    st.caption("Borra todos los datos de Redis y reinicia el entorno.")

    # Sin inputs, solo acción directa
    if st.button("☢️ PURGAR SISTEMA COMPLETO", type="primary"):
        try:
            # Llamamos al nuevo endpoint global
            response = requests.delete(f"{API_URL}/admin/reset-all")

            if response.status_code == 200:
                st.toast("✅ Sistema reiniciado. Memoria limpia.", icon="🧹")

                # Reiniciamos también el motor local para que no siga enviando datos viejos
                engine.stop_stream()
                st.session_state.engine = SimulationEngine()
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"Error del servidor: {response.text}")
        except Exception as e:
            st.error(f"No se pudo conectar con la API: {e}")

    st.divider()

    st.markdown("### Velocidad")
    speed = st.slider("Multiplicador", 0.5, 10.0, 1.0, 0.5)
    if speed != engine.speed_multiplier:
        engine.set_speed(speed)

# --- MAIN PANEL ---
c1, c2 = st.columns([3, 1])
c1.title("TACTIX: Match Simulator")
if engine.running:
    status_html = '<span class="status-live">EN VIVO</span>'
else:
    status_html = '<span class="status-off">DETENIDO</span>'
c2.markdown(f"## {status_html}", unsafe_allow_html=True)

# METRICAS
m1, m2, m3, m4 = st.columns(4)
time_display = f"{int(engine.current_time // 60):02d}:{int(engine.current_time % 60):02d}"
m1.metric("Tiempo de Juego", time_display if engine.current_time >= 0 else "--:--")
m2.metric("Periodo", engine.current_period)
m3.metric("Frames Tracking", engine.total_tracking)
m4.metric("Eventos Disparados", engine.total_events)

st.divider()

# BOTONERA ACCIÓN
b1, b2, b3 = st.columns(3)

with b1:
    # EL TRIGGER DE ALINEACIÓN (Paso 1)
    if st.button("📋 1. Enviar Alineación (Metadata)", type="primary"):
        with st.spinner("Enviando Metadata a Redis..."):
            res = engine.send_alignment()
            if res:
                st.success("Alineación cargada en el sistema.")
            else:
                st.error("Error cargando alineación.")

with b2:
    # EL TRIGGER DE INICIO (Paso 2)
    if st.button("▶️ 2. Iniciar Partido", disabled=engine.running):
        engine.start_stream()
        st.rerun()

with b3:
    if st.button("⏹️ Pausar / Detener", disabled=not engine.running):
        engine.stop_stream()
        st.rerun()

# LOGS EN VIVO
st.subheader("📡 Monitor de Salida")
c_track, c_event = st.columns(2)

with c_track:
    st.markdown("**Últimos Logs del Engine**")
    st.text_area("", "\n".join(engine.simple_logs), height=200)

with c_event:
    st.markdown("**Eventos Enviados (Live)**")
    if engine.sent_eventing_log:
        st.dataframe(pd.DataFrame(engine.sent_eventing_log).iloc[::-1], height=200, hide_index=True)
    else:
        st.info("Esperando eventos...")

# AUTO-REFRESH UI
if engine.running:
    time.sleep(1)
    st.rerun()
