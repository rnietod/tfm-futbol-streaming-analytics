# simulator/app.py (Versión 4.1 - Completa)
import streamlit as st
import pandas as pd
import time
import sys
import os
from datetime import datetime, timedelta

# 1. SETUP
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try:
    from engine import SimulationEngine
except ImportError:
    st.error("❌ Error importando engine.py")
    st.stop()


def format_time(seconds):
    if seconds < 0:
        return "--:--"
    td = timedelta(seconds=int(seconds))
    return f"{td.seconds//60:02d}:{td.seconds%60:02d}"


def get_latency_html(ms):
    if ms < 50:
        return f'<span class="latency-ok">{ms} ms</span>'
    if ms < 200:
        return f'<span class="latency-high">{ms} ms</span>'
    return f'<span class="latency-fail">{ms} ms</span>'


# 2. CONFIG UI
st.set_page_config(page_title="TACTIX Simulator", page_icon="⚽", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    .status-badge { padding: 5px 10px; border-radius: 4px; font-weight: bold; color: white; }
    .bg-wait { background-color: #FF9100; color: black; }
    .bg-live { background-color: #00C853; }
    .bg-stop { background-color: #D50000; }
    div[data-testid="stMetric"] { background-color: #1A1A1A; border: 1px solid #333333; border-radius: 6px; padding: 10px; }
    .latency-ok { color: #00FF00; font-weight: bold; }
    .latency-fail { color: #FF4500; font-weight: bold; }
    div.stButton > button:first-child { font-weight: bold; border-radius: 8px; font-size: 18px; padding: 10px; width: 100%; background-color: #00529F; color: white; }
    div[data-testid="stDataFrame"] { width: 100%; }
</style>
""", unsafe_allow_html=True)

# 3. STATE (Self-Healing)


def create_engine(): return SimulationEngine(env="dev")


if 'engine' not in st.session_state:
    st.session_state.engine = create_engine()
elif not hasattr(st.session_state.engine, 'sent_tracking_log'):
    st.session_state.engine = create_engine()

engine = st.session_state.engine

# 4. SIDEBAR
with st.sidebar:
    st.header("Configuración")
    st.info(f"Proyecto: `{engine.project_id}`")
    st.divider()
    if st.button("♻️ Hard Reset"):
        st.session_state.engine = create_engine()
        st.rerun()

# 5. HEADER
c1, c2 = st.columns([3, 1])
with c1:
    st.title("⚽ TACTIX Match Director")
with c2:
    status = '<span class="status-badge bg-stop">⏹️ OFFLINE</span>'
    if engine.running:
        status = '<span class="status-badge bg-live">🔴 LIVE MATCH</span>' if engine.current_time >= 0 else '<span class="status-badge bg-wait">⚠️ WAITING KICKOFF</span>'
    st.markdown(f"### {status}", unsafe_allow_html=True)

st.divider()

# 6. SCOREBOARD
if engine.current_time < 0:
    st.info(f"📡 Calibrando Cámaras... Frames Nulos Enviados: {engine.total_tracking}")
else:
    progress = (engine.current_time / engine.total_game_time) if engine.total_game_time > 0 else 0
    st.progress(min(progress, 1.0))

m1, m2, m3, m4 = st.columns(4)
m1.metric("⏱️ Tiempo", format_time(engine.current_time))
m2.metric("Total Enviados", f"{engine.total_tracking + engine.total_events:,}")
m3.metric("Latencia", "", help="Ping")
m3.markdown(get_latency_html(engine.latency_ms), unsafe_allow_html=True)
m4.metric("Errores", f"{engine.errors}", delta_color="inverse")

st.divider()

# 7. CONTROLES
btn1, btn2, btn3, btn4 = st.columns(4)
with btn1:
    if st.button("📋 Alineación"):
        with st.spinner("Cargando..."):
            engine.send_alignment()
        st.rerun()
with btn2:
    if st.button("▶️ Iniciar", disabled=engine.running):
        engine.start_stream()
        st.rerun()
with btn3:
    if st.button("⏹️ Detener", disabled=not engine.running):
        engine.stop_stream()
        st.rerun()
with btn4:
    new_speed = st.slider("Speed", 1.0, 10.0, engine.speed_multiplier, 0.5)
    if new_speed != engine.speed_multiplier:
        engine.set_speed(new_speed)

# 8. MONITOR DE RESUMEN (TABLA + LOGS)
st.subheader("📊 Resumen de Transmisión")

col_summ, col_logs = st.columns([1, 1])

with col_summ:
    t_count = max(1, engine.metrics['tracking']['count'])
    e_count = max(1, engine.metrics['eventing']['count'])

    data = {
        "Fuente": ["Tracking", "Eventing"],
        "Volumen": [engine.metrics['tracking']['count'], engine.metrics['eventing']['count']],
        "Latencia (ms)": [f"{engine.metrics['tracking']['total_latency']/t_count:.2f}", f"{engine.metrics['eventing']['total_latency']/e_count:.2f}"]
    }
    st.dataframe(pd.DataFrame(data), hide_index=True, use_container_width=True)

with col_logs:
    st.write("📜 **Log de Operaciones**")
    st.text_area("", "\n".join(engine.simple_logs), height=120, disabled=True)

st.divider()

# 9. AUDITORÍA DETALLADA (TABLAS REGISTRO A REGISTRO)
st.subheader("📝 Auditoría en Vivo")

tab_track, tab_event = st.tabs(["📡 Tracking Stream (Frames)", "⚽ Eventing Stream (Plays)"])

with tab_track:
    if engine.sent_tracking_log:
        df_track = pd.DataFrame(engine.sent_tracking_log).iloc[::-1]  # Invertir para ver lo último primero
        st.dataframe(df_track, use_container_width=True, height=300)
    else:
        st.info("Esperando inicio de transmisión...")

with tab_event:
    if engine.sent_eventing_log:
        df_event = pd.DataFrame(engine.sent_eventing_log).iloc[::-1]
        st.dataframe(df_event, use_container_width=True)
    else:
        st.info("Esperando eventos de juego...")

# Auto-refresh
if engine.running:
    time.sleep(1)
    st.rerun()
