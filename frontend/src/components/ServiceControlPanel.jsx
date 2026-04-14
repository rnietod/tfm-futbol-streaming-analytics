import React, { useState, useEffect, useRef } from 'react';
import { Wifi, WifiOff, RefreshCw } from 'lucide-react';

const API_BASE = 'http://localhost:8000';

const SERVICES = [
  { id: 'api',       label: 'FastAPI',    description: 'Backend · Puerto 8000',  icon: '⚡', fixed: true },
  { id: 'worker',    label: 'Worker',     description: 'Redis → PostgreSQL',      icon: '⚙️' },
  { id: 'dashboard', label: 'Dashboard',  description: 'Streamlit · Puerto 8501', icon: '📊' },
];

/** Toggle puro: usa style en vez de clases dinámicas para evitar purge de Tailwind */
const Toggle = ({ checked, onChange, disabled, loading }) => {
  return (
    <button
      role="switch"
      aria-checked={checked}
      disabled={disabled || loading}
      onClick={() => !disabled && !loading && onChange(!checked)}
      style={{
        position: 'relative',
        display: 'inline-flex',
        alignItems: 'center',
        width: '36px',
        height: '20px',
        borderRadius: '9999px',
        border: 'none',
        cursor: disabled ? 'not-allowed' : 'pointer',
        backgroundColor: disabled ? '#3f3f46' : checked ? '#22c55e' : '#3f3f46',
        transition: 'background-color 0.3s ease',
        opacity: disabled ? 0.45 : 1,
        flexShrink: 0,
        padding: 0,
      }}
    >
      <span
        style={{
          position: 'absolute',
          width: '14px',
          height: '14px',
          borderRadius: '50%',
          backgroundColor: 'white',
          boxShadow: '0 1px 3px rgba(0,0,0,0.4)',
          transition: 'left 0.25s ease',
          left: checked ? '19px' : '3px',
          top: '3px',
          animation: loading ? 'pulse 1s infinite' : 'none',
        }}
      />
    </button>
  );
};

const ServiceControlPanel = ({ wsStatus }) => {
  const [open, setOpen]         = useState(false);
  // Estado local optimista — se actualiza inmediatamente al togglear
  const [localState, setLocalState] = useState({ api: true, worker: false, dashboard: false });
  const [loading, setLoading]   = useState({});
  const panelRef                = useRef(null);

  /* ── Sync con API ── */
  const fetchStatus = async () => {
    try {
      const res = await fetch(`${API_BASE}/admin/services/status`);
      if (res.ok) {
        const data = await res.json();
        setLocalState(prev => ({ ...prev, ...data }));
      }
    } catch { /* API no disponible aún */ }
  };

  useEffect(() => {
    fetchStatus();
    const id = setInterval(fetchStatus, 5000);
    return () => clearInterval(id);
  }, []);

  /* ── Cerrar al click fuera ── */
  useEffect(() => {
    const handler = (e) => {
      if (panelRef.current && !panelRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  /* ── Toggle con actualización optimista ── */
  const handleToggle = async (serviceId, newValue) => {
    // 1. Actualizar visualmente YA (antes de esperar la API)
    setLocalState(prev => ({ ...prev, [serviceId]: newValue }));
    setLoading(prev => ({ ...prev, [serviceId]: true }));

    const action = newValue ? 'start' : 'stop';
    try {
      await fetch(`${API_BASE}/admin/services/${action}/${serviceId}`, { method: 'POST' });
      // Sync real tras 1.5s
      setTimeout(fetchStatus, 1500);
    } catch (e) {
      // Si la API falla, revertir
      setLocalState(prev => ({ ...prev, [serviceId]: !newValue }));
      console.warn(`No se pudo ${action} el servicio ${serviceId}. ¿Está corriendo la API?`);
    } finally {
      setTimeout(() => setLoading(prev => ({ ...prev, [serviceId]: false })), 1000);
    }
  };

  const isOnline = wsStatus === 'ONLINE';

  return (
    <div className="relative" ref={panelRef}>

      {/* ── TRIGGER: el chip ONLINE / OFFLINE ── */}
      <button
        onClick={() => setOpen(v => !v)}
        className={`flex items-center gap-2 px-3 py-1 rounded-full bg-black/40 border transition-all
          hover:brightness-125 active:scale-95
          ${isOnline ? 'border-emerald-500/30' : 'border-red-500/20'}`}
      >
        {isOnline
          ? <Wifi    size={12} className="text-emerald-400" />
          : <WifiOff size={12} className="text-red-400" />}
        <span className={`font-mono text-[10px] font-bold ${isOnline ? 'text-emerald-400' : 'text-red-400'}`}>
          {wsStatus}
        </span>
      </button>

      {/* ── DROPDOWN ── */}
      {open && (
        <div
          className="absolute right-0 top-full mt-2 w-64 rounded-2xl border border-white/10
            bg-zinc-950/95 backdrop-blur-2xl shadow-2xl z-[200] overflow-hidden"
          style={{ animation: 'fadeSlideDown 0.15s ease-out forwards' }}
        >
          {/* Cabecera */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-white/5">
            <span className="text-[10px] font-bold tracking-widest text-zinc-400 uppercase">Dev Services</span>
            <button onClick={fetchStatus} className="text-zinc-600 hover:text-white transition-colors" title="Refresh">
              <RefreshCw size={11} />
            </button>
          </div>

          {/* Servicios */}
          <div className="p-2 flex flex-col gap-1">
            {SERVICES.map(svc => {
              const isRunning = localState[svc.id] === true;
              const isLoading = loading[svc.id];

              return (
                <div key={svc.id}
                  className="flex items-center justify-between px-3 py-2.5 rounded-xl hover:bg-white/5 transition-colors"
                >
                  <div className="flex items-center gap-2.5 min-w-0">
                    <span className="text-base leading-none select-none">{svc.icon}</span>
                    <div className="min-w-0">
                      <p className="text-xs font-semibold text-white leading-tight">{svc.label}</p>
                      <p className="text-[9px] text-zinc-500 leading-tight truncate">{svc.description}</p>
                    </div>
                  </div>

                  <Toggle
                    checked={isRunning}
                    onChange={(val) => handleToggle(svc.id, val)}
                    disabled={svc.fixed}
                    loading={isLoading}
                  />
                </div>
              );
            })}
          </div>

          {/* Pie */}
          <p className="text-center text-[9px] text-zinc-700 px-4 pb-3 pt-1">
            Requiere uvicorn corriendo para controlar servicios
          </p>
        </div>
      )}

      <style>{`
        @keyframes fadeSlideDown {
          from { opacity: 0; transform: translateY(-6px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
};

export default ServiceControlPanel;
