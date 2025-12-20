import { useState, useEffect, useRef, useCallback } from 'react';

const API_URL = "http://localhost:8000"; // Ajustar según config
const BUFFER_SIZE = 500; // Traemos 500 frames (aprox 20 segs) por petición
const DEBOUNCE_MS = 300; // Esperamos 300ms antes de pedir datos al arrastrar

/**
 * useMatchHistory
 * Maneja la lógica híbrida entre datos en vivo (WebSocket) y repetición (HTTP).
 * * @param {string} matchId - ID del partido
 * @param {boolean} isLive - ¿Estamos en modo "En Vivo"?
 * @param {number} requestedFrame - El frame que el slider/timeline está pidiendo
 * @param {object} liveData - El paquete de datos más reciente que llegó por WS
 */
export const useMatchHistory = (matchId, isLive, requestedFrame, liveData) => {
    // Estado local
    const [events, setEvents] = useState([]);
    const [displayData, setDisplayData] = useState(null);
    const [isLoadingHistory, setIsLoadingHistory] = useState(false);

    // Cache de frames: { [frameIdx]: data }
    // Usamos useRef para el buffer masivo para no re-renderizar el componente 500 veces al hacer fetch
    const frameBuffer = useRef({});
    
    // Control de peticiones
    const fetchTimeout = useRef(null);
    const abortController = useRef(null);
    const eventsLoaded = useRef(false);

    // ==========================================
    // 1. CARGA DE EVENTOS (CONTEXTO)
    // ==========================================
    useEffect(() => {
        // Solo cargamos los eventos una vez al montar o cambiar de match
        const fetchEvents = async () => {
            try {
                const res = await fetch(`${API_URL}/match/${matchId}/events/history`);
                if (res.ok) {
                    const data = await res.json();
                    setEvents(data);
                    eventsLoaded.current = true;
                }
            } catch (err) {
                console.error("Error cargando eventos:", err);
            }
        };

        if (matchId && !eventsLoaded.current) {
            fetchEvents();
        }
        
        // Reset al cambiar de partido
        return () => { eventsLoaded.current = false; };
    }, [matchId]);


    // ==========================================
    // 2. GESTIÓN DEL BUFFER DE TRACKING (VIDEO)
    // ==========================================
    const fetchTrackingChunk = useCallback(async (startFrame) => {
        // Cancelar petición anterior si existe (Evita Race Conditions)
        if (abortController.current) {
            abortController.current.abort();
        }
        abortController.current = new AbortController();

        setIsLoadingHistory(true);

        try {
            const endFrame = startFrame + BUFFER_SIZE;
            const url = `${API_URL}/match/${matchId}/tracking/history?start_frame=${startFrame}&end_frame=${endFrame}`;
            
            const res = await fetch(url, { signal: abortController.current.signal });
            const data = await res.json();

            // Llenar el buffer (Data Merging)
            data.forEach(item => {
                frameBuffer.current[item.frame_idx] = item.tracking;
            });

            // Forzar actualización si el frame solicitado ya llegó
            if (frameBuffer.current[startFrame]) {
                setDisplayData(frameBuffer.current[startFrame]);
            }
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error("Error fetching tracking history:", err);
            }
        } finally {
            setIsLoadingHistory(false);
        }
    }, [matchId]);


    // ==========================================
    // 3. ORQUESTADOR: LIVE VS HISTORY
    // ==========================================
    useEffect(() => {
        // MODO LIVE: Paso directo, ignoramos buffer y API
        if (isLive) {
            if (liveData) {
                setDisplayData(liveData);
                // Opcional: Guardar lo live en buffer por si el usuario retrocede inmediatamente
                const currentIdx = liveData.frame; // Asumiendo que liveData trae 'frame'
                if (currentIdx) frameBuffer.current[currentIdx] = liveData;
            }
            return;
        }

        // MODO HISTORIA: Chequeo de Buffer
        const cachedFrame = frameBuffer.current[requestedFrame];

        if (cachedFrame) {
            // A. HIT: Tenemos el dato en memoria
            setDisplayData(cachedFrame);
        } else {
            // B. MISS: Necesitamos hacer fetch (con Debounce)
            // No mostramos nada o mostramos un loader/estado anterior

            // Limpiar timeout anterior (Debounce)
            if (fetchTimeout.current) clearTimeout(fetchTimeout.current);

            fetchTimeout.current = setTimeout(() => {
                fetchTrackingChunk(requestedFrame);
            }, DEBOUNCE_MS);
        }

    }, [isLive, requestedFrame, liveData, fetchTrackingChunk]);


    return {
        displayData,      // Los datos de posición para pintar AHORA
        events,           // La lista completa de eventos
        isLoadingHistory,  // Para mostrar un spinner si estamos buffereando
        history: frameBuffer.current
    };
};
