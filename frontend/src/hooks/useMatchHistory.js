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
    const pendingRequests = useRef(new Set());

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
        // 1. Evitar peticiones duplicadas para el mismo chunk
        // Normalizamos el startFrame para que siempre pida bloques alineados (0, 500, 1000...)
        // Esto evita pedir frame 400-900 y luego 401-901.
        const chunkKey = Math.floor(startFrame / BUFFER_SIZE) * BUFFER_SIZE; // O usa startFrame directo si prefieres exactitud

        if (pendingRequests.current.has(chunkKey)) {
            return; // Ya hay una petición en vuelo para este bloque, calma.
        }

        // Marcamos como pendiente
        pendingRequests.current.add(chunkKey);
        setIsLoadingHistory(true);

        // Cancelar petición anterior si existe (Evita Race Conditions)
        if (abortController.current) {
            abortController.current.abort();
        }
        abortController.current = new AbortController();

        try {
            const endFrame = startFrame + BUFFER_SIZE;
            const url = `${API_URL}/match/${matchId}/tracking/history?start_frame=${startFrame}&end_frame=${endFrame}`;
            
            const res = await fetch(url, { signal: abortController.current.signal });
            if (!res.ok) throw new Error("Network response was not ok");

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
            // Limpieza
            pendingRequests.current.delete(chunkKey);
            setIsLoadingHistory(false);
        }
    }, [matchId, requestedFrame]);


    // ==========================================
    // 3. ORQUESTADOR: LIVE VS HISTORY
    // ==========================================
    useEffect(() => {
        // MODO LIVE: Paso directo, ignoramos buffer y API
        if (isLive && liveData) {
                setDisplayData(liveData);
                // Guardamos en buffer por si el usuario retrocede 1 segundo después
            if (liveData.frame) frameBuffer.current[liveData.frame] = liveData;
        }
    }, [isLive, liveData]);

        // MODO HISTORIA: Chequeo de Buffer
        useEffect(() => {
        // Si estamos en vivo, este efecto NO debe hacer nada.
        // Esto evita que la llegada de `liveData` reinicie el timer del fetch histórico.
        if (isLive) return;

        const cachedFrame = frameBuffer.current[requestedFrame];

        if (cachedFrame) {
            setDisplayData(cachedFrame);
        } else {
            // MISS: Necesitamos fetch. Usamos Debounce.
            if (fetchTimeout.current) clearTimeout(fetchTimeout.current);

            fetchTimeout.current = setTimeout(() => {
                // Alineamos la petición para optimizar caché (opcional, pero recomendado)
                // const alignedFrame = Math.floor(requestedFrame / 100) * 100; 
                fetchTrackingChunk(requestedFrame);
            }, DEBOUNCE_MS);
        }

        return () => {
             if (fetchTimeout.current) clearTimeout(fetchTimeout.current);
        };
    }, [isLive, requestedFrame, fetchTrackingChunk]);


    return {
        displayData,
        events,
        isLoadingHistory,
        historyRef: frameBuffer // Exponemos la ref para el loop de App.jsx
    };
};
