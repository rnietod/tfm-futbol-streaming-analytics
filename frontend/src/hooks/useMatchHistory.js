import { useState, useEffect, useRef, useCallback } from 'react';
import {
    BUFFER_SIZE,
    alignChunkStart,
    nextChunkStart,
    shouldPrefetchNext,
    isChunkLoaded,
} from '../lib/replayBuffer';

const API_URL = "http://localhost:8000"; // Ajustar según config
const DEBOUNCE_MS = 300; // Esperamos 300ms antes de pedir datos al arrastrar

/**
 * useMatchHistory
 * Maneja la lógica híbrida entre datos en vivo (WebSocket) y repetición (HTTP).
 *
 * Replay fluido: los chunks se piden ALINEADOS a límites fijos (0, 500, 1000…)
 * para que el caché y la deduplicación coincidan, y se hace PREFETCH del
 * siguiente chunk antes de agotar el actual (mata el parón de frontera).
 *
 * @param {string} matchId - ID del partido
 * @param {boolean} isLive - ¿Estamos en modo "En Vivo"?
 * @param {number} requestedFrame - El frame que el slider/timeline está pidiendo
 * @param {object} liveData - El paquete de datos más reciente que llegó por WS
 */
export const useMatchHistory = (matchId, isLive, requestedFrame, liveData, isPlaying = false) => {
    const [events, setEvents] = useState([]);
    const [displayData, setDisplayData] = useState(null);
    const [isLoadingHistory, setIsLoadingHistory] = useState(false);

    // Buffer masivo en ref para no re-renderizar al rellenar cientos de frames.
    const frameBuffer = useRef({});
    const pendingRequests = useRef(new Set()); // chunkStarts en vuelo
    const requestedFrameRef = useRef(requestedFrame); // frame pedido más reciente

    const fetchTimeout = useRef(null);
    const abortController = useRef(null);
    const eventsLoaded = useRef(false);

    // ==========================================
    // 1. CARGA DE EVENTOS (CONTEXTO)
    // ==========================================
    useEffect(() => {
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
        return () => { eventsLoaded.current = false; };
    }, [matchId]);

    // ==========================================
    // 2. FETCH DE UN CHUNK ALINEADO
    // ==========================================
    // urgent=true  → seek del usuario (cancela el fetch urgente anterior, marca loading).
    // urgent=false → prefetch en segundo plano (no aborta, no bloquea la UI).
    const fetchChunk = useCallback(async (frameHint, { urgent = false } = {}) => {
        const chunkStart = alignChunkStart(frameHint);

        if (pendingRequests.current.has(chunkStart)) return;       // ya en vuelo
        if (isChunkLoaded(frameBuffer.current, chunkStart)) return; // ya cargado

        pendingRequests.current.add(chunkStart);
        if (urgent) setIsLoadingHistory(true);

        let signal;
        if (urgent) {
            if (abortController.current) abortController.current.abort();
            abortController.current = new AbortController();
            signal = abortController.current.signal;
        }

        try {
            const endFrame = chunkStart + BUFFER_SIZE;
            const url = `${API_URL}/match/${matchId}/tracking/history?start_frame=${chunkStart}&end_frame=${endFrame}`;
            const res = await fetch(url, signal ? { signal } : undefined);
            if (!res.ok) throw new Error("Network response was not ok");

            const data = await res.json();
            data.forEach(item => {
                frameBuffer.current[item.frame_idx] = item.tracking;
            });

            // Refrescar el display si el frame que el usuario pide ya llegó.
            const want = requestedFrameRef.current;
            if (frameBuffer.current[want]) {
                setDisplayData(frameBuffer.current[want]);
            }
        } catch (err) {
            if (err.name !== 'AbortError') {
                console.error("Error fetching tracking history:", err);
            }
        } finally {
            pendingRequests.current.delete(chunkStart);
            if (urgent) setIsLoadingHistory(false);
        }
    }, [matchId]);

    // ==========================================
    // 3. ORQUESTADOR: LIVE
    // ==========================================
    useEffect(() => {
        if (isLive && liveData) {
            setDisplayData(liveData);
            if (liveData.frame) frameBuffer.current[liveData.frame] = liveData;
        }
    }, [isLive, liveData]);

    // ==========================================
    // 4. ORQUESTADOR: HISTORIA (caché + prefetch)
    // ==========================================
    useEffect(() => {
        if (isLive) return;
        requestedFrameRef.current = requestedFrame;

        const cachedFrame = frameBuffer.current[requestedFrame];

        if (cachedFrame) {
            // Durante el play, el frame lo fija el rAF (batched con setSliderValue)
            // para evitar un setState anidado en fase passive -> bucle de renders.
            // Aquí solo lo fijamos al hacer seek/scrub (pausado).
            if (!isPlaying) setDisplayData(cachedFrame);
            // Prefetch del siguiente chunk si nos acercamos al final del actual.
            if (shouldPrefetchNext(requestedFrame)) {
                const ncs = nextChunkStart(requestedFrame);
                if (!isChunkLoaded(frameBuffer.current, ncs)) {
                    fetchChunk(ncs, { urgent: false });
                }
            }
        } else {
            // MISS (seek): fetch del chunk actual con debounce para no spamear al arrastrar.
            if (fetchTimeout.current) clearTimeout(fetchTimeout.current);
            fetchTimeout.current = setTimeout(() => {
                fetchChunk(requestedFrame, { urgent: true });
            }, DEBOUNCE_MS);
        }

        return () => {
            if (fetchTimeout.current) clearTimeout(fetchTimeout.current);
        };
    }, [isLive, requestedFrame, isPlaying, fetchChunk]);

    return {
        displayData,
        setDisplayData, // el rAF de replay fija el frame directamente (batched)
        events,
        isLoadingHistory,
        historyRef: frameBuffer, // expuesto para el game loop de App.jsx
    };
};
