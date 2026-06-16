import React, { useRef } from 'react';
import PlayerMarker from './PlayerMarker';
import VoronoiOverlay from './pitch/VoronoiOverlay';
import PitchControlOverlay from './pitch/PitchControlOverlay';
import { buildPoints, buildMetricPoints } from '../lib/pitchModels';

// Dimensiones Oficiales
const REAL_PITCH_LENGTH = 105;
const REAL_PITCH_WIDTH = 68;

const FootballPitch = ({ matchState, latestEvent, playerMap = {}, width = 1100, height = 700, onPlayerClick, layers = {} }) => {

  // Último frame con jugadores: los ~53% de frames vacíos (gaps de detección) harían
  // parpadear marcadores y capas. Mantenemos el último válido para una vista estable.
  const lastGoodRef = useRef(null);

  // (Funciones de conversión matemática IGUAL QUE ANTES...)
  const trackingToPixels = (x_meters, y_meters) => {
    if (x_meters === null || y_meters === null || x_meters === undefined) return { x: -100, y: -100 };
    const pct_x = (x_meters + (REAL_PITCH_LENGTH / 2)) / REAL_PITCH_LENGTH;
    const pct_y = (y_meters + (REAL_PITCH_WIDTH / 2)) / REAL_PITCH_WIDTH;
    return getCanvasCoords(pct_x, pct_y);
  };

  const optaToPixels = (x_opta, y_opta) => {
    if (x_opta === undefined || y_opta === undefined) return { x: -100, y: -100 };
    const pct_x = parseFloat(x_opta) / 100;
    const pct_y = parseFloat(y_opta) / 100;
    return getCanvasCoords(pct_x, pct_y);
  };

  const getCanvasCoords = (pct_x, pct_y) => {
    const PADDING_X = width * 0.05; 
    const PADDING_Y = height * 0.05;
    const USABLE_WIDTH = width - (PADDING_X * 2);
    const USABLE_HEIGHT = height - (PADDING_Y * 2);
    const safe_x = Math.max(0, Math.min(1, pct_x));
    const safe_y = Math.max(0, Math.min(1, pct_y));
    return {
      x: PADDING_X + (safe_x * USABLE_WIDTH),
      y: PADDING_Y + ((1 - safe_y) * USABLE_HEIGHT)
    };
  };

  if (!matchState || !matchState.player_data) {
    return (
      <div style={{ width, height }} className="flex items-center justify-center border border-cyber-neon/30 rounded-lg bg-black/60 backdrop-blur-sm relative overflow-hidden">
        <div className="absolute inset-0 bg-[url('/custom-pitch.jpg')] bg-cover bg-center opacity-30 blur-sm"></div>
        <p className="text-cyber-neon font-mono animate-pulse tracking-[0.3em] relative z-10 font-bold">
          ESTABLECIENDO ENLACE SATELITAL...
        </p>
      </div>
    );
  }

  // Frame efectivo: si el actual no trae jugadores (frame vacío), usamos el último válido.
  const hasPlayers = Array.isArray(matchState.player_data)
    && matchState.player_data.some((p) => p && p.x != null);
  if (hasPlayers) lastGoodRef.current = matchState;
  const frameState = hasPlayers ? matchState : (lastGoodRef.current || matchState);

  const players = frameState.player_data;
  const ball = frameState.ball_data;

  // Área de juego en píxeles (clip de Voronoi) y puntos de los jugadores en el
  // mismo espacio que los marcadores (vía trackingToPixels).
  const PAD_X = width * 0.05;
  const PAD_Y = height * 0.05;
  const pitchBounds = [PAD_X, PAD_Y, width - PAD_X, height - PAD_Y];
  const points = buildPoints(players, playerMap, trackingToPixels);
  const metricPoints = layers.pitchControl ? buildMetricPoints(players, playerMap) : [];

  return (
    <div 
      style={{ 
        position: 'relative', width, height, margin: '0 auto',
        backgroundImage: 'url("/custom-pitch.jpg")', 
        backgroundSize: 'cover', backgroundPosition: 'center',
        borderRadius: '8px', border: '1px solid rgba(0, 242, 255, 0.3)',
        boxShadow: '0 0 30px rgba(0,0,0,0.5)'
      }}
      className="overflow-hidden"
    >
      <svg width="100%" height="100%" className="absolute top-0 left-0 z-0 pointer-events-none">
         <g stroke="rgba(255,255,255,0.6)" strokeWidth="2" fill="none">
            <rect x="5%" y="5%" width="90%" height="90%" />
            <line x1="50%" y1="5%" x2="50%" y2="95%" />
            <circle cx="50%" cy="50%" r={height * 0.09} />
            <circle cx="50%" cy="50%" r="2" fill="rgba(255,255,255,0.6)" />
            <rect x="5%" y="30%" width="12%" height="40%" />
            <rect x="5%" y="40%" width="5%" height="20%" />
            <rect x="83%" y="30%" width="12%" height="40%" />
            <rect x="90%" y="40%" width="5%" height="20%" />
         </g>
         <rect x="0" y="0" width="100%" height="100%" fill="none" stroke="rgba(0, 242, 255, 0.1)" strokeWidth="4" className="animate-pulse-slow"/>
      </svg>

      {/* CAPAS TÁCTICAS */}
      <PitchControlOverlay
        points={metricPoints}
        frame={frameState.frame}
        bounds={pitchBounds}
        width={width}
        height={height}
        active={layers.pitchControl}
      />

      <VoronoiOverlay
        points={points}
        bounds={pitchBounds}
        width={width}
        height={height}
        active={layers.voronoi}
        combined={layers.pitchControl}
      />

      {latestEvent && latestEvent.raw_x && (
        <div 
          key={latestEvent.id || Date.now()}
          className="absolute z-10 pointer-events-none"
          style={{ ...optaToPixels(latestEvent.raw_x, latestEvent.raw_y), transform: 'translate(-50%, -50%)' }}
        >
          <span className="flex h-20 w-20 relative">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyber-neon opacity-50"></span>
            <span className="relative inline-flex rounded-full h-20 w-20 border-4 border-cyber-neon opacity-80 shadow-neon"></span>
          </span>
        </div>
      )}

      {/* LAYER JUGADORES - INTEGRADO CON METADATA */}
      {players.map((player, idx) => {
        if (player.x === null) return null;
        const { x, y } = trackingToPixels(player.x, player.y);
        
        // 1. Buscamos en el Mapa usando el ID
        const pid = String(player.player_id);
        const meta = playerMap[pid] || {};
        
        // 2. Usamos el dorsal de la metadata (o el del tracking si viene, o '?')
        const dorsal = meta.number || player.jersey_number || '';
        
        // 3. Usamos el team_id para colorear
        const teamSide = (meta.team_id === 275 || player.team_id === 275) ? 'home' : 'away';
        
        // 4. Portero?
        const isGK = dorsal === 1 || dorsal === 13 || dorsal === '1' || dorsal === '13';

        return (
          <PlayerMarker 
            key={player.player_id || idx}
            number={dorsal}
            team={teamSide}
            x={x}
            y={y}
            isGK={isGK}
            onClick={() => onPlayerClick(player)}
          />
        );
      })}

      {ball && ball.x !== null && (() => {
        const { x, y } = trackingToPixels(ball.x, ball.y);
        return (
          <div
            className="absolute z-30 will-change-transform"
            style={{
              left: x, top: y, transform: 'translate(-50%, -50%)',
              transition: 'all 0.1s linear'
            }}
          >
            <div className="w-5 h-5 bg-yellow-300 rounded-full border-2 border-black shadow-[0_0_20px_rgba(255,255,0,0.9)] flex items-center justify-center">
                <div className="w-2 h-2 bg-black/30 rounded-full blur-[1px]"></div>
            </div>
          </div>
        );
      })()}

    </div>
  );
};

export default FootballPitch;