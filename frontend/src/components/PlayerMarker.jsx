import React from 'react';

const PlayerMarker = ({ number, team, x, y, isGK }) => {
  // Configuración de Colores Tácticos (Cyberpunk Neon)
  // Home: Rojo Neón | Away: Cyan Neón | GK: Amarillo
  const baseColor = isGK ? '#fbbf24' : (team === 'home' ? '#ff2a6d' : '#00f2ff');
  
  return (
    <div
      className="absolute flex flex-col items-center justify-center will-change-transform z-10"
      style={{
        left: x,
        top: y,
        transform: 'translate(-50%, -50%)',
        // INTERPOLACIÓN CLAVE: 100ms linear coincide con tu tasa de datos (10Hz)
        transition: 'all 0.1s linear' 
      }}
    >
      {/* Círculo Exterior (Halo) */}
      <div 
        className="rounded-full flex items-center justify-center shadow-[0_0_8px_rgba(0,0,0,0.8)]"
        style={{
          width: 22, // Tamaño más grande para ver el número
          height: 22,
          backgroundColor: 'rgba(10, 15, 30, 0.8)', // Fondo oscuro semitransparente
          border: `2px solid ${baseColor}`,
          boxShadow: `0 0 10px ${baseColor}, inset 0 0 5px ${baseColor}`
        }}
      >
        {/* Número */}
        <span className="text-[10px] font-black font-mono leading-none text-white drop-shadow-md">
          {number}
        </span>
      </div>

      {/* Triángulo de dirección (Opcional, decorativo por ahora) */}
      <div 
        className="w-0 h-0 border-l-[3px] border-r-[3px] border-b-[4px] border-l-transparent border-r-transparent mt-0.5"
        style={{ borderBottomColor: baseColor }}
      />
    </div>
  );
};

export default PlayerMarker;