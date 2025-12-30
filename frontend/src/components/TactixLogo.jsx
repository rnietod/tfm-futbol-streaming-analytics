import React from 'react';

const TactixLogo = ({ size = 40, className = "", variant = "default" }) => {
  // Variantes de color
  // 'default': Usa el gradiente de marca (Cyan -> Blue)
  // 'mono': Usa el color de texto actual (fill-current / stroke-current) ideal para gris o blanco
  
  const isDefault = variant === 'default';

  return (
    <svg 
      width={size} 
      height={size} 
      viewBox="0 0 100 100" 
      fill="none" 
      xmlns="http://www.w3.org/2000/svg"
      className={`${className} overflow-visible`}
      aria-label="Tactix Logo"
    >
      <defs>
        {/* Gradiente Cyber-Tactix */}
        <linearGradient id="tactixGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#22d3ee" /> {/* Cyan-400 */}
          <stop offset="100%" stopColor="#3b82f6" /> {/* Blue-500 */}
        </linearGradient>
        
        {/* Filtro de brillo sutil para el modo 'default' */}
        <filter id="glow-logo" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="2" result="coloredBlur" />
          <feMerge>
            <feMergeNode in="coloredBlur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {/* GRUPO PRINCIPAL */}
      {/* Si es default usamos url(#gradiente), si no 'currentColor' para heredar color del texto */}
      <g 
        stroke={isDefault ? "url(#tactixGradient)" : "currentColor"} 
        strokeWidth="8" 
        strokeLinecap="round" 
        strokeLinejoin="round"
        filter={isDefault ? "url(#glow-logo)" : ""}
      >
        
        {/* 1. EL ESCUDO CIRCUITO (Hexágono Estilizado Abierto) */}
        {/* Representa la defensa y la estructura de datos */}
        <path 
          d="M50 5 
             L88 25 
             V65 
             L50 95 
             L12 65 
             V25 
             L50 5 Z" 
          className="opacity-20" // El escudo exterior es sutil, el marco del sistema
          fill="none"
          strokeWidth="4"
        />

        {/* 2. LA "X" DE CONEXIÓN (El Corazón Táctico) */}
        {/* Dos líneas que se cruzan dinámicamente, no tocando los bordes para dar aire */}
        
        {/* Línea 1: Diagonal Superior Izquierda a Inferior Derecha */}
        <path d="M30 30 L70 70" />
        
        {/* Línea 2: Diagonal Superior Derecha a Inferior Izquierda */}
        {/* Cortada en el centro para dar efecto de profundidad 3D/superposición */}
        <path d="M70 30 L56 44" /> {/* Segmento superior */}
        <path d="M44 56 L30 70" /> {/* Segmento inferior */}

        {/* 3. NODOS DE DATOS (Círculos sólidos) */}
        {/* Puntos de anclaje que sugieren 'Data Points' o jugadores */}
        <circle cx="30" cy="30" r="1.5" stroke="none" fill={isDefault ? "#22d3ee" : "currentColor"} />
        <circle cx="70" cy="70" r="1.5" stroke="none" fill={isDefault ? "#3b82f6" : "currentColor"} />
        <circle cx="70" cy="30" r="1.5" stroke="none" fill={isDefault ? "#3b82f6" : "currentColor"} />
        <circle cx="30" cy="70" r="1.5" stroke="none" fill={isDefault ? "#22d3ee" : "currentColor"} />

      </g>
    </svg>
  );
};

export default TactixLogo;