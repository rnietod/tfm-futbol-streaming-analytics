import React from 'react';

const DynamicBackground = () => {
  return (
    // CAMBIO IMPORTANTE: 'bg-black/20' en lugar de 'bg-zinc-950'
    // Esto permite que la imagen de 'index.css' se vea a través de las líneas de neón.
    <div className="fixed inset-0 z-0 pointer-events-none bg-black/20 overflow-hidden">
      
      <style>{`
        @keyframes flow-y {
          0% { transform: translateY(-100%); opacity: 0; }
          50% { opacity: 1; }
          100% { transform: translateY(100%); opacity: 0; }
        }
        
        @keyframes pulse-energy {
          0%, 100% { opacity: 0.1; }
          50% { opacity: 0.3; }
        }

        @keyframes dash-flow {
          to { stroke-dashoffset: -1000; }
        }

        .circuit-line {
            stroke-dasharray: 20 800; 
            animation: dash-flow 30s linear infinite;
        }
      `}</style>

      {/* Eliminamos la capa base opaca anterior y dejamos solo una viñeta suave */}
      <div className="absolute inset-0 opacity-30"
           style={{
             background: `radial-gradient(circle at 50% 50%, transparent 0%, #000000 100%)`
           }}
      />

      <svg className="absolute inset-0 w-full h-full opacity-40" preserveAspectRatio="none">
        <defs>
          <linearGradient id="fade-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="rgb(6, 182, 212)" stopOpacity="0" />
            <stop offset="50%" stopColor="rgb(6, 182, 212)" stopOpacity="0.4" />
            <stop offset="100%" stopColor="rgb(6, 182, 212)" stopOpacity="0" />
          </linearGradient>
        </defs>

        <g stroke="url(#fade-gradient)" strokeWidth="1">
           <line x1="10%" y1="0" x2="0%" y2="100%" />
           <line x1="30%" y1="0" x2="20%" y2="100%" />
           <line x1="50%" y1="0" x2="50%" y2="100%" strokeOpacity="0.5" />
           <line x1="70%" y1="0" x2="80%" y2="100%" />
           <line x1="90%" y1="0" x2="100%" y2="100%" />
        </g>

        <g className="circuit-line" stroke="cyan" strokeWidth="2" strokeOpacity="0.6" filter="url(#glow)">
            <path d="M 50 0 V 1000" vectorEffect="non-scaling-stroke" />
            <path d="M 0 500 H 2000" vectorEffect="non-scaling-stroke" className="opacity-20" />
            <line x1="40%" y1="0" x2="0%" y2="100%" />
            <line x1="60%" y1="0" x2="100%" y2="100%" />
        </g>
      </svg>
      
      {/* Viñeta final para profundidad */}
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_center,transparent_0%,black_100%)] opacity-80" />

    </div>
  );
};

export default DynamicBackground;