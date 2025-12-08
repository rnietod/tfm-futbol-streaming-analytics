import React from 'react';

const FootballPitch = ({ matchState, width = 800, height = 530 }) => {
  // Opta usa coordenadas 0-100.
  // Las escalamos al tamaño de nuestro SVG (width/height).
  const scaleX = (x) => (x / 100) * width;
  const scaleY = (y) => (y / 100) * height;

  // Si no hay datos, mostramos un mensaje de carga
  if (!matchState) {
    return (
      <div style={{ width, height, display: 'flex', alignItems: 'center', justifyContent: 'center', border: '2px solid #444', color: '#666' }}>
        <p>Esperando señal del estadio...</p>
      </div>
    );
  }

  return (
    <div style={{ position: 'relative', width: width, margin: '0 auto' }}>
      {/* --- CAPA 1: EL CÉSPED (SVG) --- */}
      <svg width={width} height={height} style={{ background: '#2e7d32', border: '5px solid #1b5e20', borderRadius: '4px' }}>
        {/* Patrón de césped (rayas) */}
        <defs>
          <pattern id="grass" x="0" y="0" width="100" height="100" patternUnits="userSpaceOnUse">
            <rect width="50" height="100" fill="#2e7d32" />
            <rect x="50" width="50" height="100" fill="#388e3c" />
          </pattern>
        </defs>
        <rect width="100%" height="100%" fill="url(#grass)" />

        {/* Líneas del campo (Blancas semitransparentes) */}
        <g stroke="rgba(255,255,255,0.8)" strokeWidth="2" fill="none">
          {/* Contorno */}
          <rect x="20" y="20" width={width - 40} height={height - 40} />
          {/* Medio campo */}
          <line x1={width / 2} y1="20" x2={width / 2} y2={height - 20} />
          <circle cx={width / 2} cy={height / 2} r={height * 0.1} />
          <circle cx={width / 2} cy={height / 2} r="2" fill="white" />

          {/* Áreas (Izquierda) */}
          <rect x="20" y={(height / 2) - (height * 0.2)} width={width * 0.15} height={height * 0.4} />
          <rect x="20" y={(height / 2) - (height * 0.1)} width={width * 0.05} height={height * 0.2} />
          
          {/* Áreas (Derecha) */}
          <rect x={width - 20 - (width * 0.15)} y={(height / 2) - (height * 0.2)} width={width * 0.15} height={height * 0.4} />
          <rect x={width - 20 - (width * 0.05)} y={(height / 2) - (height * 0.1)} width={width * 0.05} height={height * 0.2} />
        </g>
      </svg>

      {/* --- CAPA 2: LOS JUGADORES (HTML Overlay) --- */}
      {matchState.objects.map((obj) => {
        // Diferenciamos colores por equipo
        let color = 'white';
        let stroke = 'black';
        let size = 16;
        let label = obj.jersey_number;

        if (obj.team === 'home') {
          color = '#d32f2f'; // Rojo local
          stroke = 'white';
        } else if (obj.team === 'away') {
          color = '#1976d2'; // Azul visitante
          stroke = 'white';
        } else if (obj.team === 'ball') {
          color = '#ffeb3b'; // Balón amarillo
          stroke = 'black';
          size = 10;
          label = '';
        }

        return (
          <div
            key={obj.id}
            style={{
              position: 'absolute',
              transition: 'all 0.1s linear', // Suavizado de movimiento
              left: scaleX(obj.x),
              top: scaleY(obj.y),
              width: size,
              height: size,
              backgroundColor: color,
              border: `2px solid ${stroke}`,
              borderRadius: '50%',
              transform: 'translate(-50%, -50%)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: '10px',
              fontWeight: 'bold',
              color: stroke,
              boxShadow: '0 2px 4px rgba(0,0,0,0.3)',
              zIndex: obj.team === 'ball' ? 10 : 1
            }}
          >
            {label}
          </div>
        );
      })}
    </div>
  );
};

export default FootballPitch;