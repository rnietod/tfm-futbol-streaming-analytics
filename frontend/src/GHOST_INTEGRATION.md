# Guía de Integración Ghost Engine (AURA)

Aquí tienes los pasos finales para conectar el cerebro (Backend) con la belleza visual (Frontend).

## 1. Archivos Creados
He generado los siguientes archivos TypeScript listos para usar:
- `types/ghost.ts`: Interfaces para `GhostTickerResponse` y `FullProfileResponse`.
- `services/ghostService.ts`: Servicio para consumir los endpoints (polling y on-click).
- `hooks/useGhostTicker.ts`: Hook personalizado que maneja el polling cada 5s.
- `components/SmartGhostTicker.tsx`: Componente contenedor que conecta el hook con la UI.

## 2. Integración en `App.jsx`

Para completar la misión, necesitamos actualizar `src/App.jsx` para usar estos nuevos superpoderes.

### Paso A: Importar el Servicio y el Componente Inteligente

```javascript
// Al inicio de App.jsx
import { SmartGhostTicker } from './components/SmartGhostTicker'; // Nuevo componente
import { ghostService } from './services/ghostService'; // Para el perfil completo
```

### Paso B: Actualizar `handleSelectPlayer` (Consumo del Endpoint Pesado)

Actualmente solo seleccionas el jugador con datos básicos. Vamos a enriquecerlo con la "Enciclopedia completa".

```javascript
  const handleSelectPlayer = async (playerData) => {
      // 1. Mostrar estado inicial (loading/datos básicos)
      setSelectedPlayer({ ...playerData, isLoading: true });

      try {
          // 2. Llamada al Endpoint Pesado (On-Click)
          // Asumimos match_id = "test_match" por ahora
          const fullProfile = await ghostService.getPlayerFullProfile("test_match", playerData.id);
          
          // 3. Actualizar con los datos reales (Metrics, Percentiles, etc.)
          setSelectedPlayer(prev => ({
              ...prev,
              ...fullProfile, // Fusionamos la respuesta del backend
              isLoading: false
          }));
      } catch (err) {
          console.error("Error cargando perfil táctico:", err);
          // Manejo de error o mantener datos básicos
          setSelectedPlayer(prev => ({ ...prev, isLoading: false }));
      }
  };
```

### Paso C: Reemplazar el Ticker en el JSX

Busca `<GhostTicker ... />` y reemplázalo por el nuevo `<SmartGhostTicker ... />`. Ya no necesitas calcular `tickerPlayers` manualmente con datos aleatorios.

**Antes:**
```jsx
{/* GHOST TICKER */}
<GhostTicker 
    players={tickerPlayers} 
    onPlayerClick={handleSelectPlayer} 
/>
```

**Ahora:**
```jsx
{/* SMART GHOST TICKER (CONNECTED) */}
<SmartGhostTicker 
    matchId="test_match"
    playerMap={playerMap}
    onPlayerClick={handleSelectPlayer}
/>
```

**Nota:** Puedes eliminar la lógica de `tickerPlayers` y el `useMemo` que generaba datos aleatorios `(Math.random() ...)` en `App.jsx`, ya que `SmartGhostTicker` se encarga de todo.

---
¡Listo! Ahora el frontend respira los datos vivos del Ghost Engine. 👻⚽
