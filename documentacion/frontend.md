# Frontend — Estructura y sistema de diseño

App **React 18 + Vite** en `frontend/`. UI con **NextUI/HeroUI** + **Tailwind**,
gráficos con **recharts** y **SVG a mano** (D3 en el stack, poco usado aún).
Dev en `http://127.0.0.1:5173`. El build (`frontend/dist/`) está **trackeado** en git.

## Arranque y navegación

- `src/main.jsx` monta `App`.
- `src/App.jsx` es la raíz: gestiona las pestañas con `activeTab`
  (`dashboard | matchstats | dofa | vision`), abre el WebSocket
  (`/ws/match/test_match`), y corre el **game-loop de replay** (rAF + refs,
  batcheando estado para evitar el antiguo tirón "robótico").

## Pestañas y componentes principales

### Dashboard
Layout en grid de 12 columnas: **feed de eventos** (izq) · **campo + marcador +
controles** (centro) · **panel de comparación** (der, al seleccionar jugador).
Arriba, el **Ghost Ticker**.

| Componente | Rol |
| :--- | :--- |
| `GhostTicker.jsx` | Marquee infinito con la desviación σ por jugador; filtro local/visitante; pausa al hover. |
| `PlayerHoverCard.jsx` | Mini-tarjeta al pasar el ratón (foto + σ + radar esperado vs real); render con `createPortal`. |
| `PlayerAvatar.jsx` | Foto del jugador (proxy GCS) con fallback a iniciales. |
| `FootballPitch.jsx` | Campo SVG con los 22 jugadores, evento destacado y capas tácticas. |
| `PlayerMarker.jsx` | Marcador individual de jugador en el campo. |
| `pitch/PitchLayerMenu.jsx`, `VoronoiOverlay.jsx`, `PitchControlOverlay.jsx` | Capas tácticas (Voronoi / Pitch Control). |
| `PlayerComparisonPanel.jsx`, `PlayerGlassCard.jsx` | Panel de comparación de jugador. |
| `EventFeed` (en `App.jsx`) | Feed de eventos clickable (salta el replay a la jugada). |
| `DynamicBackground.jsx`, `TactixLogo.jsx` | Fondo animado y logo. |

### Match Stats (`components/matchstats/`)
`MatchStatsTab.jsx` orquesta: `TeamComparison`, `ShotMapViz`, `MomentumChart`,
`PassingNetworkPitch`, `AccuratePassesViz`, `PhysicalMetricsViz`,
`PlayerDeepDive`, `PlayerPitchAnalytics`. Estado de error/empty con botón RETRY.

### DOFA (`components/dofa/`)
`DofaTab.jsx`: matriz SWOT (4 cuadrantes), XI previsto sobre el campo, goles por
minuto, top goleadores/asistentes, zonas de tiro y heatmap, con selector de equipo.

### Gemini Vision
*Placeholder* "Coming Soon" (en `App.jsx`).

## Librería interna (`src/lib/`)
- `replayBuffer.js` — chunking/prefetch/dedup de frames del replay (con tests).
- `eventReplay.js` — mapeo evento → instante objetivo del replay (con tests).
- `pitchModels.js` — modelos geométricos del campo (con tests).
- `utils/playerImage.js` — URL de la foto (proxy backend) + iniciales de fallback.

## Sistema de diseño

Definido en `tailwind.config.js` (tema NextUI **dark**) e `index.css`.

| Token | Valor | Uso |
| :--- | :--- | :--- |
| `background` | `#09090b` (zinc-950) | Fondo (+ textura `cyber-bg.png` con overlay). |
| `foreground` | `#ECEDEE` | Texto. |
| `primary` | `#006FEE` | Azul de acción / equipo **local**. |
| `danger` | `#f31260` | Rojo/magenta de alerta / equipo **visitante**. |
| `success` | `#17c964` | Datos positivos / punto "live". |
| Fuente datos | **JetBrains Mono** | Números, tiempos, métricas. |
| Fuente UI | **Inter** | Texto general. |

Utilidades en `index.css`: `.tactix-glass`, `.tactix-card` (*glassmorphism*:
`bg-zinc-900/60 backdrop-blur border border-white/5`), `.text-muted`, `.no-scrollbar`.
Radios NextUI: 4/8/12px (small/medium/large).

### Notas de diseño (de la auditoría)
- **Desktop-first:** el campo es fijo (1150×720, escalado por `transform`) y el grid
  no tiene breakpoints → desborda en móvil/tablet.
- **`danger`** sirve a la vez de "visitante" y "error" (colisión semántica; valorar
  un token `away` propio).
- Tipografías muy pequeñas (`text-[9px]/[10px]`) con `zinc-500/600` → contraste bajo.
- `.cyber-scrollbar` (en `App.css`) está definido pero sin usar (CSS muerto menor).
- Bundle JS ~1 MB sin code-splitting → candidato a `React.lazy` por pestaña.
