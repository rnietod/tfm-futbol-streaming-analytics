import React, { useMemo } from 'react';

// ============================================================
// PASSING NETWORK PITCH — VERTICAL (Portrait), Y-axis inverted
// ViewBox: 88 wide × 128 tall
// Pitch is flipped: attacking direction goes bottom → top
// ============================================================

const PAD = 9;
const PW  = 70;   // pitch width  (68m)
const PL  = 110;  // pitch length (105m)
const VB_W = PW + PAD * 2;  // 88
const VB_H = PL + PAD * 2;  // 128

// Proportional dimensions
const PA_W = PW * (40.32 / 68);
const PA_D = PL * (16.5 / 105);
const GA_W = PW * (18.32 / 68);
const GA_D = PL * (5.5 / 105);
const CC_R = PL * (9.15 / 105);
const PS_D = PL * (11 / 105);

// Convert normalised data coords (0-100 each) → vertical SVG viewBox coords.
// INVERTED: dataX=0 → bottom of SVG, dataX=100 → top of SVG
//           dataY=0 → left,          dataY=100 → right
const toVB = (dataX, dataY) => ({
  svgX: PAD + (dataY / 100) * PW,
  svgY: PAD + PL - (dataX / 100) * PL,   // ← inverted Y
});

// --- VERTICAL PITCH MARKINGS ---
const PitchMarkings = () => {
  const cx = PAD + PW / 2;
  const cy = PAD + PL / 2;
  return (
    <g stroke="rgba(255,255,255,0.38)" strokeWidth="0.55" fill="none">
      <rect x={PAD} y={PAD} width={PW} height={PL} rx="0.5" />
      <line x1={PAD} y1={cy} x2={PAD + PW} y2={cy} />
      <circle cx={cx} cy={cy} r={CC_R} />
      <circle cx={cx} cy={cy} r={0.7} fill="rgba(255,255,255,0.28)" />
      {/* TOP goal area */}
      <rect x={PAD + (PW - PA_W) / 2} y={PAD}             width={PA_W} height={PA_D} />
      <rect x={PAD + (PW - GA_W) / 2} y={PAD}             width={GA_W} height={GA_D} />
      <circle cx={cx} cy={PAD + PS_D} r={0.5} fill="rgba(255,255,255,0.22)" />
      {/* BOTTOM goal area */}
      <rect x={PAD + (PW - PA_W) / 2} y={PAD + PL - PA_D} width={PA_W} height={PA_D} />
      <rect x={PAD + (PW - GA_W) / 2} y={PAD + PL - GA_D} width={GA_W} height={GA_D} />
      <circle cx={cx} cy={PAD + PL - PS_D} r={0.5} fill="rgba(255,255,255,0.22)" />
      {/* Corner arcs */}
      <path d={`M ${PAD+3} ${PAD} A 3 3 0 0 0 ${PAD} ${PAD+3}`} />
      <path d={`M ${PAD+PW-3} ${PAD} A 3 3 0 0 1 ${PAD+PW} ${PAD+3}`} />
      <path d={`M ${PAD} ${PAD+PL-3} A 3 3 0 0 0 ${PAD+3} ${PAD+PL}`} />
      <path d={`M ${PAD+PW} ${PAD+PL-3} A 3 3 0 0 1 ${PAD+PW-3} ${PAD+PL}`} />
      {/* Goals */}
      <rect x={PAD+(PW-GA_W)/2} y={PAD-3}    width={GA_W} height={3} strokeOpacity="0.4" />
      <rect x={PAD+(PW-GA_W)/2} y={PAD+PL}   width={GA_W} height={3} strokeOpacity="0.4" />
    </g>
  );
};

// ============================================================
const PassingNetworkPitch = ({ teamA, teamB, selectedTeam, analysisMode = 'passing', dataType = 'eventing' }) => {
  const data      = selectedTeam === 'teamA' ? teamA : teamB;
  const teamColor = selectedTeam === 'teamA' ? '#006FEE' : '#f31260';

  const passNetwork = data?.passNetwork || [];
  const eventPositions = data?.averagePositions || [];
  const trackingPositions = data?.averagePositionsTracking || [];

  const averagePositions = useMemo(() => {
    if (dataType !== 'tracking') return eventPositions;
    return eventPositions.map(ep => {
      // Robust multi-criteria matching (jersey number, ID, or substring) because:
      // 1. player_id in eventing is null, but present in tracking.
      // 2. name formats differ (eventing has full names, tracking has short names).
      const tp = trackingPositions.find(t => {
        const numMatch = t.number !== null && ep.number !== null && Number(t.number) === Number(ep.number);
        const idMatch = t.player_id && ep.player_id && String(t.player_id) === String(ep.player_id);
        const nameMatch = t.name && ep.name && (
          ep.name.toLowerCase().includes(t.name.toLowerCase()) || 
          t.name.toLowerCase().includes(ep.name.toLowerCase())
        );
        return numMatch || idMatch || nameMatch;
      });
      if (tp) {
        return { ...ep, x: tp.x, y: tp.y };
      }
      return ep;
    });
  }, [dataType, eventPositions, trackingPositions]);

  const hasData = passNetwork.length > 0 && averagePositions.length > 0;

  const maxPasses = useMemo(() => {
    if (!passNetwork.length) return 1;
    return Math.max(...passNetwork.map(p => p.count), 1);
  }, [passNetwork]);

  const scaledPositions = useMemo(() =>
    averagePositions.map(p => ({ ...p, ...toVB(p.x, p.y) })),
  [averagePositions]);

  return (
    <div className="w-full h-full flex flex-col">
      <div
        className="relative flex-1 min-h-0 rounded-xl overflow-hidden shadow-2xl"
        style={{ border: '1px solid rgba(0,242,255,0.28)', boxShadow: '0 0 28px rgba(0,0,0,0.55)' }}
      >
        <svg
          viewBox={`0 0 ${VB_W} ${VB_H}`}
          preserveAspectRatio="xMidYMid meet"
          className="w-full h-full"
          style={{ backgroundImage: 'url("/custom-pitch.jpg")', backgroundSize: 'cover', backgroundPosition: 'center' }}
        >
          <PitchMarkings />

          {analysisMode === 'passing' && hasData && (
            <>
              {passNetwork.map((pass, i) => {
                const from = scaledPositions.find(p => p.name === pass.from_name);
                const to   = scaledPositions.find(p => p.name === pass.to_name);
                if (!from || !to || pass.count <= 1) return null;

                const ratio       = pass.count / maxPasses;
                // Cap thickness at 1.8 and keep opacity low (max 0.55)
                const strokeWidth = Math.min(ratio * 2.2 + 0.25, 1.8);
                const opacity     = Math.min(ratio * 0.45 + 0.15, 0.55);

                return (
                  <line
                    key={`pass-${i}`}
                    x1={from.svgX} y1={from.svgY}
                    x2={to.svgX}   y2={to.svgY}
                    stroke={teamColor}
                    strokeWidth={strokeWidth}
                    strokeOpacity={opacity}
                    strokeLinecap="round"
                  />
                );
              })}

              {scaledPositions.map((player, idx) => {
                const nodeKey = player.player_id && player.player_id !== 'null' ? player.player_id : (player.name || idx);
                return (
                  <g key={`node-${nodeKey}`}>
                  <circle cx={player.svgX} cy={player.svgY} r="5" fill={teamColor} fillOpacity="0.12" />
                  <circle cx={player.svgX} cy={player.svgY} r="3.8"
                    fill="rgba(0,0,0,0.88)" stroke={teamColor} strokeWidth="0.65" />
                  <text
                    x={player.svgX} y={player.svgY + 1.4}
                    fontSize="3.4" fill="white" fontWeight="bold"
                    textAnchor="middle" dominantBaseline="middle"
                    style={{ fontFamily: 'Inter, system-ui, sans-serif' }}
                  >
                    {player.number ?? '?'}
                  </text>
                </g>
                );
              })}
            </>
          )}

          {!hasData && (
            <text
              x={VB_W / 2} y={VB_H / 2}
              fontSize="4" fill="rgba(0,242,255,0.4)"
              textAnchor="middle" dominantBaseline="middle"
              fontWeight="600" style={{ fontFamily: 'monospace' }}
            >
              AWAITING DATA...
            </text>
          )}
        </svg>

        {/* Neon border pulse */}
        <svg width="100%" height="100%" className="absolute top-0 left-0 pointer-events-none">
          <rect x="0" y="0" width="100%" height="100%"
            fill="none" stroke="rgba(0,242,255,0.07)" strokeWidth="4" />
        </svg>
      </div>
    </div>
  );
};

export default PassingNetworkPitch;
