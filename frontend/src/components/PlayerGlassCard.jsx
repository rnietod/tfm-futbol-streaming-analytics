import React, { useState, useEffect, useMemo } from 'react';
import { Card, CardBody, CardFooter, Avatar, Button, Chip, Spinner } from "@nextui-org/react";
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, ResponsiveContainer } from 'recharts';
import { X, TrendingUp, Shield } from 'lucide-react';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// Fallback visual si el backend no contesta a tiempo
const FALLBACK_STATS = [
    { metric: "PAC", expected: 50, projected: 0, fullMark: 100 },
    { metric: "SHO", expected: 50, projected: 0, fullMark: 100 },
    { metric: "PAS", expected: 50, projected: 0, fullMark: 100 },
    { metric: "DRI", expected: 50, projected: 0, fullMark: 100 },
    { metric: "DEF", expected: 50, projected: 0, fullMark: 100 },
    { metric: "PHY", expected: 50, projected: 0, fullMark: 100 },
];

const PlayerGlassCard = ({ player, matchScore, matchInfo, isTracked = true, onClose }) => {
    const [radarData, setRadarData] = useState(FALLBACK_STATS);

    // 1. Deducir si el jugador va Ganando, Empatando o Perdiendo
    const gameState = useMemo(() => {
        if (!matchScore || !matchInfo || !player) return 'Drawing';

        let homeG = matchScore.home;
        let awayG = matchScore.away;

        if (player.team_name === matchInfo.home_team_name) {
            if (homeG > awayG) return 'Winning';
            if (homeG < awayG) return 'Losing';
            return 'Drawing';
        } else if (player.team_name === matchInfo.away_team_name) {
            if (awayG > homeG) return 'Winning';
            if (awayG < homeG) return 'Losing';
            return 'Drawing';
        }
        return 'Drawing';
    }, [matchScore, matchInfo, player]);

    // 2. Traer el Perfil Fantasma Real
    useEffect(() => {
        if (!player || !(player.id || player.player_id)) {
            return;
        }

        const fetchGhostProfile = async () => {
            try {
                const playerId = player.id || player.player_id;
                const res = await fetch(`${API_BASE_URL}/player/${playerId}/ghost_profile?game_state=${gameState}`);
                if (!res.ok) throw new Error("API Falla");

                const data = await res.json();

                // Si la BD aún está procesando Cold Start o falla, fallback
                if (data.status === 'processing' || data.error || !data.player_type) {
                    setRadarData(FALLBACK_STATS);
                } else {
                    // Mapeo Dinámico según Tipo de Jugador (Field vs GK)
                    const formatPct = (val, isProj = false) => {
                        if (isProj && (data.minutes_played == null || data.minutes_played < 10)) {
                            return 0; // Ocultar si van menos de 10 min
                        }
                        return val == null ? 0 : Math.round(val * 100);
                    };

                    if (data.player_type === 'Goalkeeper') {
                        setRadarData([
                            { metric: "SAV", expected: formatPct(data.pct_saves), projected: formatPct(data.proj_pct_saves, true), fullMark: 100 },
                            { metric: "DIS", expected: formatPct(data.pct_distribution), projected: formatPct(data.proj_pct_distribution, true), fullMark: 100 },
                            { metric: "REF", expected: formatPct(data.pct_saves), projected: formatPct(data.proj_pct_saves, true), fullMark: 100 }, // Reflex aprox
                            { metric: "POS", expected: 50, projected: formatPct(50, true), fullMark: 100 }, // Posicionamiento mock
                            { metric: "SPD", expected: 50, projected: formatPct(50, true), fullMark: 100 }, // Velocidad mock
                            { metric: "KIC", expected: formatPct(data.pct_distribution), projected: formatPct(data.proj_pct_distribution, true), fullMark: 100 }, // Kicking
                        ]);
                    } else {
                        setRadarData([
                            { metric: "SHO", expected: formatPct(data.pct_shots), projected: formatPct(data.proj_pct_shots, true), fullMark: 100 },
                            { metric: "PAS", expected: formatPct(data.pct_creation), projected: formatPct(data.proj_pct_creation, true), fullMark: 100 },
                            { metric: "DRI", expected: formatPct(data.pct_progression), projected: formatPct(data.proj_pct_progression, true), fullMark: 100 }, // Progression aprox dribble
                            { metric: "DEF", expected: formatPct(data.pct_defense), projected: formatPct(data.proj_pct_defense, true), fullMark: 100 },
                            { metric: "PHY", expected: formatPct(data.pct_workrate), projected: formatPct(data.proj_pct_workrate, true), fullMark: 100 }, // Workrate aprox phy
                            { metric: "PAC", expected: 75, projected: formatPct(0.75, true), fullMark: 100 }, // Pace (Not available directly yet)
                        ]);
                    }
                }
            } catch (error) {
                console.error("Error Obteniendo Radar DB:", error);
                setRadarData(FALLBACK_STATS);
            }
        };

        fetchGhostProfile();
    }, [player, gameState]);

    if (!player) return null;

    // Calcular OVR Ficticio basado en los stats del radar y la posición del jugador
    const calcOVR = () => {
        if (!isTracked) return 0;
        if (!radarData || radarData.length === 0) return 0;

        let ovr = 0;
        const group = player.position_group || 'Center'; // Default fallback

        const getVal = (m) => {
            const stat = radarData.find(d => d.metric === m);
            return stat ? stat.expected : 0;
        };

        if (group.includes('Attacker') || group.includes('Forward') || group === 'FWD') {
            // Delanteros: SHO 35%, DRI/PAC 40%, PAS 15%, PHY 10%
            ovr = getVal("SHO") * 0.35 + getVal("PAC") * 0.20 + getVal("DRI") * 0.20 + getVal("PAS") * 0.15 + getVal("PHY") * 0.10;
        } else if (group.includes('Midfield') || group === 'Center' || group === 'MID') {
            // Medios: PAS 30%, DRI 20%, DEF 15%, SHO 20%, PHY 15%
            ovr = getVal("PAS") * 0.30 + getVal("DRI") * 0.20 + getVal("DEF") * 0.15 + getVal("SHO") * 0.20 + getVal("PHY") * 0.15;
        } else if (group.includes('Defender') || group.includes('Back') || group === 'Defense' || group === 'DEF') {
            // Defensas: DEF 40%, PHY 25%, PAC 15%, PAS 15%, DRI 5%
            ovr = getVal("DEF") * 0.40 + getVal("PHY") * 0.25 + getVal("PAC") * 0.15 + getVal("PAS") * 0.15 + getVal("DRI") * 0.05;
        } else if (group === 'Goalkeeper' || group === 'GK' || group.includes('Goalkeeper')) {
            // Porteros: DEF (SAV/REF) 40%, PAS (DIS/KIC) 30%, POS 20%, SPD 10%
            const sav = getVal("SAV");
            const ref = getVal("REF");
            const dis = getVal("DIS");
            const kic = getVal("KIC");
            const pos = getVal("POS");
            const spd = getVal("SPD");
            ovr = ((sav + ref) / 2) * 0.40 + ((dis + kic) / 2) * 0.30 + pos * 0.20 + spd * 0.10;
        } else {
            // Fallback genérico: Promedio simple
            const sum = radarData.reduce((acc, curr) => acc + curr.expected, 0);
            ovr = sum / radarData.length;
        }

        return Math.round(ovr);
    }

    const displayRadarData = isTracked
        ? radarData
        : radarData.map(stat => ({ ...stat, projected: 0 }));

    return (
        <div className="relative group">
            {/* Efecto de "Glow" trasero ambiental */}
            <div className={`absolute -inset-1 rounded-[2rem] blur-2xl opacity-40 group-hover:opacity-60 transition duration-1000 ${gameState === 'Winning' ? 'bg-gradient-to-r from-success/50 to-green-600/50' :
                gameState === 'Losing' ? 'bg-gradient-to-r from-danger/50 to-red-600/50' :
                    'bg-gradient-to-r from-primary/50 to-purple-600/50'
                }`}></div>

            <Card className="w-[340px] bg-zinc-950/80 backdrop-blur-2xl border border-white/10 shadow-2xl rounded-[24px] overflow-visible">

                {/* Botón Cerrar Flotante */}
                <Button
                    isIconOnly
                    size="sm"
                    variant="light"
                    className="absolute top-3 right-3 z-50 text-zinc-400 hover:text-white bg-black/20 hover:bg-white/10 rounded-full"
                    onPress={onClose}
                >
                    <X size={18} />
                </Button>

                <CardBody className="p-0 flex flex-col items-center pt-8 pb-4 relative">

                    {/* HEADER: Avatar & Info */}
                    <div className="relative mb-6 flex flex-col items-center">
                        {/* Anillo de energía rotando sutilmente */}
                        <div className="absolute inset-0 rounded-full border border-dashed border-primary/30 w-24 h-24 -m-2 animate-[spin_10s_linear_infinite]" />

                        <Avatar
                            src="https://i.pravatar.cc/150?u=a042581f4e29026704d" // Placeholder
                            className="w-20 h-20 text-large border-2 border-primary shadow-[0_0_20px_rgba(0,111,238,0.4)]"
                        />

                        <div className="mt-4 text-center">
                            <h2 className={`text-2xl font-bold tracking-tight flex items-center justify-center gap-2 ${isTracked ? 'text-white' : 'text-warning'}`}>
                                {player.name || "JUGADOR"}
                                <span className="text-sm font-mono text-primary border border-primary/30 px-1 rounded bg-primary/10">
                                    #{player.number || "00"}
                                </span>
                            </h2>
                            <span className="text-xs text-zinc-400 uppercase tracking-[0.2em] font-bold">
                                {player.team_name || "EQUIPO"} • {gameState.toUpperCase()}
                            </span>
                        </div>
                    </div>

                    {/* BODY: Radar Chart */}
                    <div className="w-full h-[220px] relative -ml-2 flex items-center justify-center">
                        <ResponsiveContainer width="100%" height="100%">
                            <RadarChart cx="50%" cy="50%" outerRadius="70%" data={displayRadarData}>
                                <PolarGrid stroke="#3f3f46" strokeDasharray="3 3" />
                                <PolarAngleAxis
                                    dataKey="metric"
                                    tick={{ fill: '#a1a1aa', fontSize: 10, fontWeight: 'bold' }}
                                />
                                {/* Radar Esperado - BigQuery (Azul Claro) */}
                                <Radar
                                    name="BigQuery"
                                    dataKey="expected"
                                    stroke="#38bdf8"
                                    strokeWidth={2}
                                    fill="#38bdf8"
                                    fillOpacity={0.25}
                                />
                                {/* Radar Proyectado - PostgreSQL (Rojo) */}
                                <Radar
                                    name="PostgreSQL"
                                    dataKey="projected"
                                    stroke="#f31260"
                                    strokeWidth={3}
                                    fill="#f31260"
                                    fillOpacity={0.35}
                                />
                            </RadarChart>
                        </ResponsiveContainer>

                        {/* Valoración General en el centro del Radar */}
                        <div className="absolute top-[48%] left-1/2 -translate-x-1/2 -translate-y-1/2 text-center pointer-events-none">
                            <span className="text-3xl font-bold text-white drop-shadow-md">{calcOVR()}</span>
                            <div className="text-[8px] text-zinc-500 uppercase font-bold">OVR</div>
                        </div>
                    </div>

                </CardBody>

                <CardFooter className="px-6 pb-6 pt-0">
                    <Button
                        className="w-full bg-white/5 hover:bg-white/10 text-white border border-white/10 shadow-lg group"
                        variant="flat"
                        size="lg"
                    >
                        <span className="group-hover:text-primary transition-colors">VER ANÁLISIS COMPLETO</span>
                    </Button>
                </CardFooter>
            </Card>
        </div>
    );
};

export default PlayerGlassCard;
