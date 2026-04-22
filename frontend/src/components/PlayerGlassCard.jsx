import React, { useState, useEffect, useMemo } from 'react';
import { Card, CardBody, CardFooter, Avatar, Button, Chip } from "@nextui-org/react";
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, ResponsiveContainer } from 'recharts';
import { X, TrendingUp, Shield } from 'lucide-react';

// MOCK DATA (Estático por ahora, listo para recibir props reales luego)
const MOCK_STATS = [
  { metric: "PAC", value: 88, fullMark: 100 }, // Pace
  { metric: "SHO", value: 82, fullMark: 100 }, // Shooting
  { metric: "PAS", value: 91, fullMark: 100 }, // Passing
  { metric: "DRI", value: 87, fullMark: 100 }, // Dribbling
  { metric: "DEF", value: 45, fullMark: 100 }, // Defense
  { metric: "PHY", value: 70, fullMark: 100 }, // Physical
];

const PlayerGlassCard = ({ player, homeGoals, awayGoals, homeTeamId, onClose }) => {
  const [statsData, setStatsData] = useState(MOCK_STATS);
  const [loading, setLoading] = useState(true);

  // Compute Game State
  const gameState = useMemo(() => {
     if (!player) return "Drawing";
     const isHome = player.team_id === homeTeamId;
     if (isHome) {
         if (homeGoals > awayGoals) return "Winning";
         if (homeGoals < awayGoals) return "Losing";
     } else {
         if (awayGoals > homeGoals) return "Winning";
         if (awayGoals < homeGoals) return "Losing";
     }
     return "Drawing";
  }, [player, homeGoals, awayGoals, homeTeamId]);

  // Fetch real stats
  useEffect(() => {
     if (!player) return;
     setLoading(true);
     fetch(`http://127.0.0.1:8000/match/test_match/player/${player.id}/profile?state=${gameState}`)
        .then(res => res.json())
        .then(data => {
            if (data.stats) {
                setStatsData(data.stats);
            } else {
                setStatsData(MOCK_STATS); // fallback
            }
        })
        .catch(e => {
            console.error("Error fetching player profile:", e);
            setStatsData(MOCK_STATS);
        })
        .finally(() => setLoading(false));
  }, [player, gameState]);

  // Si no hay jugador seleccionado, no renderizamos nada (o null)
  if (!player) return null;

  return (
    <div className="relative group">
        {/* Efecto de "Glow" trasero ambiental */}
        <div className="absolute -inset-1 bg-gradient-to-r from-primary/50 to-purple-600/50 rounded-[2rem] blur-2xl opacity-40 group-hover:opacity-60 transition duration-1000"></div>

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
                        <h2 className="text-2xl font-bold text-white tracking-tight flex items-center justify-center gap-2">
                            {player.name || "J. ÁLVAREZ"}
                            <span className="text-sm font-mono text-primary border border-primary/30 px-1 rounded bg-primary/10">
                                #{player.number || "00"}
                            </span>
                        </h2>
                        <span className="text-xs text-zinc-400 uppercase tracking-[0.2em] font-bold">
                            {player.team_name || "Unknown Team"}
                        </span>
                    </div>

                    {/* Badges Rápidas */}
                    <div className="flex gap-2 mt-3">
                        <Chip size="sm" variant="flat" color="success" startContent={<TrendingUp size={12}/>} classNames={{content: "font-bold"}}>8.4 Form</Chip>
                        <Chip size="sm" variant="flat" color="warning" startContent={<Shield size={12}/>} classNames={{content: "font-bold"}}>MVP</Chip>
                    </div>
                </div>

                {/* BODY: Radar Chart */}
                <div className="w-full h-[220px] relative -ml-2">
                    <ResponsiveContainer width="100%" height="100%">
                        <RadarChart cx="50%" cy="50%" outerRadius="70%" data={statsData}>
                            <PolarGrid stroke="#3f3f46" strokeDasharray="3 3" />
                            <PolarAngleAxis 
                                dataKey="metric" 
                                tick={{ fill: '#a1a1aa', fontSize: 10, fontWeight: 'bold' }} 
                            />
                            <Radar
                                name={player.name}
                                dataKey="value"
                                stroke="#006FEE"
                                strokeWidth={2}
                                fill="#006FEE"
                                fillOpacity={0.4}
                            />
                        </RadarChart>
                    </ResponsiveContainer>
                    
                    {/* Valoración General en el centro del Radar */}
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-center pointer-events-none">
                        <span className="text-3xl font-bold text-white drop-shadow-md">
                            {Math.round(statsData.reduce((acc, curr) => acc + curr.value, 0) / statsData.length)}
                        </span>
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