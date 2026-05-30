import React, { useState, useMemo, useEffect } from 'react';
import { BarChart3, Users, User, ChevronRight, Loader2, WifiOff } from 'lucide-react';
import TeamComparison from './TeamComparison';
import PlayerDeepDive from './PlayerDeepDive';
import { useMatchStats, usePlayerPitchData } from '../../hooks/useMatchStats';

// ============================================================
// MATCH STATS TAB — Orquestador con datos reales
// ============================================================

const MATCH_ID = 'test_match';

// --- TOGGLE PILL ---
const TogglePill = ({ options, value, onChange, colorA, colorB }) => (
  <div className="inline-flex rounded-xl bg-zinc-900/80 border border-white/5 p-1 gap-0.5">
    {options.map((opt, idx) => {
      const isActive = value === opt.value;
      const color = idx === 0 ? (colorA || '#006FEE') : (colorB || '#f31260');
      return (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`
            px-4 py-1.5 rounded-lg text-[11px] font-bold uppercase tracking-wider
            transition-all duration-300 relative overflow-hidden
            ${isActive ? 'text-white shadow-lg' : 'text-zinc-500 hover:text-zinc-300'}
          `}
          style={isActive ? {
            backgroundColor: `color-mix(in srgb, ${color} 25%, #18181b)`,
            boxShadow: `0 0 20px color-mix(in srgb, ${color} 20%, transparent)`,
          } : {}}
        >
          {opt.icon && <opt.icon size={12} className="inline mr-1.5 -mt-0.5" />}
          {opt.label}
          {isActive && (
            <div
              className="absolute bottom-0 left-1/4 right-1/4 h-[2px] rounded-full"
              style={{ backgroundColor: color }}
            />
          )}
        </button>
      );
    })}
  </div>
);

// --- PLAYER CHIP ---
const PlayerChip = ({ player, isActive, onClick, teamColor }) => (
  <button
    onClick={onClick}
    className={`
      flex items-center gap-2 px-3 py-1.5 rounded-lg text-[11px] font-medium
      transition-all duration-300 border whitespace-nowrap flex-shrink-0
      ${isActive
        ? 'text-white border-white/20 shadow-lg'
        : 'text-zinc-500 border-transparent hover:text-zinc-300 hover:bg-white/5'
      }
    `}
    style={isActive ? {
      backgroundColor: `color-mix(in srgb, ${teamColor} 20%, #18181b)`,
      borderColor: `color-mix(in srgb, ${teamColor} 30%, transparent)`,
    } : {}}
  >
    <span
      className={`w-5 h-5 rounded-md flex items-center justify-center text-[9px] font-bold ${
        isActive ? 'text-white' : 'text-zinc-600'
      }`}
      style={isActive ? { backgroundColor: `color-mix(in srgb, ${teamColor} 40%, transparent)` } : { backgroundColor: 'rgba(255,255,255,0.05)' }}
    >
      {player.info.number}
    </span>
    {player.info.shortName}
    <span className="text-[8px] text-zinc-600 uppercase">{player.info.position}</span>
  </button>
);

// --- LOADING STATE ---
const LoadingState = () => (
  <div className="flex-1 flex items-center justify-center">
    <div className="text-center space-y-3">
      <Loader2 size={32} className="text-primary animate-spin mx-auto" />
      <p className="text-zinc-500 text-xs font-medium uppercase tracking-wider">Loading match stats...</p>
    </div>
  </div>
);

// --- ERROR STATE ---
const ErrorState = ({ error, onRetry }) => (
  <div className="flex-1 flex items-center justify-center">
    <div className="text-center space-y-3">
      <WifiOff size={32} className="text-zinc-700 mx-auto" />
      <p className="text-zinc-500 text-sm">Could not load match data</p>
      <p className="text-zinc-700 text-xs font-mono">{error}</p>
      <button
        onClick={onRetry}
        className="px-4 py-1.5 rounded-lg bg-primary/20 text-primary text-xs font-bold uppercase tracking-wider hover:bg-primary/30 transition-colors"
      >
        Retry
      </button>
    </div>
  </div>
);


// ============================================================
// MAIN ORCHESTRATOR
// ============================================================
const MatchStatsTab = ({ targetPlayer }) => {
  const [selectedTeam, setSelectedTeam] = useState('teamA');
  const [viewMode, setViewMode] = useState('overview');
  const [selectedPlayerId, setSelectedPlayerId] = useState(null);

  // Fetch real data from API
  const { statsData, isLoading, error, refetch } = useMatchStats(MATCH_ID);

  // Derive team data
  const teamA = statsData?.teamA || null;
  const teamB = statsData?.teamB || null;

  // Build roster from players dict grouped by team
  const { rosterA, rosterB } = useMemo(() => {
    if (!statsData?.players || !teamA || !teamB) return { rosterA: [], rosterB: [] };

    const a = [];
    const b = [];

    Object.values(statsData.players).forEach(p => {
      if (p.info.teamName === teamA.teamName) {
        a.push(p);
      } else if (p.info.teamName === teamB.teamName) {
        b.push(p);
      }
    });

    // Sort by position priority then number
    const posOrder = { GK: 0, RCB: 1, LCB: 1, RB: 2, LB: 2, CB: 1, RDM: 3, LDM: 3, RM: 4, LM: 4, CM: 4, RW: 5, LW: 5, CF: 6, RF: 6, LF: 6, ST: 6, SUB: 9 };
    const sort = (arr) => arr.sort((x, y) => (posOrder[x.info.position] ?? 7) - (posOrder[y.info.position] ?? 7) || x.info.number - y.info.number);

    return { rosterA: sort(a), rosterB: sort(b) };
  }, [statsData, teamA, teamB]);

  const activeRoster = selectedTeam === 'teamA' ? rosterA : rosterB;
  const teamColor = selectedTeam === 'teamA' ? '#006FEE' : '#f31260';
  const teamShortA = teamA?.teamShort || 'A';
  const teamShortB = teamB?.teamShort || 'B';

  // Handle incoming targetPlayer from Dashboard
  useEffect(() => {
    if (targetPlayer && statsData?.players) {
        const targetName = (targetPlayer.name || "").toLowerCase();
        const targetNum = targetPlayer.number;
        const targetLastName = targetName.split(' ').pop(); // e.g. "griezmann"
        
        let found = null;
        
        // 1. Try exact match by shortName or name
        found = Object.values(statsData.players).find(
            p => (p.info.shortName || "").toLowerCase() === targetName || 
                 (p.info.name || "").toLowerCase() === targetName
        );
        
        // 2. If not found, try fuzzy match (last name + number)
        if (!found) {
            found = Object.values(statsData.players).find(p => {
                const optaName = (p.info.name || "").toLowerCase();
                const optaNum = p.info.number;
                
                // Matches last name and has the exact same number
                if (targetNum && optaNum === targetNum && optaName.includes(targetLastName)) {
                    return true;
                }
                
                // Or just strongly matches the last name if no number is available
                if (optaName.includes(targetLastName) && targetLastName.length > 3) {
                    return true;
                }
                
                return false;
            });
        }
        
        // 3. Fallback: just number match (Risky, but better than nothing if names are totally different)
        if (!found && targetNum) {
             found = Object.values(statsData.players).find(p => p.info.number === targetNum);
        }
        
        if (found) {
            setViewMode('player');
            setSelectedPlayerId(found.info.id);
            if (found.info.teamName === teamA?.teamName) setSelectedTeam('teamA');
            else if (found.info.teamName === teamB?.teamName) setSelectedTeam('teamB');
        }
    }
  }, [targetPlayer, statsData, teamA, teamB]);

  // Fetch pitch data for selected player
  const { pitchData: livePitchData, isLoading: pitchLoading } = usePlayerPitchData(
    MATCH_ID,
    viewMode === 'player' ? selectedPlayerId : null
  );

  // Auto-select first player when switching to player mode
  const handleViewChange = (mode) => {
    setViewMode(mode);
    if (mode === 'player' && !selectedPlayerId && activeRoster.length > 0) {
      setSelectedPlayerId(activeRoster[0]?.info.id);
    }
  };

  // When switching teams in player mode, reset to first player
  const handleTeamChange = (team) => {
    setSelectedTeam(team);
    const roster = team === 'teamA' ? rosterA : rosterB;
    if (viewMode === 'player' && roster.length > 0) {
      setSelectedPlayerId(roster[0]?.info.id);
    }
  };

  // Find full player data (stats + pitch)
  const selectedPlayer = useMemo(() => {
    if (!selectedPlayerId || !statsData?.players) return null;
    const base = statsData.players[selectedPlayerId];
    if (!base) return null;

    return {
      ...base,
      pitchData: livePitchData || { heatmap: [], passNetwork: [], touchMap: [] },
    };
  }, [selectedPlayerId, statsData, livePitchData]);

  // --- Loading / Error / Empty states ---
  if (isLoading) return <LoadingState />;
  if (error) return <ErrorState error={error} onRetry={refetch} />;
  if (!teamA || !teamB) return <ErrorState error="No team data available" onRetry={refetch} />;

  return (
    <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
      {/* CONTROLS BAR */}
      <div className="flex items-center justify-between px-6 py-3 border-b border-white/5 bg-zinc-950/60 backdrop-blur-xl">
        <div className="flex items-center gap-4">
          {/* Team Selector */}
          <TogglePill
            options={[
              { value: 'teamA', label: teamShortA },
              { value: 'teamB', label: teamShortB },
            ]}
            value={selectedTeam}
            onChange={handleTeamChange}
            colorA="#006FEE"
            colorB="#f31260"
          />

          <ChevronRight size={14} className="text-zinc-700" />

          {/* View Mode Selector */}
          <TogglePill
            options={[
              { value: 'overview', label: 'Team Overview', icon: Users },
              { value: 'player', label: 'Player Deep Dive', icon: User },
            ]}
            value={viewMode}
            onChange={handleViewChange}
            colorA={teamColor}
            colorB={teamColor}
          />
        </div>

        {/* Live indicator */}
        <div className="flex items-center gap-2">
          <div className="w-1.5 h-1.5 rounded-full bg-success animate-pulse" />
          <span className="text-[10px] font-bold tracking-widest text-zinc-600 uppercase">
            Live · 5s refresh
          </span>
        </div>
      </div>

      {/* PLAYER SELECTOR (conditional) */}
      {viewMode === 'player' && (
        <div className="flex items-center gap-1 px-6 py-2 border-b border-white/5 bg-zinc-950/40 overflow-x-auto no-scrollbar">
          <span className="text-[9px] font-bold tracking-widest text-zinc-700 uppercase mr-2 flex-shrink-0">Squad</span>
          {activeRoster.map(player => (
            <PlayerChip
              key={player.info.id}
              player={player}
              isActive={selectedPlayerId === player.info.id}
              onClick={() => setSelectedPlayerId(player.info.id)}
              teamColor={teamColor}
            />
          ))}
        </div>
      )}

      {/* CONTENT AREA */}
      {viewMode === 'overview' ? (
        /* overview: no outer scroll — TeamComparison fills height with flex */
        <div className="flex-1 min-h-0 overflow-hidden p-4">
          <TeamComparison
            teamA={teamA}
            teamB={teamB}
            selectedTeam={selectedTeam}
          />
        </div>
      ) : (
        /* player deep dive: scrollable */
        <div className="flex-1 min-h-0 overflow-y-auto p-6 no-scrollbar">
          <PlayerDeepDive playerData={selectedPlayer} pitchLoading={pitchLoading} />
        </div>
      )}
    </div>
  );
};

export default MatchStatsTab;
