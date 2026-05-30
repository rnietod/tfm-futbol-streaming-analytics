// ============================================================
// MATCH STATS HUB — TypeScript Interfaces
// ============================================================

// --- TEAM-LEVEL STATS ---

export interface TeamTopStats {
  xG: number;
  totalShots: number;
  shotsOnTarget: number;
  possession: number; // 0-100
}

export interface TeamPassingStats {
  accuratePasses: number;
  passAccuracy: number; // 0-100
  progressivePasses: number;
}

export interface TeamDefenseStats {
  interceptions: number;
  tacklesWon: number;
  clearances: number;
}

export interface TeamStats {
  teamName: string;
  teamShort: string;
  topStats: TeamTopStats;
  passing: TeamPassingStats;
  defense: TeamDefenseStats;
}

// --- PLAYER-LEVEL STATS ---

export interface PlayerInfo {
  id: string;
  name: string;
  shortName: string;
  number: number;
  position: string;
  teamId: string;
}

export interface PlayerAggregatedStats {
  minutesPlayed: number;
  goals: number;
  assists: number;
  shots: number;
  shotsOnTarget: number;
  xG: number;
  passesCompleted: number;
  passAccuracy: number;
  keyPasses: number;
  progressivePasses: number;
  tackles: number;
  interceptions: number;
  duelsWon: number;
  aerialDuelsWon: number;
  touches: number;
  distanceCovered: number; // km
}

// --- PITCH VISUALIZATION DATA ---

/** Continuous tracking coordinate for heatmap rendering */
export interface TrackingPoint {
  x: number;       // 0-100 normalized
  y: number;       // 0-100 normalized
  intensity: number; // 0-1
}

/** Pass origin→destination for pass network */
export interface PassLink {
  from_x: number;  // 0-100
  from_y: number;  // 0-100
  to_x: number;    // 0-100
  to_y: number;    // 0-100
  count: number;   // number of passes on this link
  successful: boolean;
}

/** Individual ball touch event on pitch */
export interface TouchPoint {
  x: number;       // 0-100
  y: number;       // 0-100
  eventType: 'pass' | 'shot' | 'dribble' | 'cross' | 'tackle' | 'reception';
}

export interface PlayerPitchData {
  heatmap: TrackingPoint[];
  passNetwork: PassLink[];
  touchMap: TouchPoint[];
}

// --- COMPOSITE TYPES ---

export interface PlayerFullData {
  info: PlayerInfo;
  stats: PlayerAggregatedStats;
  pitchData: PlayerPitchData;
}

export interface MatchStatsData {
  teamA: TeamStats;
  teamB: TeamStats;
  rosterA: PlayerFullData[];
  rosterB: PlayerFullData[];
}
