export interface GhostPlayer {
    player_id: string;
    ghost_deviation: number;
    trend: 'up' | 'down' | 'stable';
}

export interface GhostTickerResponse {
    match_minute: number;
    status: 'active' | 'calibrating' | 'paused' | 'ended';
    players: GhostPlayer[];
}

export interface MetricData {
    live: number;
    historical: number;
    deviation: number;
    importance: number;
}

export interface PlayerMetrics {
    [key: string]: MetricData | any;
    percentiles?: {
        [key: string]: number;
    };
}

export interface FullProfileResponse {
    player_id: string;
    position_group: string;
    metrics: PlayerMetrics;
}
