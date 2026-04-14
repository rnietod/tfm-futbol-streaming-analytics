import { GhostTickerResponse, FullProfileResponse } from '../types/ghost';

const API_BASE_URL = 'http://localhost:8000'; // Or generic/env

export const ghostService = {
    /**
     * Fetches the lightweight ghost ticker data (deviation signals).
     */
    getGhostTicker: async (matchId: string): Promise<GhostTickerResponse> => {
        const response = await fetch(`${API_BASE_URL}/match/${matchId}/ghost-ticker`);
        if (!response.ok) {
            throw new Error(`Error fetching ghost ticker: ${response.statusText}`);
        }
        return response.json();
    },

    /**
     * Fetches the heavy full profile for a specific player (on-click).
     */
    getPlayerFullProfile: async (matchId: string, playerId: string): Promise<FullProfileResponse> => {
        const response = await fetch(`${API_BASE_URL}/match/${matchId}/player/${playerId}/full-profile`);
        if (!response.ok) {
            throw new Error(`Error fetching full profile: ${response.statusText}`);
        }
        return response.json();
    }
};
