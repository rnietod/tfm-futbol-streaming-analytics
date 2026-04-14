import { useState, useEffect, useRef } from 'react';
import { ghostService } from '../services/ghostService';
import { GhostTickerResponse } from '../types/ghost';

interface UseGhostTickerResult {
    data: GhostTickerResponse | null;
    isLoading: boolean;
    error: Error | null;
}

export const useGhostTicker = (matchId: string): UseGhostTickerResult => {
    const [data, setData] = useState<GhostTickerResponse | null>(null);
    const [isLoading, setIsLoading] = useState<boolean>(true);
    const [error, setError] = useState<Error | null>(null);

    useEffect(() => {
        let isMounted = true;

        const fetchData = async () => {
            try {
                const result = await ghostService.getGhostTicker(matchId);
                if (isMounted) {
                    setData(result);
                    setError(null);
                }
            } catch (err: any) {
                if (isMounted) {
                    setError(err);
                }
            } finally {
                if (isMounted) {
                    setIsLoading(false);
                }
            }
        };

        // Initial fetch
        fetchData();

        // Polling interval (5 seconds)
        const intervalId = setInterval(fetchData, 5000);

        return () => {
            isMounted = false;
            clearInterval(intervalId);
        };
    }, [matchId]);

    return { data, isLoading, error };
};
