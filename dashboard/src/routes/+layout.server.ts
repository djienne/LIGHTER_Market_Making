import { getControlPlaneSnapshot } from '$lib/server/control-plane';

export async function load() {
	const snapshot = await getControlPlaneSnapshot();

	return {
		summary: {
			activeStrategies: snapshot.derived.activeStrategies,
			runningChallengers: snapshot.derived.runningChallengers,
			pendingRequests: snapshot.derived.pendingRequests,
			totalCapitalUsd: snapshot.derived.totalCapitalUsd,
			riskScore: snapshot.derived.riskScore,
			healthStatus: snapshot.derived.healthStatus,
			serviceActive: snapshot.live.serviceActive,
			liveSource: snapshot.live.source,
			cacheAgeSeconds: snapshot.live.cacheAgeSeconds,
			autopilotMode: snapshot.metrics.autopilotMode
		}
	};
}
