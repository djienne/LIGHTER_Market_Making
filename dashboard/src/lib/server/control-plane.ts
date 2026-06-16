import type { ControlPlaneSnapshot, LiveSnapshot, StrategyStatus, TelemetryReachability } from '$lib/types';

import { getLiveSnapshot } from './vps';
import { loadState } from './store';

function deriveTelemetry(live: LiveSnapshot): {
	reachability: TelemetryReachability;
	summary: string;
	operatorAction: string;
} {
	if (live.source === 'live') {
		return {
			reachability: 'reachable',
			summary: 'VPS telemetry refresh is live.',
			operatorAction: 'No operator action required.'
		};
	}

	if (live.source === 'cache') {
		return {
			reachability: 'degraded',
			summary:
				live.cacheAgeSeconds !== null
					? `Dashboard is serving cached VPS telemetry (${live.cacheAgeSeconds}s old).`
					: 'Dashboard is serving cached VPS telemetry.',
			operatorAction: live.fetchError
				? 'Restore SSH/API access to the VPS before trusting the live service view.'
				: 'Verify the next refresh returns to live telemetry before making operational decisions.'
		};
	}

	return {
		reachability: 'unreachable',
		summary: 'Control plane cannot reach live VPS telemetry.',
		operatorAction: live.fetchError
			? 'Fix SSH/API access or inspect the VPS manually before acting on service health.'
			: 'Inspect the VPS manually before making any live operational change.'
	};
}

export async function getControlPlaneSnapshot(options: { forceLive?: boolean } = {}): Promise<ControlPlaneSnapshot> {
	const [state, live] = await Promise.all([loadState(), getLiveSnapshot({ force: options.forceLive })]);
	const liveStatus: StrategyStatus = live.serviceActive ? 'running' : 'failed';
	const telemetry = deriveTelemetry(live);
	const strategies = state.strategies.map((strategy) => {
		if (strategy.id !== 'btc-mm-canary') return strategy;

		return {
			...strategy,
			status: liveStatus,
			lastCheckedAt: live.checkedAt,
			fillCount: Math.max(
				strategy.fillCount,
				live.orders.reduce(
					(sum, order) => sum + (order.initialBaseAmount > order.remainingBaseAmount ? 1 : 0),
					0
				)
			)
		};
	});
	const pendingRequests = state.actionRequests.filter((request) => request.status === 'pending').length;
	const activeStrategies = strategies.filter((strategy) => strategy.status === 'running').length;
	const runningChallengers = state.challengers.filter((run) => run.status === 'running').length;
	const totalCapitalUsd = strategies
		.filter((strategy) => strategy.status !== 'stopped' && strategy.phase !== 'retired')
		.reduce((sum, strategy) => sum + strategy.capitalUsd, 0);
	const hasCriticalErrors = live.recentErrors.length > 0 || live.health.incidentCounts.critical > 0;
	const orderPenalty = live.orders.length > 2 ? 30 : 0;
	const warningPenalty = Math.min(20, live.health.incidentCounts.warning * 8);
	const criticalPenalty = Math.min(40, live.health.incidentCounts.critical * 20);
	const riskScore = Math.max(
		0,
		100 - orderPenalty - warningPenalty - criticalPenalty - (hasCriticalErrors ? 10 : 0) - (live.serviceActive ? 0 : 30)
	);

	return {
		...state,
		strategies,
		live,
		derived: {
			activeStrategies,
			runningChallengers,
			pendingRequests,
			totalCapitalUsd,
			riskScore,
			healthStatus: live.health.status,
			telemetry
		}
	};
}
