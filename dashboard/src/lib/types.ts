export type StrategyPhase = 'candidate' | 'challenger_dry_run' | 'canary_live' | 'champion_live' | 'retired';
export type StrategyStatus = 'planned' | 'queued' | 'running' | 'paused' | 'stopped' | 'failed' | 'unknown';
export type RequestStatus = 'pending' | 'approved' | 'rejected' | 'applied';
export type RequestType = 'promote_challenger' | 'increase_capital' | 'start_canary' | 'retire_strategy';

export type MarketMakerStrategy = {
	id: string;
	pair: string;
	name: string;
	phase: StrategyPhase;
	status: StrategyStatus;
	role: 'champion' | 'challenger' | 'candidate';
	serviceName?: string;
	host?: string;
	accountIndex?: string;
	configPath?: string;
	startedAt?: string;
	lastCheckedAt?: string;
	capitalUsd: number;
	maxPositionUsd: number;
	leverage: number;
	parameters: Record<string, number | string | boolean>;
	score: number | null;
	monthlyReturnPct: number | null;
	drawdownPct: number | null;
	adverseSelectionBps: number | null;
	fillCount: number;
	notes: string;
};

export type ChallengerRun = {
	id: string;
	pair: string;
	name: string;
	status: StrategyStatus;
	startedAt?: string;
	endsAt?: string;
	horizonHours: number;
	slots: number;
	bestScore: number | null;
	bestConfigLabel: string | null;
	progressPct: number;
	pnlUsd: number | null;
	fills: number;
	adverseSelectionBps: number | null;
	recommendation: 'wait' | 'candidate' | 'reject' | 'promote';
};

export type ActionRequest = {
	id: string;
	type: RequestType;
	status: RequestStatus;
	title: string;
	pair: string;
	createdAt: string;
	strategyId?: string;
	challengerId?: string;
	rationale: string;
	riskNotes: string;
	payload: Record<string, string | number | boolean | null>;
};

export type LiveOrder = {
	orderIndex: string;
	clientOrderIndex: string;
	side: 'buy' | 'sell';
	price: number;
	remainingBaseAmount: number;
	initialBaseAmount: number;
	timeInForce: string;
	status: string;
	updatedAt?: string;
};

export type LiveHealthSeverity = 'info' | 'warning' | 'critical';

export type LiveHealthIncident = {
	id: string;
	severity: LiveHealthSeverity;
	title: string;
	detail: string;
	value?: string;
	detectedAt?: string;
};

export type LiveHealthStreamState = {
	id: 'user-stats' | 'account-data' | 'account-orders';
	label: string;
	status: 'healthy' | 'degraded' | 'critical';
	lastSeenAt?: string;
	lastPayloadAt?: string;
	lastSubscriptionAt?: string;
	freshnessSource: 'payload' | 'subscription' | 'none';
	staleForSeconds: number | null;
	reconnects: number;
};

export type LiveHealthSummary = {
	status: 'healthy' | 'degraded' | 'critical';
	watchdogReconnects: number;
	maxSanityLatencyMs: number | null;
	maxSanityDiffBps: number | null;
	staleForSeconds: number;
	streams: LiveHealthStreamState[];
	incidentCounts: {
		warning: number;
		critical: number;
	};
	incidents: LiveHealthIncident[];
};

export type LiveTelemetrySource = 'live' | 'cache' | 'unavailable';

export type TelemetryReachability = 'reachable' | 'degraded' | 'unreachable';

export type LiveServiceDetails = {
	activeState: string;
	subState: string;
	mainPid: number | null;
	result: string | null;
	execMainStatus: number | null;
	restartCount: number | null;
	activeEnterTimestamp?: string;
};

export type LiveQualityMetrics = {
	updated_at?: string;
	window_seconds?: number;
	fills?: {
		count?: number;
		buy?: number;
		sell?: number;
		volume_usd?: number;
	};
	pnl?: {
		realized_usd?: number;
		pnl_bps_on_volume?: number;
		portfolio_value_usd?: number | null;
		available_capital_usd?: number | null;
	};
	spread_capture_bps_avg?: number;
	markouts?: Record<string, { count?: number; avg_bps?: number; adverse_avg_bps?: number }>;
	inventory?: {
		position_size?: number;
		position_value_usd?: number;
		avg_inventory_usd?: number;
		avg_boundary_ratio?: number;
		max_position_usd?: number | null;
	};
	quality?: {
		score?: number;
		adjustment?: {
			spread_multiplier?: number;
			size_multiplier?: number;
			adverse_bps?: number;
			sample_count?: number;
			reason?: string;
		};
	};
};

export type LiveSnapshot = {
	ok: boolean;
	checkedAt: string;
	source: LiveTelemetrySource;
	host: string;
	serviceActive: boolean;
	serviceStatus: string;
	serviceDetails: LiveServiceDetails;
	memoryMb: number | null;
	cpuPct: number | null;
	positionSize: number | null;
	portfolioValueUsd: number | null;
	availableCapitalUsd: number | null;
	orders: LiveOrder[];
	recentWarnings: string[];
	recentErrors: string[];
	rawLogTail: string[];
	liveMetrics: LiveQualityMetrics | null;
	liveState: Record<string, unknown> | null;
	health: LiveHealthSummary;
	message?: string;
	cacheAgeSeconds: number | null;
	fetchError?: string;
};

export type ControlPlaneState = {
	version: number;
	updatedAt: string;
	strategies: MarketMakerStrategy[];
	challengers: ChallengerRun[];
	actionRequests: ActionRequest[];
	metrics: {
		last24hPnlUsd: number;
		last24hFills: number;
		bestPair: string | null;
		autopilotMode: 'observe' | 'research' | 'promote-with-approval';
	};
};

export type ControlPlaneSnapshot = ControlPlaneState & {
	live: LiveSnapshot;
	derived: {
		activeStrategies: number;
		runningChallengers: number;
		pendingRequests: number;
		totalCapitalUsd: number;
		riskScore: number;
		healthStatus: LiveHealthSummary['status'];
		telemetry: {
			reachability: TelemetryReachability;
			summary: string;
			operatorAction: string;
		};
	};
};
