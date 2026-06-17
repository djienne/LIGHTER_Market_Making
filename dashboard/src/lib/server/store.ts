import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

import type { ActionRequest, ChallengerRun, ControlPlaneState, MarketMakerStrategy } from '$lib/types';

function repoRoot() {
	return process.cwd().endsWith('/dashboard') ? resolve(process.cwd(), '..') : process.cwd();
}

const statePath = resolve(repoRoot(), '.runtime/mm-control-state.json');

function nowIso() {
	return new Date().toISOString();
}

function seedStrategies(): MarketMakerStrategy[] {
	return [
		{
			id: 'btc-mm-canary',
			pair: 'BTC',
			name: 'BTC market maker canary',
			phase: 'canary_live',
			status: 'running',
			role: 'champion',
			serviceName: 'lighter-mm-btc.service',
			host: '51.83.253.195',
			accountIndex: '281474976538716',
			configPath: 'config.json',
			startedAt: nowIso(),
			capitalUsd: 100,
			maxPositionUsd: 64,
			leverage: 1,
			parameters: {
				levels_per_side: 1,
				capital_usage_percent: 0.08,
				vol_to_half_spread: 48,
				min_half_spread_bps: 4,
				skew: 1.5,
				c1_ticks: 120,
				quote_update_threshold_bps: 12,
				quota_recovery: false
			},
			score: null,
			monthlyReturnPct: null,
			drawdownPct: null,
			adverseSelectionBps: null,
			fillCount: 0,
			notes: 'Premier canary BTC live. Le but est de valider execution, fills, inventory et stabilite avant de scaler.'
		}
	];
}

function seedChallengers(): ChallengerRun[] {
	return [
		{
			id: 'eth-mm-grid-72h',
			pair: 'ETH',
			name: 'ETH wide-spread grid',
			status: 'planned',
			horizonHours: 72,
			slots: 175,
			bestScore: null,
			bestConfigLabel: null,
			progressPct: 0,
			pnlUsd: null,
			fills: 0,
			adverseSelectionBps: null,
			recommendation: 'wait'
		},
		{
			id: 'btc-mm-challenger-tight',
			pair: 'BTC',
			name: 'BTC tighter alpha challenger',
			status: 'planned',
			horizonHours: 24,
			slots: 96,
			bestScore: null,
			bestConfigLabel: null,
			progressPct: 0,
			pnlUsd: null,
			fills: 0,
			adverseSelectionBps: null,
			recommendation: 'wait'
		},
		{
			id: 'sol-mm-grid-72h',
			pair: 'SOL',
			name: 'SOL liquidity probe',
			status: 'planned',
			horizonHours: 72,
			slots: 125,
			bestScore: null,
			bestConfigLabel: null,
			progressPct: 0,
			pnlUsd: null,
			fills: 0,
			adverseSelectionBps: null,
			recommendation: 'wait'
		}
	];
}

function seedState(): ControlPlaneState {
	return {
		version: 1,
		updatedAt: nowIso(),
		strategies: seedStrategies(),
		challengers: seedChallengers(),
		actionRequests: [],
		metrics: {
			last24hPnlUsd: 0,
			last24hFills: 0,
			bestPair: null,
			autopilotMode: 'research'
		}
	};
}

export async function loadState(): Promise<ControlPlaneState> {
	try {
		const raw = await readFile(statePath, 'utf8');
		return JSON.parse(raw) as ControlPlaneState;
	} catch {
		const state = seedState();
		await saveState(state);
		return state;
	}
}

export async function saveState(state: ControlPlaneState) {
	await mkdir(dirname(statePath), { recursive: true });
	await writeFile(statePath, JSON.stringify({ ...state, updatedAt: nowIso() }, null, 2));
}

export async function updateState(mutator: (state: ControlPlaneState) => void | Promise<void>) {
	const state = await loadState();
	await mutator(state);
	await saveState(state);
	return state;
}

export function createActionRequest(input: Omit<ActionRequest, 'id' | 'createdAt' | 'status'>): ActionRequest {
	const compact = Math.random().toString(36).slice(2, 8);
	return {
		...input,
		id: `req_${Date.now()}_${compact}`,
		createdAt: nowIso(),
		status: 'pending'
	};
}
