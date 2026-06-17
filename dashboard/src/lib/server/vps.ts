import { execFile } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { promisify } from 'node:util';

import type {
	LiveHealthIncident,
	LiveServiceDetails,
	LiveHealthStreamState,
	LiveHealthSummary,
	LiveOrder,
	LiveSnapshot
} from '$lib/types';

const execFileAsync = promisify(execFile);

const DEFAULT_HOST = '51.83.253.195';
const DEFAULT_USER = 'ubuntu';
const DEFAULT_REMOTE_DIR = '/home/ubuntu/LIGHTER_Market_Making';
const DEFAULT_SERVICE = 'lighter-mm-btc.service';
const DEFAULT_ACCOUNT_INDEX = '281474976538716';
const DEFAULT_MARKET_ID = '1';
const LIVE_CACHE_TTL_MS = 30_000;
const LIVE_STALE_TTL_MS = 10 * 60_000;
const ACCOUNT_STREAM_SPECS = [
	{
		id: 'user-stats',
		label: 'user_stats',
		incidentLabel: 'user stats',
		watchdogTitle: 'User stats WebSocket reconnecting repeatedly',
		watchdog: /user stats websocket watchdog triggered/i,
		payloadEvidence: [/Received user stats for account/i],
		subscriptionEvidence: [/Subscribed to user_stats\//i],
		warningAfterSeconds: 3_600,
		criticalAfterSeconds: 7_200
	},
	{
		id: 'account-data',
		label: 'account_all',
		incidentLabel: 'account data',
		watchdogTitle: 'Account data WebSocket reconnecting repeatedly',
		watchdog: /account data websocket watchdog triggered/i,
		payloadEvidence: [/WebSocket position update for market/i],
		subscriptionEvidence: [/Subscribed to account_all\//i],
		warningAfterSeconds: 3_600,
		criticalAfterSeconds: 7_200
	},
	{
		id: 'account-orders',
		label: 'account_orders',
		incidentLabel: 'account orders',
		watchdogTitle: 'Account orders WebSocket reconnecting repeatedly',
		watchdog: /account orders websocket watchdog triggered/i,
		payloadEvidence: [/account_orders WS ready \(snapshot\)/i],
		subscriptionEvidence: [/Subscribed to account_orders\//i],
		warningAfterSeconds: 1_800,
		criticalAfterSeconds: 3_600
	}
] as const;

let liveCache: { expiresAt: number; snapshot: LiveSnapshot } | null = null;
let refreshInFlight: Promise<LiveSnapshot> | null = null;

function env(name: string, fallback: string) {
	return process.env[name] || fallback;
}

function repoRoot() {
	return process.cwd().endsWith('/dashboard') ? resolve(process.cwd(), '..') : process.cwd();
}

function cachePath() {
	return resolve(repoRoot(), '.runtime', 'mm-live-snapshot.json');
}

function emptySnapshot(message: string): LiveSnapshot {
	return {
		ok: false,
		checkedAt: new Date().toISOString(),
		source: 'unavailable',
		host: env('MM_VPS_HOST', DEFAULT_HOST),
		serviceActive: false,
		serviceStatus: 'unknown',
		serviceDetails: {
			activeState: 'unknown',
			subState: 'unknown',
			mainPid: null,
			result: null,
			execMainStatus: null,
			restartCount: null
		},
		memoryMb: null,
		cpuPct: null,
		positionSize: null,
		portfolioValueUsd: null,
		availableCapitalUsd: null,
		orders: [],
		recentWarnings: [],
		recentErrors: [],
		rawLogTail: [],
		liveMetrics: null,
		liveState: null,
		health: {
			status: 'critical',
			watchdogReconnects: 0,
			maxSanityLatencyMs: null,
			maxSanityDiffBps: null,
			staleForSeconds: 0,
			streams: [],
			incidentCounts: {
				warning: 0,
				critical: 1
			},
			incidents: [
				{
					id: 'snapshot-unavailable',
					severity: 'critical',
					title: 'Live snapshot unavailable',
					detail: message
				}
			]
		},
		message,
		cacheAgeSeconds: null,
		fetchError: message
	};
}

function parseServiceProperties(raw: string): LiveServiceDetails {
	const entries = raw
		.split('|')
		.map((line) => line.trim())
		.filter(Boolean)
		.map((line) => {
			const separator = line.indexOf('=');
			return separator === -1 ? null : [line.slice(0, separator), line.slice(separator + 1)] as const;
		})
		.filter((entry): entry is readonly [string, string] => entry !== null);

	const props = Object.fromEntries(entries);
	const activeEnterTimestamp = props.ActiveEnterTimestamp;
	const parsedActiveEnterMs = activeEnterTimestamp ? Date.parse(activeEnterTimestamp) : Number.NaN;

	return {
		activeState: props.ActiveState || 'unknown',
		subState: props.SubState || 'unknown',
		mainPid: parseNumber(props.MainPID),
		result: props.Result && props.Result !== 'success' ? props.Result : props.Result || null,
		execMainStatus: parseNumber(props.ExecMainStatus),
		restartCount: parseNumber(props.NRestarts),
		activeEnterTimestamp:
			activeEnterTimestamp && activeEnterTimestamp !== 'n/a' && Number.isFinite(parsedActiveEnterMs)
				? new Date(parsedActiveEnterMs).toISOString()
				: undefined
	};
}

function withHealthIncidents(health: LiveHealthSummary, extraIncidents: LiveHealthIncident[]): LiveHealthSummary {
	const incidents = [...health.incidents];
	for (const incident of extraIncidents) {
		if (!incidents.some((existing) => existing.id === incident.id)) {
			incidents.push(incident);
		}
	}

	const critical = incidents.filter((incident) => incident.severity === 'critical').length;
	const warning = incidents.filter((incident) => incident.severity === 'warning').length;

	return {
		...health,
		status: critical > 0 ? 'critical' : warning > 0 ? 'degraded' : 'healthy',
		incidentCounts: {
			warning,
			critical
		},
		incidents
	};
}

function classifyFetchError(error: string) {
	if (/permission denied \(publickey\)/i.test(error)) {
		return {
			id: 'ssh-auth-failed',
			title: 'VPS SSH authentication failed',
			detail: 'The control plane could not authenticate to the VPS, so live telemetry could not be refreshed.'
		};
	}

	if (/connect timeout|operation timed out|timed out/i.test(error)) {
		return {
			id: 'ssh-timeout',
			title: 'VPS telemetry refresh timed out',
			detail: 'The control plane timed out while trying to refresh live telemetry from the VPS.'
		};
	}

	return {
		id: 'telemetry-refresh-failed',
		title: 'VPS telemetry refresh failed',
		detail: 'The control plane could not refresh live telemetry from the VPS.'
	};
}

function withCacheFallback(snapshot: LiveSnapshot, ageMs: number, fetchError?: string): LiveSnapshot {
	const cacheAgeSeconds = Math.max(0, Math.round(ageMs / 1000));
	const refreshIncident = fetchError
		? (() => {
				const classified = classifyFetchError(fetchError);
				return {
					id: classified.id,
					severity: cacheAgeSeconds >= 300 ? 'critical' : 'warning',
					title: classified.title,
					detail: `${classified.detail} Cached telemetry is being shown instead.`,
					value: `${cacheAgeSeconds}s old`
				} satisfies LiveHealthIncident;
			})()
		: ({
				id: 'cached-telemetry',
				severity: cacheAgeSeconds >= 120 ? 'warning' : 'info',
				title: 'Showing cached VPS telemetry',
				detail: 'The dashboard is serving the most recent cached live snapshot while a background refresh runs.',
				value: `${cacheAgeSeconds}s old`
			} satisfies LiveHealthIncident);

	return {
		...snapshot,
		source: 'cache',
		cacheAgeSeconds,
		fetchError: fetchError ?? snapshot.fetchError,
		message: fetchError ? `Using cached telemetry: ${fetchError}` : snapshot.message,
		health: withHealthIncidents(snapshot.health, [refreshIncident])
	};
}

async function ssh(script: string, timeout = 20_000) {
	const user = env('MM_VPS_USER', DEFAULT_USER);
	const host = env('MM_VPS_HOST', DEFAULT_HOST);
	const target = `${user}@${host}`;
	const { stdout, stderr } = await execFileAsync(
		'ssh',
		['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', target, script],
		{ timeout, maxBuffer: 1024 * 1024 * 4 }
	);

	return `${stdout}${stderr ? `\n${stderr}` : ''}`;
}

function parseNumber(value: unknown) {
	const number = Number(value);
	return Number.isFinite(number) ? number : null;
}

function normaliseOrder(order: Record<string, unknown>): LiveOrder {
	return {
		orderIndex: String(order.order_index ?? order.order_id ?? ''),
		clientOrderIndex: String(order.client_order_index ?? order.client_order_id ?? ''),
		side: order.is_ask ? 'sell' : 'buy',
		price: Number(order.price ?? 0),
		remainingBaseAmount: Number(order.remaining_base_amount ?? 0),
		initialBaseAmount: Number(order.initial_base_amount ?? 0),
		timeInForce: String(order.time_in_force ?? ''),
		status: String(order.status ?? ''),
		updatedAt: order.updated_at ? String(order.updated_at) : undefined
	};
}

function isBenignDustCloseLine(line: string) {
	if (/emergency_close .*invalid order base or quote amount/i.test(line)) return true;
	if (/failed to close small startup position .*continuing .*skew/i.test(line)) return true;
	return /0\.000010/.test(line) && /(emergency_close|open position detected|detected open position)/i.test(line);
}

function isBenignStaleCreateLine(line: string) {
	return /Dropping stale create (?:buy|sell)\[\d+\]: slot already has order \d+/i.test(line);
}

function isErrorLogLine(line: string) {
	if (isBenignDustCloseLine(line)) return false;
	if (isBenignStaleCreateLine(line)) return false;
	if (/error|traceback|reject/i.test(line)) return true;
	return /(?:status|http|code|api|response)[^0-9]*(429|503)|(429|503)[^a-z]*(status|http|code|api|response)/i.test(
		line
	);
}

function parseLatestPortfolioMetrics(logs: string[]) {
	let availableCapitalUsd: number | null = null;
	let portfolioValueUsd: number | null = null;
	const pattern = /Available Capital=\$(\-?\d+(?:\.\d+)?),\s*Portfolio Value=\$(\-?\d+(?:\.\d+)?)/i;

	for (let index = logs.length - 1; index >= 0; index -= 1) {
		const match = logs[index]?.match(pattern);
		if (!match) continue;
		availableCapitalUsd = Number(match[1]);
		portfolioValueUsd = Number(match[2]);
		break;
	}

	return { availableCapitalUsd, portfolioValueUsd };
}

function parseLatestPositionSize(logs: string[]) {
	const pattern = /WebSocket position update for market\s+\d+:\s+[^\s]+\s+->\s+([^\s]+)/i;

	for (let index = logs.length - 1; index >= 0; index -= 1) {
		const match = logs[index]?.match(pattern);
		if (!match) continue;
		const parsed = Number(match[1]);
		if (Number.isFinite(parsed)) return parsed;
	}

	return null;
}

function parseLogTimestamp(line: string) {
	const isoMatch = line.match(/(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/);
	if (isoMatch) {
		const parsed = Date.parse(`${isoMatch[1]}T${isoMatch[2]}Z`);
		return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
	}

	const monthMap: Record<string, string> = {
		Jan: '01',
		Feb: '02',
		Mar: '03',
		Apr: '04',
		May: '05',
		Jun: '06',
		Jul: '07',
		Aug: '08',
		Sep: '09',
		Oct: '10',
		Nov: '11',
		Dec: '12'
	};
	const shortMatch = line.match(/^([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})/);
	if (!shortMatch) return null;
	const month = monthMap[shortMatch[1]];
	if (!month) return null;
	const year = new Date().getUTCFullYear();
	const day = shortMatch[2].padStart(2, '0');
	const parsed = Date.parse(`${year}-${month}-${day}T${shortMatch[3]}Z`);
	return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

function lastDetectedAt(lines: string[]) {
	for (let index = lines.length - 1; index >= 0; index -= 1) {
		const detectedAt = parseLogTimestamp(lines[index] ?? '');
		if (detectedAt) return detectedAt;
	}

	return undefined;
}

function secondsSince(checkedAt: string, lastSeenAt?: string) {
	if (!lastSeenAt) return null;
	return Math.max(0, Math.round((Date.parse(checkedAt) - Date.parse(lastSeenAt)) / 1000));
}

function hasRecoveryAfter(logs: string[], lineIndex: number, recoveryPatterns: readonly RegExp[]) {
	for (let index = lineIndex + 1; index < logs.length; index += 1) {
		const line = logs[index] ?? '';
		if (recoveryPatterns.some((pattern) => pattern.test(line))) return true;
	}
	return false;
}

function watchdogLinesFor(
	logs: string[],
	spec: (typeof ACCOUNT_STREAM_SPECS)[number],
	options: { recovered: boolean }
) {
	return logs.filter((line, index) => {
		if (!spec.watchdog.test(line)) return false;
		const recovered = hasRecoveryAfter(logs, index, spec.subscriptionEvidence);
		return options.recovered ? recovered : !recovered;
	});
}

function recoveredAccountWatchdogLines(logs: string[]) {
	return ACCOUNT_STREAM_SPECS.flatMap((spec) => watchdogLinesFor(logs, spec, { recovered: true }));
}

function unresolvedAccountWatchdogLines(logs: string[]) {
	return ACCOUNT_STREAM_SPECS.flatMap((spec) => watchdogLinesFor(logs, spec, { recovered: false }));
}

function parseStreamStates(logs: string[], checkedAt: string): LiveHealthStreamState[] {
	return ACCOUNT_STREAM_SPECS.map((spec) => {
		const unresolvedWatchdogs = watchdogLinesFor(logs, spec, { recovered: false });
		const payloadLines = logs.filter((line) => spec.payloadEvidence.some((pattern) => pattern.test(line)));
		const subscriptionLines = logs.filter((line) => spec.subscriptionEvidence.some((pattern) => pattern.test(line)));
		const lastPayloadAt = lastDetectedAt(payloadLines);
		const lastSubscriptionAt = lastDetectedAt(subscriptionLines);
		const lastSeenAt = lastPayloadAt ?? lastSubscriptionAt;
		const staleForSeconds = secondsSince(checkedAt, lastSeenAt);
		const freshnessSource = lastPayloadAt ? 'payload' : lastSubscriptionAt ? 'subscription' : 'none';
		const status =
			unresolvedWatchdogs.length >= 2 || (staleForSeconds !== null && staleForSeconds >= spec.criticalAfterSeconds)
				? 'critical'
				: unresolvedWatchdogs.length === 1 || (staleForSeconds !== null && staleForSeconds >= spec.warningAfterSeconds)
					? 'degraded'
					: 'healthy';

		return {
			id: spec.id,
			label: spec.label,
			status,
			lastSeenAt,
			lastPayloadAt,
			lastSubscriptionAt,
			freshnessSource,
			staleForSeconds,
			reconnects: unresolvedWatchdogs.length
		};
	});
}

function parseHealth(
	logs: string[],
	checkedAt: string,
	serviceActive: boolean,
	serviceDetails: LiveServiceDetails,
	serviceMessage?: string
) {
	const incidents: LiveHealthIncident[] = [];
	const watchdogLines = unresolvedAccountWatchdogLines(logs);
	const latencySamples = logs
		.map((line) => {
			const match = line.match(/latency=(\d+)ms/i);
			return match ? Number(match[1]) : null;
		})
		.filter((value): value is number => value !== null);
	const elevatedLatencySamples = latencySamples.filter((value) => value >= 750);
	const sanityDiffSamples = logs
		.flatMap((line) => {
			const match = line.match(/bid_diff=([0-9.]+)%\s+ask_diff=([0-9.]+)%/i);
			if (!match) return [];
			return [Number(match[1]) * 100, Number(match[2]) * 100];
		})
		.filter((value) => Number.isFinite(value));
	const latestLogAt = [...logs]
		.reverse()
		.map(parseLogTimestamp)
		.find((value) => value !== null);
	const staleForSeconds = latestLogAt
		? Math.max(0, Math.round((Date.parse(checkedAt) - Date.parse(latestLogAt)) / 1000))
		: 0;
	const streams = parseStreamStates(logs, checkedAt);
	const maxSanityLatencyMs =
		latencySamples.length > 0 ? latencySamples.reduce((max, value) => Math.max(max, value), 0) : null;
	const maxSanityDiffBps =
		sanityDiffSamples.length > 0 ? sanityDiffSamples.reduce((max, value) => Math.max(max, value), 0) : null;

	if (!serviceActive) {
		incidents.push({
			id: 'service-down',
			severity: 'critical',
			title: 'Systemd service offline',
			detail: 'The live BTC canary service is not active on the VPS.'
		});
	}

	if (serviceDetails.subState === 'auto-restart') {
		incidents.push({
			id: 'service-auto-restart',
			severity: 'critical',
			title: 'Systemd service is auto-restarting',
			detail: 'The live BTC canary service is currently in systemd auto-restart rather than a stable running state.'
		});
	}

	if (serviceActive && serviceDetails.mainPid === 0) {
		incidents.push({
			id: 'service-missing-main-pid',
			severity: 'critical',
			title: 'Systemd reports no live main PID',
			detail: 'The service is marked active but systemd is not tracking a main process.'
		});
	}

	if (serviceDetails.result && serviceDetails.result !== 'success') {
		incidents.push({
			id: 'service-result',
			severity: serviceDetails.result === 'exit-code' ? 'critical' : 'warning',
			title: 'Systemd recorded a non-success service result',
			detail: `The last systemd result for the live BTC canary service was ${serviceDetails.result}.`,
			value:
				serviceDetails.execMainStatus !== null ? `exit status ${serviceDetails.execMainStatus}` : serviceDetails.result
		});
	}

	if ((serviceDetails.restartCount ?? 0) >= 1) {
		incidents.push({
			id: 'service-restarts',
			severity: (serviceDetails.restartCount ?? 0) >= 3 ? 'critical' : 'warning',
			title: 'Systemd restart count is non-zero',
			detail: 'The live BTC canary service has restarted under systemd supervision.',
			value: `${serviceDetails.restartCount} restart(s)`,
			detectedAt: serviceDetails.activeEnterTimestamp
		});
	}

	if (watchdogLines.length >= 2) {
		incidents.push({
			id: 'watchdog-reconnects',
			severity: watchdogLines.length >= 4 ? 'critical' : 'warning',
			title: 'Unrecovered WebSocket watchdog reconnects',
			detail: `${watchdogLines.length} watchdog reconnect(s) did not show a later subscription recovery in the recent log window.`,
			value: `${watchdogLines.length} events`,
			detectedAt: parseLogTimestamp(watchdogLines[watchdogLines.length - 1]) ?? undefined
		});
	}

	for (const stream of ACCOUNT_STREAM_SPECS) {
		const matchingLines = watchdogLinesFor(logs, stream, { recovered: false });
		if (matchingLines.length < 2) continue;

		incidents.push({
			id: `${stream.id}-watchdog`,
			severity: matchingLines.length >= 3 ? 'critical' : 'warning',
			title: stream.watchdogTitle,
			detail: `${matchingLines.length} ${stream.incidentLabel} watchdog reconnect(s) did not recover cleanly in the current telemetry window.`,
			value: `${matchingLines.length} reconnects`,
			detectedAt: lastDetectedAt(matchingLines)
		});
	}

	for (const stream of streams) {
		if (stream.status === 'healthy') continue;

		incidents.push({
			id: `${stream.id}-freshness`,
			severity: stream.status === 'critical' ? 'critical' : 'warning',
			title:
				stream.status === 'critical'
					? `${stream.label} stream missing or stale`
					: `${stream.label} stream needs attention`,
			detail:
				stream.lastSeenAt === undefined
					? `No recent ${stream.label} stream evidence was found in the current telemetry window.`
					: `${stream.label} last showed activity ${secondsSince(checkedAt, stream.lastSeenAt)}s before the latest control-plane check.`,
			value:
				stream.staleForSeconds === null
					? `${stream.reconnects} reconnects`
					: `${stream.staleForSeconds}s lag · ${stream.reconnects} reconnects`,
			detectedAt: stream.lastSeenAt
		});
	}

	for (const stream of streams) {
		if (stream.freshnessSource !== 'subscription' || stream.reconnects === 0) continue;

		incidents.push({
			id: `${stream.id}-subscription-only`,
			severity: 'warning',
			title: `${stream.label} freshness is reconnect-driven`,
			detail: `The latest ${stream.label} evidence is only a subscription/reconnect event, not a payload update. The stream may be recovering without delivering useful data yet.`,
			value: stream.staleForSeconds === null ? `${stream.reconnects} reconnects` : `${stream.staleForSeconds}s lag`,
			detectedAt: stream.lastSubscriptionAt
		});
	}

	if (maxSanityLatencyMs !== null && maxSanityLatencyMs >= 1_000) {
		incidents.push({
			id: 'sanity-latency',
			severity: maxSanityLatencyMs >= 2_500 ? 'critical' : 'warning',
			title: 'Orderbook sanity latency elevated',
			detail: 'REST fallback checks are taking longer than the nominal control-plane target.',
			value: `${maxSanityLatencyMs} ms`
		});
	}

	if (elevatedLatencySamples.length >= 3) {
		incidents.push({
			id: 'sanity-latency-recurring',
			severity: elevatedLatencySamples.length >= 6 && Math.max(...elevatedLatencySamples) >= 1_500 ? 'critical' : 'warning',
			title: 'REST sanity latency elevated repeatedly',
			detail: `${elevatedLatencySamples.length} sanity checks reached at least 750 ms in the recent telemetry window.`,
			value: `max ${Math.max(...elevatedLatencySamples)} ms`,
			detectedAt: lastDetectedAt(logs.filter((line) => /latency=(7[5-9]\d|[89]\d\d|\d{4,})ms/i.test(line)))
		});
	}

	if (maxSanityDiffBps !== null && maxSanityDiffBps >= 5) {
		incidents.push({
			id: 'sanity-diff',
			severity: maxSanityDiffBps >= 10 ? 'critical' : 'warning',
			title: 'Orderbook sanity drift observed',
			detail: 'Observed bid/ask divergence between REST fallback and WebSocket book.',
			value: `${maxSanityDiffBps.toFixed(2)} bps`
		});
	}

	if (staleForSeconds >= 120) {
		incidents.push({
			id: 'stale-logs',
			severity: staleForSeconds >= 300 ? 'critical' : 'warning',
			title: 'Live telemetry looks stale',
			detail: 'Recent logs are older than expected relative to the dashboard refresh time.',
			value: `${staleForSeconds}s lag`,
			detectedAt: latestLogAt ?? undefined
		});
	}

	if (serviceMessage) {
		incidents.push({
			id: 'query-warning',
			severity: 'warning',
			title: 'Exchange query warning',
			detail: serviceMessage
		});
	}

	const criticalCount = incidents.filter((incident) => incident.severity === 'critical').length;
	const warningCount = incidents.filter((incident) => incident.severity === 'warning').length;
	const status = criticalCount > 0 ? 'critical' : warningCount > 0 ? 'degraded' : 'healthy';

	return {
		status,
		watchdogReconnects: watchdogLines.length,
		maxSanityLatencyMs,
		maxSanityDiffBps,
		staleForSeconds,
		streams,
		incidentCounts: {
			warning: warningCount,
			critical: criticalCount
		},
		incidents
	} as const;
}

async function readCachedSnapshotWithAge(options: { allowStale?: boolean } = {}) {
	const now = Date.now();
	if (liveCache) {
		const ageMs = Math.max(0, now - Date.parse(liveCache.snapshot.checkedAt));
		if (options.allowStale || ageMs < LIVE_STALE_TTL_MS) {
			return { snapshot: liveCache.snapshot, ageMs };
		}
	}

	try {
		const raw = await readFile(cachePath(), 'utf8');
		const snapshot = JSON.parse(raw) as LiveSnapshot;
		const checkedAt = Date.parse(snapshot.checkedAt);
		if (!Number.isFinite(checkedAt)) return null;
		const ageMs = Math.max(0, now - checkedAt);
		if (options.allowStale || ageMs < LIVE_STALE_TTL_MS) {
			return { snapshot, ageMs };
		}
	} catch {
		// No usable cache yet.
	}

	return null;
}

async function writeCachedSnapshot(snapshot: LiveSnapshot) {
	liveCache = { expiresAt: Date.now() + LIVE_CACHE_TTL_MS, snapshot };
	try {
		await mkdir(resolve(repoRoot(), '.runtime'), { recursive: true });
		await writeFile(cachePath(), JSON.stringify(snapshot, null, 2));
	} catch {
		// Cache writes must never break the dashboard.
	}
}

async function fetchLiveSnapshot(): Promise<LiveSnapshot> {
	const remoteDir = env('MM_REMOTE_DIR', DEFAULT_REMOTE_DIR);
	const service = env('MM_SERVICE_NAME', DEFAULT_SERVICE);
	const accountIndex = env('MM_ACCOUNT_INDEX', DEFAULT_ACCOUNT_INDEX);
	const marketId = env('MM_MARKET_ID', DEFAULT_MARKET_ID);

	const script = `
set -e
cd ${remoteDir}
SERVICE="${service}"
ACCOUNT_INDEX="${accountIndex}"
MARKET_ID="${marketId}"
ACTIVE=$(systemctl is-active "$SERVICE" || true)
STATUS=$(systemctl show "$SERVICE" --property=ActiveState,SubState,MainPID,Result,ExecMainStatus,NRestarts,ActiveEnterTimestamp --no-pager 2>/dev/null | tr '\\n' '|' || true)
PID=$(systemctl show -p MainPID --value "$SERVICE" 2>/dev/null || echo 0)
PROC_JSON=$(python3 - "$PID" <<'PY'
import json, os, sys
pid = sys.argv[1]
out = {"cpuPct": None, "memoryMb": None}
if pid and pid != "0":
    try:
        import subprocess
        raw = subprocess.check_output(["ps", "-o", "%cpu=,rss=", "-p", pid], text=True).strip()
        if raw:
            cpu, rss = raw.split()[:2]
            out = {"cpuPct": float(cpu), "memoryMb": round(float(rss) / 1024, 1)}
    except Exception:
        pass
print(json.dumps(out))
PY
)
LOGS=$(journalctl -u "$SERVICE" --since "40 minutes ago" -o short-iso --no-pager | tail -n 160 | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().splitlines()))')
LIVE_FILES=$(python3 - <<'PY'
import json, os
def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None
print(json.dumps({
    "metrics": load("logs/live_metrics_BTC.json"),
    "state": load("logs/live_state_BTC.json"),
}))
PY
)
ORDERS_JSON=$(set +e; . venv/bin/activate >/dev/null 2>&1 && set -a && . ./.env >/dev/null 2>&1 && set +a && LIGHTER_MM_CONFIG="${remoteDir}/config.json" python - <<'PY'
import asyncio, json, os, requests
import market_maker_v2 as mm

async def main():
    out = {"orders": [], "positionSize": None, "portfolioValueUsd": None, "availableCapitalUsd": None, "error": None}
    client = None
    try:
        client = mm.build_signer_client(mm.BASE_URL, int(os.environ["ACCOUNT_INDEX"]), os.environ["API_KEY_PRIVATE_KEY"], int(os.environ["API_KEY_INDEX"]))
        auth = mm._generate_ws_auth_token(client)
        r = requests.get(
            f"{mm.BASE_URL}/api/v1/accountActiveOrders",
            params={"account_index": os.environ["ACCOUNT_INDEX"], "market_id": ${marketId}, "auth": auth},
            timeout=10,
        )
        r.raise_for_status()
        out["orders"] = r.json().get("orders", [])
    except Exception as exc:
        out["error"] = str(exc)
    finally:
        if client is not None:
            close = getattr(client, "close", None)
            if close:
                res = close()
                if hasattr(res, "__await__"):
                    await res
    print(json.dumps(out))

asyncio.run(main())
PY
)
python3 - <<PY
import json
print(json.dumps({
  "serviceActive": "$ACTIVE" == "active",
  "serviceStatus": "$STATUS",
  "process": json.loads("""$PROC_JSON"""),
  "logs": json.loads("""$LOGS"""),
  "liveFiles": json.loads("""$LIVE_FILES"""),
  "orders": json.loads("""$ORDERS_JSON""")
}))
PY
`;

	try {
		const output = await ssh(script, 25_000);
		const start = output.lastIndexOf('\n{');
		const jsonText = start >= 0 ? output.slice(start + 1) : output;
		const parsed = JSON.parse(jsonText) as {
			serviceActive: boolean;
			serviceStatus: string;
			process: { cpuPct: number | null; memoryMb: number | null };
			logs: string[];
			liveFiles?: { metrics?: LiveSnapshot['liveMetrics']; state?: Record<string, unknown> | null };
			orders: { orders?: Record<string, unknown>[]; error?: string | null };
		};
		const logs = parsed.logs ?? [];
		const benignWatchdogs = new Set(recoveredAccountWatchdogLines(logs));
		const recentWarnings = logs
			.filter(
				(line) =>
					/warning/i.test(line) &&
					!benignWatchdogs.has(line) &&
					!isBenignDustCloseLine(line) &&
					!isBenignStaleCreateLine(line)
			)
			.slice(-12);
		const recentErrors = logs.filter(isErrorLogLine).slice(-12);
		const orders = (parsed.orders.orders ?? []).map(normaliseOrder);
		const { availableCapitalUsd, portfolioValueUsd } = parseLatestPortfolioMetrics(logs);
		const positionSize = parseLatestPositionSize(logs);
		const serviceDetails = parseServiceProperties(parsed.serviceStatus);

		const message = parsed.orders.error ? `Lighter query error: ${parsed.orders.error}` : undefined;
		const checkedAt = new Date().toISOString();
		return {
			ok: parsed.serviceActive && !parsed.orders.error,
			checkedAt,
			source: 'live',
			host: env('MM_VPS_HOST', DEFAULT_HOST),
			serviceActive: parsed.serviceActive,
			serviceStatus: parsed.serviceStatus,
			serviceDetails,
			memoryMb: parseNumber(parsed.process.memoryMb),
			cpuPct: parseNumber(parsed.process.cpuPct),
			positionSize,
			portfolioValueUsd,
			availableCapitalUsd,
			orders,
			liveMetrics: parsed.liveFiles?.metrics ?? null,
			liveState: parsed.liveFiles?.state ?? null,
			recentWarnings,
			recentErrors,
			rawLogTail: logs.slice(-40),
			health: parseHealth(logs, checkedAt, parsed.serviceActive, serviceDetails, message),
			message,
			cacheAgeSeconds: null,
			fetchError: parsed.orders.error ?? undefined
		};
	} catch (error) {
		return emptySnapshot(error instanceof Error ? error.message : String(error));
	}
}

export async function getLiveSnapshot(options: { force?: boolean } = {}): Promise<LiveSnapshot> {
	if (!options.force) {
		const cached = await readCachedSnapshotWithAge();
		if (cached) {
			if (!refreshInFlight) {
				refreshInFlight = fetchLiveSnapshot()
					.then(async (snapshot) => {
						await writeCachedSnapshot(snapshot);
						return snapshot;
					})
					.finally(() => {
						refreshInFlight = null;
					});
			}
			return withCacheFallback(cached.snapshot, cached.ageMs);
		}
	}

	if (!refreshInFlight || options.force) {
		refreshInFlight = fetchLiveSnapshot()
			.then(async (snapshot) => {
				await writeCachedSnapshot(snapshot);
				return snapshot;
			})
			.finally(() => {
				refreshInFlight = null;
			});
	}

	const snapshot = await refreshInFlight;
	if (snapshot.source === 'unavailable') {
		const cached = await readCachedSnapshotWithAge({ allowStale: true });
		if (cached) {
			return withCacheFallback(cached.snapshot, cached.ageMs, snapshot.fetchError ?? snapshot.message);
		}
	}

	return snapshot;
}

export async function restartLiveService() {
	const service = env('MM_SERVICE_NAME', DEFAULT_SERVICE);
	await ssh(`sudo systemctl restart ${service}`, 15_000);
}
