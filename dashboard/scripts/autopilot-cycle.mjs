#!/usr/bin/env node
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '../..');
const statePath = resolve(repoRoot, '.runtime/mm-control-state.json');

function nowIso() {
	return new Date().toISOString();
}

async function loadState() {
	try {
		return JSON.parse(await readFile(statePath, 'utf8'));
	} catch {
		return null;
	}
}

async function saveState(state) {
	await mkdir(dirname(statePath), { recursive: true });
	await writeFile(statePath, JSON.stringify({ ...state, updatedAt: nowIso() }, null, 2));
}

const state = await loadState();
if (!state) {
	console.log('No control-plane state yet. Open the dashboard once to seed it.');
	process.exit(0);
}

const pending = state.actionRequests.filter((request) => request.status === 'pending');
const running = state.challengers.filter((run) => run.status === 'running');
const queued = state.challengers.filter((run) => run.status === 'queued');

if (!running.length && queued.length) {
	const next = queued[0];
	next.status = 'running';
	next.startedAt = nowIso();
	next.progressPct = Math.max(next.progressPct ?? 0, 1);
	console.log(`Started simulated challenger cycle for ${next.pair}: ${next.id}`);
} else {
	for (const run of running) {
		run.progressPct = Math.min(100, Number(run.progressPct ?? 0) + 4);
		if (run.progressPct >= 100) {
			run.status = 'paused';
			run.recommendation = Number(run.bestScore ?? 0) >= 70 ? 'promote' : 'candidate';
		}
	}
	console.log(`Autopilot observe: ${running.length} running, ${queued.length} queued, ${pending.length} pending requests.`);
}

await saveState(state);
