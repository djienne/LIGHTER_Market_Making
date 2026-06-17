<script lang="ts">
	import {
		AlertTriangle,
		CheckCircle2,
		Cpu,
		Radio,
		RotateCw,
		ShieldAlert,
		ShieldCheck,
		Timer
	} from '@lucide/svelte';

	import { formatDateTime, formatPct, formatUsd } from '$lib/format';

	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const snapshot = $derived(data.snapshot);
	const live = $derived(snapshot.live);
	const btc = $derived(snapshot.strategies.find((strategy) => strategy.id === 'btc-mm-canary'));
	const warnings = $derived(live.recentWarnings.slice(-4));
	const errors = $derived(live.recentErrors.slice(-4));
	const incidents = $derived(live.health.incidents);
	const streams = $derived(live.health.streams);
	const serviceDetails = $derived(live.serviceDetails);
	const telemetry = $derived(snapshot.derived.telemetry);
	const liveQuality = $derived(live.liveMetrics);
	const qualityAdjustment = $derived(liveQuality?.quality?.adjustment);
	const markout30 = $derived(liveQuality?.markouts?.['30']);

	function formatLag(seconds: number) {
		if (seconds < 60) return `${seconds}s`;
		const minutes = Math.floor(seconds / 60);
		const remainder = seconds % 60;
		return remainder > 0 ? `${minutes}m ${remainder}s` : `${minutes}m`;
	}

	function describeTelemetrySource() {
		if (live.source === 'live') return 'Live refresh from VPS';
		if (live.source === 'cache') {
			return live.cacheAgeSeconds !== null
				? `Cached VPS snapshot · ${formatLag(live.cacheAgeSeconds)} old`
				: 'Cached VPS snapshot';
		}
		return 'Telemetry unavailable';
	}

	function formatDetectedAt(value: string | undefined) {
		if (!value) return null;
		return formatDateTime(value);
	}

	function streamBadge(status: 'healthy' | 'degraded' | 'critical') {
		return status === 'healthy' ? 'running' : status === 'degraded' ? 'paused' : 'failed';
	}

	function describeStreamFreshness(source: 'payload' | 'subscription' | 'none') {
		if (source === 'payload') return 'payload';
		if (source === 'subscription') return 're-subscribe only';
		return 'no signal';
	}
</script>

<div class="grid grid-4">
	<section class="stat-card">
		<span>Strategies live</span>
		<strong>{snapshot.derived.activeStrategies}</strong>
		<span>{btc?.pair ?? 'BTC'} · {btc?.phase ?? 'canary'}</span>
	</section>
	<section class="stat-card">
		<span>Risk score</span>
		<strong class={snapshot.derived.riskScore > 80 ? 'positive' : 'negative'}>
			{snapshot.derived.riskScore}
		</strong>
		<span>{live.orders.length} ordre(s) live · {describeTelemetrySource()}</span>
	</section>
	<section class="stat-card">
		<span>Capital alloue</span>
		<strong>{formatUsd(snapshot.derived.totalCapitalUsd)}</strong>
		<span>Levier {btc?.leverage ?? 1}x · max position {formatUsd(btc?.maxPositionUsd ?? 0)}</span>
	</section>
	<section class="stat-card">
		<span>Telemetry</span>
		<strong
			class={
				telemetry.reachability === 'reachable'
					? 'positive'
					: telemetry.reachability === 'degraded'
						? ''
						: 'negative'
			}
		>
			{telemetry.reachability}
		</strong>
		<span>{describeTelemetrySource()} · {snapshot.challengers.length} challengers queued</span>
	</section>
</div>

<div class="grid grid-2" style="margin-top: 16px;">
	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Health summary</h2>
				<p>Incidents derives du service, des watchdogs et de la fraicheur des logs.</p>
			</div>
			{#if live.health.status === 'critical'}
				<ShieldAlert size={20} aria-hidden="true" />
			{:else if live.health.status === 'degraded'}
				<AlertTriangle size={20} aria-hidden="true" />
			{:else}
				<CheckCircle2 size={20} aria-hidden="true" />
			{/if}
		</div>
		<div class="panel-body stack">
			<div class="grid grid-5">
				<div>
					<div class="eyebrow">Critical</div>
					<strong>{live.health.incidentCounts.critical}</strong>
				</div>
				<div>
					<div class="eyebrow">Warnings</div>
					<strong>{live.health.incidentCounts.warning}</strong>
				</div>
				<div>
					<div class="eyebrow">Watchdogs</div>
					<strong>{live.health.watchdogReconnects}</strong>
				</div>
				<div>
					<div class="eyebrow">Sanity latency</div>
					<strong>{live.health.maxSanityLatencyMs ?? 0} ms</strong>
				</div>
				<div>
					<div class="eyebrow">Log lag</div>
					<strong>{formatLag(live.health.staleForSeconds)}</strong>
				</div>
			</div>
			<div class="incident-list">
				{#each incidents as incident (incident.id)}
					<article class={`incident incident-${incident.severity}`}>
						<div class="incident-copy">
							<strong>{incident.title}</strong>
							<p>{incident.detail}</p>
						</div>
						<div class="incident-meta">
							{#if incident.value}
								<span>{incident.value}</span>
							{/if}
							{#if formatDetectedAt(incident.detectedAt)}
								<span>{formatDetectedAt(incident.detectedAt)}</span>
							{/if}
							<span class={`status status-${incident.severity === 'critical' ? 'failed' : incident.severity === 'warning' ? 'paused' : 'running'}`}>
								{incident.severity}
							</span>
						</div>
					</article>
				{:else}
					<div class="incident incident-info">
						<div class="incident-copy">
							<strong>No active health incidents</strong>
							<p>Le service, la fraicheur des logs et les checks REST paraissent stables.</p>
						</div>
					</div>
				{/each}
			</div>
			<div class="table-wrap">
				<table class="compact-table">
					<thead>
						<tr>
							<th>Stream</th>
							<th>Status</th>
							<th>Signal</th>
							<th>Last seen</th>
							<th>Lag</th>
							<th>Reconnects</th>
						</tr>
					</thead>
					<tbody>
						{#each streams as stream (stream.id)}
							<tr>
								<td><span class="mono">{stream.label}</span></td>
								<td>
									<span class={`status status-${streamBadge(stream.status)}`}>{stream.status}</span>
								</td>
								<td>{describeStreamFreshness(stream.freshnessSource)}</td>
								<td>{formatDetectedAt(stream.lastSeenAt) ?? 'n/a'}</td>
								<td>{stream.staleForSeconds === null ? 'n/a' : formatLag(stream.staleForSeconds)}</td>
								<td>{stream.reconnects}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</div>
	</section>

	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Live BTC canary</h2>
				<p>Etat du service VPS, reachability du control plane et des ordres Lighter actifs.</p>
			</div>
			<span class={live.serviceActive ? 'status status-running' : 'status status-failed'}>
				<Radio size={13} aria-hidden="true" />
				{live.serviceActive ? 'running' : 'offline'}
			</span>
		</div>
		<div class="panel-body stack">
			<div class="grid grid-3">
				<div>
					<div class="eyebrow">Telemetry</div>
					<strong>{telemetry.reachability}</strong>
					<div class="muted">{telemetry.summary}</div>
				</div>
				<div>
					<div class="eyebrow">CPU</div>
					<strong>{live.cpuPct ?? 0}%</strong>
				</div>
				<div>
					<div class="eyebrow">Memoire</div>
					<strong>{live.memoryMb ?? 0} MB</strong>
				</div>
				<div>
					<div class="eyebrow">Check</div>
					<strong>{formatDateTime(live.checkedAt)}</strong>
					<div class="muted">{describeTelemetrySource()} · lag {formatLag(live.health.staleForSeconds)}</div>
				</div>
			</div>
			<div class="grid grid-3">
				<div>
					<div class="eyebrow">Service</div>
					<strong>{serviceDetails.activeState}</strong>
					<div class="muted">substate {serviceDetails.subState}</div>
				</div>
				<div>
					<div class="eyebrow">Main PID</div>
					<strong>{serviceDetails.mainPid ?? 'n/a'}</strong>
					<div class="muted">result {serviceDetails.result ?? 'success'}</div>
				</div>
				<div>
					<div class="eyebrow">Systemd restarts</div>
					<strong>{serviceDetails.restartCount ?? 0}</strong>
					<div class="muted">
						{serviceDetails.execMainStatus !== null
							? `exit status ${serviceDetails.execMainStatus}`
							: 'no recent exit code'}
					</div>
				</div>
			</div>
			<div class="grid grid-3">
				<div>
					<div class="eyebrow">Stream health</div>
					<strong>{live.health.status}</strong>
				</div>
				<div>
					<div class="eyebrow">Stream reconnects</div>
					<strong>{live.health.watchdogReconnects}</strong>
				</div>
				<div>
					<div class="eyebrow">Active since</div>
					<strong>{formatDetectedAt(serviceDetails.activeEnterTimestamp) ?? 'n/a'}</strong>
				</div>
			</div>
			<div class="grid grid-3">
				<div>
					<div class="eyebrow">Position</div>
					<strong>{live.positionSize?.toFixed(5) ?? 'n/a'}</strong>
				</div>
				<div>
					<div class="eyebrow">Portfolio</div>
					<strong>{live.portfolioValueUsd !== null ? formatUsd(live.portfolioValueUsd) : 'n/a'}</strong>
				</div>
				<div>
					<div class="eyebrow">Available</div>
					<strong>{live.availableCapitalUsd !== null ? formatUsd(live.availableCapitalUsd) : 'n/a'}</strong>
				</div>
			</div>
			<div class="grid grid-3">
				<div>
					<div class="eyebrow">Live score</div>
					<strong>{liveQuality?.quality?.score?.toFixed(1) ?? 'n/a'}</strong>
					<div class="muted">{liveQuality?.fills?.count ?? 0} fills window</div>
				</div>
				<div>
					<div class="eyebrow">Markout 30s</div>
					<strong>{markout30 ? `${(markout30.adverse_avg_bps ?? 0).toFixed(2)} bps` : 'n/a'}</strong>
					<div class="muted">{markout30?.count ?? 0} samples adverse</div>
				</div>
				<div>
					<div class="eyebrow">Quality guard</div>
					<strong>{qualityAdjustment?.reason ?? 'n/a'}</strong>
					<div class="muted">
						spread x{(qualityAdjustment?.spread_multiplier ?? 1).toFixed(2)} · size x{(qualityAdjustment?.size_multiplier ?? 1).toFixed(2)}
					</div>
				</div>
			</div>
			{#if live.message}
				<p class="copy negative">{live.message}</p>
			{/if}
			<div class={`operator-note operator-${telemetry.reachability}`}>
				<strong>Operator action</strong>
				<p>{telemetry.operatorAction}</p>
			</div>

			<div class="table-wrap">
				<table>
					<thead>
						<tr>
							<th>Side</th>
							<th>Prix</th>
							<th>Size</th>
							<th>TIF</th>
							<th>Status</th>
						</tr>
					</thead>
					<tbody>
						{#each live.orders as order (order.clientOrderIndex)}
							<tr>
								<td>{order.side}</td>
								<td>{formatUsd(order.price)}</td>
								<td>{order.remainingBaseAmount.toFixed(5)}</td>
								<td>{order.timeInForce}</td>
								<td><span class="status status-running">{order.status}</span></td>
							</tr>
						{:else}
							<tr>
								<td colspan="5" class="muted">Aucun ordre actif detecte.</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</div>
	</section>

	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Autopilot guardrails</h2>
				<p>Ce que le systeme peut faire sans augmenter le risque.</p>
			</div>
			<ShieldCheck size={20} aria-hidden="true" />
		</div>
		<div class="panel-body stack">
			<p class="copy">
				L'autopilot peut lancer des challengers dry-run, nettoyer des resultats faibles, creer des demandes de validation et pauser en cas de danger. Il ne peut pas augmenter le capital, le levier ou promouvoir une config live sans validation.
			</p>
			<div class="grid grid-2">
				<div>
					<div class="eyebrow">Cible</div>
					<strong>{formatPct(3)} a {formatPct(8)} / mois</strong>
					<p class="copy">A valider en forward live, pas comme promesse.</p>
				</div>
				<div>
					<div class="eyebrow">Cycle</div>
					<strong>champion / challenger</strong>
					<p class="copy">Dry-run, canary, puis promotion manuelle.</p>
				</div>
			</div>
			<a class="button" href="/research">
				<Timer size={17} aria-hidden="true" />
				Voir research queue
			</a>
		</div>
	</section>
</div>

<div class="grid grid-2" style="margin-top: 16px;">
	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Warnings recents</h2>
				<p>Reconnects, watchdogs et evenements a surveiller.</p>
			</div>
			<AlertTriangle size={20} aria-hidden="true" />
		</div>
		<div class="panel-body">
			<div class="log-lines">
				{#each warnings as line, index (`${index}-${line}`)}
					<span>{line}</span>
				{:else}
					<span>Aucun warning recent.</span>
				{/each}
			</div>
		</div>
	</section>

	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Critical log lines</h2>
				<p>Dernieres erreurs fortes detectees dans les logs.</p>
			</div>
			{#if errors.length}
				<AlertTriangle size={20} aria-hidden="true" />
			{:else}
				<CheckCircle2 size={20} aria-hidden="true" />
			{/if}
		</div>
		<div class="panel-body">
			<div class="log-lines">
				{#each errors as line, index (`${index}-${line}`)}
					<span class="negative">{line}</span>
				{:else}
					<span>Aucune erreur critique recente.</span>
				{/each}
			</div>
		</div>
	</section>
</div>

<div class="panel" style="margin-top: 16px;">
	<div class="panel-header">
		<div>
			<h2>Next improvement loop</h2>
			<p>Automatisation active: 8 executions horaires pour ameliorer ce control plane.</p>
		</div>
		<RotateCw size={20} aria-hidden="true" />
	</div>
	<div class="panel-body">
		<p class="copy">
			Priorites: metrics 5m, dry-run challengers, scoring adverse selection, demandes de promotion, puis multi-paires.
		</p>
	</div>
</div>
