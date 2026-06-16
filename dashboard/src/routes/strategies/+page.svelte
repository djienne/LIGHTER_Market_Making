<script lang="ts">
	import { Pause, Play, RotateCw, ShieldAlert } from '@lucide/svelte';

	import { formatDateTime, formatPct, formatUsd } from '$lib/format';

	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const snapshot = $derived(data.snapshot);
</script>

<section class="panel">
	<div class="panel-header">
		<div>
			<h2>Strategies lancees</h2>
			<p>Vue operationnelle des bots MM actifs, canary et champions.</p>
		</div>
	</div>
	<div class="table-wrap">
		<table>
			<thead>
				<tr>
					<th>Strategie</th>
					<th>Phase</th>
					<th>Status</th>
					<th>Capital</th>
					<th>Max position</th>
					<th>Perf estimee</th>
					<th>Check</th>
					<th>Actions</th>
				</tr>
			</thead>
			<tbody>
				{#each snapshot.strategies as strategy (strategy.id)}
					<tr>
						<td>
							<strong>{strategy.pair}</strong>
							<div class="muted">{strategy.name}</div>
						</td>
						<td>{strategy.phase}</td>
						<td><span class={`status status-${strategy.status}`}>{strategy.status}</span></td>
						<td>{formatUsd(strategy.capitalUsd)}</td>
						<td>{formatUsd(strategy.maxPositionUsd)}</td>
						<td>
							{formatPct(strategy.monthlyReturnPct)}
							<div class="muted">DD {formatPct(strategy.drawdownPct)}</div>
						</td>
						<td>{formatDateTime(strategy.lastCheckedAt)}</td>
						<td>
							<div class="inline-actions">
								{#if strategy.id === 'btc-mm-canary'}
									<form method="POST" action="?/restartBtc">
										<button class="button" type="submit" title="Redemarrer le service BTC">
											<RotateCw size={15} aria-hidden="true" />
										</button>
									</form>
								{/if}
								<form method="POST" action="?/requestRetire">
									<input type="hidden" name="strategyId" value={strategy.id} />
									<button class="button danger" type="submit" title="Demander le retrait">
										<ShieldAlert size={15} aria-hidden="true" />
									</button>
								</form>
							</div>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</section>

<div class="grid grid-2" style="margin-top: 16px;">
	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Ordres live</h2>
				<p>Controle que le bot respecte la limite d'ordres.</p>
			</div>
		</div>
		<div class="table-wrap">
			<table>
				<thead>
					<tr>
						<th>Side</th>
						<th>Prix</th>
						<th>Size</th>
						<th>Post-only</th>
					</tr>
				</thead>
				<tbody>
					{#each snapshot.live.orders as order (order.clientOrderIndex)}
						<tr>
							<td>{order.side}</td>
							<td>{formatUsd(order.price)}</td>
							<td>{order.remainingBaseAmount.toFixed(5)}</td>
							<td>{order.timeInForce}</td>
						</tr>
					{:else}
						<tr><td colspan="4" class="muted">Aucun ordre actif.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	</section>

	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Command model</h2>
				<p>Actions live sensibles gardees sous validation.</p>
			</div>
		</div>
		<div class="panel-body stack">
			<p class="copy">
				Le dashboard peut redemarrer BTC pour corriger une panne claire. Les changements de capital, promotion champion, retrait durable ou nouvelle paire live passent par Requests.
			</p>
			<div class="inline-actions">
				<span class="status status-running"><Play size={13} aria-hidden="true" /> safe restart</span>
				<span class="status status-paused"><Pause size={13} aria-hidden="true" /> validation required</span>
			</div>
		</div>
	</section>
</div>
