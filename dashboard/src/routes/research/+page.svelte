<script lang="ts">
	import { FlaskConical, GitPullRequest, Plus } from '@lucide/svelte';

	import { formatPct, formatSignedUsd } from '$lib/format';

	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const snapshot = $derived(data.snapshot);
</script>

<div class="grid grid-2">
	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Nouveau challenger</h2>
				<p>Ajoute un candidat dry-run a la queue de recherche.</p>
			</div>
			<FlaskConical size={20} aria-hidden="true" />
		</div>
		<div class="panel-body">
			<form class="grid grid-3" method="POST" action="?/queueChallenger">
				<label>
					<span class="eyebrow">Pair</span>
					<input class="button" name="pair" value="ETH" />
				</label>
				<label>
					<span class="eyebrow">Horizon h</span>
					<input class="button" name="horizonHours" type="number" value="72" min="6" />
				</label>
				<label>
					<span class="eyebrow">Slots</span>
					<input class="button" name="slots" type="number" value="175" min="1" />
				</label>
				<button class="button primary" type="submit">
					<Plus size={17} aria-hidden="true" />
					Queue
				</button>
			</form>
		</div>
	</section>

	<section class="panel">
		<div class="panel-header">
			<div>
				<h2>Selection policy</h2>
				<p>Le winner n'est pas le plus rentable brut.</p>
			</div>
		</div>
		<div class="panel-body">
			<p class="copy">
				Score = PnL + stabilite + spread capture - adverse selection - inventory - rejects. Promotion uniquement apres dry-run puis canary live coherent.
			</p>
		</div>
	</section>
</div>

<section class="panel" style="margin-top: 16px;">
	<div class="panel-header">
		<div>
			<h2>Challengers</h2>
			<p>Comparaison des variantes candidates par paire.</p>
		</div>
	</div>
	<div class="table-wrap">
		<table>
			<thead>
				<tr>
					<th>Run</th>
					<th>Status</th>
					<th>Progress</th>
					<th>Score</th>
					<th>PnL</th>
					<th>Fills</th>
					<th>Adverse</th>
					<th>Decision</th>
				</tr>
			</thead>
			<tbody>
				{#each snapshot.challengers as run (run.id)}
					<tr>
						<td>
							<strong>{run.pair}</strong>
							<div class="muted">{run.name}</div>
						</td>
						<td><span class={`status status-${run.status}`}>{run.status}</span></td>
						<td>
							<div class="progress" title={`${run.progressPct}%`}>
								<span style={`width: ${Math.max(0, Math.min(100, run.progressPct))}%`}></span>
							</div>
						</td>
						<td>{run.bestScore ?? 'n/a'}</td>
						<td>{run.pnlUsd === null ? 'n/a' : formatSignedUsd(run.pnlUsd)}</td>
						<td>{run.fills}</td>
						<td>{run.adverseSelectionBps === null ? 'n/a' : `${run.adverseSelectionBps.toFixed(2)} bps`}</td>
						<td>
							<div class="inline-actions">
								<span class={`status status-${run.recommendation}`}>{run.recommendation}</span>
								<form method="POST" action="?/createPromotionRequest">
									<input type="hidden" name="challengerId" value={run.id} />
									<button class="button" type="submit">
										<GitPullRequest size={15} aria-hidden="true" />
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
