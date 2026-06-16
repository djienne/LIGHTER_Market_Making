<script lang="ts">
	import { Check, X } from '@lucide/svelte';

	import { formatDateTime } from '$lib/format';

	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const snapshot = $derived(data.snapshot);
</script>

<section class="panel">
	<div class="panel-header">
		<div>
			<h2>Action requests</h2>
			<p>Validation humaine avant toute action live sensible.</p>
		</div>
	</div>
	<div class="table-wrap">
		<table>
			<thead>
				<tr>
					<th>Demande</th>
					<th>Pair</th>
					<th>Status</th>
					<th>Risque</th>
					<th>Date</th>
					<th>Action</th>
				</tr>
			</thead>
			<tbody>
				{#each snapshot.actionRequests as item (item.id)}
					<tr>
						<td>
							<strong>{item.title}</strong>
							<div class="muted">{item.rationale}</div>
						</td>
						<td>{item.pair}</td>
						<td><span class={`status status-${item.status}`}>{item.status}</span></td>
						<td>{item.riskNotes}</td>
						<td>{formatDateTime(item.createdAt)}</td>
						<td>
							{#if item.status === 'pending'}
								<div class="inline-actions">
									<form method="POST" action="?/approve">
										<input type="hidden" name="id" value={item.id} />
										<button class="button primary" type="submit">
											<Check size={15} aria-hidden="true" />
										</button>
									</form>
									<form method="POST" action="?/reject">
										<input type="hidden" name="id" value={item.id} />
										<button class="button danger" type="submit">
											<X size={15} aria-hidden="true" />
										</button>
									</form>
								</div>
							{:else}
								<span class="muted">clos</span>
							{/if}
						</td>
					</tr>
				{:else}
					<tr>
						<td colspan="6" class="muted">Aucune demande en attente.</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</section>
