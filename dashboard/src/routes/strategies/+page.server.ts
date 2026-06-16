import { fail } from '@sveltejs/kit';

import { getControlPlaneSnapshot } from '$lib/server/control-plane';
import { createActionRequest, updateState } from '$lib/server/store';
import { restartLiveService } from '$lib/server/vps';

export async function load() {
	return {
		snapshot: await getControlPlaneSnapshot()
	};
}

export const actions = {
	requestRetire: async ({ request }) => {
		const form = await request.formData();
		const strategyId = String(form.get('strategyId') ?? '');
		if (!strategyId) return fail(400, { message: 'Missing strategyId' });

		await updateState((state) => {
			const strategy = state.strategies.find((item) => item.id === strategyId);
			if (!strategy) return;
			state.actionRequests.unshift(
				createActionRequest({
					type: 'retire_strategy',
					title: `Retirer ${strategy.pair} du live`,
					pair: strategy.pair,
					strategyId,
					rationale: 'Demande manuelle depuis le dashboard.',
					riskNotes: 'Action sensible: necessite validation avant arret durable.',
					payload: { strategyId }
				})
			);
		});

		return { ok: true };
	},
	restartBtc: async () => {
		await restartLiveService();
		return { ok: true };
	}
};
