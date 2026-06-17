import { fail } from '@sveltejs/kit';

import { getControlPlaneSnapshot } from '$lib/server/control-plane';
import { createActionRequest, updateState } from '$lib/server/store';

export async function load() {
	return {
		snapshot: await getControlPlaneSnapshot()
	};
}

export const actions = {
	queueChallenger: async ({ request }) => {
		const form = await request.formData();
		const pair = String(form.get('pair') ?? '').trim().toUpperCase();
		if (!pair) return fail(400, { message: 'Pair required' });

		await updateState((state) => {
			const id = `${pair.toLowerCase()}-mm-grid-${Date.now()}`;
			state.challengers.unshift({
				id,
				pair,
				name: `${pair} adaptive grid challenger`,
				status: 'queued',
				startedAt: new Date().toISOString(),
				horizonHours: Number(form.get('horizonHours') ?? 72),
				slots: Number(form.get('slots') ?? 175),
				bestScore: null,
				bestConfigLabel: null,
				progressPct: 0,
				pnlUsd: null,
				fills: 0,
				adverseSelectionBps: null,
				recommendation: 'wait'
			});
		});

		return { ok: true };
	},
	createPromotionRequest: async ({ request }) => {
		const form = await request.formData();
		const challengerId = String(form.get('challengerId') ?? '');
		if (!challengerId) return fail(400, { message: 'Missing challengerId' });

		await updateState((state) => {
			const challenger = state.challengers.find((item) => item.id === challengerId);
			if (!challenger) return;
			state.actionRequests.unshift(
				createActionRequest({
					type: 'start_canary',
					title: `Passer ${challenger.pair} en canary`,
					pair: challenger.pair,
					challengerId,
					rationale: `Challenger ${challenger.name} marque comme candidat depuis Research.`,
					riskNotes: 'Demarrer avec petit capital et subaccount dedie. Validation humaine requise.',
					payload: {
						challengerId,
						pair: challenger.pair,
						horizonHours: challenger.horizonHours,
						bestScore: challenger.bestScore
					}
				})
			);
		});

		return { ok: true };
	}
};
