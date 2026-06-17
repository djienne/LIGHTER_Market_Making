import { fail } from '@sveltejs/kit';

import { getControlPlaneSnapshot } from '$lib/server/control-plane';
import { updateState } from '$lib/server/store';

export async function load() {
	return {
		snapshot: await getControlPlaneSnapshot()
	};
}

export const actions = {
	approve: async ({ request }) => {
		const form = await request.formData();
		const id = String(form.get('id') ?? '');
		if (!id) return fail(400, { message: 'Missing request id' });

		await updateState((state) => {
			const item = state.actionRequests.find((requestItem) => requestItem.id === id);
			if (item) item.status = 'approved';
		});

		return { ok: true };
	},
	reject: async ({ request }) => {
		const form = await request.formData();
		const id = String(form.get('id') ?? '');
		if (!id) return fail(400, { message: 'Missing request id' });

		await updateState((state) => {
			const item = state.actionRequests.find((requestItem) => requestItem.id === id);
			if (item) item.status = 'rejected';
		});

		return { ok: true };
	}
};
