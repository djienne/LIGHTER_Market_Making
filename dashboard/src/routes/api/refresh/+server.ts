import { json } from '@sveltejs/kit';

import { getControlPlaneSnapshot } from '$lib/server/control-plane';

export async function GET() {
	return json(await getControlPlaneSnapshot({ forceLive: true }));
}
