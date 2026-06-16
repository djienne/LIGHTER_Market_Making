import { getControlPlaneSnapshot } from '$lib/server/control-plane';

export async function load() {
	return {
		snapshot: await getControlPlaneSnapshot()
	};
}
