export function formatUsd(value: number | null | undefined) {
	const number = Number(value ?? 0);
	return `$${number.toLocaleString('en-US', {
		minimumFractionDigits: 2,
		maximumFractionDigits: 2
	})}`;
}

export function formatSignedUsd(value: number | null | undefined) {
	const number = Number(value ?? 0);
	const sign = number > 0 ? '+' : number < 0 ? '-' : '';
	return `${sign}${formatUsd(Math.abs(number))}`;
}

export function formatPct(value: number | null | undefined) {
	if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
	const number = Number(value);
	const sign = number > 0 ? '+' : number < 0 ? '-' : '';
	return `${sign}${Math.abs(number).toFixed(2)}%`;
}

export function formatDateTime(value: string | undefined) {
	if (!value) return 'n/a';
	return new Intl.DateTimeFormat('fr-FR', {
		day: '2-digit',
		month: '2-digit',
		hour: '2-digit',
		minute: '2-digit'
	}).format(new Date(value));
}
