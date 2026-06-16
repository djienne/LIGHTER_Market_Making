<script lang="ts">
	import { page } from '$app/state';
	import { Activity, FlaskConical, Gauge, GitPullRequest, Radio, ShieldCheck } from '@lucide/svelte';

	import '../app.css';
	import { formatUsd } from '$lib/format';

	import type { LayoutProps } from './$types';

	let { children, data }: LayoutProps = $props();

	const pathname = $derived(page.url.pathname);
	const navItems = [
		{ href: '/', label: 'Overview', icon: Gauge },
		{ href: '/strategies', label: 'Strategies', icon: Radio },
		{ href: '/research', label: 'Research', icon: FlaskConical },
		{ href: '/requests', label: 'Requests', icon: GitPullRequest }
	];

	function navClass(href: string) {
		if (href === '/') return pathname === '/' ? 'nav-item active' : 'nav-item';
		return pathname.startsWith(href) ? 'nav-item active' : 'nav-item';
	}
</script>

<svelte:head>
	<title>Lighter MM Control</title>
</svelte:head>

<div class="app-shell">
	<aside class="sidebar" aria-label="Navigation principale">
		<a class="brand" href="/">
			<span class="brand-mark">MM</span>
			<span>
				<strong>Lighter MM</strong>
				<span>Research control plane</span>
			</span>
		</a>

		<section class="sidebar-card" aria-label="Etat global">
			<div class="eyebrow">Capital MM</div>
			<div class="big-number">{formatUsd(data.summary.totalCapitalUsd)}</div>
			<div class="sidebar-grid">
				<div>
					<span>Live</span>
					<strong>{data.summary.activeStrategies}</strong>
				</div>
				<div>
					<span>Challengers</span>
					<strong>{data.summary.runningChallengers}</strong>
				</div>
				<div>
					<span>Risk score</span>
					<strong>{data.summary.riskScore}</strong>
				</div>
				<div>
					<span>Requests</span>
					<strong>{data.summary.pendingRequests}</strong>
				</div>
			</div>
			<span
				class={
					data.summary.healthStatus === 'critical'
						? 'status status-failed'
						: data.summary.healthStatus === 'degraded'
							? 'status status-paused'
							: 'status status-running'
				}
			>
				<Activity size={13} aria-hidden="true" />
				{data.summary.healthStatus === 'critical'
					? 'Health critical'
					: data.summary.healthStatus === 'degraded'
						? 'Health degraded'
						: 'Health healthy'}
			</span>
			<span class="muted">
				{data.summary.liveSource === 'live'
					? 'telemetry live'
					: data.summary.liveSource === 'cache'
						? `telemetry cachee${data.summary.cacheAgeSeconds !== null ? ` · ${data.summary.cacheAgeSeconds}s` : ''}`
						: 'telemetry indisponible'}
			</span>
		</section>

		<nav class="nav">
			{#each navItems as item (item.href)}
				<a class={navClass(item.href)} href={item.href}>
					<item.icon size={18} aria-hidden="true" />
					<span>{item.label}</span>
				</a>
			{/each}
		</nav>

		<div class="sidebar-footer">
			<span>Autopilot</span>
			<strong>{data.summary.autopilotMode}</strong>
			<span><ShieldCheck size={13} aria-hidden="true" /> validation humaine pour le live.</span>
		</div>
	</aside>

	<div class="main-shell">
		<header class="topbar">
			<div>
				<h1>Market making control plane</h1>
				<p>Surveille les strategies live, compare les challengers et valide les winners avant toute promotion.</p>
			</div>
			<a class="button" href="/requests">
				<GitPullRequest size={17} aria-hidden="true" />
				{data.summary.pendingRequests} validation
			</a>
		</header>

		<main>
			{@render children()}
		</main>
	</div>
</div>
