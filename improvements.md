# Market Maker Improvements

Objectif: transformer ce market maker Lighter en outil operationnel autonome, capable de tester, comparer, promouvoir et surveiller des strategies de market making avec un risque controle. Le systeme doit viser plusieurs pourcents par mois uniquement si les donnees live le justifient, sans forcer le rendement au prix d'une exposition dangereuse.

## Principes

- Le live utilise toujours une configuration `champion` stable par paire.
- Les nouvelles configurations tournent d'abord en `challenger` dry-run sur le flux live.
- Aucune modification sensible du live ne doit etre appliquee sans validation humaine, sauf rollback/pause de securite.
- Les decisions doivent etre basees sur des metriques microstructure, pas seulement sur le PnL brut.
- Chaque paire doit avoir des limites de risque independantes et, au debut, idealement un subaccount dedie.
- Les donnees brutes doivent rester locales ou compressees; Supabase doit recevoir des agregats utiles, pas chaque evenement de marche.

## Architecture Cible

### 1. Runner Live

Process responsable de la strategie reellement active.

- Une paire = un service systemd ou container dedie.
- Une config champion par paire.
- Limites strictes:
  - max position notional;
  - max drawdown journalier;
  - max inventory time;
  - max order rejects;
  - max requotes/minute;
  - max adverse selection.
- Shutdown propre:
  - cancel all orders;
  - verification qu'aucun ordre ne reste live;
  - alerte si verification impossible.

### 2. Runner Challenger

Process de recherche continue qui tourne en dry-run sur le flux live.

- Lance plusieurs variantes de config en parallele.
- Ne place aucun ordre reel.
- Compare les challengers au champion.
- Peut tourner en continu par fenetres de 6h, 12h, 24h, 72h.
- Doit enregistrer les resultats agreges par fenetre.

Exemples de parametres a explorer:

- `vol_to_half_spread`;
- `min_half_spread_bps`;
- `skew`;
- `c1_ticks`;
- `capital_usage_percent`;
- `levels_per_side`;
- `default_quote_update_threshold_bps`;
- regime filters;
- alpha source: Binance OBI, Lighter OBI, mix, ou none.

### 3. Recorder Marche

Capture les donnees necessaires pour ameliorer la simulation.

Minimum:

- top of book Lighter;
- snapshots L2 periodiques;
- trades publics;
- ordres live du bot;
- fills;
- latence entre decision, envoi, ack et fill;
- post-only rejects;
- cancel/replace events.

Ideal:

- replay haute frequence local du carnet;
- compression par chunks;
- retention courte pour donnees brutes;
- retention longue pour agregats.

But: construire progressivement un simulateur historique plus realiste que le dry-run simple.

### 4. Metrics Engine

Agrege les performances par paire, config et fenetre.

Metriques obligatoires:

- PnL net;
- PnL realise;
- PnL latent;
- capture spread moyenne;
- fees;
- fill count;
- fill rate;
- quote uptime;
- requotes/minute;
- post-only reject rate;
- 429/503 API;
- inventory moyen;
- inventory max;
- temps moyen en inventaire;
- max inventory duration;
- drawdown;
- win/loss par side;
- PnL long inventory vs short inventory.

Metriques microstructure critiques:

- adverse selection apres fill a 1s, 5s, 30s, 2min;
- prix mid apres fill vs prix fill;
- toxic flow score;
- slippage theorique;
- distance moyenne quote-mid;
- probabilite de fill par distance au mid;
- PnL par regime de volatilite.

Sans ces metriques, le PnL brut peut donner une fausse impression de performance.

### 5. Scoring Champion/Challenger

Le score ne doit pas optimiser seulement le rendement.

Score propose:

```text
score =
  pnl_score
  + stability_score
  + spread_capture_score
  - drawdown_penalty
  - adverse_selection_penalty
  - inventory_penalty
  - reject_rate_penalty
  - instability_penalty
```

Un challenger peut battre le champion uniquement si:

- PnL net superieur avec marge significative;
- drawdown inferieur ou comparable;
- adverse selection non degradee;
- inventory max sous limite;
- pas de hausse dangereuse des rejects ou requotes;
- comportement stable sur plusieurs fenetres;
- au moins une fenetre de marche difficile incluse si possible.

## Cycle Autonome

### Cadence Recommandee

Toutes les 15 minutes:

- collecter metriques live;
- verifier sante des services;
- verifier ordres orphelins;
- verifier positions et inventaire;
- declencher pause de securite si necessaire.

Toutes les 1 heure:

- calculer agregats 1h;
- evaluer toxic flow;
- comparer live vs dry-run recent;
- detecter anomalies.

Toutes les 4 heures:

- lancer ou ajuster un batch challenger si la machine est disponible;
- tester de nouvelles variantes autour du champion;
- enrichir le backlog de paires candidates.

Chaque jour:

- rapport de performance par paire;
- classement des challengers;
- detection des regimes ou le bot gagne/perd;
- proposition de promotion en canary si un challenger est robuste.

Chaque semaine:

- revalidation complete des champions;
- ajustement de l'univers de paires;
- suppression des configs faibles;
- analyse des couts API/logs;
- controle de drift.

Chaque mois:

- revue globale portefeuille;
- allocation entre paires;
- hausse ou baisse progressive du capital;
- validation que la strategie reste rentable apres stress periods.

## Etats d'une Strategie

```text
candidate -> challenger_dry_run -> canary_live -> champion_live -> retired
```

### Candidate

Config creee par l'autopilot ou manuellement.

### Challenger Dry-Run

Tourne sur flux live, sans argent reel.

Conditions minimales:

- au moins 24h de donnees;
- pas de bug technique;
- metriques completes.

### Canary Live

Petit capital reel.

Conditions minimales:

- taille reduite;
- max position stricte;
- auto-pause si PnL ou inventory derape;
- comparaison dry-run/live.

### Champion Live

Config active principale.

Conditions minimales:

- plusieurs jours de canary;
- dry-run/live coherents;
- pas d'anomalie critique;
- validation humaine.

### Retired

Config abandonnee mais conservee pour audit.

## Autopilot

L'autopilot doit etre un orchestrateur prudent.

Il peut faire automatiquement:

- lancer des challengers dry-run;
- arreter des challengers non performants;
- nettoyer les logs et artefacts;
- generer des rapports;
- proposer une nouvelle paire a tester;
- proposer une promotion;
- proposer une baisse de risque;
- mettre en pause automatiquement en cas de danger critique.

Il ne doit pas faire sans validation:

- augmenter le capital live;
- passer une paire en champion live;
- changer la config champion live;
- retirer des fonds;
- ouvrir une nouvelle paire live avec capital reel;
- augmenter le levier;
- desactiver une limite de risque.

### Decisions Automatiques Autorisees

Pause immediate si:

- ordre orphelin detecte;
- position superieure au max autorise;
- pertes journalieres au-dessus du seuil;
- flux carnet incoherent;
- plus de donnees account WS;
- API rejects repetes;
- divergence live/dry-run anormale;
- latence excessive.

### Decisions Avec Validation

Creer une `action_request` si:

- un challenger bat le champion;
- une paire candidate est prometteuse;
- une hausse de capital est recommandee;
- une config live doit etre remplacee;
- un champion doit etre retire.

## Optimisation Continue

### Exploration Locale

Pour chaque champion, generer des challengers proches:

- spread plus large;
- spread plus serre;
- skew plus fort;
- skew plus faible;
- seuil de requote plus ou moins agressif;
- alpha active/inactive;
- niveaux 1 vs 2;
- capital usage plus prudent.

### Exploration Globale

Tester regulierement des familles plus eloignees:

- differentes sources alpha;
- filtres de regime;
- filtres volatilite;
- horaires/session filters;
- paires plus ou moins liquides;
- mode risk-off directionnel.

### Anti-Overfit

Ne jamais promouvoir une config uniquement parce qu'elle gagne sur une petite fenetre.

Garde-fous:

- validation sur plusieurs fenetres;
- comparaison contre champion;
- penalite adverse selection;
- penalite inventory;
- minimum fill count;
- stress test sur periode volatile;
- canary live obligatoire.

## Paires Cibles

Priorite initiale:

- BTC;
- ETH;
- SOL;
- BNB si disponible et assez liquide;
- autres grosses paires avec profondeur suffisante.

Critere de selection:

- volume eleve;
- spread non nul;
- carnet stable;
- peu de gaps;
- bonne correlation avec alpha externe;
- faible taux de rejects;
- pas trop de toxic flow.

Pour HYPE/LIT:

- ne pas supposer que Binance OBI fonctionne;
- preferer OBI Lighter ou source externe plus correlee;
- tester en dry-run avant tout canary.

## Donnees et Stockage

Ne pas envoyer tous les trades ou ticks vers Supabase.

Local:

- logs bruts;
- snapshots carnet;
- events detailles;
- fichiers compresses;
- retention courte.

Supabase:

- agregats 1m/5m/1h;
- etat services;
- action requests;
- configs;
- rapports;
- alertes.

Retention proposee:

- raw market data: 3 a 7 jours;
- events detailles: 14 a 30 jours;
- agregats: illimite;
- configs et decisions: illimite.

## Roadmap

### Phase 1 - Stabilisation BTC

- Lancer BTC canary avec 100 USDC.
- Confirmer 24h sans erreur critique.
- Ajouter logrotate.
- Ajouter metriques agreges locales.
- Verifier dry-run/live parity.

### Phase 2 - Observabilite

- Construire `mm_metrics_5m`.
- Ajouter dashboard minimal:
  - PnL;
  - inventory;
  - quote uptime;
  - adverse selection;
  - fill rate;
  - rejects;
  - active orders.
- Ajouter alertes internes.

### Phase 3 - Challenger Engine

- Lancer grid dry-run continu.
- Comparer champion/challengers.
- Generer rapports automatiques.
- Creer action requests pour promotions.

### Phase 4 - Canary Promotion

- Passer les meilleurs challengers en canary live.
- Comparer canary vs dry-run.
- Promouvoir seulement apres validation.

### Phase 5 - Multi-Paires

- Ajouter ETH.
- Ajouter SOL/BNB si liquidite OK.
- Allocation capital par score risque/rendement.
- Limits portefeuille globales.

### Phase 6 - Replay Historique

- Enregistrer carnet Lighter.
- Construire replay local.
- Simuler queue pessimiste.
- Stress tester configs avant canary.

## Definition of Done

Le systeme peut etre considere operationnel quand:

- chaque paire a un champion identifiable;
- les challengers tournent automatiquement;
- les promotions passent par validation;
- les metriques microstructure existent;
- le live peut se pauser seul en cas de danger;
- les logs sont limites et exploitables;
- la performance est mesuree net de risque;
- le capital augmente progressivement, jamais d'un bloc.

## Hypothese de Rendement

Objectif raisonnable a valider:

- court terme: stabilite et absence de perte structurelle;
- ensuite: quelques pourcents par mois sur les meilleures paires;
- objectif superieur: uniquement via diversification multi-paires et capital scaling progressif.

Un rendement eleve n'est acceptable que si:

- il survit a plusieurs regimes;
- il ne vient pas d'une accumulation d'inventaire;
- il ne depend pas d'un dry-run trop optimiste;
- il reste coherent en canary live.
