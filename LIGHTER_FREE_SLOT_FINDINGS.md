# Lighter Exchange — Free Slot & Volume Quota Findings

## Volume Quota Mechanics

- Premium accounts start with **1,000 quota units**
- Max stackable: 5,000,000 units
- Quota increases **+1 per $7 USD** of trading volume (fills only)
- Quota does NOT regenerate on a timer — only through volume
- `SendTx` and `SendTxBatch` are the only endpoints constrained by volume quota
- In a `SendTxBatch` with N transactions, **N quota units** are consumed

### Transaction types that consume quota
| Type | TX ID |
|------|-------|
| L2CreateOrder | 14 |
| L2CancelAllOrders | 5 |
| L2ModifyOrder | 17 |
| L2CreateGroupedOrders | 28 |

Individual cancel orders (L2CancelOrder) do **NOT** consume quota.

## The "Free SendTx" Mechanism

> "Every 15 seconds, you get a free SendTx which won't consume volume quota (nor show remaining quota)."

### Key findings from testing (2026-03-03)

1. **The free slot DOES work at 0 quota** — confirmed with `create_market_order` returning success
2. **It applies only to `sendTx` (single)**, NOT to `sendTxBatch`
3. The `volume_quota_remaining` field in the response is `None` when using a free slot (as documented: "nor show remaining quota")
4. Spacing of **16s between sends** is reliable (15s is the documented minimum, +1s safety margin)

### Critical: Nonce corruption after 429 errors

When the SDK hits a 429 "Not enough volume quota" error, the nonce counter gets out of sync:
- The SDK's `nonce_manager.next_nonce()` increments the local counter
- The exchange never processes the tx (rejects at quota layer before nonce validation)
- Subsequent sends fail with "invalid nonce" until resync

**Fix:** Call `client.nonce_manager.hard_refresh_nonce(API_KEY_INDEX)` after any quota 429 error.

### Critical: Rapid-fire requests trigger firewall blocks

The Lighter API sits behind CloudFront with a **60-second static firewall cooldown**. Sending multiple requests in quick succession (even with 429 errors) can trigger this:
- The `X-Cache: Error from cloudfront` header indicates a firewall-level block
- After triggering, ALL subsequent requests get 429 even if the free slot timer has elapsed
- **Fix:** After any burst of 429 errors, wait 20+ seconds before the next attempt
- At startup, if quota is 0, avoid rapid-fire cancel retries — bail immediately

### Practical free-slot strategy for 0-quota recovery

1. Detect quota=0 (from batch error response or cancel_all_orders failure)
2. Immediately refresh nonce: `hard_refresh_nonce(API_KEY_INDEX)`
3. Switch to **single-op REST mode**: send 1 tx at a time via `client.send_tx(tx_type, tx_info)`
4. Space sends at **16s intervals** (15s free slot + 1s safety)
5. For quota recovery: send IOC market orders via `client.create_market_order()` — fills generate volume, volume generates quota
6. Once quota > 0, switch back to batch mode via WS `sendTxBatch`

### What does NOT work at 0 quota

- `sendTxBatch` (REST or WS) — always requires quota, even for 1 op in the batch
- Rapid retries — triggers CloudFront firewall, making things worse
- Relying on `sendTx` without nonce refresh — nonces get corrupted by prior 429s

## Rate Limits (separate from quota)

| Tier | Weight/min |
|------|-----------|
| Premium | 24,000 |
| Standard | 60 |

- `sendTx` / `sendTxBatch` weight = 6
- Per-user tx type limit: 40 "Default" txs per rolling minute
- Firewall cooldown: 60 seconds (static)
- API server cooldown: `weightOfEndpoint / (totalWeight / 60)` seconds
