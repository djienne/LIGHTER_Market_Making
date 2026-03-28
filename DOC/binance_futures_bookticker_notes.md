# Binance Futures `@bookTicker` Notes

Reference note for later implementation work. This is a short summary of the official Binance USD-M Futures websocket docs, with links to the source pages.

## Official docs

- Connect:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect
- Individual Symbol Book Ticker Streams:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- Partial Book Depth Streams:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- Diff Book Depth Streams:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
- Local order book procedure:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- Live subscribe / unsubscribe:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams

## Key transport details

- Base websocket URL: `wss://fstream.binance.com`
- Raw single-stream path: `/ws/<streamName>`
- Combined-stream path: `/stream?streams=<stream1>/<stream2>/...`
- Symbol names are lowercase on stream names
- Connections are expected to be dropped at the 24-hour mark
- Raw `/ws/` connections use non-combined payloads

## `@bookTicker`

- Stream name: `<symbol>@bookTicker`
- Example raw endpoint:
  `wss://fstream.binance.com/ws/btcusdt@bookTicker`
- Purpose: best bid / ask price and quantity for one symbol
- Update speed in docs: real-time
- Useful fields:
  - `e`: event type, expected `bookTicker`
  - `u`: order book update id
  - `E`: event time
  - `T`: transaction time
  - `s`: symbol
  - `b`: best bid price
  - `B`: best bid quantity
  - `a`: best ask price
  - `A`: best ask quantity

## Partial depth stream currently used by the repo

- Stream name pattern: `<symbol>@depth<levels>@100ms`
- Current repo usage is equivalent to top-20 partial depth at 100ms
- Payload includes:
  - `U`: first update id in event
  - `u`: final update id in event
  - `pu`: previous final update id
  - `b`: bid updates as `[price, qty]`
  - `a`: ask updates as `[price, qty]`
- This stream gives more depth context than `@bookTicker`, but it is not the lowest-latency top-of-book feed

## Diff depth stream

- Stream name pattern: `<symbol>@depth@100ms`
- Use this only if maintaining a full local Binance order book is needed
- Official procedure requires:
  - buffering stream events
  - fetching a REST snapshot
  - aligning on `lastUpdateId`
  - checking `pu == previous u`
  - restarting from snapshot on sequence gaps

## Live subscribe / unsubscribe messages

- Subscribe request uses:
  - `method: "SUBSCRIBE"`
  - `params: ["btcusdt@bookTicker"]`
  - `id: <unsigned int>`
- Success response returns:
  - `result: null`
  - same `id`

## Current implementation (binance_obi.py)

Two independent feeds, each with its own shared state object:

- **`BinanceBookTickerClient`** connects to `@bookTicker`, publishes `SharedBBO` (best bid/ask, quantities, mid, update ID, timestamps)
- **`BinanceDiffDepthClient`** connects to `@depth@100ms` with REST snapshot sync (`/fapi/v1/depth?limit=1000`), maintains a local Binance order book (CBookSide), computes depth-derived imbalance alpha, publishes `SharedAlpha`

Ownership:
- bookTicker owns: Binance best bid/ask, quantities, mid, BBO staleness
- Diff depth owns: local Binance book, sequence state, depth-derived imbalance alpha
- The trading loop reads both independently via `state.binance_bbo` and `state.binance_alpha`

The old `@depth20@100ms` partial-depth approach has been removed.
