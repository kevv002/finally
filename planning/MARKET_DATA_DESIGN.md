# Market Data Backend — Design

Implementation-ready design for the FinAlly market data subsystem: the unified interface, the price cache, the GBM simulator, the Massive client, and the SSE stream. It supersedes `archive/MARKET_DATA_DESIGN.md` and brings the already-built `backend/app/market/` code in line with `PLAN.md` (sections 6 and 8) and with the real `massive` client models.

Everything lives under `backend/app/market/`. Public imports stay `from app.market import ...`.

---

## 1. What changes versus the code that exists

The subsystem is built and has 73 passing tests. These are the deltas:

| # | Area | Existing | Change | Why |
|---|------|----------|--------|-----|
| 1 | `PriceUpdate` / `PriceCache` | no baseline price | add `open_price` (+ `day_change_percent`) | PLAN §6: "daily change %" is vs `open_price` |
| 2 | SSE | sends *all* tickers whenever *any* changed | per-client: full snapshot on connect, then only changed tickers | PLAN §6; reconnect resyncs immediately |
| 3 | Simulator seed for unknown symbols | `uniform(50, 300)` | `uniform(20, 500)` | PLAN §6 |
| 4 | **Massive timestamp (bug)** | reads `snap.last_trade.timestamp / 1000` | read `last_trade.sip_timestamp / 1e9` | See below |
| 5 | Massive previous close | not read | `snap.prev_day.close` → `open_price` | PLAN §6 |
| 6 | `add_ticker` | blind append | `MassiveDataSource` validates the symbol and seeds the price; raises `UnknownTickerError` | PLAN §8: "Massive rejects unknown symbols with 400" |
| 7 | Poll race | a poll in flight can re-insert a just-removed ticker | filter results against the live ticker set | correctness |
| 8 | Held ticker on watchlist removal | archive §11 kept tracking it | **superseded**: removal is rejected while a position is held, so tracked tickers == watchlist | PLAN §6, §13 |
| 9 | `stream.py` module-level `router` | route re-registered if factory called twice | build the router inside the factory | review item 3.6 |
| 10 | `PriceCache.version` | read outside the lock | read inside the lock | review item 3.4 |

**Bug in #4 (verified against the installed `massive` package).** `TickerSnapshot.last_trade` is a `LastTrade` dataclass with fields `price`, `size`, `sip_timestamp`, `participant_timestamp`, `trf_timestamp`... and **no `timestamp`**. The existing `_poll_once` raises `AttributeError` on every snapshot, catches it as "malformed snapshot", and skips it, so real-data mode would never write a price. The unit tests did not catch it because they build snapshots with `MagicMock`, which invents any attribute. Fix the code and make the tests use the real model classes (section 11). The archived notes also say `day.previous_close` and millisecond timestamps. Both are wrong: the previous close is `prev_day.close` (an `Agg`), and Polygon/Massive v2 snapshot timestamps are Unix **nanoseconds**.

---

## 2. Architecture

```
                         MarketDataSource (ABC)
                ┌─────────────────┴──────────────────┐
        SimulatorDataSource                   MassiveDataSource
        (GBM, 500 ms loop)                    (REST snapshot poll, 15 s)
                └─────────────────┬──────────────────┘
                                  ▼  update(ticker, price, ts, open_price)
                              PriceCache  (thread-safe, latest PriceUpdate per ticker)
                  ┌───────────────┼────────────────┬──────────────┐
                  ▼               ▼                ▼              ▼
           SSE /api/stream   GET /api/watchlist  execute_trade  portfolio valuation
```

Rules that keep the rest of the backend source-agnostic:

- Sources **push** into the cache; consumers only **read** the cache and never call a source for prices.
- The only calls consumers make on a source are `add_ticker` / `remove_ticker` when the watchlist changes.
- **Tracked tickers == the watchlist.** The source is started with the DB watchlist, and routes call `add_ticker` / `remove_ticker` only after the watchlist change is valid.

File layout (unchanged; `interface.py` gains a few names):

```
backend/app/market/
  __init__.py       re-exports
  models.py         PriceUpdate
  cache.py          PriceCache
  interface.py      MarketDataSource, errors, normalize_ticker
  seed_prices.py    constants
  simulator.py      GBMSimulator + SimulatorDataSource
  massive_client.py MassiveDataSource
  factory.py        create_market_data_source
  stream.py         SSE router factory
```

---

## 3. Data model — `models.py`

`open_price` is the daily-change baseline (simulator: seed price; Massive: previous close). It is a required field placed before `timestamp`, so existing tests that build `PriceUpdate` positionally need updating.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float  # price at the prior update (drives the flash direction)
    open_price: float  # daily-change baseline: seed price or previous close
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute change from the previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from the previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def day_change_percent(self) -> float:
        """Percentage change versus open_price (the watchlist 'daily change')."""
        if self.open_price == 0:
            return 0.0
        return round((self.price - self.open_price) / self.open_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' versus the previous update."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for SSE / REST."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "open_price": self.open_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "day_change_percent": self.day_change_percent,
            "direction": self.direction,
        }
```

`frozen=True` matters beyond safety: **every cache write creates a new object**, which the SSE layer uses for cheap change detection (section 9).

---

## 4. Price cache — `cache.py`

`open_price` handling in `update()`:

1. an explicit `open_price` argument wins (Massive passes the previous close on every poll; the simulator passes the seed price on first write);
2. otherwise the previous update's `open_price` is inherited;
3. otherwise (first ever write, no baseline known) the price itself is used.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest PriceUpdate per ticker.

    Writers: one MarketDataSource. Readers: SSE, watchlist/portfolio routes, execute_trade.
    A Lock (not asyncio.Lock) because Massive results are processed from a thread pool
    while readers run on the event loop.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        open_price: float | None = None,
    ) -> PriceUpdate:
        """Record a price. previous_price and direction come from the prior entry."""
        with self._lock:
            prev = self._prices.get(ticker)
            price = round(price, 2)
            if open_price is None:
                open_price = prev.open_price if prev else price
            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=prev.price if prev else price,
                open_price=round(open_price, 2),
                timestamp=timestamp or time.time(),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow-copy snapshot; the PriceUpdate objects are immutable."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonic write counter (bumped on every update)."""
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Example:

```python
cache = PriceCache()
cache.update("AAPL", 190.00, open_price=190.00)   # first write: previous == price, direction "flat"
u = cache.update("AAPL", 190.52)                   # open_price inherited
u.direction, u.change, u.day_change_percent        # ('up', 0.52, 0.2737)
```

`version` is no longer used by SSE (section 9). It stays because it is cheap and tested; delete it if nothing else needs it.

---

## 5. Unified interface — `interface.py`

The ABC is unchanged except for the contract on `add_ticker`, plus a small error hierarchy and one symbol-normalizing function shared by the REST routes, the chat handler, and the sources.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

TICKER_RE = re.compile(r"^[A-Z.]{1,5}$")


class MarketDataError(Exception):
    """A market data problem whose message is safe to show the user (routes map it to HTTP 400)."""


class UnknownTickerError(MarketDataError):
    """The provider does not recognise this symbol."""


def normalize_ticker(raw: str) -> str:
    """Uppercase + strip, then validate against ^[A-Z.]{1,5}$ (PLAN §8)."""
    ticker = raw.strip().upper()
    if not TICKER_RE.match(ticker):
        raise MarketDataError(f"Invalid ticker symbol: {raw!r}")
    return ticker


class MarketDataSource(ABC):
    """Contract for market data providers.

    Sources push PriceUpdates into a shared PriceCache on their own schedule.
    Callers pass *normalized* tickers (see normalize_ticker).
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing prices. Called once. Must not raise on provider failure:
        the app should still boot, and the next cycle retries."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop background work. Safe to call repeatedly."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking `ticker`. No-op if already tracked.

        Guarantee on return: the ticker is tracked AND has a price in the cache.
        Raises UnknownTickerError if the provider rejects the symbol, or
        MarketDataError if the provider cannot be reached. On error nothing is tracked.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking `ticker` and drop it from the cache. No-op if not tracked."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers."""
```

The "price in the cache on return" guarantee closes the gap where a freshly added ticker has no price: the simulator seeds immediately, and Massive fetches the snapshot as part of validation.

---

## 6. Simulator

### 6.1 Seed data — `seed_prices.py`

Constants are unchanged. The only behavioral change is the random seed range for unknown symbols, which lives in `simulator.py` (below). `DEFAULT_PARAMS` (`sigma=0.25, mu=0.05`) still applies to unknown symbols.

The correlation matrix can never fail Cholesky, whatever tickers are added. Every pair gets a 0.3 baseline, and the tech (0.6) and finance (0.5) pairs add a positive block on top. That is a sum of a positive-semidefinite common-factor matrix and non-negative per-group blocks, with a unit diagonal, so it is always positive definite. TSLA and unknown symbols sit at the 0.3 baseline.

### 6.2 `GBMSimulator` — math engine

The step math is unchanged:

```
S(t+dt) = S(t) · exp((μ − σ²/2)·dt + σ·√dt·Z)        dt = 0.5 / (252·6.5·3600) ≈ 8.48e-8
```

Worked scale check for AAPL (σ = 0.22, S ≈ $190): per-tick σ·√dt ≈ 6.4e-5, which is about **$0.012 per tick**. Over one simulated hour (7,200 ticks) the standard deviation is about 0.54% (about $1.03). That gives a visible 1-cent flicker on the watchlist and a realistic drift on the charts. A 2–5% shock (p = 0.001 per ticker per tick) fires roughly every 50 s across 10 tickers.

Two changes, both in `_add_ticker_internal`:

```python
from .seed_prices import SEED_PRICES, DEFAULT_PARAMS, TICKER_PARAMS  # etc. unchanged

def _add_ticker_internal(self, ticker: str) -> None:
    """Add a ticker without rebuilding Cholesky (for batch initialization)."""
    if ticker in self._prices:
        return
    self._tickers.append(ticker)
    # Known tickers use their realistic seed; any other valid symbol gets $20-$500 (PLAN §6).
    self._prices[ticker] = SEED_PRICES.get(ticker) or round(random.uniform(20.0, 500.0), 2)
    self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))
```

Rounding the random seed keeps it a clean 2-decimal price so that `open_price` equals the first displayed price exactly.

### 6.3 `SimulatorDataSource` — async wrapper

The loop is unchanged. Only the two places that write a ticker's *first* price change, so that the seed price becomes `open_price`:

```python
async def start(self, tickers: list[str]) -> None:
    self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
    for ticker in tickers:
        self._seed_cache(ticker)  # cache is populated before the first SSE tick
    self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
    logger.info("Simulator started with %d tickers", len(tickers))

async def add_ticker(self, ticker: str) -> None:
    """Accepts any valid symbol (the route already validated the format)."""
    if self._sim is None or ticker in self._sim.get_tickers():
        return
    self._sim.add_ticker(ticker)
    self._seed_cache(ticker)
    logger.info("Simulator: added ticker %s", ticker)

def _seed_cache(self, ticker: str) -> None:
    """Write the starting price with open_price == seed. Later writes inherit open_price."""
    price = self._sim.get_price(ticker)
    self._cache.update(ticker, price, open_price=price)
```

`_run_loop` calls `self._cache.update(ticker, price)` with no `open_price`, so each ticker's baseline stays at its seed for the whole session. That is the intended "daily change".

---

## 7. Massive API client — `massive_client.py`

### 7.1 What we call

One endpoint for everything: the multi-ticker snapshot, one HTTP call regardless of watchlist size (critical for the 5 requests/min free tier).

```python
client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=["AAPL", "MSFT"])
# -> list[TickerSnapshot]
```

Fields used (verified against the `massive` models):

| Need | Field | Notes |
|------|-------|-------|
| price | `snap.last_trade.price` | `float \| None` |
| timestamp | `snap.last_trade.sip_timestamp` | Unix **nanoseconds** → divide by `1e9`; may be `None` |
| `open_price` | `snap.prev_day.close` | previous session close (`prev_day` is an `Agg`); may be `None` |

`last_trade` and `prev_day` are `None` when absent from the payload, so check them. When `open_price` is unavailable we pass `None` and the cache falls back to the first price seen.

### 7.2 Implementation

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType, TickerSnapshot

from .cache import PriceCache
from .interface import MarketDataError, MarketDataSource, UnknownTickerError

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint for all tracked tickers in one call.

    Free tier: 5 req/min -> poll_interval=15 uses 4/min, leaving headroom for the
    one validation call that add_ticker makes. Paid tiers can use 2-5 s.
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # prices available immediately; failure is logged, not raised
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval", len(tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        """Validate the symbol with one snapshot call, seed the cache, then track it."""
        if ticker in self._tickers:
            return
        try:
            snaps = await asyncio.to_thread(self._fetch_snapshots, [ticker])
        except Exception as e:
            logger.error("Massive validation of %s failed: %s", ticker, e)
            raise MarketDataError("Market data is unavailable right now, try again shortly") from e
        if self._apply(snaps, allowed={ticker}) == 0:
            raise UnknownTickerError(f"Unknown ticker: {ticker}")
        if ticker not in self._tickers:
            self._tickers.append(ticker)
        logger.info("Massive: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internals ---

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One cycle. A failed cycle keeps the last cached prices and retries next interval."""
        tickers = list(self._tickers)
        if not tickers or not self._client:
            return
        try:
            snaps = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except Exception as e:  # 401 bad key, 429 rate limit, network
            logger.error("Massive poll failed: %s", e)
            return
        # Re-read the live set after the await: a ticker removed mid-poll must not reappear.
        count = self._apply(snaps, allowed=set(self._tickers))
        logger.debug("Massive poll: updated %d/%d tickers", count, len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list[TickerSnapshot]:
        """Blocking REST call; runs in a worker thread. Takes its own ticker list (no shared state)."""
        return self._client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=tickers)

    def _apply(self, snaps: list[TickerSnapshot], allowed: set[str]) -> int:
        """Write usable snapshots for `allowed` tickers into the cache. Returns how many were written."""
        written = 0
        for snap in snaps:
            if snap.ticker not in allowed:
                continue
            trade = snap.last_trade
            if trade is None or not trade.price:
                logger.warning("Massive: no last trade for %s, skipping", snap.ticker)
                continue
            self._cache.update(
                ticker=snap.ticker,
                price=trade.price,
                timestamp=trade.sip_timestamp / 1e9 if trade.sip_timestamp else None,
                open_price=snap.prev_day.close if snap.prev_day and snap.prev_day.close else None,
            )
            written += 1
        return written
```

### 7.3 Failure behavior

| Situation | Behavior |
|-----------|----------|
| Bad key (401), rate limit (429), network error during a poll | logged; cache keeps last prices (stale beats blank); retried next interval |
| Failure during the startup poll | app still boots; prices appear after a later successful poll. Positions are valued at `avg_cost` meanwhile (PLAN §8) |
| Snapshot with no `last_trade` | that ticker skipped with a warning; others still written |
| `add_ticker` for an unknown symbol | `UnknownTickerError` → HTTP 400 `{"error": "Unknown ticker: XYZ"}`; nothing is tracked |
| `add_ticker` while the API is down or rate-limited | `MarketDataError` → HTTP 400 with a retry message; nothing is tracked |
| Market closed | last trade is stale but valid; timestamps show the real trade time |

### 7.4 To verify with a live key (cannot be proven offline)

1. Unknown symbol: that `get_snapshot_all(tickers=["ZZZZZ"])` returns an empty list rather than raising a 404. The code handles an empty result as `UnknownTickerError`. If it raises instead, users would see the "unavailable" message for typos, and `add_ticker` should catch the 404 explicitly.
2. `sip_timestamp` is in nanoseconds on a live response (the field name and type are verified, the unit comes from the API docs).

---

## 8. Factory — `factory.py`

Unchanged. `massive` is a core dependency, so imports stay at module top.

```python
def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty -> MassiveDataSource, otherwise SimulatorDataSource."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    return SimulatorDataSource(price_cache=price_cache)
```

The poll interval is a constructor argument (default 15 s). PLAN §5 defines no env var for it, so none is added.

---

## 9. SSE streaming — `stream.py`

Behavior required by PLAN §6: on connect send the current price of every tracked ticker, then only tickers that changed, checked every ~500 ms.

Each client keeps `sent`, the `{ticker: PriceUpdate}` it last saw. Because the cache stores a **new immutable object on every write**, "changed" is just `sent.get(t) is not update`. That needs no per-ticker version numbers and no global version, and a new connection (`sent` empty) naturally receives a full snapshot, so reconnect-resync needs no special case.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache
from .models import PriceUpdate

logger = logging.getLogger(__name__)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build the router per call (no module-level state), closed over the cache."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """text/event-stream: full snapshot on connect, then only changed tickers."""
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return router


async def _generate_events(
    price_cache: PriceCache, request: Request, interval: float = 0.5
) -> AsyncGenerator[str, None]:
    """Yield SSE frames until the client disconnects."""
    yield "retry: 1000\n\n"  # EventSource reconnects after 1 s
    sent: dict[str, PriceUpdate] = {}  # what THIS client has already received
    logger.info("SSE client connected")
    try:
        while not await request.is_disconnected():
            current = price_cache.get_all()
            changed = {t: u.to_dict() for t, u in current.items() if sent.get(t) is not u}
            sent = current  # also forgets removed tickers
            if changed:
                yield f"data: {json.dumps(changed)}\n\n"
            await asyncio.sleep(interval)
    finally:
        logger.info("SSE client disconnected")
```

Wire format: one event is a JSON object keyed by ticker, containing a single ticker or all of them on the first frame:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.0,"previous_price":190.0,"open_price":190.0,"timestamp":1763400000.1,"change":0.0,"change_percent":0.0,"day_change_percent":0.0,"direction":"flat"},"GOOGL":{...},...}

data: {"AAPL":{"ticker":"AAPL","price":190.01,"previous_price":190.0,"open_price":190.0,"timestamp":1763400000.6,"change":0.01,"change_percent":0.0053,"day_change_percent":0.0053,"direction":"up"}}

```

Client side (the frontend merges each event into its price map, so partial events are fine):

```ts
const es = new EventSource("/api/stream/prices");
es.onmessage = (e) => {
  const changed: Record<string, PriceEvent> = JSON.parse(e.data);
  setPrices((prev) => ({ ...prev, ...changed }));
};
// es.readyState: OPEN -> "connected", CONNECTING -> "reconnecting", CLOSED -> "disconnected"
```

Removed tickers are not announced on the stream. The frontend drops a ticker when its own watchlist `DELETE` succeeds, and a reconnect starts from a full snapshot of the current set.

---

## 10. Integration with the rest of the backend

### 10.1 App wiring

The cache and SSE router are created with the app (so tests can inject a fake cache). The source starts and stops in the lifespan, along with the DB init and the 30 s snapshot task from PLAN §7.

```python
def create_app(price_cache: PriceCache | None = None) -> FastAPI:
    cache = price_cache or PriceCache()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db()                                   # lazy schema + seed
        source = create_market_data_source(cache)
        await source.start(watchlist_tickers())     # tracked tickers == DB watchlist
        app.state.market_source = source
        # ... start 30 s portfolio snapshot task ...
        yield
        # ... stop snapshot task ...
        await source.stop()

    app = FastAPI(title="FinAlly", lifespan=lifespan)
    app.state.price_cache = cache
    app.include_router(create_stream_router(cache))
    # ... other /api routers, then the static mount last ...
    return app
```

### 10.2 Watchlist service (shared by the REST routes and the chat handler)

One pair of functions, so the LLM's `watchlist_changes` obey exactly the same rules. DB helper names are illustrative. The handlers are `async def` because they must `await` the source, and the SQLite calls are sub-millisecond.

```python
async def add_to_watchlist(raw: str, source: MarketDataSource) -> str:
    """Validate, make the source track it (may raise), then persist. Returns the normalized ticker."""
    ticker = normalize_ticker(raw)                  # MarketDataError -> 400
    await source.add_ticker(ticker)                 # UnknownTickerError / MarketDataError -> 400
    db.watchlist_insert_or_ignore(ticker)           # idempotent
    return ticker


async def remove_from_watchlist(raw: str, source: MarketDataSource) -> str:
    ticker = normalize_ticker(raw)
    if db.get_position(ticker) is not None:
        raise MarketDataError(f"Cannot remove {ticker}: you hold a position")
    db.watchlist_delete(ticker)
    await source.remove_ticker(ticker)
    return ticker
```

Ordering matters. On **add**, the source goes first because it is the step that can fail, so a rejected symbol never reaches the DB. On **remove**, the held-position check goes first, so a held ticker never leaves the source, which preserves the invariant that every position has a live price.

Routes map errors uniformly: `except MarketDataError as e: return JSONResponse({"error": str(e)}, status_code=400)`.

### 10.3 Reading the cache

```python
# GET /api/watchlist: DB list joined with live prices (null until the first price)
cache = request.app.state.price_cache
[
    {"ticker": t, "price": u.price if u else None, "open_price": u.open_price if u else None}
    for t in db.watchlist_tickers()
    for u in [cache.get(t)]
]

# execute_trade: the fill price is the cache price (PLAN §8 trade rules)
price = cache.get_price(ticker)
if price is None:
    raise TradeError(f"No price available for {ticker} yet")
```

Portfolio valuation falls back to `avg_cost` when `cache.get_price` is `None` (that logic belongs to the portfolio module).

---

## 11. Tests

Existing tests keep working except where `PriceUpdate` is constructed positionally (add `open_price`). Add or change the following. `pytest-asyncio` is already used.

**Cache — `open_price` rules**

```python
def test_open_price_defaults_to_first_price():
    u = PriceCache().update("AAPL", 190.0)
    assert u.open_price == 190.0

def test_open_price_is_inherited_then_overridable():
    cache = PriceCache()
    cache.update("AAPL", 190.0, open_price=188.0)
    assert cache.update("AAPL", 191.0).open_price == 188.0
    assert cache.update("AAPL", 191.0, open_price=189.0).open_price == 189.0

def test_day_change_percent():
    u = PriceCache().update("AAPL", 190.0, open_price=100.0)
    assert u.day_change_percent == 90.0
```

**Simulator**

```python
def test_unknown_symbol_seed_in_range():
    for _ in range(50):
        assert 20.0 <= GBMSimulator(["ZZZ"]).get_price("ZZZ") <= 500.0

@pytest.mark.asyncio
async def test_open_price_is_seed_and_stays_fixed():
    cache = PriceCache()
    source = SimulatorDataSource(cache, update_interval=0.01)
    await source.start(["AAPL"])
    assert cache.get("AAPL").open_price == SEED_PRICES["AAPL"]
    await asyncio.sleep(0.1)
    assert cache.get("AAPL").open_price == SEED_PRICES["AAPL"]
    await source.stop()

def test_default_watchlist_cholesky_succeeds():
    assert GBMSimulator(list(SEED_PRICES))._cholesky is not None
```

**Massive — use the real model classes, not `MagicMock`.** This is the regression test for the `.timestamp` bug.

```python
from massive.rest.models import TickerSnapshot

def snap(ticker="AAPL", price=125.07, prev_close=129.61, t_ns=1_675_190_399_000_000_000):
    return TickerSnapshot.from_dict({
        "ticker": ticker,
        "lastTrade": {"p": price, "s": 100, "t": t_ns},
        "prevDay": {"c": prev_close},
    })

@pytest.fixture
def source():
    s = MassiveDataSource("key", PriceCache(), poll_interval=60)
    s._client = object()          # non-None so polling proceeds
    s._tickers = ["AAPL"]
    return s

@pytest.mark.asyncio
async def test_poll_maps_price_open_and_timestamp(source):
    source._fetch_snapshots = lambda tickers: [snap()]
    await source._poll_once()
    u = source._cache.get("AAPL")
    assert (u.price, u.open_price, u.timestamp) == (125.07, 129.61, 1_675_190_399.0)

@pytest.mark.asyncio
async def test_missing_last_trade_skipped(source):
    source._fetch_snapshots = lambda tickers: [TickerSnapshot.from_dict({"ticker": "AAPL"})]
    await source._poll_once()
    assert source._cache.get("AAPL") is None

@pytest.mark.asyncio
async def test_add_unknown_ticker_raises_and_is_not_tracked(source):
    source._fetch_snapshots = lambda tickers: []
    with pytest.raises(UnknownTickerError):
        await source.add_ticker("ZZZZ")
    assert "ZZZZ" not in source.get_tickers()

@pytest.mark.asyncio
async def test_add_known_ticker_seeds_cache(source):
    source._fetch_snapshots = lambda tickers: [snap("MSFT", 420.0, 418.0)]
    await source.add_ticker("MSFT")
    assert "MSFT" in source.get_tickers() and source._cache.get_price("MSFT") == 420.0

@pytest.mark.asyncio
async def test_api_failure_on_add_raises_market_data_error(source):
    def boom(tickers): raise RuntimeError("429")
    source._fetch_snapshots = boom
    with pytest.raises(MarketDataError):
        await source.add_ticker("MSFT")

@pytest.mark.asyncio
async def test_removed_mid_poll_does_not_reappear(source):
    def fetch(tickers):
        source._tickers.remove("AAPL")   # removal lands while the request is in flight
        return [snap()]
    source._fetch_snapshots = fetch
    await source._poll_once()
    assert source._cache.get("AAPL") is None
```

**SSE — drive the generator directly with a fake request**

```python
class FakeRequest:
    client = None
    async def is_disconnected(self):
        return False

@pytest.mark.asyncio
async def test_full_snapshot_then_only_changes():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    cache.update("MSFT", 420.0)
    gen = _generate_events(cache, FakeRequest(), interval=0)
    assert await anext(gen) == "retry: 1000\n\n"
    first = json.loads((await anext(gen)).removeprefix("data: "))
    assert set(first) == {"AAPL", "MSFT"}                # full snapshot on connect
    cache.update("AAPL", 191.0)
    second = json.loads((await anext(gen)).removeprefix("data: "))
    assert set(second) == {"AAPL"}                       # only the changed ticker
    await gen.aclose()
```

**Normalizer**

```python
@pytest.mark.parametrize("raw,ok", [("aapl", "AAPL"), (" brk.b ", "BRK.B"), ("TOOLONG", None), ("A1", None), ("", None)])
def test_normalize_ticker(raw, ok):
    if ok:
        assert normalize_ticker(raw) == ok
    else:
        with pytest.raises(MarketDataError):
            normalize_ticker(raw)
```

Route-level tests (HTTP 400 on invalid / unknown / held-ticker removal, response shapes) belong to the API-routes step and use `create_app(price_cache=fake_cache)` with a stub `MarketDataSource`.

---

## 12. Configuration

| Parameter | Where | Default | Notes |
|-----------|-------|---------|-------|
| `MASSIVE_API_KEY` | env | empty | non-empty selects Massive, empty selects the simulator |
| `update_interval` | `SimulatorDataSource` | 0.5 s | simulator tick |
| `event_probability` | `GBMSimulator` | 0.001 | per ticker per tick, 2–5% shock |
| `poll_interval` | `MassiveDataSource` | 15 s | free tier; paid 2–5 s |
| SSE interval | `_generate_events` | 0.5 s | change check per client |
| SSE `retry` | `_generate_events` | 1000 ms | EventSource reconnect delay |
| Unknown-symbol seed | `GBMSimulator` | $20–$500 | simulator only |

The package `__init__.py` re-exports are unchanged. Add `MarketDataError`, `UnknownTickerError` and `normalize_ticker` so routes import them from `app.market`.

---

## 13. Implementation order

Small steps, each validated before the next (`uv run --extra dev pytest -v` and `ruff check app/ tests/`):

1. `models.py` + `cache.py`: `open_price`, `day_change_percent`, `version` under the lock. Fix tests that build `PriceUpdate`; add the cache tests.
2. `interface.py`: errors + `normalize_ticker` (+ test); export from `__init__.py`.
3. `simulator.py`: seed range, `_seed_cache` with `open_price`, `add_ticker` no-op guard (+ tests).
4. `massive_client.py`: rewrite per section 7 and replace `MagicMock` snapshots with `TickerSnapshot.from_dict` fixtures. Write the regression test first and watch it fail against the current code.
5. `stream.py`: per-client change detection, router built inside the factory (+ SSE test).
6. Run `market_data_demo.py` (simulator) to confirm the live behavior.
7. With a real `MASSIVE_API_KEY`, run the two checks in section 7.4 once.
8. Wire into the app (section 10) as part of build-order step 1 of `PLAN.md`. Afterwards refresh `MARKET_DATA_SUMMARY.md` and `backend/CLAUDE.md` (they still describe the old `PriceUpdate` fields and SSE behavior).
