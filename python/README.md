# tickbar

High-performance tick-to-bar aggregator for financial market data.

Converts raw trade/quote ticks into OHLCV bars with configurable
time alignment, gap filling, VWAP, and corporate action adjustments.
One-pass state machine — **6.7M ticks/s** from Python.

---

## Key features

- **Fast** — 6.7M ticks/s from Python (PEP 3118 buffer protocol, zero-copy)
- **One-pass streaming** — no windowing, no sorting
- **VWAP** per bar
- **Gap filling** — empty bars for periods with no activity
- **Forward fill** — propagate last close through empty bars
- **Corporate actions** — split/dividend backward adjustment
- **Export** — CSV, Arrow IPC, Polars DataFrame

## Installation

```bash
pip install tickbar
```

Requires Python ≥ 3.11.

## Quick start

```python
from tickbar import TickAggregator, Tick

agg = TickAggregator(interval_secs=60)
agg.push_tick(Tick(0, 100.0, 1000.0))
agg.push_tick(Tick(1_000_000_000, 100.5, 500.0))
bars = agg.finalize()

print(f"{len(bars)} bars")
for record in bars.to_records():
    # [ts_ns, open, high, low, close, volume, tick_count, vwap]
    print(record)
```

## API reference

### `Tick`

Represents a single market data tick.

```python
from tickbar import Tick

# timestamp (int, nanoseconds), price (float), volume (float)
tick = Tick(0, 100.0, 1000.0)
repr(tick)  # "Tick(ts=0, price=100.0, volume=1000.0)"
```

### `BarSeries`

A collection of completed bars, returned by `TickAggregator.finalize()`.

```python
from tickbar import TickAggregator

agg = TickAggregator(60)
# ... push ticks ...
bars = agg.finalize()

len(bars)               # Number of bars
repr(bars)              # "BarSeries(1950 bars)"

# Extract as list of [ts, open, high, low, close, volume, tick_count, vwap]
records = bars.to_records()
```

### `TickAggregator`

The main aggregation class. Built with a fixed interval in seconds.

```python
from tickbar import TickAggregator, Tick

# Constructor
agg = TickAggregator(interval_secs=60)

# Push one tick
agg.push_tick(tick)

# Push a batch of Tick objects
agg.push_ticks([tick1, tick2, tick3])

# Push from three int64 arrays — zero-copy via PEP 3118 buffer protocol
# Supports numpy, memoryview, array.array, bytes, etc.
agg.push_from_buffer(timestamps, prices, volumes)

# Push from three numpy int64 arrays — zero-copy via __array_interface__
agg.push_from_numpy(timestamps, prices, volumes)

# Push from three Python lists — copied into Rust
agg.push_from_arrays(timestamps, prices, volumes)

# Push from packed bytes — 32 bytes per tick
agg.push_from_bytes(data)

# Finalize and get bars (consumes the aggregator)
bars = agg.finalize()
```

### Gap filling

Enable gap filling to produce empty bars for periods with no activity.
Each gap-filled bar carries the previous bar's close price as open/high/low/close
and zero volume:

```python
from tickbar import TickAggregator, Tick

# Gap filling is configured via TickAggregator.builder()
# (requires building from Rust with fill_gaps=True)
```

### Error handling

If you push out-of-order ticks (older timestamp than the previous tick),
`push_tick` and `push_ticks` raise a `ValueError`:

```python
agg = TickAggregator(interval_secs=60)
agg.push_tick(Tick(100, 100.0, 1000.0))   # ts=100
agg.push_tick(Tick(50, 99.0, 500.0))      # ts=50 — ValueError!
```

The zero-copy methods (`push_from_buffer`, `push_from_numpy`, `push_from_bytes`)
skip ordering validation for maximum throughput. Ensure your data is pre-sorted
by timestamp before using them.

### Fixed-point scale

Prices and volumes use fixed-point `int64` values. Typical scales:

| Asset | Price scale | Example |
|-------|-------------|---------|
| Stocks | 8 decimals (`100_000_000`) | `$100.50 → 10_050_000_000` |
| Crypto | 8+ decimals | `$0.00123 → 123_000` |
| FX | 5-6 decimals | `1.12345 → 112_345_000` |

The `push_from_arrays`, `push_from_numpy`, `push_from_buffer`, and
`push_from_bytes` methods all work directly with these int64 values.

### `push_from_bytes` format

The bytes must contain tightly packed `Tick` structs (32 bytes each):

| Offset | Type | Field |
|--------|------|-------|
| 0 | `i64` | `timestamp_nanos` |
| 8 | `i64` | `price` (fixed-point) |
| 16 | `i64` | `volume` (fixed-point) |
| 24 | `u64` | `flags` (0=trade, 1=quote) |

```python
import struct
import numpy as np

# Build 3 ticks manually
data = b"".join(
    struct.pack("<qqqQ", ts, price, vol, 0)
    for ts, price, vol in [(0, 100_000_000_00, 1000), (1_000_000_000, 100_500_000_00, 500)]
)
agg = TickAggregator(60)
agg.push_from_bytes(data)
bars = agg.finalize()
```

#### Push method comparison

| Method | Zero-copy? | ticks/s | When to use |
|--------|-----------|---------|-------------|
| `push_from_buffer` | yes (PEP 3118) | 6-7M | Data in numpy/memoryview/array.array — fastest |
| `push_from_numpy` | yes (`__array_interface__`) | 5-6.5M | Data already in numpy arrays |
| `push_from_bytes` | yes | 5-6.5M | Data in packed binary buffers |
| `push_from_arrays` | no (copied) | 2.5-3.5M | Data in Python lists |
| `push_ticks` | mixed | ~0.9M | Batch of Tick objects |
| `push_tick` | N/A | ~0.8M | Streaming one tick at a time |

## Performance

| Path | Throughput | vs pandas |
|------|-----------|-----------|
| Python buffer (PEP 3118) | 6.7M ticks/s | 1.4× |
| pandas resample | 4.7M ticks/s | 1.0× |

Benchmarked on 70K ticks from 9 S&P tickers via yfinance, aggregated
to 1-minute bars. Hardware: Linux x86_64, CPython 3.12.

## Real-world workflows

### 50M ticks from Parquet → pandas

Load a Parquet file, aggregate to 1-minute bars via numpy zero-copy,
back to pandas for your backtester.

```python
import numpy as np
import pandas as pd
from tickbar import TickAggregator

# Load from Parquet
df = pd.read_parquet("tick_data_2024.parquet")
print(f"Loaded {len(df):,} ticks")

# Prepare int64 arrays (zero-copy into tickbar)
PRICE_SCALE = 100_000_000
timestamps = df["timestamp_ns"].values.astype(np.int64)
prices = (df["price"].values * PRICE_SCALE).astype(np.int64)
volumes = df["volume"].values.astype(np.int64)

# Aggregate
agg = TickAggregator(interval_secs=60)
agg.push_from_buffer(timestamps, prices, volumes)
bars = agg.finalize()
print(f"Produced {len(bars)} bars")

# Back to pandas
records = np.array(bars.to_records())
df_bars = pd.DataFrame(
    records,
    columns=["ts_ns", "open", "high", "low", "close", "volume", "tick_count", "vwap"],
)
df_bars["ts"] = pd.to_datetime(df_bars["ts_ns"], unit="ns")
```

### yfinance tickers → multi-timeframe

Download 10 tickers, aggregate to 1-minute bars.

```python
import numpy as np
import yfinance as yf
from tickbar import TickAggregator

tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "WMT"]
data = yf.download(tickers, period="10d", interval="1m", group_by="ticker", progress=False)

all_ticks = []
for t in tickers:
    df = data[t].dropna()
    for idx, row in df.iterrows():
        ts = int(idx.timestamp() * 1e9)
        price = int(round(float(row["Open"]) * 100_000_000))
        vol = int(float(row["Volume"]))
        all_ticks.append((ts, price, vol))

all_ticks.sort(key=lambda x: x[0])
ts = np.array([x[0] for x in all_ticks], dtype=np.int64)
pr = np.array([x[1] for x in all_ticks], dtype=np.int64)
vo = np.array([x[2] for x in all_ticks], dtype=np.int64)

agg = TickAggregator(60)
agg.push_from_buffer(ts, pr, vo)
bars = agg.finalize()
print(f"1-min bars: {len(bars)}")
```

### Kafka → gap-filled bars → database

Consume a Kafka trade stream, aggregate with gap filling,
write completed bars to TimescaleDB.

```python
from tickbar import TickAggregator, Tick
from kafka import KafkaConsumer
import asyncpg
import json

consumer = KafkaConsumer("market-trades", bootstrap_servers="localhost:9092")
agg = TickAggregator(60)
pool = await asyncpg.create_pool("postgresql://localhost/tickdb")

for msg in consumer:
    trade = json.loads(msg.value)
    tick = Tick(trade["ts"], trade["price"], trade["size"])
    agg.push_tick(tick)

bars = agg.finalize()
# Write to database ...
```

## License

MIT
