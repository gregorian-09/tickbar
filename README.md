# tickbar

[![Crates.io][crates-badge]][crates-url]
[![PyPI][pypi-badge]][pypi-url]
![Rust 2024](https://img.shields.io/badge/rust-2024-edition)

High-performance tick-to-bar aggregator for financial market data.

Converts raw trade/quote ticks into OHLCV bars with configurable
time alignment, gap filling, VWAP, and corporate action adjustments.
One-pass state machine — **119M ticks/s** native Rust, **6.7M ticks/s**
from Python (PEP 3118 buffer protocol).

---

## Features

- **Fast** — 119M ticks/s (Rust), 6.7M ticks/s (Python zero-copy)
- **One-pass streaming** — no windowing, no sorting
- **VWAP** per bar
- **Gap filling** + forward fill
- **Multi-symbol parallel** (rayon)
- **Time alignment** — UTC or custom offset
- **Corporate actions** — split/dividend backward adjustment
- **Export** — CSV, Arrow IPC, Polars DataFrame
- **Memory-mapped I/O** — `MmapTickReader`
- **Python bindings** — zero-copy via PEP 3118 buffer protocol

## Quick start

### Rust

```rust
use std::time::Duration;
use tickbar::{TickAggregator, Tick};

let mut agg = TickAggregator::builder()
    .interval(Duration::from_secs(60))
    .symbol("AAPL")
    .build()?;

agg.push_tick(Tick::from_trade(0, 100.0, 1000.0))?;
agg.push_tick(Tick::from_trade(1_000_000_000, 100.5, 500.0))?;
let bars = agg.finalize();
```

### Python

```python
from tickbar import TickAggregator, Tick

agg = TickAggregator(interval_secs=60)
agg.push_tick(Tick(0, 100.0, 1000.0))
agg.push_tick(Tick(1_000_000_000, 100.5, 500.0))
bars = agg.finalize()
```

## Performance

| Path | Throughput | vs pandas |
|------|-----------|-----------|
| Rust native | 119M ticks/s | 25× |
| Python buffer (PEP 3118) | 6.7M ticks/s | 1.4× |
| pandas resample | 4.7M ticks/s | 1.0× |

## Documentation

- [Rust API docs (docs.rs)](https://docs.rs/tickbar) — full module reference with runnable examples
- [GitHub repository](https://github.com/anomalyco/tickbar) — source, issues, contributing

### Installation

```toml
[dependencies]
tickbar = "0.1"
```

```bash
pip install tickbar
```

## License

MIT

[crates-badge]: https://img.shields.io/crates/v/tickbar.svg
[crates-url]: https://crates.io/crates/tickbar
[pypi-badge]: https://img.shields.io/pypi/v/tickbar.svg
[pypi-url]: https://pypi.org/project/tickbar/
