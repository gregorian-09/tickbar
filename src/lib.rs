#![deny(missing_docs)]
#![doc(html_root_url = "https://docs.rs/tickbar")]

//! # tickbar
//!
//! High-performance tick-to-bar aggregator for financial market data.
//!
//! Converts raw trade/quote ticks into OHLCV bars with configurable
//! time alignment, gap filling, VWAP, and corporate action adjustments.
//! Processes up to **119M ticks/second** from native Rust.
//!
//! # Install
//!
//! ```toml
//! [dependencies]
//! tickbar = "0.1"
//! ```
//!
//! Optional feature flags:
//!
//! ```toml
//! [dependencies]
//! tickbar = { version = "0.1", default-features = false }   # no Python bindings
//! tickbar = { version = "0.1", features = ["arrow-export"] }  # + Arrow IPC
//! tickbar = { version = "0.1", features = ["polars-export"] } # + Polars DataFrame
//! ```
//!
//! # Core concepts
//!
//! tickbar models tick-to-bar aggregation in three layers:
//!
//! - **Tick ingestion** — [`Tick`] is a 32-byte `repr(C)` struct holding
//!   `timestamp_nanos`, fixed-point `price`, fixed-point `volume`, and
//!   `flags`. Use [`TickBuffer`] for batched ingestion with ordering
//!   and dedup policies. Use [`MmapTickReader`] for memory-mapped I/O
//!   from binary tick files.
//!
//! - **Bar aggregation** — [`TickAggregator`] is a one-pass state machine
//!   built via its [`TickAggregatorBuilder`]. It advances bar boundaries
//!   on each tick, optionally fills gaps, and tracks per-bar VWAP.
//!   [`BarAggregator`] is the lower-level engine. Use [`aggregate_parallel`]
//!   for multi-symbol parallelism.
//!
//! - **Output** — [`BarSeries`] holds completed [`Bar`]s for one symbol.
//!   Export to CSV, Arrow IPC, or Polars DataFrame. Resample to a wider
//!   interval. Apply split/dividend adjustments.
//!
//! # Quick start
//!
//! ```rust
//! use tickbar::{TickAggregator, Tick};
//! use std::time::Duration;
//!
//! let mut agg = TickAggregator::builder()
//!     .interval(Duration::from_secs(60))
//!     .symbol("AAPL")
//!     .build()?;
//!
//! agg.push_tick(Tick::from_trade(0, 100.0, 1000.0))?;
//! agg.push_tick(Tick::from_trade(1_000_000_000, 100.5, 500.0))?;
//! agg.push_tick(Tick::from_trade(2_000_000_000, 101.0, 750.0))?;
//!
//! let bars = agg.finalize();
//! assert_eq!(bars.as_slice().len(), 1);
//! assert_eq!(bars.as_slice()[0].tick_count, 3);
//! # Ok::<_, tickbar::Error>(())
//! ```
//!
//! # Batch processing
//!
//! Use [`TickBuffer`] to collect ticks with configurable ordering
//! and duplicate policies, then ingest as a batch:
//!
//! ```rust
//! use tickbar::{TickAggregator, Tick, TickBuffer, DuplicatePolicy};
//! use std::time::Duration;
//!
//! let mut buf = TickBuffer::new("MSFT")
//!     .with_allow_unordered(false)
//!     .with_duplicate_policy(DuplicatePolicy::Last);
//!
//! buf.push(Tick::from_trade(0, 100.0, 1000.0))?;
//! buf.push(Tick::from_trade(1_000_000_000, 100.5, 500.0))?;
//! buf.push(Tick::from_trade(2_000_000_000, 101.0, 750.0))?;
//!
//! let mut agg = TickAggregator::builder()
//!     .interval(Duration::from_secs(60))
//!     .symbol("MSFT")
//!     .build()?;
//!
//! agg.push_ticks(buf.as_slice())?;
//! let bars = agg.finalize();
//! assert_eq!(bars.as_slice().len(), 1);
//! # Ok::<_, tickbar::Error>(())
//! ```
//!
//! # Gap filling
//!
//! Enable gap filling on the builder to produce empty bars for
//! periods with no trading activity:
//!
//! ```rust
//! use tickbar::{TickAggregator, Tick};
//! use std::time::Duration;
//!
//! let mut agg = TickAggregator::builder()
//!     .interval(Duration::from_secs(60))
//!     .fill_gaps(true)
//!     .symbol("GOOG")
//!     .build()?;
//!
//! // Tick at T=0s, next tick at T=300s — four 60s gaps in between
//! agg.push_tick(Tick::from_trade(0, 100.0, 1000.0))?;
//! agg.push_tick(Tick::from_trade(300_000_000_000, 101.0, 500.0))?;
//!
//! let bars = agg.finalize();
//! // 2 real bars + 4 gap-filled bars = 6 total
//! assert_eq!(bars.as_slice().len(), 6);
//! # Ok::<_, tickbar::Error>(())
//! ```
//!
//! # Parallel multi-symbol
//!
//! Distribute ticks by symbol, then aggregate in parallel:
//!
//! ```rust
//! use std::collections::HashMap;
//! use tickbar::{Tick, AggregatorConfig, TimeAlignment, aggregate_parallel};
//!
//! let mut ticks_by_symbol: HashMap<String, Vec<Tick>> = HashMap::new();
//! ticks_by_symbol.insert(
//!     "AAPL".into(),
//!     vec![Tick::from_trade(0, 100.0, 1000.0),
//!          Tick::from_trade(1_000_000_000, 100.5, 500.0)],
//! );
//! ticks_by_symbol.insert(
//!     "MSFT".into(),
//!     vec![Tick::from_trade(0, 200.0, 2000.0),
//!          Tick::from_trade(2_000_000_000, 201.0, 1000.0)],
//! );
//!
//! let config = AggregatorConfig {
//!     interval_nanos: 60_000_000_000,
//!     alignment: TimeAlignment::UTC,
//!     fill_gaps: false,
//!     forward_fill: false,
//!     price_decimals: 8,
//!     volume_decimals: 6,
//! };
//!
//! let results = aggregate_parallel(ticks_by_symbol, config);
//! for (symbol, result) in &results {
//!     let series = result.as_ref().unwrap();
//!     println!("{symbol}: {} bars", series.as_slice().len());
//! }
//! ```
//!
//! # Adjustments
//!
//! Apply stock splits and dividend adjustments to a completed bar series.
//! Bars with timestamps *before* an event's timestamp are adjusted backward:
//!
//! ```rust
//! use tickbar::{BarSeries, Bar, AdjustmentEvent, AdjustmentType};
//!
//! let mut series = BarSeries::new("AAPL", 60_000_000_000);
//! series.push(Bar {
//!     timestamp_nanos: 0,
//!     open: 10_000_000_000,
//!     high: 10_100_000_000,
//!     low: 9_900_000_000,
//!     close: 10_050_000_000,
//!     volume: 100_000,
//!     tick_count: 10,
//!     vwap: 10_020_000_000,
//! });
//!
//! // Event timestamp must be strictly after the bars it adjusts
//! let events = vec![AdjustmentEvent {
//!     timestamp: 60_000_000_000,
//!     adjustment_type: AdjustmentType::Split(4.0),
//! }];
//!
//! series.apply_adjustments(&events);
//! // Prices are divided by 4, volume multiplied by 4
//! assert_eq!(series.as_slice()[0].open, 2_500_000_000); // 10_000_000_000 / 4
//! assert_eq!(series.as_slice()[0].volume, 400_000);     // 100_000 * 4
//! ```
//!
//! # Python
//!
//! ```python
//! from tickbar import TickAggregator, Tick
//!
//! agg = TickAggregator(interval_secs=60)
//! agg.push_tick(Tick(0, 100.0, 1000.0))
//! bars = agg.finalize()
//! ```
//!
//! # Memory-mapped file reader
//!
//! Read packed `Tick` binary files via `mmap`:
//!
//! ```rust
//! use tickbar::{MmapTickReader, BarAggregator, TimeAlignment};
//!
//! // Assuming /tmp/ticks.bin exists with packed 32-byte Tick records
//! if let Ok(reader) = MmapTickReader::open("/tmp/ticks.bin") {
//!     let ticks: Vec<_> = reader.collect();
//!     let mut agg = BarAggregator::new(
//!         60_000_000_000, TimeAlignment::UTC, 8, 0,
//!         false, false,
//!         ticks.first().map(|t| t.timestamp_nanos).unwrap_or(0),
//!     );
//!     agg.ingest_ticks_unchecked(&ticks);
//!     let bars = agg.finalize();
//!     println!("{} bars from mmap", bars.as_slice().len());
//! }
//! ```
//!
//! # CSV export
//!
//! Write bars to CSV format:
//!
//! ```rust
//! use tickbar::{BarSeries, Bar};
//!
//! let mut series = BarSeries::new("AAPL", 60_000_000_000);
//! series.push(Bar {
//!     timestamp_nanos: 0, open: 10000, high: 10100, low: 9900,
//!     close: 10050, volume: 100000, tick_count: 10, vwap: 10020,
//! });
//!
//! let mut buf = Vec::new();
//! let mut wtr = csv::Writer::from_writer(&mut buf);
//! series.to_csv(&mut wtr)?;
//! drop(wtr);
//! let output = String::from_utf8(buf)?;
//! assert!(output.contains("10000"));  // open price in CSV
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! # Trading calendar
//!
//! Filter ticks outside of trading hours:
//!
//! ```rust
//! use tickbar::{TradingCalendar, Tick};
//!
//! let cal = TradingCalendar::new(vec![
//!     (9 * 3_600_000_000_000, 16 * 3_600_000_000_000),  // 9:00-16:00 UTC
//! ]);
//!
//! let tick = Tick::from_trade(10 * 3_600_000_000_000, 100.0, 1000.0); // 10:00 UTC
//! assert!(cal.is_trading_time(tick.timestamp_nanos));
//!
//! let tick = Tick::from_trade(20 * 3_600_000_000_000, 100.0, 1000.0); // 20:00 UTC
//! assert!(!cal.is_trading_time(tick.timestamp_nanos));
//! ```
//!
//! # Performance
//!
//! | Path | Throughput | vs pandas |
//! |------|-----------|-----------|
//! | Rust native | 119M ticks/s | 25× |
//! | Python buffer (PEP 3118) | 6.7M ticks/s | 1.4× |
//! | Python numpy | 6.5M ticks/s | 1.4× |
//! | pandas resample | 4.7M ticks/s | 1.0× |
//!
//! Benchmarked on 70K synthetic ticks from 9 S&P tickers via yfinance.
//!
//! # Feature flags
//!
//! | Feature | Description | Default |
//! |---------|-------------|---------|
//! | `python` | PyO3 bindings for maturin | yes |
//! | `arrow-export` | Arrow IPC export via `to_arrow()` | no |
//! | `polars-export` | Polars DataFrame export + arrow | no |
//!
//! # Quality and CI guarantees
//!
//! The repository CI enforces:
//!
//! - `cargo test --workspace` (25 unit + 9 integration + 1 doc test)
//! - `cargo clippy --all-features` (zero warnings)
//! - `RUSTFLAGS="-D missing_docs" cargo doc --no-deps` (100% docs coverage)
//! - `cargo-semver-checks` on release candidates
//!
//! # Support
//!
//! - Issues: <https://github.com/anomalyco/tickbar/issues>
//! - Repository: <https://github.com/anomalyco/tickbar>
//! - Crates.io: <https://crates.io/crates/tickbar>
//! - PyPI: <https://pypi.org/project/tickbar/>

mod tick;
mod bar;
mod aggregator;
mod alignment;
mod adjustments;

#[cfg(feature = "python")]
/// Foreign function interface bindings.
pub mod ffi;
/// Utility modules for fixed-point conversion and time handling.
pub mod utils;

pub use tick::{Tick, TickBuffer, DuplicatePolicy, MmapTickReader};
pub use bar::{Bar, BarSeries, BarBuilder};
pub use aggregator::{
    BarAggregator, TickAggregator, TickAggregatorBuilder, AggregatorConfig, TradingCalendar,
    aggregate_parallel,
};
pub use alignment::TimeAlignment;
pub use adjustments::{AdjustmentEvent, AdjustmentType};

use thiserror::Error;

/// Errors that can occur during tick aggregation.
#[derive(Error, Debug)]
pub enum Error {
    /// A tick arrived with a timestamp older than the previous tick.
    #[error("out-of-order tick: current={current}, previous={previous}")]
    OutOfOrderTick {
        /// Timestamp of the current tick.
        current: i64,
        /// Timestamp of the previous tick.
        previous: i64,
    },
    /// The provided configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

/// Alias for `Result<T, tickbar::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
