#![deny(missing_docs)]

//! # tickbar
//!
//! High-performance tick-to-bar aggregator for financial market data.
//!
//! Converts raw trade/quote ticks into OHLCV bars with configurable
//! time alignment, gap filling, VWAP, and corporate action adjustments.
//! Processes up to **119M ticks/second** from native Rust.
//!
//! # Features
//!
//! - **Fast** — 119M ticks/s (Rust), 6.6M ticks/s (Python numpy path)
//! - **Multi-symbol parallel** — `aggregate_parallel()` via rayon
//! - **Streaming** — one-pass state machine, optional gap fill
//! - **VWAP** — volume-weighted average price per bar
//! - **Adjustments** — split/dividend adjustment events
//! - **Export** — CSV, Arrow IPC, Polars DataFrame (optional features)
//! - **Python bindings** — PyO3 via maturin
//!
//! # Quick start
//!
//! ```rust
//! use tickbar::{TickAggregator, Tick, TimeAlignment};
//! use std::time::Duration;
//!
//! let mut agg = TickAggregator::builder()
//!     .interval(Duration::from_secs(60))
//!     .build()?;
//!
//! agg.push_tick(Tick::from_trade(0, 100.0, 1000.0))?;
//! let bars = agg.finalize();
//! # Ok::<_, tickbar::Error>(())
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
//! # Performance
//!
//! | Path | Throughput | vs pandas |
//! |------|-----------|-----------|
//! | Rust native | 119M ticks/s | 25× |
//! | Python numpy | 6.6M ticks/s | 1.4× |
//! | Python bytes | 6.6M ticks/s | 1.4× |
//! | pandas resample | 4.7M ticks/s | 1.0× |
//!
//! Benchmarked on 70K synthetic ticks from 9 S&P tickers via yfinance.

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
