#![deny(missing_docs)]

//! # tickbar
//!
//! High-performance tick-to-bar aggregator for financial market data.
//!
//! Converts raw trade/quote ticks into OHLCV bars with configurable
//! time alignment, gap filling, and corporate action adjustments.
//!
//! # Examples
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

pub use tick::{Tick, TickBuffer, DuplicatePolicy};
pub use bar::{Bar, BarSeries, BarBuilder};
pub use aggregator::{BarAggregator, TickAggregator, TickAggregatorBuilder, AggregatorConfig};
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
