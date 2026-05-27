use crate::alignment::TimeAlignment;
use crate::bar::{Bar, BarBuilder, BarSeries};
use crate::tick::Tick;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for batch processing.
#[derive(Clone)]
pub struct AggregatorConfig {
    /// Bar interval in nanoseconds.
    pub interval_nanos: i64,
    /// Time alignment strategy.
    pub alignment: TimeAlignment,
    /// Whether to generate empty bars for gaps.
    pub fill_gaps: bool,
    /// Whether to forward-fill close prices for empty bars.
    pub forward_fill: bool,
    /// Number of decimal places for prices.
    pub price_decimals: u8,
    /// Number of decimal places for volumes.
    pub volume_decimals: u8,
}

/// Core aggregation state machine.
pub struct BarAggregator {
    current_bar: BarBuilder,
    completed_bars: Vec<Bar>,
    pub(crate) interval_nanos: i64,
    _alignment: TimeAlignment,
    _price_scale: u8,
    _volume_scale: u8,
    fill_gaps: bool,
    _forward_fill: bool,
    last_timestamp: Option<i64>,
}

impl BarAggregator {
    /// Create a new `BarAggregator`.
    pub fn new(
        interval_nanos: i64,
        alignment: TimeAlignment,
        price_scale: u8,
        volume_scale: u8,
        fill_gaps: bool,
        forward_fill: bool,
        first_tick_ts: i64,
    ) -> Self {
        let aligned_start = alignment.align(first_tick_ts);
        BarAggregator {
            current_bar: BarBuilder::new(aligned_start, aligned_start + interval_nanos),
            completed_bars: Vec::new(),
            interval_nanos,
            _alignment: alignment,
            _price_scale: price_scale,
            _volume_scale: volume_scale,
            fill_gaps,
            _forward_fill: forward_fill,
            last_timestamp: None,
        }
    }

    /// Ingest a pre-sorted slice of ticks, skipping ordering validation.
    ///
    /// Caller guarantees ticks are monotonically increasing by timestamp.
    pub fn ingest_ticks_unchecked(&mut self, ticks: &[Tick]) {
        for tick in ticks {
            if tick.timestamp_nanos >= self.current_bar.end_time {
                self.finalize_current_bar();
                self.advance_to_bar(tick.timestamp_nanos);
            }
            self.current_bar.update(tick.price, tick.volume);
        }
    }

    /// Ingest a slice of ticks with ordering validation.
    ///
    /// Returns `OutOfOrderTick` if a tick arrives out of sequence.
    pub fn ingest_ticks(&mut self, ticks: &[Tick]) -> Result<(), crate::Error> {
        for tick in ticks {
            if let Some(last) = self.last_timestamp
                && tick.timestamp_nanos < last
            {
                return Err(crate::Error::OutOfOrderTick {
                    current: tick.timestamp_nanos,
                    previous: last,
                });
            }
            self.last_timestamp = Some(tick.timestamp_nanos);

            if tick.timestamp_nanos >= self.current_bar.end_time {
                self.finalize_current_bar();
                self.advance_to_bar(tick.timestamp_nanos);
            }
            self.current_bar.update(tick.price, tick.volume);
        }
        Ok(())
    }

    /// Ingest pre-sorted data from parallel arrays, avoiding `Tick` construction.
    pub fn ingest_from_arrays(&mut self, timestamps: &[i64], prices: &[i64], volumes: &[i64]) {
        let n = timestamps.len().min(prices.len()).min(volumes.len());
        for i in 0..n {
            let ts = timestamps[i];
            if ts >= self.current_bar.end_time {
                self.finalize_current_bar();
                self.advance_to_bar(ts);
            }
            self.current_bar.update(prices[i], volumes[i]);
        }
    }

    fn advance_to_bar(&mut self, ts: i64) {
        while ts >= self.current_bar.end_time {
            if self.current_bar.is_empty() && self.fill_gaps {
                self.emit_empty_bar();
            }
            self.advance_bar_window();
        }
    }

    fn finalize_current_bar(&mut self) {
        if !self.current_bar.is_empty() {
            let bar = self.current_bar.build();
            self.completed_bars.push(bar);
        }
    }

    fn advance_bar_window(&mut self) {
        let next_start = self.current_bar.end_time;
        let next_end = next_start + self.interval_nanos;
        self.current_bar = BarBuilder::new(next_start, next_end);
    }

    fn emit_empty_bar(&mut self) {
        let close = self.completed_bars.last().map(|b| b.close).unwrap_or(0);
        let bar = Bar {
            timestamp_nanos: self.current_bar.start_time,
            open: close,
            high: close,
            low: close,
            close,
            volume: 0,
            tick_count: 0,
            vwap: close,
        };
        self.completed_bars.push(bar);
    }

    /// Drain all completed bars.
    pub fn drain_completed(&mut self) -> Vec<Bar> {
        std::mem::take(&mut self.completed_bars)
    }

    /// Peek at completed bars.
    pub fn peek_completed(&self) -> &[Bar] {
        &self.completed_bars
    }

    /// Finalize and return a `BarSeries` containing all bars
    /// including the in-progress (current) bar.
    pub fn finalize(self) -> BarSeries {
        let mut series = BarSeries::new("", self.interval_nanos);
        for bar in self.completed_bars {
            series.push(bar);
        }
        if !self.current_bar.is_empty() {
            series.push(self.current_bar.build());
        }
        series
    }
}

/// Public-facing aggregator with a builder pattern.
pub struct TickAggregator {
    pub(crate) aggregator: BarAggregator,
    symbol: String,
}

impl TickAggregator {
    /// Create a new builder.
    pub fn builder() -> TickAggregatorBuilder {
        TickAggregatorBuilder::default()
    }

    /// Push a single tick.
    pub fn push_tick(&mut self, tick: Tick) -> Result<(), crate::Error> {
        self.aggregator.ingest_ticks(std::slice::from_ref(&tick))
    }

    /// Push a batch of ticks.
    pub fn push_ticks(&mut self, ticks: &[Tick]) -> Result<(), crate::Error> {
        self.aggregator.ingest_ticks(ticks)
    }

    /// Drain completed bars.
    pub fn drain_completed(&mut self) -> Vec<Bar> {
        self.aggregator.drain_completed()
    }

    /// Peek at completed bars.
    pub fn peek_completed(&self) -> &[Bar] {
        self.aggregator.peek_completed()
    }

    /// Finalize and return all bars as a `BarSeries`.
    pub fn finalize(self) -> BarSeries {
        let interval = self.aggregator.interval_nanos;
        let bars = self.aggregator.finalize().into_inner();
        let mut series = BarSeries::new(&self.symbol, interval);
        for bar in bars {
            series.push(bar);
        }
        series
    }

    /// Process a batch of ticks with the given config and return bars.
    pub fn process_batch(
        ticks: &[Tick],
        config: AggregatorConfig,
        symbol: &str,
    ) -> crate::Result<BarSeries> {
        let first_ts = ticks.first().map_or(0, |t| t.timestamp_nanos);
        let mut agg = BarAggregator::new(
            config.interval_nanos,
            config.alignment,
            config.price_decimals,
            config.volume_decimals,
            config.fill_gaps,
            config.forward_fill,
            first_ts,
        );
        agg.ingest_ticks(ticks)?;

        // Finalize any in-progress bar
        if !agg.current_bar.is_empty() {
            agg.completed_bars.push(agg.current_bar.build());
        }

        let mut series = BarSeries::new(symbol, config.interval_nanos);
        for bar in agg.completed_bars {
            series.push(bar);
        }
        Ok(series)
    }
}

/// Builder for `TickAggregator`.
pub struct TickAggregatorBuilder {
    interval: Option<Duration>,
    alignment: TimeAlignment,
    fill_gaps: bool,
    forward_fill: bool,
    price_decimals: u8,
    volume_decimals: u8,
    symbol: String,
}

impl Default for TickAggregatorBuilder {
    fn default() -> Self {
        TickAggregatorBuilder {
            interval: None,
            alignment: TimeAlignment::UTC,
            fill_gaps: false,
            forward_fill: false,
            price_decimals: 8,
            volume_decimals: 0,
            symbol: String::new(),
        }
    }
}

impl TickAggregatorBuilder {
    /// Set the bar interval.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set exchange-aligned time with a timezone offset in seconds.
    pub fn align_to_exchange(mut self, tz_offset: i32) -> Self {
        self.alignment = TimeAlignment::Custom(tz_offset as i64 * 1_000_000_000);
        self
    }

    /// Enable or disable gap filling.
    pub fn fill_gaps(mut self, enable: bool) -> Self {
        self.fill_gaps = enable;
        self
    }

    /// Set the symbol.
    pub fn symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    /// Build the `TickAggregator`.
    pub fn build(self) -> crate::Result<TickAggregator> {
        let interval_nanos = self
            .interval
            .ok_or_else(|| crate::Error::InvalidConfiguration("interval is required".into()))?
            .as_nanos() as i64;

        let aggregator = BarAggregator::new(
            interval_nanos,
            self.alignment,
            self.price_decimals,
            self.volume_decimals,
            self.fill_gaps,
            self.forward_fill,
            0,
        );

        Ok(TickAggregator {
            aggregator,
            symbol: self.symbol,
        })
    }
}

/// Aggregate ticks for multiple symbols in parallel.
///
/// Each symbol's ticks are processed independently on separate rayon threads.
/// Returns a map from symbol name to its `BarSeries`.
pub fn aggregate_parallel(
    ticks_by_symbol: HashMap<String, Vec<Tick>>,
    config: AggregatorConfig,
) -> HashMap<String, crate::Result<BarSeries>> {
    use rayon::prelude::*;

    ticks_by_symbol
        .into_par_iter()
        .map(|(symbol, ticks)| {
            let result = TickAggregator::process_batch(&ticks, config.clone(), &symbol);
            (symbol, result)
        })
        .collect()
}

/// A simple trading calendar that defines valid trading sessions.
#[derive(Clone, Debug)]
pub struct TradingCalendar {
    /// Ordered list of (start_nanos, end_nanos) trading sessions.
    sessions: Vec<(i64, i64)>,
}

impl TradingCalendar {
    /// Create a new `TradingCalendar` from a sorted list of sessions.
    pub fn new(sessions: Vec<(i64, i64)>) -> Self {
        TradingCalendar { sessions }
    }

    /// Returns `true` if the given timestamp falls within a trading session.
    pub fn is_trading_time(&self, timestamp_nanos: i64) -> bool {
        self.sessions
            .binary_search_by(|&(start, end)| {
                if timestamp_nanos < start {
                    std::cmp::Ordering::Greater
                } else if timestamp_nanos >= end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bar_aggregator_simple() {
        let ticks = vec![
            Tick {
                timestamp_nanos: 0,
                price: 100,
                volume: 1000,
                flags: 0,
            },
            Tick {
                timestamp_nanos: 30_000_000_000,
                price: 110,
                volume: 500,
                flags: 0,
            },
        ];
        let mut agg = BarAggregator::new(60_000_000_000, TimeAlignment::UTC, 8, 0, false, false, 0);
        agg.ingest_ticks(&ticks).unwrap();
        let series = agg.finalize();
        assert_eq!(series.as_slice().len(), 1);
        let bar = series.as_slice()[0];
        assert_eq!(bar.open, 100);
        assert_eq!(bar.close, 110);
        assert_eq!(bar.high, 110);
        assert_eq!(bar.low, 100);
        assert_eq!(bar.volume, 1500);
    }

    #[test]
    fn test_bar_aggregator_multiple_bars() {
        let ticks = vec![
            Tick {
                timestamp_nanos: 0,
                price: 100,
                volume: 1000,
                flags: 0,
            },
            Tick {
                timestamp_nanos: 61_000_000_000,
                price: 200,
                volume: 500,
                flags: 0,
            },
        ];
        let mut agg = BarAggregator::new(60_000_000_000, TimeAlignment::UTC, 8, 0, false, false, 0);
        agg.ingest_ticks(&ticks).unwrap();
        let series = agg.finalize();
        assert_eq!(series.as_slice().len(), 2);
        assert_eq!(series.as_slice()[0].close, 100);
        assert_eq!(series.as_slice()[1].close, 200);
    }

    #[test]
    fn test_gap_filling() {
        let ticks = vec![
            Tick {
                timestamp_nanos: 0,
                price: 100,
                volume: 1000,
                flags: 0,
            },
            Tick {
                timestamp_nanos: 180_000_000_000,
                price: 200,
                volume: 500,
                flags: 0,
            },
        ];
        let mut agg = BarAggregator::new(60_000_000_000, TimeAlignment::UTC, 8, 0, true, false, 0);
        agg.ingest_ticks(&ticks).unwrap();
        let series = agg.finalize();
        let bars = series.as_slice();
        assert_eq!(bars.len(), 4);
        assert_eq!(bars[0].timestamp_nanos, 0);
        assert_eq!(bars[0].close, 100);
        assert_eq!(bars[1].close, 100);
        assert_eq!(bars[2].close, 100);
        assert_eq!(bars[3].close, 200);
    }

    #[test]
    fn test_tick_aggregator_builder() {
        let mut agg = TickAggregator::builder()
            .interval(Duration::from_secs(60))
            .symbol("AAPL")
            .build()
            .unwrap();
        agg.push_tick(Tick {
            timestamp_nanos: 0,
            price: 100,
            volume: 1000,
            flags: 0,
        })
        .unwrap();
        let series = agg.finalize();
        assert_eq!(series.symbol(), "AAPL");
        assert_eq!(series.as_slice().len(), 1);
    }

    #[test]
    fn test_process_batch() {
        let ticks = vec![Tick {
            timestamp_nanos: 0,
            price: 100,
            volume: 1000,
            flags: 0,
        }];
        let config = AggregatorConfig {
            interval_nanos: 60_000_000_000,
            alignment: TimeAlignment::UTC,
            fill_gaps: false,
            forward_fill: false,
            price_decimals: 8,
            volume_decimals: 0,
        };
        let series = TickAggregator::process_batch(&ticks, config, "AAPL").unwrap();
        assert_eq!(series.symbol(), "AAPL");
        assert_eq!(series.as_slice().len(), 1);
    }

    #[test]
    fn test_aggregate_parallel() {
        let mut map = HashMap::new();
        map.insert(
            "AAPL".to_string(),
            vec![Tick {
                timestamp_nanos: 0,
                price: 100,
                volume: 1000,
                flags: 0,
            }],
        );
        map.insert(
            "GOOG".to_string(),
            vec![Tick {
                timestamp_nanos: 0,
                price: 200,
                volume: 2000,
                flags: 0,
            }],
        );
        let config = AggregatorConfig {
            interval_nanos: 60_000_000_000,
            alignment: TimeAlignment::UTC,
            fill_gaps: false,
            forward_fill: false,
            price_decimals: 8,
            volume_decimals: 0,
        };
        let results = aggregate_parallel(map, config);
        assert_eq!(results.len(), 2);
        assert!(results.get("AAPL").unwrap().is_ok());
        assert!(results.get("GOOG").unwrap().is_ok());
    }

    #[test]
    fn test_trading_calendar() {
        let sessions = vec![(0, 86_400_000_000_000)];
        let cal = TradingCalendar::new(sessions);
        assert!(cal.is_trading_time(0));
        assert!(cal.is_trading_time(43_200_000_000_000));
        assert!(!cal.is_trading_time(86_400_000_000_000));
        assert!(!cal.is_trading_time(100_000_000_000_000));
    }

    #[test]
    fn test_builder_missing_interval() {
        let result = TickAggregator::builder().build();
        assert!(result.is_err());
    }
}
