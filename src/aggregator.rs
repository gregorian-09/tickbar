use crate::alignment::TimeAlignment;
use crate::bar::{Bar, BarBuilder, BarSeries};
use crate::tick::Tick;
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
    interval_nanos: i64,
    _alignment: TimeAlignment,
    _price_scale: u8,
    _volume_scale: u8,
    fill_gaps: bool,
    _forward_fill: bool,
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
        }
    }

    /// Ingest a slice of ticks, updating aggregation state.
    ///
    /// Ticks must be sorted by timestamp.
    pub fn ingest_ticks(&mut self, ticks: &[Tick]) {
        for tick in ticks {
            if tick.timestamp_nanos < self.current_bar.end_time {
                self.current_bar
                    .update(tick.price, tick.volume);
                continue;
            }
            self.finalize_current_bar();
            while tick.timestamp_nanos >= self.current_bar.end_time {
                if self.current_bar.is_empty() && self.fill_gaps {
                    self.emit_empty_bar();
                }
                self.advance_bar_window();
            }
            self.current_bar
                .update(tick.price, tick.volume);
        }
    }

    fn finalize_current_bar(&mut self) {
        if !self.current_bar.is_empty() {
            let bar = self.current_bar.build();
            let adjusted = Bar {
                timestamp_nanos: bar.timestamp_nanos,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume,
                tick_count: bar.tick_count,
                vwap: bar.vwap,
            };
            self.completed_bars.push(adjusted);
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
    aggregator: BarAggregator,
    symbol: String,
}

impl TickAggregator {
    /// Create a new builder.
    pub fn builder() -> TickAggregatorBuilder {
        TickAggregatorBuilder::default()
    }

    /// Push a single tick.
    pub fn push_tick(&mut self, tick: Tick) -> Result<(), crate::Error> {
        self.aggregator.ingest_ticks(std::slice::from_ref(&tick));
        Ok(())
    }

    /// Push a batch of ticks.
    pub fn push_ticks(&mut self, ticks: &[Tick]) -> Result<(), crate::Error> {
        self.aggregator.ingest_ticks(ticks);
        Ok(())
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
        let series = self.aggregator.finalize();
        let bars = series.into_inner();
        let interval = bars.first().map_or(0, |_| 0);
        let mut final_series = BarSeries::new(&self.symbol, interval);
        for bar in bars {
            final_series.push(bar);
        }
        final_series
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
        agg.ingest_ticks(ticks);

        let mut series = BarSeries::new(symbol, config.interval_nanos);
        for bar in agg.drain_completed() {
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
