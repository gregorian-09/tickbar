use smol_str::SmolStr;

/// A single OHLCV bar — 48 bytes.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct Bar {
    /// Bar start time (UTC nanoseconds).
    pub timestamp_nanos: i64,
    /// Open price.
    pub open: i64,
    /// High price.
    pub high: i64,
    /// Low price.
    pub low: i64,
    /// Close price.
    pub close: i64,
    /// Total volume.
    pub volume: i64,
    /// Number of ticks in this bar.
    pub tick_count: u32,
    /// Volume-weighted average price.
    pub vwap: i64,
}

/// A time-ordered series of bars for a single symbol.
pub struct BarSeries {
    bars: Vec<Bar>,
    symbol: SmolStr,
    interval_nanos: i64,
    _price_scale: u8,
    _volume_scale: u8,
    timezone_offset: i32,
}

impl BarSeries {
    /// Create a new `BarSeries`.
    pub fn new(symbol: impl Into<SmolStr>, interval_nanos: i64) -> Self {
        BarSeries {
            bars: Vec::new(),
            symbol: symbol.into(),
            interval_nanos,
            _price_scale: 8,
            _volume_scale: 0,
            timezone_offset: 0,
        }
    }

    /// Return a reference to the underlying bars.
    pub fn as_slice(&self) -> &[Bar] {
        &self.bars
    }

    /// Consume the series and return the inner bar vector.
    pub fn into_inner(self) -> Vec<Bar> {
        self.bars
    }

    /// Push a completed bar onto the series.
    pub fn push(&mut self, bar: Bar) {
        self.bars.push(bar);
    }

    /// Return the symbol for this series.
    pub fn symbol(&self) -> &SmolStr {
        &self.symbol
    }

    /// Return the bar interval in nanoseconds.
    pub fn interval_nanos(&self) -> i64 {
        self.interval_nanos
    }

    /// Set the timezone offset (seconds from UTC).
    pub fn with_timezone_offset(mut self, offset: i32) -> Self {
        self.timezone_offset = offset;
        self
    }
}

/// Mutable builder for constructing a `Bar` from incoming ticks.
#[derive(Debug)]
pub struct BarBuilder {
    /// Bar start time.
    pub start_time: i64,
    /// Bar end time (exclusive).
    pub end_time: i64,
    /// First tick price.
    pub open: Option<i64>,
    /// Max price.
    pub high: i64,
    /// Min price.
    pub low: i64,
    /// Last tick price.
    pub close: i64,
    /// Sum of volume.
    pub volume_sum: i64,
    /// Sum(price * volume).
    pub vwap_numerator: i64,
    /// Number of ticks.
    pub tick_count: u32,
}

impl BarBuilder {
    /// Create a new `BarBuilder` for the interval `[start, end)`.
    pub fn new(start_time: i64, end_time: i64) -> Self {
        BarBuilder {
            start_time,
            end_time,
            open: None,
            high: i64::MIN,
            low: i64::MAX,
            close: 0,
            volume_sum: 0,
            vwap_numerator: 0,
            tick_count: 0,
        }
    }

    /// Update the bar with a new tick's price and volume.
    #[inline(always)]
    pub fn update(&mut self, price: i64, volume: i64) {
        if self.open.is_none() {
            self.open = Some(price);
        }
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume_sum += volume;
        self.vwap_numerator += price * volume;
        self.tick_count += 1;
    }

    /// Build the final `Bar`.
    pub fn build(&self) -> Bar {
        let open = self.open.unwrap_or(self.close);
        Bar {
            timestamp_nanos: self.start_time,
            open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume_sum,
            tick_count: self.tick_count,
            vwap: if self.volume_sum > 0 {
                self.vwap_numerator / self.volume_sum
            } else {
                self.close
            },
        }
    }

    /// Returns `true` if this builder has received at least one tick.
    pub fn is_empty(&self) -> bool {
        self.tick_count == 0
    }
}
