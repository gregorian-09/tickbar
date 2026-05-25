use smol_str::SmolStr;
use std::io::Write;

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
    pub(crate) bars: Vec<Bar>,
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

    /// Return a mutable reference to the underlying bars.
    pub fn bars_mut(&mut self) -> &mut Vec<Bar> {
        &mut self.bars
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

    /// Export bars to CSV format.
    ///
    /// Writes CSV rows: timestamp_nanos,open,high,low,close,volume,tick_count,vwap
    pub fn to_csv<W: Write>(&self, writer: &mut csv::Writer<W>) -> Result<(), csv::Error> {
        for bar in &self.bars {
            writer.serialize((
                bar.timestamp_nanos,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.tick_count,
                bar.vwap,
            ))?;
        }
        writer.flush()?;
        Ok(())
    }

    /// Resample bars to a new (larger) interval.
    ///
    /// The new interval must be an integer multiple of the current interval.
    /// Returns `InvalidConfiguration` if the new interval is not a multiple.
    pub fn resample(&self, new_interval_nanos: i64) -> Result<BarSeries, crate::Error> {
        if new_interval_nanos % self.interval_nanos != 0 {
            return Err(crate::Error::InvalidConfiguration(
                "new interval must be a multiple of the current interval".into(),
            ));
        }
        let factor = (new_interval_nanos / self.interval_nanos) as usize;
        let mut out = BarSeries::new(self.symbol.clone(), new_interval_nanos);

        for chunk in self.bars.chunks(factor) {
            let first = chunk.first().ok_or_else(|| {
                crate::Error::InvalidConfiguration("empty chunk during resample".into())
            })?;
            let last = chunk.last().ok_or_else(|| {
                crate::Error::InvalidConfiguration("empty chunk during resample".into())
            })?;

            let high = chunk.iter().map(|b| b.high).max().unwrap_or(first.high);
            let low = chunk.iter().map(|b| b.low).min().unwrap_or(first.low);
            let volume: i64 = chunk.iter().map(|b| b.volume).sum();
            let tick_count: u32 = chunk.iter().map(|b| b.tick_count).sum();
            let vwap = if volume > 0 {
                let weighted_sum: i64 = chunk.iter().map(|b| b.vwap * b.volume).sum();
                weighted_sum / volume
            } else {
                last.close
            };

            out.push(Bar {
                timestamp_nanos: first.timestamp_nanos,
                open: first.open,
                high,
                low,
                close: last.close,
                volume,
                tick_count,
                vwap,
            });
        }
        Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bar_builder_basic() {
        let mut b = BarBuilder::new(0, 60_000_000_000);
        b.update(100, 1000);
        b.update(200, 500);
        let bar = b.build();
        assert_eq!(bar.open, 100);
        assert_eq!(bar.high, 200);
        assert_eq!(bar.low, 100);
        assert_eq!(bar.close, 200);
        assert_eq!(bar.volume, 1500);
        assert_eq!(bar.tick_count, 2);
        assert_eq!(bar.vwap, (100 * 1000 + 200 * 500) / 1500);
    }

    #[test]
    fn test_bar_builder_empty() {
        let b = BarBuilder::new(0, 60_000_000_000);
        assert!(b.is_empty());
    }

    #[test]
    fn test_bar_builder_single_tick() {
        let mut b = BarBuilder::new(0, 60_000_000_000);
        b.update(150, 2000);
        let bar = b.build();
        assert_eq!(bar.open, 150);
        assert_eq!(bar.close, 150);
        assert_eq!(bar.high, 150);
        assert_eq!(bar.low, 150);
        assert_eq!(bar.vwap, 150);
    }

    #[test]
    fn test_bar_series_push_and_slice() {
        let mut s = BarSeries::new("AAPL", 60_000_000_000);
        assert_eq!(s.as_slice().len(), 0);
        let bar = Bar {
            timestamp_nanos: 0,
            open: 100,
            high: 110,
            low: 90,
            close: 105,
            volume: 5000,
            tick_count: 10,
            vwap: 102,
        };
        s.push(bar);
        assert_eq!(s.as_slice().len(), 1);
        assert_eq!(s.symbol(), "AAPL");
        assert_eq!(s.interval_nanos(), 60_000_000_000);
    }

    #[test]
    fn test_bar_series_resample() {
        let mut s = BarSeries::new("TEST", 60_000_000_000);
        for i in 0..4 {
            s.push(Bar {
                timestamp_nanos: i * 60_000_000_000,
                open: 100 + i,
                high: 110 + i,
                low: 90 + i,
                close: 105 + i,
                volume: 1000,
                tick_count: 5,
                vwap: 102 + i,
            });
        }
        let resampled = s.resample(120_000_000_000).unwrap();
        assert_eq!(resampled.as_slice().len(), 2);
        assert_eq!(resampled.as_slice()[0].open, 100);
        assert_eq!(resampled.as_slice()[0].close, 106);
        assert_eq!(resampled.as_slice()[0].volume, 2000);
        assert_eq!(resampled.as_slice()[0].tick_count, 10);
    }

    #[test]
    fn test_resample_invalid_interval() {
        let s = BarSeries::new("TEST", 60_000_000_000);
        let result = s.resample(90_000_000_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_csv() {
        let mut s = BarSeries::new("TEST", 60_000_000_000);
        s.push(Bar {
            timestamp_nanos: 0,
            open: 100,
            high: 110,
            low: 90,
            close: 105,
            volume: 1000,
            tick_count: 5,
            vwap: 102,
        });
        let mut buf = Vec::new();
        let mut w = csv::Writer::from_writer(&mut buf);
        s.to_csv(&mut w).unwrap();
        drop(w);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("100,110,90,105,1000,5,102"));
    }
}
