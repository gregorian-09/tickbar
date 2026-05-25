use crate::bar::BarSeries;

/// Type of corporate action adjustment.
#[derive(Clone, Copy, Debug)]
pub enum AdjustmentType {
    /// Stock split (ratio e.g. 2.0 for 2:1).
    Split(f64),
    /// Cash dividend (amount in fixed-point).
    Dividend(i64),
}

/// A corporate action event that affects historical bar prices.
#[derive(Clone, Debug)]
pub struct AdjustmentEvent {
    /// Timestamp of the event (bars before this are adjusted).
    pub timestamp: i64,
    /// The adjustment parameters.
    pub adjustment_type: AdjustmentType,
}

impl BarSeries {
    /// Apply corporate action adjustments to bars in place.
    ///
    /// Bars with timestamps before each event's timestamp are adjusted
    /// backward. Events are processed in reverse chronological order.
    pub fn apply_adjustments(&mut self, events: &[AdjustmentEvent]) {
        if self.bars.is_empty() {
            return;
        }
        for event in events.iter().rev() {
            let cutoff = self
                .bars
                .binary_search_by_key(&event.timestamp, |bar| bar.timestamp_nanos)
                .unwrap_or_else(|e| e);

            for bar in &mut self.bars[..cutoff] {
                match event.adjustment_type {
                    AdjustmentType::Split(ratio) => {
                        bar.open = (bar.open as f64 / ratio) as i64;
                        bar.high = (bar.high as f64 / ratio) as i64;
                        bar.low = (bar.low as f64 / ratio) as i64;
                        bar.close = (bar.close as f64 / ratio) as i64;
                        bar.volume = (bar.volume as f64 * ratio) as i64;
                    }
                    AdjustmentType::Dividend(amount) => {
                        bar.open -= amount;
                        bar.high -= amount;
                        bar.low -= amount;
                        bar.close -= amount;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bar::Bar;

    #[test]
    fn test_split_adjustment() {
        let mut series = BarSeries::new("TEST", 60_000_000_000);
        series.push(Bar {
            timestamp_nanos: 0,
            open: 200,
            high: 220,
            low: 180,
            close: 210,
            volume: 1000,
            tick_count: 10,
            vwap: 205,
        });
        series.push(Bar {
            timestamp_nanos: 60_000_000_000,
            open: 215,
            high: 230,
            low: 210,
            close: 225,
            volume: 1500,
            tick_count: 12,
            vwap: 220,
        });

        let events = vec![AdjustmentEvent {
            timestamp: 60_000_000_000,
            adjustment_type: AdjustmentType::Split(2.0),
        }];
        series.apply_adjustments(&events);

        let bars = series.as_slice();
        // First bar should be halved
        assert_eq!(bars[0].open, 100);
        assert_eq!(bars[0].high, 110);
        assert_eq!(bars[0].low, 90);
        assert_eq!(bars[0].close, 105);
        assert_eq!(bars[0].volume, 2000);
        // Second bar should be unchanged
        assert_eq!(bars[1].open, 215);
        assert_eq!(bars[1].close, 225);
    }

    #[test]
    fn test_dividend_adjustment() {
        let mut series = BarSeries::new("TEST", 60_000_000_000);
        series.push(Bar {
            timestamp_nanos: 0,
            open: 200,
            high: 220,
            low: 180,
            close: 210,
            volume: 1000,
            tick_count: 10,
            vwap: 205,
        });
        let events = vec![AdjustmentEvent {
            timestamp: 60_000_000_000,
            adjustment_type: AdjustmentType::Dividend(10),
        }];
        series.apply_adjustments(&events);
        assert_eq!(series.as_slice()[0].open, 190);
        assert_eq!(series.as_slice()[0].close, 200);
    }

    #[test]
    fn test_empty_series_no_panic() {
        let mut series = BarSeries::new("TEST", 60_000_000_000);
        let events = vec![AdjustmentEvent {
            timestamp: 0,
            adjustment_type: AdjustmentType::Split(2.0),
        }];
        series.apply_adjustments(&events);
        assert!(series.as_slice().is_empty());
    }
}
