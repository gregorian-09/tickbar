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
        let bars = self.as_slice();
        if bars.is_empty() {
            return;
        }

        let mut adjusted = bars.to_vec();
        for event in events.iter().rev() {
            let cutoff_idx = adjusted
                .binary_search_by_key(&event.timestamp, |bar| bar.timestamp_nanos)
                .unwrap_or_else(|e| e);

            for bar in &mut adjusted[..cutoff_idx] {
                match event.adjustment_type {
                    AdjustmentType::Split(ratio) => {
                        let r = ratio;
                        bar.open = (bar.open as f64 / r) as i64;
                        bar.high = (bar.high as f64 / r) as i64;
                        bar.low = (bar.low as f64 / r) as i64;
                        bar.close = (bar.close as f64 / r) as i64;
                        bar.volume = (bar.volume as f64 * r) as i64;
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
