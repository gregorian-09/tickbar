/// Strategy for aligning bar boundaries to a reference time.
#[derive(Clone, Copy, Debug)]
pub enum TimeAlignment {
    /// Align to the exchange timezone (e.g. 9:30 ET).
    Exchange,
    /// Align to UTC midnight.
    UTC,
    /// Custom offset in nanoseconds from UTC.
    Custom(i64),
}

impl TimeAlignment {
    /// Align a timestamp to the nearest bar boundary.
    pub fn align(self, timestamp_nanos: i64) -> i64 {
        match self {
            TimeAlignment::Exchange | TimeAlignment::UTC => {
                let nanos_per_day = 86_400_000_000_000;
                let remainder = timestamp_nanos % nanos_per_day;
                timestamp_nanos - remainder + nanos_per_day
            }
            TimeAlignment::Custom(offset_ns) => {
                timestamp_nanos + offset_ns
            }
        }
    }
}
