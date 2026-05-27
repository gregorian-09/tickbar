/// Strategy for aligning bar boundaries to a reference time.
#[derive(Clone, Copy, Debug)]
pub enum TimeAlignment {
    /// Align to the UTC day boundary (midnight).
    UTC,
    /// Custom offset in nanoseconds applied to timestamps.
    Custom(i64),
}

impl TimeAlignment {
    /// Align a timestamp to the bar start boundary.
    pub fn align(self, timestamp_nanos: i64) -> i64 {
        match self {
            TimeAlignment::UTC => {
                let nanos_per_day = 86_400_000_000_000;
                timestamp_nanos - (timestamp_nanos % nanos_per_day)
            }
            TimeAlignment::Custom(offset_ns) => timestamp_nanos + offset_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utc_alignment() {
        let aligned = TimeAlignment::UTC.align(0);
        assert_eq!(aligned, 0);

        let aligned = TimeAlignment::UTC.align(100_000_000_000);
        assert_eq!(aligned, 0);
    }

    #[test]
    fn test_custom_alignment() {
        let aligned = TimeAlignment::Custom(0).align(100);
        assert_eq!(aligned, 100);

        let aligned = TimeAlignment::Custom(10_000).align(1_000);
        assert_eq!(aligned, 11_000);
    }
}
