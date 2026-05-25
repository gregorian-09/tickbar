use memmap2::Mmap;
use smol_str::SmolStr;
use std::fs::File;
use std::path::Path;

/// Packed representation of a single market data tick — 32 bytes.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct Tick {
    /// Nanoseconds since epoch (UTC).
    pub timestamp_nanos: i64,
    /// Price in fixed-point (e.g. x10^8 for 8 decimals).
    pub price: i64,
    /// Volume in fixed-point.
    pub volume: i64,
    /// Bit flags: trade/quote, bid/ask, exchange ID, etc.
    pub flags: u64,
}

impl Tick {
    /// Create a `Tick` from trade data with f64 values.
    pub fn from_trade(timestamp: i64, price: f64, volume: f64) -> Self {
        Tick {
            timestamp_nanos: timestamp,
            price: price as i64,
            volume: volume as i64,
            flags: 0,
        }
    }

    /// Create a `Tick` from quote data.
    pub fn from_quote(timestamp: i64, _bid: f64, _ask: f64, _bid_size: f64, _ask_size: f64) -> Self {
        Tick {
            timestamp_nanos: timestamp,
            price: ((_bid + _ask) / 2.0) as i64,
            volume: (_bid_size + _ask_size) as i64,
            flags: 1,
        }
    }
}

/// How to handle ticks with duplicate timestamps.
pub enum DuplicatePolicy {
    /// Keep the first tick, discard subsequent duplicates.
    First,
    /// Keep the last tick, overwriting earlier duplicates.
    Last,
    /// Keep all ticks (aggregate normally within the bar).
    All,
    /// Return an error on duplicate timestamps.
    Error,
}

/// Contiguous buffer of ticks for a single symbol.
pub struct TickBuffer {
    /// Sorted by timestamp.
    data: Vec<Tick>,
    /// Interned symbol string.
    symbol: SmolStr,
    /// Decimal places for price conversion.
    price_scale: u8,
    /// Decimal places for volume conversion.
    volume_scale: u8,
    /// Whether to allow out-of-order insertion.
    allow_unordered: bool,
    /// How to handle duplicate timestamps.
    duplicate_policy: DuplicatePolicy,
}

impl TickBuffer {
    /// Create a new `TickBuffer` for the given symbol.
    pub fn new(symbol: impl Into<SmolStr>) -> Self {
        TickBuffer {
            data: Vec::new(),
            symbol: symbol.into(),
            price_scale: 8,
            volume_scale: 0,
            allow_unordered: false,
            duplicate_policy: DuplicatePolicy::First,
        }
    }

    /// Push a tick into the buffer.
    ///
    /// Returns `OutOfOrderTick` if the tick's timestamp is earlier
    /// than the last tick and unordered mode is disabled.
    pub fn push(&mut self, tick: Tick) -> Result<(), super::Error> {
        if !self.allow_unordered {
            if let Some(last) = self.data.last()
                && tick.timestamp_nanos < last.timestamp_nanos
            {
                return Err(super::Error::OutOfOrderTick {
                    current: tick.timestamp_nanos,
                    previous: last.timestamp_nanos,
                });
            }
        } else {
            let pos = self
                .data
                .binary_search_by_key(&tick.timestamp_nanos, |t| t.timestamp_nanos)
                .unwrap_or_else(|e| e);
            self.data.insert(pos, tick);
            return Ok(());
        }
        self.data.push(tick);
        Ok(())
    }

    /// Return the symbol for this buffer.
    pub fn symbol(&self) -> &SmolStr {
        &self.symbol
    }

    /// Return a slice of all ticks.
    pub fn as_slice(&self) -> &[Tick] {
        &self.data
    }

    /// Consume the buffer and return the inner tick vector.
    pub fn into_inner(self) -> Vec<Tick> {
        self.data
    }

    /// Set the decimal scale for price conversion.
    pub fn with_price_scale(mut self, scale: u8) -> Self {
        self.price_scale = scale;
        self
    }

    /// Set the decimal scale for volume conversion.
    pub fn with_volume_scale(mut self, scale: u8) -> Self {
        self.volume_scale = scale;
        self
    }

    /// Enable or disable out-of-order insertion.
    pub fn with_allow_unordered(mut self, allow: bool) -> Self {
        self.allow_unordered = allow;
        self
    }

    /// Set the duplicate timestamp handling policy.
    pub fn with_duplicate_policy(mut self, policy: DuplicatePolicy) -> Self {
        self.duplicate_policy = policy;
        self
    }
}

/// Iterator over `Tick` values from a memory-mapped file.
///
/// Expects the file to contain consecutive 32-byte binary `Tick` records.
pub struct MmapTickReader {
    mmap: Mmap,
    cursor: usize,
}

impl MmapTickReader {
    /// Open and memory-map a file for tick reading.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = File::open(path.as_ref())?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(MmapTickReader { mmap, cursor: 0 })
    }

    /// Return the number of remaining ticks.
    pub fn remaining(&self) -> usize {
        let remaining_bytes = self.mmap.len().saturating_sub(self.cursor);
        remaining_bytes / 32
    }
}

impl Iterator for MmapTickReader {
    type Item = Tick;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor + 32 > self.mmap.len() {
            return None;
        }
        let tick = unsafe { std::ptr::read(self.mmap[self.cursor..].as_ptr() as *const Tick) };
        self.cursor += 32;
        Some(tick)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_from_trade() {
        let t = Tick::from_trade(1_000_000, 100.5, 500.0);
        assert_eq!(t.timestamp_nanos, 1_000_000);
        assert_eq!(t.price, 100);
        assert_eq!(t.volume, 500);
    }

    #[test]
    fn test_tick_from_quote() {
        let t = Tick::from_quote(1_000_000, 100.0, 101.0, 1000.0, 500.0);
        assert_eq!(t.timestamp_nanos, 1_000_000);
        assert_eq!(t.price, 100);
        assert_eq!(t.flags, 1);
    }

    #[test]
    fn test_tick_buffer_push_ordered() {
        let mut buf = TickBuffer::new("AAPL");
        buf.push(Tick::from_trade(0, 100.0, 1000.0)).unwrap();
        buf.push(Tick::from_trade(1_000_000_000, 101.0, 500.0))
            .unwrap();
        assert_eq!(buf.as_slice().len(), 2);
    }

    #[test]
    fn test_tick_buffer_out_of_order_rejected() {
        let mut buf = TickBuffer::new("AAPL");
        buf.push(Tick::from_trade(1_000_000_000, 100.0, 1000.0))
            .unwrap();
        let err = buf.push(Tick::from_trade(0, 101.0, 500.0));
        assert!(err.is_err());
    }

    #[test]
    fn test_tick_buffer_allow_unordered() {
        let mut buf = TickBuffer::new("AAPL").with_allow_unordered(true);
        buf.push(Tick::from_trade(1_000_000_000, 100.0, 1000.0))
            .unwrap();
        buf.push(Tick::from_trade(0, 101.0, 500.0)).unwrap();
        // Should now be sorted: [ts=0, ts=1_000_000_000]
        assert_eq!(buf.as_slice()[0].timestamp_nanos, 0);
        assert_eq!(buf.as_slice()[1].timestamp_nanos, 1_000_000_000);
    }
}
