/// Convert an f64 value to an i64 fixed-point representation.
pub fn f64_to_fixed(value: f64, decimals: u8) -> i64 {
    (value * 10f64.powi(decimals as i32)) as i64
}

/// Convert an i64 fixed-point value back to f64.
pub fn fixed_to_f64(value: i64, decimals: u8) -> f64 {
    value as f64 / 10f64.powi(decimals as i32)
}
