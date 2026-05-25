use crate::bar::BarSeries as RustBarSeries;
use crate::tick::Tick as RustTick;
use crate::aggregator::TickAggregator as RustTickAggregator;
use pyo3::prelude::*;

/// A single market data tick.
#[pyclass(name = "Tick", from_py_object)]
#[derive(Clone)]
pub struct PyTick {
    inner: RustTick,
}

#[pymethods]
impl PyTick {
    #[new]
    fn new(timestamp: i64, price: f64, volume: f64) -> Self {
        PyTick {
            inner: RustTick::from_trade(timestamp, price, volume),
        }
    }

    fn __repr__(&self) -> String {
        format!("Tick(ts={}, price={}, volume={})",
            self.inner.timestamp_nanos, self.inner.price, self.inner.volume)
    }
}

/// A series of aggregated OHLCV bars.
#[pyclass(name = "BarSeries")]
pub struct PyBarSeries {
    inner: RustBarSeries,
}

#[pymethods]
impl PyBarSeries {
    fn __repr__(&self) -> String {
        format!("BarSeries({} bars)", self.inner.as_slice().len())
    }
}

/// High-performance tick-to-bar aggregator.
#[pyclass(name = "TickAggregator")]
pub struct PyTickAggregator {
    inner: RustTickAggregator,
}

#[pymethods]
impl PyTickAggregator {
    #[new]
    fn new(interval_secs: u64) -> PyResult<Self> {
        let agg = RustTickAggregator::builder()
            .interval(std::time::Duration::from_secs(interval_secs))
            .build()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(PyTickAggregator { inner: agg })
    }

    fn push_tick(&mut self, tick: &PyTick) -> PyResult<()> {
        self.inner
            .push_tick(tick.inner)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        "TickAggregator".to_string()
    }
}

/// Register the `tickbar` Python module.
#[pymodule]
fn tickbar(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTick>()?;
    m.add_class::<PyBarSeries>()?;
    m.add_class::<PyTickAggregator>()?;
    Ok(())
}
