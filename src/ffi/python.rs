use crate::bar::BarSeries as RustBarSeries;
use crate::tick::Tick as RustTick;
use crate::aggregator::TickAggregator as RustTickAggregator;
use pyo3::buffer::PyBuffer;
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

    /// Return bars as a list of [timestamp_nanos, open, high, low, close, volume, tick_count, vwap].
    fn to_records(&self) -> Vec<[f64; 8]> {
        self.inner
            .as_slice()
            .iter()
            .map(|b| {
                [
                    b.timestamp_nanos as f64,
                    b.open as f64,
                    b.high as f64,
                    b.low as f64,
                    b.close as f64,
                    b.volume as f64,
                    b.tick_count as f64,
                    b.vwap as f64,
                ]
            })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.inner.as_slice().len()
    }
}

/// High-performance tick-to-bar aggregator.
#[pyclass(name = "TickAggregator")]
pub struct PyTickAggregator {
    inner: Option<RustTickAggregator>,
}

#[pymethods]
impl PyTickAggregator {
    #[new]
    fn new(interval_secs: u64) -> PyResult<Self> {
        let agg = RustTickAggregator::builder()
            .interval(std::time::Duration::from_secs(interval_secs))
            .build()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(PyTickAggregator { inner: Some(agg) })
    }

    fn push_tick(&mut self, tick: &PyTick) -> PyResult<()> {
        self.inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("aggregator already finalized"))?
            .push_tick(tick.inner)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        "TickAggregator".to_string()
    }

    /// Push a batch of ticks (Python Tick objects).
    fn push_ticks(&mut self, ticks: Vec<PyTick>) -> PyResult<()> {
        let rust_ticks: Vec<RustTick> = ticks.into_iter().map(|t| t.inner).collect();
        self.inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("aggregator already finalized"))?
            .push_ticks(&rust_ticks)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Push ticks from raw `bytes` containing packed Tick structs (32 bytes each).
    /// Uses the unchecked (no ordering validation) ingest for maximum speed.
    fn push_from_bytes(&mut self, data: &[u8]) -> PyResult<()> {
        let agg = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("aggregator already finalized"))?;

        let tick_size = std::mem::size_of::<RustTick>();
        let n = data.len() / tick_size;
        let ticks: &[RustTick] = unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const RustTick, n)
        };
        agg.aggregator.ingest_ticks_unchecked(ticks);
        Ok(())
    }

    /// Push ticks from three Python lists/copies of ints.
    fn push_from_arrays(
        &mut self,
        timestamps: Vec<i64>,
        prices: Vec<i64>,
        volumes: Vec<i64>,
    ) -> PyResult<()> {
        let agg = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("aggregator already finalized"))?;
        agg.aggregator.ingest_from_arrays(&timestamps, &prices, &volumes);
        Ok(())
    }

    /// Push ticks from three numpy int64 arrays via __array_interface__ (zero-copy).
    fn push_from_numpy(
        &mut self,
        _py: Python<'_>,
        timestamps: Bound<'_, PyAny>,
        prices: Bound<'_, PyAny>,
        volumes: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let agg = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("aggregator already finalized"))?;

        fn array_ptr(obj: &Bound<'_, PyAny>) -> PyResult<*const i64> {
            let iface = obj.getattr("__array_interface__")?;
            let data: (usize, bool) = iface.get_item("data")?.extract()?;
            Ok(data.0 as *const i64)
        }
        fn array_len(obj: &Bound<'_, PyAny>) -> PyResult<usize> {
            let iface = obj.getattr("__array_interface__")?;
            let shape: Vec<usize> = iface.get_item("shape")?.extract()?;
            Ok(shape[0])
        }

        let n = array_len(&timestamps)?
            .min(array_len(&prices)?)
            .min(array_len(&volumes)?);
        let ts_ptr = array_ptr(&timestamps)?;
        let pr_ptr = array_ptr(&prices)?;
        let vo_ptr = array_ptr(&volumes)?;

        let timestamps = unsafe { std::slice::from_raw_parts(ts_ptr, n) };
        let prices = unsafe { std::slice::from_raw_parts(pr_ptr, n) };
        let volumes = unsafe { std::slice::from_raw_parts(vo_ptr, n) };

        agg.aggregator.ingest_from_arrays(timestamps, prices, volumes);
        Ok(())
    }

    /// Push ticks from three buffer-protocol arrays (numpy, memoryview, bytes, etc.).
    /// Zero-copy via the Python buffer protocol (PEP 3118).
    /// Faster than push_from_numpy because it avoids Python-level attribute access.
    fn push_from_buffer(
        &mut self,
        _py: Python<'_>,
        timestamps: PyBuffer<i64>,
        prices: PyBuffer<i64>,
        volumes: PyBuffer<i64>,
    ) -> PyResult<()> {
        let agg = self
            .inner
            .as_mut()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("aggregator already finalized"))?;

        let n = timestamps.item_count().min(prices.item_count()).min(volumes.item_count());

        // Safety: ReadOnlyCell<i64> is #[repr(transparent)] over UnsafeCell<i64>
        // which is #[repr(transparent)] over i64 – same layout & alignment.
        // We use buf_ptr() instead of as_slice() so we can truncate to min(n).
        let ts =
            unsafe { std::slice::from_raw_parts(timestamps.buf_ptr() as *const i64, n) };
        let pr =
            unsafe { std::slice::from_raw_parts(prices.buf_ptr() as *const i64, n) };
        let vo =
            unsafe { std::slice::from_raw_parts(volumes.buf_ptr() as *const i64, n) };

        agg.aggregator.ingest_from_arrays(ts, pr, vo);
        Ok(())
    }

    /// Finalize aggregation and return the bar series.
    fn finalize(&mut self) -> PyResult<PyBarSeries> {
        let agg = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("aggregator already finalized")
        })?;
        let series = agg.finalize();
        Ok(PyBarSeries { inner: series })
    }
}

/// Register the tickbar native extension.
#[pymodule]
fn tickbar(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTick>()?;
    m.add_class::<PyBarSeries>()?;
    m.add_class::<PyTickAggregator>()?;
    Ok(())
}
