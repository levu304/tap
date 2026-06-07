use pyo3::prelude::*;

/// Tap CDC — Change Data Capture for Python.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TapCore>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

/// Internal Rust capture engine exposed via pyo3.
#[pyclass]
struct TapCore {
    // Stub: fields will be added in Phase 2
}

#[pymethods]
#[allow(unsafe_op_in_unsafe_fn, clippy::useless_conversion)]
impl TapCore {
    #[new]
    fn new() -> Self {
        TapCore {}
    }

    /// Placeholder: start capture
    fn start(&self) -> PyResult<()> {
        Ok(())
    }

    /// Placeholder: stop capture
    fn stop(&self) -> PyResult<()> {
        Ok(())
    }
}
