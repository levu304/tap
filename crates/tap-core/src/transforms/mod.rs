//! Data transform engine.
//!
//! Applies user-defined transformations (e.g. field renaming, filtering,
//! type coercion) to CDC events before they reach downstream consumers.
//!
//! # Modules
//!
//! * [`config`] — Configuration types for transform pipelines.
//! * [`engine`] — Wasmtime-backed transform runtime (QuickJS WASM).
//! * [`filter`] — JavaScript filter-transform implementation.
//! * [`map`] — JavaScript map-transform implementation.
//! * [`mask`] — Declarative field-masking implementation.
//! * [`validate`] — Post-transform envelope validation.

pub mod config;
pub mod engine;
pub mod mask;
pub mod validate;
