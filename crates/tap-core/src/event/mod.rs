//! Change events — core data structures for representing database changes.
//! Includes the `ChangeEvent` envelope, `SourceMetadata`, `Operation` enum,
//! and the `ChangeEventBuilder`.

pub mod builder;
pub mod envelope;

pub use builder::ChangeEventBuilder;
pub use envelope::{ChangeEvent, Lsn, Operation, SourceMetadata};
