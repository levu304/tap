use napi_derive::napi;

/// Returns the Tap version string.
#[napi]
pub fn hello() -> String {
    "tap".into()
}
