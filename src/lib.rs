//! spotatui as a library: one core shared by every frontend.
//!
//! The modules stay private; the public API is exactly the entry points the
//! bin shims in `src/bin/` need, plus the `frontend` runtime module a
//! second frontend embeds.

mod cli;
mod core;
#[cfg(test)]
mod gates;
mod infra;
mod runtime;

pub use runtime::frontend;
pub use runtime::run_cli;
