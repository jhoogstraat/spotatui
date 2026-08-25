//! spotatui as a library: one core shared by every frontend.
//!
//! The modules stay private; the public API is exactly the entry points the
//! bin shims in `src/bin/` need, plus the `frontend` runtime module a
//! second frontend embeds. The terminal frontend is gated behind the `tui`
//! feature: a build without it (the headless CI leg) turns any import of
//! the terminal module from core/infra/cli into a compile error, which is
//! what keeps a second frontend from re-coupling to this one.

mod cli;
mod core;
#[cfg(test)]
mod gates;
mod infra;
mod runtime;
#[cfg(feature = "tui")]
mod tui;

pub use runtime::frontend;
pub use runtime::run_cli;
