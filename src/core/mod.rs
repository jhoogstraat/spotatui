// Frontend-neutral action vocabulary; retained for the GPUI command surface.
#[allow(dead_code)]
pub mod action;
// TEMPORARY (ticket 01): dead-code allows for surface whose only caller was
// the deleted terminal UI. Ticket 02 deletes that surface wholesale; these
// lines go with it.
#[allow(dead_code)]
pub mod app;
pub mod catalog;
mod catalog_apply;
pub mod streaming;
#[cfg(feature = "streaming")]
mod streaming_apply;

// Re-exported for producers/fold; until the handler migration commit in this
// ticket, no non-test consumer exists on some legs.
#[allow(unused_imports)]
pub use catalog::{CatalogOutcome, CatalogState, CatalogStateEvent};
pub use catalog_apply::apply_catalog_state;
#[allow(unused_imports)]
pub use streaming::{AbsorbOutcome, StreamingState, StreamingStateEvent};
#[cfg(feature = "streaming")]
#[allow(unused_imports)]
pub use streaming_apply::apply_streaming_state;

pub mod auth;
pub mod banner;
pub mod config;
#[allow(dead_code)]
pub mod first_run;
#[allow(dead_code)]
pub mod format;
pub mod geometry;
#[allow(dead_code)]
pub mod input;
pub mod limits;
pub mod migrations;
pub mod onboarding;
pub mod pagination;
pub mod paths;
pub mod persisted_playback;
pub mod plugin_api;
pub mod queue;
#[allow(dead_code)]
pub mod sort;
pub mod source;
pub mod state;
#[cfg(test)]
pub mod test_helpers;
#[allow(dead_code)]
pub mod theme;
pub mod user_config;
