// `action` is allowed dead without `scripting`: the scripting engine and the
// DJ tools are still its only producers, so a scripting-less leg counts the
// un-adopted surface as dead.
#[cfg_attr(not(feature = "scripting"), allow(dead_code))]
pub mod action;
// TEMPORARY (ticket 01): dead-code allows for surface whose only caller was
// the deleted terminal UI. Ticket 02 deletes that surface wholesale; these
// lines go with it.
#[allow(dead_code)]
pub mod app;
pub mod streaming;

#[cfg(feature = "art-decode")]
pub mod art;
pub mod auth;
pub mod banner;
pub mod config;
#[cfg(feature = "art-decode")]
pub mod cover_theme;
#[allow(dead_code)]
pub mod driver;
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
