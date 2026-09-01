//! Shared decoded-audio output engine.
//!
//! System-output capture and FFT visualization were terminal presentation
//! concerns and are intentionally not part of the playback engine. A future
//! Frontends can subscribe to playback telemetry without reopening an audio
//! capture device here.

#[cfg(feature = "audio-decode")]
mod player;
#[cfg(feature = "audio-decode")]
pub use player::LocalPlayer;
