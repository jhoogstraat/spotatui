//! Frontend-neutral viewport geometry.
//!
//! `Viewport` is the rendering surface size in the frontend's own cells
//! (terminal cells for the TUI). It replaces `ratatui::layout::Size` in `App`
//! state so `core/` carries no terminal-toolkit types.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(dead_code)] // TEMPORARY (ticket 02): consumed only by the deleted ViewState.
pub struct Viewport {
  pub width: u16,
  pub height: u16,
}
