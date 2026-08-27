#[cfg(feature = "art-decode")]
use super::*;

impl App {
  /// The theme to build on when the adaptive theme applies: the saved base
  /// while active or restoring, otherwise the user's own.
  pub fn user_theme(&self) -> crate::core::user_config::Theme {
    use crate::core::cover_theme::CoverThemeState;
    match self.cover_theme_state {
      CoverThemeState::Active { base } | CoverThemeState::Restoring { base } => base,
      CoverThemeState::Inactive => self.user_config.theme,
    }
  }

  #[cfg(not(feature = "art-decode"))]
  pub fn user_theme(&self) -> crate::core::user_config::Theme {
    self.user_config.theme
  }

  /// Store freshly decoded cover art together with the palette extracted from
  /// it. The single entry point that keeps the adaptive theme in sync with the
  /// art: `None` for the palette fades back to the user's own theme.
  #[cfg(feature = "art-decode")]
  pub fn store_cover_art(
    &mut self,
    key: String,
    img: image::DynamicImage,
    palette: Option<crate::core::cover_theme::AlbumPalette>,
  ) {
    self.cover_art.store_decoded(key, img);
    match palette {
      Some(palette) => self.set_cover_art_palette(palette),
      None => self.clear_cover_art_palette(),
    }
  }

  /// Drop the stored cover art together with its palette; the adaptive theme
  /// follows the art and fades back to the user's own theme.
  #[cfg(feature = "art-decode")]
  pub fn clear_cover_art(&mut self) {
    self.cover_art.clear();
    self.clear_cover_art_palette();
  }

  /// Store the palette extracted from freshly loaded cover art and, when
  /// Adaptive Theme is on, fade the UI accents toward it.
  #[cfg(feature = "art-decode")]
  fn set_cover_art_palette(&mut self, palette: crate::core::cover_theme::AlbumPalette) {
    self.cover_art_palette = Some(palette);
    self.apply_cover_theme();
  }

  /// Drop the stored palette and fade back to the user's own theme. A cheap
  /// no-op when nothing is applied, so it is safe on every tick of the
  /// "no art" path.
  #[cfg(feature = "art-decode")]
  fn clear_cover_art_palette(&mut self) {
    self.cover_art_palette = None;
    self.restore_cover_theme();
  }

  /// Fade toward the theme derived from the stored palette, capturing the
  /// user's theme as the restore base on first application.
  #[cfg(feature = "art-decode")]
  fn apply_cover_theme(&mut self) {
    use crate::core::cover_theme::{derive_theme, CoverThemeState};
    if !self.user_config.behavior.cover_art_theme {
      return;
    }
    let Some(palette) = self.cover_art_palette else {
      return;
    };
    // Re-applying while active or mid-restore keeps the original base; only
    // from Inactive is the live theme really the user's own (a fade-out in
    // flight leaves a blend in `user_config.theme` that must not be captured).
    let base = self.user_theme();
    self.cover_theme_state = CoverThemeState::Active { base };
    let target = derive_theme(&base, &palette);
    self.begin_theme_transition(target);
  }

  /// Fade back to the user's own theme, if an album theme is applied.
  #[cfg(feature = "art-decode")]
  fn restore_cover_theme(&mut self) {
    use crate::core::cover_theme::CoverThemeState;
    if let CoverThemeState::Active { base } = self.cover_theme_state {
      self.cover_theme_state = if self.begin_theme_transition(base) {
        CoverThemeState::Restoring { base }
      } else {
        CoverThemeState::Inactive
      };
    }
  }

  /// Start fading the live theme toward `target`. Returns false when the
  /// theme is already there (nothing to animate). A transition already headed
  /// to the same target is left to finish rather than restarted.
  #[cfg(feature = "art-decode")]
  fn begin_theme_transition(&mut self, target: crate::core::user_config::Theme) -> bool {
    use crate::core::cover_theme::ThemeTransition;
    if let Some(transition) = &self.theme_transition {
      if transition.target() == target {
        return true;
      }
    }
    if self.user_config.theme == target {
      self.theme_transition = None;
      return false;
    }
    self.theme_transition = Some(ThemeTransition::new(self.user_config.theme, target));
    true
  }

  /// Reconcile the adaptive theme after `apply_settings_changes` rewrote
  /// `user_config.theme` from the settings rows (which every save does, for
  /// every row). The rows carry the user's own colors, so that rewrite is the
  /// new base; what was actually on screen (`live_before`) is put back so the
  /// fade continues from there. `enabled_before` is the Adaptive Theme flag
  /// before the save, so a flip re-applies or restores.
  #[cfg(feature = "art-decode")]
  pub(super) fn reconcile_cover_theme_after_settings(
    &mut self,
    live_before: crate::core::user_config::Theme,
    enabled_before: bool,
  ) {
    use crate::core::cover_theme::CoverThemeState;
    match self.cover_theme_state {
      CoverThemeState::Inactive => {}
      previous => {
        let base = self.user_config.theme;
        self.cover_theme_state = CoverThemeState::Active { base };
        self.user_config.theme = live_before;
        self.theme_transition = None;
        if matches!(previous, CoverThemeState::Restoring { .. }) {
          self.restore_cover_theme();
        } else {
          self.apply_cover_theme();
        }
      }
    }
    if self.user_config.behavior.cover_art_theme != enabled_before {
      if self.user_config.behavior.cover_art_theme {
        self.apply_cover_theme();
      } else {
        self.restore_cover_theme();
      }
    }
  }
}
