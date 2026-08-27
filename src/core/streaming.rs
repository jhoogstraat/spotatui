//! The streaming domain (ADR 0016 stage 3, ticket 14): playback state arrives
//! as typed spine events and is folded by one owner.
//!
//! Producers — the librespot event handler first, then the recovery teardown
//! and the boot-time backend-pending flip — emit a
//! [`StreamingStateEvent`] instead of writing shared `App` fields. The fold
//! task feeds every event through [`StreamingState::absorb`], which reduces it
//! into the authoritative playback facts and reports which surface moved so
//! the snapshot publisher can dedup on real change.
//!
//! Until ticket 18 dissolves the mega-mutex, `App` keeps the legacy mirror
//! fields (`native_is_playing`, `song_progress_ms`, …); only the fold writes
//! them, via [`crate::core::streaming_apply::apply_streaming_state`].
//!
//! Producers arrive in this same ticket; until the handler migration commit
//! lands, CI legs compile this module without a consumer, so dead-code stays
//! allowed at module scope (deleted when the last consumer wires up).
#![cfg_attr(any(not(feature = "streaming"), not(test)), allow(dead_code))]

use crate::core::app::NativeTrackInfo;

/// One observed native-streaming fact. Payload-only: everything a producer
/// knows about the moment, nothing about how state should react — reactions
/// (recovery observers, queue continuation) belong to `App` methods the fold
/// calls after absorbing.
#[derive(Debug, Clone)]
pub enum StreamingStateEvent {
  /// Audio is audibly advancing at this position.
  Playing { track_uri: String, position_ms: u32 },
  /// Playback paused at this position (or never started).
  Paused { track_uri: String, position_ms: u32 },
  /// A completed seek repositioned the track.
  Seeked { position_ms: u32 },
  /// librespot committed to loading this track.
  Loading { track_uri: String, position_ms: u32 },
  /// Metadata for the now-decoding track arrived.
  TrackChanged {
    info: NativeTrackInfo,
    track_uri: String,
  },
  /// The player stopped; no native metadata is current.
  Stopped,
  /// A track finished. Ordering matters here: the decision payload below is
  /// what the *handler* computed from `App`'s own queue/recovery logic before
  /// any mirror was touched.
  EndOfTrack {
    /// Whether progress should show zero while the next item loads.
    reset_progress: bool,
    /// Whether stop-after-current-track consumed this end.
    stop_after_current: bool,
  },
  /// librespot reported the device volume changed (physical or Connect sync).
  VolumeChanged { volume_percent: u8 },
  /// Periodic decoder clock: librespot's interstitial position updates.
  PositionChanged { position_ms: u32 },
}

impl StreamingStateEvent {
  /// Which published surface this event can move. The fold uses it to skip
  /// snapshot rebuilds that cannot differ.
  pub fn surface(&self) -> Surface {
    match self {
      StreamingStateEvent::Playing { .. }
      | StreamingStateEvent::Paused { .. }
      | StreamingStateEvent::Seeked { .. }
      | StreamingStateEvent::Loading { .. }
      | StreamingStateEvent::TrackChanged { .. }
      | StreamingStateEvent::Stopped
      | StreamingStateEvent::EndOfTrack { .. } => Surface::Playback,
      StreamingStateEvent::VolumeChanged { .. } => Surface::Volume,
      // Position refinements do not change any published field shape except
      // progress; they are still Playback-surface work, but the fold treats
      // them as cheap without being free — hence their own tag for tests.
      StreamingStateEvent::PositionChanged { .. } => Surface::Position,
    }
  }
}

/// Which observable surface a folded event touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
  Playback,
  Volume,
  Position,
}

/// A seek the fold must issue toward the player because an optimistic UI seek
/// outran confirmation. Absorbing it records the pending target; the App-level
/// flush path (existing throttle machinery) owns actually sending it.
/// Reserved for ticket 16's command-side seek consolidation.
#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeekIntent {
  pub position_ms: u32,
}

/// Authoritative streaming-playback facts, reduced in stream order.
///
/// The stale-position rule is order-based, not clock-based: librespot
/// positions are monotonic within one track, and every legitimate backward
/// step (new track, confirmed seek) carries an explicit marker in its event.
/// Anything else moving the position backwards mid-track is a delayed duplicate
/// from an older connection and must lose to the newer fact already folded.
#[derive(Debug, Clone, Default)]
pub struct StreamingState {
  /// `Some(true/false)` once playback state is known; `None` before any
  /// transport event. Legacy mirror of `native_is_playing`.
  pub is_playing: Option<bool>,
  /// The last authoritative position, in milliseconds.
  pub position_ms: u128,
  /// Monotonic floor guard against stale duplicates (see type doc).
  position_floor: u128,
  /// Whether `position_ms` is anchored to the current track. A load
  /// de-anchors progress: positions arriving before the new track's first
  /// transport event belong to the old track in either direction and are
  /// dropped wholesale.
  anchored: bool,
  /// The currently decoding track's immediate metadata.
  pub track_info: Option<NativeTrackInfo>,
  /// Bare base62 identity of the current track (full URI for local files).
  pub track_id: Option<String>,
}

/// What absorbing one event produced. Mirrors are written when `mirrors`
/// says anything visible moved; the bools separate "real info" changes from
/// pure position advances so the fold can rebuild only what it must.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsorbOutcome {
  pub info_changed: bool,
  pub play_state_changed: bool,
  pub position_changed: bool,
}

impl AbsorbOutcome {
  /// No published surface moved.
  pub fn none() -> Self {
    AbsorbOutcome {
      info_changed: false,
      play_state_changed: false,
      position_changed: false,
    }
  }

  fn with_info() -> Self {
    AbsorbOutcome {
      info_changed: true,
      play_state_changed: false,
      position_changed: false,
    }
  }

  fn with_play_state() -> Self {
    AbsorbOutcome {
      info_changed: false,
      play_state_changed: true,
      position_changed: false,
    }
  }

  fn with_position() -> Self {
    AbsorbOutcome {
      info_changed: false,
      play_state_changed: false,
      position_changed: true,
    }
  }

  /// Any published field could differ after this absorb.
  pub fn any(&self) -> bool {
    self.info_changed || self.play_state_changed || self.position_changed
  }
}

impl StreamingState {
  /// Reduce one event in stream order. Returns what moved.
  pub fn absorb(&mut self, event: &StreamingStateEvent) -> AbsorbOutcome {
    match event {
      StreamingStateEvent::Playing {
        track_uri,
        position_ms,
      } => {
        let (new_track, mut outcome) = self.adopt_identity_if_new(track_uri);
        if !self.is_playing.unwrap_or(false) {
          self.is_playing = Some(true);
          outcome.play_state_changed = true;
        }
        // Authoritative across a track boundary or as the first transport
        // fact after one: mid-track backward jumps are stale duplicates
        // (connection swap replays).
        outcome.position_changed = self.advance_position(*position_ms, new_track || !self.anchored);
        outcome
      }
      StreamingStateEvent::Paused {
        track_uri,
        position_ms,
      } => {
        let (new_track, mut outcome) = self.adopt_identity_if_new(track_uri);
        if self.is_playing.unwrap_or(true) {
          self.is_playing = Some(false);
          outcome.play_state_changed = true;
        }
        outcome.position_changed = self.advance_position(*position_ms, new_track || !self.anchored);
        outcome
      }
      StreamingStateEvent::Seeked { position_ms } => {
        // A confirmed seek wins over every earlier fact: authoritative.
        let changed = self.advance_position(*position_ms, true);
        AbsorbOutcome::none().set_position_changed(changed)
      }
      StreamingStateEvent::Loading {
        track_uri,
        position_ms: _,
      } => {
        let (new_track, outcome) = self.adopt_identity_if_new(track_uri);
        if !new_track {
          // Re-load of the same track: mirrors today's no-op.
          return AbsorbOutcome::none();
        }
        // Mirror parity: `native_is_playing` survives a load (mirrors today's
        // handler). Progress holds the old value but de-anchors, so neither
        // direction of stale position can land until the new track's first
        // transport/seek fact arrives.
        self.deanchor();
        outcome
      }
      StreamingStateEvent::TrackChanged { info, track_uri } => {
        let (_new_track, mut outcome) = self.adopt_identity_if_new(track_uri);
        self.track_info = Some(info.clone());
        outcome.info_changed = true;
        // Fresh metadata always restarts progress at zero, de-anchored until
        // the first transport/seek fact for this track arrives.
        outcome.position_changed = self.reset_progress_unanchored();
        self.deanchor();
        outcome
      }
      StreamingStateEvent::Stopped => {
        let mut outcome = AbsorbOutcome::none();
        if self.track_info.is_some() || self.track_id.is_some() {
          self.track_info = None;
          self.track_id = None;
          outcome.info_changed = true;
        }
        if self.is_playing != Some(false) {
          self.is_playing = Some(false);
          outcome.play_state_changed = true;
        }
        outcome.position_changed = self.advance_position(0, true);
        outcome
      }
      StreamingStateEvent::EndOfTrack { .. } => {
        // Mirror effects ride [`StreamingState::reset_for_track_end`], which
        // the fold calls once the handler's continuation decision ran; the
        // reducer itself only reports the boundary.
        AbsorbOutcome::none().set_play_state_changed(true)
      }
      StreamingStateEvent::VolumeChanged { volume_percent } => {
        let _ = volume_percent;
        AbsorbOutcome::none()
      }
      StreamingStateEvent::PositionChanged { position_ms } => {
        let changed = self.advance_position(*position_ms, false);
        AbsorbOutcome::none().set_position_changed(changed)
      }
    }
  }

  /// Install a new track identity if `track_uri` differs from the folded one.
  /// Returns whether a boundary happened (metadata cleared, floor lowered).
  fn adopt_identity_if_new(&mut self, track_uri: &str) -> (bool, AbsorbOutcome) {
    let id = crate::core::app::base62_id_of(track_uri).to_string();
    if self.track_id.as_deref() == Some(id.as_str()) {
      return (false, AbsorbOutcome::none());
    }
    self.track_id = Some(id);
    self.track_info = None;
    self.position_floor = 0;
    (true, AbsorbOutcome::with_info())
  }

  /// Advance toward `position_ms`. Returns whether the stored value moved.
  ///
  /// `authoritative` marks events that may legitimately move backwards
  /// (a confirmed seek or a track boundary): they reset the floor.
  /// Everything else is subject to the floor — late duplicates of old
  /// positions are dropped instead of scrubbing the playbar backwards.
  fn advance_position(&mut self, position_ms: u32, authoritative: bool) -> bool {
    let position = u128::from(position_ms);
    if authoritative {
      self.position_floor = position;
    } else {
      if !self.anchored {
        // A boundary event is still waiting for its transport/seek fact:
        // positions arriving meanwhile belong to the previous track and are
        // dropped wholesale, whichever direction they point.
        return false;
      }
      if position < self.position_floor {
        return false;
      }
      self.position_floor = position;
    }
    self.anchored = true;
    if self.position_ms == position {
      return false;
    }
    self.position_ms = position;
    true
  }

  /// Drop un-anchored progress: called after boundary events so stale
  /// positions from the previous track cannot contribute.
  fn reset_progress_unanchored(&mut self) -> bool {
    self.position_floor = 0;
    self.anchored = false;
    let changed = self.position_ms != 0;
    self.position_ms = 0;
    changed
  }

  /// Keep the current progress but de-anchor it: no further PositionChanged
  /// lands until the new track's transport/seek fact arrives.
  fn deanchor(&mut self) {
    self.anchored = false;
  }

  /// Explicit end-of-track mirror reset chosen by the producer.
  pub fn reset_for_track_end(&mut self, keep_track_metadata: bool) {
    self.reset_progress_unanchored();
    self.is_playing = Some(false);
    if !keep_track_metadata {
      self.track_info = None;
      self.track_id = None;
    }
  }

  /// Drop all state: the session ended.
  pub fn clear(&mut self) {
    self.reset_progress_unanchored();
    self.is_playing = Some(false);
    self.track_info = None;
    self.track_id = None;
  }
}

impl AbsorbOutcome {
  fn set_position_changed(mut self, changed: bool) -> Self {
    self.position_changed = changed;
    self
  }

  fn set_play_state_changed(mut self, changed: bool) -> Self {
    self.play_state_changed = changed;
    self
  }
}

#[cfg(test)]
mod streaming_domain_tests {
  use super::*;
  use crate::core::app::NativeTrackKind;

  fn playing(uri: &str, position_ms: u32) -> StreamingStateEvent {
    StreamingStateEvent::Playing {
      track_uri: uri.to_string(),
      position_ms,
    }
  }

  fn paused(uri: &str, position_ms: u32) -> StreamingStateEvent {
    StreamingStateEvent::Paused {
      track_uri: uri.to_string(),
      position_ms,
    }
  }

  fn info(name: &str) -> NativeTrackInfo {
    NativeTrackInfo {
      name: name.to_string(),
      artists: vec!["artist".to_string()],
      album: String::new(),
      duration_ms: 200_000,
      kind: NativeTrackKind::Track,
      image_url: None,
    }
  }

  #[test]
  fn playing_then_paused_folds_into_known_play_state() {
    let mut state = StreamingState::default();
    assert!(state.absorb(&playing("spotify:track:abc", 1_000)).any());
    let outcome = state.absorb(&paused("spotify:track:abc", 1_500));
    assert!(outcome.play_state_changed);
    assert_eq!(state.is_playing, Some(false));
    assert_eq!(state.position_ms, 1_500);
  }

  #[test]
  fn stale_mid_track_position_cannot_scrub_backwards() {
    // Ticket 14's stale-position rule: PositionChanged duplicates that arrive
    // out of order must lose to the newer fact already folded.
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 0));
    state.absorb(&StreamingStateEvent::PositionChanged {
      position_ms: 30_000,
    });
    let outcome = state.absorb(&StreamingStateEvent::PositionChanged {
      position_ms: 12_000,
    });
    assert!(!outcome.any(), "a backward duplicate is a no-op");
    assert_eq!(state.position_ms, 30_000);
  }

  #[test]
  fn confirmed_seek_wins_over_later_stale_duplicates() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 40_000));
    let outcome = state.absorb(&StreamingStateEvent::Seeked { position_ms: 5_000 });
    assert!(outcome.position_changed);
    assert_eq!(state.position_ms, 5_000);
    // The seek lowered the floor; the next advance continues from there.
    state.absorb(&StreamingStateEvent::PositionChanged { position_ms: 6_000 });
    assert_eq!(state.position_ms, 6_000);
    // A pre-seek duplicate still loses.
    let outcome = state.absorb(&StreamingStateEvent::PositionChanged { position_ms: 4_999 });
    assert!(!outcome.any());
    assert_eq!(state.position_ms, 6_000);
  }

  #[test]
  fn new_track_boundary_accepts_a_lower_position() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:first", 90_000));
    // Next track starts from zero even though the old one was further along.
    let outcome = state.absorb(&playing("spotify:track:second", 100));
    assert!(outcome.any());
    assert_eq!(state.track_id.as_deref(), Some("second"));
    assert_eq!(state.position_ms, 100);
  }

  #[test]
  fn loading_replaces_identity_and_rebases_progress_to_zero() {
    // Seek-during-load shape (ticket 06 semantics): a load issued while the
    // old track was still reporting positions re-bases progress; later events
    // for the *loading* track are authoritative from its start position.
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:first", 80_000));
    let outcome = state.absorb(&StreamingStateEvent::Loading {
      track_uri: "spotify:track:second".to_string(),
      position_ms: 0,
    });
    assert!(outcome.info_changed);
    assert_eq!(state.track_id.as_deref(), Some("second"));
    assert_eq!(
      state.is_playing,
      Some(true),
      "mirror parity: a load never flips transport knowledge"
    );
    assert_eq!(state.position_ms, 80_000, "progress holds but de-anchors");
    // Old-track duplicates must not sneak in after the load, in either
    // direction: pre-load positions belong to the old track.
    let outcome = state.absorb(&StreamingStateEvent::PositionChanged {
      position_ms: 81_000,
    });
    assert!(!outcome.any(), "stale post-load forward position rejected");
    let outcome = state.absorb(&StreamingStateEvent::PositionChanged {
      position_ms: 20_000,
    });
    assert!(!outcome.any(), "stale backward position rejected too");
    assert_eq!(state.position_ms, 80_000);
  }

  #[test]
  fn seek_event_after_loading_confirms_position() {
    let mut state = StreamingState::default();
    state.absorb(&StreamingStateEvent::Loading {
      track_uri: "spotify:track:next".to_string(),
      position_ms: 0,
    });
    state.absorb(&StreamingStateEvent::Seeked {
      position_ms: 45_000,
    });
    assert_eq!(state.position_ms, 45_000);
    // The user dragged backwards right after the load; forward-only would
    // wrongly drop it.
    let outcome = state.absorb(&StreamingStateEvent::Seeked {
      position_ms: 10_000,
    });
    assert!(outcome.position_changed);
    assert_eq!(state.position_ms, 10_000);
  }

  #[test]
  fn loading_the_same_track_again_is_not_an_identity_change() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:same", 5_000));
    let outcome = state.absorb(&StreamingStateEvent::Loading {
      track_uri: "spotify:track:same".to_string(),
      position_ms: 5_000,
    });
    assert!(!outcome.info_changed && !outcome.position_changed);
    assert_eq!(
      state.is_playing,
      Some(true),
      "re-load keeps transport state"
    );
  }

  #[test]
  fn track_changed_sets_metadata_and_resets_progress_zero_deanchored() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:first", 50_000));
    let outcome = state.absorb(&StreamingStateEvent::TrackChanged {
      info: info("Second"),
      track_uri: "spotify:track:second".to_string(),
    });
    assert!(outcome.info_changed);
    assert_eq!(state.track_info.as_ref().unwrap().name, "Second");
    assert_eq!(state.position_ms, 0);
    assert_eq!(
      state.is_playing,
      Some(true),
      "mirror parity: metadata never flips transport knowledge"
    );
    // Un-anchored: the old track's progress cannot land after this.
    let outcome = state.absorb(&StreamingStateEvent::PositionChanged {
      position_ms: 51_000,
    });
    assert!(!outcome.any());
    // The next transport fact re-anchors wherever it lands.
    let outcome = state.absorb(&StreamingStateEvent::Playing {
      track_uri: "spotify:track:second".to_string(),
      position_ms: 1_500,
    });
    assert!(outcome.any() || state.position_ms == 1_500);
    assert_eq!(state.position_ms, 1_500);
  }

  #[test]
  fn stopped_drops_metadata_and_pins_not_playing() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 10_000));
    let outcome = state.absorb(&StreamingStateEvent::Stopped);
    assert!(outcome.info_changed && outcome.play_state_changed);
    assert_eq!(state.track_info, None);
    assert_eq!(state.is_playing, Some(false));
    assert_eq!(state.position_ms, 0);
  }

  #[test]
  fn volume_events_do_not_move_streaming_facts() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 1_000));
    let outcome = state.absorb(&StreamingStateEvent::VolumeChanged { volume_percent: 44 });
    assert!(!outcome.any());
  }

  #[test]
  fn local_file_uri_keeps_full_identity_like_base62_normalization() {
    // `base62_id_of` passes non-spotify URIs through whole (mirror behavior:
    // `spotify:local:` URIs stay whole at the event boundary).
    let mut state = StreamingState::default();
    state.absorb(&playing("file:///music/song.mp3", 0));
    assert_eq!(state.track_id.as_deref(), Some("file:///music/song.mp3"));
  }

  #[test]
  fn end_of_track_reset_clears_track_and_position() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 150_000));
    state.reset_for_track_end(false);
    assert_eq!(state.position_ms, 0);
    assert_eq!(state.is_playing, Some(false));
    assert_eq!(state.track_id, None);
    assert_eq!(state.track_info, None);
  }

  #[test]
  fn teardown_clears_everything_including_queue_slot_metadata() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 42_000));
    state.clear();
    assert_eq!(state.is_playing, Some(false));
    assert_eq!(state.position_ms, 0);
    assert_eq!(state.track_id, None);
    assert_eq!(state.track_info, None);
  }

  #[test]
  fn duplicate_identical_positions_are_no_ops() {
    let mut state = StreamingState::default();
    state.absorb(&playing("spotify:track:abc", 8_000));
    let outcome = state.absorb(&StreamingStateEvent::PositionChanged { position_ms: 8_000 });
    assert!(!outcome.any());
  }

  #[test]
  fn paused_before_any_transport_event_reports_known_false_once() {
    let mut state = StreamingState::default();
    let first = state.absorb(&paused("spotify:track:idle", 0));
    assert!(first.play_state_changed);
    let second = state.absorb(&paused("spotify:track:idle", 0));
    assert!(!second.any());
  }
}
