use std::time::Instant;

use chrono::TimeDelta;

use super::streaming::{AbsorbOutcome, StreamingStateEvent};
use crate::core::app::App;
use crate::infra::network::IoEvent;

/// The fold task's single write path into App for native-streaming facts
/// (ADR 0016 ticket 14).
///
/// Gated to `streaming`: every mirrored field and recovery observer it touches
/// exists only under that feature, exactly like the librespot event handler
/// this extracts.
///
/// Reduces a [`StreamingStateEvent`] through the domain's
/// [`StreamingState`] (stream order, stale-position floor, seek-during-load
/// de-anchor) and mirrors the result onto the legacy fields every existing
/// reader consumes, with byte-for-byte parity to the former direct writes in
/// `infra/player/events.rs`. Producers emit; only the fold calls this.
/// Temporary dead-code allow: the handler migration (this ticket) is the
/// consumer; the allow dies with the direct writes it replaces.
#[cfg_attr(not(test), allow(dead_code))]
pub fn apply_streaming_state(app: &mut App, event: StreamingStateEvent) -> AbsorbOutcome {
  // 1. Domain first: stream-ordered stale/seek rules live in StreamingState.
  let outcome = app.streaming_state.absorb(&event);

  // 2. Mirror whatever moved. Each write below replays one the event handler
  //    used to make inline; ordering follows the handler's own sequence.
  // The handler wrote the transport flag unconditionally on both transport
  // arms; the domain holds it, so mirror whenever it is known.
  if let Some(is_playing) = app.streaming_state.is_playing {
    app.native_is_playing = Some(is_playing);
  }
  match &event {
    StreamingStateEvent::Playing {
      track_uri,
      position_ms,
    } => {
      // Identity refresh when the boundary arrived without a TrackChanged
      // (restore confirmation): matches the handler's `last_track_id`
      // update plus its GetCurrentPlayback dispatch on change.
      let id = crate::core::app::base62_id_of(track_uri).to_string();
      if app.last_track_id.as_deref() != Some(id.as_str()) {
        app.last_track_id = Some(id.clone());
        app.dispatch(IoEvent::GetCurrentPlayback);
      }
      if outcome.position_changed {
        app.song_progress_ms = u128::from(*position_ms);
        if let Some(context) = app.current_playback_context.as_mut() {
          context.is_playing = true;
          context.progress = Some(TimeDelta::milliseconds(i64::from(*position_ms)));
        }
        app.instant_since_last_current_playback_poll = Instant::now();
      } else if app.song_progress_ms != u128::from(*position_ms) {
        // Progress held an older value (de-anchored boundary): Playing
        // re-anchors it outright.
        app.song_progress_ms = u128::from(*position_ms);
      }
      // One-shot stop-after-current: fires once, on the first Playing after
      // a natural track end, mirroring the handler's latch consumption.
      if app.pending_stop_after_track {
        app.pending_stop_after_track = false;
        if let Some(context) = app.current_playback_context.as_mut() {
          context.is_playing = false;
        }
        app.dispatch(IoEvent::PausePlayback);
      }
    }
    StreamingStateEvent::Paused { position_ms, .. } => {
      if outcome.position_changed || app.song_progress_ms != u128::from(*position_ms) {
        app.song_progress_ms = u128::from(*position_ms);
        if let Some(context) = app.current_playback_context.as_mut() {
          context.is_playing = false;
          context.progress = Some(TimeDelta::milliseconds(i64::from(*position_ms)));
        }
        app.instant_since_last_current_playback_poll = Instant::now();
      }
    }
    StreamingStateEvent::Seeked { .. } => {
      if outcome.position_changed {
        // The handler cleared any queued seek target and stamped the
        // recovery snapshot with the confirmed position.
        app.seek_ms = None;
        app.song_progress_ms = app.streaming_state.position_ms;
        let position = app.streaming_state.position_ms.min(u32::MAX as u128) as u32;
        app.set_native_recovery_position(position);
        if let Some(context) = app.current_playback_context.as_mut() {
          context.progress = Some(TimeDelta::milliseconds(
            app.streaming_state.position_ms as i64,
          ));
        }
        app.instant_since_last_current_playback_poll = Instant::now();
      }
    }
    StreamingStateEvent::Loading {
      track_uri,
      position_ms,
    } => {
      if outcome.info_changed {
        // The restore-observer half of Loading: stale-load rejection lives
        // in the method; the fold replays it once per boundary.
        app.observe_native_loading(track_uri.clone(), *position_ms);
      }
    }
    StreamingStateEvent::TrackChanged { info, track_uri } => {
      if outcome.info_changed {
        app.native_track_info = Some(info.clone());
        app.last_track_id = Some(crate::core::app::base62_id_of(track_uri).to_string());
        if outcome.position_changed {
          app.song_progress_ms = app.streaming_state.position_ms;
        }
        app.instant_since_last_current_playback_poll = Instant::now();
        app.dispatch(IoEvent::GetCurrentPlayback);
      }
    }
    StreamingStateEvent::Stopped => {
      if outcome.any() {
        if let Some(context) = app.current_playback_context.as_mut() {
          context.is_playing = false;
        }
        if outcome.position_changed {
          app.song_progress_ms = 0;
        }
        if outcome.info_changed {
          app.native_track_info = None;
          app.last_track_id = None;
        }
      }
    }
    StreamingStateEvent::Teardown => {
      // Mirror the old teardown clears. `is_streaming_active`,
      // `native_device_id`, `seek_ms`, recovery fields, and
      // `current_playback_context` stay owned by the recovery path (they are
      // routing state), exactly as before.
      if outcome.position_changed {
        app.song_progress_ms = 0;
      }
      if outcome.info_changed {
        app.native_track_info = None;
        app.last_track_id = None;
      }
    }
    StreamingStateEvent::PositionChanged { .. } => {
      if outcome.position_changed {
        // Recovery snapshot rides every confirmed position advance.
        let position = app.streaming_state.position_ms.min(u32::MAX as u128) as u32;
        app.set_native_recovery_position(position);
      }
    }
    StreamingStateEvent::EndOfTrack { .. } | StreamingStateEvent::VolumeChanged { .. } => {}
  }

  if let StreamingStateEvent::VolumeChanged { volume_percent } = &event {
    apply_volume_changed(app, *volume_percent);
  }

  outcome
}

#[cfg(feature = "streaming")]
#[cfg_attr(not(test), allow(dead_code))]
fn apply_volume_changed(app: &mut App, volume_percent: u8) {
  if let Some(pending) = app.pending_volume {
    if volume_percent == pending {
      app.pending_volume = None;
      app.last_dispatched_volume = None;
    }
    return;
  }
  if let Some(context) = app.current_playback_context.as_mut() {
    context.device.volume_percent = Some(u32::from(volume_percent));
  }
  app.runtime_state.volume_percent = volume_percent.min(100);
  let _ = app.save_runtime_state(&crate::core::state::PersistedRuntimeState::volume_percent(
    volume_percent.min(100),
  ));
}

#[cfg(feature = "streaming")]
#[cfg_attr(not(test), allow(dead_code))]
impl App {
  /// Method form of the free function in this module, for fold-shaped
  /// callers already holding the App lock.
  pub fn apply_streaming_state(&mut self, event: StreamingStateEvent) -> AbsorbOutcome {
    apply_streaming_state(self, event)
  }
}

#[cfg(all(test, feature = "streaming"))]
mod tests {
  use super::*;
  use crate::core::app::{NativeTrackInfo, NativeTrackKind};
  use crate::core::user_config::UserConfig;
  use rspotify::model::RepeatState;
  use std::sync::mpsc::{channel, Receiver};

  fn app_with_io() -> (App, Receiver<IoEvent>) {
    let (tx, rx) = channel();
    (App::new(tx, UserConfig::new(), None), rx)
  }

  fn playing(uri: &str, position_ms: u32) -> StreamingStateEvent {
    StreamingStateEvent::Playing {
      track_uri: uri.to_string(),
      position_ms,
    }
  }

  fn native_info(name: &str) -> NativeTrackInfo {
    NativeTrackInfo {
      name: name.to_string(),
      artists: vec!["a".to_string()],
      album: String::new(),
      duration_ms: 100_000,
      kind: NativeTrackKind::Track,
      image_url: None,
    }
  }

  #[test]
  fn playing_writes_all_transport_mirrors_like_the_handler_did() {
    let (mut app, mut rx) = app_with_io();
    app.is_streaming_active = true;

    apply_streaming_state(&mut app, playing("spotify:track:abc", 3_000));
    assert_eq!(app.song_progress_ms, 3_000);
    assert_eq!(app.native_is_playing, Some(true));
    assert_eq!(app.last_track_id.as_deref(), Some("abc"));
    assert!(
      matches!(rx.try_recv(), Ok(IoEvent::GetCurrentPlayback)),
      "identity change drives the context refresh"
    );
    if let Some(context) = &app.current_playback_context {
      assert!(context.is_playing);
      assert_eq!(context.progress.map(|p| p.num_milliseconds()), Some(3_000));
    }
  }

  #[test]
  fn paused_event_flips_the_mirror_without_flipping_metadata() {
    let (mut app, _rx) = app_with_io();
    app.apply_streaming_state(playing("spotify:track:abc", 1_000));
    app.apply_streaming_state(StreamingStateEvent::Paused {
      track_uri: "spotify:track:abc".to_string(),
      position_ms: 4_000,
    });
    assert_eq!(app.native_is_playing, Some(false));
    assert_eq!(app.song_progress_ms, 4_000);
    if let Some(context) = &app.current_playback_context {
      assert!(!context.is_playing);
    }
  }

  #[test]
  fn stale_backward_position_leaves_every_mirror_untouched() {
    let (mut app, _rx) = app_with_io();
    app.apply_streaming_state(playing("spotify:track:abc", 30_000));
    let before = app.song_progress_ms;
    let outcome = app.apply_streaming_state(StreamingStateEvent::PositionChanged {
      position_ms: 12_000,
    });
    assert!(!outcome.any());
    assert_eq!(app.song_progress_ms, before);
  }

  #[test]
  fn stop_after_current_consumes_one_playing_frame_then_pauses() {
    let (mut app, mut rx) = app_with_io();
    app.pending_stop_after_track = true;

    app.apply_streaming_state(playing("spotify:track:abc", 0));
    assert!(!app.pending_stop_after_track, "one-shot latch cleared");
    // Identity refresh first, then the one-shot pause.
    assert!(matches!(rx.try_recv(), Ok(IoEvent::GetCurrentPlayback)));
    assert!(matches!(rx.try_recv(), Ok(IoEvent::PausePlayback)));
  }

  #[test]
  fn volume_confirming_a_pending_value_clears_both_volume_mirrors() {
    let (mut app, _rx) = app_with_io();
    app.pending_volume = Some(40);

    apply_streaming_state(
      &mut app,
      StreamingStateEvent::VolumeChanged { volume_percent: 40 },
    );
    assert_eq!(app.pending_volume, None);
    assert_eq!(app.last_dispatched_volume, None);
  }

  #[test]
  fn unexpected_volume_persists_the_runtime_value() {
    let (mut app, _rx) = app_with_io();
    app.current_playback_context = None;
    app.apply_streaming_state(StreamingStateEvent::VolumeChanged { volume_percent: 77 });
    assert_eq!(app.runtime_state.volume_percent, 77);
  }

  #[test]
  fn track_changed_replaces_metadata_and_resets_progress() {
    let (mut app, mut rx) = app_with_io();
    app.apply_streaming_state(playing("spotify:track:first", 50_000));
    let _ = rx.try_recv();
    app.apply_streaming_state(StreamingStateEvent::TrackChanged {
      info: native_info("Second"),
      track_uri: "spotify:track:second".to_string(),
    });
    assert_eq!(app.native_track_info.as_ref().unwrap().name, "Second");
    assert_eq!(app.last_track_id.as_deref(), Some("second"));
    assert_eq!(app.song_progress_ms, 0);
    assert!(matches!(rx.try_recv(), Ok(IoEvent::GetCurrentPlayback)));
  }

  #[test]
  fn stopped_drops_metadata_pins_not_playing_and_zeroes_progress() {
    let (mut app, _rx) = app_with_io();
    app.apply_streaming_state(playing("spotify:track:abc", 9_000));
    app.apply_streaming_state(StreamingStateEvent::Stopped);
    assert_eq!(app.native_track_info, None);
    assert_eq!(app.last_track_id, None);
    assert_eq!(app.native_is_playing, Some(false));
    assert_eq!(app.song_progress_ms, 0);
  }

  #[test]
  fn seek_event_stamps_recovery_position_and_clears_pending_seek() {
    let (mut app, _rx) = app_with_io();
    // A live recovery snapshot, as if a restore were in progress: the Seeked
    // handler stamps its position so an in-flight replay resumes from there.
    let generation = app.record_native_playback_request(
      None,
      Some(vec!["spotify:track:abc".to_string()]),
      Some(0),
      true,
      false,
      RepeatState::Off,
    );
    app.seek_ms = Some(123);
    app.apply_streaming_state(playing("spotify:track:abc", 10_000));
    app.apply_streaming_state(StreamingStateEvent::Seeked {
      position_ms: 45_000,
    });
    assert_eq!(app.song_progress_ms, 45_000);
    assert_eq!(app.seek_ms, None);
    assert_eq!(
      app
        .native_playback_recovery
        .as_ref()
        .map(|snapshot| snapshot.position_ms),
      Some(45_000)
    );
  }

  #[test]
  fn loading_seed_routes_through_the_restore_observer() {
    let mut app = App::default();
    let generation = app.record_native_playback_request(
      None,
      Some(vec!["spotify:track:expected".to_string()]),
      Some(0),
      true,
      false,
      RepeatState::Off,
    );
    app.begin_native_playback_restore(generation).unwrap();

    app.apply_streaming_state(StreamingStateEvent::Loading {
      track_uri: "spotify:track:stale".to_string(),
      position_ms: 0,
    });
    // The stale load was rejected by the observer (no loading_track_uri
    // recorded onto the recovery snapshot).
    assert_eq!(
      app
        .native_playback_recovery
        .as_ref()
        .and_then(|snapshot| snapshot.loading_track_uri.clone()),
      None
    );
  }
}
