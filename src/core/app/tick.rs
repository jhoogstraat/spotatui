use super::*;

const REMOTE_PLAYBACK_POLL_INTERVAL: Duration = Duration::from_secs(1);
const PLAYBACK_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(250);

#[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
#[derive(Debug, PartialEq)]
enum DecodedAdvance {
  Dispatch { replay: bool },
  SuspendToQueue,
  Teardown,
  None,
}

#[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
fn decoded_advance(
  finished: bool,
  advancing: bool,
  has_next: bool,
  queue_len: usize,
  repeat: crate::infra::queue::RepeatMode,
) -> DecodedAdvance {
  use crate::infra::queue::{advance_decision, Decision};
  match advance_decision(finished, advancing, has_next, queue_len, repeat) {
    Decision::AdvanceContext => DecodedAdvance::Dispatch { replay: false },
    Decision::RepeatTrack => DecodedAdvance::Dispatch { replay: true },
    Decision::SuspendToQueue => DecodedAdvance::SuspendToQueue,
    Decision::Teardown => DecodedAdvance::Teardown,
    Decision::None => DecodedAdvance::None,
  }
}

impl App {
  /// The position base carried by the latest typed playback fact. The window
  /// projects visible motion from the snapshot timestamp.
  pub fn playback_position_ms(&self) -> Option<u128> {
    Some(self.song_progress_ms)
  }

  pub(crate) fn poll_current_playback(&mut self) {
    if !self.spotify_connected {
      return;
    }
    let interval_ms = if self.is_streaming_active {
      self.user_config.behavior.playback_poll_seconds as u128 * 1000
    } else {
      1_000
    };
    if !self.is_fetching_current_playback
      && self
        .instant_since_last_current_playback_poll
        .elapsed()
        .as_millis()
        >= interval_ms
    {
      self.is_fetching_current_playback = true;
      match self.seek_ms {
        Some(seek_ms) => self.apply_seek(seek_ms as u32),
        None => self.dispatch(IoEvent::GetCurrentPlayback),
      }
    }
  }

  pub(crate) fn playback_resync_deadline(&self) -> Option<Instant> {
    let playing = self.native_is_playing.unwrap_or(false)
      || self
        .current_playback_context
        .as_ref()
        .is_some_and(|context| context.is_playing);
    if !self.spotify_connected || self.is_fetching_current_playback || !playing {
      return None;
    }
    let interval = if self.is_streaming_active {
      Duration::from_secs(self.user_config.behavior.playback_poll_seconds)
    } else {
      REMOTE_PLAYBACK_POLL_INTERVAL
    };
    Some(self.instant_since_last_current_playback_poll + interval)
  }

  /// Re-arm playback probes only while they have live work. This named
  /// one-shot deadline replaces the global tick without publishing position.
  pub(crate) fn playback_maintenance_deadline(&self) -> Option<Instant> {
    #[allow(unused_mut)]
    let mut active = self.pending_volume.is_some()
      && !self.is_volume_change_in_flight
      && self.last_dispatched_volume != self.pending_volume;
    #[cfg(feature = "streaming")]
    {
      active |= self.is_streaming_active && self.native_is_playing.unwrap_or(false);
    }
    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    {
      active |= self.queue_now_decoded_player().is_some();
    }
    #[cfg(feature = "local-files")]
    {
      active |= self.local_playback.is_some();
    }
    #[cfg(feature = "subsonic")]
    {
      active |= self.subsonic_playback.is_some();
    }
    #[cfg(feature = "youtube")]
    {
      active |= self.youtube_playback.is_some();
    }
    #[cfg(feature = "internet-radio")]
    {
      active |= self.radio_playback.is_some();
    }
    #[cfg(feature = "ai-dj")]
    {
      active |= crate::infra::network::dj::wants_top_up(
        self.native_queue.len(),
        &self.dj,
        self.spotify_external_device_active(),
      );
    }
    active.then(|| Instant::now() + PLAYBACK_MAINTENANCE_INTERVAL)
  }

  pub(crate) fn run_playback_maintenance(&mut self) {
    #[cfg(feature = "streaming")]
    if let Some(player) = self.streaming_player.clone() {
      if let Some(error) = player.take_audio_backend_error() {
        self.pause_native_playback();
        self.set_status_message(format!("Audio backend failed: {error}"), 15);
      }
    }

    self.flush_pending_volume();

    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    {
      use crate::infra::queue::QueueNowPlaying;
      let advance = match self.queue_now.as_mut() {
        Some(QueueNowPlaying::Decoded(decoded))
          if decoded.player.is_finished() && !decoded.advancing =>
        {
          decoded.advancing = true;
          true
        }
        _ => false,
      };
      if advance {
        self.dispatch(IoEvent::AdvanceNativeQueue);
      }
    }

    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    macro_rules! maintain_decoded {
      ($playback:ident, $tracks:ident) => {
        if !self.queue_owns_playback() {
          use crate::infra::queue::next_index;
          let decision = self.$playback.as_ref().map(|session| {
            decoded_advance(
              session.player.is_finished(),
              session.advancing,
              next_index(session.index, session.$tracks.len()).is_some(),
              self.native_queue.len(),
              self.decoded_repeat,
            )
          });
          match decision {
            Some(DecodedAdvance::Dispatch { replay }) => {
              if let Some(session) = self.$playback.as_mut() {
                session.advancing = true;
              }
              self.dispatch(if replay {
                IoEvent::ReplayCurrentTrack
              } else {
                IoEvent::NextTrack
              });
            }
            Some(DecodedAdvance::SuspendToQueue) => {
              self.suspend_active_decoded_context_for_skip(
                crate::infra::queue::SuspendCause::AutoAdvance,
              );
              self.dispatch(IoEvent::AdvanceNativeQueue);
            }
            Some(DecodedAdvance::Teardown) => self.$playback = None,
            Some(DecodedAdvance::None) | None => {}
          }
        }
      };
    }
    #[cfg(feature = "local-files")]
    maintain_decoded!(local_playback, queue);
    #[cfg(feature = "subsonic")]
    maintain_decoded!(subsonic_playback, tracks);
    #[cfg(feature = "youtube")]
    maintain_decoded!(youtube_playback, tracks);

    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    self.reconcile_decoded_shuffle();

    #[cfg(feature = "internet-radio")]
    match self.radio_playback.as_ref() {
      Some(radio) if !radio.player.is_finished() => self.radio_stream_started = true,
      Some(_) if self.radio_stream_started => {
        self.radio_playback = None;
        self.radio_stream_started = false;
        self.set_status_message("Radio stream ended", 4);
      }
      Some(_) => {}
      None => self.radio_stream_started = false,
    }

    #[cfg(feature = "ai-dj")]
    if crate::infra::network::dj::wants_top_up(
      self.native_queue.len(),
      &self.dj,
      self.spotify_external_device_active(),
    ) {
      let turn_seq = self.dj.begin_turn(crate::infra::dj::TurnKind::Refill);
      let generation = self.dj.generation;
      self.dispatch_without_spinner(IoEvent::DjTopUp(generation, turn_seq));
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn idle_signed_in_engine_has_no_playback_deadline() {
    let (tx, _rx) = channel();
    let app = App::new(tx, UserConfig::new(), Some(SystemTime::now()));
    assert!(app.playback_resync_deadline().is_none());
    assert!(app.playback_maintenance_deadline().is_none());
  }

  #[test]
  fn pending_volume_arms_named_maintenance() {
    let (tx, _rx) = channel();
    let mut app = App::new(tx, UserConfig::new(), None);
    app.pending_volume = Some(42);
    assert!(app.playback_maintenance_deadline().is_some());
  }

  #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
  #[test]
  fn decoded_completion_policy_is_preserved() {
    use crate::infra::queue::RepeatMode;
    assert_eq!(
      decoded_advance(true, false, true, 0, RepeatMode::Track),
      DecodedAdvance::Dispatch { replay: true }
    );
    assert_eq!(
      decoded_advance(true, false, false, 1, RepeatMode::Off),
      DecodedAdvance::SuspendToQueue
    );
    assert_eq!(
      decoded_advance(true, false, false, 0, RepeatMode::Off),
      DecodedAdvance::Teardown
    );
  }
}
