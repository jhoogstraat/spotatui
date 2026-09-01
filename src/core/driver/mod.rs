//! The frontend-neutral scheduler ("the driver"): everything the app must do
//! on a timer, extracted from the terminal event loop so any frontend can run
//! playback correctly by calling [`Driver::tick`] at its tick rate.
//!
//! One tick advances playback state (`App::update_on_tick` plus the debounced
//! seek/volume flushes), refreshes the OAuth token, syncs OS presence
//! (Discord, MPRIS, window title), detects track changes (lyrics + cover
//! art), auto-advances the native queue and the decoded sources, and feeds the
//! audio visualizer. Runtime-state and decoded-session persistence are owned by
//! the Playback Engine's explicit deadline facts.
//!
//! What stays with the frontend, injected through [`TickEnv`] or kept at the
//! call site: the visualizer bar count (a function of frontend geometry), the
//! terminal image-protocol probe behind `TickEnv::cover_art_full_image_support`,
//! and the macOS `NSRunLoop` pump (the frontend owns the main thread).
//!
//! A frontend that stops ticking is a bug this module makes loud:
//! `App::playback_position_ms` reports stale once no tick has run for two
//! seconds, instead of leaving a silently frozen playbar.

mod plan;
mod presence;

use crate::core::app::{App, RouteId};
#[cfg(feature = "discord-rpc")]
use crate::infra::discord_rpc;
#[cfg(all(feature = "mpris", target_os = "linux"))]
use crate::infra::mpris;
use crate::infra::network::IoEvent;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::{Duration, Instant};

#[cfg(feature = "discord-rpc")]
pub type DiscordRpcHandle = Option<discord_rpc::DiscordRpcManager>;
#[cfg(not(feature = "discord-rpc"))]
pub type DiscordRpcHandle = Option<()>;

#[cfg(all(feature = "mpris", target_os = "linux"))]
pub type MprisHandle = Option<Arc<mpris::MprisManager>>;
#[cfg(not(all(feature = "mpris", target_os = "linux")))]
pub type MprisHandle = Option<()>;

/// Per-tick inputs only the frontend can supply.
pub struct TickEnv {
  /// Wall-clock instant of this tick; injected so the driver's throttling
  /// decisions stay deterministic under test.
  pub now: Instant,
}

/// The scheduler's own state: everything that used to live as locals of the
/// terminal event loop, one instance per frontend session.
pub struct Driver {
  /// Real-time position updates from the native player's event handler,
  /// read lock-free instead of waiting on the polled Spotify context.
  shared_position: Option<Arc<AtomicU64>>,
  #[cfg(all(feature = "mpris", target_os = "linux"))]
  mpris_manager: MprisHandle,
  #[cfg(feature = "discord-rpc")]
  discord_rpc_manager: DiscordRpcHandle,
  /// Previous tick's `is_streaming_active`, so a native session that ends can
  /// push a final stopped state to MPRIS clients.
  #[cfg(all(feature = "mpris", target_os = "linux"))]
  prev_is_streaming_active: bool,
  #[cfg(feature = "discord-rpc")]
  discord_presence: presence::DiscordPresenceState,
  #[cfg(all(feature = "mpris", target_os = "linux"))]
  mpris_state: presence::MprisState,
  window_title: presence::WindowTitleState,
  /// Whether the active internet-radio stream has produced audio yet, so the
  /// tick can tell "stream just started, sink not filled" apart from "stream
  /// died / drained" — both of which report `is_finished()` (empty sink).
  #[cfg(feature = "internet-radio")]
  radio_stream_started: bool,
}

impl Driver {
  pub fn new(
    shared_position: Option<Arc<AtomicU64>>,
    mpris_manager: MprisHandle,
    discord_rpc_manager: DiscordRpcHandle,
  ) -> Self {
    #[cfg(not(all(feature = "mpris", target_os = "linux")))]
    let _ = mpris_manager;
    #[cfg(not(feature = "discord-rpc"))]
    let _ = discord_rpc_manager;

    Driver {
      shared_position,
      #[cfg(all(feature = "mpris", target_os = "linux"))]
      mpris_manager,
      #[cfg(feature = "discord-rpc")]
      discord_rpc_manager,
      #[cfg(all(feature = "mpris", target_os = "linux"))]
      prev_is_streaming_active: false,
      #[cfg(feature = "discord-rpc")]
      discord_presence: presence::DiscordPresenceState::default(),
      #[cfg(all(feature = "mpris", target_os = "linux"))]
      mpris_state: presence::MprisState::default(),
      window_title: presence::WindowTitleState::default(),
      #[cfg(feature = "internet-radio")]
      radio_stream_started: false,
    }
  }

  /// One tick. The frontend calls this at its tick rate with the `App` lock
  /// held; everything slow (file I/O, network) is dispatched off-lock. Must
  /// run inside a Tokio runtime: some source operations use
  /// `tokio::task::spawn_blocking`, which panics without one.
  pub fn tick(&mut self, app: &mut App, elapsed: Duration, _env: TickEnv) {
    #[cfg(feature = "streaming")]
    if let Some(player) = app.streaming_player.clone() {
      if let Some(error) = player.take_audio_backend_error() {
        app.pause_native_playback();
        app.set_status_message(format!("Audio backend failed: {error}"), 15);
      }
    }

    #[cfg(all(feature = "mpris", target_os = "linux"))]
    {
      let current_is_streaming_active = app.is_streaming_active;
      if self.prev_is_streaming_active && !current_is_streaming_active {
        if let Some(ref mpris) = self.mpris_manager {
          mpris.set_stopped();
        }
      }
      self.prev_is_streaming_active = current_is_streaming_active;
    }

    app.update_on_tick(elapsed);

    app.flush_pending_volume();

    #[cfg(feature = "discord-rpc")]
    if let Some(ref manager) = self.discord_rpc_manager {
      presence::update_discord_presence(manager, &mut self.discord_presence, app);
    }

    #[cfg(all(feature = "mpris", target_os = "linux"))]
    if let Some(ref mpris) = self.mpris_manager {
      presence::update_mpris_state(mpris, &mut self.mpris_state, app);
    }

    // Native queue slot: when the queued track finishes, advance the queue
    // (play the next queued item, or resume the suspended context). Runs
    // before the per-source blocks so it takes precedence over them.
    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    {
      use crate::infra::queue::QueueNowPlaying;
      let advance = match app.queue_now.as_mut() {
        Some(QueueNowPlaying::Decoded(d))
          if plan::native_queue_advance_due(d.player.is_finished(), d.advancing) =>
        {
          d.advancing = true; // atomic check-and-set: one dispatch only
          true
        }
        _ => false,
      };
      if advance {
        app.dispatch(IoEvent::AdvanceNativeQueue);
      }
    }

    // Auto-queue refill. Dispatch only: this path holds `&mut App`, so the
    // brain call itself must happen on the detached service lane. The
    // generation goes along for the ride so a refill the user has since
    // abandoned (DJ off, vibe shift, source change) can be dropped when it
    // lands instead of queueing tracks for a session that is gone.
    #[cfg(feature = "ai-dj")]
    {
      if crate::infra::network::dj::wants_top_up(
        app.native_queue.len(),
        &app.dj,
        app.spotify_external_device_active(),
      ) {
        let turn_seq = app.dj.begin_turn(crate::infra::dj::TurnKind::Refill);
        let generation = app.dj.generation;
        // Not `dispatch`: that pins the global `is_loading` spinner until the
        // service-lane task finishes, which for a brain call is minutes. The
        // DJ's own `thinking` flag is the progress surface here.
        app.dispatch_without_spinner(IoEvent::DjTopUp(generation, turn_seq));
      }
    }

    // Decoded-source auto-advance, one macro invocation per source (the
    // blocks are identical except for which `*_playback` session and queue
    // field they read). Each session reads its progress live from the
    // player at render time; the only state self-managed here is
    // end-of-track. When the sink drains and no track change is already in
    // flight (`!advancing`), `plan::decoded_advance` picks the move: advance /
    // replay (repeat-one) / suspend to the native queue / tear down.
    //
    // Decide under one borrow, then act after the borrow ends. `advancing`
    // is set *synchronously here* (atomic check-and-set, before
    // dispatching) because the sink stays empty for the whole decode — or,
    // for Subsonic/YouTube, multi-second download — window; without it the
    // next tick would re-dispatch and skip several tracks per advance.
    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    macro_rules! decoded_auto_advance {
      ($app:ident, $playback:ident, $queue:ident) => {
        if !$app.queue_owns_playback() {
          use crate::infra::queue::next_index;
          let queue_len = $app.native_queue.len();
          let repeat = $app.decoded_repeat;
          let advance = $app.$playback.as_ref().map(|s| {
            plan::decoded_advance(
              s.player.is_finished(),
              s.advancing,
              next_index(s.index, s.$queue.len()).is_some(),
              queue_len,
              repeat,
            )
          });
          match advance {
            Some(plan::DecodedAdvance::Dispatch { replay }) => {
              if let Some(s) = $app.$playback.as_mut() {
                s.advancing = true; // atomic check-and-set: one dispatch only
              }
              $app.dispatch(if replay {
                IoEvent::ReplayCurrentTrack
              } else {
                IoEvent::NextTrack
              });
            }
            Some(plan::DecodedAdvance::SuspendToQueue) => {
              // End-of-track handoff: under Repeat One the context resumes
              // the same track, so a queued song can't consume the repeat.
              $app.suspend_active_decoded_context_for_skip(
                crate::infra::queue::SuspendCause::AutoAdvance,
              );
              $app.dispatch(IoEvent::AdvanceNativeQueue);
            }
            Some(plan::DecodedAdvance::Teardown) => $app.$playback = None,
            Some(plan::DecodedAdvance::None) | None => {}
          }
        }
      };
    }
    #[cfg(feature = "local-files")]
    decoded_auto_advance!(app, local_playback, queue);
    #[cfg(feature = "subsonic")]
    decoded_auto_advance!(app, subsonic_playback, tracks);
    #[cfg(feature = "youtube")]
    decoded_auto_advance!(app, youtube_playback, tracks);

    // Apply any shuffle toggle that was deferred while a track change was in
    // flight, now that the advance may have committed (a cheap no-op when the
    // queue order already matches the decoded shuffle state).
    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    app.reconcile_decoded_shuffle();

    // Internet radio has no queue to auto-advance; instead the tick watches
    // for a live stream that dies (server disconnect or the ring buffer
    // draining to EOF), which leaves `is_finished()` (empty sink) true while
    // the session was never paused. `is_finished()` is also true during the
    // brief pre-playback window before the first bytes arrive, so only tear
    // down once the stream has actually started producing audio.
    #[cfg(feature = "internet-radio")]
    match app.radio_playback.as_ref() {
      Some(radio) => {
        if !radio.player.is_finished() {
          self.radio_stream_started = true;
        } else if self.radio_stream_started {
          app.radio_playback = None;
          self.radio_stream_started = false;
          app.set_status_message("Radio stream ended", 4);
        }
      }
      None => self.radio_stream_started = false,
    }

    // A decoded non-Spotify source owns the sink while its `*_playback` is
    // `Some`. Drive `song_progress_ms` from its live player, and do NOT let
    // the (paused) librespot position below clobber it.
    #[allow(unused_mut)]
    let mut source_owns_playback = false;
    // The native queue slot owns the sink when playing a decoded track; read
    // progress from its player first (it may share the suspended context's
    // player, in which case a per-source block below reads the same value).
    #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
    if let Some(crate::infra::queue::QueueNowPlaying::Decoded(d)) = app.queue_now.as_ref() {
      source_owns_playback = true;
      app.song_progress_ms = d.player.position().as_millis();
    }
    #[allow(unused_variables)]
    let spotify_queue_slot = app.queue_now_is_spotify();
    #[cfg(feature = "local-files")]
    if !spotify_queue_slot {
      if let Some(local) = app.local_playback.as_ref() {
        source_owns_playback = true;
        let position_ms = local.player.position().as_millis();
        app.song_progress_ms = position_ms;
      }
    }
    #[cfg(feature = "subsonic")]
    if !spotify_queue_slot {
      if let Some(subsonic) = app.subsonic_playback.as_ref() {
        source_owns_playback = true;
        let position_ms = subsonic.player.position().as_millis();
        app.song_progress_ms = position_ms;
      }
    }
    #[cfg(feature = "internet-radio")]
    if !spotify_queue_slot {
      if let Some(radio) = app.radio_playback.as_ref() {
        source_owns_playback = true;
        let position_ms = radio.player.position().as_millis();
        app.song_progress_ms = position_ms;
      }
    }
    #[cfg(feature = "youtube")]
    if !spotify_queue_slot {
      if let Some(youtube) = app.youtube_playback.as_ref() {
        source_owns_playback = true;
        let position_ms = youtube.player.position().as_millis();
        app.song_progress_ms = position_ms;
      }
    }

    #[cfg(feature = "streaming")]
    if !source_owns_playback {
      if let Some(ref pos) = self.shared_position {
        if app.is_streaming_active {
          let recently_seeked = app
            .last_native_seek
            .is_some_and(|t| t.elapsed().as_millis() < crate::core::app::SEEK_POSITION_IGNORE_MS);

          if !recently_seeked {
            let position_ms = pos.load(std::sync::atomic::Ordering::Relaxed);
            if position_ms > 0 {
              app.song_progress_ms = position_ms as u128;
            }
          }
        }
      }
    }
    #[cfg(not(feature = "streaming"))]
    if !source_owns_playback {
      if let Some(ref pos) = self.shared_position {
        if app.is_streaming_active {
          let position_ms = pos.load(std::sync::atomic::Ordering::Relaxed);
          if position_ms > 0 {
            app.song_progress_ms = position_ms as u128;
          }
        }
      }
    }
  }

  /// The one-shot startup dispatch, fired by the frontend right after its
  /// first frame: initial Spotify fetches, the startup route's data, the
  /// persisted active source's sidebar data, and `--play-file`.
  pub fn dispatch_startup(&mut self, app: &mut App) {
    // Spotify-only startup fetches: skip them entirely when launched against a
    // free source with no Spotify session, so the network layer doesn't reject
    // three events with "connect Spotify" status flashes on every launch.
    if app.spotify_connected {
      app.dispatch(IoEvent::GetCurrentPlayback);
      app.dispatch(IoEvent::GetPlaylists);
      app.dispatch(IoEvent::GetUser);
    }
    // startup_route seeds the nav stack directly (App::new), bypassing the
    // handlers that normally fetch a screen's data on navigation — kick
    // off that fetch here or the screen renders empty until re-entered.
    // (Home needs nothing extra; Discover fetches from within its menu.)
    // Spotify-backed screens are gated on a connected session; Stats reads
    // local history so it always fetches.
    match app.get_current_route().id {
      RouteId::RecentlyPlayed if app.spotify_connected => app.dispatch(IoEvent::GetRecentlyPlayed),
      RouteId::AlbumList if app.spotify_connected => {
        app.dispatch(IoEvent::GetCurrentUserSavedAlbums(None))
      }
      RouteId::Artists if app.spotify_connected => app.dispatch(IoEvent::GetFollowedArtists(None)),
      RouteId::Podcasts if app.spotify_connected => {
        app.dispatch(IoEvent::GetCurrentUserSavedShows(None))
      }
      RouteId::Stats => {
        app.stats_loading = true;
        let period = app.stats_period;
        app.dispatch(IoEvent::LoadListeningStats(period));
      }
      _ => {}
    }
    // A persisted non-Spotify active source needs its sidebar data loaded
    // too (all of these are inert no-ops when the feature is off).
    match app.active_source {
      crate::core::source::Source::Local => app.dispatch(IoEvent::GetLocalPlaylists),
      crate::core::source::Source::Subsonic => app.dispatch(IoEvent::GetSubsonicPlaylists),
      crate::core::source::Source::Radio => app.dispatch(IoEvent::GetRadioStations),
      crate::core::source::Source::YouTube => app.dispatch(IoEvent::GetYouTubePlaylists),
      crate::core::source::Source::Spotify => {}
    }
    if app.user_config.behavior.enable_global_song_count {
      app.dispatch(IoEvent::FetchGlobalSongCount);
    }
    app.dispatch(IoEvent::FetchAnnouncements);

    // `--play-file`: kick off local playback now that dispatch is wired.
    if let Some(uri) = app.pending_play_file.take() {
      app.dispatch(IoEvent::StartPlayback(Some(uri), None, None));
    }
  }

  /// The window title the frontend should apply now, or `None` when it is
  /// unchanged. Called once per frame (cheaper than a tick, and a title
  /// change should not wait for one).
  pub fn next_window_title(&mut self, app: &App) -> Option<String> {
    presence::next_window_title(&mut self.window_title, app)
  }

  /// On teardown: the default title to restore if this session changed it.
  pub fn window_title_reset(&mut self) -> Option<&'static str> {
    presence::window_title_reset(&mut self.window_title)
  }

  /// Clear any published rich presence on the way out.
  pub fn clear_presence(&self) {
    #[cfg(feature = "discord-rpc")]
    if let Some(ref manager) = self.discord_rpc_manager {
      manager.clear();
    }
  }
}
