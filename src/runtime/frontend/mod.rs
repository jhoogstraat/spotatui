//! The embedded runtime seam: boot Spotatui's services without any terminal
//! UI, drive them through the shared [`Action`] vocabulary, and observe
//! immutable snapshots.
//!
//! Public surface, deliberately Spotatui-shaped and nothing more:
//! [`Runtime::boot`], [`Runtime::subscribe`], [`Runtime::apply`],
//! [`Runtime::shutdown`], the re-exported [`Action`]/[`ActionOutcome`]/
//! [`Onboarding`] types, and this module's snapshot type. `App`, `IoEvent`,
//! `Network`, rspotify, and librespot types stay private.

use anyhow::Result;
use log::info;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex};

pub use crate::core::action::{ActionOutcome, EngineAction, LibraryTarget};
pub use crate::core::onboarding::Onboarding;
pub use crate::core::plugin_api::{PlaybackState, PlaylistInfo, TrackInfo};
pub use crate::core::source::Source;

/// Library data made visible to embedded frontends. `None` means the
/// corresponding Spotify request has not produced a page yet; `Some(empty)`
/// is a successfully fetched empty list.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LibrarySnapshot {
  pub liked_songs: Option<Vec<TrackInfo>>,
  pub recently_played: Option<Vec<TrackInfo>>,
  pub playlists: Option<Vec<PlaylistInfo>>,
}

/// One published runtime state. Composed from the existing snapshot builders;
/// equality-deduplicated before publication so a quiet tick publishes nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
  /// Current playback state from `plugin_api::playback_state`.
  pub playback: Option<PlaybackState>,
  /// The user's Spotify library data fetched so far.
  pub library: LibrarySnapshot,
  /// Authoritative position at publish time (`None` while stale).
  pub position_ms: Option<u64>,
  /// When this snapshot was folded from engine state; a frontend projects
  /// position forward from `position_ms` using this timestamp.
  pub as_of: std::time::Instant,
  /// The native queue's upcoming items, in order.
  pub queue_upcoming: Vec<TrackInfo>,
  /// Track rows of the latest catalog search.
  pub search_tracks: Vec<TrackInfo>,
  /// A catalog fetch is in flight.
  pub search_loading: bool,
  /// A Spotify session exists.
  pub spotify_connected: bool,
  /// Current status message (the notice surface), if any.
  pub notice: Option<String>,
  /// Whether that notice is an error (blocks normal message overwrites).
  pub notice_is_error: bool,
  /// Native streaming backend is up.
  pub audio_ready: bool,
  /// Native streaming init is still running.
  pub audio_pending: bool,
}

/// Console implementation of the onboarding surface for embedders with no UI
/// of their own (the example binary; tests). Prompts read stdin exactly like
/// the historical console flow.
pub struct ConsoleOnboarding;

impl Onboarding for ConsoleOnboarding {
  fn info(&self, text: &str) {
    println!("{text}");
  }

  fn progress(&self, text: &str) {
    use std::io::Write;
    print!("{text}");
    let _ = std::io::stdout().flush();
  }

  fn prompt_line(&self, prompt: &str) -> Result<String> {
    use std::io::Write;
    print!("{prompt}");
    let _ = std::io::stdout().flush();
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input)
  }

  fn pick_sources(
    &self,
    _options: &[crate::core::source::Source],
  ) -> Result<Option<Vec<crate::core::source::Source>>> {
    // Embedded boots never run the first-run picker.
    Ok(None)
  }
}

/// Boot inputs. Everything else derives from the data root.
#[derive(Debug, Clone)]
pub struct Options {
  /// Directory under which config, cache, and state directories are created
  /// (`config/`, `cache/`, `state/`). Nothing outside this root is read or
  /// written — including librespot's credential cache.
  pub data_root: std::path::PathBuf,
}

impl Options {
  pub fn new(data_root: impl Into<std::path::PathBuf>) -> Self {
    Self {
      data_root: data_root.into(),
    }
  }
}

use super::engine::{Changed, EngineCommand, EngineEvent, Spine};

pub use super::engine::EngineCommand as FrontendEngineCommand;

/// An embedded Spotatui runtime: shared service startup, the driver loop on
/// its own thread, an event-spine-fed snapshot publisher, and the quit
/// sequence. The engine emits [`EngineEvent`]s through its [`Spine`]; the
/// publisher folds them into deduplicated immutable snapshots (ADR 0016
/// stage 2). No bell rings without an event behind it.
pub struct Runtime {
  rt: tokio::runtime::Runtime,
  app: Arc<Mutex<App>>,
  snapshot_tx: watch::Sender<Snapshot>,
  shutdown_tx: watch::Sender<bool>,
  spine: Spine,
  /// Wake handle for the driver backstop loop. External producers that
  /// mutate App-side pending state (e.g. a scripted volume ramp) can poke
  /// this so flush duties run promptly instead of waiting out the backstop.
  /// Currently only the shutdown path reads it, via `self.tick_wake` never
  /// being poked externally — kept for ticket 16's producer rewiring.
  #[allow(dead_code)]
  tick_wake: Arc<tokio::sync::Notify>,
  tick_done_rx: Option<tokio::sync::oneshot::Receiver<()>>,
}

use crate::core::app::App;

impl Runtime {
  /// Boot the runtime synchronously. Runs the full shared bootstrap
  /// (config, state restore, Spotify authentication) plus every shared
  /// service, then starts the tick loop and snapshot publisher on an owned
  /// Tokio runtime. Blocks while interactive onboarding prompts run.
  ///
  /// No console prompt other than `Onboarding` can fire in this mode: the
  /// first-run source picker, the telemetry opt-in, and the auth-setup
  /// migration prompt are all skipped.
  pub fn boot(options: Options, onboarding: Arc<dyn Onboarding>) -> Result<Self> {
    crate::core::paths::set_data_root(options.data_root);
    // Seed client.yml before the bootstrap reads it: the shared client ID
    // and port 8989 mean the auth setup wizard is never reached, and this
    // Connect device stays distinguishable from a concurrently running TUI.
    // The only prompt left is the manual redirect-URL paste.
    let mut client_config = crate::core::config::ClientConfig::new();
    client_config.seed_default_spotify_config("Rust Player")?;
    let rt = tokio::runtime::Builder::new_multi_thread()
      .enable_all()
      .build()?;
    let boot = rt.block_on(super::bootstrap::boot(
      &super::bootstrap::BootOptions {
        config_path: None,
        reconfigure_auth: false,
        // No console prompt may fire from an embedded boot.
        ui_launch: false,
        cli_subcommand: false,
        update_check_enabled: false,
      },
      onboarding,
    ))?;
    let services = rt.block_on(super::startup::start_services(boot))?;

    let app = Arc::clone(&services.app);
    let (snapshot_tx, _snapshot_rx) = watch::channel(Snapshot::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // The event spine is opened by `start_services` so every producer shares
    // one channel; this side receives events, the fold reduces them. On legs
    // without `streaming` no producer exists, so the frontend opens a local
    // spine just for command-side pokes.
    #[cfg(not(feature = "streaming"))]
    let (command_spine, events_rx) = Spine::open(Default::default());
    #[cfg(feature = "streaming")]
    let events_rx = services.spine_rx;
    // The frontend's own spine handle for command-side pokes: on streaming
    // legs the command path reuses the shared producer spine so ordering with
    // player events is preserved.
    #[cfg(feature = "streaming")]
    let command_spine = services.spine.clone();
    let spine = command_spine.clone();
    // The fold task: consumes typed spine events, reduces them into App
    // (single writer of the streaming mirrors), rebuilds once, and
    // equality-dedups the publication. Also wakes on the legacy change bell
    // until every producer is migrated (tickets 15–17 delete it).
    {
      let app = Arc::clone(&app);
      let tx = snapshot_tx.clone();
      rt.spawn(async move {
        fold_loop(app, tx, events_rx).await;
      });
    }
    // Legacy-bell wake: `apply`, the pump, and remaining direct writers ring
    // `change_notify`; a small proxy converts those pokes into spine events so
    // the fold stays the only place that decides when to republish.
    {
      let app = Arc::clone(&app);
      let spine = command_spine.clone();
      rt.spawn(async move {
        let notify = Arc::clone(&app.lock().await.change_notify);
        loop {
          notify.notified().await;
          spine.emit(EngineEvent::changed(Changed::Session)).await;
        }
      });
    }

    // The tick thread owns `Driver`, which is deliberately `!Send` (the Lua
    // scripting engine is thread-local). Run it on a dedicated OS thread with
    // a current-thread reactor; everything it shares (`Arc<Mutex<App>>`, the
    // shutdown watch, the tick-wake notify) is `Send`.
    let (tick_done_tx, tick_done_rx) = tokio::sync::oneshot::channel::<()>();
    let tick_wake = Arc::new(tokio::sync::Notify::new());
    {
      let app = Arc::clone(&app);
      let shared_position = services.shared_position_for_ui;
      let history_collector = services.history_collector;
      let tick_wake = Arc::clone(&tick_wake);
      std::thread::Builder::new()
        .name("spotatui-frontend-tick".to_string())
        .spawn(move || {
          let tick_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("frontend tick runtime");
          tick_rt.block_on(async move {
            driver_loop(
              app,
              shutdown_rx,
              shared_position,
              history_collector,
              tick_wake,
            )
            .await;
          });
          let _ = tick_done_tx.send(());
        })
        .expect("spawn frontend tick thread");
    }

    Ok(Runtime {
      rt,
      app,
      snapshot_tx,
      shutdown_tx,
      spine,
      tick_wake,
      tick_done_rx: Some(tick_done_rx),
    })
  }

  /// Subscribe to immutable snapshots. The current state arrives immediately.
  pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
    self.snapshot_tx.subscribe()
  }

  /// The engine's spine handle: producers inside this process emit events
  /// here instead of poking shared state.
  pub fn spine(&self) -> &Spine {
    &self.spine
  }

  /// Execute one external command. Returns `false` when it cannot be
  /// dispatched (no session for it). Commands funnel through `App::apply`'s
  /// ownership-aware arms; completion arrives later as state on the watch.
  pub fn execute(&self, command: EngineCommand) -> bool {
    let action = match command {
      EngineCommand::PlayUris { uris, offset } => EngineAction::PlayUris { uris, offset },
      EngineCommand::Pause => EngineAction::Pause,
      EngineCommand::Resume => EngineAction::Play,
      EngineCommand::Seek(ms) => EngineAction::SeekTo(u32::try_from(ms).unwrap_or(u32::MAX)),
      EngineCommand::NextTrack => EngineAction::NextTrack,
      EngineCommand::PreviousTrack => EngineAction::PreviousTrack,
      EngineCommand::Volume(percent) => EngineAction::SetVolume(percent.min(100)),
      EngineCommand::EnqueueNative(track) => EngineAction::EnqueueNative(track),
      EngineCommand::RemoveNativeQueued(index) => EngineAction::RemoveNativeQueued(index),
      EngineCommand::MoveNativeQueued { index, up } => EngineAction::MoveNativeQueued { index, up },
      EngineCommand::ClearNativeQueue => EngineAction::ClearNativeQueue,
      EngineCommand::SearchActiveSource(query) => EngineAction::SearchActiveSource(query),
      EngineCommand::Browse(target) => EngineAction::OpenLibrary(target),
      EngineCommand::Reauthenticate => EngineAction::BeginSpotifyLogin,
      EngineCommand::DismissNotice => EngineAction::DismissNotice,
    };
    self.apply(action);
    true
  }

  /// Apply one action synchronously. Infallible at accept time: failures
  /// arrive later as notices in a published snapshot.
  pub fn apply(&self, action: EngineAction) -> ActionOutcome {
    self.rt.block_on(async {
      let outcome = {
        let mut app = self.app.lock().await;
        app.apply(action)
      };
      // Event-spine wake: one StateChanged covers any mutation apply made.
      self.spine.emit_now(EngineEvent::changed(Changed::Session));
      // Legacy bell as well while internal producers still rely on it;
      // later ADR 0016 stages remove it with the mega-mutex.
      self.app.lock().await.change_notify.notify_one();
      outcome
    })
  }

  /// The Tokio handle the runtime runs on, for host tasks that must share
  /// its reactor.
  pub fn handle(&self) -> tokio::runtime::Handle {
    self.rt.handle().clone()
  }

  /// Shut down cleanly: persist the playback session and debounced runtime
  /// state, pause native playback, stop the collector, and end every task
  /// this runtime started. Blocks until the quit sequence finishes.
  pub fn shutdown(self) -> Result<()> {
    self.rt.block_on(async {
      let _ = self.shutdown_tx.send(true);
      if let Some(done) = self.tick_done_rx {
        // The tick thread runs the quit sequence (session persist, state
        // flush, native pause) before it signals done.
        let _ = done.await;
      }
    });
    drop(self.snapshot_tx);
    Ok(())
  }

  /// Install the fork's panic hook so audio-backend panics stay recoverable.
  /// Call once before boot.
  pub fn install_panic_hook() {
    super::bootstrap::install_panic_hook();
  }
}

/// Activity classes that decide the driver's wake cadence. The old loop ran
/// `Driver::tick` every 250 ms unconditionally; this loop runs it *when
/// something happens* plus a slow backstop for the few duties still sized in
/// wall-clock time (API poll resync, watchdog, notice expiry — each becomes a
/// dedicated timer in stage 3, shrinking the backstop further).
#[derive(Debug, Clone, Copy, PartialEq)]
enum DriverWake {
  /// Playback state says audio is advancing; position/keepalive cadence.
  Playing,
  /// Nothing playing but a session exists (poll resync, watchdog rearm).
  Idle,
}

impl DriverWake {
  fn backstop(self) -> Duration {
    match self {
      // While playing the position advances under events from librespot;
      // this backstop just bounds worst-case stall if events starve.
      DriverWake::Playing => Duration::from_millis(250),
      // Idle: only the Web-API resync poll (5 s default) and the watchdog
      // need wall-clock attention between real events.
      DriverWake::Idle => Duration::from_secs(2),
    }
  }
}

async fn driver_loop(
  app: Arc<Mutex<App>>,
  mut shutdown_rx: watch::Receiver<bool>,
  shared_position: Option<Arc<std::sync::atomic::AtomicU64>>,
  history_collector: crate::infra::history::HistoryCollectorHandle,
  tick_wake: Arc<tokio::sync::Notify>,
) {
  use crate::core::driver::{DiscordRpcHandle, MprisHandle};
  #[cfg(all(feature = "mpris", target_os = "linux"))]
  let mpris_manager: MprisHandle = None;
  #[cfg(not(all(feature = "mpris", target_os = "linux")))]
  let mpris_manager: MprisHandle = None;
  #[cfg(feature = "discord-rpc")]
  let discord_rpc_manager: DiscordRpcHandle = None;
  #[cfg(not(feature = "discord-rpc"))]
  let discord_rpc_manager: DiscordRpcHandle = None;
  let mut driver =
    crate::core::driver::Driver::new(shared_position, mpris_manager, discord_rpc_manager);

  // The startup dispatch fires at boot, not on a timer.
  {
    let mut app = app.lock().await;
    driver.dispatch_startup(&mut app);
    app.change_notify.notify_one();
  }

  let mut last_tick_at = Instant::now();
  loop {
    // Pick the backstop from live playback state; nothing wakes without an
    // event or this (long) deadline.
    let wake = {
      let app = app.lock().await;
      if app.playback_position_ms().is_some() && app.native_is_playing.unwrap_or(false) {
        DriverWake::Playing
      } else {
        DriverWake::Idle
      }
    };
    let backstop = tokio::time::sleep(wake.backstop());
    tokio::pin!(backstop);
    tokio::select! {
      _ = &mut backstop => {}
      _ = tick_wake.notified() => {}
      changed = shutdown_rx.changed() => {
        if changed.is_err() || *shutdown_rx.borrow() {
          break;
        }
        continue;
      }
    }
    let elapsed = last_tick_at.elapsed();
    last_tick_at = Instant::now();
    {
      let mut app = app.lock().await;
      driver.tick(
        &mut app,
        elapsed,
        crate::core::driver::TickEnv {
          now: Instant::now(),
          #[cfg(any(feature = "audio-viz", feature = "audio-viz-cpal"))]
          viz_bars: None,
          #[cfg(feature = "art-decode")]
          cover_art_full_image_support: false,
        },
      );
      // Tick-produced changes (poll resync, notice expiry) republish via the
      // legacy bell; the bell proxy converts that into a spine event for the
      // fold. Typed streaming events bypass the bell entirely (ticket 14).
      app.change_notify.notify_one();
    }
    if *shutdown_rx.borrow_and_update() {
      break;
    }
  }

  // The runner's quit sequence, minus terminal teardown.
  let session = app.lock().await.current_persisted_session();
  if let Some(session) = session {
    if let Ok(path) = crate::core::persisted_playback::default_session_path() {
      if let Err(e) = crate::core::persisted_playback::save(&path, &session) {
        log::warn!("[session] failed to persist playback session on exit: {e}");
      }
    }
  }
  {
    let mut app = app.lock().await;
    driver.on_quit(&mut app);
  }
  {
    let mut app = app.lock().await;
    app.flush_state_save(true);
  }
  #[cfg(feature = "streaming")]
  super::startup::pause_native_playback_before_exit(&app).await;
  history_collector.shutdown().await;
  info!("frontend runtime shut down cleanly");
}

impl Default for Snapshot {
  fn default() -> Self {
    Snapshot {
      playback: None,
      library: LibrarySnapshot::default(),
      position_ms: None,
      as_of: Instant::now(),
      queue_upcoming: Vec::new(),
      search_tracks: Vec::new(),
      search_loading: false,
      spotify_connected: false,
      notice: None,
      notice_is_error: false,
      #[cfg(feature = "streaming")]
      audio_ready: false,
      #[cfg(not(feature = "streaming"))]
      audio_ready: false,
      #[cfg(feature = "streaming")]
      audio_pending: false,
      #[cfg(not(feature = "streaming"))]
      audio_pending: false,
    }
  }
}

/// The fold task (ADR 0016 ticket 14): one consumer of the spine, the single
/// writer of published state. Typed streaming events are reduced through the
/// domain before the snapshot rebuild; coarse `StateChanged` pokes just
/// trigger a rebuild.
async fn fold_loop(
  app: Arc<Mutex<App>>,
  tx: watch::Sender<Snapshot>,
  mut events_rx: tokio::sync::mpsc::Receiver<EngineEvent>,
) {
  loop {
    let event = tokio::select! {
      event = events_rx.recv() => match event {
        Some(event) => event,
        None => break, // spine closed
      },
      _ = tx.closed() => break,
    };
    if let EngineEvent::Streaming(streaming) = &event {
      #[cfg(feature = "streaming")]
      {
        let mut app = app.lock().await;
        app.apply_streaming_state(streaming.clone());
      }
      #[cfg(not(feature = "streaming"))]
      {
        let _ = streaming;
      }
    }
    let guard = app.lock().await;
    let next = build_snapshot(&guard);
    drop(guard);
    tx.send_if_modified(|current| {
      if *current == next {
        false
      } else {
        *current = next;
        true
      }
    });
    if tx.is_closed() {
      break;
    }
  }
}

fn build_snapshot(app: &App) -> Snapshot {
  let search = crate::core::plugin_api::search_results_snapshot(app);
  Snapshot {
    as_of: Instant::now(),
    playback: crate::core::plugin_api::playback_state(app),
    library: LibrarySnapshot {
      liked_songs: (!app.library.saved_tracks.pages.is_empty())
        .then(|| crate::core::plugin_api::saved_tracks_snapshot(app)),
      recently_played: app
        .recently_played
        .result
        .as_ref()
        .map(|_| crate::core::plugin_api::recently_played_snapshot(app)),
      playlists: app
        .playlists
        .as_ref()
        .map(|_| crate::core::plugin_api::playlists_snapshot(app)),
    },
    position_ms: app.playback_position_ms().map(|p| p as u64),
    queue_upcoming: app.native_queue.clone(),
    search_tracks: search.tracks,
    search_loading: app.search_loading,
    spotify_connected: app.spotify_connected,
    notice: app.status_message.clone(),
    notice_is_error: app.status_message_is_error,
    #[cfg(feature = "streaming")]
    audio_ready: app.streaming_player.is_some(),
    #[cfg(not(feature = "streaming"))]
    audio_ready: false,
    #[cfg(feature = "streaming")]
    audio_pending: app.native_backend_pending,
    #[cfg(not(feature = "streaming"))]
    audio_pending: false,
  }
}
