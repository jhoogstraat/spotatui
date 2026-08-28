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
use rspotify::model::idtypes::Id as _;
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

use crate::core::app::{
  ActiveBlock, AlbumTableContext, App, EpisodeTableContext, RouteId, ScrollableResultPages,
  SelectedAlbum, SelectedFullAlbum, SelectedFullShow, SelectedShow,
};
use crate::core::catalog::CatalogStateEvent;
use crate::core::catalog::PlaylistSelection;
#[cfg(feature = "streaming")]
use crate::infra::network::library::structurize_playlist_folders;
use crate::infra::network::library::{
  build_flat_playlist_items, maybe_prompt_community_pin, reconcile_playlist_selection,
};
use crate::infra::network::IoEvent;
use crate::runtime::engine::{DeviceList, FoldAck, HostSyncState, PlaylistRefreshPrevious};

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
    // one channel; this side receives events, the fold reduces them.
    let events_rx = services.spine_rx;
    // The fold also answers producer queries (ticket 16): claim/release,
    // staleness checks, and the small read set detached workers need.
    let folds = rt.block_on(services.spine_queries.take());
    // The frontend's own spine handle for command-side pokes: on streaming
    // legs the command path reuses the shared producer spine so ordering with
    // player events is preserved.
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
        fold_loop(app, tx, events_rx, folds).await;
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
/// trigger a rebuild. Session facts (ticket 16) replay the exact old handler
/// code under the fold's lock; producer queries are answered here too, so no
/// producer outside this task ever locks `App`.
/// Open a spine, install it on `App`, and spawn the fold task (ticket 17).
/// Command surfaces that live outside the full runtime boot (CLI mode, and
/// spine-driven tests) need a live fold for their queries and events to be
/// answered. Returns the spine handle and the snapshot watch receiver.
pub(crate) async fn spawn_fold(
  app: &Arc<tokio::sync::Mutex<App>>,
) -> (
  crate::runtime::engine::Spine,
  tokio::sync::watch::Receiver<Snapshot>,
) {
  let (spine, events_rx, spine_queries) = crate::runtime::engine::Spine::open(Default::default());
  app.lock().await.spine = Some(spine.clone());
  let spine_queries = spine_queries.take().await;
  let (snapshot_tx, snapshot_rx) = tokio::sync::watch::channel(Snapshot {
    playback: None,
    library: Default::default(),
    position_ms: None,
    as_of: std::time::Instant::now(),
    queue_upcoming: Vec::new(),
    search_tracks: Vec::new(),
    search_loading: false,
    spotify_connected: false,
    notice: None,
    notice_is_error: false,
    audio_ready: false,
    audio_pending: false,
  });
  tokio::spawn(fold_loop(
    app.clone(),
    snapshot_tx,
    events_rx,
    spine_queries,
  ));
  (spine, snapshot_rx)
}

/// The fold task (ADR 0016 ticket 14): one consumer of the spine, the single
/// writer of App state outside producers. `pub(super)` so the CLI mode can
/// run one for its subcommand lifetime (ticket 17: the CLI reads published
/// state instead of locking App).
pub(super) async fn fold_loop(
  app: Arc<Mutex<App>>,
  tx: watch::Sender<Snapshot>,
  mut events_rx: tokio::sync::mpsc::Receiver<EngineEvent>,
  folds: Option<crate::runtime::engine::FoldHandles>,
) {
  let (mut queries, mut acks) = match folds {
    Some(handles) => (Some(handles.queries), Some(handles.acks)),
    None => (None, None),
  };
  loop {
    // Answer any producer query that arrived since the last event before
    // (possibly) blocking on the next one: the try_recv drain below clears a
    // backlog, and the select watches the same channels so a quiet spine
    // with a pending query still answers.
    if let Some(queries) = queries.as_mut() {
      while let Ok((query, responder)) = queries.rx.try_recv() {
        let answer = {
          let mut guard = app.lock().await;
          answer_query(&mut guard, query)
        };
        let _ = responder.send(Ok(answer));
      }
    }
    // Apply any acked event the same way: fold it, then complete the
    // responder with the post-fold facts (stream order is queue order, so
    // the answer reflects every event queued before it).
    if let Some(acks) = acks.as_mut() {
      while let Ok((event, responder)) = acks.rx.try_recv() {
        let ack = {
          let mut guard = app.lock().await;
          apply_acked(&mut guard, &event)
        };
        let _ = responder.send(ack);
        publish(&app, &tx).await;
      }
    }
    // The select keeps queries and acks responsive while parked on events:
    // producers on quiet surfaces (CLI mode, DJ turn loop, MCP tools) block
    // on answers, so the fold must not wait for an unrelated event first.
    let event = tokio::select! {
      event = events_rx.recv() => match event {
        Some(event) => event,
        None => break, // spine closed
      },
      answer = async {
        match queries.as_mut() {
          Some(queries) => queries.rx.recv().await,
          None => std::future::pending().await,
        }
      } => {
        match answer {
          Some((query, responder)) => {
            let answer = {
              let mut guard = app.lock().await;
              answer_query(&mut guard, query)
            };
            let _ = responder.send(Ok(answer));
          }
          // Query channel closed (spine dropped): stop watching it.
          None => queries = None,
        }
        continue;
      }
      acked = async {
        match acks.as_mut() {
          Some(acks) => acks.rx.recv().await,
          None => std::future::pending().await,
        }
      } => {
        match acked {
          Some((event, responder)) => {
            let ack = {
              let mut guard = app.lock().await;
              apply_acked(&mut guard, &event)
            };
            let _ = responder.send(ack);
            publish(&app, &tx).await;
          }
          // Ack channel closed: stop watching it.
          None => acks = None,
        }
        continue;
      }
      _ = tx.closed() => break,
    };
    match &event {
      EngineEvent::Streaming(streaming) => {
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
      EngineEvent::Catalog(catalog) => {
        let mut app = app.lock().await;
        crate::core::apply_catalog_state(&mut app, catalog);
      }
      EngineEvent::Session(fact) => {
        let mut app = app.lock().await;
        apply_session_fact(&mut app, fact.clone());
      }
      EngineEvent::StateChanged(_) => {}
    }
    publish(&app, &tx).await;
    if tx.is_closed() {
      break;
    }
  }
}

/// Rebuild the snapshot and equality-dedup the publication.
async fn publish(app: &Arc<Mutex<App>>, tx: &watch::Sender<Snapshot>) {
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
}
fn apply_acked(app: &mut App, event: &EngineEvent) -> Option<FoldAck> {
  match event {
    EngineEvent::Catalog(CatalogStateEvent::SavedTracksPageUpserted { .. }) => {
      let outcome = crate::core::apply_catalog_state(app, event_catalog(event));
      Some(FoldAck {
        next_saved_tracks_offset: if outcome.stale_event_dropped {
          None
        } else {
          app.next_missing_saved_tracks_offset_continuous()
        },
        next_playlist_tracks_offset: None,
      })
    }
    EngineEvent::Catalog(CatalogStateEvent::PlaylistTracksPageUpserted { .. }) => {
      let outcome = crate::core::apply_catalog_state(app, event_catalog(event));
      Some(FoldAck {
        next_saved_tracks_offset: None,
        next_playlist_tracks_offset: if outcome.stale_event_dropped {
          None
        } else {
          app.next_missing_playlist_tracks_offset_continuous()
        },
      })
    }
    _ => {
      // Only catalog upserts carry post-fold facts today; other acked
      // events fold normally and answer the default ack.
      fold_one(app, event);
      Some(FoldAck {
        next_saved_tracks_offset: None,
        next_playlist_tracks_offset: None,
      })
    }
  }
}

/// Fold-side publisher for a completed playlist refresh (ticket 16 batch 2):
/// generation check, folder structure, selection reconcile, community-pin
/// prompt — the whole write section the old detached task locked for.
fn publish_playlist_refresh(
  app: &mut App,
  generation: u64,
  playlists: Vec<crate::core::plugin_api::PlaylistInfo>,
  original_selection: PlaylistSelection,
  pagination_error: Option<String>,
  folder_nodes: crate::runtime::engine::FolderNodes,
) {
  if app.playlist_refresh_generation != generation {
    return;
  }
  if let Some(error) = pagination_error {
    log::warn!("playlist refresh incomplete: {error}");
    if !app.all_playlists.is_empty() {
      // The fold snapshotted nothing extra here: the visible list *is* the
      // previous complete list (page 1 replaced it only when safe), so
      // restoring means reconciling the selection against it.
      reconcile_playlist_selection(
        app,
        original_selection.playlist_id.as_deref(),
        original_selection.folder_id,
        original_selection.selected_index,
      );
    }
    app.set_status_message("Playlist refresh incomplete; kept the previous list.", 8);
    return;
  }

  let folder_items = build_folder_items(app, &playlists, folder_nodes);

  app.all_playlists = playlists;
  app
    .plugin_data_generations
    .bump(crate::core::app::PluginDataKind::Playlists);
  app.playlist_folder_items = folder_items;

  reconcile_playlist_selection(
    app,
    original_selection.playlist_id.as_deref(),
    original_selection.folder_id,
    original_selection.selected_index,
  );

  maybe_prompt_community_pin(app);
}

/// Derive the sidebar folder items: from the fetched rootlist tree when one
/// arrived, otherwise a flat list. On a pagination failure the previous
/// nodes stay in place (nothing is written).
#[cfg(feature = "streaming")]
fn build_folder_items(
  app: &mut App,
  playlists: &[crate::core::plugin_api::PlaylistInfo],
  folder_nodes: crate::runtime::engine::FolderNodes,
) -> Vec<crate::core::app::PlaylistFolderItem> {
  if let Some(nodes) = folder_nodes {
    app._playlist_folder_nodes = Some(nodes.clone());
    return structurize_playlist_folders(&nodes, playlists);
  }
  build_flat_playlist_items(playlists)
}

#[cfg(not(feature = "streaming"))]
fn build_folder_items(
  _app: &mut App,
  playlists: &[crate::core::plugin_api::PlaylistInfo],
  _folder_nodes: crate::runtime::engine::FolderNodes,
) -> Vec<crate::core::app::PlaylistFolderItem> {
  build_flat_playlist_items(playlists)
}
/// Rebuild a `&CatalogStateEvent` reference from an owned event match arm.
fn event_catalog(event: &EngineEvent) -> &CatalogStateEvent {
  match event {
    EngineEvent::Catalog(catalog) => catalog,
    _ => unreachable!("apply_acked only passes catalog events here"),
  }
}

/// Fold one event without publishing (shared by the acked and plain paths).
fn fold_one(app: &mut App, event: &EngineEvent) {
  match event {
    EngineEvent::Streaming(streaming) => {
      #[cfg(feature = "streaming")]
      {
        app.apply_streaming_state(streaming.clone());
      }
      #[cfg(not(feature = "streaming"))]
      {
        let _ = streaming;
      }
    }
    EngineEvent::Catalog(catalog) => {
      crate::core::apply_catalog_state(app, catalog);
    }
    EngineEvent::Session(fact) => apply_session_fact(app, fact.clone()),
    EngineEvent::StateChanged(_) => {}
  }
}

/// The fold's reducer for session facts (ticket 16). Byte-for-byte parity
/// with the inline writes each producer used to make under the App lock.
pub(crate) fn apply_session_fact(app: &mut App, fact: crate::runtime::engine::SessionFact) {
  use crate::runtime::engine::{PostConnectFetch, SessionFact};
  match fact {
    SessionFact::Auth {
      transition,
      new_expiry,
      post_connect_fetches,
    } => {
      let changed = app.auth_state.transition(transition, new_expiry);
      if changed {
        app.change_notify.notify_one();
      }
      // Legacy mirrors (deleted with App itself in ticket 18).
      app.spotify_token_expiry = app.auth_state.token_expiry;
      app.auth_refresh_in_progress = app.auth_state.refresh_in_progress;
      app.spotify_connected = app.auth_state.connected;
      for fetch in post_connect_fetches {
        app.dispatch(match fetch {
          PostConnectFetch::User => IoEvent::GetUser,
          PostConnectFetch::Playlists => IoEvent::GetPlaylists,
          PostConnectFetch::CurrentPlayback => IoEvent::GetCurrentPlayback,
        });
      }
    }
    SessionFact::Status {
      message,
      ttl_secs,
      is_error,
    } => {
      if is_error {
        app.set_error_status_message(message, ttl_secs);
      } else {
        app.set_status_message(message, ttl_secs);
      }
    }
    SessionFact::LoadingCleared => {
      app.is_loading = false;
      app.search_loading = false;
    }
    SessionFact::AuthMirrors {
      token_expiry,
      refresh_in_progress,
      connected,
    } => {
      app.spotify_token_expiry = token_expiry;
      app.auth_refresh_in_progress = refresh_in_progress;
      app.spotify_connected = connected;
    }
    SessionFact::Error(message) => {
      app.handle_error(anyhow::anyhow!(message));
    }
    SessionFact::ClearPendingPlaylistOpen(id) => {
      if id.as_deref().is_none_or(|wanted| {
        app
          .pending_playlist_open
          .as_deref()
          .is_some_and(|current| current == wanted)
      }) {
        app.pending_playlist_open = None;
      }
    }
    SessionFact::DispatchPlaylistRefresh => {
      app.dispatch(IoEvent::GetPlaylists);
    }
    SessionFact::StatsLoadingCleared => {
      app.stats_loading = false;
    }
    SessionFact::ArtistView(artist) => {
      app.artist = Some(*artist);
      app.push_navigation_stack(RouteId::Artist, ActiveBlock::ArtistBlock);
    }
    SessionFact::AlbumTracksSimplified { album, tracks } => {
      app.selected_album_simplified = Some(SelectedAlbum {
        album,
        tracks,
        selected_index: 0,
      });
      app.album_table_context = AlbumTableContext::Simplified;
      app.push_navigation_stack(RouteId::AlbumTracks, ActiveBlock::AlbumTracks);
    }
    SessionFact::AlbumTracksFull { album } => {
      app.selected_album_full = Some(SelectedFullAlbum {
        album,
        selected_index: 0,
      });
      app.album_table_context = AlbumTableContext::Full;
      app.push_navigation_stack(RouteId::AlbumTracks, ActiveBlock::AlbumTracks);
    }
    SessionFact::ShowEpisodesLanded { page, show } => {
      app.library.show_episodes = ScrollableResultPages::new();
      app.library.show_episodes.add_pages(page);
      app.selected_show_simplified = Some(SelectedShow { show });
      app.episode_table_context = EpisodeTableContext::Simplified;
      app.push_navigation_stack(RouteId::PodcastEpisodes, ActiveBlock::EpisodeTable);
    }
    SessionFact::ShowEpisodesAppended { page } => {
      app.library.show_episodes.add_pages(page);
    }
    SessionFact::FullShowLanded { show } => {
      app.selected_show_full = Some(SelectedFullShow { show });
      app.episode_table_context = EpisodeTableContext::Full;
      app.push_navigation_stack(RouteId::PodcastEpisodes, ActiveBlock::EpisodeTable);
    }
    SessionFact::FollowedArtistsConfirmed(ids) => {
      for id in ids {
        app.followed_artist_ids_set.insert(id);
      }
    }
    SessionFact::ClearNativeShuffleSession => {
      #[cfg(feature = "streaming")]
      app.clear_native_shuffle_session();
    }
    SessionFact::ClearNativeShufflePendingReload => {
      // The load that would have confirmed this reload failed; a later
      // genuine `TrackChanged` must not be mistaken for it.
      #[cfg(feature = "streaming")]
      if let Some(session) = app.native_spotify_shuffle.as_mut() {
        session.pending_reload_index = None;
      }
    }
    #[cfg(feature = "dj-core")]
    SessionFact::DjLibraryIndexed(result) => {
      use crate::infra::dj::DjLine;
      app.dj.library_indexing = false;
      match result {
        Ok(library) => {
          let summary = library.summary();
          app.dj.library = Some(*library);
          app.dj.push_line(DjLine::system(summary.clone()));
          app.set_status_message(format!("DJ: {summary}"), 5);
        }
        Err(e) => {
          log::warn!("DJ: could not index the library: {e}");
          let message = format!("DJ: could not read your playlists ({e}); recommending unfiltered");
          app.dj.push_line(DjLine::system(message.clone()));
          app.set_error_status_message(message, 8);
        }
      }
    }
    #[cfg(feature = "ai-dj")]
    SessionFact::DjTurnFinished {
      turn_seq,
      outcome,
      vibe_on_success,
      must_act,
    } => {
      use crate::infra::dj::DjLine;
      // Whether this turn is still the current one. Only if it still owns the
      // flag: an abandoned turn finishing would otherwise clear the flag its
      // replacement had already set, and the refill tick gates on exactly that
      // flag.
      let owned = app.dj.turn_seq == turn_seq;
      app.dj.finish_turn(turn_seq);
      match outcome {
        Ok(summary) => {
          // The listener's words only become the standing auto-queue direction
          // when the turn actually did something. Otherwise "what did I play
          // last week?" would silently steer every later refill. A vibe the
          // model set itself is more considered than the raw sentence, so it
          // wins.
          if summary.acted && !summary.abandoned && !summary.vibe_set {
            if let Some(vibe) = vibe_on_success {
              app.dj.vibe = Some(vibe);
            }
          }
          // A refill happens while the listener is on another screen, so the
          // transcript is not a surface they can see. The interactive case is
          // deliberately left transcript-only.
          if must_act && !summary.abandoned {
            if summary.acted {
              app.set_status_message("DJ: topped up the queue".to_string(), 5);
            } else {
              // A turn that had to queue and did not is a failure, not a quiet
              // success. A vibe shift has just *emptied* the queue, and with
              // auto-queue off nothing retries — so saying nothing leaves the
              // listener with silence and no explanation.
              let message = "DJ: nothing was queued that time. Ask again, or try a different \
                             direction.";
              app.dj.push_line(DjLine::system(message));
              app.set_error_status_message(message.to_string(), 6);
            }
          }
        }
        Err(e) => {
          // Silent when the turn has been superseded. An agent CLI hits its
          // timeout a minute and a half after the listener moved on, and a
          // failure toast for a turn they abandoned would land over the
          // conversation they are now having.
          if owned {
            // Naming the backend matters when several are configurable: "DJ
            // error" alone does not say whether to check the CLI or the key.
            let backend = crate::infra::dj::session::build_brain(&app.user_config.behavior)
              .map(|brain| brain.label())
              .unwrap_or("DJ");
            let message = format!("DJ error ({backend}): {e}");
            app.dj.push_line(DjLine::system(message.clone()));
            // A toast, never `handle_error` — that pushes a modal error page
            // over whatever the user was doing.
            app.set_error_status_message(message, 8);
          }
        }
      }
    }
    SessionFact::ApplyAction(action) => {
      // Ticket 17: the DJ tools / MCP executor mutations. The fold is the
      // single writer, so their old inline `app.apply(...)` under the lock
      // becomes a fact reduced here; publishing happens on the normal
      // event path.
      app.apply(action);
    }
    #[cfg(feature = "dj-core")]
    SessionFact::DjPushLine { speaker, text } => {
      app.dj.push_line(crate::infra::dj::DjLine { speaker, text });
    }
    #[cfg(feature = "dj-core")]
    SessionFact::DjStep(step) => {
      app.dj.step = step;
    }
    SessionFact::User(result) => match result {
      Ok(user) => {
        app.user = Some(user);
      }
      Err(message) => {
        // The rate-limit notice: the same sink the old inline write used.
        app.set_status_message(message, 6);
      }
    },
    SessionFact::Lyrics {
      identity,
      lyrics,
      synced,
      found,
    } => {
      // Re-gate on the track-change detector's identity (the old inline
      // stale-check before every write): a fetch for a song that is no
      // longer wanted must not land.
      if app.desired_lyrics_identity.as_ref() == Some(&identity) {
        if found {
          app.lyrics = lyrics;
          app.lyrics_synced = synced;
          app.lyrics_status = crate::core::app::LyricsStatus::Found;
        } else {
          app.lyrics_status = crate::core::app::LyricsStatus::NotFound;
        }
        app
          .plugin_data_generations
          .bump(crate::core::app::PluginDataKind::Lyrics);
      }
    }
    SessionFact::GlobalSongCount { count, failed } => {
      if let Some(count) = count {
        app.global_song_count = Some(count);
        app.global_song_count_failed = false;
      } else if failed {
        app.global_song_count_failed = true;
      }
    }
    SessionFact::Announcements(announcements) => {
      let had_active_announcement = app.active_announcement.is_some();
      app.enqueue_announcements(announcements);
      if !had_active_announcement && app.active_announcement.is_some() {
        app.push_navigation_stack(
          crate::core::app::RouteId::AnnouncementPrompt,
          crate::core::app::ActiveBlock::AnnouncementPrompt,
        );
      }
    }
    SessionFact::StatsLoaded {
      period,
      stats,
      streaks,
    } => {
      app.listening_streaks = Some(streaks);
      // Cycling periods quickly can race two loads; only the response for
      // the currently selected period may land.
      if app.stats_period == period {
        app.stats_data = Some(stats);
        app.stats_loading = false;
      }
    }
    SessionFact::Party { status, session } => {
      app.party_status = status;
      app.party_session = session;
    }
    SessionFact::PartySessionUpdate(update) => {
      use crate::infra::network::sync::PartySessionFact;
      match update {
        PartySessionFact::RoomCode(code) => {
          if let Some(session) = &mut app.party_session {
            session.code = code;
          }
        }
        PartySessionFact::HostName(name) => {
          if let Some(session) = &mut app.party_session {
            session.host_name = name;
          }
        }
        PartySessionFact::GuestJoined(name) => {
          if let Some(session) = &mut app.party_session {
            if !session.guests.contains(&name) {
              session.guests.push(name.clone());
            }
          }
          // The old relay handlers wrote the toast fields directly (unscaled
          // TTL, error message untouched) — keep that parity, not the
          // `set_status_message` sink's scaling and error-suppression rules.
          app.status_message = Some(format!("{name} joined the party"));
          app.status_message_expires_at = Some(Instant::now() + Duration::from_secs(3));
        }
        PartySessionFact::GuestLeft(name) => {
          if let Some(session) = &mut app.party_session {
            if let Some(pos) = session.guests.iter().position(|g| g == &name) {
              session.guests.remove(pos);
            }
          }
          app.status_message = Some(format!("{name} left the party"));
          app.status_message_expires_at = Some(Instant::now() + Duration::from_secs(3));
        }
        PartySessionFact::ControlMode(mode) => {
          if let Some(session) = &mut app.party_session {
            session.control_mode = mode;
          }
        }
        PartySessionFact::Ended(message) => {
          app.party_status = crate::infra::network::sync::PartyStatus::Disconnected;
          app.party_session = None;
          // Direct write for parity with the old relay handler (see above).
          app.status_message = Some(message);
          app.status_message_expires_at = Some(Instant::now() + Duration::from_secs(5));
        }
        PartySessionFact::Error(message) => {
          app.party_status = crate::infra::network::sync::PartyStatus::Disconnected;
          app.party_session = None;
          app.handle_error(anyhow::anyhow!("Party: {message}"));
        }
      }
    }
    SessionFact::RecentlyPlayed { page, navigate } => {
      app.recently_played.result = Some(page);
      app.sort_recently_played_items();
      app
        .plugin_data_generations
        .bump(crate::core::app::PluginDataKind::RecentlyPlayed);
      if navigate {
        app.push_navigation_stack(
          crate::core::app::RouteId::RecentlyPlayed,
          crate::core::app::ActiveBlock::RecentlyPlayed,
        );
      }
    }
    SessionFact::DispatchIo(events) => {
      for event in events {
        app.dispatch(event);
      }
    }
    SessionFact::CreatePlaylistSearchResults(tracks) => {
      app.create_playlist_search_results = tracks;
    }
    SessionFact::RecommendedTracks { tracks } => {
      app.track_table.tracks = tracks;
      app.track_table.context = Some(crate::core::app::TrackTableContext::RecommendedTracks);
      app.push_navigation_stack(
        crate::core::app::RouteId::Recommendations,
        crate::core::app::ActiveBlock::TrackTable,
      );
    }
    SessionFact::QueueLanded(queue) => {
      app.queue = queue;
      // Completion, not success, is what plugin data requests wait on.
      app
        .plugin_data_generations
        .bump(crate::core::app::PluginDataKind::Queue);
    }
    SessionFact::TransientStatus { message, ttl_secs } => {
      // Raw write for parity with the old direct `status_message` stores: no
      // TTL scaling, no error coupling.
      app.status_message = Some(message);
      app.status_message_expires_at = Some(Instant::now() + Duration::from_secs(ttl_secs));
    }
    SessionFact::VolumeChangeDropped => {
      app.is_volume_change_in_flight = false;
      app.pending_volume = None;
      app.last_dispatched_volume = None;
    }
    SessionFact::TransportStateMirrored {
      is_playing,
      shuffle,
      repeat,
      volume,
    } => {
      if let Some(v) = is_playing {
        if let Some(ctx) = &mut app.current_playback_context {
          ctx.is_playing = v;
        }
      }
      if let Some(v) = shuffle {
        if let Some(ctx) = &mut app.current_playback_context {
          ctx.shuffle_state = v;
        }
        app.runtime_state.shuffle_enabled = v;
        app.schedule_state_save(crate::core::state::PersistedRuntimeState::shuffle_enabled(
          v,
        ));
      }
      if let Some(v) = repeat {
        if let Some(ctx) = &mut app.current_playback_context {
          ctx.repeat_state = v;
        }
      }
      if let Some(v) = volume {
        if let Some(ctx) = &mut app.current_playback_context {
          ctx.device.volume_percent = Some(u32::from(v));
        }
        app.runtime_state.volume_percent = v.min(100);
        app.schedule_state_save(crate::core::state::PersistedRuntimeState::volume_percent(
          v.min(100),
        ));
        app.is_volume_change_in_flight = false;
        app.last_dispatched_volume = Some(v);
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::ClearStartPlaybackReplay => {
      // A new transport command supersedes any parked replay.
      app.pending_start_playback = None;
      app.native_load_watchdog = None;
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativePauseMirrored => {
      app.set_native_playback_intent(false);
      app.native_is_playing = Some(false);
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.is_playing = false;
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeSeekMirrored { position_ms } => {
      app.set_native_recovery_position(position_ms);
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeShuffleMirrored { state } => {
      app.set_native_recovery_shuffle(state);
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.shuffle_state = state;
      }
      app.runtime_state.shuffle_enabled = state;
      app.schedule_state_save(crate::core::state::PersistedRuntimeState::shuffle_enabled(
        state,
      ));
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeRepeatMirrored { state } => {
      app.set_native_recovery_repeat(state);
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.repeat_state = state;
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeVolumeMirrored { volume } => {
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.device.volume_percent = Some(u32::from(volume));
      }
      app.runtime_state.volume_percent = volume.min(100);
      app.schedule_state_save(crate::core::state::PersistedRuntimeState::volume_percent(
        volume.min(100),
      ));
      app.is_volume_change_in_flight = false;
      app.last_dispatched_volume = Some(volume);
      // pending_volume stays set — cleared when the API confirms the value.
    }
    SessionFact::DevicesLanded { devices, navigate } => {
      if navigate {
        app.push_navigation_stack(RouteId::SelectedDevice, ActiveBlock::SelectDevice);
      }
      #[cfg(feature = "streaming")]
      let mut payload = rspotify::model::device::DevicePayload { devices };
      #[cfg(not(feature = "streaming"))]
      let payload = rspotify::model::device::DevicePayload { devices };
      #[cfg(feature = "streaming")]
      {
        // The recovery interlock is a check-and-set and gates the merge
        // below, so both stay in this one reducer step.
        let recovering = app.request_native_streaming_recovery_if_disconnected(true);
        if !recovering {
          include_native_streaming_device(app, &mut payload);
        }
      }
      app.devices = Some(payload);
      app
        .plugin_data_generations
        .bump(crate::core::app::PluginDataKind::Devices);
    }
  }
}

/// Merge the native streaming player into a freshly fetched device list
/// (streaming builds): when the player is up, its device joins the payload —
/// or fills in the missing id on the matching Web API row — so the device
/// picker shows the local device even before Spotify lists it.
#[cfg(feature = "streaming")]
fn include_native_streaming_device(
  app: &crate::core::app::App,
  payload: &mut rspotify::model::device::DevicePayload,
) {
  use rspotify::model::{enums::DeviceType, Device};

  let Some(player) = app.streaming_player.as_ref() else {
    return;
  };

  if !player.is_available() {
    return;
  }

  let device_name = player.device_name();
  let device_id = app
    .native_device_id
    .clone()
    .unwrap_or_else(|| player.device_id());

  if let Some(device) = payload
    .devices
    .iter_mut()
    .find(|device| device.name.eq_ignore_ascii_case(device_name))
  {
    if device.id.is_none() {
      device.id = Some(device_id);
    }
    return;
  }

  payload.devices.push(Device {
    id: Some(device_id),
    is_active: app.is_streaming_active,
    is_private_session: false,
    is_restricted: false,
    name: device_name.to_string(),
    _type: DeviceType::Computer,
    volume_percent: Some(player.get_volume().into()),
  });
}

/// Takes `&mut App` because the claim and generation queries mutate the
/// fold-owned bookkeeping — which is exactly the single-writer contract.
pub(crate) fn answer_query(
  app: &mut App,
  query: crate::runtime::engine::FoldQuery,
) -> crate::runtime::engine::FoldAnswer {
  use crate::core::catalog::LibrarySection;
  use crate::runtime::engine::{FoldAnswer, FoldQuery};
  match query {
    FoldQuery::LibraryRequestLive { generation, .. } => {
      FoldAnswer::Bool(app.saved_tracks_prefetch_generation == generation)
    }
    FoldQuery::ClaimLibraryOffset { section, .. } => {
      let claimed = match section {
        LibrarySection::SavedTracks { offset } => {
          app.saved_tracks_prefetch_in_flight.insert(offset)
        }
      };
      FoldAnswer::Bool(claimed)
    }
    FoldQuery::ReleaseLibraryOffset { section } => {
      let LibrarySection::SavedTracks { offset } = section;
      app.saved_tracks_prefetch_in_flight.remove(&offset);
      FoldAnswer::Bool(true)
    }
    FoldQuery::NextLibraryOffset { section } => {
      let offset = match section {
        LibrarySection::SavedTracks { .. } => app.next_missing_saved_tracks_offset_continuous(),
      };
      FoldAnswer::Offset(offset)
    }
    FoldQuery::LikedState { ids } => FoldAnswer::Liked(
      ids
        .iter()
        .map(|id| app.liked_song_ids_set.contains(id))
        .collect(),
    ),
    FoldQuery::UserId => FoldAnswer::User(app.user.as_ref().map(|user| user.id.clone())),
    FoldQuery::PlaylistTrackTableActive { playlist_id } => {
      let active = rspotify::model::idtypes::PlaylistId::from_id(playlist_id.as_str())
        .ok()
        .map(|id| id.into_static())
        .is_some_and(|id| app.is_playlist_track_table_active_for(&id));
      FoldAnswer::Bool(active)
    }
    FoldQuery::PlaylistRefreshLive { generation } => {
      FoldAnswer::Bool(app.playlist_refresh_generation == generation)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::StreamingPlayerHandle => FoldAnswer::StreamingPlayer(app.streaming_player.clone()),
    FoldQuery::IoTx => FoldAnswer::IoTx(app.io_tx_clone()),
    FoldQuery::PublishPlaylistRefresh {
      generation,
      playlists,
      original_selection,
      pagination_error,
      folder_nodes,
    } => {
      publish_playlist_refresh(
        app,
        generation,
        playlists,
        original_selection,
        pagination_error,
        folder_nodes,
      );
      FoldAnswer::Bool(true)
    }
    FoldQuery::RegisterPlaylistRefresh {
      first_page,
      first_items,
    } => {
      let original_selection = PlaylistSelection {
        playlist_id: app.get_selected_playlist_id(),
        folder_id: app.current_playlist_folder_id,
        selected_index: None,
      };
      let had_previous_complete = !app.all_playlists.is_empty();
      // Snapshot the existing complete list only when background pagination
      // can fail partway and there is something worth restoring.
      let previous_complete = if had_previous_complete {
        PlaylistRefreshPrevious {
          playlists: app.all_playlists.clone(),
          folder_nodes: app._playlist_folder_nodes.clone(),
          folder_items: app.playlist_folder_items.clone(),
        }
      } else {
        PlaylistRefreshPrevious::default()
      };
      app.playlist_refresh_generation = app.playlist_refresh_generation.wrapping_add(1);
      app.playlists = Some(first_page);
      let selected_is_in_first_page =
        original_selection
          .playlist_id
          .as_ref()
          .is_some_and(|selected| {
            first_items
              .iter()
              .any(|p| p.id.as_deref() == Some(selected.as_str()))
          });
      if !had_previous_complete
        || original_selection.playlist_id.is_none()
        || selected_is_in_first_page
      {
        app.all_playlists = first_items.clone();
        app
          .plugin_data_generations
          .bump(crate::core::app::PluginDataKind::Playlists);
        app.playlist_folder_items = build_flat_playlist_items(&first_items);
        reconcile_playlist_selection(
          app,
          original_selection.playlist_id.as_deref(),
          original_selection.folder_id,
          original_selection.selected_index,
        );
      }
      let page_one_selection = PlaylistSelection {
        playlist_id: app.get_selected_playlist_id(),
        folder_id: app.current_playlist_folder_id,
        selected_index: None,
      };
      FoldAnswer::PlaylistRefreshRegistered {
        generation: app.playlist_refresh_generation,
        original_selection,
        page_one_selection,
        previous_complete,
      }
    }
    // Ticket 17: command-surface reads of published state (CLI, DJ tools).
    // The fold answers under its own lock; the caller never locks App.
    FoldQuery::PlaybackContext => {
      FoldAnswer::Playback(app.current_playback_context.clone().map(Box::new))
    }
    FoldQuery::Devices => FoldAnswer::Devices(app.devices.as_ref().map(DeviceList::from)),
    FoldQuery::Playlists => FoldAnswer::Playlists(app.playlists.clone()),
    FoldQuery::TableTracks => FoldAnswer::Tracks(app.track_table.tracks.clone()),
    FoldQuery::SearchResults => FoldAnswer::Search(Box::new(app.search_results.clone())),
    FoldQuery::LastError => FoldAnswer::StatusText(app.api_error.clone()),
    FoldQuery::MediaSnapshot => {
      FoldAnswer::Media(crate::infra::media_metadata::current_playback_snapshot(app))
    }
    FoldQuery::QueueDepth => FoldAnswer::U64(app.native_queue.len() as u64),
    FoldQuery::QueueTracks => FoldAnswer::QueueTracks(app.native_queue.clone()),
    #[cfg(feature = "dj-core")]
    FoldQuery::DjGeneration => FoldAnswer::U64(app.dj.generation),
    FoldQuery::AnnouncementSources { seen_ids } => FoldAnswer::AnnouncementSources {
      enabled: app.user_config.behavior.enable_announcements,
      feed_url: app.user_config.behavior.announcement_feed_url.clone(),
      seen_ids: seen_ids.then(|| app.runtime_state.seen_announcement_ids.clone()),
    },
    FoldQuery::PartyRelayUrl => {
      FoldAnswer::StatusText(app.user_config.behavior.relay_server_url.clone())
    }
    FoldQuery::IsPartyHost => FoldAnswer::Bool(
      matches!(&app.party_session, Some(s) if s.role == crate::infra::network::sync::PartyRole::Host),
    ),
    FoldQuery::IsPartyGuest => FoldAnswer::Bool(matches!(
      &app.party_session,
      Some(s) if s.role == crate::infra::network::sync::PartyRole::Guest
    )),
    FoldQuery::HostSyncState => {
      let sync = match (&app.party_session, &app.current_playback_context) {
        (Some(s), Some(ctx)) if s.role == crate::infra::network::sync::PartyRole::Host => {
          let uri = match &ctx.item {
            Some(rspotify::model::PlayableItem::Track(t)) => {
              t.id.as_ref().map(|id| id.uri()).unwrap_or_default()
            }
            Some(rspotify::model::PlayableItem::Episode(e)) => e.id.uri(),
            Some(_) | None => return FoldAnswer::HostSync(None),
          };
          Some(HostSyncState {
            track_uri: uri,
            position_ms: app.song_progress_ms as u64,
            is_playing: ctx.is_playing,
          })
        }
        _ => None,
      };
      FoldAnswer::HostSync(sync)
    }
    FoldQuery::GuestSyncCurrent => {
      let uri = match &app.current_playback_context {
        Some(ctx) => match &ctx.item {
          Some(rspotify::model::PlayableItem::Track(t)) => {
            t.id.as_ref().map(|id| id.uri()).unwrap_or_default()
          }
          Some(rspotify::model::PlayableItem::Episode(e)) => e.id.uri(),
          Some(_) | None => String::new(),
        },
        None => String::new(),
      };
      let playing = app
        .current_playback_context
        .as_ref()
        .map(|c| c.is_playing)
        .unwrap_or(false);
      let progress = app.song_progress_ms as u64;
      FoldAnswer::GuestSyncCurrent(uri, playing, progress)
    }
    FoldQuery::ClaimLyricsFetch { identity } => {
      // The lyrics fetch's claim (ticket 17): atomic identity re-check plus
      // loading flip — the old two-block inline section. A superseded
      // request (the track already changed again) never shows a spinner.
      if app.desired_lyrics_identity.as_ref() == Some(&identity) {
        app.lyrics_status = crate::core::app::LyricsStatus::Loading;
        app.lyrics = None;
        app.lyrics_synced = false;
        FoldAnswer::Bool(true)
      } else {
        FoldAnswer::Bool(false)
      }
    }
    #[cfg(feature = "dj-core")]
    FoldQuery::DjVibe => FoldAnswer::DjVibe {
      vibe: app.dj.vibe.clone(),
      auto_queue: app.dj.auto_queue,
    },
    FoldQuery::FlushStateSave => {
      app.flush_state_save(true);
      FoldAnswer::Bool(true)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::TrackTableIsSavedTracks => FoldAnswer::Bool(matches!(
      app.track_table.context,
      Some(crate::core::app::TrackTableContext::SavedTracks)
    )),
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeShuffleInstall {
      install,
      park,
      arm_watchdog,
      mark_playing,
    } => crate::infra::network::native_shuffle::claim_native_shuffle_install(
      app,
      install,
      park,
      arm_watchdog,
      mark_playing,
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeShuffleToggle { on } => {
      crate::infra::network::native_shuffle::claim_native_shuffle_toggle(app, on)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeShuffleLapReshuffle => {
      crate::infra::network::native_shuffle::claim_native_shuffle_lap_reshuffle(app)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeShuffleResume {
      resume_index,
      generation,
    } => crate::infra::network::native_shuffle::claim_native_shuffle_resume(
      app,
      resume_index,
      generation,
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::FoldNativeShuffleFullFetch { generation, result } => {
      FoldAnswer::NativeShuffleReload(
        crate::infra::network::native_shuffle::apply_native_shuffle_full_fetch(
          app, generation, result,
        )
        .map(Box::new),
      )
    }
    #[cfg(feature = "dj-core")]
    FoldQuery::DjSkipKeys => FoldAnswer::Strings(app.dj_skip_keys().into_iter().collect()),
    #[cfg(feature = "dj-core")]
    FoldQuery::ClaimDjLibraryIndex => {
      // Idempotent by way of `library_indexing`: the in-TUI toggle, the first
      // turn, and an MCP `search_tracks` can all ask for this, and a second
      // crawl would be pure cost.
      let claimed = !(app.dj.library_indexing || app.dj.library.is_some());
      if claimed {
        app.dj.library_indexing = true;
      }
      FoldAnswer::Bool(claimed)
    }
    #[cfg(feature = "dj-core")]
    FoldQuery::DjLibraryIndexPending => {
      FoldAnswer::Bool(app.dj.library.is_some() || app.dj.library_indexing)
    }
    #[cfg(feature = "dj-core")]
    FoldQuery::DjLibrary => FoldAnswer::DjLibrary(app.dj.library.clone().map(Box::new)),
    #[cfg(feature = "dj-core")]
    FoldQuery::ApplyDjQueueTracks { tracks } => {
      use crate::core::action::ActionOutcome;
      match app.apply(crate::core::action::EngineAction::QueueTracks(tracks)) {
        ActionOutcome::Queued { accepted } => FoldAnswer::U64(accepted as u64),
        ActionOutcome::Applied => FoldAnswer::U64(0),
      }
    }
    #[cfg(feature = "ai-dj")]
    FoldQuery::DjBrain => FoldAnswer::DjBrain(
      crate::infra::dj::session::build_brain(&app.user_config.behavior)
        .map(Box::new)
        .map_err(|e| format!("{e}")),
    ),
    #[cfg(feature = "ai-dj")]
    FoldQuery::ClaimDjTopUp {
      generation,
      turn_seq,
    } => {
      // Cheap early bail before spending a brain call on a stale request, and
      // the `finish_turn` in that bail is load-bearing: the dispatcher already
      // set `thinking`, and `wants_top_up` gates on it.
      let stale = app.dj.generation != generation || !app.dj.auto_queue;
      if stale {
        log::debug!("DJ: dropping a stale top-up (generation {generation})");
        app.dj.finish_turn(turn_seq);
      }
      FoldAnswer::Bool(stale)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativeCanClaimPlayerCommand => FoldAnswer::Bool(
      app
        .streaming_player
        .as_ref()
        .is_some_and(|p| p.is_available())
        || app.native_backend_pending
        || app.native_activation_pending,
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeReconnectingStatus {
      is_no_active_device,
    } => {
      // Expected noise while a native backend materializes: surface a status
      // message instead of routing to the full-screen Error. The check and
      // the notice stay one step.
      if !is_no_active_device || !app.native_backend_pending {
        FoldAnswer::Bool(false)
      } else {
        app.set_status_message("Reconnecting native streaming…", 5);
        FoldAnswer::Bool(true)
      }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeSettlingStatus {
      is_restriction_violated,
    } => {
      // De-escalate a `403 Restriction violated` while the native session is
      // still settling; a steady-state restriction stays fatal.
      if !is_restriction_violated || !(app.native_backend_pending || app.native_activation_pending)
      {
        FoldAnswer::Bool(false)
      } else {
        app.set_status_message("Native streaming is still settling…", 5);
        FoldAnswer::Bool(true)
      }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::IsNativeStreamingActiveForPlayback => FoldAnswer::Bool(
      crate::infra::network::playback::fold_is_native_streaming_active_for_playback(app),
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::SymmetricPlaybackBackend => FoldAnswer::StreamingPlayer(
      crate::infra::network::playback::fold_symmetric_playback_backend(app),
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::StartPlaybackBackend { saved_device_id } => FoldAnswer::StreamingPlayer(
      crate::infra::network::playback::fold_start_playback_backend(app, saved_device_id.as_deref()),
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::TransferPlaybackBackend { device_id } => FoldAnswer::StreamingPlayer(
      crate::infra::network::playback::fold_transfer_playback_backend(app, &device_id),
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeRecoveryIfDisconnected => {
      FoldAnswer::Bool(app.request_native_streaming_recovery_if_disconnected(true))
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlaybackOriginFallback => {
      let origin = app.native_playback_origin.unwrap_or_else(|| {
        if app
          .current_playback_context
          .as_ref()
          .and_then(|ctx| ctx.context.as_ref())
          .is_some()
        {
          crate::core::app::NativePlaybackOrigin::Context
        } else {
          crate::core::app::NativePlaybackOrigin::RawList
        }
      });
      FoldAnswer::NativeOrigin(origin)
    }
    FoldQuery::SavedTracksViewInfo => FoldAnswer::Bool(
      app.track_table.context == Some(crate::core::app::TrackTableContext::SavedTracks),
    ),
    FoldQuery::SavedTracksGeneration => FoldAnswer::U64(app.saved_tracks_prefetch_generation),
    FoldQuery::SpotifyConnected => FoldAnswer::Bool(app.spotify_connected),
    FoldQuery::ReleasePlaylistTracksPage {
      playlist_id,
      offset,
    } => {
      app.playlist_tracks_prefetch_in_flight.remove(&offset);
      if app.pending_playlist_open.as_deref() == Some(playlist_id.as_str()) {
        app.pending_playlist_open = None;
      }
      FoldAnswer::Bool(true)
    }
    FoldQuery::ClearPlaylistTrackFilter => {
      app.clear_playlist_track_filter();
      FoldAnswer::Bool(true)
    }
    FoldQuery::PlaylistTrackSearchApplied {
      playlist_id,
      query,
      matches,
    } => {
      let applied = rspotify::model::idtypes::PlaylistId::from_id(playlist_id.as_str())
        .ok()
        .map(|id| id.into_static())
        .is_some_and(|id| app.apply_playlist_track_search_results(&id, query.clone(), matches));
      if !applied {
        app.pending_playlist_track_search = None;
      }
      FoldAnswer::Bool(applied)
    }
    FoldQuery::ClearPendingPlaylistTrackSearch => {
      app.pending_playlist_track_search = None;
      FoldAnswer::Bool(true)
    }
    FoldQuery::PlaylistTrackAdded { playlist_id } => {
      let playlist_name = app
        .all_playlists
        .iter()
        .find(|playlist| playlist.id.as_deref() == Some(playlist_id.as_str()))
        .map(|playlist| playlist.name.clone());

      if rspotify::model::idtypes::PlaylistId::from_id(playlist_id.as_str())
        .ok()
        .map(|id| id.into_static())
        .is_some_and(|id| app.is_current_route_playlist_track_table_for(&id))
      {
        let playlist_offset = app.playlist_offset;
        app.dispatch(IoEvent::GetPlaylistItems(playlist_id, playlist_offset));
      }

      FoldAnswer::StatusText(
        playlist_name
          .map(|name| format!("Added to {name}"))
          .unwrap_or_else(|| "Added to playlist".to_string()),
      )
    }
    FoldQuery::OpenSavedTracksView => {
      // The liked-songs view re-opened: the reset is the fold's business
      // (same writes as the SavedTracksReset event reducer).
      crate::core::apply_catalog_state(
        app,
        &crate::core::catalog::CatalogStateEvent::SavedTracksReset,
      );
      FoldAnswer::Bool(true)
    }
    FoldQuery::MergeLikedState { ids, is_saved } => {
      for (i, id) in ids.iter().enumerate() {
        match is_saved.get(i) {
          Some(true) => {
            app.liked_song_ids_set.insert(id.clone());
          }
          Some(false) => {
            app.liked_song_ids_set.remove(id);
          }
          None => {}
        }
      }
      FoldAnswer::Bool(true)
    }
    FoldQuery::ClaimLikedLookupBatch => {
      if app.liked_lookup_pending.is_empty() {
        app.liked_lookup_worker_running = false;
        return FoldAnswer::LikedBatch(None);
      }
      let batch: Vec<String> = app.liked_lookup_pending.iter().take(40).cloned().collect();
      for id in &batch {
        app.liked_lookup_pending.remove(id);
      }
      FoldAnswer::LikedBatch(Some((batch, app.liked_state_epoch)))
    }
    FoldQuery::ApplyLikedLookup {
      epoch,
      batch,
      is_saved,
    } => {
      if app.liked_state_epoch != epoch {
        // A local like/unlike landed while this read was in flight; the
        // response may predate it, so re-read instead of applying it.
        app.liked_lookup_pending.extend(batch);
      } else {
        for (i, id) in batch.into_iter().enumerate() {
          match is_saved.get(i) {
            Some(true) => {
              app.liked_song_ids_set.insert(id);
            }
            Some(false) => {
              app.liked_song_ids_set.remove(&id);
            }
            None => {}
          }
        }
      }
      FoldAnswer::Bool(true)
    }
    FoldQuery::RependLikedLookup { batch } => {
      app.liked_lookup_pending.extend(batch);
      FoldAnswer::Bool(true)
    }
    FoldQuery::StopLikedLookupWorker => {
      app.liked_lookup_worker_running = false;
      FoldAnswer::Bool(true)
    }
    FoldQuery::ReleasePlaylistSortFetch { playlist_id } => {
      app.playlist_sort_fetch_in_flight.remove(&playlist_id);
      FoldAnswer::Bool(true)
    }
    FoldQuery::ClaimPlaylistSortFetch { playlist_id } => {
      FoldAnswer::Bool(app.playlist_sort_fetch_in_flight.insert(playlist_id))
    }
    FoldQuery::EnqueueLikedLookup { ids } => {
      for id in &ids {
        app.liked_lookup_pending.insert(id.clone());
      }
      if app.liked_lookup_pending.is_empty() || app.liked_lookup_worker_running {
        FoldAnswer::Bool(false)
      } else {
        app.liked_lookup_worker_running = true;
        FoldAnswer::Bool(true)
      }
    }
    FoldQuery::ApplySortedPlaylistTracks {
      playlist_id,
      tracks,
    } => {
      app.playlist_sort_fetch_in_flight.remove(&playlist_id);
      use crate::core::sort::Sorter;
      let sort_state = app.playlist_sort;
      let sorter = Sorter::new(sort_state);
      let mut tracks = tracks;
      sorter.sort_tracks(&mut tracks);
      let applied = rspotify::model::idtypes::PlaylistId::from_id(playlist_id.as_str())
        .ok()
        .map(|id| id.into_static())
        .is_some_and(|id| app.apply_sorted_playlist_tracks_if_current(&id, tracks));
      FoldAnswer::Bool(applied)
    }
    FoldQuery::ClaimPlaylistTracksOffset {
      playlist_id,
      offset,
    } => {
      let claimed = rspotify::model::idtypes::PlaylistId::from_id(playlist_id.as_str())
        .ok()
        .map(|id| id.into_static())
        .is_some_and(|id| {
          // The old saved-tracks generation check has no playlist-tracks
          // counterpart (no generation exists there); the claim is
          // table-active + not already in flight.
          app.is_playlist_track_table_active_for(&id)
        })
        && !app.playlist_tracks_prefetch_in_flight.contains(&offset)
        && app.playlist_tracks_prefetch_in_flight.insert(offset);
      FoldAnswer::Bool(claimed)
    }
    FoldQuery::ReleasePlaylistTracksOffset {
      playlist_id: _,
      offset,
    } => {
      app.playlist_tracks_prefetch_in_flight.remove(&offset);
      FoldAnswer::Bool(true)
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::core::app::App;
  use std::sync::Arc;
  use tokio::sync::Mutex;

  /// A query sent while the fold is parked on an empty event stream must
  /// still be answered (ticket 17 regression): CLI mode, the DJ turn loop,
  /// and the MCP tools all block on queries from quiet surfaces. Before the
  /// fold's select watched the query channels, this test hung.
  #[tokio::test]
  async fn a_parked_fold_still_answers_queries() {
    let app = Arc::new(Mutex::new(App::default()));
    let (spine, _rx) = spawn_fold(&app).await;
    // Let the fold task run to its park point (no events flowing).
    for _ in 0..10 {
      tokio::task::yield_now().await;
    }
    let answered = tokio::time::timeout(
      std::time::Duration::from_secs(2),
      spine.query(crate::runtime::engine::FoldQuery::QueueDepth),
    )
    .await
    .expect("the parked fold must answer the query instead of hanging");
    assert!(matches!(
      answered,
      Ok(crate::runtime::engine::FoldAnswer::U64(0))
    ));
  }
}
