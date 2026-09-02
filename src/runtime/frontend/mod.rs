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
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{watch, Mutex};

pub use crate::core::action::{ActionOutcome, EngineAction, LibraryTarget, OpenTarget};
pub use crate::core::onboarding::Onboarding;
pub use crate::core::plugin_api::{
  AlbumInfo, ArtistInfo, ArtistRef, PlaybackState, PlaylistInfo, TrackInfo,
};
pub use crate::core::source::Source;

/// Library data made visible to embedded frontends. `None` means the
/// corresponding Spotify request has not produced a page yet; `Some(empty)`
/// is a successfully fetched empty list.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LibrarySnapshot {
  pub liked_songs: Option<Arc<[TrackInfo]>>,
  pub recently_played: Option<Arc<[TrackInfo]>>,
  pub playlists: Option<Arc<[PlaylistInfo]>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchDetail {
  Artist {
    tracks: Arc<[TrackInfo]>,
    albums: Arc<[AlbumInfo]>,
  },
  Album {
    tracks: Arc<[TrackInfo]>,
  },
  Playlist {
    tracks: Arc<[TrackInfo]>,
  },
}

/// One published runtime state. Composed from the existing snapshot builders;
/// equality-deduplicated before publication so a quiet tick publishes nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
  /// Current playback state from `plugin_api::playback_state`.
  pub playback: Option<PlaybackState>,
  /// Whether the active Playback Session is produced by this process's native
  /// streaming backend rather than a remote Playback Device.
  pub native_playback: bool,
  /// The user's Spotify library data fetched so far.
  pub library: LibrarySnapshot,
  /// Catalog request intent, folded with the action that initiated it.
  pub search_query: Option<String>,
  pub library_target: Option<LibraryTarget>,
  /// Authoritative position at publish time (`None` while stale).
  pub position_ms: Option<u64>,
  /// When this snapshot was folded from engine state; a frontend projects
  /// position forward from `position_ms` using this timestamp.
  pub as_of: std::time::Instant,
  /// The native queue's upcoming items, in order.
  pub queue_upcoming: Vec<TrackInfo>,
  /// Rows of the latest catalog search, grouped by Spotify content type.
  pub search_tracks: Arc<[TrackInfo]>,
  pub search_artists: Arc<[ArtistInfo]>,
  pub search_albums: Arc<[AlbumInfo]>,
  pub search_playlists: Arc<[PlaylistInfo]>,
  pub search_detail: Option<SearchDetail>,
  /// Monotonic revisions for accepted catalog completions. These are engine
  /// facts; adapters only translate them and never compare or cache rows.
  pub search_revision: u64,
  pub detail_revision: u64,
  pub library_revision: u64,
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

use super::engine::{EngineCommand, EngineEvent, Spine};

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
use crate::runtime::engine::{DeviceList, FoldAck, PlaylistRefreshPrevious};

pub use super::engine::EngineCommand as FrontendEngineCommand;

/// An embedded Spotatui runtime: shared service startup, an event-spine-fed
/// snapshot publisher, and the quit sequence. The engine emits
/// [`EngineEvent`]s through its [`Spine`]; the publisher folds them into
/// deduplicated immutable snapshots (ADR 0016 stage 2).
pub struct Runtime {
  rt: tokio::runtime::Runtime,
  app: Arc<Mutex<App>>,
  snapshot_tx: watch::Sender<Snapshot>,
  shutdown_tx: watch::Sender<bool>,
  spine: Spine,
  history_collector: Option<crate::infra::history::HistoryCollectorHandle>,
}

impl Runtime {
  /// Boot the runtime synchronously. Runs the full shared bootstrap
  /// (config, state restore, Spotify authentication) plus every shared
  /// service, then starts the snapshot publisher on an owned
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
    let (snapshot_tx, snapshot_rx) = watch::channel(Snapshot::default());
    let (shutdown_tx, _) = watch::channel(false);
    // The event spine is opened by `start_services` so every producer shares
    // one channel; this side receives events, the fold reduces them.
    let events_rx = services.spine_rx;
    // The fold also answers producer queries (ticket 16): claim/release,
    // staleness checks, and the small read set detached workers need.
    let folds = rt.block_on(services.spine_queries.take());
    let (deadline_tx, deadline_rx) = watch::channel(0u64);
    // The frontend's own spine handle for command-side pokes: on streaming
    // legs the command path reuses the shared producer spine so ordering with
    // player events is preserved.
    let command_spine = services.spine.clone();
    let spine = command_spine.clone();
    let fold_spine = command_spine.clone();
    // The fold task: consumes typed spine events, reduces them into App
    // (single writer of the streaming mirrors), rebuilds once, and
    // equality-dedups the publication.
    {
      let app = Arc::clone(&app);
      let tx = snapshot_tx.clone();
      let deadline_tx = deadline_tx.clone();
      rt.spawn(async move {
        fold_loop(app, tx, events_rx, folds, deadline_tx, fold_spine).await;
      });
    }
    // Deadlines are folded as named timer facts. The private change watch
    // wakes the scheduler whenever a fold arms/replaces a deadline; it never
    // wakes on a periodic cadence.
    {
      let app = Arc::clone(&app);
      let spine = command_spine.clone();
      let shutdown_rx = shutdown_tx.subscribe();
      rt.spawn(deadline_scheduler(
        app,
        spine,
        snapshot_rx,
        deadline_rx,
        Some(shutdown_rx),
      ));
    }

    Ok(Runtime {
      rt,
      app,
      snapshot_tx,
      shutdown_tx,
      spine,
      history_collector: Some(services.history_collector),
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
      self
        .spine
        .emit_acked(EngineEvent::Session(
          crate::runtime::engine::SessionFact::ApplyAction(action),
        ))
        .await
        .and_then(|ack| ack.action_outcome)
        .unwrap_or(ActionOutcome::Applied)
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
      {
        let mut app = self.app.lock().await;
        app.flush_persisted_session_force();
      }
      let _ = self
        .spine
        .query(crate::runtime::engine::FoldQuery::FlushStateSave)
        .await;
      #[cfg(feature = "streaming")]
      super::startup::pause_native_playback_before_exit(&self.app).await;
      if let Some(history) = self.history_collector {
        history.shutdown().await;
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

/// Return the nearest armed deadline. Keeping this decision pure over the
/// current App state gives the scheduler one small, deterministic seam while
/// the actual timer task stays private to the Playback Engine.
fn next_deadline(app: &App) -> Option<(Instant, crate::runtime::engine::TimerFact)> {
  let mut deadlines = vec![
    app
      .status_message_expires_at
      .map(|deadline| (deadline, crate::runtime::engine::TimerFact::NoticeExpired)),
    app
      .api_error_expires_at
      .map(|deadline| (deadline, crate::runtime::engine::TimerFact::ApiErrorExpired)),
    app
      .playback_resync_deadline()
      .map(|deadline| (deadline, crate::runtime::engine::TimerFact::PlaybackResync)),
    app.playback_maintenance_deadline().map(|deadline| {
      (
        deadline,
        crate::runtime::engine::TimerFact::PlaybackMaintenance,
      )
    }),
    app.pending_seek_deadline().map(|deadline| {
      (
        deadline,
        crate::runtime::engine::TimerFact::SeekThrottleElapsed,
      )
    }),
    app.state_save_due.map(|deadline| {
      (
        deadline,
        crate::runtime::engine::TimerFact::RuntimeStateSaveDue,
      )
    }),
    app.persisted_session_deadline().map(|deadline| {
      (
        deadline,
        crate::runtime::engine::TimerFact::PlaybackSessionPersistDue,
      )
    }),
  ];
  if app.auth_state.connected && !app.auth_state.refresh_in_progress {
    if let Some(expiry) = app.auth_state.token_expiry {
      deadlines.push(Some((
        crate::core::auth::token_refresh_deadline(expiry, SystemTime::now(), Instant::now()),
        crate::runtime::engine::TimerFact::AuthRefreshDue,
      )));
    }
  }
  #[cfg(feature = "streaming")]
  if let Some(deadline) = app.native_load_watchdog_deadline() {
    deadlines.push(Some((
      deadline,
      crate::runtime::engine::TimerFact::NativeLoadWatchdog,
    )));
  }
  deadlines
    .into_iter()
    .flatten()
    .min_by_key(|(deadline, _)| *deadline)
}

/// Sleep until the nearest armed deadline and emit its named fact. Fold-side
/// deadline changes and snapshot changes wake the task so a newly armed or
/// replaced deadline is re-evaluated immediately; with no deadline, it parks
/// on the watches indefinitely.
async fn deadline_scheduler(
  app: Arc<Mutex<App>>,
  spine: Spine,
  mut snapshots: watch::Receiver<Snapshot>,
  mut deadline_changes: watch::Receiver<u64>,
  mut shutdown_rx: Option<watch::Receiver<bool>>,
) {
  loop {
    let next = {
      let guard = app.lock().await;
      next_deadline(&guard)
    };
    match next {
      Some((deadline, fact)) => {
        let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(sleep);
        tokio::select! {
          _ = &mut sleep => spine.emit(EngineEvent::Timer(fact)).await,
          changed = snapshots.changed() => {
            if changed.is_err() { break; }
          }
          changed = deadline_changes.changed() => {
            if changed.is_err() { break; }
          }
          changed = shutdown_changed(&mut shutdown_rx) => if changed { break },
        }
      }
      None => {
        tokio::select! {
          changed = snapshots.changed() => {
            if changed.is_err() { break; }
          }
          changed = deadline_changes.changed() => {
            if changed.is_err() { break; }
          }
          changed = shutdown_changed(&mut shutdown_rx) => if changed { break },
        }
      }
    }
  }
}

async fn shutdown_changed(shutdown_rx: &mut Option<watch::Receiver<bool>>) -> bool {
  match shutdown_rx {
    Some(rx) => rx.changed().await.is_err() || *rx.borrow(),
    None => std::future::pending().await,
  }
}

impl Default for Snapshot {
  fn default() -> Self {
    Snapshot {
      playback: None,
      native_playback: false,
      library: LibrarySnapshot::default(),
      search_query: None,
      library_target: None,
      position_ms: None,
      as_of: Instant::now(),
      queue_upcoming: Vec::new(),
      search_tracks: Arc::from([]),
      search_artists: Arc::from([]),
      search_albums: Arc::from([]),
      search_playlists: Arc::from([]),
      search_detail: None,
      search_revision: 0,
      detail_revision: 0,
      library_revision: 0,
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
/// domain before the snapshot rebuild. Session facts (ticket 16) replay the exact old handler
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
  let (deadline_tx, deadline_rx) = tokio::sync::watch::channel(0u64);
  let (snapshot_tx, snapshot_rx) = tokio::sync::watch::channel(Snapshot {
    playback: None,
    native_playback: false,
    library: Default::default(),
    search_query: None,
    library_target: None,
    position_ms: None,
    as_of: std::time::Instant::now(),
    queue_upcoming: Vec::new(),
    search_tracks: Arc::from([]),
    search_artists: Arc::from([]),
    search_albums: Arc::from([]),
    search_playlists: Arc::from([]),
    search_detail: None,
    search_revision: 0,
    detail_revision: 0,
    library_revision: 0,
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
    deadline_tx.clone(),
    spine.clone(),
  ));
  tokio::spawn(deadline_scheduler(
    app.clone(),
    spine.clone(),
    snapshot_rx.clone(),
    deadline_rx,
    None,
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
  deadline_tx: watch::Sender<u64>,
  spine: crate::runtime::engine::Spine,
) {
  let mut catalog = CatalogPublication::default();
  let (mut queries, mut acks) = match folds {
    Some(handles) => (Some(handles.queries), Some(handles.acks)),
    None => (None, None),
  };
  publish(&app, &tx, &mut catalog).await;
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
        signal_deadline(&deadline_tx);
      }
    }
    // Apply any acked event the same way: fold it, then complete the
    // responder with the post-fold facts (stream order is queue order, so
    // the answer reflects every event queued before it).
    if let Some(acks) = acks.as_mut() {
      while let Ok((event, responder)) = acks.rx.try_recv() {
        let ack = {
          let mut guard = app.lock().await;
          apply_acked(&mut guard, event)
        };
        let _ = responder.send(ack);
        signal_deadline(&deadline_tx);
        publish(&app, &tx, &mut catalog).await;
        spine.signal_folded();
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
            signal_deadline(&deadline_tx);
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
              apply_acked(&mut guard, event)
            };
            let _ = responder.send(ack);
            signal_deadline(&deadline_tx);
            publish(&app, &tx, &mut catalog).await;
            spine.signal_folded();
          }
          // Ack channel closed: stop watching it.
          None => acks = None,
        }
        continue;
      }
      _ = tx.closed() => break,
    };
    match event {
      EngineEvent::Streaming(streaming) => {
        #[cfg(feature = "streaming")]
        {
          let mut app = app.lock().await;
          app.apply_streaming_state(streaming);
        }
        #[cfg(not(feature = "streaming"))]
        {
          let _ = streaming;
        }
      }
      EngineEvent::Catalog(catalog) => {
        let mut app = app.lock().await;
        crate::core::apply_catalog_state(&mut app, &catalog);
      }
      EngineEvent::Session(fact) => {
        let mut app = app.lock().await;
        apply_session_fact(&mut app, fact);
      }
      EngineEvent::Decoded(fact) => {
        let mut app = app.lock().await;
        apply_decoded_source_fact(&mut app, fact);
      }
      EngineEvent::History(fact) => {
        let mut app = app.lock().await;
        apply_history_fact(&mut app, fact);
      }
      EngineEvent::Timer(fact) => {
        let mut app = app.lock().await;
        apply_timer_fact(&mut app, fact);
      }
    }
    signal_deadline(&deadline_tx);
    publish(&app, &tx, &mut catalog).await;
    spine.signal_folded();
    if tx.is_closed() {
      break;
    }
  }
}

fn signal_deadline(tx: &watch::Sender<u64>) {
  tx.send_modify(|generation| *generation = generation.wrapping_add(1));
}

/// Rebuild the snapshot and equality-dedup the publication.
#[derive(Default)]
struct CatalogPublication {
  search_revision: u64,
  detail_revision: u64,
  library_revision: u64,
}

impl CatalogPublication {
  fn revise(&mut self, mut next: Snapshot, previous: &Snapshot) -> Snapshot {
    // `build_snapshot` receives mutable engine state, so it naturally
    // creates fresh row containers. Keep the accepted immutable allocations
    // when their source data is unchanged; only the changed surface is
    // replaced and receives a new revision.
    if previous.search_tracks == next.search_tracks {
      next.search_tracks = Arc::clone(&previous.search_tracks);
    }
    if previous.search_artists == next.search_artists {
      next.search_artists = Arc::clone(&previous.search_artists);
    }
    if previous.search_albums == next.search_albums {
      next.search_albums = Arc::clone(&previous.search_albums);
    }
    if previous.search_playlists == next.search_playlists {
      next.search_playlists = Arc::clone(&previous.search_playlists);
    }
    if previous.library.liked_songs == next.library.liked_songs {
      next.library.liked_songs = previous.library.liked_songs.as_ref().map(Arc::clone);
    }
    if previous.library.recently_played == next.library.recently_played {
      next.library.recently_played = previous.library.recently_played.as_ref().map(Arc::clone);
    }
    if previous.library.playlists == next.library.playlists {
      next.library.playlists = previous.library.playlists.as_ref().map(Arc::clone);
    }
    if previous.search_detail == next.search_detail {
      next.search_detail = previous.search_detail.clone();
    }
    let search_changed = {
      let old = previous;
      old.search_loading != next.search_loading
        || old.search_tracks != next.search_tracks
        || old.search_artists != next.search_artists
        || old.search_albums != next.search_albums
        || old.search_playlists != next.search_playlists
    };
    if search_changed
      && !next.search_loading
      && (!next.search_tracks.is_empty()
        || !next.search_artists.is_empty()
        || !next.search_albums.is_empty()
        || !next.search_playlists.is_empty())
    {
      self.search_revision = self.search_revision.wrapping_add(1);
    }
    let detail_changed = previous.search_detail != next.search_detail;
    if detail_changed && next.search_detail.is_some() {
      self.detail_revision = self.detail_revision.wrapping_add(1);
    }
    let library_changed = previous.library != next.library;
    if library_changed
      && (next.library.liked_songs.is_some()
        || next.library.recently_played.is_some()
        || next.library.playlists.is_some())
    {
      self.library_revision = self.library_revision.wrapping_add(1);
    }
    next.search_revision = self.search_revision;
    next.detail_revision = self.detail_revision;
    next.library_revision = self.library_revision;
    next
  }
}

async fn publish(
  app: &Arc<Mutex<App>>,
  tx: &watch::Sender<Snapshot>,
  catalog: &mut CatalogPublication,
) {
  let guard = app.lock().await;
  let mut next = {
    let previous = tx.borrow();
    catalog.revise(build_snapshot(&guard), &previous)
  };
  drop(guard);
  tx.send_if_modified(|current| {
    // `as_of` describes an accepted publication, not engine content. Ignore
    // its freshly sampled value while deciding whether content changed.
    let published_at = next.as_of;
    next.as_of = current.as_of;
    if *current == next {
      false
    } else {
      next.as_of = published_at;
      *current = next;
      true
    }
  });
}
fn apply_acked(app: &mut App, event: EngineEvent) -> Option<FoldAck> {
  match &event {
    EngineEvent::Catalog(CatalogStateEvent::SavedTracksPageUpserted { .. }) => {
      let outcome = crate::core::apply_catalog_state(app, event_catalog(&event));
      Some(FoldAck {
        next_saved_tracks_offset: if outcome.stale_event_dropped {
          None
        } else {
          app.next_missing_saved_tracks_offset_continuous()
        },
        next_playlist_tracks_offset: None,
        action_outcome: None,
      })
    }
    EngineEvent::Catalog(CatalogStateEvent::PlaylistTracksPageUpserted { .. }) => {
      let outcome = crate::core::apply_catalog_state(app, event_catalog(&event));
      Some(FoldAck {
        next_saved_tracks_offset: None,
        next_playlist_tracks_offset: if outcome.stale_event_dropped {
          None
        } else {
          app.next_missing_playlist_tracks_offset_continuous()
        },
        action_outcome: None,
      })
    }
    EngineEvent::Session(crate::runtime::engine::SessionFact::ApplyAction(action)) => {
      Some(FoldAck {
        next_saved_tracks_offset: None,
        next_playlist_tracks_offset: None,
        action_outcome: Some(app.apply(action.clone())),
      })
    }
    _ => {
      // Only catalog upserts carry post-fold facts today; other acked
      // events fold normally and answer the default ack.
      match event {
        EngineEvent::Decoded(fact) => {
          apply_decoded_source_fact(app, fact);
        }
        _ => fold_one(app, &event),
      }
      Some(FoldAck {
        next_saved_tracks_offset: None,
        next_playlist_tracks_offset: None,
        action_outcome: None,
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
    EngineEvent::Decoded(_) => {}
    EngineEvent::History(fact) => apply_history_fact(app, fact.clone()),
    EngineEvent::Timer(fact) => apply_timer_fact(app, *fact),
  }
}

fn apply_timer_fact(app: &mut App, fact: crate::runtime::engine::TimerFact) {
  use crate::runtime::engine::TimerFact;
  match fact {
    TimerFact::AuthRefreshDue => {
      if app.auth_state.needs_refresh(SystemTime::now()) {
        app.auth_state.refresh_in_progress = true;
        app.auth_refresh_in_progress = true;
        app.dispatch(IoEvent::RefreshAuthentication);
      }
    }
    TimerFact::NoticeExpired => {
      if app
        .status_message_expires_at
        .is_some_and(|deadline| Instant::now() >= deadline)
      {
        app.dismiss_status_message();
      }
    }
    TimerFact::ApiErrorExpired => app.expire_api_error(),
    #[cfg(feature = "streaming")]
    TimerFact::NativeLoadWatchdog => app.handle_native_load_watchdog(),
    TimerFact::PlaybackResync => app.poll_current_playback(),
    TimerFact::PlaybackMaintenance => app.run_playback_maintenance(),
    TimerFact::SeekThrottleElapsed => {
      #[cfg(feature = "streaming")]
      app.flush_pending_native_seek();
      app.flush_pending_api_seek();
      app.flush_pending_source_seek();
    }
    TimerFact::RuntimeStateSaveDue => app.flush_state_save(false),
    TimerFact::PlaybackSessionPersistDue => app.flush_persisted_session(),
  }
}

fn apply_history_fact(app: &mut App, fact: crate::runtime::engine::HistoryFact) {
  use crate::runtime::engine::HistoryFact;
  match fact {
    HistoryFact::Streaks(streaks) => app.listening_streaks = Some(streaks),
    HistoryFact::RecapReady { path, listens } => {
      if app.get_current_route().active_block == crate::core::app::ActiveBlock::Input {
        app.set_status_message(
          format!(
            "Monthly listening recap ready at {} ({} listens)",
            path.display(),
            listens
          ),
          10,
        );
      } else {
        app.recap_prompt = Some(crate::core::app::RecapPromptState { path, listens });
        app.push_navigation_stack(
          crate::core::app::RouteId::RecapPrompt,
          crate::core::app::ActiveBlock::RecapPrompt,
        );
      }
    }
  }
}

/// Single fold-side writer for decoded-source state.  Keeping this reducer in
/// the frontend fold means source handlers can publish completed work without
/// racing the snapshot publisher.
pub(crate) fn apply_decoded_source_fact(
  app: &mut App,
  fact: crate::runtime::engine::DecodedSourceFact,
) {
  use crate::runtime::engine::DecodedSourceFact;
  match fact {
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalPlaylists(items) => app.local_playlists = items,
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalTracks(items) => {
      app.track_table.tracks = items;
      app.track_table.selected_index = 0;
      app.track_table.context = Some(crate::core::app::TrackTableContext::LocalPlaylist);
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalPlayback(state) => app.local_playback = Some(state),
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalPlaybackCleared => {
      if let Some(local) = app.local_playback.take() {
        local.player.stop();
      }
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalAdvancing(value) => {
      if let Some(local) = app.local_playback.as_mut() {
        local.advancing = value;
      }
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalIndex { target, info } => {
      if let Some(local) = app.local_playback.as_mut() {
        local.index = target;
        local.advancing = false;
        if let Some(info) = info {
          local.name = info.name;
          local.artists = info.artists.join(", ");
          local.album = info.album;
          local.duration_ms = info.duration_ms;
          local.failed_since_played.clear();
        }
      }
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalFailedIndex { target, message } => {
      let exhausted = app.local_playback.as_mut().is_some_and(|local| {
        local.index = target;
        local.failed_since_played.insert(target);
        if local.failed_since_played.len() < local.queue.len() {
          local.advancing = false;
          false
        } else {
          true
        }
      });
      if exhausted {
        if let Some(local) = app.local_playback.take() {
          local.player.stop();
        }
        app.set_status_message(format!("{message} — no playable tracks left, stopping"), 6);
      } else {
        app.set_status_message(message, 6);
      }
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalReplaySucceeded => {
      if let Some(local) = app.local_playback.as_mut() {
        local.advancing = false;
        local.failed_since_played.clear();
      }
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalStatus { message, ttl_secs } => {
      app.set_status_message(message, ttl_secs);
    }
    #[cfg(feature = "local-files")]
    DecodedSourceFact::LocalRepeat(mode) => app.decoded_repeat = mode,
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicPlaylists(items) => app.subsonic_playlists = items,
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicTracks(items) => {
      app.track_table.tracks = items;
      app.track_table.selected_index = 0;
      app.track_table.context = Some(crate::core::app::TrackTableContext::SubsonicPlaylist);
    }
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicPlayback(state) => app.subsonic_playback = Some(state),
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicPlaybackCleared => {
      if let Some(s) = app.subsonic_playback.take() {
        s.player.stop();
      }
    }
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicAdvancing(value) => {
      if let Some(s) = app.subsonic_playback.as_mut() {
        s.advancing = value;
      }
    }
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicIndex {
      target,
      tempfile,
      display,
    } => {
      if let Some(s) = app.subsonic_playback.as_mut() {
        s.index = target;
        s.advancing = false;
        s.tempfile = tempfile;
      }
      if app.subsonic_playback.is_some() {
        app.set_status_message(format!("\u{266a} {display}"), 4);
      }
    }
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicReplaySucceeded => {
      if let Some(s) = app.subsonic_playback.as_mut() {
        s.advancing = false;
      }
    }
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicStatus { message, ttl_secs } => {
      app.set_status_message(message, ttl_secs);
    }
    #[cfg(feature = "subsonic")]
    DecodedSourceFact::SubsonicRepeat(mode) => app.decoded_repeat = mode,
    #[cfg(feature = "internet-radio")]
    DecodedSourceFact::RadioStatus { message, ttl_secs } => {
      app.set_status_message(message, ttl_secs);
    }
    #[cfg(feature = "internet-radio")]
    DecodedSourceFact::RadioStations(items) => app.radio_stations = items,
    #[cfg(feature = "internet-radio")]
    DecodedSourceFact::RadioPlayback(state) => app.radio_playback = Some(state),
    #[cfg(feature = "internet-radio")]
    DecodedSourceFact::RadioPlaybackCleared => {
      if let Some(s) = app.radio_playback.take() {
        s.player.stop();
      }
    }
    #[cfg(feature = "youtube")]
    DecodedSourceFact::YouTubePlaylists(items) => app.youtube_playlists = items,
    #[cfg(feature = "youtube")]
    DecodedSourceFact::YouTubeTracks(items) => {
      app.track_table.tracks = items;
      app.track_table.selected_index = 0;
      app.track_table.context = Some(crate::core::app::TrackTableContext::YouTubePlaylist);
    }
    #[cfg(feature = "youtube")]
    DecodedSourceFact::YouTubePlayback(state) => app.youtube_playback = Some(state),
    #[cfg(feature = "youtube")]
    DecodedSourceFact::YouTubePlaybackCleared => {
      if let Some(s) = app.youtube_playback.take() {
        s.player.stop();
      }
    }
    DecodedSourceFact::Volume(volume) => {
      app.runtime_state.volume_percent = volume;
      app.schedule_state_save(crate::core::state::PersistedRuntimeState::volume_percent(
        volume,
      ));
    }
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
      let _ = changed;
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
      app.auth_state.token_expiry = token_expiry;
      app.auth_state.refresh_in_progress = refresh_in_progress;
      app.auth_state.connected = connected;
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
    SessionFact::RecentlyPlayed { page, navigate } => {
      app.recently_played.result = Some(page);
      app.sort_recently_played_items();
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
    }
    #[cfg(feature = "streaming")]
    SessionFact::PlaybackPolled {
      context: Some(context),
      state,
    } => {
      crate::infra::network::playback::apply_playback_poll(app, *context, *state);
    }
    #[cfg(feature = "streaming")]
    SessionFact::PlaybackPolled { context: None, .. } => {
      app.instant_since_last_current_playback_poll = Instant::now();
      app.is_fetching_current_playback = false;
      app.seek_ms.take();
    }
    #[cfg(not(feature = "streaming"))]
    SessionFact::PlaybackPolled { context } => {
      crate::infra::network::playback::apply_playback_poll(app, context.map(|c| *c));
    }
    SessionFact::PlaybackPollIdle { clear_context } => {
      if clear_context {
        app.current_playback_context = None;
      }
      app.instant_since_last_current_playback_poll = Instant::now();
      app.is_fetching_current_playback = false;
      app.seek_ms.take();
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeIdleDeviceReconciled {
      native_device_id,
      device_name,
      volume,
      activated_at,
    } => {
      if let Some(when) = activated_at {
        app.last_device_activation = Some(when);
      }
      app.mark_native_streaming_device_available(native_device_id, device_name, volume);
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeRestoreApplied {
      generation,
      desired_playing,
      position_ms,
      shuffle,
      repeat,
    } => {
      if app.native_playback_restore_generation() == Some(generation) {
        app.is_streaming_active = true;
        app.native_is_playing = Some(desired_playing);
        app.song_progress_ms = position_ms as u128;
        if let Some(ctx) = &mut app.current_playback_context {
          ctx.is_playing = desired_playing;
          ctx.shuffle_state = shuffle;
          ctx.repeat_state = repeat;
        }
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeRestoreFailed { generation } => {
      if app.native_playback_restore_generation() == Some(generation) {
        app.native_restore_pending = None;
        app.native_load_watchdog = None;
        app.set_status_message("Native playback restore failed; reconnecting again.", 8);
        app.force_native_streaming_recovery(true);
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeRestoreNoState { generation } => {
      if app.native_playback_restore_generation() == Some(generation) {
        app.native_restore_pending = None;
        app.native_load_watchdog = None;
        app.set_status_message(
          "Native connection recovered, but there was no playback state to restore.",
          8,
        );
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeTransferActivated {
      native_device_id,
      activation_time,
    } => {
      app.is_streaming_active = true;
      app.native_activation_pending = true;
      app.native_playback_origin = None;
      app.native_device_id = Some(native_device_id);
      app.current_playback_context = None;
      app.last_device_activation = Some(activation_time);
      app.instant_since_last_current_playback_poll = activation_time - Duration::from_secs(6);
    }
    SessionFact::ExternalTransferCompleted { .. } => {
      app.current_playback_context = None;
      #[cfg(feature = "streaming")]
      {
        app.is_streaming_active = false;
        app.native_playback_origin = None;
        app.clear_native_playback_recovery();
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::AutoSelectApplied {
      native_device_id,
      activation_time,
      external_playback_active,
    } => {
      if external_playback_active {
        crate::infra::network::playback::clear_native_recovery_intent_for_external_playback(app);
        app.set_status_message("Playback remains on another Spotify device.", 8);
      } else {
        app.is_streaming_active = true;
        app.native_activation_pending = false;
        app.last_device_activation = Some(activation_time);
        app.instant_since_last_current_playback_poll = activation_time - Duration::from_secs(6);
      }
      app.native_device_id = Some(native_device_id);
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativePlaybackIntentStopped => app.set_native_playback_intent(false),
    #[cfg(feature = "streaming")]
    SessionFact::ForceNativeRecovery => app.force_native_streaming_recovery(true),
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
    }
    #[cfg(feature = "streaming")]
    SessionFact::ParkStartPlayback {
      context_uri,
      uris,
      offset,
    } => app.park_start_playback(context_uri, uris, offset),
    #[cfg(feature = "streaming")]
    SessionFact::NativePlaybackActivated(activation) => {
      app.is_streaming_active = true;
      app.last_device_activation = Some(activation.activation_time);
      app.native_activation_pending = false;
      app.native_playback_origin = activation.origin;
      app.native_device_id = Some(activation.native_device_id);
      if activation.backdate_poll {
        app.instant_since_last_current_playback_poll =
          activation.activation_time - Duration::from_secs(6);
      }
      if activation.clear_context {
        app.current_playback_context = None;
      }
      if activation.bare_resume {
        app.set_native_playback_intent(true);
      } else if let Some(request) = activation.request {
        let repeat = app
          .current_playback_context
          .as_ref()
          .map_or(rspotify::model::RepeatState::Off, |ctx| ctx.repeat_state);
        app.record_native_playback_request(
          request.context_uri,
          request.uris,
          request.offset,
          request.desired_playing,
          request.shuffle,
          repeat,
        );
      }
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeApiContextStarted {
      context_uri,
      uris,
      offset,
      shuffle_applied,
      desired_shuffle_state,
    } => {
      app.instant_since_last_current_playback_poll = Instant::now() - Duration::from_secs(6);
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.is_playing = true;
        if shuffle_applied {
          ctx.shuffle_state = desired_shuffle_state;
        }
      }
      if shuffle_applied {
        app.runtime_state.shuffle_enabled = desired_shuffle_state;
        app.schedule_state_save(crate::core::state::PersistedRuntimeState::shuffle_enabled(
          desired_shuffle_state,
        ));
      }
      app.park_start_playback(Some(context_uri), uris, offset);
      app.native_load_watchdog = Some(Instant::now());
      app.dispatch(IoEvent::GetCurrentPlayback);
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeResumeApiStarted { native_device_id } => {
      if let Some(id) = native_device_id {
        app.native_device_id = Some(id);
      }
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.is_playing = true;
      }
      app.dispatch(IoEvent::GetCurrentPlayback);
    }
    #[cfg(feature = "streaming")]
    SessionFact::NativeLoadAccepted {
      context_uri,
      uris,
      offset,
      desired_shuffle_state,
      record,
    } => {
      if let Some(request) = record {
        let repeat = app
          .current_playback_context
          .as_ref()
          .map_or(rspotify::model::RepeatState::Off, |ctx| ctx.repeat_state);
        app.record_native_playback_request(
          request.context_uri,
          request.uris,
          request.offset,
          request.desired_playing,
          request.shuffle,
          repeat,
        );
      }
      app.park_start_playback(context_uri, uris, offset);
      app.native_load_watchdog = Some(Instant::now());
      if let Some(ctx) = &mut app.current_playback_context {
        ctx.is_playing = true;
        ctx.shuffle_state = desired_shuffle_state;
      }
      app.runtime_state.shuffle_enabled = desired_shuffle_state;
      app.schedule_state_save(crate::core::state::PersistedRuntimeState::shuffle_enabled(
        desired_shuffle_state,
      ));
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
#[cfg(feature = "streaming")]
fn current_playback_matches_native(
  app: &App,
  player: &crate::infra::player::StreamingPlayer,
) -> bool {
  let Some(ctx) = app.current_playback_context.as_ref() else {
    return app.is_streaming_active;
  };
  if let Some(native_id) = app.native_device_id.as_ref() {
    if ctx.device.id.as_ref() == Some(native_id) {
      return true;
    }
  }
  ctx.device.name.eq_ignore_ascii_case(player.device_name()) && app.has_fresh_native_activity()
}

fn native_playback_active(app: &App) -> bool {
  #[cfg(feature = "streaming")]
  {
    app
      .streaming_player
      .as_ref()
      .is_some_and(|player| current_playback_matches_native(app, player))
  }
  #[cfg(not(feature = "streaming"))]
  {
    let _ = app;
    false
  }
}

pub(crate) fn answer_query(
  app: &mut App,
  query: crate::runtime::engine::FoldQuery,
) -> crate::runtime::engine::FoldAnswer {
  use crate::core::catalog::LibrarySection;
  use crate::runtime::engine::{FoldAnswer, FoldQuery};
  match query {
    FoldQuery::Decoded(query) => answer_decoded_query(app, query),
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
    FoldQuery::HistoryState => {
      FoldAnswer::HistoryState(crate::runtime::engine::HistoryQueryState {
        playback: crate::infra::media_metadata::current_playback_snapshot(app),
        sync_token: app.user_config.behavior.sync_token.clone(),
        enable_monthly_recap_prompt: app.user_config.behavior.enable_monthly_recap_prompt,
      })
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
    FoldQuery::DesiredShuffleState => FoldAnswer::Bool(
      app
        .current_playback_context
        .as_ref()
        .map(|ctx| ctx.shuffle_state)
        .unwrap_or(app.runtime_state.shuffle_enabled),
    ),
    #[cfg(feature = "streaming")]
    FoldQuery::NativeDeviceId => FoldAnswer::NativeDeviceId(app.native_device_id.clone()),
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlaybackActivationDecision(activation) => {
      use crate::infra::network::playback::{
        native_device_preference_update, saved_device_matches_native_player,
      };
      let saved_matches = saved_device_matches_native_player(
        activation.saved_device_id.as_deref(),
        Some(&activation.native_device_id),
        app.devices.as_ref(),
        &activation.native_device_name,
      );
      let recent_activation = app
        .last_device_activation
        .is_some_and(|instant| instant.elapsed() < Duration::from_secs(5));
      let should_transfer = if app.native_activation_pending {
        !recent_activation
      } else {
        !app.is_streaming_active && !recent_activation
      };
      FoldAnswer::NativeActivation(crate::runtime::engine::NativeActivationDecision {
        should_transfer,
        preference: native_device_preference_update(
          activation.saved_device_id.as_deref(),
          activation.explicit_persist,
          saved_matches,
        ),
      })
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativeResumeDirectPossible => {
      FoldAnswer::Bool(app.native_track_info.is_some() || app.last_track_id.is_some())
    }
    #[cfg(feature = "streaming")]
    FoldQuery::StartPlaybackReplayMatches {
      context_uri,
      uris,
      offset,
    } => FoldAnswer::Bool(app.pending_start_playback.as_ref().is_some_and(|pending| {
      pending.recovery_attempts > 0
        && pending.context_uri == context_uri
        && pending.uris == uris
        && pending.offset == offset
    })),
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeBackendPendingPark {
      context_uri,
      uris,
      offset,
    } => {
      if app.native_backend_pending {
        app.park_start_playback(context_uri, uris, offset);
        app.set_status_message("Reconnecting native streaming; playback will resume.", 6);
        FoldAnswer::Bool(true)
      } else {
        FoldAnswer::Bool(false)
      }
    }
    FoldQuery::PlaybackPollState => {
      #[cfg(feature = "streaming")]
      {
        let player = app.streaming_player.clone();
        let local_state = player
          .as_ref()
          .filter(|player| player.is_available())
          .and_then(|player| {
            app.current_playback_context.as_ref().map(|ctx| {
              (
                Some(player.get_volume()),
                ctx.shuffle_state,
                ctx.repeat_state,
                app.native_is_playing,
              )
            })
          });
        FoldAnswer::PlaybackPoll(Box::new(
          crate::infra::network::playback::PlaybackPollState {
            player,
            native_device_id: app.native_device_id.clone(),
            native_streaming_was_active: app.is_streaming_active,
            native_activation_pending: app.native_activation_pending,
            native_track_id: app.last_track_id.clone(),
            native_info_name: app.native_track_info.as_ref().map(|i| i.name.clone()),
            has_fresh_native_activity: app.has_fresh_native_activity(),
            native_is_playing: app.native_is_playing,
            persisted_shuffle: app.runtime_state.shuffle_enabled,
            current_context: app.current_playback_context.clone(),
            local_state,
          },
        ))
      }
      #[cfg(not(feature = "streaming"))]
      {
        FoldAnswer::PlaybackPoll(Box::new(crate::infra::network::playback::PlaybackPollState))
      }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::ClaimNativeRestore { generation } => {
      if app.pending_start_playback.is_some()
        || app.native_playback_restore_generation() != Some(generation)
      {
        return FoldAnswer::NativeRestore(None);
      }
      let Some(player) = app
        .streaming_player
        .as_ref()
        .filter(|player| player.is_connected())
        .cloned()
      else {
        app.force_native_streaming_recovery(true);
        return FoldAnswer::NativeRestore(None);
      };
      let Some(snapshot) = app.begin_native_playback_restore(generation) else {
        return FoldAnswer::NativeRestore(None);
      };
      app.native_load_watchdog = Some(std::time::Instant::now());
      app.set_status_message("Native connection restored; restoring playback.", 6);
      FoldAnswer::NativeRestore(Some(Box::new(
        crate::infra::network::playback::NativeRestoreClaim { player, snapshot },
      )))
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativeIdleDeviceState {
      saved_device_id,
      native_device_id,
      native_device_name,
    } => FoldAnswer::NativeIdleDevice {
      saved_device_matches_native:
        crate::infra::network::playback::saved_device_matches_native_player(
          saved_device_id.as_deref(),
          Some(&native_device_id),
          app.devices.as_ref(),
          &native_device_name,
        ),
    },
    #[cfg(feature = "streaming")]
    FoldQuery::AutoSelectDecision {
      native_device_id,
      native_device_name,
      saved_device_id,
      persist_device_id,
      yield_to_external_playback,
    } => {
      let saved_matches = crate::infra::network::playback::saved_device_matches_native_player(
        saved_device_id.as_deref(),
        Some(&native_device_id),
        app.devices.as_ref(),
        &native_device_name,
      );
      let recent_activation = app
        .last_device_activation
        .is_some_and(|instant| instant.elapsed() < Duration::from_secs(5));
      let external = app.current_playback_context.as_ref().is_some_and(|ctx| {
        crate::infra::network::playback::should_yield_native_recovery(
          yield_to_external_playback,
          ctx.is_playing,
          ctx.device.id.as_deref(),
          &native_device_id,
        )
      });
      FoldAnswer::AutoSelect {
        should_transfer: !app.native_activation_pending
          && !app.is_streaming_active
          && !recent_activation
          && !external,
        external_playback_active: external,
        preference: crate::infra::network::playback::native_device_preference_update(
          saved_device_id.as_deref(),
          persist_device_id,
          saved_matches,
        ),
      }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativeContinuationState { previous_track_id } => FoldAnswer::NativeContinuation {
      transition_advanced: app.native_transition_has_advanced(&previous_track_id),
      raw_next: app.native_raw_list_next_request(&previous_track_id),
      raw_list_exhausted: app.native_raw_list_playback_exhausted(&previous_track_id),
      native_device_id: app.native_device_id.clone(),
    },
    FoldQuery::SavedTracksViewInfo => FoldAnswer::Bool(
      app.track_table.context == Some(crate::core::app::TrackTableContext::SavedTracks),
    ),
    FoldQuery::SavedTracksGeneration => FoldAnswer::U64(app.saved_tracks_prefetch_generation),
    FoldQuery::SpotifyConnected => FoldAnswer::Bool(app.spotify_connected),
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerState { player } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &player));
      let decoded_queue_slot = {
        #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
        {
          app.queue_now_decoded_player().is_some()
        }
        #[cfg(not(any(feature = "local-files", feature = "subsonic", feature = "youtube")))]
        {
          false
        }
      };
      FoldAnswer::NativePlayerState(crate::runtime::engine::NativePlayerState {
        current,
        desired_playing: app.native_playback_recovery.as_ref().map_or_else(
          || app.native_is_playing.unwrap_or(false),
          |snapshot| snapshot.desired_playing,
        ),
        queue_suspended: app.queue_suspended.is_some(),
        decoded_queue_slot,
        queue_now_is_spotify: app.queue_now_is_spotify(),
        volume_percent: app.runtime_state.volume_percent,
      })
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativeRuntimeVolume => {
      FoldAnswer::NativeRuntimeVolume(app.runtime_state.volume_percent)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerPlaying {
      player,
      track_uri,
      position_ms,
    } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      if current {
        let restored = app.observe_native_playback_state(track_uri.clone(), position_ms, true);
        if app.native_restore_pending.is_none() {
          app.native_load_watchdog = None;
        }
        app.pending_start_playback = None;
        app.native_backend_pending = false;
        if restored {
          app.set_status_message("Native playback restored.", 5);
        }
        app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::Playing {
          track_uri,
          position_ms,
        });
      }
      FoldAnswer::NativePlayerPlaying { current }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerPaused {
      player,
      track_uri,
      position_ms,
    } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      if current {
        let restored = app.observe_native_playback_state(track_uri.clone(), position_ms, false);
        if app.native_restore_pending.is_none() {
          app.native_load_watchdog = None;
        }
        if restored {
          app.set_status_message("Native playback restored in paused state.", 5);
        }
        app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::Paused {
          track_uri,
          position_ms,
        });
      }
      FoldAnswer::NativePlayerPaused { current }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerTrackChanged {
      player,
      track_uri,
      info,
      previous_track_id,
    } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      let mut reload_uri = None;
      let mut transition_advanced = false;
      if current {
        if app.native_restore_pending.is_none() {
          app.native_load_watchdog = None;
        }
        app.pending_start_playback = None;
        app.native_backend_pending = false;
        let kind = info.kind;
        app.observe_native_track_changed(track_uri.clone(), kind, info.duration_ms);
        let playing_id = crate::core::app::base62_id_of(&track_uri).to_string();
        app.sync_native_shuffle_index(&playing_id);
        reload_uri = app
          .spotify_queue_guard_reload_uri(&playing_id)
          .map(|uri| (uri, app.queue_slot_desired_playing));
        transition_advanced = previous_track_id
          .as_deref()
          .is_some_and(|id| app.native_transition_has_advanced(id));
        app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::TrackChanged {
          info,
          track_uri,
        });
      }
      FoldAnswer::NativePlayerTrackChanged(crate::runtime::engine::NativePlayerTrackChanged {
        current,
        reload_uri,
        transition_advanced,
      })
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerEndOfTrack {
      player,
      previous_track_id,
    } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      let mut should_ensure_playback = false;
      let mut stop_after_current = false;
      let mut reissue_queue_slot = None;
      if current {
        if app.user_config.behavior.stop_after_current_track {
          stop_after_current = true;
          app.pending_stop_after_track = true;
          app.set_native_playback_intent(false);
        } else if let Some(uri) = app.spotify_queue_guard_reload_uri(&previous_track_id) {
          reissue_queue_slot = Some((uri, app.queue_slot_desired_playing));
        } else if !app.handle_native_spotify_track_end() {
          if app.native_raw_list_playback_exhausted(&previous_track_id) {
            app.set_native_playback_intent(false);
          } else {
            should_ensure_playback = true;
          }
        }
        app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::EndOfTrack {
          reset_progress: true,
          stop_after_current,
        });
      }
      FoldAnswer::NativePlayerEndOfTrack(crate::runtime::engine::NativePlayerEndOfTrack {
        current,
        should_ensure_playback,
        stop_after_current,
        reissue_queue_slot,
      })
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerDisconnect {
      player,
      shared_position,
      shared_is_playing,
      status_message,
      restore_playback,
    } => {
      let Some(current) = app.streaming_player.as_ref() else {
        return FoldAnswer::NativePlayerRecovery(None);
      };
      if !Arc::ptr_eq(current, &player) {
        return FoldAnswer::NativePlayerRecovery(None);
      }
      let reselect_device =
        restore_playback && crate::runtime::frontend::current_playback_matches_native(app, &player);
      let continue_after_track = if reselect_device {
        app.prepare_native_playback_recovery(shared_position, shared_is_playing);
        app
          .native_playback_recovery
          .as_ref()
          .and_then(|s| s.completed_track_uri())
          .map(crate::core::app::base62_id_of)
          .map(str::to_string)
      } else {
        app.clear_native_playback_recovery();
        None
      };
      if !restore_playback {
        app.pending_start_playback = None;
        if app.queue_now_is_spotify() {
          app.queue_now = None;
          app.spotify_queue_guard_reloads = 0;
        }
      }
      app.streaming_player = None;
      app.is_streaming_active = false;
      app.native_activation_pending = false;
      app.native_device_id = None;
      app.native_playback_origin = None;
      app.convert_shuffled_suspension_to_context(None);
      app.clear_native_shuffle_session();
      app.last_device_activation = None;
      app.seek_ms = None;
      app.current_playback_context = None;
      app.set_status_message(status_message, 8);
      app.dispatch(IoEvent::GetCurrentPlayback);
      app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::Teardown);
      FoldAnswer::NativePlayerRecovery(Some(crate::infra::player::StreamingRecoveryRequest {
        reselect_device,
        restore_playback,
        continue_after_track,
      }))
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerInstall { player } => {
      let old = app.streaming_player.replace(player.clone());
      app.native_backend_pending = false;
      FoldAnswer::StreamingPlayer(old)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerRecoveryFailed { error } => {
      app.native_backend_pending = false;
      app.native_restore_pending = None;
      app.native_load_watchdog = None;
      if app.pending_start_playback.take().is_some() {
        app.set_status_message(
          format!("Native recovery failed; playback request dropped: {error}"),
          8,
        );
        FoldAnswer::Bool(true)
      } else {
        app.set_status_message(format!("Native recovery failed: {error}"), 8);
        FoldAnswer::Bool(false)
      }
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerRecoveryRoute {
      player,
      mut request,
      device_name,
    } => {
      if !app
        .streaming_player
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &player))
      {
        return FoldAnswer::Bool(false);
      }
      app.native_backend_pending = false;
      let replay_queue_slot = app.pending_start_playback.is_none()
        && request.restore_playback
        && app.queue_now_is_spotify();
      if request.reselect_device || replay_queue_slot {
        app.dispatch(IoEvent::AutoSelectStreamingDevice(device_name, false, true));
      }
      if app.pending_start_playback.is_some() {
        app.replay_pending_start_playback();
      } else if replay_queue_slot {
        app.dispatch(IoEvent::ReplayPublishedSpotifyQueueSlot);
      } else if let Some(previous) = request.continue_after_track.take() {
        if app.native_transition_has_advanced(&previous) {
          if let Some(generation) = app.native_playback_restore_generation() {
            app.dispatch(IoEvent::RestoreNativePlayback(generation));
          }
        } else {
          app.dispatch(IoEvent::EnsurePlaybackContinues(previous));
        }
      } else if request.reselect_device {
        if let Some(generation) = app.native_playback_restore_generation() {
          app.dispatch(IoEvent::RestoreNativePlayback(generation));
        }
      } else if request.restore_playback {
        app.set_status_message("Native streaming recovered.", 6);
      }
      FoldAnswer::Bool(true)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerUnavailable { player } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      if !current {
        return FoldAnswer::NativePlayerFactApplied(false);
      }
      app.native_load_watchdog = None;
      app.native_restore_pending = None;
      app.pending_start_playback = None;
      app.native_backend_pending = false;
      if let Some(ctx) = app.current_playback_context.as_mut() {
        ctx.is_playing = false;
      }
      app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::Stopped);
      FoldAnswer::NativePlayerFactApplied(true)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerLoading {
      player,
      track_uri,
      position_ms,
    } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      if !current {
        return FoldAnswer::NativePlayerFactApplied(false);
      }
      if app.native_load_watchdog.is_some() {
        app.native_load_watchdog = Some(Instant::now());
      }
      app.apply_streaming_state(crate::core::streaming::StreamingStateEvent::Loading {
        track_uri,
        position_ms,
      });
      FoldAnswer::NativePlayerFactApplied(true)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerReconnect { player, connected } => {
      if !app
        .streaming_player
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &player))
      {
        return FoldAnswer::Bool(false);
      }
      app.native_backend_pending = !connected;
      if connected && app.native_load_watchdog.is_some() {
        app.native_load_watchdog = Some(Instant::now());
      }
      app.set_status_message(
        if connected {
          "Native streaming connection recovered."
        } else {
          "Native streaming connection lost; reconnecting."
        },
        if connected { 5 } else { 6 },
      );
      FoldAnswer::Bool(true)
    }
    #[cfg(feature = "streaming")]
    FoldQuery::NativePlayerFact { player, event } => {
      let current = app
        .streaming_player
        .as_ref()
        .is_some_and(|p| Arc::ptr_eq(p, &player));
      if current {
        app.apply_streaming_state(event);
      }
      FoldAnswer::NativePlayerFactApplied(current)
    }
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

#[allow(unused_variables)]
fn answer_decoded_query(
  app: &mut App,
  query: crate::runtime::engine::DecodedSourceQuery,
) -> crate::runtime::engine::FoldAnswer {
  #[cfg(any(
    feature = "local-files",
    feature = "subsonic",
    feature = "internet-radio",
    feature = "youtube"
  ))]
  use crate::runtime::engine::DecodedSourceAnswer as A;
  use crate::runtime::engine::{DecodedSourceQuery as Q, FoldAnswer};
  match query {
    Q::Volume => FoldAnswer::Decoded(crate::runtime::engine::DecodedSourceAnswer::Volume(
      app.runtime_state.volume_percent,
    )),
    #[cfg(feature = "local-files")]
    Q::LocalConfig => FoldAnswer::Decoded(A::LocalConfig(
      app.user_config.behavior.local_music_path.clone(),
    )),
    #[cfg(feature = "local-files")]
    Q::LocalPlayer => FoldAnswer::Decoded(A::LocalPlayer(
      app
        .local_playback
        .as_ref()
        .map(|s| std::sync::Arc::clone(&s.player)),
    )),
    #[cfg(feature = "local-files")]
    Q::LocalQueue => FoldAnswer::Decoded(A::LocalQueue {
      index: app.local_playback.as_ref().map_or(0, |s| s.index),
      len: app.local_playback.as_ref().map_or(0, |s| s.queue.len()),
      repeat: app.decoded_repeat,
    }),
    #[cfg(feature = "local-files")]
    Q::LocalReplay => {
      FoldAnswer::Decoded(A::LocalReplay(app.local_playback.as_mut().and_then(|s| {
        s.advancing = true;
        s.queue
          .get(s.index)
          .and_then(|uri| crate::infra::local::file_uri_to_path(uri).ok())
          .map(|p| (std::sync::Arc::clone(&s.player), p))
      })))
    }
    #[cfg(feature = "local-files")]
    Q::LocalShuffle => FoldAnswer::Decoded(A::LocalShuffle(app.decoded_shuffle)),
    #[cfg(feature = "local-files")]
    Q::LocalSkip { forward } => FoldAnswer::Decoded(A::LocalSkip(
      app.local_playback.as_mut().and_then(|local| {
        local.advancing = true;
        crate::infra::queue::advance_index(
          local.index,
          local.queue.len(),
          app.decoded_repeat,
          forward,
        )
      }),
    )),
    #[cfg(feature = "local-files")]
    Q::LocalPlayIndex(target) => FoldAnswer::Decoded(A::LocalPlayIndex(
      app.local_playback.as_mut().and_then(|local| {
        local.queue.get(target).map(|uri| {
          local.advancing = true;
          (std::sync::Arc::clone(&local.player), uri.clone())
        })
      }),
    )),
    #[cfg(feature = "local-files")]
    Q::LocalRepeat(state) => {
      FoldAnswer::Decoded(A::LocalRepeat(app.set_decoded_repeat_from_state(state)))
    }
    #[cfg(feature = "subsonic")]
    Q::SubsonicConfig => FoldAnswer::Decoded(A::SubsonicConfig {
      url: app.user_config.behavior.subsonic_url.clone(),
      username: app.user_config.behavior.subsonic_username.clone(),
      password: app.user_config.behavior.subsonic_password.clone(),
    }),
    #[cfg(feature = "subsonic")]
    Q::SubsonicPlayer => FoldAnswer::Decoded(A::SubsonicPlayer(
      app
        .subsonic_playback
        .as_ref()
        .map(|s| std::sync::Arc::clone(&s.player)),
    )),
    #[cfg(feature = "subsonic")]
    Q::SubsonicShuffle => FoldAnswer::Decoded(A::SubsonicShuffle(app.decoded_shuffle)),
    #[cfg(feature = "subsonic")]
    Q::SubsonicTracks(uris) => {
      let search = app
        .search_results
        .tracks
        .as_ref()
        .map(|p| p.items.as_slice());
      FoldAnswer::Decoded(A::SubsonicTracks(
        crate::infra::subsonic::dispatch::snapshot_tracks(&app.track_table.tracks, search, &uris),
      ))
    }
    #[cfg(feature = "subsonic")]
    Q::SubsonicReplay => {
      FoldAnswer::Decoded(A::SubsonicReplay(app.subsonic_playback.as_mut().map(|s| {
        s.advancing = true;
        (
          std::sync::Arc::clone(&s.player),
          s.tempfile.path().to_path_buf(),
        )
      })))
    }
    #[cfg(feature = "subsonic")]
    Q::SubsonicSkip { forward } => FoldAnswer::Decoded(A::SubsonicSkip(
      app.subsonic_playback.as_mut().and_then(|s| {
        s.advancing = true;
        crate::infra::queue::advance_index(s.index, s.tracks.len(), app.decoded_repeat, forward)
      }),
    )),
    #[cfg(feature = "subsonic")]
    Q::SubsonicPlayIndex(target) => {
      let plan = app
        .subsonic_playback
        .as_mut()
        .map(|s| match s.tracks.get(target) {
          None => Ok(None),
          Some(track) => match track
            .uri
            .as_deref()
            .map(crate::infra::subsonic::track_id_from_uri)
          {
            Some(Ok(id)) => {
              s.advancing = true;
              Ok(Some((
                std::sync::Arc::clone(&s.player),
                std::sync::Arc::clone(&s.source),
                id.to_string(),
                track.name.clone(),
              )))
            }
            _ => Err(()),
          },
        });
      FoldAnswer::Decoded(A::SubsonicPlayIndex(plan))
    }
    #[cfg(feature = "subsonic")]
    Q::SubsonicRepeat(state) => {
      FoldAnswer::Decoded(A::SubsonicRepeat(app.set_decoded_repeat_from_state(state)))
    }
    #[cfg(feature = "internet-radio")]
    Q::RadioPlayer => FoldAnswer::Decoded(A::RadioPlayer(
      app
        .radio_playback
        .as_ref()
        .map(|s| std::sync::Arc::clone(&s.player)),
    )),
    #[cfg(feature = "internet-radio")]
    Q::RadioStationsSnapshot(uri) => FoldAnswer::Decoded(A::RadioStationsSnapshot(
      crate::infra::radio::dispatch::snapshot_station(app, &uri),
    )),
    #[cfg(feature = "internet-radio")]
    Q::RadioConfig => FoldAnswer::Decoded(A::RadioConfig {
      configured: app.user_config.behavior.radio_stations.clone(),
      saved: app.runtime_state.radio_stations.clone(),
    }),
    #[cfg(any(
      feature = "local-files",
      feature = "subsonic",
      feature = "internet-radio"
    ))]
    Q::ClaimBackendPlayers { keep } => {
      let answer = A::BackendPlayers {
        #[cfg(feature = "streaming")]
        streaming: app.streaming_player.clone(),
        #[cfg(feature = "local-files")]
        local: app
          .local_playback
          .as_ref()
          .map(|s| std::sync::Arc::clone(&s.player)),
        #[cfg(feature = "subsonic")]
        subsonic: app
          .subsonic_playback
          .as_ref()
          .map(|s| std::sync::Arc::clone(&s.player)),
        #[cfg(feature = "internet-radio")]
        radio: app
          .radio_playback
          .as_ref()
          .map(|s| std::sync::Arc::clone(&s.player)),
        #[cfg(feature = "youtube")]
        youtube: app
          .youtube_playback
          .as_ref()
          .map(|s| std::sync::Arc::clone(&s.player)),
      };
      #[cfg(feature = "local-files")]
      if keep != Some(crate::core::source::Source::Local) {
        app.local_playback.take();
      }
      #[cfg(feature = "subsonic")]
      if keep != Some(crate::core::source::Source::Subsonic) {
        app.subsonic_playback.take();
      }
      #[cfg(feature = "internet-radio")]
      if keep != Some(crate::core::source::Source::Radio) {
        app.radio_playback.take();
      }
      #[cfg(feature = "youtube")]
      if keep != Some(crate::core::source::Source::YouTube) {
        app.youtube_playback.take();
      }
      FoldAnswer::Decoded(answer)
    }
  }
}

fn build_snapshot(app: &App) -> Snapshot {
  let search = crate::core::plugin_api::search_results_snapshot(app);
  let search_detail = match app.get_current_route().id {
    crate::core::app::RouteId::Artist => app.artist.as_ref().map(|artist| SearchDetail::Artist {
      tracks: artist.top_tracks.clone().into(),
      albums: artist.albums.items.clone().into(),
    }),
    crate::core::app::RouteId::AlbumTracks => app
      .selected_album_simplified
      .as_ref()
      .map(|album| SearchDetail::Album {
        tracks: album.tracks.items.clone().into(),
      })
      .or_else(|| {
        app
          .selected_album_full
          .as_ref()
          .map(|album| SearchDetail::Album {
            tracks: album.album.tracks.clone().into(),
          })
      }),
    crate::core::app::RouteId::TrackTable => {
      app
        .playlist_tracks
        .as_ref()
        .map(|page| SearchDetail::Playlist {
          tracks: page
            .items
            .iter()
            .filter_map(|(_, item)| match item {
              crate::core::plugin_api::PlayableInfo::Track(track) => Some(track.clone()),
              crate::core::plugin_api::PlayableInfo::Episode(_) => None,
            })
            .collect::<Vec<_>>()
            .into(),
        })
    }
    _ => None,
  };
  Snapshot {
    as_of: Instant::now(),
    playback: crate::core::plugin_api::playback_state(app),
    native_playback: native_playback_active(app),
    library: LibrarySnapshot {
      liked_songs: (!app.library.saved_tracks.pages.is_empty())
        .then(|| Arc::from(crate::core::plugin_api::saved_tracks_snapshot(app))),
      recently_played: app
        .recently_played
        .result
        .as_ref()
        .map(|_| Arc::from(crate::core::plugin_api::recently_played_snapshot(app))),
      playlists: app
        .playlists
        .as_ref()
        .map(|_| Arc::from(crate::core::plugin_api::playlists_snapshot(app))),
    },
    search_query: app.catalog_query.clone(),
    library_target: app.catalog_library_target,
    position_ms: app.playback_position_ms().map(|p| p as u64),
    queue_upcoming: app.native_queue.clone(),
    search_tracks: search.tracks.into(),
    search_artists: search.artists.into(),
    search_albums: search.albums.into(),
    search_playlists: search.playlists.into(),
    search_detail,
    search_revision: 0,
    detail_revision: 0,
    library_revision: 0,
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

  #[tokio::test]
  async fn fold_publishes_state_that_existed_before_startup() {
    let app = Arc::new(Mutex::new(App::default()));
    app.lock().await.spotify_connected = true;

    let (_spine, mut rx) = spawn_fold(&app).await;
    tokio::time::timeout(std::time::Duration::from_secs(2), rx.changed())
      .await
      .expect("fold should publish its initial App state")
      .expect("snapshot channel should remain open");

    assert!(rx.borrow().spotify_connected);
  }

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

  #[tokio::test]
  async fn action_facts_are_folded_before_snapshot_publication() {
    let app = Arc::new(Mutex::new(App::default()));
    let (spine, mut snapshots) = spawn_fold(&app).await;
    // Consume the initial publication so the next change is unambiguous.
    snapshots.changed().await.unwrap();
    let ack = spine
      .emit_acked(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::ApplyAction(
          crate::core::action::EngineAction::Notify("folded".into(), 5),
        ),
      ))
      .await;
    assert_eq!(
      ack.and_then(|ack| ack.action_outcome),
      Some(crate::core::action::ActionOutcome::Applied)
    );
    tokio::time::timeout(std::time::Duration::from_secs(1), snapshots.changed())
      .await
      .expect("action fact should publish")
      .unwrap();
    assert_eq!(snapshots.borrow().notice.as_deref(), Some("folded"));
  }

  #[tokio::test]
  async fn identical_visible_state_does_not_publish_a_fresh_timestamp() {
    let app = Arc::new(Mutex::new(App::default()));
    let (spine, mut snapshots) = spawn_fold(&app).await;
    snapshots.changed().await.unwrap();
    let fact = || {
      crate::runtime::engine::EngineEvent::Session(crate::runtime::engine::SessionFact::Status {
        message: "same".into(),
        ttl_secs: 5,
        is_error: false,
      })
    };
    spine.emit(fact()).await;
    snapshots.changed().await.unwrap();
    spine.emit(fact()).await;
    assert!(
      tokio::time::timeout(std::time::Duration::from_millis(20), snapshots.changed())
        .await
        .is_err()
    );
  }

  #[tokio::test]
  async fn catalog_request_intent_is_folded_into_the_snapshot() {
    let app = Arc::new(Mutex::new(App::default()));
    let (spine, mut snapshots) = spawn_fold(&app).await;
    snapshots.changed().await.unwrap();
    spine
      .emit_acked(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::ApplyAction(
          crate::core::action::EngineAction::OpenLibrary(
            crate::core::action::LibraryTarget::Playlists,
          ),
        ),
      ))
      .await;
    snapshots.changed().await.unwrap();
    assert_eq!(
      snapshots.borrow().library_target,
      Some(crate::core::action::LibraryTarget::Playlists)
    );
  }

  #[tokio::test]
  async fn activity_observers_wake_after_the_fact_is_folded() {
    let app = Arc::new(Mutex::new(App::default()));
    let (spine, mut snapshots) = spawn_fold(&app).await;
    snapshots.changed().await.unwrap();
    let mut activity = spine.subscribe_activity();
    spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::Status {
          message: "ready".into(),
          ttl_secs: 5,
          is_error: false,
        },
      ))
      .await;
    activity.changed().await.unwrap();
    let state = spine
      .query(crate::runtime::engine::FoldQuery::HistoryState)
      .await
      .unwrap();
    assert!(matches!(
      state,
      crate::runtime::engine::FoldAnswer::HistoryState(_)
    ));
    assert_eq!(app.lock().await.status_message.as_deref(), Some("ready"));
  }

  #[tokio::test]
  async fn idle_fold_does_not_publish_without_an_event() {
    let app = Arc::new(Mutex::new(App::default()));
    let (_spine, mut snapshots) = spawn_fold(&app).await;
    snapshots.changed().await.unwrap();
    assert!(
      tokio::time::timeout(std::time::Duration::from_millis(20), snapshots.changed())
        .await
        .is_err()
    );
  }

  #[test]
  fn next_deadline_returns_the_nearest_named_fact() {
    let mut app = App::default();
    let now = Instant::now();
    app.status_message_expires_at = Some(now + Duration::from_secs(2));
    app.api_error_expires_at = Some(now + Duration::from_secs(1));

    assert_eq!(
      next_deadline(&app).map(|(_, fact)| fact),
      Some(crate::runtime::engine::TimerFact::ApiErrorExpired)
    );

    let mut app = App::default();
    app.auth_state.connected = true;
    app.auth_state.token_expiry = Some(SystemTime::now() + crate::core::auth::TOKEN_REFRESH_MARGIN);
    assert_eq!(
      next_deadline(&app).map(|(_, fact)| fact),
      Some(crate::runtime::engine::TimerFact::AuthRefreshDue)
    );

    let mut app = App::default();
    app.state_save_due = Some(now);
    assert_eq!(
      next_deadline(&app).map(|(_, fact)| fact),
      Some(crate::runtime::engine::TimerFact::RuntimeStateSaveDue)
    );
  }

  #[test]
  fn catalog_rows_reuse_arc_across_non_catalog_publication() {
    let mut publication = CatalogPublication::default();
    let mut first = Snapshot::default();
    first.search_tracks = Arc::from([TrackInfo {
      uri: Some("spotify:track:one".into()),
      name: "one".into(),
      artists: vec![],
      album: String::new(),
      duration_ms: 1,
      id: None,
      album_id: None,
      artist_refs: vec![],
      is_playable: true,
      is_local: false,
      track_number: 0,
      explicit: false,
      image_url: None,
    }]);
    let first = publication.revise(first, &Snapshot::default());
    let first_rows = Arc::clone(&first.search_tracks);
    let mut next = first.clone();
    next.position_ms = Some(1);
    let next = publication.revise(next, &first);
    assert!(Arc::ptr_eq(&first_rows, &next.search_tracks));
  }

  #[test]
  fn next_deadline_arms_playback_resync_and_seek_throttle() {
    let mut app = App::default();
    app.spotify_connected = true;
    app.native_is_playing = Some(true);
    app.instant_since_last_current_playback_poll = Instant::now() - Duration::from_secs(1);
    app.pending_api_seek = Some(1_000);
    app.last_api_seek = Some(Instant::now() - Duration::from_millis(200));

    assert_eq!(
      next_deadline(&app).map(|(_, fact)| fact),
      Some(crate::runtime::engine::TimerFact::PlaybackResync)
    );

    app.instant_since_last_current_playback_poll = Instant::now();
    assert_eq!(
      next_deadline(&app).map(|(_, fact)| fact),
      Some(crate::runtime::engine::TimerFact::SeekThrottleElapsed)
    );
  }

  #[tokio::test]
  async fn scheduler_waits_for_a_deadline_instead_of_heartbeat_waking() {
    let app = Arc::new(Mutex::new(App::default()));
    let (spine, mut events, _) = crate::runtime::engine::Spine::open(Default::default());
    let (_snapshot_tx, snapshot_rx) = watch::channel(Snapshot::default());
    let (_deadline_tx, deadline_rx) = watch::channel(0u64);
    app.lock().await.status_message_expires_at = Some(Instant::now() + Duration::from_millis(40));
    tokio::spawn(deadline_scheduler(
      app,
      spine,
      snapshot_rx,
      deadline_rx,
      None,
    ));

    assert!(
      tokio::time::timeout(Duration::from_millis(10), events.recv())
        .await
        .is_err(),
      "scheduler must remain parked before the nearest deadline"
    );
    assert!(matches!(
      tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("deadline should fire")
        .expect("spine should remain open"),
      crate::runtime::engine::EngineEvent::Timer(crate::runtime::engine::TimerFact::NoticeExpired)
    ));
  }

  #[tokio::test]
  async fn scheduler_emits_auth_refresh_at_the_safety_margin() {
    let app = Arc::new(Mutex::new(App::default()));
    {
      let mut guard = app.lock().await;
      guard.auth_state.connected = true;
      guard.auth_state.token_expiry = Some(
        SystemTime::now() + crate::core::auth::TOKEN_REFRESH_MARGIN + Duration::from_millis(40),
      );
    }
    let (spine, mut events, _) = crate::runtime::engine::Spine::open(Default::default());
    let (_snapshot_tx, snapshot_rx) = watch::channel(Snapshot::default());
    let (_deadline_tx, deadline_rx) = watch::channel(0u64);
    tokio::spawn(deadline_scheduler(
      app,
      spine,
      snapshot_rx,
      deadline_rx,
      None,
    ));

    assert!(
      tokio::time::timeout(Duration::from_millis(20), events.recv())
        .await
        .is_err()
    );
    assert!(matches!(
      tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("auth deadline should fire")
        .expect("spine should remain open"),
      crate::runtime::engine::EngineEvent::Timer(crate::runtime::engine::TimerFact::AuthRefreshDue)
    ));
  }

  #[tokio::test]
  async fn notice_expiry_reaches_the_fold_while_idle() {
    let app = Arc::new(Mutex::new(App::default()));
    {
      let mut guard = app.lock().await;
      guard.status_message = Some("temporary".to_string());
      guard.status_message_expires_at = Some(Instant::now() + Duration::from_millis(40));
    }
    let (_spine, mut snapshots) = spawn_fold(&app).await;
    snapshots
      .changed()
      .await
      .expect("fold should publish initial state");
    assert_eq!(snapshots.borrow().notice.as_deref(), Some("temporary"));

    tokio::time::timeout(Duration::from_secs(1), async {
      loop {
        snapshots.changed().await.expect("fold should remain open");
        if snapshots.borrow().notice.is_none() {
          break;
        }
      }
    })
    .await
    .expect("notice should expire without another playback or catalog event");
  }

  #[cfg(feature = "local-files")]
  #[test]
  fn decoded_source_tracks_are_reduced_by_the_fold() {
    let mut app = App::default();
    apply_decoded_source_fact(
      &mut app,
      crate::runtime::engine::DecodedSourceFact::LocalTracks(Vec::new()),
    );
    assert_eq!(
      app.track_table.context,
      Some(crate::core::app::TrackTableContext::LocalPlaylist)
    );
    assert!(app.track_table.tracks.is_empty());
  }

  #[test]
  fn history_streak_and_recap_facts_reduce_without_app_lock() {
    use crate::runtime::engine::HistoryFact;
    let mut app = App::default();
    apply_history_fact(
      &mut app,
      HistoryFact::Streaks(crate::infra::history::StreakSummary {
        current_days: 2,
        longest_days: 3,
        today_ms: 1_000,
      }),
    );
    assert_eq!(
      app.listening_streaks.as_ref().map(|s| s.current_days),
      Some(2)
    );

    apply_history_fact(
      &mut app,
      HistoryFact::RecapReady {
        path: std::path::PathBuf::from("recap.html"),
        listens: 4,
      },
    );
    assert_eq!(app.recap_prompt.as_ref().map(|p| p.listens), Some(4));

    app.push_navigation_stack(
      crate::core::app::RouteId::Search,
      crate::core::app::ActiveBlock::Input,
    );
    apply_history_fact(
      &mut app,
      HistoryFact::RecapReady {
        path: std::path::PathBuf::from("recap.html"),
        listens: 5,
      },
    );
    assert_eq!(app.recap_prompt.as_ref().map(|p| p.listens), Some(4));
    assert!(app.status_message.is_some());
  }

  #[tokio::test]
  async fn history_query_answers_snapshot_and_config_atomically() {
    let app = Arc::new(Mutex::new(App::default()));
    app
      .lock()
      .await
      .user_config
      .behavior
      .enable_monthly_recap_prompt = true;
    let (spine, _rx) = spawn_fold(&app).await;
    let answer = spine
      .query(crate::runtime::engine::FoldQuery::HistoryState)
      .await
      .expect("fold answers history state");
    assert!(matches!(
      answer,
      crate::runtime::engine::FoldAnswer::HistoryState(state)
        if state.enable_monthly_recap_prompt && state.sync_token.is_none()
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_start_claim_and_load_are_fold_owned() {
    use crate::core::app::NativePlaybackOrigin;
    use crate::runtime::engine::{
      FoldAnswer, FoldQuery, NativeDevicePreferenceUpdate, NativePlaybackActivation,
      NativePlaybackRequestSpec, SessionFact,
    };

    let mut app = App::default();
    let uris = vec!["spotify:track:one".to_string()];
    let activation = NativePlaybackActivation {
      native_device_id: "native-device".to_string(),
      native_device_name: "Spotatui".to_string(),
      saved_device_id: None,
      explicit_persist: false,
      origin: Some(NativePlaybackOrigin::RawList),
      activation_time: Instant::now(),
      backdate_poll: false,
      clear_context: false,
      bare_resume: false,
      request: Some(NativePlaybackRequestSpec {
        context_uri: None,
        uris: Some(uris.clone()),
        offset: Some(0),
        desired_playing: true,
        shuffle: true,
      }),
    };
    let answer = answer_query(
      &mut app,
      FoldQuery::NativePlaybackActivationDecision(activation.clone()),
    );

    assert!(matches!(
      answer,
      FoldAnswer::NativeActivation(decision)
        if decision.should_transfer
          && decision.preference == NativeDevicePreferenceUpdate::Persist
    ));
    assert!(!app.is_streaming_active);
    assert!(app.native_playback_recovery.is_none());

    apply_session_fact(&mut app, SessionFact::NativePlaybackActivated(activation));
    assert!(app.is_streaming_active);
    assert_eq!(app.native_device_id.as_deref(), Some("native-device"));
    assert_eq!(
      app.native_playback_origin,
      Some(NativePlaybackOrigin::RawList)
    );
    assert_eq!(
      app
        .native_playback_recovery
        .as_ref()
        .and_then(|snapshot| snapshot.uris.as_ref()),
      Some(&uris)
    );

    apply_session_fact(
      &mut app,
      SessionFact::NativeLoadAccepted {
        context_uri: None,
        uris: Some(uris.clone()),
        offset: Some(0),
        desired_shuffle_state: true,
        record: None,
      },
    );

    assert!(app.native_load_watchdog.is_some());
    assert_eq!(
      app
        .pending_start_playback
        .as_ref()
        .and_then(|pending| pending.uris.as_ref()),
      Some(&uris)
    );
    assert!(app.runtime_state.shuffle_enabled);
  }
}
