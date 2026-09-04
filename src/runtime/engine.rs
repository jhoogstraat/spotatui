//! The engine's event spine and command vocabulary (ADR 0016, stage 2).
//!
//! The tick loop is gone. Every runtime mutation now travels one path:
//!
//! ```text
//! producer (apply arms, streaming player events, Web API results,
//!           armed timers)  →  EngineEvent channel
//!                          →  fold task owns the published Snapshot
//! ```
//!
//! [`EngineCommand`] is the external input vocabulary — the window-facing
//! contract this product serves (rust-player `player-core::Command` maps onto
//! it one-to-one). Internal producers push [`EngineEvent`]s instead; the fold
//! task is the single writer of the published snapshot watch, so the old
//! two-hop publish (fork publish → adapter re-map/re-publish) collapses to
//! one equality-checked publication per genuine change.
//!
//! Ticket 16: producers stop writing shared state entirely. A Web API
//! handler fetches, then emits a result event — including error paths. Reads
//! that a producer needs to *decide* (was this request superseded? what is
//! the next missing page offset?) go through [`Spine::query`], which the
//! fold answers under its existing lock; no producer locks `App` any more.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch, Mutex};

#[allow(unused_imports)]
use crate::core::catalog::CatalogStateEvent;
use crate::core::streaming::StreamingStateEvent;

/// A typed runtime event. Every state mutation a frontend can observe enters
/// the spine through one of these.
///
/// Deliberately small in this ticket: playback/collection lifecycle changes
/// are latched by the fold task as "state moved" pokes that trigger a snapshot
/// rebuild, mirroring today's change-bell but with real typing and no lock
/// convoy. Later stages replace the coarse pokes with payload-carrying events
/// as each domain extracts (tickets 08–11).
#[derive(Debug)]
pub enum EngineEvent {
  /// A named one-shot deadline fired by the private engine scheduler.
  Timer(TimerFact),
  /// A typed streaming-state fact (ticket 14): the fold reduces these through
  /// `StreamingState::absorb` + `apply_streaming_state` instead of any
  /// producer touching shared playback fields.
  Streaming(StreamingStateEvent),
  /// A typed catalog fact (ticket 15): search/library/queue mutations from
  /// the dispatchers and network layer, folded with stale-request
  /// enforcement.
  Catalog(crate::core::catalog::CatalogStateEvent),
  /// A session fact (ticket 16): auth transitions, notices, and the loading
  /// spinner, emitted by the network layer instead of written under the App
  /// lock.
  Session(SessionFact),
  /// Decoded-source domain facts (local, Subsonic, radio, YouTube).  The
  /// envelope keeps source-specific reducers out of this central enum while
  /// giving every source one fold-owned write path.
  Decoded(DecodedSourceFact),
  /// Listening-history cache and recap facts, reduced by the fold.
  History(HistoryFact),
  /// Native-player and recovery completions.  The player handle is carried
  /// through the event so the fold can reject facts from a replaced backend.
  #[cfg(feature = "streaming")]
  Native(NativePlaybackFact),
}

/// Work completed by a native player/recovery worker.  These are facts, not
/// commands: workers perform librespot/device I/O and the fold owns every
/// authoritative state mutation.
#[cfg(feature = "streaming")]
pub enum NativePlaybackFact {
  /// A plain streaming state observation (position/loading/transport/volume).
  State {
    player: Arc<crate::infra::player::StreamingPlayer>,
    event: StreamingStateEvent,
  },
  Playing {
    player: Arc<crate::infra::player::StreamingPlayer>,
    track_uri: String,
    position_ms: u32,
  },
  Paused {
    player: Arc<crate::infra::player::StreamingPlayer>,
    track_uri: String,
    position_ms: u32,
  },
  TrackChanged {
    player: Arc<crate::infra::player::StreamingPlayer>,
    track_uri: String,
    info: crate::core::app::NativeTrackInfo,
    previous_track_id: Option<String>,
  },
  EndOfTrack {
    player: Arc<crate::infra::player::StreamingPlayer>,
    previous_track_id: String,
  },
  Disconnect {
    player: Arc<crate::infra::player::StreamingPlayer>,
    position_ms: u32,
    is_playing: bool,
    status_message: String,
    restore_playback: bool,
  },
  Install {
    player: Arc<crate::infra::player::StreamingPlayer>,
    /// `Some` for a recovery worker completion; `None` for first startup.
    generation: Option<u64>,
  },
  RecoveryFailed {
    generation: u64,
    error: String,
  },
  RecoveryRoute {
    player: Arc<crate::infra::player::StreamingPlayer>,
    request: crate::infra::player::StreamingRecoveryRequest,
    device_name: String,
  },
  Unavailable {
    player: Arc<crate::infra::player::StreamingPlayer>,
  },
  Loading {
    player: Arc<crate::infra::player::StreamingPlayer>,
    track_uri: String,
    position_ms: u32,
  },
  Reconnect {
    player: Arc<crate::infra::player::StreamingPlayer>,
    connected: bool,
  },
}

#[cfg(feature = "streaming")]
impl std::fmt::Debug for NativePlaybackFact {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let name = match self {
      Self::State { .. } => "State",
      Self::Playing { .. } => "Playing",
      Self::Paused { .. } => "Paused",
      Self::TrackChanged { .. } => "TrackChanged",
      Self::EndOfTrack { .. } => "EndOfTrack",
      Self::Disconnect { .. } => "Disconnect",
      Self::Install { .. } => "Install",
      Self::RecoveryFailed { .. } => "RecoveryFailed",
      Self::RecoveryRoute { .. } => "RecoveryRoute",
      Self::Unavailable { .. } => "Unavailable",
      Self::Loading { .. } => "Loading",
      Self::Reconnect { .. } => "Reconnect",
    };
    f.write_str(name)
  }
}

/// One-shot deadline facts emitted by the private engine scheduler.
///
/// The fact is deliberately named rather than carrying an arbitrary callback:
/// the fold remains the only writer and stale firings can be ignored when a
/// newer deadline replaced the one that was armed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerFact {
  /// Refresh the Spotify access token at the existing safety-margin deadline.
  AuthRefreshDue,
  /// Retire the status-bar notice when its TTL has elapsed.
  NoticeExpired,
  /// Retire an API error page after its backstop lifetime.
  ApiErrorExpired,
  /// Check a native load/restore that has produced no transport confirmation.
  #[cfg(feature = "streaming")]
  NativeLoadWatchdog,
  /// Reconcile playback state from Spotify for a remote playback device.
  PlaybackResync,
  /// Probe completion/coalescing while playback work is active. Re-armed as a
  /// one-shot only while a decoded/native session or pending volume needs it.
  PlaybackMaintenance,
  /// Flush the latest coalesced seek after its throttle window.
  SeekThrottleElapsed,
  /// Flush a debounced runtime-state patch.
  RuntimeStateSaveDue,
  /// Persist or clear the active decoded playback session.
  PlaybackSessionPersistDue,
}

/// Fold-owned writes produced by the listening-history collector.
#[derive(Debug, Clone)]
pub enum HistoryFact {
  /// Replace the cached streak summary after initial load or a new listen.
  Streaks(crate::infra::history::StreakSummary),
  /// A monthly recap is ready. The fold decides whether to show a prompt or
  /// a status notice from the current route, so producers never touch App.
  RecapReady {
    path: std::path::PathBuf,
    listens: usize,
  },
}

/// Fold-owned writes for the decoded sources.  Read-only decisions still use
/// the existing `FoldQuery` protocol; these facts carry only completed work.
pub enum DecodedSourceFact {
  /// Native queue ownership facts. Queue workers emit these after their I/O;
  /// the fold is the only writer of `queue_now`/`queue_suspended`.
  #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
  QueuePendingDecoded {
    player: Arc<crate::infra::audio::LocalPlayer>,
    track: crate::core::plugin_api::TrackInfo,
    fetch_id: u64,
  },
  #[cfg(feature = "local-files")]
  QueueDecodedLocalReady { fetch_id: u64 },
  #[cfg(feature = "streaming")]
  QueueSpotifyPublished(crate::core::plugin_api::TrackInfo),
  #[cfg(feature = "streaming")]
  QueueSpotifyCleared,
  #[cfg(feature = "streaming")]
  QueueTransportIntent { generation: u64, is_playing: bool },
  #[cfg(feature = "streaming")]
  QueueSpotifyPausedRestored { generation: u64 },
  #[cfg(feature = "streaming")]
  QueueNativeTrackPopped {
    track: crate::core::plugin_api::TrackInfo,
  },
  #[cfg(any(
    feature = "streaming",
    feature = "local-files",
    feature = "subsonic",
    feature = "youtube"
  ))]
  QueuePlaybackCleared,
  #[cfg(any(
    feature = "streaming",
    feature = "local-files",
    feature = "subsonic",
    feature = "youtube"
  ))]
  QueueSlotCleared,
  /// Ordered teardown of every decoded backend except `keep`. The ack carries
  /// the claimed handles so the caller can stop them without another query.
  #[cfg(any(
    feature = "local-files",
    feature = "subsonic",
    feature = "youtube",
    feature = "internet-radio"
  ))]
  ClaimBackendPlayers {
    keep: Option<crate::core::source::Source>,
  },
  #[cfg(any(
    feature = "streaming",
    feature = "local-files",
    feature = "subsonic",
    feature = "youtube",
    feature = "internet-radio"
  ))]
  QueueTakeSuspended,
  #[cfg(any(feature = "subsonic", feature = "youtube"))]
  QueueFetchClaimed { fetch_id: u64 },
  #[cfg(feature = "local-files")]
  LocalPlaylists(Vec<crate::core::plugin_api::PlaylistInfo>),
  #[cfg(feature = "local-files")]
  LocalTracks(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(feature = "local-files")]
  LocalPlayback(crate::infra::local::LocalPlaybackState),
  #[cfg(feature = "local-files")]
  LocalPlaybackCleared,
  #[cfg(feature = "subsonic")]
  SubsonicPlaylists(Vec<crate::core::plugin_api::PlaylistInfo>),
  #[cfg(feature = "subsonic")]
  SubsonicTracks(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(feature = "subsonic")]
  SubsonicPlayback(crate::infra::subsonic::SubsonicPlaybackState),
  #[cfg(feature = "subsonic")]
  SubsonicPlaybackCleared,
  #[cfg(feature = "internet-radio")]
  RadioStations(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(feature = "internet-radio")]
  RadioPlayback(crate::infra::radio::RadioPlaybackState),
  #[cfg(feature = "internet-radio")]
  RadioPlaybackCleared,
  #[cfg(feature = "internet-radio")]
  RadioResume(crate::core::plugin_api::TrackInfo),
  #[cfg(feature = "youtube")]
  YouTubePlaylists(Vec<crate::core::plugin_api::PlaylistInfo>),
  #[cfg(feature = "youtube")]
  YouTubeTracks(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(feature = "youtube")]
  YouTubeOpenPlaylist(Option<String>),
  /// A playlist was deleted; the fold clears the open marker only when this
  /// URI is the one currently displayed.
  #[cfg(feature = "youtube")]
  YouTubePlaylistDeleted(String),
  #[cfg(feature = "youtube")]
  YouTubePlayback(crate::infra::youtube::YouTubePlaybackState),
  #[cfg(feature = "youtube")]
  YouTubePlaybackCleared,
  #[cfg(feature = "youtube")]
  YouTubeStatus { message: String, ttl_secs: u64 },
  #[cfg(feature = "youtube")]
  YouTubeAdvancing(bool),
  #[cfg(feature = "youtube")]
  YouTubeIndex {
    target: usize,
    tempfile: tempfile::NamedTempFile,
  },
  #[cfg(feature = "youtube")]
  YouTubeReplaySucceeded,
  /// Shared runtime volume mirror used by all decoded sources.
  Volume(u8),
  #[cfg(feature = "local-files")]
  LocalAdvancing(bool),
  #[cfg(feature = "local-files")]
  LocalIndex {
    target: usize,
    info: Option<crate::core::plugin_api::TrackInfo>,
  },
  #[cfg(feature = "local-files")]
  LocalFailedIndex { target: usize, message: String },
  #[cfg(feature = "local-files")]
  LocalReplaySucceeded,
  #[cfg(feature = "local-files")]
  LocalStatus { message: String, ttl_secs: u64 },
  #[cfg(feature = "local-files")]
  LocalRepeat(crate::infra::queue::RepeatMode),
  #[cfg(feature = "subsonic")]
  SubsonicAdvancing(bool),
  #[cfg(feature = "subsonic")]
  SubsonicIndex {
    target: usize,
    tempfile: tempfile::NamedTempFile,
    display: String,
  },
  #[cfg(feature = "subsonic")]
  SubsonicReplaySucceeded,
  #[cfg(feature = "subsonic")]
  SubsonicStatus { message: String, ttl_secs: u64 },
  #[cfg(feature = "subsonic")]
  SubsonicRepeat(crate::infra::queue::RepeatMode),
  #[cfg(feature = "internet-radio")]
  RadioStatus { message: String, ttl_secs: u64 },
  /// Completion of a detached decoded queue fetch. The producer owns only
  /// immutable I/O inputs and reports every terminal outcome here; the fold
  /// decides whether the fetch is still the live queue slot.
  #[cfg(any(feature = "subsonic", feature = "youtube"))]
  QueueFetchCompleted {
    fetch_id: u64,
    result: QueueFetchResult,
  },
}

/// Typed decoded-source reads. Source handlers receive the live `Spine` and
/// ask the fold for exactly the snapshot needed for a decision.
#[derive(Debug)]
pub enum DecodedSourceQuery {
  #[cfg(feature = "local-files")]
  LocalConfig,
  #[cfg(feature = "local-files")]
  LocalPlayer,
  #[cfg(feature = "local-files")]
  LocalQueue,
  #[cfg(feature = "local-files")]
  LocalReplay,
  #[cfg(feature = "local-files")]
  LocalShuffle,
  #[cfg(feature = "local-files")]
  LocalSkip {
    forward: bool,
  },
  #[cfg(feature = "local-files")]
  LocalPlayIndex(usize),
  #[cfg(feature = "local-files")]
  LocalRepeat(rspotify::model::enums::RepeatState),
  #[cfg(feature = "subsonic")]
  SubsonicConfig,
  #[cfg(feature = "subsonic")]
  SubsonicPlayer,
  #[cfg(feature = "subsonic")]
  SubsonicShuffle,
  #[cfg(feature = "subsonic")]
  SubsonicTracks(Vec<String>),
  #[cfg(feature = "subsonic")]
  SubsonicReplay,
  #[cfg(feature = "subsonic")]
  SubsonicSkip {
    forward: bool,
  },
  #[cfg(feature = "subsonic")]
  SubsonicPlayIndex(usize),
  #[cfg(feature = "subsonic")]
  SubsonicRepeat(rspotify::model::enums::RepeatState),
  #[cfg(feature = "youtube")]
  YouTubeOpenPlaylist,
  #[cfg(feature = "internet-radio")]
  RadioPlayer,
  #[cfg(feature = "internet-radio")]
  RadioStationsSnapshot(String),
  #[cfg(feature = "internet-radio")]
  RadioConfig,
  #[cfg(feature = "youtube")]
  YouTubeConfig,
  #[cfg(feature = "youtube")]
  YouTubePlayer,
  #[cfg(feature = "youtube")]
  YouTubeTracks(Vec<String>),
  #[cfg(feature = "youtube")]
  YouTubeSkip {
    forward: bool,
  },
  #[cfg(feature = "youtube")]
  YouTubeReplay,
  #[cfg(feature = "youtube")]
  YouTubePlayIndex(usize),
  #[cfg(feature = "youtube")]
  YouTubeShuffle,
  #[cfg(feature = "youtube")]
  YouTubeRepeat(rspotify::model::enums::RepeatState),
  Volume,
}

pub enum DecodedSourceAnswer {
  Volume(u8),
  #[cfg(feature = "local-files")]
  LocalConfig(Option<String>),
  #[cfg(feature = "local-files")]
  LocalPlayer(Option<Arc<crate::infra::audio::LocalPlayer>>),
  #[cfg(feature = "local-files")]
  LocalQueue {
    index: usize,
    len: usize,
    repeat: crate::infra::queue::RepeatMode,
  },
  #[cfg(feature = "local-files")]
  LocalReplay(Option<(Arc<crate::infra::audio::LocalPlayer>, std::path::PathBuf)>),
  #[cfg(feature = "local-files")]
  LocalShuffle(bool),
  #[cfg(feature = "local-files")]
  LocalSkip(Option<usize>),
  #[cfg(feature = "local-files")]
  LocalPlayIndex(Option<(Arc<crate::infra::audio::LocalPlayer>, String)>),
  #[cfg(feature = "local-files")]
  LocalRepeat(bool),
  #[cfg(feature = "subsonic")]
  SubsonicConfig {
    url: Option<String>,
    username: Option<String>,
    password: Option<String>,
  },
  #[cfg(feature = "subsonic")]
  SubsonicPlayer(Option<Arc<crate::infra::audio::LocalPlayer>>),
  #[cfg(feature = "subsonic")]
  SubsonicShuffle(bool),
  #[cfg(feature = "subsonic")]
  SubsonicTracks(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(feature = "subsonic")]
  SubsonicReplay(Option<(Arc<crate::infra::audio::LocalPlayer>, std::path::PathBuf)>),
  #[cfg(feature = "subsonic")]
  SubsonicSkip(Option<usize>),
  #[cfg(feature = "subsonic")]
  SubsonicPlayIndex(SubsonicPlayTarget),
  #[cfg(feature = "subsonic")]
  SubsonicRepeat(bool),
  #[cfg(feature = "youtube")]
  YouTubeOpenPlaylist(Option<String>),
  #[cfg(feature = "internet-radio")]
  RadioPlayer(Option<Arc<crate::infra::audio::LocalPlayer>>),
  #[cfg(feature = "internet-radio")]
  RadioStationsSnapshot(crate::core::plugin_api::TrackInfo),
  #[cfg(feature = "internet-radio")]
  RadioConfig {
    configured: Vec<crate::core::state::RadioStationConfig>,
    saved: Vec<crate::core::state::RadioStationConfig>,
  },
  #[cfg(feature = "youtube")]
  YouTubeConfig(Option<String>),
  #[cfg(feature = "youtube")]
  YouTubePlayer(Option<Arc<crate::infra::audio::LocalPlayer>>),
  #[cfg(feature = "youtube")]
  YouTubeTracks(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(feature = "youtube")]
  YouTubeSkip(Option<usize>),
  #[cfg(feature = "youtube")]
  YouTubeReplay(Option<(Arc<crate::infra::audio::LocalPlayer>, std::path::PathBuf)>),
  #[cfg(feature = "youtube")]
  YouTubePlayIndex(
    Option<(
      Arc<crate::infra::audio::LocalPlayer>,
      Arc<crate::infra::youtube::YouTubeSource>,
      String,
      String,
    )>,
  ),
  #[cfg(feature = "youtube")]
  YouTubeShuffle(bool),
  #[cfg(feature = "youtube")]
  YouTubeRepeat(bool),
}

#[cfg(feature = "subsonic")]
pub type SubsonicPlayTarget = Option<
  Result<
    Option<(
      Arc<crate::infra::audio::LocalPlayer>,
      Arc<crate::infra::subsonic::SubsonicSource>,
      String,
      String,
    )>,
    (),
  >,
>;

impl std::fmt::Debug for DecodedSourceFact {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let name = match self {
      #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
      Self::QueuePendingDecoded { .. } => "QueuePendingDecoded",
      #[cfg(feature = "local-files")]
      Self::QueueDecodedLocalReady { .. } => "QueueDecodedLocalReady",
      #[cfg(feature = "streaming")]
      Self::QueueSpotifyPublished(_) => "QueueSpotifyPublished",
      #[cfg(feature = "streaming")]
      Self::QueueSpotifyCleared => "QueueSpotifyCleared",
      #[cfg(feature = "streaming")]
      Self::QueueTransportIntent { .. } => "QueueTransportIntent",
      #[cfg(feature = "streaming")]
      Self::QueueSpotifyPausedRestored { .. } => "QueueSpotifyPausedRestored",
      #[cfg(feature = "streaming")]
      Self::QueueNativeTrackPopped { .. } => "QueueNativeTrackPopped",
      #[cfg(any(
        feature = "streaming",
        feature = "local-files",
        feature = "subsonic",
        feature = "youtube"
      ))]
      Self::QueuePlaybackCleared => "QueuePlaybackCleared",
      #[cfg(any(
        feature = "streaming",
        feature = "local-files",
        feature = "subsonic",
        feature = "youtube"
      ))]
      Self::QueueSlotCleared => "QueueSlotCleared",
      #[cfg(any(
        feature = "local-files",
        feature = "subsonic",
        feature = "youtube",
        feature = "internet-radio"
      ))]
      Self::ClaimBackendPlayers { .. } => "ClaimBackendPlayers",
      #[cfg(any(
        feature = "streaming",
        feature = "local-files",
        feature = "subsonic",
        feature = "youtube",
        feature = "internet-radio"
      ))]
      Self::QueueTakeSuspended => "QueueTakeSuspended",
      #[cfg(any(feature = "subsonic", feature = "youtube"))]
      Self::QueueFetchClaimed { .. } => "QueueFetchClaimed",
      #[cfg(feature = "local-files")]
      Self::LocalPlaylists(_) => "LocalPlaylists",
      #[cfg(feature = "local-files")]
      Self::LocalTracks(_) => "LocalTracks",
      #[cfg(feature = "local-files")]
      Self::LocalPlayback(_) => "LocalPlayback",
      #[cfg(feature = "local-files")]
      Self::LocalPlaybackCleared => "LocalPlaybackCleared",
      #[cfg(feature = "subsonic")]
      Self::SubsonicPlaylists(_) => "SubsonicPlaylists",
      #[cfg(feature = "subsonic")]
      Self::SubsonicTracks(_) => "SubsonicTracks",
      #[cfg(feature = "subsonic")]
      Self::SubsonicPlayback(_) => "SubsonicPlayback",
      #[cfg(feature = "subsonic")]
      Self::SubsonicPlaybackCleared => "SubsonicPlaybackCleared",
      #[cfg(feature = "internet-radio")]
      Self::RadioStations(_) => "RadioStations",
      #[cfg(feature = "internet-radio")]
      Self::RadioPlayback(_) => "RadioPlayback",
      #[cfg(feature = "internet-radio")]
      Self::RadioPlaybackCleared => "RadioPlaybackCleared",
      #[cfg(feature = "internet-radio")]
      Self::RadioResume(_) => "RadioResume",
      #[cfg(feature = "youtube")]
      Self::YouTubePlaylists(_) => "YouTubePlaylists",
      #[cfg(feature = "youtube")]
      Self::YouTubeTracks(_) => "YouTubeTracks",
      #[cfg(feature = "youtube")]
      Self::YouTubeOpenPlaylist(_) => "YouTubeOpenPlaylist",
      #[cfg(feature = "youtube")]
      Self::YouTubePlaylistDeleted(_) => "YouTubePlaylistDeleted",
      #[cfg(feature = "youtube")]
      Self::YouTubePlayback(_) => "YouTubePlayback",
      #[cfg(feature = "youtube")]
      Self::YouTubePlaybackCleared => "YouTubePlaybackCleared",
      #[cfg(feature = "youtube")]
      Self::YouTubeStatus { .. } => "YouTubeStatus",
      #[cfg(feature = "youtube")]
      Self::YouTubeAdvancing(_) => "YouTubeAdvancing",
      #[cfg(feature = "youtube")]
      Self::YouTubeIndex { .. } => "YouTubeIndex",
      #[cfg(feature = "youtube")]
      Self::YouTubeReplaySucceeded => "YouTubeReplaySucceeded",
      #[cfg(feature = "local-files")]
      Self::LocalAdvancing(_) => "LocalAdvancing",
      #[cfg(feature = "local-files")]
      Self::LocalIndex { .. } => "LocalIndex",
      #[cfg(feature = "local-files")]
      Self::LocalFailedIndex { .. } => "LocalFailedIndex",
      #[cfg(feature = "local-files")]
      Self::LocalReplaySucceeded => "LocalReplaySucceeded",
      #[cfg(feature = "local-files")]
      Self::LocalStatus { .. } => "LocalStatus",
      #[cfg(feature = "local-files")]
      Self::LocalRepeat(_) => "LocalRepeat",
      #[cfg(feature = "subsonic")]
      Self::SubsonicAdvancing(_) => "SubsonicAdvancing",
      #[cfg(feature = "subsonic")]
      Self::SubsonicIndex { .. } => "SubsonicIndex",
      #[cfg(feature = "subsonic")]
      Self::SubsonicReplaySucceeded => "SubsonicReplaySucceeded",
      #[cfg(feature = "subsonic")]
      Self::SubsonicStatus { .. } => "SubsonicStatus",
      #[cfg(feature = "subsonic")]
      Self::SubsonicRepeat(_) => "SubsonicRepeat",
      #[cfg(feature = "internet-radio")]
      Self::RadioStatus { .. } => "RadioStatus",
      #[cfg(any(feature = "subsonic", feature = "youtube"))]
      Self::QueueFetchCompleted { .. } => "QueueFetchCompleted",
      Self::Volume(_) => "Volume",
    };
    f.write_str(name)
  }
}

/// Terminal result of a decoded queue worker. `Empty`, `Cancelled`, and
/// `Superseded` are explicit so an abandoned worker never looks like a silent
/// success or mutates the next queue item.
#[cfg(any(feature = "subsonic", feature = "youtube"))]
pub enum QueueFetchResult {
  Success(tempfile::NamedTempFile),
  Empty,
  Failure(String),
  Cancelled,
  Superseded,
}

#[cfg(any(feature = "subsonic", feature = "youtube"))]
impl std::fmt::Debug for QueueFetchResult {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str(match self {
      Self::Success(_) => "Success",
      Self::Empty => "Empty",
      Self::Failure(_) => "Failure",
      Self::Cancelled => "Cancelled",
      Self::Superseded => "Superseded",
    })
  }
}

/// Which persisted playback-session operation completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionPersistenceOperation {
  Save,
  Clear,
}

/// One session-level fact (ticket 16). The fold applies these through the
/// same methods the old inline writes used, so behavior is byte-for-byte
/// parity while the lock sites disappear. Debug and Clone are manual:
/// `IoEvent` has no Debug and must not grow one.
pub enum SessionFact {
  /// Reserve a metadata navigation generation before an async request.
  MetadataRequested,
  /// Completion of an ordered playback-session write. The fold retries failed
  /// writes and ignores a clear that no longer matches current state.
  PersistenceCompleted {
    operation: SessionPersistenceOperation,
    success: bool,
  },
  /// Flush all pending durable state before the fold closes.
  FlushPersistence,
  /// A metadata request ended empty or failed; stale generations are ignored.
  MetadataTerminal {
    generation: u64,
    message: Option<String>,
  },
  /// An auth-state transition (token refreshed, login completed, session
  /// lost). The fold runs `AuthState::transition` + the legacy mirror sync.
  Auth {
    transition: crate::core::auth::AuthTransition,
    new_expiry: Option<std::time::SystemTime>,
    /// Post-transition wiring the old `complete_spotify_login` did under the
    /// same lock: dispatch the post-connect data fetches.
    post_connect_fetches: Vec<PostConnectFetch>,
  },
  /// A status-bar notice with a TTL, scaled by the config at the fold.
  Status {
    message: String,
    ttl_secs: u64,
    is_error: bool,
  },
  /// Apply a relative seek against the fold-owned current position.
  RelativeSeek(i64),
  /// The deferred native startup window opened or closed.
  #[cfg(feature = "streaming")]
  NativeBackendPending(bool),
  /// Complete deferred native startup in one ordered fold step: clear the
  /// interlock, apply the configured Spotify startup action, then replay any
  /// user request parked while the backend initialized.
  #[cfg(feature = "streaming")]
  NativeStartupFinished {
    spotify_startup_behavior: Option<crate::core::user_config::StartupBehavior>,
    initial_shuffle_enabled: bool,
  },
  /// The generic end-of-handler reset: global spinner down.
  LoadingCleared,
  /// Direct mirror sync for the token-refresh path (requests.rs), whose
  /// closure only knows the expiry: the fold reconciles the auth domain and
  /// the legacy mirrors from this.
  AuthMirrors {
    token_expiry: Option<std::time::SystemTime>,
    refresh_in_progress: bool,
    connected: bool,
  },
  /// Route a failure through `App::handle_error` (the old inline
  /// `self.handle_error(e)` call sites): error page hint + 60s TTL.
  Error(String),
  /// Clear `pending_playlist_open` when it still names `id` (`None` clears
  /// unconditionally — the auth-gate paths).
  ClearPendingPlaylistOpen(Option<String>),
  /// Dispatch `GetPlaylists` from the fold (the post-create refresh and
  /// other places that re-pull the sidebar list after a mutation).
  DispatchPlaylistRefresh,
  /// The stats screen's load ended without rows: drop its scoped spinner.
  StatsLoadingCleared,
  /// Apply an in-process producer's action through the fold (ticket 17:
  /// DJ tools, MCP executor). The fold runs `App::apply`'s ownership-aware
  /// arm under its own lock — the exact old inline `app.apply(...)` writes.
  ApplyAction(crate::core::action::EngineAction),
  /// One DJ transcript line (ticket 17: the agent loop stopped locking App).
  #[cfg(feature = "dj-core")]
  DjPushLine {
    speaker: crate::infra::dj::DjSpeaker,
    text: String,
  },
  /// The DJ's per-turn progress flag (`…thinking` row).
  #[cfg(feature = "dj-core")]
  DjStep(Option<(usize, usize)>),
  // --- Ticket 17 (network envelope): session-surface facts.
  /// The signed-in profile response (the old `get_user`'s `app.user` write).
  /// `Err` surfaces the rate-limit notice through the same fold sink.
  User(Result<crate::core::app::UserInfo, String>),
  /// The global play counter (telemetry read; `None` = fetch failed).
  GlobalSongCount {
    count: Option<u64>,
    failed: bool,
  },
  /// A deduplicated announcement feed: the fold enqueues and opens the
  /// prompt route only when nothing was active (the old inline block).
  Announcements(Vec<crate::core::app::Announcement>),
  /// A finished listening-history stats load. The fold applies only when
  /// the period still matches the selected one (the old inline race check).
  StatsLoaded {
    period: crate::infra::history::RecapPeriod,
    stats: crate::infra::history::StatsData,
    streaks: crate::infra::history::StreakSummary,
  },
  /// A completed recently-played page (liked checks are dispatched by the
  /// producer; the fold sorts + bumps + optionally navigates).
  RecentlyPlayed {
    page: crate::core::pagination::CursorPaged<crate::core::plugin_api::TrackInfo>,
    navigate: bool,
  },
  /// Fire these IoEvents from the fold (`App::dispatch`, the old inline
  /// spinner-raising semantics). For membership checks a producer cannot
  /// send itself without locking App for the channel handle.
  DispatchIo(Vec<crate::infra::network::IoEvent>),
  /// The create-playlist track picker's candidate rows.
  CreatePlaylistSearchResults(Vec<crate::core::plugin_api::TrackInfo>),
  /// The recommendation table: the folded rows (seed prepended) replace the
  /// visible table and open the recommendations route.
  RecommendedTracks {
    tracks: Vec<crate::core::plugin_api::TrackInfo>,
  },
  /// The artist view landed (metadata.rs): the fold writes `app.artist` and
  /// pushes the navigation route. Boxed — the view bundles three pages.
  ArtistView {
    generation: u64,
    artist: Box<crate::core::app::Artist>,
  },
  /// An album's tracklist landed. `Simplified` comes from the playlist-style
  /// listing (context Simplified); `Full` from the album endpoint (context
  /// Full). Both push the AlbumTracks route with a reset selection.
  AlbumTracksSimplified {
    generation: u64,
    album: crate::core::plugin_api::AlbumInfo,
    tracks: crate::core::pagination::Paged<crate::core::plugin_api::TrackInfo>,
  },
  AlbumTracksFull {
    generation: u64,
    album: crate::core::plugin_api::AlbumInfo,
  },
  /// Podcast episodes landed: `reset` clears the paged library cache and
  /// selects the show (entry from a show list); otherwise pages append to the
  /// existing cache (scroll pagination).
  ShowEpisodesLanded {
    generation: u64,
    page: crate::core::pagination::Paged<crate::core::plugin_api::EpisodeInfo>,
    show: crate::core::plugin_api::ShowInfo,
  },
  ShowEpisodesAppended {
    generation: u64,
    page: crate::core::pagination::Paged<crate::core::plugin_api::EpisodeInfo>,
  },
  /// The full show landed (metadata.rs).
  FullShowLanded {
    generation: u64,
    show: crate::core::plugin_api::ShowInfo,
  },
  /// The follow-check round-trip for ids already known to be followed
  /// (metadata.rs seeds the set directly; the IoEvent lane covers the rest).
  FollowedArtistsConfirmed(Vec<String>),
  /// The app-owned native-shuffle session is gone (failed load or explicit
  /// teardown): the fold runs `App::clear_native_shuffle_session`.
  ClearNativeShuffleSession,
  /// A reload a native-shuffle load was confirming is not coming (the load
  /// failed): drop the session's pending reload confirmation so a later
  /// genuine `TrackChanged` is not mistaken for it.
  ClearNativeShufflePendingReload,
  /// A finished DJ playlist crawl: the fold releases the in-flight flag and
  /// installs the library (or reports the failure in transcript + toast).
  #[cfg(feature = "dj-core")]
  DjLibraryIndexed(Result<Box<crate::infra::dj::DjLibrary>, String>),
  /// A finished DJ turn: release the turn's progress flag (only its own
  /// turn may), then apply the success/failure reporting the old `ask_dj`
  /// epilogue did.
  #[cfg(feature = "ai-dj")]
  DjTurnFinished {
    turn_seq: u64,
    outcome: Result<DjTurnSummary, String>,
    vibe_on_success: Option<String>,
    must_act: bool,
  },
  /// The device list landed (user.rs `get_devices`). The fold runs the whole
  /// old lock section: navigation, the native recovery interlock (a
  /// check-and-set must not be split from the write it gates), the
  /// native-device merge, the cache write, and the plugin generation bump.
  /// `Device` clones; the payload wrapper does not.
  DevicesLanded {
    devices: Vec<rspotify::model::device::Device>,
    navigate: bool,
  },
  // --- Ticket 17 (playback family): start_playback mirrors. Streaming-only.
  /// Park a StartPlayback request for replay once a usable backend exists
  /// (the recovery park and every "backend may materialize" park).
  #[cfg(feature = "streaming")]
  ParkStartPlayback {
    context_uri: Option<String>,
    uris: Option<Vec<String>>,
    offset: Option<usize>,
  },
  /// The native player has been activated; install its App-owned mirrors and
  /// recovery intent after the producer has called `player.activate()`.
  #[cfg(feature = "streaming")]
  NativePlaybackActivated(NativePlaybackActivation),
  /// A context start via the Web API succeeded (`start_native_context_via_api`):
  /// the whole Ok-arm write (poll backdate, play/shuffle mirror, park for the
  /// watchdog, re-poll dispatch).
  #[cfg(feature = "streaming")]
  NativeApiContextStarted {
    context_uri: String,
    uris: Option<Vec<String>>,
    offset: Option<usize>,
    shuffle_applied: bool,
    desired_shuffle_state: bool,
  },
  /// A bare-resume through the Web API on the native device succeeded
  /// (`me/player/play`): mirror the play state and re-poll. `Some` also
  /// records the native device id (the resume path that discovered it).
  #[cfg(feature = "streaming")]
  NativeResumeApiStarted {
    native_device_id: Option<String>,
  },
  /// A direct native load was accepted: park for the watchdog, arm it, and
  /// optimistically mirror the play/shuffle state (the watchdog corrects a
  /// zombie session). `record` additionally writes the recovery snapshot
  /// (the fallback path, which had no prior activation-time record).
  #[cfg(feature = "streaming")]
  NativeLoadAccepted {
    context_uri: Option<String>,
    uris: Option<Vec<String>>,
    offset: Option<usize>,
    desired_shuffle_state: bool,
    record: Option<NativePlaybackRequestSpec>,
  },
  // --- Ticket 17 (playback family): transport-command mirrors. Each fact
  // names the outcome whose UI/recovery writes it folds. The native-backed
  // mirrors are streaming-only; `TransportStateMirrored` and the volume-drop
  // also serve the Web API legs that run on slim builds.
  /// A new transport command supersedes any parked replay: drop
  /// `pending_start_playback` and the load watchdog.
  #[cfg(feature = "streaming")]
  ClearStartPlaybackReplay,
  /// A native pause landed: mirror it into the play intent, native play
  /// state, and the context.
  #[cfg(feature = "streaming")]
  NativePauseMirrored,
  /// A native seek landed: record the recovery position.
  #[cfg(feature = "streaming")]
  NativeSeekMirrored {
    position_ms: u32,
  },
  /// A native shuffle toggle landed: recovery state + context + runtime
  /// state + the persisted save.
  #[cfg(feature = "streaming")]
  NativeShuffleMirrored {
    state: bool,
  },
  /// A native repeat toggle landed: recovery state + context.
  #[cfg(feature = "streaming")]
  NativeRepeatMirrored {
    state: rspotify::model::RepeatState,
  },
  /// A native volume change landed: context, runtime state, persisted save,
  /// and the in-flight bookkeeping (`pending_volume` stays until the API
  /// confirms the value).
  #[cfg(feature = "streaming")]
  NativeVolumeMirrored {
    volume: u8,
  },
  /// A Web API transport command landed: mirror whichever of play state,
  /// shuffle, repeat, volume the command wrote (each `Some` mirrors exactly
  /// the writes its success path did).
  TransportStateMirrored {
    is_playing: Option<bool>,
    shuffle: Option<bool>,
    repeat: Option<rspotify::model::RepeatState>,
    volume: Option<u8>,
  },
  /// A Web API volume change failed: drop the in-flight bookkeeping so the UI
  /// falls back to whatever the API last reported.
  VolumeChangeDropped,
  /// A raw, unscaled status notice (the old direct `status_message` writes —
  /// no TTL scaling, no error-flag coupling).
  TransientStatus {
    message: String,
    ttl_secs: u64,
  },
  /// A Web API queue load landed: install (or clear) the queue and bump its
  /// plugin-data generation (completion, not success, is what requests wait
  /// on).
  QueueLanded(Option<crate::core::app::QueueState>),
  /// Restore the persisted native queue during startup.
  #[cfg(feature = "streaming")]
  NativeQueueLanded(Vec<crate::core::plugin_api::TrackInfo>),
  /// A playback poll landed; the fold applies all native/context mirrors.
  #[cfg(feature = "streaming")]
  PlaybackPolled {
    context: Option<Box<rspotify::model::context::CurrentPlaybackContext>>,
    state: Box<crate::infra::network::playback::PlaybackPollState>,
  },
  #[cfg(not(feature = "streaming"))]
  PlaybackPolled {
    context: Option<Box<rspotify::model::context::CurrentPlaybackContext>>,
  },
  /// Playback polling finished with no active context (or an idle 404).
  PlaybackPollIdle {
    clear_context: bool,
  },
  #[cfg(feature = "streaming")]
  NativeIdleDeviceReconciled {
    native_device_id: String,
    device_name: String,
    volume: u8,
    activated_at: Option<std::time::Instant>,
  },
  #[cfg(feature = "streaming")]
  NativeRestoreApplied {
    generation: u64,
    desired_playing: bool,
    position_ms: u64,
    shuffle: bool,
    repeat: rspotify::model::RepeatState,
  },
  #[cfg(feature = "streaming")]
  NativeRestoreFailed {
    generation: u64,
  },
  #[cfg(feature = "streaming")]
  NativeRestoreNoState {
    generation: u64,
  },
  #[cfg(feature = "streaming")]
  NativeTransferActivated {
    native_device_id: String,
    activation_time: std::time::Instant,
  },
  ExternalTransferCompleted {
    device_id: String,
  },
  #[cfg(feature = "streaming")]
  AutoSelectApplied {
    native_device_id: String,
    activation_time: std::time::Instant,
    external_playback_active: bool,
  },
  #[cfg(feature = "streaming")]
  NativePlaybackIntentStopped,
  #[cfg(feature = "streaming")]
  ForceNativeRecovery,
}

/// The parts of a DJ turn outcome the epilogue reports on.
#[cfg(feature = "ai-dj")]
#[derive(Debug, Clone)]
pub struct DjTurnSummary {
  pub acted: bool,
  pub abandoned: bool,
  pub vibe_set: bool,
}

/// The data fetches `complete_spotify_login` used to dispatch under the same
/// lock that connected the session (Clonable by construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostConnectFetch {
  User,
  Playlists,
  CurrentPlayback,
}

impl std::fmt::Debug for SessionFact {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      SessionFact::MetadataRequested => f.write_str("MetadataRequested"),
      SessionFact::PersistenceCompleted { operation, success } => f
        .debug_struct("PersistenceCompleted")
        .field("operation", operation)
        .field("success", success)
        .finish(),
      SessionFact::FlushPersistence => f.write_str("FlushPersistence"),
      SessionFact::MetadataTerminal { .. } => f.write_str("MetadataTerminal"),
      SessionFact::Auth {
        transition,
        new_expiry,
        ..
      } => f
        .debug_struct("Auth")
        .field("transition", transition)
        .field("new_expiry", new_expiry)
        .finish_non_exhaustive(),
      SessionFact::Status {
        message,
        ttl_secs,
        is_error,
      } => f
        .debug_struct("Status")
        .field("message", message)
        .field("ttl_secs", ttl_secs)
        .field("is_error", is_error)
        .finish(),
      SessionFact::RelativeSeek(offset) => f.debug_tuple("RelativeSeek").field(offset).finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeBackendPending(pending) => f
        .debug_tuple("NativeBackendPending")
        .field(pending)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeStartupFinished {
        spotify_startup_behavior,
        initial_shuffle_enabled,
      } => f
        .debug_struct("NativeStartupFinished")
        .field("spotify_startup_behavior", spotify_startup_behavior)
        .field("initial_shuffle_enabled", initial_shuffle_enabled)
        .finish(),
      SessionFact::LoadingCleared => f.write_str("LoadingCleared"),
      SessionFact::AuthMirrors {
        token_expiry,
        refresh_in_progress,
        connected,
      } => f
        .debug_struct("AuthMirrors")
        .field("token_expiry", token_expiry)
        .field("refresh_in_progress", refresh_in_progress)
        .field("connected", connected)
        .finish(),
      SessionFact::Error(message) => f.debug_tuple("Error").field(message).finish(),
      SessionFact::ClearPendingPlaylistOpen(id) => {
        f.debug_tuple("ClearPendingPlaylistOpen").field(id).finish()
      }
      SessionFact::DispatchPlaylistRefresh => f.write_str("DispatchPlaylistRefresh"),
      SessionFact::StatsLoadingCleared => f.write_str("StatsLoadingCleared"),
      SessionFact::ArtistView { .. } => f.write_str("ArtistView"),
      SessionFact::AlbumTracksSimplified { .. } => f.write_str("AlbumTracksSimplified"),
      SessionFact::AlbumTracksFull { .. } => f.write_str("AlbumTracksFull"),
      SessionFact::ShowEpisodesLanded { .. } => f.write_str("ShowEpisodesLanded"),
      SessionFact::ShowEpisodesAppended { .. } => f.write_str("ShowEpisodesAppended"),
      SessionFact::FullShowLanded { .. } => f.write_str("FullShowLanded"),
      SessionFact::FollowedArtistsConfirmed(ids) => f
        .debug_tuple("FollowedArtistsConfirmed")
        .field(&ids.len())
        .finish(),
      SessionFact::ClearNativeShuffleSession => f.write_str("ClearNativeShuffleSession"),
      SessionFact::ClearNativeShufflePendingReload => {
        f.write_str("ClearNativeShufflePendingReload")
      }
      #[cfg(feature = "dj-core")]
      SessionFact::DjLibraryIndexed(result) => f
        .debug_tuple("DjLibraryIndexed")
        .field(&result.is_ok())
        .finish(),
      #[cfg(feature = "ai-dj")]
      SessionFact::DjTurnFinished {
        turn_seq, outcome, ..
      } => f
        .debug_struct("DjTurnFinished")
        .field("turn_seq", turn_seq)
        .field("ok", &outcome.is_ok())
        .finish_non_exhaustive(),
      SessionFact::DevicesLanded { navigate, .. } => f
        .debug_struct("DevicesLanded")
        .field("navigate", navigate)
        .finish_non_exhaustive(),
      #[cfg(feature = "streaming")]
      SessionFact::ParkStartPlayback { .. } => f.write_str("ParkStartPlayback"),
      #[cfg(feature = "streaming")]
      SessionFact::NativePlaybackActivated(_) => f.write_str("NativePlaybackActivated"),
      #[cfg(feature = "streaming")]
      SessionFact::NativeApiContextStarted { .. } => f.write_str("NativeApiContextStarted"),
      #[cfg(feature = "streaming")]
      SessionFact::NativeResumeApiStarted { .. } => f.write_str("NativeResumeApiStarted"),
      #[cfg(feature = "streaming")]
      SessionFact::NativeLoadAccepted { .. } => f.write_str("NativeLoadAccepted"),
      #[cfg(feature = "streaming")]
      SessionFact::ClearStartPlaybackReplay => f.write_str("ClearStartPlaybackReplay"),
      #[cfg(feature = "streaming")]
      SessionFact::NativePauseMirrored => f.write_str("NativePauseMirrored"),
      #[cfg(feature = "streaming")]
      SessionFact::NativeSeekMirrored { position_ms } => f
        .debug_struct("NativeSeekMirrored")
        .field("position_ms", position_ms)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeShuffleMirrored { state } => f
        .debug_struct("NativeShuffleMirrored")
        .field("state", state)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeRepeatMirrored { state } => f
        .debug_struct("NativeRepeatMirrored")
        .field("state", state)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeVolumeMirrored { volume } => f
        .debug_struct("NativeVolumeMirrored")
        .field("volume", volume)
        .finish(),
      SessionFact::PlaybackPolled { .. } => f.write_str("PlaybackPolled"),
      SessionFact::PlaybackPollIdle { .. } => f.write_str("PlaybackPollIdle"),
      #[cfg(feature = "streaming")]
      SessionFact::NativeIdleDeviceReconciled { .. } => f.write_str("NativeIdleDeviceReconciled"),
      #[cfg(feature = "streaming")]
      SessionFact::NativeRestoreApplied { generation, .. } => f
        .debug_struct("NativeRestoreApplied")
        .field("generation", generation)
        .finish_non_exhaustive(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeRestoreFailed { generation } => f
        .debug_struct("NativeRestoreFailed")
        .field("generation", generation)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeRestoreNoState { generation } => f
        .debug_struct("NativeRestoreNoState")
        .field("generation", generation)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeTransferActivated { .. } => f.write_str("NativeTransferActivated"),
      SessionFact::ExternalTransferCompleted { device_id } => f
        .debug_tuple("ExternalTransferCompleted")
        .field(device_id)
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::AutoSelectApplied { .. } => f.write_str("AutoSelectApplied"),
      #[cfg(feature = "streaming")]
      SessionFact::NativePlaybackIntentStopped => f.write_str("NativePlaybackIntentStopped"),
      #[cfg(feature = "streaming")]
      SessionFact::ForceNativeRecovery => f.write_str("ForceNativeRecovery"),
      SessionFact::TransportStateMirrored {
        is_playing,
        shuffle,
        repeat,
        volume,
      } => f
        .debug_struct("TransportStateMirrored")
        .field("is_playing", &is_playing.is_some())
        .field("shuffle", &shuffle.is_some())
        .field("repeat", &repeat.is_some())
        .field("volume", volume)
        .finish(),
      SessionFact::VolumeChangeDropped => f.write_str("VolumeChangeDropped"),
      SessionFact::TransientStatus { ttl_secs, .. } => f
        .debug_struct("TransientStatus")
        .field("ttl_secs", ttl_secs)
        .finish_non_exhaustive(),
      SessionFact::QueueLanded(queue) => f
        .debug_tuple("QueueLanded")
        .field(&queue.is_some())
        .finish(),
      #[cfg(feature = "streaming")]
      SessionFact::NativeQueueLanded(queue) => f
        .debug_tuple("NativeQueueLanded")
        .field(&queue.len())
        .finish(),
      SessionFact::ApplyAction(action) => f.debug_tuple("ApplyAction").field(action).finish(),
      #[cfg(feature = "dj-core")]
      SessionFact::DjPushLine { speaker, text } => f
        .debug_struct("DjPushLine")
        .field("speaker", speaker)
        .field("text", text)
        .finish(),
      #[cfg(feature = "dj-core")]
      SessionFact::DjStep(step) => f.debug_tuple("DjStep").field(step).finish(),
      SessionFact::User(result) => f.debug_tuple("User").field(&result.is_ok()).finish(),
      SessionFact::GlobalSongCount { count, failed } => f
        .debug_struct("GlobalSongCount")
        .field("count", &count.is_some())
        .field("failed", failed)
        .finish(),
      SessionFact::Announcements(announcements) => f
        .debug_tuple("Announcements")
        .field(&announcements.len())
        .finish(),
      SessionFact::StatsLoaded { period, .. } => {
        f.debug_tuple("StatsLoaded").field(period).finish()
      }
      SessionFact::RecentlyPlayed { navigate, .. } => f
        .debug_struct("RecentlyPlayed")
        .field("navigate", navigate)
        .finish_non_exhaustive(),
      SessionFact::DispatchIo(events) => f.debug_tuple("DispatchIo").field(&events.len()).finish(),
      SessionFact::CreatePlaylistSearchResults(tracks) => f
        .debug_tuple("CreatePlaylistSearchResults")
        .field(&tracks.len())
        .finish(),
      SessionFact::RecommendedTracks { tracks } => f
        .debug_tuple("RecommendedTracks")
        .field(&tracks.len())
        .finish(),
    }
  }
}

impl EngineEvent {
  /// Emit-side result of a typed streaming fact: what the fold will absorb.
  /// Mirrors [`AbsorbOutcome`] so a producer can decide cheaply whether the
  /// event it just built is worth pushing (identical positions, duplicate
  /// loads) before touching the channel.
  pub fn streaming_is_significant(event: &StreamingStateEvent) -> bool {
    match event {
      // Position-only events are cheap and frequent; emit them all — the
      // fold's floor drops duplicates. Significance here only filters
      // obviously-unobservable construction, not ordering decisions.
      StreamingStateEvent::PositionChanged { .. } => true,
      _ => true,
    }
  }
}

/// What the embedder asks the engine to do. The single input vocabulary:
/// `player-core::Command` maps one-to-one here (the fork's `Action` enum was
/// the terminal UI's input shape and stays internal to in-process producers).
#[derive(Debug, Clone, PartialEq)]
pub enum EngineCommand {
  /// Start playing this resolved playable (`spotify:track:…` URIs etc.).
  PlayUris {
    uris: Vec<String>,
    offset: Option<usize>,
  },
  Pause,
  Resume,
  /// Absolute seek in milliseconds, clamped by the receiving domain.
  Seek(u64),
  NextTrack,
  PreviousTrack,
  Volume(u8),
  /// Enqueue one native-queue track (multi-source queue entry point).
  EnqueueNative(crate::core::plugin_api::TrackInfo),
  RemoveNativeQueued(usize),
  MoveNativeQueued {
    index: usize,
    up: bool,
  },
  ClearNativeQueue,
  /// Run a source-scoped catalog search.
  SearchActiveSource(String),
  /// Open a library section in the second browse column.
  Browse(LibraryTarget),
  /// Re-run sign-in after an expired or failed session.
  Reauthenticate,
  /// Dismiss the current notice (if any). The real dismissal primitive —
  /// not an error-message-shaped hack.
  DismissNotice,
}

pub use crate::core::action::LibraryTarget;

/// A read a producer needs answered against the fold's authoritative state
/// (ticket 16). The fold answers under its existing App lock; the producer
/// never locks. Every variant names the exact decision it feeds, so the
/// protocol stays auditable.
pub enum FoldQuery {
  /// Whether OS media-key integrations are currently enabled.
  MediaKeysEnabled,
  /// Decoded-source read envelope; concrete variants are kept in the source
  /// domain so this central vocabulary stays compact.
  Decoded(DecodedSourceQuery),
  /// Snapshot the decoded queue player without changing ownership.
  #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
  QueueDecodedPlayer,
  /// Was this library request superseded before it even fetched? The
  /// detached prefetch workers check their generation against the fold's
  /// (the old `app.saved_tracks_prefetch_generation == generation` read).
  LibraryRequestLive {
    section: crate::core::catalog::LibrarySection,
    generation: u64,
  },
  /// The next missing page offset for an offset-keyed library cache, plus
  /// the live generation to carry into the follow-up claim.
  NextLibraryOffset {
    section: crate::core::catalog::LibrarySection,
  },
  /// Claim an offset for fetching (the old `in_flight.insert(offset)` RMW):
  /// true when the claim was free and is now held.
  ClaimLibraryOffset {
    section: crate::core::catalog::LibrarySection,
    generation: u64,
  },
  /// Release a claimed offset (fetch failed or the task is exiting).
  ReleaseLibraryOffset {
    section: crate::core::catalog::LibrarySection,
    generation: u64,
  },
  /// Liked membership for these ids (the like toggle reads it to decide
  /// save-vs-delete; the detached worker used to merge under one lock).
  LikedState { ids: Vec<String> },
  /// The current user id, for handlers that need it in a payload (playlist
  /// creation).
  UserId,
  /// Whether a playlist's track table is the active browse surface (the
  /// stale-page contract check `is_playlist_track_table_active_for`).
  PlaylistTrackTableActive { playlist_id: String },
  /// Merge a contains-response into the liked set (the CLI's inline resolve
  /// path): each id is inserted or removed per the API's answer.
  MergeLikedState {
    ids: Vec<String>,
    is_saved: Vec<bool>,
  },
  /// Atomically drain up to 40 pending liked-state ids for the detached
  /// worker; `None` in the payload means the set is empty and the worker
  /// should exit.
  ClaimLikedLookupBatch,
  /// Merge a worker's contains-response, re-pending it when a local
  /// like/unlike (epoch bump) landed while the read was in flight.
  ApplyLikedLookup {
    epoch: u64,
    batch: Vec<String>,
    is_saved: Vec<bool>,
  },
  /// Put a failed batch back in the pending set (transient failure).
  RependLikedLookup { batch: Vec<String> },
  /// Clear the single-worker running flag (persistent failure path).
  StopLikedLookupWorker,
  /// Release a playlist-sort fetch slot (the detached sort task exited or
  /// its fetch failed).
  ReleasePlaylistSortFetch { playlist_id: String },
  /// Atomically claim the playlist-sort fetch slot (the detached sort task
  /// start; `false` when one is already in flight for this playlist).
  ClaimPlaylistSortFetch { playlist_id: String },
  /// A track was added to a playlist: refresh the visible table when that
  /// playlist is on screen; answer with the status text the old inline
  /// block produced.
  PlaylistTrackAdded { playlist_id: String },
  /// Enqueue liked-state ids for the detached worker; `true` when the
  /// caller must spawn the worker (it made the first claim).
  EnqueueLikedLookup { ids: Vec<String> },
  /// Is this playlist-refresh generation still the live one?
  PlaylistRefreshLive { generation: u64 },
  /// Release a playlist page-1 fetch slot + clear the pending open (the
  /// error path of `GetPlaylistItems`).
  ReleasePlaylistTracksPage {
    playlist_id: String,
    offset: u32,
    generation: u64,
  },
  /// Clear the playlist track filter (the empty-query branch of
  /// `SearchPlaylistTracks`); the fold rebuilds the unfiltered table.
  ClearPlaylistTrackFilter,
  /// Apply playlist-track search results (the completed-filter branch).
  PlaylistTrackSearchApplied {
    playlist_id: String,
    query: String,
    matches: Vec<(rspotify::model::track::FullTrack, usize)>,
  },
  /// Clear a pending playlist track search that can no longer land (the
  /// playlist changed mid-search).
  ClearPendingPlaylistTrackSearch,
  /// The saved-tracks view was re-opened (fold resets the cache + bumps
  /// the generation before the page-1 fetch dispatches).
  OpenSavedTracksView,
  /// Register a playlist refresh: snapshot the previous complete list,
  /// bump the generation, install page 1, reconcile the selection, and
  /// capture the page-1 selection for the background task. The old three
  /// lock sections of `get_current_user_playlists`, one atomic step.
  RegisterPlaylistRefresh {
    /// The mapped page-1 listing (installed as `app.playlists`).
    first_page: crate::core::pagination::Paged<crate::core::plugin_api::PlaylistInfo>,
    /// The page-1 items (installed as the visible list when there is no
    /// previous complete list to preserve).
    first_items: Vec<crate::core::plugin_api::PlaylistInfo>,
  },
  /// The live streaming player handle (rootlist fetch; `None` when native
  /// streaming is down).
  #[cfg(feature = "streaming")]
  StreamingPlayerHandle,
  /// A clone of the App's IoEvent sender (the in-TUI login callback task
  /// needs the channel handle without locking App).
  IoTx,
  /// Publish a completed playlist refresh (or restore the previous list on
  /// pagination failure). The fold owns the generation check, folder
  /// structure, selection reconcile, and the community-pin prompt.
  PublishPlaylistRefresh {
    generation: u64,
    playlists: Vec<crate::core::plugin_api::PlaylistInfo>,
    original_selection: crate::core::catalog::PlaylistSelection,
    pagination_error: Option<String>,
    /// The rootlist folder tree fetched by the producer task (async, off
    /// the fold); `None` falls back to the flat list. Streaming-only: the
    /// slim build has no rootlist source, so the field vanishes there.
    folder_nodes: FolderNodes,
  },
  /// Apply the full-playlist sort: reads the live sort state + active-table
  /// check, replaces the visible rows when current, releases the slot.
  ApplySortedPlaylistTracks {
    playlist_id: String,
    tracks: Vec<rspotify::model::track::FullTrack>,
  },
  /// The live saved-tracks cache generation (the page-1 handler stamps its
  /// fetch with it; the fold invalidates against it).
  SavedTracksGeneration,
  /// Whether a Spotify session exists (the network layer's auth gate).
  SpotifyConnected,
  #[cfg(feature = "streaming")]
  NativePlayerState {
    player: std::sync::Arc<crate::infra::player::StreamingPlayer>,
  },
  #[cfg(feature = "streaming")]
  NativeRuntimeVolume,
  /// Claim a playlist-tracks page offset for fetching (the old
  /// `playlist_tracks_prefetch_in_flight.insert(offset)` RMW, combined with
  /// the generation + still-open checks into one atomic fold decision).
  ClaimPlaylistTracksOffset {
    playlist_id: String,
    offset: u32,
    generation: u64,
  },
  /// Read the current playlist-track table generation before an async page
  /// request.
  PlaylistTracksGeneration,
  /// Release a claimed playlist-tracks offset (fetch failed).
  ReleasePlaylistTracksOffset {
    playlist_id: String,
    offset: u32,
    generation: u64,
  },
  /// Claim or read the generation for saved albums/shows/artists. `reset`
  /// starts a fresh page-one request; pagination reads the current token.
  LibraryGeneration {
    section: crate::core::catalog::LibrarySection,
  },
  /// The saved-tracks view state the `GetCurrentSavedTracks` handler needs
  /// to decide what to do with a fresh page 1: is the table already
  /// showing this section, and which offset should the user's pending
  /// selection apply to. Replaces the lock-and-branch the handler did.
  SavedTracksViewInfo,
  // --- Ticket 17 (last readers): command-surface reads of published state.
  // The CLI and DJ tools stopped locking App; these name the exact decision
  // each read feeds, per the FoldQuery protocol rule above.
  /// The current playback context (CLI toggle/seek/mark/status/share).
  PlaybackContext,
  /// The device list (CLI device pick + transfer).
  Devices,
  /// The sidebar playlist listing (CLI `list playlists`).
  Playlists,
  /// The visible track-table rows (CLI `list liked`).
  TableTracks,
  /// Resolve a YouTube browse row for a playlist mutation.
  #[cfg(feature = "youtube")]
  YouTubeTrack(String),
  /// The latest catalog search results (CLI `play`/`search`).
  SearchResults,
  /// The last surfaced API error text (CLI decides success vs. failure).
  LastError,
  /// Force the pending state save out to disk now (CLI end-of-command).
  FlushStateSave,
  /// The source-agnostic playback snapshot (DJ now-playing, window title).
  MediaSnapshot,
  /// Atomic history collector read: playback snapshot and sync token/config.
  HistoryState,
  /// The native queue's depth (DJ now-playing payload).
  QueueDepth,
  /// The native queue's tracks, in order (DJ `get_queue`).
  QueueTracks,
  /// The DJ generation counter (the agent turn loop's adopt-one-bump rule).
  #[cfg(feature = "dj-core")]
  DjGeneration,
  /// The standing DJ vibe and whether auto-queue is on (vibe-effect text).
  #[cfg(feature = "dj-core")]
  DjVibe,
  /// Immutable DJ turn inputs sampled by the fold before a worker starts.
  #[cfg(feature = "dj-core")]
  DjContext,
  /// The announcement-surface sources: whether the feature is enabled, the
  /// configured feed URL, and the seen-announcement dedup set.
  AnnouncementSources { seen_ids: bool },
  // --- Ticket 17 (playback family): native-shuffle claims. Each names the
  // decision it feeds; each is a read-and-write the caller must not race.
  /// Is the visible track table standing in for Liked Songs? Decides whether
  /// a raw URI list (search/recommendation vs. library) is shuffleable.
  #[cfg(feature = "streaming")]
  TrackTableIsSavedTracks,
  /// Install an app-owned native-shuffle session: the fold stamps the fresh
  /// generation and applies the park/watchdog/context writes in one atomic
  /// step (the caller's next action — loading Spirc — must not race a
  /// replacement session). Answers with the generation (which gates the
  /// background full-context fetch) plus the player/seek/play state sampled
  /// at install time for the reload.
  #[cfg(feature = "streaming")]
  ClaimNativeShuffleInstall {
    install: NativeShuffleInstall,
    park: Option<StartPlaybackPark>,
    arm_watchdog: bool,
    mark_playing: bool,
  },
  /// Apply a shuffle toggle to the app-owned session (reorder + flip the
  /// flag + set the pending reload) and return what the caller should do.
  #[cfg(feature = "streaming")]
  ClaimNativeShuffleToggle { on: bool },
  /// Re-randomize the session's order for a repeat-all lap (current track
  /// first) and return the reload it implies.
  #[cfg(feature = "streaming")]
  ClaimNativeShuffleLapReshuffle,
  /// Apply a queue-drain resume against the stored session generation (a
  /// stale resume must not touch a replaced session) and return the reload
  /// or the "queue finished" outcome the fold already reported.
  #[cfg(feature = "streaming")]
  ClaimNativeShuffleResume {
    resume_index: Option<usize>,
    generation: u64,
  },
  /// Fold a completed full-context fetch into the live session (merge,
  /// suspend remap, failure fallback — the whole old lock section) and
  /// return the reload it implies, if any.
  #[cfg(feature = "streaming")]
  FoldNativeShuffleFullFetch {
    generation: u64,
    result: Result<(Vec<String>, bool), String>,
  },
  // --- Ticket 17 (playback family): DJ auto-queue claims.
  /// The dedupe keys the DJ's suggestion gate skips against (queued, playing,
  /// recently played).
  #[cfg(feature = "dj-core")]
  DjSkipKeys,
  /// Claim the playlist crawl that backs `exclude_owned` (the old
  /// `library_indexing` check-and-flip): `true` when the claim was free and
  /// is now held.
  #[cfg(feature = "dj-core")]
  ClaimDjLibraryIndex,
  /// Is a playlist crawl already done or in flight? The pre-dispatch dedup
  /// check (`request_library_index`) reads this instead of claiming — the
  /// dispatched crawl claims on its own when it runs.
  #[cfg(feature = "dj-core")]
  DjLibraryIndexPending,
  /// The cached avoid-library index, if the crawl finished. Decides whether
  /// ownership checks are complete and carries the crawled id set.
  #[cfg(feature = "dj-core")]
  DjLibrary,
  /// Queue these tracks through `App::apply` and answer with how many the
  /// queue engine accepted (the DJ's report must say, atomically with the
  /// queueing).
  #[cfg(feature = "dj-core")]
  ApplyDjQueueTracks {
    tracks: Vec<crate::core::plugin_api::TrackInfo>,
  },
  /// Build the agent brain from the live user config (the turn loop needs it
  /// without locking App).
  #[cfg(feature = "ai-dj")]
  DjBrain,
  /// Is this auto-queue top-up stale, releasing its progress flag if so (the
  /// old check-and-`finish_turn` bail — a bail that skipped the release would
  /// wedge auto-queue for the rest of the session)?
  #[cfg(feature = "ai-dj")]
  ClaimDjTopUp { generation: u64, turn_seq: u64 },
  // --- Ticket 17 (playback family): transport-command claims and reads.
  /// Can a native backend claim a failed player command (available player, or
  /// a backend/activation still materializing)? Decides the saved-device
  /// retry gate.
  #[cfg(feature = "streaming")]
  NativeCanClaimPlayerCommand,
  /// Was the NO_ACTIVE_DEVICE error expected noise while a native backend
  /// materializes, showing the reconnecting notice if so? The producer
  /// classifies the error text (no lock needed); the fold runs the
  /// check-and-notice, so the pending flag is not split from the status it
  /// gates.
  #[cfg(feature = "streaming")]
  ClaimNativeReconnectingStatus { is_no_active_device: bool },
  /// Was the 403 restriction violation de-escalated while the native session
  /// is still settling, showing the settling notice if so? The producer
  /// classifies the error text; the fold runs the check-and-notice.
  #[cfg(feature = "streaming")]
  ClaimNativeSettlingStatus { is_restriction_violated: bool },
  /// Is native streaming the active playback device per the context/heuristic
  /// rules? (The fold runs the same logic the producer's read used.)
  #[cfg(feature = "streaming")]
  IsNativeStreamingActiveForPlayback,
  /// Resolve the transport backend for a symmetric playback operation: the
  /// native player handle when native is selected, `None` for the Web API.
  #[cfg(feature = "streaming")]
  SymmetricPlaybackBackend,
  /// Resolve the transport backend for `start_playback` (native is selected
  /// when already active *or* when the activation heuristics say so). The
  /// saved device id feeds the activation heuristic (it lives on the
  /// producer's client config).
  #[cfg(feature = "streaming")]
  StartPlaybackBackend { saved_device_id: Option<String> },
  /// Resolve the transport backend for a device transfer: native only when
  /// the target `device_id` refers to the native streaming device.
  #[cfg(feature = "streaming")]
  TransferPlaybackBackend { device_id: String },
  /// Claim a native streaming recovery if the player is disconnected (the old
  /// `request_native_streaming_recovery_if_disconnected` check-and-set).
  #[cfg(feature = "streaming")]
  ClaimNativeRecoveryIfDisconnected,
  /// The requested playback origin for a bare resume (no context, no uris):
  /// the recorded origin, else derived from the current context.
  #[cfg(feature = "streaming")]
  NativePlaybackOriginFallback,
  // --- Ticket 17 (playback family): start_playback claims and reads.
  /// The shuffle state a new playback should carry (the context's, else the
  /// persisted preference — the old pre-start read of `start_playback`).
  DesiredShuffleState,
  /// The recorded native device id, if any (`resolve_native_playback_route`).
  #[cfg(feature = "streaming")]
  NativeDeviceId,
  /// Compute the transfer decision and device-preference verdict from live
  /// state before the producer transfers and activates the player.
  #[cfg(feature = "streaming")]
  NativePlaybackActivationDecision(NativePlaybackActivation),
  /// Is there local playback state a bare resume can ride (the old
  /// `can_resume_direct_native` read: native metadata or a known last track)?
  #[cfg(feature = "streaming")]
  NativeResumeDirectPossible,
  /// Does a parked StartPlayback replay with recovery attempts behind it
  /// still describe this exact request? Decides the API-context retry route
  /// (a direct load of the same request already failed silently once).
  #[cfg(feature = "streaming")]
  StartPlaybackReplayMatches {
    context_uri: Option<String>,
    uris: Option<Vec<String>>,
    offset: Option<usize>,
  },
  /// Park a failed StartPlayback while a native backend is still materializing
  /// (the `native_backend_pending` check-and-park): `true` when the fold
  /// parked it and surfaced the reconnecting notice — the producer returns
  /// instead of surfacing the error.
  #[cfg(feature = "streaming")]
  ClaimNativeBackendPendingPark {
    context_uri: Option<String>,
    uris: Option<Vec<String>>,
    offset: Option<usize>,
  },
  /// Snapshot the fields a playback poll needs before its HTTP round trip.
  PlaybackPollState,
  /// Claim a native restore generation and return its player/snapshot.
  #[cfg(feature = "streaming")]
  ClaimNativeRestore { generation: u64 },
  /// Native idle reconciliation inputs (device cache + recovery state).
  #[cfg(feature = "streaming")]
  NativeIdleDeviceState {
    saved_device_id: Option<String>,
    native_device_id: String,
    native_device_name: String,
  },
  /// Auto-select's check-and-decide step.
  #[cfg(feature = "streaming")]
  AutoSelectDecision {
    native_device_id: String,
    native_device_name: String,
    saved_device_id: Option<String>,
    persist_device_id: bool,
    yield_to_external_playback: bool,
  },
  /// Native continuation's check-and-read step.
  #[cfg(feature = "streaming")]
  NativeContinuationState { previous_track_id: String },
}

/// The answer side of [`FoldQuery`]. Kept as a plain enum so the fold can
/// match exhaustively and producers cannot mis-cast.
/// Debug is manual: `StreamingPlayer` has no Debug.
pub enum FoldAnswer {
  Decoded(DecodedSourceAnswer),
  Bool(bool),
  U64(u64),
  Offset(Option<u32>),
  Liked(Vec<bool>),
  User(Option<String>),
  /// A claimed liked-lookup batch plus the epoch it was captured under.
  LikedBatch(Option<(Vec<String>, u64)>),
  /// The current playback context (or `None` when nothing is playing).
  Playback(Option<Box<rspotify::model::context::CurrentPlaybackContext>>),
  /// The device list (or `None` before the first fetch).
  Devices(Option<DeviceList>),
  /// The sidebar playlist listing.
  Playlists(Option<crate::core::pagination::Paged<crate::core::plugin_api::PlaylistInfo>>),
  /// The visible track-table rows.
  Tracks(Vec<crate::core::plugin_api::TrackInfo>),
  /// A YouTube browse row resolved by source-owned identity.
  #[cfg(feature = "youtube")]
  Track(Option<crate::core::plugin_api::TrackInfo>),
  /// The latest catalog search results.
  Search(Box<crate::core::app::SearchResult>),
  /// The source-agnostic playback snapshot (or `None` when nothing plays).
  Media(Option<crate::infra::media_metadata::PlaybackSnapshot>),
  HistoryState(HistoryQueryState),
  /// The native queue's tracks, in order.
  QueueTracks(Vec<crate::core::plugin_api::TrackInfo>),
  #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
  QueueDecodedPlayer(Option<Arc<crate::infra::audio::LocalPlayer>>),
  #[cfg(feature = "streaming")]
  NativePlayerState(NativePlayerState),
  #[cfg(feature = "streaming")]
  NativeRuntimeVolume(u8),
  /// The standing DJ vibe and whether the auto-queue DJ is on.
  #[cfg(feature = "dj-core")]
  DjVibe {
    vibe: Option<String>,
    auto_queue: bool,
  },
  /// Conversation/configuration inputs for one DJ worker turn.
  #[cfg(feature = "dj-core")]
  DjContext {
    period: crate::infra::history::RecapPeriod,
    want: usize,
    transcript: Vec<crate::infra::dj::DjLine>,
    avoid_library: bool,
  },
  /// The announcement-surface answer (feed config + optional dedup set).
  AnnouncementSources {
    enabled: bool,
    feed_url: Option<String>,
    seen_ids: Option<Vec<String>>,
  },
  /// Status text a fold query computed (the add-to-playlist ack).
  StatusText(String),
  /// The registration result for a playlist refresh (the page-1 install).
  PlaylistRefreshRegistered {
    /// The freshly bumped refresh generation.
    generation: u64,
    /// The selection as of before the refresh (for the background task).
    original_selection: crate::core::catalog::PlaylistSelection,
    /// The selection after page 1 landed (the background task compares it
    /// to decide whether the user has navigated since).
    page_one_selection: crate::core::catalog::PlaylistSelection,
    /// The previous complete list, if pagination fails partway.
    previous_complete: PlaylistRefreshPrevious,
  },
  /// A cloned handle to the live streaming player (rootlist query).
  #[cfg(feature = "streaming")]
  StreamingPlayer(Option<std::sync::Arc<crate::infra::player::StreamingPlayer>>),
  /// A clone of the App's IoEvent sender (`None` when the channel is gone).
  IoTx(Option<std::sync::mpsc::Sender<crate::infra::network::IoEvent>>),
  /// The native-shuffle install outcome: the stamped generation plus the
  /// player/seek/play state sampled at install time (`None` when no player
  /// is live).
  #[cfg(feature = "streaming")]
  NativeShuffleInstall {
    generation: u64,
    reload: Option<Box<NativeShuffleReloadContext>>,
  },
  /// The native-shuffle toggle decision (built/spirc-fallback/reload/no-op).
  #[cfg(feature = "streaming")]
  NativeShuffleToggle(Box<NativeShuffleToggleDecision>),
  /// A prepared session reload, or `None` when the fold decided there is
  /// nothing to reload (lap reshuffle denied, stale resume, no session).
  #[cfg(feature = "streaming")]
  NativeShuffleReload(Option<Box<NativeShuffleReload>>),
  /// A set of strings (DJ skip keys).
  #[cfg(feature = "dj-core")]
  Strings(Vec<String>),
  /// The avoid-library index, if the crawl finished.
  #[cfg(feature = "dj-core")]
  DjLibrary(Option<Box<crate::infra::dj::DjLibrary>>),
  /// The configured agent brain, `Err` carrying the config error.
  #[cfg(feature = "ai-dj")]
  DjBrain(Result<Box<crate::infra::dj::brain::DjBrain>, String>),
  /// The playback origin a bare resume should record.
  #[cfg(feature = "streaming")]
  NativeOrigin(crate::core::app::NativePlaybackOrigin),
  /// The recorded native device id, if any.
  #[cfg(feature = "streaming")]
  NativeDeviceId(Option<String>),
  /// The activation decision produced by the fold.
  #[cfg(feature = "streaming")]
  NativeActivation(NativeActivationDecision),
  /// Playback poll snapshot.
  PlaybackPoll(Box<crate::infra::network::playback::PlaybackPollState>),
  #[cfg(feature = "streaming")]
  NativeRestore(Option<Box<crate::infra::network::playback::NativeRestoreClaim>>),
  #[cfg(feature = "streaming")]
  NativeIdleDevice {
    saved_device_matches_native: bool,
  },
  #[cfg(feature = "streaming")]
  AutoSelect {
    should_transfer: bool,
    external_playback_active: bool,
    preference: NativeDevicePreferenceUpdate,
  },
  #[cfg(feature = "streaming")]
  NativeContinuation {
    transition_advanced: bool,
    raw_next: Option<crate::core::app::PendingStartPlayback>,
    raw_list_exhausted: bool,
    native_device_id: Option<String>,
  },
}

/// Published state needed by the listening-history producer for one tick.
#[derive(Debug, Clone)]
pub struct HistoryQueryState {
  pub playback: Option<crate::infra::media_metadata::PlaybackSnapshot>,
  pub sync_token: Option<String>,
  pub enable_monthly_recap_prompt: bool,
}

impl std::fmt::Debug for FoldAnswer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      FoldAnswer::Decoded(_) => f.write_str("Decoded"),
      FoldAnswer::Bool(b) => f.debug_tuple("Bool").field(b).finish(),
      FoldAnswer::U64(v) => f.debug_tuple("U64").field(v).finish(),
      FoldAnswer::Offset(v) => f.debug_tuple("Offset").field(v).finish(),
      FoldAnswer::Liked(v) => f.debug_tuple("Liked").field(v).finish(),
      FoldAnswer::User(v) => f.debug_tuple("User").field(v).finish(),
      FoldAnswer::LikedBatch(v) => f.debug_tuple("LikedBatch").field(v).finish(),
      FoldAnswer::Playback(v) => f.debug_tuple("Playback").field(&v.is_some()).finish(),
      FoldAnswer::Devices(v) => f.debug_tuple("Devices").field(&v.is_some()).finish(),
      FoldAnswer::Playlists(v) => f.debug_tuple("Playlists").field(&v.is_some()).finish(),
      FoldAnswer::Tracks(v) => f.debug_tuple("Tracks").field(&v.len()).finish(),
      #[cfg(feature = "youtube")]
      FoldAnswer::Track(v) => f.debug_tuple("Track").field(&v.is_some()).finish(),
      FoldAnswer::Search(v) => f.debug_tuple("Search").field(v).finish(),
      FoldAnswer::Media(v) => f.debug_tuple("Media").field(&v.is_some()).finish(),
      FoldAnswer::HistoryState(v) => f
        .debug_struct("HistoryState")
        .field("playback", &v.playback.is_some())
        .field("sync_token", &v.sync_token.is_some())
        .field("recap", &v.enable_monthly_recap_prompt)
        .finish(),
      FoldAnswer::QueueTracks(v) => f.debug_tuple("QueueTracks").field(&v.len()).finish(),
      #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
      FoldAnswer::QueueDecodedPlayer(v) => f
        .debug_tuple("QueueDecodedPlayer")
        .field(&v.is_some())
        .finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativePlayerState(v) => f.debug_tuple("NativePlayerState").field(v).finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeRuntimeVolume(v) => f.debug_tuple("NativeRuntimeVolume").field(v).finish(),
      #[cfg(feature = "dj-core")]
      FoldAnswer::DjVibe { vibe, auto_queue } => f
        .debug_struct("DjVibe")
        .field("vibe", vibe)
        .field("auto_queue", auto_queue)
        .finish(),
      #[cfg(feature = "dj-core")]
      FoldAnswer::DjContext { period, want, transcript, avoid_library } => f
        .debug_struct("DjContext")
        .field("period", period)
        .field("want", want)
        .field("transcript_lines", &transcript.len())
        .field("avoid_library", avoid_library)
        .finish(),
      FoldAnswer::AnnouncementSources {
        enabled,
        feed_url: _,
        seen_ids,
      } => f
        .debug_struct("AnnouncementSources")
        .field("enabled", enabled)
        .field("seen_ids", &seen_ids.is_some())
        .finish_non_exhaustive(),
      FoldAnswer::StatusText(v) => f.debug_tuple("StatusText").field(v).finish(),
      FoldAnswer::PlaylistRefreshRegistered { generation, .. } => f
        .debug_struct("PlaylistRefreshRegistered")
        .field("generation", generation)
        .finish_non_exhaustive(),
      #[cfg(feature = "streaming")]
      FoldAnswer::StreamingPlayer(player) => f
        .debug_tuple("StreamingPlayer")
        .field(&player.is_some())
        .finish(),
      FoldAnswer::IoTx(io_tx) => f.debug_tuple("IoTx").field(&io_tx.is_some()).finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeOrigin(origin) => f.debug_tuple("NativeOrigin").field(origin).finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeDeviceId(id) => f.debug_tuple("NativeDeviceId").field(id).finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeActivation(decision) => f
        .debug_struct("NativeActivation")
        .field("should_transfer", &decision.should_transfer)
        .field("preference", &decision.preference)
        .finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeShuffleInstall { generation, reload } => f
        .debug_struct("NativeShuffleInstall")
        .field("generation", generation)
        .field("reload", &reload.is_some())
        .finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeShuffleToggle(_) => f.write_str("NativeShuffleToggle"),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeShuffleReload(v) => f
        .debug_tuple("NativeShuffleReload")
        .field(&v.is_some())
        .finish(),
      #[cfg(feature = "dj-core")]
      FoldAnswer::Strings(v) => f.debug_tuple("Strings").field(&v.len()).finish(),
      #[cfg(feature = "dj-core")]
      FoldAnswer::DjLibrary(v) => f.debug_tuple("DjLibrary").field(&v.is_some()).finish(),
      #[cfg(feature = "ai-dj")]
      FoldAnswer::DjBrain(v) => f.debug_tuple("DjBrain").field(&v.is_ok()).finish(),
      FoldAnswer::PlaybackPoll(_) => f.write_str("PlaybackPoll"),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeRestore(v) => f.debug_tuple("NativeRestore").field(&v.is_some()).finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeIdleDevice {
        saved_device_matches_native,
      } => f
        .debug_tuple("NativeIdleDevice")
        .field(saved_device_matches_native)
        .finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::AutoSelect {
        should_transfer,
        external_playback_active,
        ..
      } => f
        .debug_struct("AutoSelect")
        .field("should_transfer", should_transfer)
        .field("external", external_playback_active)
        .finish(),
      #[cfg(feature = "streaming")]
      FoldAnswer::NativeContinuation {
        transition_advanced,
        raw_next,
        raw_list_exhausted,
        native_device_id,
      } => f
        .debug_struct("NativeContinuation")
        .field("advanced", transition_advanced)
        .field("next", &raw_next.is_some())
        .field("exhausted", raw_list_exhausted)
        .field("device", &native_device_id)
        .finish(),
    }
  }
}

/// The rootlist folder tree payload: `Some` under `streaming`, `None` on
/// slim legs (no field plumbing there).
#[cfg(feature = "streaming")]
pub type FolderNodes = Option<Vec<crate::core::app::PlaylistFolderNode>>;
#[cfg(not(feature = "streaming"))]
pub type FolderNodes = ();

/// The previous complete playlist list, snapshotted by the fold when
/// background pagination may fail partway.
#[derive(Debug, Default)]
pub struct PlaylistRefreshPrevious {
  pub playlists: Vec<crate::core::plugin_api::PlaylistInfo>,
  pub folder_nodes: Option<Vec<crate::core::app::PlaylistFolderNode>>,
  pub folder_items: Vec<crate::core::app::PlaylistFolderItem>,
}

/// Fold-published device summary (ticket 17): rspotify's `DevicePayload`
/// carries no `Clone`, so the fold answers with the exact fields command
/// surfaces consume (name, id, volume).
#[derive(Debug, Clone, Default)]
pub struct DeviceList {
  pub devices: Vec<DeviceSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct DeviceSummary {
  pub id: Option<String>,
  pub name: String,
  pub volume_percent: Option<u32>,
}

impl From<&rspotify::model::device::DevicePayload> for DeviceList {
  fn from(payload: &rspotify::model::device::DevicePayload) -> Self {
    DeviceList {
      devices: payload
        .devices
        .iter()
        .map(|d| DeviceSummary {
          id: d.id.clone(),
          name: d.name.clone(),
          volume_percent: d.volume_percent,
        })
        .collect(),
    }
  }
}

// --- Ticket 17 (playback family): native-shuffle session claims. All
// streaming-only — the slim build has no native player.

/// The session fields a native-shuffle install writes (the generation is
/// stamped by the fold, atomically with the install).
#[cfg(feature = "streaming")]
#[derive(Debug)]
pub struct NativeShuffleInstall {
  pub order: Vec<String>,
  pub original: Vec<String>,
  pub index: usize,
  pub shuffled: bool,
  pub fetch_complete: bool,
  pub pending_reload_index: Option<usize>,
}

/// The `park_start_playback` payload a start-path install parks alongside the
/// session, so zombie-session recovery replays the original request.
#[cfg(feature = "streaming")]
#[derive(Debug)]
pub struct StartPlaybackPark {
  pub context_uri: Option<String>,
  pub uris: Option<Vec<String>>,
  pub offset: Option<usize>,
}

// --- Ticket 17 (playback family): start_playback activation + load facts.
// Streaming-only — the slim build never routes playback natively.

/// The request payload `record_native_playback_request` writes for the
/// recovery snapshot (the repeat state is read fold-side from the context at
/// apply time, exactly where the old inline block read it).
#[cfg(feature = "streaming")]
#[derive(Clone, Debug)]
pub struct NativePlaybackRequestSpec {
  pub context_uri: Option<String>,
  pub uris: Option<Vec<String>>,
  pub offset: Option<usize>,
  pub desired_playing: bool,
  pub shuffle: bool,
}

#[cfg(feature = "streaming")]
#[derive(Debug, Clone, Copy)]
pub struct NativePlayerState {
  pub current: bool,
  pub desired_playing: bool,
  pub queue_suspended: bool,
  pub decoded_queue_slot: bool,
  pub queue_now_is_spotify: bool,
  pub volume_percent: u8,
}

/// Whether a native playback activation should persist the native device id
/// into the client config (`Persist`), or leave the user's saved device alone
/// (`KeepExistingPreference`). Lives here because the producer (client
/// config) and the fold (App) each need half of the decision.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeDevicePreferenceUpdate {
  Persist,
  KeepExistingPreference,
}

/// The native-playback activation payload. A query first computes the
/// producer-owned transfer/config decision; after `player.activate()`, a fact
/// applies the App-owned mirrors and recovery intent.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug)]
pub struct NativePlaybackActivation {
  pub native_device_id: String,
  pub native_device_name: String,
  /// The Web API client's saved device id (lives on the producer).
  pub saved_device_id: Option<String>,
  pub explicit_persist: bool,
  pub origin: Option<crate::core::app::NativePlaybackOrigin>,
  pub activation_time: std::time::Instant,
  /// Backdate the poll clock (`activation_time - 6s`) so the next poll runs
  /// immediately — the paths that hand playback to the player without a
  /// context to confirm.
  pub backdate_poll: bool,
  /// Drop the previous device's playback context (the transfer path).
  pub clear_context: bool,
  /// A bare resume (no context, no uris): record play intent only.
  pub bare_resume: bool,
  /// A concrete playback request: record it for recovery (repeat read
  /// fold-side from the context).
  pub request: Option<NativePlaybackRequestSpec>,
}

/// What [`FoldQuery::NativePlaybackActivationDecision`] answers back.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
pub struct NativeActivationDecision {
  /// Move the player to this device before it takes playback.
  pub should_transfer: bool,
  /// The device-preference verdict the producer applies to its client config.
  pub preference: NativeDevicePreferenceUpdate,
}

/// Player state sampled at native-shuffle install time for the reload the
/// caller drives next.
#[cfg(feature = "streaming")]
pub struct NativeShuffleReloadContext {
  pub player: std::sync::Arc<crate::infra::player::StreamingPlayer>,
  pub seek_ms: u32,
  pub is_playing: bool,
}

/// A session reload the fold has fully prepared: everything
/// `load_session_tracks` needs, sampled atomically with the session mutation
/// that requested it.
#[cfg(feature = "streaming")]
pub struct NativeShuffleReload {
  pub player: std::sync::Arc<crate::infra::player::StreamingPlayer>,
  pub order: Vec<String>,
  pub index: usize,
  pub seek_ms: u32,
  pub is_playing: bool,
}

/// What a native-shuffle toggle should do, decided and applied atomically by
/// the fold (the old inline `Action` of `toggle_native_shuffle_session`).
#[cfg(feature = "streaming")]
pub enum NativeShuffleToggleDecision {
  /// The app-owned session was reordered; reload it.
  Reload(NativeShuffleReload),
  /// No session covers this; build one around the current context.
  Build {
    context_uri: String,
    current_uri: String,
  },
  /// Fall back to Spirc's own shuffle on this player.
  Spirc(std::sync::Arc<crate::infra::player::StreamingPlayer>),
  Nothing,
}

/// The result of an acked emit: the fold has *applied* the event, and hands
/// back what the producer used to read after its own write. Today: the next
/// missing offset for both offset-keyed library caches (`None` when the
/// event was dropped as stale or the cache is complete).
#[derive(Debug, Clone, Default)]
pub struct FoldAck {
  /// Generation assigned when a catalog request is accepted.
  pub catalog_generation: Option<u64>,
  /// Generation allocated for a saved-library refresh.
  pub library_generation: Option<u64>,
  /// Generation allocated for a metadata navigation request.
  pub metadata_generation: Option<u64>,
  /// The next missing saved-tracks offset after the folded upsert.
  pub next_saved_tracks_offset: Option<u32>,
  /// The next missing playlist-tracks offset after the folded upsert.
  pub next_playlist_tracks_offset: Option<u32>,
  /// Synchronous result of a folded external action.
  pub action_outcome: Option<crate::core::action::ActionOutcome>,
  /// Native fact result used only for decisions that must happen after the
  /// fold's generation/pointer guard (e.g. queue continuation).
  #[cfg(feature = "streaming")]
  pub native: Option<NativePlaybackAck>,
  /// Track atomically removed by a native-queue pop fact.
  #[cfg(feature = "streaming")]
  pub queue_track: Option<crate::core::plugin_api::TrackInfo>,
  /// Handles removed by an ordered backend-ownership claim.
  #[cfg(any(
    feature = "local-files",
    feature = "subsonic",
    feature = "youtube",
    feature = "internet-radio"
  ))]
  pub backend_players: Option<ClaimedBackendPlayers>,
  /// Result of an ordered queue worker claim (the player and volume snapshot).
  #[cfg(any(feature = "subsonic", feature = "youtube"))]
  pub queue_fetch_claim: Option<QueueFetchClaim>,
  /// Result of an ordered suspended-context take. `None` means this ack was
  /// for another fact; `Some(None)` means no context was suspended.
  #[cfg(any(
    feature = "streaming",
    feature = "local-files",
    feature = "subsonic",
    feature = "youtube",
    feature = "internet-radio"
  ))]
  pub queue_suspended: Option<Option<crate::core::queue::SuspendedContext>>,
}

#[cfg(feature = "streaming")]
#[derive(Clone)]
pub enum NativePlaybackAck {
  Applied(bool),
  Playing {
    current: bool,
  },
  Paused {
    current: bool,
  },
  TrackChanged {
    current: bool,
    reload_uri: Option<(String, bool)>,
    transition_advanced: bool,
  },
  EndOfTrack {
    current: bool,
    should_ensure_playback: bool,
    stop_after_current: bool,
    reissue_queue_slot: Option<(String, bool)>,
  },
  Disconnect(Option<crate::infra::player::StreamingRecoveryRequest>),
  Installed {
    old: Option<Arc<crate::infra::player::StreamingPlayer>>,
    accepted: bool,
  },
  RecoveryFailed(bool),
  RecoveryRoute(bool),
}

#[cfg(feature = "streaming")]
impl std::fmt::Debug for NativePlaybackAck {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let name = match self {
      Self::Applied(_) => "Applied",
      Self::Playing { .. } => "Playing",
      Self::Paused { .. } => "Paused",
      Self::TrackChanged { .. } => "TrackChanged",
      Self::EndOfTrack { .. } => "EndOfTrack",
      Self::Disconnect(_) => "Disconnect",
      Self::Installed { .. } => "Installed",
      Self::RecoveryFailed(_) => "RecoveryFailed",
      Self::RecoveryRoute(_) => "RecoveryRoute",
    };
    f.write_str(name)
  }
}

#[cfg(any(
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube",
  feature = "internet-radio"
))]
#[derive(Clone)]
pub struct ClaimedBackendPlayers {
  #[cfg(feature = "streaming")]
  pub streaming: Option<Arc<crate::infra::player::StreamingPlayer>>,
  #[cfg(feature = "local-files")]
  pub local: Option<Arc<crate::infra::audio::LocalPlayer>>,
  #[cfg(feature = "subsonic")]
  pub subsonic: Option<Arc<crate::infra::audio::LocalPlayer>>,
  #[cfg(feature = "internet-radio")]
  pub radio: Option<Arc<crate::infra::audio::LocalPlayer>>,
  #[cfg(feature = "youtube")]
  pub youtube: Option<Arc<crate::infra::audio::LocalPlayer>>,
}

#[cfg(any(
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube",
  feature = "internet-radio"
))]
impl std::fmt::Debug for ClaimedBackendPlayers {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ClaimedBackendPlayers")
      .finish_non_exhaustive()
  }
}

#[cfg(any(feature = "subsonic", feature = "youtube"))]
#[derive(Clone)]
pub struct QueueFetchClaim {
  pub player: Arc<crate::infra::audio::LocalPlayer>,
  pub volume: u8,
  pub lease: Arc<QueueFetchLease>,
}

#[cfg(any(feature = "subsonic", feature = "youtube"))]
impl std::fmt::Debug for QueueFetchClaim {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("QueueFetchClaim")
      .field("volume", &self.volume)
      .finish_non_exhaustive()
  }
}

#[cfg(any(feature = "streaming", feature = "subsonic", feature = "youtube"))]
pub struct QueueFetchLease {
  gate: std::sync::Mutex<()>,
  valid: std::sync::atomic::AtomicBool,
}

#[cfg(any(feature = "streaming", feature = "subsonic", feature = "youtube"))]
impl QueueFetchLease {
  pub fn new() -> Self {
    Self {
      gate: std::sync::Mutex::new(()),
      valid: std::sync::atomic::AtomicBool::new(true),
    }
  }

  pub fn revoke(&self) {
    let _guard = self.gate.lock().expect("queue lease gate poisoned");
    self
      .valid
      .store(false, std::sync::atomic::Ordering::Release);
  }

  /// Run the sink-start mutation only while this fetch still owns its slot.
  /// The callback must stay tiny: decoding happens before this gate is taken.
  pub(crate) fn with_valid<T>(&self, f: impl FnOnce() -> T) -> Option<T> {
    let _guard = self.gate.lock().expect("queue lease gate poisoned");
    if !self.valid.load(std::sync::atomic::Ordering::Acquire) {
      return None;
    }
    Some(f())
  }

  #[cfg(any(feature = "subsonic", feature = "youtube"))]
  pub fn play_file(
    &self,
    player: &crate::infra::audio::LocalPlayer,
    path: &std::path::Path,
  ) -> anyhow::Result<()> {
    let decoder = match crate::infra::audio::LocalPlayer::decode_file(path) {
      Ok(decoder) => decoder,
      Err(error) => {
        // Preserve LocalPlayer's failure semantics without clearing a sink
        // after teardown has revoked this worker.
        let _ = self.with_valid(|| player.stop());
        return Err(error);
      }
    };
    self
      .with_valid(|| {
        player.play_decoded(decoder);
      })
      .ok_or_else(|| anyhow::anyhow!("queue fetch lease revoked"))
  }
}

/// Channel bounds: the engine never blocks the window on a full channel, but
/// the bounds keep a misbehaving producer from buffering unbounded copies.
#[derive(Debug, Clone, Copy)]
pub struct SpineBounds {
  /// Events buffered before slow producers start overwriting policy.
  pub event_capacity: usize,
}

impl Default for SpineBounds {
  fn default() -> Self {
    SpineBounds {
      event_capacity: 256,
    }
  }
}

/// The send half every producer holds, plus the receive half the fold task
/// consumes. Cheap to clone (`mpsc::Sender`).
#[derive(Debug, Clone)]
pub struct Spine {
  events_tx: mpsc::Sender<SpineMessage>,
  queries_tx: mpsc::Sender<(FoldQuery, oneshot::Sender<Result<FoldAnswer, SpineClosed>>)>,
  activity_tx: watch::Sender<u64>,
}

/// One FIFO envelope consumed by the fold. Acked commands share this queue
/// with ordinary facts, so an acknowledgement can never overtake an earlier
/// worker completion.
pub(crate) enum SpineMessage {
  Event(EngineEvent),
  Ack {
    event: EngineEvent,
    responder: AckSender,
  },
}

/// The query channel is closed: the fold task is gone (boot order or
/// shutdown). Producers treat this as "no answer; skip the fetch" — the
/// same stance as a `None` spine in tests.
#[derive(Debug)]
pub struct SpineClosed;

impl std::fmt::Display for SpineClosed {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str("engine fold is closed")
  }
}
impl std::error::Error for SpineClosed {}

impl Spine {
  pub(crate) fn open(bounds: SpineBounds) -> (Spine, mpsc::Receiver<SpineMessage>, QueryReceiver) {
    let (events_tx, events_rx) = mpsc::channel(bounds.event_capacity);
    let (queries_tx, queries_rx) = mpsc::channel(bounds.event_capacity);
    let (activity_tx, _) = watch::channel(0u64);
    (
      Spine {
        events_tx,
        queries_tx,
        activity_tx,
      },
      events_rx,
      QueryReceiver {
        queries_rx: Mutex::new(Some(queries_rx)),
      },
    )
  }

  /// Push an event, waiting for bounded capacity so unique facts are not lost.
  pub async fn emit(&self, event: EngineEvent) {
    let _ = self.events_tx.send(SpineMessage::Event(event)).await;
  }
  /// Synchronous fire-and-forget for contexts holding no async reactor
  /// (blocking threads, drop paths). Same lossy-on-overflow contract.
  pub fn emit_now(&self, event: EngineEvent) {
    let _ = self.events_tx.try_send(SpineMessage::Event(event));
  }

  /// Emit one event and wait until the fold has applied it, receiving the
  /// post-fold facts the producer used to read under its own lock after the
  /// write (the prefetch loop's next-missing-offset read). The ack waits on
  /// the fold's queue order, so the answer reflects every event queued
  /// before this one — stream order is preserved, no lock needed.
  pub async fn emit_acked(&self, event: EngineEvent) -> Option<FoldAck> {
    let (tx, rx) = oneshot::channel();
    self
      .events_tx
      .send(SpineMessage::Ack {
        event,
        responder: tx,
      })
      .await
      .ok()?;
    rx.await.ok().flatten()
  }

  /// Subscribe to producer activity without receiving or consuming events.
  /// Observers use this as an event-driven wake source; no heartbeat is
  /// generated while the engine is idle.
  pub fn subscribe_activity(&self) -> watch::Receiver<u64> {
    self.activity_tx.subscribe()
  }

  /// Notify observers only after the fold has applied an event. Keeping this
  /// on the consumer side prevents observers from racing a queued fact and
  /// reading the previous state.
  pub(crate) fn signal_folded(&self) {
    self
      .activity_tx
      .send_modify(|generation| *generation = generation.wrapping_add(1));
  }

  /// Ask the fold for the state a decision needs, awaiting the answer.
  /// Errors only when the fold task is gone (boot ordering or shutdown).
  pub async fn query(&self, query: FoldQuery) -> Result<FoldAnswer, SpineClosed> {
    let (tx, rx) = oneshot::channel();
    self
      .queries_tx
      .send((query, tx))
      .await
      .map_err(|_| SpineClosed)?;
    rx.await.map_err(|_| SpineClosed)?
  }

  /// Resolve once the receiving end is up, for boot ordering asserts.
  pub async fn settled(&self, timeout: Duration) -> bool {
    self.events_tx.capacity() > 0 || timeout > Duration::ZERO
  }
}

/// The receive half of the query channel; the fold task drains it between
/// events. Interior mutability so the fold can hold it beside `events_rx`.
type QuerySender = oneshot::Sender<Result<FoldAnswer, SpineClosed>>;
type AckSender = oneshot::Sender<Option<FoldAck>>;

pub struct QueryReceiver {
  queries_rx: Mutex<Option<mpsc::Receiver<(FoldQuery, QuerySender)>>>,
}

impl QueryReceiver {
  /// Take the receive halves for the fold task. `async` because the CLI and
  /// spine-driven tests take these from inside a runtime (ticket 17); boot
  /// awaits it inside its `block_on`.
  pub async fn take(&self) -> Option<FoldHandles> {
    let queries = self
      .queries_rx
      .lock()
      .await
      .take()
      .map(|rx| FoldQueries { rx });
    Some(FoldHandles { queries: queries? })
  }
}

/// The fold's receive halves, taken once at boot by the fold task.
pub struct FoldHandles {
  pub queries: FoldQueries,
}

/// Drain helper: `recv().await` / `try_recv()` on the taken receiver.
pub struct FoldQueries {
  pub(crate) rx: mpsc::Receiver<(FoldQuery, oneshot::Sender<Result<FoldAnswer, SpineClosed>>)>,
}

/// Emit one catalog completion through the ordered producer spine.
pub(crate) async fn emit_catalog_event(
  spine: &Spine,
  event: crate::core::catalog::CatalogStateEvent,
) {
  spine.emit(EngineEvent::Catalog(event)).await;
}

/// Emit one decoded-source fact through the fold.
#[allow(dead_code)]
pub(crate) async fn emit_decoded_source(spine: &Spine, fact: DecodedSourceFact) {
  spine.emit(EngineEvent::Decoded(fact)).await;
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn emit_preserves_unique_facts_under_backpressure() {
    let (spine, mut events, _) = Spine::open(SpineBounds { event_capacity: 1 });
    spine
      .emit(EngineEvent::Session(SessionFact::Auth {
        transition: crate::core::auth::AuthTransition::Connected,
        new_expiry: None,
        post_connect_fetches: Vec::new(),
      }))
      .await;
    let pending = spine.emit(EngineEvent::Session(SessionFact::Auth {
      transition: crate::core::auth::AuthTransition::Disconnected,
      new_expiry: None,
      post_connect_fetches: Vec::new(),
    }));
    tokio::pin!(pending);

    assert!(
      tokio::time::timeout(Duration::from_millis(10), &mut pending)
        .await
        .is_err()
    );
    assert!(matches!(
      events.recv().await,
      Some(SpineMessage::Event(EngineEvent::Session(
        SessionFact::Auth { .. }
      )))
    ));
    tokio::time::timeout(Duration::from_secs(2), &mut pending)
      .await
      .expect("event should send after capacity is available");
    assert!(matches!(
      events.recv().await,
      Some(SpineMessage::Event(EngineEvent::Session(
        SessionFact::Auth { .. }
      )))
    ));
  }

  #[tokio::test]
  async fn idle_spine_has_no_periodic_activity_wake() {
    let (spine, _events, _) = Spine::open(Default::default());
    let mut activity = spine.subscribe_activity();
    assert!(
      tokio::time::timeout(Duration::from_millis(20), activity.changed())
        .await
        .is_err()
    );
  }

  #[tokio::test]
  async fn acked_events_share_fifo_with_worker_events() {
    let (spine, mut events, _) = Spine::open(Default::default());
    let first = EngineEvent::Session(SessionFact::Status {
      message: "worker-first".into(),
      ttl_secs: 1,
      is_error: false,
    });
    spine.emit_now(first);
    let ack_task = tokio::spawn({
      let spine = spine.clone();
      async move {
        spine
          .emit_acked(EngineEvent::Session(SessionFact::ApplyAction(
            crate::core::action::EngineAction::Notify("command-second".into(), 1),
          )))
          .await
      }
    });

    assert!(
      matches!(events.recv().await, Some(SpineMessage::Event(EngineEvent::Session(
      SessionFact::Status { message, .. }
    ))) if message == "worker-first")
    );
    let Some(SpineMessage::Ack { responder, .. }) = events.recv().await else {
      panic!("acked event should follow the worker event");
    };
    let _ = responder.send(None);
    assert!(ack_task.await.unwrap().is_none());
  }

  #[cfg(any(feature = "streaming", feature = "subsonic", feature = "youtube"))]
  #[test]
  fn queue_fetch_lease_serializes_start_and_teardown() {
    use std::sync::{mpsc, Arc, Barrier};

    let lease = Arc::new(QueueFetchLease::new());
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let started_lease = Arc::clone(&lease);
    let started_entered = Arc::clone(&entered);
    let started_release = Arc::clone(&release);
    let worker = std::thread::spawn(move || {
      started_lease.with_valid(|| {
        started_entered.wait();
        started_release.wait();
      })
    });

    entered.wait();
    let (revoked, done) = mpsc::channel();
    let revoke_lease = Arc::clone(&lease);
    let revoker = std::thread::spawn(move || {
      revoke_lease.revoke();
      revoked.send(()).unwrap();
    });
    assert!(
      done.try_recv().is_err(),
      "teardown waits for the short start gate"
    );
    release.wait();
    assert!(worker.join().unwrap().is_some());
    revoker.join().unwrap();
    assert!(done.try_recv().is_ok());
    assert!(lease.with_valid(|| ()).is_none(), "stale start is rejected");
  }
}
