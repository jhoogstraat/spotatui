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

use tokio::sync::{mpsc, oneshot, Mutex};

use crate::core::app::App;
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
  /// Something mutated observable session state: rebuild + republish if it
  /// differs from what is live. Carries which surface changed so later stages
  /// can shard the fold task without re-plumbing producers.
  StateChanged(Changed),
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
}

/// One session-level fact (ticket 16). The fold applies these through the
/// same methods the old inline writes used, so behavior is byte-for-byte
/// parity while the lock sites disappear. Debug and Clone are manual:
/// `IoEvent` has no Debug and must not grow one.
#[derive(Clone)]
pub enum SessionFact {
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
  /// A finished lyrics fetch. The fold re-gates on `desired_lyrics_identity`
  /// (the old inline stale-check) before applying; the loading-half claim is
  /// [`FoldQuery::ClaimLyricsFetch`].
  Lyrics {
    identity: (String, String),
    lyrics: Option<Vec<(u128, String)>>,
    synced: bool,
    found: bool,
  },
  /// The global play counter (telemetry read; `None` = fetch failed).
  GlobalSongCount { count: Option<u64>, failed: bool },
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
  /// Party-mode state transition (status bar + optional session swap).
  Party {
    status: crate::infra::network::sync::PartyStatus,
    session: Option<crate::infra::network::sync::PartySession>,
  },
  /// Party session field updates from the relay (room code, host name,
  /// guest join/leave, control mode, room ended/error teardown).
  PartySessionUpdate(crate::infra::network::sync::PartySessionFact),
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
  ArtistView(Box<crate::core::app::Artist>),
  /// An album's tracklist landed. `Simplified` comes from the playlist-style
  /// listing (context Simplified); `Full` from the album endpoint (context
  /// Full). Both push the AlbumTracks route with a reset selection.
  AlbumTracksSimplified {
    album: crate::core::plugin_api::AlbumInfo,
    tracks: crate::core::pagination::Paged<crate::core::plugin_api::TrackInfo>,
  },
  AlbumTracksFull {
    album: crate::core::plugin_api::AlbumInfo,
  },
  /// Podcast episodes landed: `reset` clears the paged library cache and
  /// selects the show (entry from a show list); otherwise pages append to the
  /// existing cache (scroll pagination).
  ShowEpisodesLanded {
    page: crate::core::pagination::Paged<crate::core::plugin_api::EpisodeInfo>,
    show: crate::core::plugin_api::ShowInfo,
  },
  ShowEpisodesAppended {
    page: crate::core::pagination::Paged<crate::core::plugin_api::EpisodeInfo>,
  },
  /// The full show landed (metadata.rs).
  FullShowLanded {
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
  /// The device list landed (user.rs `get_devices`). The fold runs the whole
  /// old lock section: navigation, the native recovery interlock (a
  /// check-and-set must not be split from the write it gates), the
  /// native-device merge, the cache write, and the plugin generation bump.
  /// `Device` clones; the payload wrapper does not.
  DevicesLanded {
    devices: Vec<rspotify::model::device::Device>,
    navigate: bool,
  },
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
      SessionFact::ArtistView(_) => f.write_str("ArtistView"),
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
      SessionFact::DevicesLanded { navigate, .. } => f
        .debug_struct("DevicesLanded")
        .field("navigate", navigate)
        .finish_non_exhaustive(),
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
      SessionFact::Lyrics { identity, .. } => f.debug_tuple("Lyrics").field(identity).finish(),
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
      SessionFact::Party { status, session } => f
        .debug_struct("Party")
        .field("status", status)
        .field("session", &session.is_some())
        .finish(),
      SessionFact::PartySessionUpdate(update) => {
        f.debug_tuple("PartySessionUpdate").field(&update).finish()
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

/// Which state surface produced an event. Coarse on purpose: publication
/// dedup decides whether anything actually sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Changed {
  Playback,
  Queue,
  Catalog,
  Session,
  Notice,
  Audio,
}

impl EngineEvent {
  /// Convenience constructor for whole-surface churn.
  pub fn changed(which: Changed) -> Self {
    EngineEvent::StateChanged(which)
  }

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
/// the terminal UI's input shape and stays internal where other in-process
/// producers — DJ tools, scripting effects — still speak it).
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
#[derive(Debug)]
pub enum FoldQuery {
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
  ReleasePlaylistTracksPage { playlist_id: String, offset: u32 },
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
  /// Claim a playlist-tracks page offset for fetching (the old
  /// `playlist_tracks_prefetch_in_flight.insert(offset)` RMW, combined with
  /// the generation + still-open checks into one atomic fold decision).
  ClaimPlaylistTracksOffset { playlist_id: String, offset: u32 },
  /// Release a claimed playlist-tracks offset (fetch failed).
  ReleasePlaylistTracksOffset { playlist_id: String, offset: u32 },
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
  /// The latest catalog search results (CLI `play`/`search`).
  SearchResults,
  /// The last surfaced API error text (CLI decides success vs. failure).
  LastError,
  /// Force the pending state save out to disk now (CLI end-of-command).
  FlushStateSave,
  /// The source-agnostic playback snapshot (DJ now-playing, window title).
  MediaSnapshot,
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
  ClaimLyricsFetch {
    /// The (track, joined-artists) identity the fetch was dispatched for.
    identity: (String, String),
  },
  /// The relay server URL from the user config (the party connect paths).
  PartyRelayUrl,
  /// Whether a party session is live and hosting (relay commands from
  /// guests are only accepted by the host).
  IsPartyHost,
  /// Whether a party session is live as a guest (incoming sync handling).
  IsPartyGuest,
  /// The host's playback identity to broadcast: current track URI, position,
  /// and play state. `None` when not hosting or nothing is playing.
  HostSyncState,
  /// The guest's current playback identity: (uri, is_playing, progress).
  GuestSyncCurrent,
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
}

/// The answer side of [`FoldQuery`]. Kept as a plain enum so the fold can
/// match exhaustively and producers cannot mis-cast.
/// Debug is manual: `StreamingPlayer` has no Debug.
pub enum FoldAnswer {
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
  /// The latest catalog search results.
  Search(Box<crate::core::app::SearchResult>),
  /// The source-agnostic playback snapshot (or `None` when nothing plays).
  Media(Option<crate::infra::media_metadata::PlaybackSnapshot>),
  /// The native queue's tracks, in order.
  QueueTracks(Vec<crate::core::plugin_api::TrackInfo>),
  /// The standing DJ vibe and whether the auto-queue DJ is on.
  #[cfg(feature = "dj-core")]
  DjVibe {
    vibe: Option<String>,
    auto_queue: bool,
  },
  /// The announcement-surface answer (feed config + optional dedup set).
  AnnouncementSources {
    enabled: bool,
    feed_url: Option<String>,
    seen_ids: Option<Vec<String>>,
  },
  /// The host's playback identity to broadcast (`None` = nothing to sync).
  HostSync(Option<HostSyncState>),
  /// The guest's current playback identity: (uri, is_playing, progress).
  GuestSyncCurrent(String, bool, u64),
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
}

impl std::fmt::Debug for FoldAnswer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
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
      FoldAnswer::Search(v) => f.debug_tuple("Search").field(v).finish(),
      FoldAnswer::Media(v) => f.debug_tuple("Media").field(&v.is_some()).finish(),
      FoldAnswer::QueueTracks(v) => f.debug_tuple("QueueTracks").field(&v.len()).finish(),
      #[cfg(feature = "dj-core")]
      FoldAnswer::DjVibe { vibe, auto_queue } => f
        .debug_struct("DjVibe")
        .field("vibe", vibe)
        .field("auto_queue", auto_queue)
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
      FoldAnswer::HostSync(v) => f.debug_tuple("HostSync").field(&v.is_some()).finish(),
      FoldAnswer::GuestSyncCurrent(uri, playing, progress) => f
        .debug_tuple("GuestSyncCurrent")
        .field(&uri.len())
        .field(playing)
        .field(&progress)
        .finish(),
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

/// The host's playback identity to broadcast from the fold (ticket 17):
/// the old inline read of party host + context + progress.
#[derive(Debug, Clone)]
pub struct HostSyncState {
  pub track_uri: String,
  pub position_ms: u64,
  pub is_playing: bool,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldAck {
  /// The next missing saved-tracks offset after the folded upsert.
  pub next_saved_tracks_offset: Option<u32>,
  /// The next missing playlist-tracks offset after the folded upsert.
  pub next_playlist_tracks_offset: Option<u32>,
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
  events_tx: mpsc::Sender<EngineEvent>,
  queries_tx: mpsc::Sender<(FoldQuery, oneshot::Sender<Result<FoldAnswer, SpineClosed>>)>,
  acks_tx: mpsc::Sender<(EngineEvent, oneshot::Sender<Option<FoldAck>>)>,
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
  pub fn open(bounds: SpineBounds) -> (Spine, mpsc::Receiver<EngineEvent>, QueryReceiver) {
    let (events_tx, events_rx) = mpsc::channel(bounds.event_capacity);
    let (queries_tx, queries_rx) = mpsc::channel(bounds.event_capacity);
    let (acks_tx, acks_rx) = mpsc::channel(bounds.event_capacity);
    (
      Spine {
        events_tx,
        queries_tx,
        acks_tx,
      },
      events_rx,
      QueryReceiver {
        queries_rx: Mutex::new(Some(queries_rx)),
        acks_rx: Mutex::new(Some(acks_rx)),
      },
    )
  }

  /// Push an event. Never blocks: if the buffer is momentarily full the
  /// coalescing publisher will catch up on its next wake anyway, and losing
  /// a duplicate poke costs nothing while blocking every producer would.
  pub async fn emit(&self, event: EngineEvent) {
    let _ = self.events_tx.try_send(event);
  }
  /// Synchronous fire-and-forget for contexts holding no async reactor
  /// (blocking threads, drop paths). Same lossy-on-overflow contract.
  pub fn emit_now(&self, event: EngineEvent) {
    let _ = self.events_tx.try_send(event);
  }

  /// Emit one event and wait until the fold has applied it, receiving the
  /// post-fold facts the producer used to read under its own lock after the
  /// write (the prefetch loop's next-missing-offset read). The ack waits on
  /// the fold's queue order, so the answer reflects every event queued
  /// before this one — stream order is preserved, no lock needed.
  pub async fn emit_acked(&self, event: EngineEvent) -> Option<FoldAck> {
    let (tx, rx) = oneshot::channel();
    self.acks_tx.send((event, tx)).await.ok()?;
    rx.await.ok().flatten()
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
  acks_rx: Mutex<Option<mpsc::Receiver<(EngineEvent, AckSender)>>>,
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
    let acks = self.acks_rx.lock().await.take().map(|rx| FoldAcks { rx });
    Some(FoldHandles {
      queries: queries?,
      acks: acks?,
    })
  }
}

/// The fold's receive halves, taken once at boot by the fold task.
pub struct FoldHandles {
  pub queries: FoldQueries,
  pub acks: FoldAcks,
}

/// Drain helper: `recv().await` / `try_recv()` on the taken receiver.
pub struct FoldQueries {
  pub(crate) rx: mpsc::Receiver<(FoldQuery, oneshot::Sender<Result<FoldAnswer, SpineClosed>>)>,
}

/// The acked-emit receive half: pairs an event with the responder the fold
/// completes after applying it.
pub struct FoldAcks {
  pub(crate) rx: mpsc::Receiver<(EngineEvent, oneshot::Sender<Option<FoldAck>>)>,
}
impl FoldQueries {
  pub async fn recv(
    &mut self,
  ) -> Option<(FoldQuery, oneshot::Sender<Result<FoldAnswer, SpineClosed>>)> {
    self.rx.recv().await
  }
}

/// Read the producer spine off `App` and emit one catalog event (ticket 15).
///
/// Producers run under their own locks; the spine handle lives on `App`
/// (installed at boot), so emitting is lock-free. `None` in tests that never
/// installed a spine: callers treat that as "fold absent, nothing to do".
pub(crate) async fn emit_catalog_event(
  app: &Arc<Mutex<App>>,
  event: crate::core::catalog::CatalogStateEvent,
) {
  if let Some(spine) = app.lock().await.spine.as_ref() {
    spine.emit(EngineEvent::Catalog(event)).await;
  }
}
