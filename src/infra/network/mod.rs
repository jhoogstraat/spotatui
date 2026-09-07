#[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
pub mod dj;
pub mod ids;
pub mod library;
pub mod mapping;
pub mod metadata;
pub mod native_shuffle;
pub mod playback;
pub mod recommend;
pub mod requests;
pub mod search;
pub mod spotify_source;
pub mod user;
pub mod utils;

use crate::core::auth;
use crate::core::config::ClientConfig;
use crate::core::plugin_api::{ShowInfo, TrackInfo};
use crate::infra::redirect_uri::redirect_uri_web_server_async;
use crate::runtime::engine::{EngineEvent, FoldAnswer, FoldQuery};
use crate::runtime::engine::{PostConnectFetch, SessionFact};
use anyhow::anyhow;
use rspotify::model::{
  album::SimplifiedAlbum,
  enums::{Country, RepeatState},
};
// `parse_response_code` / `request_token` for the in-TUI login live on this trait.
use rspotify::clients::OAuthClient;
use rspotify::AuthCodePkceSpotify;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use tokio::sync::Mutex;

// Re-export traits
use self::library::LibraryNetwork;
use self::metadata::MetadataNetwork;
use self::playback::PlaybackNetwork;
use self::recommend::RecommendationNetwork;
use self::search::SearchNetwork;
use self::user::UserNetwork;
use self::utils::UtilsNetwork;

// Most variants were constructed by TUI handlers (deleted, ticket 01); ticket
// 02's event-spine work replaces this enum. Never `cfg` a variant out for
// feature-gating - a build leg missing a feature would then compile clean
// while the feature is silently absent.
#[allow(dead_code)]
pub enum IoEvent {
  /// Internal wake-up used to stop the blocking sync-to-async bridge.
  #[allow(dead_code)]
  ShutdownPump,
  GetCurrentPlayback,
  /// After a track transition (e.g., EndOfTrack), ensure we don't end up paused on the next item.
  /// The payload is the previous track identifier (either base62 id or a `spotify:track:` URI).
  #[allow(dead_code)]
  EnsurePlaybackContinues(String),
  RefreshAuthentication,
  GetPlaylists,
  GetDevices,
  /// Refresh the device list without opening the device picker.
  #[allow(dead_code)]
  GetDevicesSilent,
  GetSearchResults(String, Option<Country>),
  /// Playlist id/URI, page offset.
  GetPlaylistItems(String, u32),
  /// Playlist id/URI, query.
  SearchPlaylistTracks(String, String),
  GetCurrentSavedTracks(Option<u32>),
  /// Context URI (album/artist/playlist), specific playable URIs, start offset.
  StartPlayback(Option<String>, Option<Vec<String>>, Option<usize>),
  /// Restore a generation-guarded native playback snapshot after rebuilding
  /// the librespot backend. This is a direct Spirc load and does not use the Web API.
  #[cfg(feature = "streaming")]
  RestoreNativePlayback(u64),
  /// Replay the published native Spotify queue slot after recovery. Routed by
  /// the native queue before the Spotify network handler.
  #[cfg(feature = "streaming")]
  ReplayPublishedSpotifyQueueSlot,
  UpdateSearchLimits(u32, u32),
  Seek(u32),
  NextTrack,
  PreviousTrack,
  ForcePreviousTrack,
  Shuffle(bool), // desired shuffle state
  Repeat(RepeatState),
  PausePlayback,
  ChangeVolume(u8),
  /// Artist id/URI, display name, market.
  GetArtist(String, String, Option<Country>),
  GetAlbumTracks(Box<SimplifiedAlbum>),
  /// Seed artist ids/URIs, seed track ids/URIs, first seed track, market.
  GetRecommendationsForSeed(
    Option<Vec<String>>,
    Option<Vec<String>>,
    Box<Option<TrackInfo>>,
    Option<Country>,
  ),
  GetCurrentUserSavedAlbums(Option<u32>),
  CurrentUserSavedAlbumsContains(Vec<String>),
  CurrentUserSavedAlbumDelete(String),
  CurrentUserSavedAlbumAdd(String),
  UserUnfollowArtists(Vec<String>),
  UserFollowArtists(Vec<String>),
  /// Owner user id, playlist id/URI, public flag.
  UserFollowPlaylist(String, String, Option<bool>),
  /// Owner user id, playlist id/URI.
  UserUnfollowPlaylist(String, String),
  /// Playlist id/URI, track id/URI.
  AddTrackToPlaylist(String, String),
  /// Playlist id/URI, track id/URI, position.
  RemoveTrackFromPlaylistAtPosition(String, String, usize),
  GetUser,
  /// Playable URI (track or episode) to toggle in saved tracks.
  ToggleSaveTrack(String),
  /// Track id/URI, market.
  GetRecommendationsForTrackId(String, Option<Country>),
  GetRecentlyPlayed,
  /// Refresh recently-played data without opening a screen.
  #[allow(dead_code)]
  GetRecentlyPlayedSilent,
  /// Pagination cursor: artist id/URI to fetch after.
  GetFollowedArtists(Option<String>),
  UserArtistFollowCheck(Vec<String>),
  GetAlbum(String),
  TransferPlaybackToDevice(String, bool),
  #[allow(dead_code)]
  AutoSelectStreamingDevice(String, bool, bool), // Auto-select a device by name (used for native streaming): (device name, persist_device_id, yield_to_external_playback)
  GetAlbumForTrack(String),
  CurrentUserSavedTracksContains(Vec<String>),
  GetCurrentUserSavedShows(Option<u32>),
  CurrentUserSavedShowsContains(Vec<String>),
  CurrentUserSavedShowDelete(String),
  CurrentUserSavedShowAdd(String),
  GetShowEpisodes(Box<ShowInfo>),
  GetShow(String),
  GetCurrentShowEpisodes(String, Option<u32>),
  /// Playable URI (track or episode) to enqueue.
  AddItemToQueue(String),
  GetQueue,
  /// Advance the native cross-source queue: play the next queued item, or resume
  /// the suspended per-source context when the queue drains. Consumed by
  /// `infra::queue::dispatch::route_queue_event` (wired first in the pump); it
  /// never reaches the Spotify network handler.
  AdvanceNativeQueue,
  /// Replay the current track of the active decoded source (repeat-one). Consumed
  /// by the per-source routers (`route_local_event` / `route_subsonic_event` /
  /// `route_youtube_event`); it never reaches the Spotify network handler. Only
  /// dispatched by the runner tick under a decoded source feature.
  #[cfg_attr(
    not(any(feature = "local-files", feature = "subsonic", feature = "youtube")),
    allow(dead_code)
  )]
  ReplayCurrentTrack,
  /// Resume a suspended native-streaming Spotify context after the native queue
  /// drains (context URI, resume-track URI). Re-loads the context on the native
  /// device via the existing `start_playback` machinery. `allow(dead_code)`:
  /// only constructed under `streaming`, but the handler arm is unconditional.
  #[allow(dead_code)]
  ResumeSpotifyContext(Option<String>, Option<String>),
  /// Toggle the native-Spotify client-side shuffle session on/off: reorder the
  /// app-owned track list and reload it into Spirc (building a session
  /// mid-playback when possible; falls back to Spirc shuffle otherwise).
  #[cfg_attr(not(feature = "streaming"), allow(dead_code))]
  ToggleNativeShuffleSession(bool),
  /// Re-randomize the shuffle session for a fresh repeat-all lap (dispatched
  /// when the last track wraps back to the first).
  #[cfg_attr(not(feature = "streaming"), allow(dead_code))]
  ReshuffleNativeShuffleLap,
  /// Resume the suspended shuffle session at the given index once the native
  /// queue drains (`None` = session exhausted, finish instead). The `u64` is the
  /// session generation the resume was snapshotted from; the handler applies the
  /// index only while the live session still matches, so a session replaced
  /// mid-drain cannot inherit a stale index.
  #[cfg_attr(not(feature = "streaming"), allow(dead_code))]
  ResumeNativeShuffleSession(Option<usize>, u64),
  IncrementGlobalSongCount,
  FetchGlobalSongCount,
  FetchAnnouncements,
  /// Get user's top tracks for Discover feature (with time range)
  /// Get Top Artists Mix - fetches top artists and their top tracks
  /// Fetch all playlist tracks and apply sorting
  FetchAllPlaylistTracksAndSort(String),
  /// Search tracks to add to a new playlist
  SearchTracksForPlaylist(String),
  /// Create a new playlist: playlist name, track ids/URIs.
  CreateNewPlaylist(String, Vec<String>),
  /// Fetch the current user's own friend code from spotatui.com
  /// Fetch the current user's friends list from spotatui.com
  /// Add a friend by their 6-character friend code
  /// Add a friend by their spotatui.com user ID
  /// Unfollow a friend by their spotatui.com user ID
  /// Search spotatui.com users by display name or friend code
  /// Aggregate local listening history for the Stats screen
  LoadListeningStats(crate::infra::history::RecapPeriod),
  /// Export the shareable HTML recap and open it in the browser
  GenerateRecap(crate::infra::history::RecapPeriod),
  /// List the folders under the configured local music directory (handled by
  /// `infra::local::dispatch`; a no-op on the Spotify network).
  GetLocalPlaylists,
  /// List the audio files in a local folder, identified by its `file://` URI.
  /// The URI is only read by `infra::local::dispatch` (the `local-files`
  /// feature); without it the event is an inert no-op.
  #[cfg_attr(not(feature = "local-files"), allow(dead_code))]
  GetLocalTracks(String),
  /// List the user's Subsonic server playlists (handled by
  /// `infra::subsonic::dispatch`; a no-op on the Spotify network).
  GetSubsonicPlaylists,
  /// List the tracks of a Subsonic playlist, identified by its
  /// `subsonic:playlist:` URI. Only read by `infra::subsonic::dispatch` (the
  /// `subsonic` feature); without it the event is an inert no-op.
  #[cfg_attr(not(feature = "subsonic"), allow(dead_code))]
  GetSubsonicTracks(String),
  /// Run a Subsonic catalog search and populate `app.search_results`. Only read
  /// by `infra::subsonic::dispatch`; an inert no-op without the `subsonic` feature.
  #[cfg_attr(not(feature = "subsonic"), allow(dead_code))]
  GetSubsonicSearchResults(String),
  /// Load the configured internet-radio stations into the sidebar (handled by
  /// `infra::radio::dispatch`; a no-op on the Spotify network).
  GetRadioStations,
  /// Search the radio-browser.info directory and populate `app.search_results`.
  /// Only read by `infra::radio::dispatch`; an inert no-op without the
  /// `internet-radio` feature.
  #[cfg_attr(not(feature = "internet-radio"), allow(dead_code))]
  GetRadioSearchResults(String),
  /// Run a YouTube search (via yt-dlp) and populate `app.search_results`.
  /// Only read by `infra::youtube::dispatch`; an inert no-op without the
  /// `youtube` feature.
  #[cfg_attr(not(feature = "youtube"), allow(dead_code))]
  GetYouTubeSearchResults(String),
  /// Load the local YouTube playlists file into the sidebar (handled by
  /// `infra::youtube::dispatch`; a no-op on the Spotify network).
  GetYouTubePlaylists,
  /// Open a local YouTube playlist's tracks in the shared track table,
  /// identified by its `youtube:playlist:` URI.
  #[cfg_attr(not(feature = "youtube"), allow(dead_code))]
  GetYouTubeTracks(String),
  /// Create a local YouTube playlist with the given name.
  #[cfg_attr(not(feature = "youtube"), allow(dead_code))]
  CreateYouTubePlaylist(String),
  /// Delete the local YouTube playlist with the given `youtube:playlist:` URI.
  #[cfg_attr(not(feature = "youtube"), allow(dead_code))]
  DeleteYouTubePlaylist(String),
  /// Add a video (bare id or `youtube:` URI; metadata resolved from the browse
  /// views) to a local YouTube playlist (URI or bare id).
  #[cfg_attr(not(feature = "youtube"), allow(dead_code))]
  AddTrackToYouTubePlaylist(String, String),
  /// Remove a video (bare id or `youtube:` URI) from a local YouTube playlist.
  #[cfg_attr(not(feature = "youtube"), allow(dead_code))]
  RemoveTrackFromYouTubePlaylist(String, String),
  /// Start an in-TUI Spotify OAuth login: open the browser and spawn the callback
  /// server. Dispatched from the `d` source picker when Spotify is unconfigured.
  /// Runs without a Spotify session (bypasses the auth gate).
  BeginSpotifyLogin,
  /// Complete an in-TUI Spotify login with the OAuth callback URL received by the
  /// spawned server. Bypasses the auth gate (there is no session yet).
  CompleteSpotifyLogin(String),
  /// Abandon an in-flight in-TUI Spotify login (callback server timed out or
  /// failed), clearing the pending state so the user can retry.
  CancelSpotifyLogin,
  /// A DJ tool call that needs the live Spotify client, plus the channel to
  /// answer on.
  ///
  /// Runs on the **serial** lane, not the service lane: resolving a track name
  /// to a URI needs the real Spotify client, and the service lane deliberately
  /// builds its `Network` with `None` for it. It does bypass the auth *gate* so
  /// the handler can answer an unauthenticated caller with a useful message
  /// rather than silently dropping the response channel.
  ///
  /// Boxed because the payload is much larger than the other variants.
  #[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
  DjToolCall(
    Box<(
      crate::infra::dj::tools::DjToolCall,
      tokio::sync::oneshot::Sender<crate::infra::dj::tools::ToolOutcome>,
    )>,
  ),
  /// Ask the in-TUI DJ's brain for tracks. Optionally carries the listener's
  /// words; `None` means "keep the queue going in the current direction".
  ///
  /// Runs on the **service** lane: a brain call can take a minute or more (an
  /// agent CLI is a subprocess with real startup cost), and parking that on the
  /// serial pump would freeze every other event behind it. It uses fold queries
  /// and facts plus its own HTTP client / subprocess, which keeps the service
  /// lane independent of serial state.
  #[cfg(feature = "ai-dj")]
  AskDj(Box<crate::infra::dj::AskDjRequest>),
  /// Top the queue up because it is running low.
  ///
  /// Fields, in order: the DJ generation this refill was dispatched for, so a
  /// stale one can be dropped; and the turn sequence from `DjState::begin_turn`,
  /// so only this refill may clear the progress flag. Both are `u64`, so a
  /// transposition would compile and then discard the wrong turn's flag.
  #[cfg(feature = "ai-dj")]
  DjTopUp(u64, u64),
  /// Crawl the listener's own playlists for the avoid-library filter.
  ///
  /// Serial lane: it needs the real Spotify client. Dispatched when the filter is
  /// switched on, so the index is usually warm by the time the first batch comes
  /// back from the brain; the resolve step builds it inline if it is not.
  ///
  /// The MCP front door dispatches it too, from `search_tracks`, so that marking
  /// results as owned never crawls *inside* a latency-sensitive tool call. Hence
  /// the gate is both features rather than `ai-dj` alone.
  #[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
  DjIndexLibrary,
}

/// An in-flight in-TUI Spotify login. Holds the exact PKCE client that generated
/// the authorize URL: PKCE stores the `code_verifier` inside the instance, so the
/// token exchange in `complete_spotify_login` MUST run on this same client.
struct PendingLogin {
  spotify: AuthCodePkceSpotify,
  token_cache_path: PathBuf,
}

pub struct Network {
  /// The authenticated Spotify client, or `None` when spotatui was launched
  /// against a free source (YouTube/Subsonic/Radio/Local) without a Spotify
  /// session. Spotify-bound `IoEvent`s early-return at the auth gate in
  /// `handle_network_event` when this is `None`; the `spotify()` accessor is
  /// only reached from handlers that run behind that gate. In-TUI login
  /// (`CompleteSpotifyLogin`) fills this in live.
  pub spotify: Option<AuthCodePkceSpotify>,
  pub large_search_limit: u32,
  pub small_search_limit: u32,
  pub client_config: ClientConfig,
  /// The engine spine handle (ticket 16): handlers emit result events here
  /// instead of writing App fields. Populated by `start_services`; tests
  /// without a fold leave it `None` and handlers skip emitting.
  pub spine: Option<crate::runtime::engine::Spine>,
  /// Immutable source configuration copied at startup; workers never need the
  /// mutable application state to build a source resolver.
  pub ytdlp_path: Option<String>,
  #[cfg(feature = "streaming")]
  native_idle_recovery: playback::NativeIdleRecoveryState,
  pub token_cache_path: PathBuf,
  /// In-flight in-TUI Spotify login, if any (see `begin_spotify_login`).
  pending_login: Option<PendingLogin>,
  /// How many playback polls in a row came back 401. Spotify's player service
  /// intermittently rejects a valid token (issue #395) and the poll runs once a
  /// second against an external device, so a lone 401 is noise; only a sustained
  /// run means the session is actually broken and deserves the error route.
  /// Reset by every poll the API answers.
  consecutive_playback_auth_failures: u8,
  /// TTL caches so re-visiting the same artist/album skips the round trip +
  /// pacing tax (see `metadata::MetadataTtlCache`).
  artist_cache: metadata::MetadataTtlCache<metadata::CachedArtistData>,
  album_cache: metadata::MetadataTtlCache<rspotify::model::album::FullAlbum>,
  album_tracks_cache: metadata::MetadataTtlCache<Vec<rspotify::model::track::SimplifiedTrack>>,
}

impl Network {
  #[cfg(feature = "streaming")]
  pub fn new(
    spotify: Option<AuthCodePkceSpotify>,
    client_config: ClientConfig,
    spine: Option<crate::runtime::engine::Spine>,
    ytdlp_path: Option<String>,
    token_cache_path: PathBuf,
  ) -> Self {
    Network {
      spotify,
      large_search_limit: 50,
      small_search_limit: 12,
      client_config,
      spine,
      ytdlp_path,
      native_idle_recovery: playback::NativeIdleRecoveryState::default(),
      token_cache_path,
      pending_login: None,
      consecutive_playback_auth_failures: 0,
      artist_cache: Default::default(),
      album_cache: Default::default(),
      album_tracks_cache: Default::default(),
    }
  }

  #[cfg(not(feature = "streaming"))]
  pub fn new(
    spotify: Option<AuthCodePkceSpotify>,
    client_config: ClientConfig,
    spine: Option<crate::runtime::engine::Spine>,
    ytdlp_path: Option<String>,
    token_cache_path: PathBuf,
  ) -> Self {
    Network {
      spotify,
      large_search_limit: 50,
      small_search_limit: 12,
      client_config,
      spine,
      ytdlp_path,
      token_cache_path,
      pending_login: None,
      consecutive_playback_auth_failures: 0,
      artist_cache: Default::default(),
      album_cache: Default::default(),
      album_tracks_cache: Default::default(),
    }
  }

  /// Borrow the authenticated Spotify client. Only call this from handlers that
  /// run behind the auth gate in `handle_network_event`: that gate early-returns
  /// for every Spotify-bound event when `self.spotify` is `None`, so a handler
  /// reached past it is guaranteed a live client. The `expect` documents (and
  /// enforces at runtime) that invariant; it is unreachable in normal operation.
  fn spotify(&self) -> &AuthCodePkceSpotify {
    self
      .spotify
      .as_ref()
      .expect("Spotify client present: the auth gate rejects Spotify events when it is None")
  }

  /// True for `IoEvent`s whose handlers never call [`Network::spotify`], so they
  /// run even without a Spotify session (free-source launch). Keep this in sync
  /// with the handlers: an event listed here MUST NOT reach the `spotify()`
  /// accessor. Covered here: auth refresh (a no-op when there is no client),
  /// source-agnostic services (telemetry, announcements), pure-state updates,
  /// and the per-source browse events that the
  /// source dispatchers handle upstream (only reaching here as no-ops when their
  /// feature is disabled).
  fn event_bypasses_spotify_auth(io_event: &IoEvent) -> bool {
    #[cfg(feature = "streaming")]
    if matches!(
      io_event,
      IoEvent::RestoreNativePlayback(_) | IoEvent::ReplayPublishedSpotifyQueueSlot
    ) {
      return true;
    }
    // Bypasses the gate but NOT onto the service lane: the handler needs the real
    // Spotify client, and answers an unauthenticated caller itself so whichever
    // front door asked — an MCP client or the in-TUI DJ — gets a diagnosable
    // error instead of a dropped channel.
    #[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
    if matches!(io_event, IoEvent::DjToolCall(_)) {
      return true;
    }
    // The brain call needs no Spotify session at all. The tool calls the loop
    // makes from inside it do, and those go back through `DjToolCall` above.
    #[cfg(feature = "ai-dj")]
    if matches!(io_event, IoEvent::AskDj(_) | IoEvent::DjTopUp(..)) {
      return true;
    }
    matches!(
      io_event,
      IoEvent::RefreshAuthentication
        | IoEvent::BeginSpotifyLogin
        | IoEvent::CompleteSpotifyLogin(_)
        | IoEvent::CancelSpotifyLogin
        | IoEvent::FetchGlobalSongCount
        | IoEvent::IncrementGlobalSongCount
        | IoEvent::FetchAnnouncements
        | IoEvent::UpdateSearchLimits(..)
        | IoEvent::LoadListeningStats(_)
        | IoEvent::GenerateRecap(_)
        | IoEvent::GetLocalPlaylists
        | IoEvent::GetLocalTracks(_)
        | IoEvent::GetSubsonicPlaylists
        | IoEvent::GetSubsonicTracks(_)
        | IoEvent::GetSubsonicSearchResults(_)
        | IoEvent::GetRadioStations
        | IoEvent::GetRadioSearchResults(_)
        | IoEvent::GetYouTubeSearchResults(_)
        | IoEvent::GetYouTubePlaylists
        | IoEvent::GetYouTubeTracks(_)
        | IoEvent::CreateYouTubePlaylist(_)
        | IoEvent::DeleteYouTubePlaylist(_)
        | IoEvent::AddTrackToYouTubePlaylist(..)
        | IoEvent::RemoveTrackFromYouTubePlaylist(..)
    )
  }

  /// Events that run on the concurrent service lane in `start_tokio`: a strict
  /// subset of [`Self::event_bypasses_spotify_auth`] whose handlers touch only
  /// immutable configuration plus their own HTTP clients / `spawn_blocking` —
  /// never the Spotify client, API pacing, or pending login/search limits — so
  /// they can execute on a detached task with a throwaway `Network` instead of
  /// head-of-line-blocking the serial pump.
  /// Keep in sync with the handlers: an event listed here MUST NOT access
  /// mutable serial-lane state.
  pub fn runs_on_service_lane(io_event: &IoEvent) -> bool {
    if !Self::event_bypasses_spotify_auth(io_event) {
      return false;
    }
    #[cfg(feature = "ai-dj")]
    if matches!(io_event, IoEvent::AskDj(_) | IoEvent::DjTopUp(..)) {
      return true;
    }
    matches!(
      io_event,
      IoEvent::FetchGlobalSongCount
        | IoEvent::IncrementGlobalSongCount
        | IoEvent::FetchAnnouncements
        | IoEvent::LoadListeningStats(_)
        | IoEvent::GenerateRecap(_)
    )
  }

  #[allow(clippy::cognitive_complexity)]
  pub async fn handle_network_event(&mut self, io_event: IoEvent) {
    let pending_playlist_id = match &io_event {
      IoEvent::GetPlaylistItems(id, _) => Some(id.clone()),
      _ => None,
    };
    // Ticket 16: the auth gate reads fold-published state, not bare fields.
    // A session check goes through the fold query; the reject path emits
    // facts instead of writing App fields.
    let bypass_auth = Self::event_bypasses_spotify_auth(&io_event);

    if !bypass_auth {
      // The fold's published connected state is the gate when a fold is
      // live; a failed query (no fold: CLI, tests) falls back to client
      // presence, which is what the pre-fold gate keyed on.
      let connected = if let Some(spine) = self.spine.as_ref() {
        match spine.query(FoldQuery::SpotifyConnected).await {
          Ok(FoldAnswer::Bool(connected)) => connected,
          _ => self.spotify.is_some(),
        }
      } else {
        self.spotify.is_some()
      };
      if !connected {
        self
          .emit_status(
            "Spotify not connected. Press `d` and pick Spotify to log in.".to_string(),
            6,
          )
          .await;
        self.emit_loading_cleared(&pending_playlist_id).await;
        return;
      }
      if !self.ensure_authentication_fresh(false).await {
        if let Some(id) = pending_playlist_id.as_deref() {
          self
            .emit_session(SessionFact::ClearPendingPlaylistOpen(Some(id.to_string())))
            .await;
        }
        return;
      }
    }

    match io_event {
      IoEvent::ShutdownPump => {}
      IoEvent::RefreshAuthentication => {
        self.refresh_authentication().await;
      }
      IoEvent::EnsurePlaybackContinues(previous_track_id) => {
        self.ensure_playback_continues(previous_track_id).await;
      }
      IoEvent::GetPlaylists => {
        self.get_current_user_playlists().await;
      }
      IoEvent::GetUser => {
        self.get_user().await;
      }
      IoEvent::GetDevices => {
        self.get_devices(true).await;
      }
      IoEvent::GetDevicesSilent => {
        self.get_devices(false).await;
      }
      IoEvent::GetCurrentPlayback => {
        self.get_current_playback().await;
      }
      IoEvent::GetSearchResults(search_term, country) => {
        // Ticket 15: register the request with the fold before fetching so a
        // later superseding request can drop this completion.
        let generation = self
          .emit_catalog_acked(crate::core::catalog::CatalogStateEvent::SearchRequested {
            source: crate::core::source::Source::Spotify,
            query: search_term.clone(),
            scoped_loading: true,
          })
          .await
          .and_then(|ack| ack.catalog_generation);
        let Some(generation) = generation else {
          return;
        };
        self
          .get_search_results(search_term, country, generation)
          .await;
      }

      IoEvent::GetPlaylistItems(playlist_id, playlist_offset) => {
        if let Some(id) = ids::playlist_id(&playlist_id) {
          self.get_playlist_tracks(id, playlist_offset).await;
        } else {
          self
            .emit_session(SessionFact::ClearPendingPlaylistOpen(Some(
              playlist_id.to_string(),
            )))
            .await;
          self
            .emit_status("Invalid playlist identifier.".to_string(), 5)
            .await;
        }
      }
      IoEvent::SearchPlaylistTracks(playlist_id, query) => {
        if let Some(id) = ids::playlist_id(&playlist_id) {
          self.search_playlist_tracks(id, query).await;
        }
      }
      IoEvent::GetCurrentSavedTracks(offset) => {
        self.get_current_user_saved_tracks(offset).await;
      }
      IoEvent::StartPlayback(context_uri, uris, offset) => {
        let context = context_uri.as_deref().and_then(ids::play_context_id);
        let uris = uris.map(|v| ids::playable_ids(&v));
        self.start_playback(context, uris, offset).await;
      }
      #[cfg(feature = "streaming")]
      IoEvent::RestoreNativePlayback(generation) => {
        self.restore_native_playback(generation).await;
      }
      IoEvent::UpdateSearchLimits(large_search_limit, small_search_limit) => {
        self.large_search_limit = large_search_limit;
        self.small_search_limit = small_search_limit;
      }
      IoEvent::Seek(position_ms) => {
        self.seek(position_ms).await;
      }
      IoEvent::NextTrack => {
        self.next_track().await;
      }
      IoEvent::PreviousTrack => {
        self.previous_track().await;
      }
      IoEvent::ForcePreviousTrack => {
        self.force_previous_track().await;
      }
      IoEvent::Repeat(repeat_state) => {
        self.repeat(repeat_state).await;
      }
      IoEvent::PausePlayback => {
        self.pause_playback().await;
      }
      IoEvent::ChangeVolume(volume) => {
        self.change_volume(volume).await;
      }
      IoEvent::GetArtist(artist_id, input_artist_name, country) => {
        if let Some(id) = ids::artist_id(&artist_id) {
          self.get_artist(id, input_artist_name, country).await;
        }
      }
      IoEvent::GetAlbumTracks(album) => {
        self.get_album_tracks(album).await;
      }
      IoEvent::GetRecommendationsForSeed(seed_artists, seed_tracks, first_track, country) => {
        let seed_artists = seed_artists.map(|v| ids::artist_ids(&v));
        let seed_tracks = seed_tracks.map(|v| ids::track_ids(&v));
        self
          .get_recommendations_for_seed(seed_artists, seed_tracks, first_track, country)
          .await;
      }
      IoEvent::GetCurrentUserSavedAlbums(offset) => {
        self.get_current_user_saved_albums(offset).await;
      }
      IoEvent::CurrentUserSavedAlbumsContains(album_ids) => {
        self
          .current_user_saved_albums_contains(ids::album_ids(&album_ids))
          .await;
      }
      IoEvent::CurrentUserSavedAlbumDelete(album_id) => {
        if let Some(id) = ids::album_id(&album_id) {
          self.current_user_saved_album_delete(id).await;
        }
      }
      IoEvent::CurrentUserSavedAlbumAdd(album_id) => {
        if let Some(id) = ids::album_id(&album_id) {
          self.current_user_saved_album_add(id).await;
        }
      }
      IoEvent::UserUnfollowArtists(artist_ids) => {
        self
          .user_unfollow_artists(ids::artist_ids(&artist_ids))
          .await;
      }
      IoEvent::UserFollowArtists(artist_ids) => {
        self.user_follow_artists(ids::artist_ids(&artist_ids)).await;
      }
      IoEvent::UserFollowPlaylist(playlist_owner_id, playlist_id, is_public) => {
        if let (Some(owner), Some(id)) = (
          ids::user_id(&playlist_owner_id),
          ids::playlist_id(&playlist_id),
        ) {
          self.user_follow_playlist(owner, id, is_public).await;
        }
      }
      IoEvent::UserUnfollowPlaylist(user_id, playlist_id) => {
        if let (Some(owner), Some(id)) = (ids::user_id(&user_id), ids::playlist_id(&playlist_id)) {
          self.user_unfollow_playlist(owner, id).await;
        }
      }
      IoEvent::AddTrackToPlaylist(playlist_id, track_id) => {
        if let (Some(pid), Some(tid)) = (ids::playlist_id(&playlist_id), ids::track_id(&track_id)) {
          self.add_track_to_playlist(pid, tid).await;
        }
      }
      IoEvent::RemoveTrackFromPlaylistAtPosition(playlist_id, track_id, position) => {
        if let (Some(pid), Some(tid)) = (ids::playlist_id(&playlist_id), ids::track_id(&track_id)) {
          self
            .remove_track_from_playlist_at_position(pid, tid, position)
            .await;
        }
      }

      IoEvent::ToggleSaveTrack(uri) => {
        if let Some(id) = ids::playable_id(&uri) {
          self.toggle_save_track(id).await;
        }
      }
      IoEvent::GetRecommendationsForTrackId(track_id, country) => {
        if let Some(id) = ids::track_id(&track_id) {
          self.get_recommendations_for_track_id(id, country).await;
        }
      }
      IoEvent::GetRecentlyPlayed => {
        self.get_recently_played(true).await;
      }
      IoEvent::GetRecentlyPlayedSilent => {
        self.get_recently_played(false).await;
      }
      IoEvent::GetFollowedArtists(after) => {
        self
          .get_followed_artists(after.and_then(|s| ids::artist_id(&s)))
          .await;
      }
      IoEvent::UserArtistFollowCheck(artist_ids) => {
        self
          .user_artist_check_follow(ids::artist_ids(&artist_ids))
          .await;
      }
      IoEvent::GetAlbum(album_id) => {
        if let Some(id) = ids::album_id(&album_id) {
          self.get_album(id).await;
        }
      }
      IoEvent::TransferPlaybackToDevice(device_id, persist_device_id) => {
        self
          .transfert_playback_to_device(device_id, persist_device_id)
          .await;
      }
      #[cfg(feature = "streaming")]
      IoEvent::AutoSelectStreamingDevice(device_name, persist_device_id, yield_to_external) => {
        self
          .auto_select_streaming_device(device_name, persist_device_id, yield_to_external)
          .await;
      }
      #[cfg(not(feature = "streaming"))]
      IoEvent::AutoSelectStreamingDevice(..) => {} // No-op without native streaming
      IoEvent::GetAlbumForTrack(track_id) => {
        if let Some(id) = ids::track_id(&track_id) {
          self.get_album_for_track(id).await;
        }
      }
      IoEvent::Shuffle(shuffle_state) => {
        self.shuffle(shuffle_state).await;
      }
      IoEvent::CurrentUserSavedTracksContains(track_ids) => {
        self
          .current_user_saved_tracks_contains(ids::track_ids(&track_ids))
          .await;
      }
      IoEvent::GetCurrentUserSavedShows(offset) => {
        self.get_current_user_saved_shows(offset).await;
      }
      IoEvent::CurrentUserSavedShowsContains(show_ids) => {
        self
          .current_user_saved_shows_contains(ids::show_ids(&show_ids))
          .await;
      }
      IoEvent::CurrentUserSavedShowDelete(show_id) => {
        if let Some(id) = ids::show_id(&show_id) {
          self.current_user_saved_shows_delete(id).await;
        }
      }
      IoEvent::CurrentUserSavedShowAdd(show_id) => {
        if let Some(id) = ids::show_id(&show_id) {
          self.current_user_saved_shows_add(id).await;
        }
      }
      IoEvent::GetShowEpisodes(show) => {
        self.get_show_episodes(show).await;
      }
      IoEvent::GetShow(show_id) => {
        if let Some(id) = ids::show_id(&show_id) {
          self.get_show(id).await;
        }
      }
      IoEvent::GetCurrentShowEpisodes(show_id, offset) => {
        if let Some(id) = ids::show_id(&show_id) {
          self.get_current_show_episodes(id, offset).await;
        }
      }
      IoEvent::AddItemToQueue(uri) => {
        if let Some(id) = ids::playable_id(&uri) {
          self.add_item_to_queue(id).await;
        }
      }
      IoEvent::GetQueue => {
        self.get_queue().await;
      }
      // Consumed by the queue router before it reaches the network; only lands
      // here if the router somehow let it through. No Spotify work to do.
      IoEvent::AdvanceNativeQueue => {}
      #[cfg(feature = "streaming")]
      IoEvent::ReplayPublishedSpotifyQueueSlot => {}
      // Consumed by a per-source router when a decoded source owns playback; only
      // lands here otherwise (e.g. Spotify is playing). No Spotify work to do.
      IoEvent::ReplayCurrentTrack => {}
      IoEvent::ResumeSpotifyContext(context_uri, resume_track_uri) => {
        self
          .resume_spotify_context(context_uri, resume_track_uri)
          .await;
      }
      #[cfg(feature = "streaming")]
      IoEvent::ToggleNativeShuffleSession(on) => {
        self.toggle_native_shuffle_session(on).await;
      }
      #[cfg(feature = "streaming")]
      IoEvent::ReshuffleNativeShuffleLap => {
        self.reshuffle_native_shuffle_lap().await;
      }
      #[cfg(feature = "streaming")]
      IoEvent::ResumeNativeShuffleSession(resume_index, generation) => {
        self
          .resume_native_shuffle_session(resume_index, generation)
          .await;
      }
      // Only constructed under `streaming`; inert otherwise.
      #[cfg(not(feature = "streaming"))]
      IoEvent::ToggleNativeShuffleSession(_)
      | IoEvent::ReshuffleNativeShuffleLap
      | IoEvent::ResumeNativeShuffleSession(_, _) => {}
      IoEvent::IncrementGlobalSongCount => {
        self.increment_global_song_count().await;
      }
      IoEvent::FetchGlobalSongCount => {
        self.fetch_global_song_count().await;
      }
      IoEvent::FetchAnnouncements => {
        self.fetch_announcements().await;
      }
      #[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
      IoEvent::DjToolCall(payload) => {
        let (call, responder) = *payload;
        self.run_dj_tool_call(call, responder).await;
      }
      #[cfg(feature = "ai-dj")]
      IoEvent::AskDj(request) => {
        self.ask_dj(*request).await;
      }
      #[cfg(feature = "ai-dj")]
      IoEvent::DjTopUp(generation, turn_seq) => {
        self.dj_top_up(generation, turn_seq).await;
      }
      #[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
      IoEvent::DjIndexLibrary => {
        self.dj_index_library().await;
      }
      IoEvent::FetchAllPlaylistTracksAndSort(playlist_id) => {
        if let Some(id) = ids::playlist_id(&playlist_id) {
          self.fetch_all_playlist_tracks_and_sort(id).await;
        }
      }
      IoEvent::SearchTracksForPlaylist(query) => {
        self.search_tracks_for_playlist(query).await;
      }
      IoEvent::CreateNewPlaylist(name, track_ids) => {
        self
          .create_new_playlist(name, ids::track_ids(&track_ids))
          .await;
      }
      IoEvent::LoadListeningStats(period) => {
        self.load_listening_stats(period).await;
      }
      IoEvent::GenerateRecap(period) => {
        self.generate_recap(period).await;
      }
      IoEvent::BeginSpotifyLogin => {
        self.begin_spotify_login().await;
      }
      IoEvent::CompleteSpotifyLogin(callback_url) => {
        self.complete_spotify_login(callback_url).await;
      }
      IoEvent::CancelSpotifyLogin => {
        self.cancel_spotify_login().await;
      }
      // Local-files browse events are handled by infra::local::dispatch before
      // reaching the network; they only arrive here when the feature is off.
      IoEvent::GetLocalPlaylists | IoEvent::GetLocalTracks(_) => {}
      // Subsonic browse/search events are handled by infra::subsonic::dispatch
      // before reaching the network; they only arrive here when the feature is off.
      IoEvent::GetSubsonicPlaylists
      | IoEvent::GetSubsonicTracks(_)
      | IoEvent::GetSubsonicSearchResults(_) => {}
      // Radio browse/search events are handled by infra::radio::dispatch before
      // reaching the network; they only arrive here when the feature is off.
      IoEvent::GetRadioStations | IoEvent::GetRadioSearchResults(_) => {}
      // YouTube search/playlist events are handled by infra::youtube::dispatch
      // before reaching the network; they only arrive here when the feature is
      // off.
      IoEvent::GetYouTubeSearchResults(_)
      | IoEvent::GetYouTubePlaylists
      | IoEvent::GetYouTubeTracks(_)
      | IoEvent::CreateYouTubePlaylist(_)
      | IoEvent::DeleteYouTubePlaylist(_)
      | IoEvent::AddTrackToYouTubePlaylist(..)
      | IoEvent::RemoveTrackFromYouTubePlaylist(..) => {}
    };

    {
      // Ticket 16: the end-of-handler reset is a fact; the fold clears the
      // spinner (the catalog completion owns the generation-guarded search flag).
      // Ack the terminal reset so callers cannot observe a stale spinner after
      // an auth-gate rejection or handler completion.
      self.emit_session_acked(SessionFact::LoadingCleared).await;
    }
  }

  /// Emit one session fact through the spine (no-op without a fold, i.e.
  /// in tests that never installed one).
  pub(crate) async fn emit_session(&self, fact: SessionFact) {
    if let Some(spine) = self.spine.as_ref() {
      spine.emit(EngineEvent::Session(fact)).await;
    }
  }

  /// Emit one session fact and wait for the fold to apply it, so a
  /// write-then-decide path cannot report success for a dropped event
  /// (ticket 17: the CLI's error surfacing). `true` when applied, or when
  /// no fold is live (facts are no-ops there).
  pub(crate) async fn emit_session_acked(&self, fact: SessionFact) -> bool {
    match self.spine.as_ref() {
      Some(spine) => spine.emit_acked(EngineEvent::Session(fact)).await.is_some(),
      None => true,
    }
  }

  /// Ask the fold for published state (ticket 17: command surfaces read
  /// here instead of locking App). `None` when no fold is live — the same
  /// contract as `emit_session`'s no-op, and the reason CLI mode opens one.
  pub(crate) async fn fold_query(&self, query: FoldQuery) -> Option<FoldAnswer> {
    let spine = self.spine.as_ref()?;
    spine.query(query).await.ok()
  }

  /// Emit a status notice (the old `show_status_message`, now a fact; the
  /// fold scales the TTL by the user config exactly as the old inline write
  /// did through `App::set_status_message`).
  async fn emit_status(&self, message: String, ttl_secs: u64) {
    self
      .emit_session(SessionFact::Status {
        message,
        ttl_secs,
        is_error: false,
      })
      .await;
  }

  /// The post-handler spinner clear, plus the conditional
  /// `pending_playlist_open` clear the auth-gate reject paths did.
  async fn emit_loading_cleared(&self, pending_playlist_id: &Option<String>) {
    if let Some(id) = pending_playlist_id {
      self
        .emit_session_acked(SessionFact::ClearPendingPlaylistOpen(Some(id.clone())))
        .await;
    }
    // Ack the terminal reset so callers cannot observe a stale spinner after
    // an auth-gate rejection or handler completion.
    self.emit_session_acked(SessionFact::LoadingCleared).await;
  }

  /// Route a handler failure through the fold as a fact (the old inline
  /// `self.handle_error(e)` lock site). Ack it because the CLI reads the
  /// latched error immediately after the handler returns.
  async fn emit_error(&self, e: anyhow::Error) {
    self
      .emit_session_acked(SessionFact::Error(format!("{e:#}")))
      .await;
  }

  async fn handle_error(&mut self, e: anyhow::Error) {
    // Ticket 16: the error page hint + TTL flows through the fold.
    self.emit_error(e).await;
  }

  /// Aggregate local listening history for the Stats screen. Reads the
  /// listens file off the runtime threads; never touches Spotify.
  async fn load_listening_stats(&mut self, period: crate::infra::history::RecapPeriod) {
    use crate::infra::history;

    let result = tokio::task::spawn_blocking(move || {
      let listens = history::load_listens()?;
      let filtered = history::filter_listens_for_period(&listens, period);
      Ok::<_, anyhow::Error>((
        history::build_stats_data(&filtered),
        history::compute_streaks(&listens),
      ))
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|result| result);

    match result {
      Ok((stats, streaks)) => {
        self
          .emit_session(SessionFact::StatsLoaded {
            period,
            stats,
            streaks,
          })
          .await;
      }
      Err(error) => {
        // Acked: the loading flags must be down before the caller decides.
        self
          .emit_session_acked(SessionFact::StatsLoadingCleared)
          .await;
        self
          .handle_error(anyhow!("failed to load listening history: {}", error))
          .await;
      }
    }
  }

  /// Export the shareable HTML recap off the runtime threads and open it in
  /// the browser; reads only local history, never Spotify.
  async fn generate_recap(&mut self, period: crate::infra::history::RecapPeriod) {
    use crate::infra::history;

    let result = tokio::task::spawn_blocking(move || {
      let output_path = history::recap_output_path()?;
      let count = history::export_history_recap(period, &output_path)?;
      Ok::<_, anyhow::Error>((output_path, count))
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|result| result);

    match result {
      Ok((output_path, count)) => {
        if let Err(e) = open::that(&output_path) {
          log::warn!("failed to open recap in browser: {}", e);
        }
        self
          .emit_status(
            format!(
              "Listening recap generated at {} ({} listens)",
              output_path.display(),
              count
            ),
            5,
          )
          .await;
      }
      Err(error) => {
        self
          .emit_status(format!("Failed to generate recap: {}", error), 5)
          .await;
      }
    }
  }

  async fn show_status_message(&self, message: String, ttl_secs: u64) {
    // Ticket 16: the TTL scaling happened under the lock before; the fold
    // now does it through the same `App::set_status_message` sink.
    self.emit_status(message, ttl_secs).await;
  }

  async fn refresh_authentication(&mut self) {
    self.ensure_authentication_fresh(true).await;
  }

  async fn ensure_authentication_fresh(&mut self, force: bool) -> bool {
    // No Spotify session (free-source launch): there is no token to refresh.
    // Spotify-bound events never reach here in that state because the auth
    // gate in `handle_network_event` rejects them first; the only caller that
    // can hit this branch is `RefreshAuthentication`, for which a no-op is
    // correct.
    let Some(spotify) = self.spotify.as_ref() else {
      self
        .emit_session(SessionFact::AuthMirrors {
          token_expiry: None,
          refresh_in_progress: false,
          connected: false,
        })
        .await;
      return false;
    };
    match auth::refresh_token_and_cache(spotify, &self.token_cache_path, force).await {
      Ok(expiry) => {
        self
          .emit_session(SessionFact::Auth {
            transition: auth::AuthTransition::TokenRefreshed,
            new_expiry: Some(expiry),
            post_connect_fetches: Vec::new(),
          })
          .await;
        true
      }
      Err(e) => {
        self
          .emit_session(SessionFact::AuthMirrors {
            token_expiry: None,
            refresh_in_progress: false,
            connected: true,
          })
          .await;
        // The old inline path cleared the spinner alongside the mirrors;
        // keep that pairing so a failed gate never leaves it stuck.
        self.emit_session(SessionFact::LoadingCleared).await;
        self.handle_error(anyhow!(e)).await;
        false
      }
    }
  }
  /// Start an in-TUI Spotify OAuth login: build the PKCE client, open the browser,
  /// and spawn a callback server that reports back via `CompleteSpotifyLogin`
  /// (or `CancelSpotifyLogin` on timeout/failure). Non-blocking: the UI keeps
  /// rendering while the browser round-trips.
  async fn begin_spotify_login(&mut self) {
    if self.spotify.is_some() {
      // Already connected — nothing to log into.
      return;
    }
    if self.pending_login.is_some() {
      self
        .show_status_message("Spotify login already in progress...".to_string(), 4)
        .await;
      return;
    }

    let config_paths = match self.client_config.get_or_build_paths() {
      Ok(paths) => paths,
      Err(e) => {
        self
          .show_status_message(format!("Spotify login setup failed: {e}"), 8)
          .await;
        return;
      }
    };

    let (spotify, authorize_url, port, token_cache_path) =
      match auth::prepare_interactive_login(&self.client_config, &config_paths) {
        Ok(prepared) => prepared,
        Err(e) => {
          self
            .show_status_message(format!("Spotify login setup failed: {e}"), 8)
            .await;
          return;
        }
      };

    if let Err(e) = open::that(&authorize_url) {
      log::warn!("[login] failed to open browser automatically: {e}");
      self
        .show_status_message(
          format!("Open this URL in your browser to log in to Spotify: {authorize_url}"),
          30,
        )
        .await;
    } else {
      self
        .show_status_message("Opening browser to log in to Spotify...".to_string(), 12)
        .await;
    }

    // Keep the PKCE client on `self`: PKCE stores the code_verifier inside it and
    // the token exchange in `complete_spotify_login` must reuse this instance.
    self.pending_login = Some(PendingLogin {
      spotify,
      token_cache_path,
    });

    // Drive the callback server off a spawned task so the pump/UI stay responsive.
    // The task carries only the sender + port, never the client. The channel
    // handle comes from the fold (ticket 17) — same contract as
    // `FoldAnswer::StreamingPlayer`: a cloned handle, not shared state.
    let io_tx = match self
      .fold_query(crate::runtime::engine::FoldQuery::IoTx)
      .await
    {
      Some(FoldAnswer::IoTx(Some(io_tx))) => io_tx,
      _ => return,
    };
    tokio::spawn(async move {
      let overall_timeout = Duration::from_secs(180);
      match tokio::time::timeout(overall_timeout, redirect_uri_web_server_async(port)).await {
        Ok(Ok(url)) => {
          let _ = io_tx.send(IoEvent::CompleteSpotifyLogin(url));
        }
        Ok(Err(())) => {
          log::warn!("[login] callback server failed to start");
          let _ = io_tx.send(IoEvent::CancelSpotifyLogin);
        }
        Err(_) => {
          log::warn!(
            "[login] login timed out after {}s",
            overall_timeout.as_secs()
          );
          let _ = io_tx.send(IoEvent::CancelSpotifyLogin);
        }
      }
    });
  }

  /// Finish an in-TUI Spotify login from the OAuth callback URL. The token
  /// exchange runs on the SAME PKCE client that produced the authorize URL.
  /// Native streaming still requires a restart (its init happens pre-TUI).
  async fn complete_spotify_login(&mut self, callback_url: String) {
    let Some(pending) = self.pending_login.take() else {
      return;
    };
    let PendingLogin {
      spotify,
      token_cache_path,
    } = pending;

    let Some(code) = spotify.parse_response_code(&callback_url) else {
      self
        .show_status_message("Spotify login failed: invalid callback URL.".to_string(), 8)
        .await;
      return;
    };

    if let Err(e) = spotify.request_token(&code).await {
      self
        .show_status_message(format!("Spotify login failed: {e}"), 8)
        .await;
      return;
    }

    if let Err(e) = auth::save_token_to_file(&spotify, &token_cache_path).await {
      log::warn!("[login] failed to cache token after login: {e}");
    }
    let expiry = auth::token_expiry(&spotify).await.ok();

    // A brand-new token family is in play, so the previous session's
    // forced-refresh cooldown carries no information about it.
    requests::reset_forced_refresh_cooldown().await;

    self.spotify = Some(spotify);
    self.token_cache_path = token_cache_path;
    // Ticket 16: connect is a fact. The fold transitions the auth domain,
    // syncs the mirrors, and dispatches the post-connect fetches — the
    // exact sequence the old single lock section ran.
    self
      .emit_session(SessionFact::Auth {
        transition: auth::AuthTransition::Connected,
        new_expiry: expiry,
        post_connect_fetches: vec![
          PostConnectFetch::User,
          PostConnectFetch::Playlists,
          PostConnectFetch::CurrentPlayback,
        ],
      })
      .await;
    self
      .emit_status(
        "Spotify connected. Restart spotatui to enable native playback.".to_string(),
        10,
      )
      .await;
  }

  /// Clear an abandoned in-TUI login (callback timed out or failed) so the user
  /// can retry.
  async fn cancel_spotify_login(&mut self) {
    if self.pending_login.take().is_some() {
      self
        .show_status_message(
          "Spotify login timed out. Press `d` and pick Spotify to try again.".to_string(),
          6,
        )
        .await;
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::core::app::App;
  use crate::core::config::ClientConfig;
  use crate::core::user_config::UserConfig;
  use chrono::{TimeDelta, Utc};
  use rspotify::{Config, Credentials, OAuth, Token};
  use std::time::SystemTime;

  async fn spotify_with_token(token: Token) -> AuthCodePkceSpotify {
    let spotify = AuthCodePkceSpotify::with_config(
      Credentials::new_pkce("test_client_id"),
      OAuth {
        redirect_uri: "http://localhost:8888/callback".to_string(),
        ..Default::default()
      },
      Config::default(),
    );

    let mut token_lock = spotify.token.lock().await.expect("Failed to lock token");
    *token_lock = Some(token);
    drop(token_lock);

    spotify
  }

  fn temp_token_cache_path() -> PathBuf {
    std::env::temp_dir().join(format!(
      "spotatui_network_test_token_{}.json",
      rand::random::<u32>()
    ))
  }

  #[test]
  fn every_service_lane_event_bypasses_spotify_auth() {
    use crate::infra::history::RecapPeriod;
    let events = [
      IoEvent::FetchGlobalSongCount,
      IoEvent::IncrementGlobalSongCount,
      IoEvent::FetchAnnouncements,
      IoEvent::LoadListeningStats(RecapPeriod::SevenDays),
      IoEvent::GenerateRecap(RecapPeriod::SevenDays),
    ];
    for event in events {
      assert!(Network::runs_on_service_lane(&event));
      assert!(Network::event_bypasses_spotify_auth(&event));
    }

    // The DJ's two service-lane events, which the array above cannot hold: they
    // only exist under `ai-dj`. They are the whole reason this drift matters —
    // a brain call left on the serial pump blocks every other event for minutes.
    #[cfg(feature = "ai-dj")]
    for event in [
      IoEvent::AskDj(Box::new(crate::infra::dj::AskDjRequest {
        extra_instruction: None,
        generation: 0,
        must_act: false,
        turn_seq: 0,
        vibe_on_success: None,
      })),
      IoEvent::DjTopUp(0, 0),
    ] {
      assert!(Network::runs_on_service_lane(&event));
      assert!(Network::event_bypasses_spotify_auth(&event));
    }
  }

  /// The queue router normally consumes this event first. If that invariant is
  /// ever broken, its network fallback is still a no-op and must not trigger a
  /// token refresh or login prompt before being discarded.
  #[cfg(feature = "streaming")]
  #[test]
  fn queue_slot_replay_fallback_bypasses_auth_but_stays_serial() {
    let event = IoEvent::ReplayPublishedSpotifyQueueSlot;
    assert!(Network::event_bypasses_spotify_auth(&event));
    assert!(
      !Network::runs_on_service_lane(&event),
      "the queue router owns replay ordering on the serial event pump"
    );
  }

  /// `DjToolCall` is the one event that bypasses the auth gate *without* moving
  /// onto the service lane, so it gets its own assertion.
  ///
  /// Both halves matter. It must stay on the serial lane because resolving a
  /// track name needs the real Spotify client, and the service lane builds its
  /// `Network` with `None` for it. It must bypass the gate so the handler can
  /// answer an unauthenticated caller with a diagnosable message instead of
  /// dropping the `oneshot`. Both front doors block on that channel: an MCP
  /// client through the server, and the in-TUI DJ through its own tool loop.
  #[cfg(any(feature = "mcp-server", feature = "ai-dj"))]
  #[test]
  fn dj_tool_calls_bypass_auth_but_stay_on_the_serial_lane() {
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let event = IoEvent::DjToolCall(Box::new((
      crate::infra::dj::tools::DjToolCall::GetNowPlaying,
      tx,
    )));
    assert!(Network::event_bypasses_spotify_auth(&event));
    assert!(
      !Network::runs_on_service_lane(&event),
      "the service lane has no Spotify client, so the resolver could not run there"
    );
  }

  #[tokio::test]
  async fn pre_event_auth_failure_clears_loading_state() {
    let expired_token_without_refresh = Token {
      access_token: "expired_access_token".to_string(),
      refresh_token: None,
      expires_in: TimeDelta::seconds(3600),
      expires_at: Some(Utc::now() - TimeDelta::seconds(60)),
      scopes: Default::default(),
    };
    let spotify = spotify_with_token(expired_token_without_refresh).await;
    let token_cache_path = temp_token_cache_path();
    let (io_tx, _io_rx) = std::sync::mpsc::channel();
    let app = Arc::new(Mutex::new(App::new(
      io_tx,
      UserConfig::new(),
      Some(SystemTime::now() - Duration::from_secs(60)),
    )));

    {
      let mut app = app.lock().await;
      app.is_loading = true;
      app.auth_refresh_in_progress = true;
    }

    // Ticket 16: the handler emits facts; the fold reduces them before the
    // acked terminal reset releases the handler.
    let (spine, _snapshots) = crate::runtime::frontend::spawn_fold(&app).await;
    let mut network = Network::new(
      Some(spotify),
      ClientConfig::new(),
      Some(spine.clone()),
      None,
      token_cache_path.clone(),
    );
    network.spine = Some(spine.clone());
    network.handle_network_event(IoEvent::GetUser).await;

    let app = app.lock().await;
    assert!(!app.is_loading);
    assert!(!app.auth_refresh_in_progress);

    let _ = std::fs::remove_file(token_cache_path);
  }

  #[tokio::test]
  async fn auth_gate_clears_pending_playlist_open() {
    let (io_tx, _io_rx) = std::sync::mpsc::channel();
    let app = Arc::new(Mutex::new(App::new(
      io_tx,
      UserConfig::new(),
      Some(SystemTime::now()),
    )));
    app.lock().await.pending_playlist_open = Some("playlist-id".to_string());
    let (spine, _snapshots) = crate::runtime::frontend::spawn_fold(&app).await;
    let mut network = Network::new(
      None,
      ClientConfig::new(),
      Some(spine.clone()),
      None,
      temp_token_cache_path(),
    );
    network.spine = Some(spine.clone());

    network
      .handle_network_event(IoEvent::GetPlaylistItems("playlist-id".to_string(), 0))
      .await;

    // The terminal LoadingCleared fact is acked by the handler; one query
    // additionally yields to the fold before inspecting the mirror.
    spine
      .query(crate::runtime::engine::FoldQuery::SpotifyConnected)
      .await
      .unwrap();
    assert!(app.lock().await.pending_playlist_open.is_none());
  }
}
