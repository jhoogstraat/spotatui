//! Subsonic browse/search/playback routing.
//!
//! The seam that keeps the Spotify [`Network`](crate::infra::network)
//! Spotify-only: [`route_subsonic_event`] is called from the runtime IoEvent
//! pump *after* the local-files dispatch and *before* `handle_network_event`.
//! When an event targets the Subsonic source (a browse/search request, or a
//! `subsonic:` playback URI) it is handled here and consumed; otherwise it falls
//! through to the normal Spotify dispatch.
//!
//! Unlike the local-files source, Subsonic's [`MediaSource`]/[`Searcher`] methods
//! are genuinely async (reqwest REST calls), so they are awaited directly rather
//! than run on the blocking pool.
//!
//! ## Decoupling & device ownership
//!
//! Subsonic playback owns a single piece of state, [`App::subsonic_playback`],
//! and never writes Spotify/librespot fields — the playbar reads progress/pause
//! live from the player. Only one backend holds the audio device at a time:
//! starting Subsonic pauses librespot **and** tears down any local session; the
//! reciprocal teardown lives in the local and network start paths.
//!
//! ## Streaming
//!
//! Each track is downloaded from `stream.view` to a tempfile (off the `App`
//! lock), then played from disk through the shared [`LocalPlayer`]. The download
//! window is much longer than a local decode, so the `advancing` guard is
//! load-bearing, and a download failure tears the session down rather than
//! skipping past (avoids cascading the whole queue when the network is down).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tempfile::NamedTempFile;

use super::{track_id_from_uri, SubsonicPlaybackState, SubsonicSource};
use crate::core::catalog::CatalogStateEvent;
use crate::core::pagination::Paged;
use crate::core::plugin_api::TrackInfo;
use crate::core::source::{MediaSource, Searcher, Source};
use crate::infra::audio::LocalPlayer;
use crate::infra::network::IoEvent;
use crate::infra::queue::replay_file;
use crate::runtime::engine::{
  DecodedSourceAnswer as Answer, DecodedSourceFact as Fact, DecodedSourceQuery as Query,
  EngineEvent, FoldAnswer, FoldQuery, Spine,
};

/// Environment variable that overrides the configured Subsonic password. Prefer
/// it over the plaintext config field so the secret is never written to disk.
const PASSWORD_ENV: &str = "SPOTATUI_SUBSONIC_PASSWORD";

/// Whether a URI is owned by the Subsonic source.
fn is_subsonic_uri(uri: &str) -> bool {
  uri.starts_with("subsonic:")
}

/// Intercept events that target the Subsonic source.
///
/// Returns `true` if the event was handled (and must **not** be forwarded to the
/// Spotify network), `false` to let the normal dispatch run.
pub async fn route_subsonic_event(spine: &Spine, event: &IoEvent) -> bool {
  match event {
    IoEvent::GetSubsonicPlaylists => {
      load_subsonic_playlists(spine).await;
      true
    }
    IoEvent::GetSubsonicTracks(uri) => {
      load_subsonic_tracks(spine, uri).await;
      true
    }
    IoEvent::GetSubsonicSearchResults(query) => {
      run_subsonic_search(spine, query).await;
      true
    }
    // Start a playlist of subsonic tracks: queue all and start at the offset.
    IoEvent::StartPlayback(None, Some(uris), offset)
      if uris.first().is_some_and(|u| is_subsonic_uri(u)) =>
    {
      start_subsonic_queue(spine, uris, offset.unwrap_or(0)).await;
      true
    }
    // A single subsonic track with no surrounding playlist: a one-track queue.
    IoEvent::StartPlayback(Some(uri), _, _) if is_subsonic_uri(uri) => {
      start_subsonic_queue(spine, std::slice::from_ref(uri), 0).await;
      true
    }
    // Bare "resume current" — ours only while subsonic owns the session.
    IoEvent::StartPlayback(None, None, None) => match player(spine).await {
      Some(p) => {
        p.resume();
        true
      }
      None => false,
    },
    // Any other start is a local/Spotify play: relinquish the device, then let
    // the normal dispatch run.
    IoEvent::StartPlayback(..) => {
      teardown_subsonic(spine).await;
      false
    }
    IoEvent::PausePlayback => match player(spine).await {
      Some(p) => {
        p.pause();
        true
      }
      None => false,
    },
    IoEvent::Seek(position_ms) => match player(spine).await {
      Some(p) => {
        let _ = p.seek(Duration::from_millis(*position_ms as u64));
        true
      }
      None => false,
    },
    IoEvent::ChangeVolume(volume) => match player(spine).await {
      Some(p) => {
        p.set_volume(*volume as f32 / 100.0);
        emit(spine, Fact::Volume(*volume)).await;
        true
      }
      None => false,
    },
    IoEvent::NextTrack => skip(spine, true).await,
    IoEvent::PreviousTrack | IoEvent::ForcePreviousTrack => skip(spine, false).await,
    // Repeat-one auto-advance: replay the current track (dispatched by the runner
    // tick only while Subsonic owns the session).
    IoEvent::ReplayCurrentTrack => replay_current(spine).await,
    // Explicit repeat-mode set (e.g. a plugin's `set_repeat`): store it as the
    // decoded repeat mode while a queueable decoded source owns playback (the
    // method's ownership gate excludes a context suspended under the native
    // queue), so it never reaches the Spotify context.
    IoEvent::Repeat(state) => matches!(
      decoded(spine, Query::SubsonicRepeat(*state)).await,
      Some(Answer::SubsonicRepeat(true))
    ),
    _ => false,
  }
}

async fn decoded(spine: &Spine, query: Query) -> Option<Answer> {
  match spine.query(FoldQuery::Decoded(query)).await.ok()? {
    FoldAnswer::Decoded(answer) => Some(answer),
    _ => None,
  }
}

async fn emit(spine: &Spine, fact: Fact) {
  let _ = spine.emit_acked(EngineEvent::Decoded(fact)).await;
}

async fn status(spine: &Spine, message: impl Into<String>, ttl_secs: u64) {
  emit(
    spine,
    Fact::SubsonicStatus {
      message: message.into(),
      ttl_secs,
    },
  )
  .await;
}

// ---------------------------------------------------------------------------
// Browse + search
// ---------------------------------------------------------------------------

/// Build a [`SubsonicSource`] from the saved server config, with the password
/// taken from the `SPOTATUI_SUBSONIC_PASSWORD` env var when set. Returns `None`
/// (after surfacing a status message) when no server URL is configured.
pub(crate) async fn build_source(spine: &Spine) -> Option<SubsonicSource> {
  let (url, username, config_password) = match decoded(spine, Query::SubsonicConfig).await {
    Some(Answer::SubsonicConfig {
      url,
      username,
      password,
    }) => (url, username, password),
    _ => return None,
  };

  let Some(url) = url else {
    status(
      spine,
      "No Subsonic server configured (set behavior.subsonic_url)",
      6,
    )
    .await;
    return None;
  };

  // Env override takes precedence over the plaintext config field.
  let password = std::env::var(PASSWORD_ENV)
    .ok()
    .or(config_password)
    .unwrap_or_default();

  Some(SubsonicSource::new(
    url,
    username.unwrap_or_default(),
    password,
  ))
}

/// Fetch the user's server playlists into `app.subsonic_playlists`.
async fn load_subsonic_playlists(spine: &Spine) {
  let Some(source) = build_source(spine).await else {
    return;
  };
  match source.playlists().await {
    Ok(playlists) => {
      emit(spine, Fact::SubsonicPlaylists(playlists)).await;
    }
    Err(e) => status(spine, format!("Cannot load Subsonic playlists: {e}"), 6).await,
  }
}

/// Fetch a playlist's tracks into the shared track table, tagged
/// [`TrackTableContext::SubsonicPlaylist`] so selecting a row plays it.
async fn load_subsonic_tracks(spine: &Spine, playlist_uri: &str) {
  let Some(source) = build_source(spine).await else {
    return;
  };
  match source.tracks(playlist_uri).await {
    Ok(tracks) => {
      emit(spine, Fact::SubsonicTracks(tracks)).await;
    }
    Err(e) => status(spine, format!("Cannot load Subsonic playlist: {e}"), 6).await,
  }
}

/// Run a catalog search and populate `app.search_results`.
///
/// M2 populates **only** the songs block: album/artist search-result Enter
/// dispatches rspotify-bound `GetAlbum`/`GetArtist` events that fail for Subsonic
/// ids, so those blocks are left `None` to avoid dead rows. Album/artist
/// drill-down is a tracked follow-up.
/// Run a catalog search and populate the visible rows **through the fold**
/// (ticket 15): the handler emits typed events; `search_results` is written
/// by `core::catalog_apply` on the fold task, which enforces the
/// stale-request contract.
async fn run_subsonic_search(spine: &Spine, query: &str) {
  spine
    .emit(EngineEvent::Catalog(CatalogStateEvent::SearchRequested {
      source: Source::Subsonic,
      query: query.to_string(),
      scoped_loading: true,
    }))
    .await;
  let Some(source) = build_source(spine).await else {
    return;
  };
  match source.search(query).await {
    Ok(results) => {
      let total = results.tracks.len() as u32;
      spine
        .emit(EngineEvent::Catalog(CatalogStateEvent::SearchCompleted {
          source: Source::Subsonic,
          query: query.to_string(),
          pages: Box::new(crate::core::catalog::SearchCompletionPages {
            tracks: Some(Paged {
              items: results.tracks,
              total,
              ..Default::default()
            }),
            ..Default::default()
          }),
        }))
        .await;
    }
    Err(e) => status(spine, format!("Subsonic search failed: {e}"), 6).await,
  }
}

// ---------------------------------------------------------------------------
// Playback
// ---------------------------------------------------------------------------

/// The live subsonic player, if a subsonic session is active.
async fn player(spine: &Spine) -> Option<Arc<LocalPlayer>> {
  match decoded(spine, Query::SubsonicPlayer).await? {
    Answer::SubsonicPlayer(player) => player,
    _ => None,
  }
}

/// Snapshot the `TrackInfo`s for `uris`, preserving order, looking each up in
/// **both** browse views: the track table (browse→play) and the search results
/// (search→play). A playback request can originate from either — the track-table
/// Enter sends `track_table.tracks` uris, the search-result Enter sends
/// `search_results.tracks` uris — so the metadata may live in either. Any uri
/// found in neither is dropped.
pub(crate) fn snapshot_tracks(
  table: &[TrackInfo],
  search: Option<&[TrackInfo]>,
  uris: &[String],
) -> Vec<TrackInfo> {
  uris
    .iter()
    .filter_map(|uri| find_track(table, search, uri).cloned())
    .collect()
}

/// Find a track's metadata by URI in the track table, falling back to the search
/// results.
fn find_track<'a>(
  table: &'a [TrackInfo],
  search: Option<&'a [TrackInfo]>,
  uri: &str,
) -> Option<&'a TrackInfo> {
  let matches = |t: &&TrackInfo| t.uri.as_deref() == Some(uri);
  table
    .iter()
    .find(matches)
    .or_else(|| search.and_then(|s| s.iter().find(matches)))
}

/// Release the other two backends so only subsonic holds the output device.
#[cfg_attr(
  not(any(
    feature = "streaming",
    feature = "local-files",
    feature = "internet-radio",
    feature = "youtube"
  )),
  allow(unused_variables)
)]
async fn release_other_backends(spine: &Spine, clear_subsonic: bool) {
  let backends = decoded(
    spine,
    Query::ClaimBackendPlayers {
      keep: (!clear_subsonic).then_some(crate::core::source::Source::Subsonic),
    },
  )
  .await;
  if let Some(Answer::BackendPlayers {
    #[cfg(feature = "streaming")]
    streaming,
    #[cfg(feature = "local-files")]
    local,
    #[cfg(feature = "internet-radio")]
    radio,
    #[cfg(feature = "youtube")]
    youtube,
    #[cfg(feature = "subsonic")]
      subsonic: _,
  }) = backends
  {
    #[cfg(feature = "streaming")]
    if let Some(player) = streaming {
      player.pause();
    }
    #[cfg(feature = "local-files")]
    if let Some(player) = local {
      player.stop();
    }
    #[cfg(feature = "internet-radio")]
    if let Some(player) = radio {
      player.stop();
    }
    #[cfg(feature = "youtube")]
    if let Some(player) = youtube {
      player.stop();
    }
  }
}

/// Reuse the live subsonic player, or open a fresh output device for one. A
/// freshly opened player is **not** published to `App` here — the caller
/// publishes the session only on a successful first play.
async fn acquire_player(spine: &Spine) -> Option<Arc<LocalPlayer>> {
  match tokio::task::spawn_blocking(LocalPlayer::new).await {
    Ok(Ok(p)) => Some(Arc::new(p)),
    Ok(Err(e)) => {
      status(
        spine,
        format!("No audio output for Subsonic playback: {e}"),
        6,
      )
      .await;
      None
    }
    Err(e) => {
      status(spine, format!("Audio output init failed: {e}"), 6).await;
      None
    }
  }
}

/// Download a track's audio into a fresh tempfile. Must be awaited **without**
/// holding the `App` lock (it can take seconds).
async fn download_track(source: &SubsonicSource, track_id: &str) -> Result<NamedTempFile> {
  let tmp = NamedTempFile::new().context("creating temp file for Subsonic stream")?;
  source.download_track(track_id, tmp.path()).await?;
  Ok(tmp)
}

/// Download the track at `uri` into a tempfile, for the native queue engine's
/// off-pump fetch (playing is the queue's job — it re-checks that the queue
/// slot is still current before touching the shared player).
pub(crate) async fn download_for_queue(
  source: &SubsonicSource,
  uri: &str,
) -> Result<NamedTempFile> {
  let track_id = track_id_from_uri(uri)?.to_string();
  download_track(source, &track_id).await
}

/// Begin playing a queue of subsonic tracks, taking over the session and
/// starting at `start_idx` (clamped into range).
async fn start_subsonic_queue(spine: &Spine, uris: &[String], start_idx: usize) {
  let tracks = match decoded(spine, Query::SubsonicTracks(uris.to_vec())).await {
    Some(Answer::SubsonicTracks(tracks)) => tracks,
    _ => Vec::new(),
  };
  if tracks.is_empty() {
    status(spine, "No Subsonic tracks to play", 6).await;
    return;
  }
  let index = start_idx.min(tracks.len() - 1);

  let track_id = match tracks[index].uri.as_deref().map(track_id_from_uri) {
    Some(Ok(id)) => id.to_string(),
    _ => {
      status(spine, "Invalid Subsonic track URI", 6).await;
      return;
    }
  };

  let Some(source) = build_source(spine).await else {
    return;
  };
  let source = Arc::new(source);

  // Only one backend owns the device at a time.
  let existing_player = player(spine).await;
  release_other_backends(spine, existing_player.is_none()).await;

  let Some(player) = (match existing_player {
    Some(player) => Some(player),
    None => acquire_player(spine).await,
  }) else {
    return;
  };

  // Download off the lock, then decode on the blocking pool.
  let tmp = match download_track(&source, &track_id).await {
    Ok(t) => t,
    Err(e) => {
      status(spine, format!("Cannot download Subsonic track: {e}"), 6).await;
      return;
    }
  };
  let path = tmp.path().to_path_buf();
  let decode_player = Arc::clone(&player);
  let result = tokio::task::spawn_blocking(move || decode_player.play_file(&path)).await;

  match result {
    Ok(Ok(())) => {
      if let Some(Answer::Volume(volume)) = decoded(spine, Query::Volume).await {
        player.set_volume(volume as f32 / 100.0);
      }

      let display = tracks[index].name.clone();
      // Publish the session exactly once, now that the source is decoding.
      let mut state = SubsonicPlaybackState {
        player,
        source,
        tracks,
        index,
        advancing: false,
        tempfile: tmp,
        shuffle_backup: None,
      };
      // Honor the player-global decoded shuffle for the freshly-built queue.
      let shuffle = matches!(
        decoded(spine, Query::SubsonicShuffle).await,
        Some(Answer::SubsonicShuffle(true))
      );
      if shuffle {
        state.set_shuffle(true);
      }
      emit(spine, Fact::SubsonicPlayback(state)).await;
      status(spine, format!("\u{266a} {display}"), 4).await;
    }
    Ok(Err(e)) => status(spine, format!("Cannot play Subsonic track: {e}"), 6).await,
    Err(e) => status(spine, format!("Subsonic playback task failed: {e}"), 6).await,
  }
}

/// Move the subsonic queue index in `direction` and play the new track. Returns
/// `true` if subsonic owns the session (so the event is consumed).
async fn skip(spine: &Spine, forward: bool) -> bool {
  let Some(Answer::SubsonicSkip(target)) = decoded(spine, Query::SubsonicSkip { forward }).await
  else {
    return false;
  };

  match target {
    Some(idx) => play_index(spine, idx).await,
    None => emit(spine, Fact::SubsonicAdvancing(false)).await,
  }
  true
}

/// Replay the current Subsonic track (repeat-one auto-advance) via
/// [`replay_file`]. Re-decodes the **already-downloaded tempfile** — no second
/// network fetch — and on failure tears the session down (rather than leaving
/// an empty sink that would re-fire replay every tick). Returns `true` if
/// Subsonic owns the session.
async fn replay_current(spine: &Spine) -> bool {
  let Some(Answer::SubsonicReplay(Some((player, path)))) =
    decoded(spine, Query::SubsonicReplay).await
  else {
    return false;
  };
  if replay_file(player, path).await {
    emit(spine, Fact::SubsonicReplaySucceeded).await;
  } else {
    teardown_subsonic(spine).await;
    status(spine, "Cannot replay Subsonic track", 6).await;
  }
  true
}

/// Play the queued track at `target`, reusing the published session. Used by
/// Next/Previous and the runner tick's auto-advance.
///
/// A download failure tears the session down (deliberate divergence from the
/// local source's skip-past): on a network outage, walking the whole queue at
/// tick speed would spray error toasts, so one failure ends playback instead.
pub(crate) async fn play_index(spine: &Spine, target: usize) {
  let plan = match decoded(spine, Query::SubsonicPlayIndex(target)).await {
    Some(Answer::SubsonicPlayIndex(plan)) => plan,
    _ => return,
  };

  let (player, source, track_id, display) = match plan {
    Some(Ok(Some((p, s, id, display)))) => (p, s, id, display),
    Some(Ok(None)) => {
      emit(spine, Fact::SubsonicAdvancing(false)).await;
      return;
    }
    Some(Err(())) => {
      teardown_subsonic(spine).await;
      status(spine, "Invalid Subsonic track URI", 6).await;
      return;
    }
    None => return,
  };

  let tmp = match download_track(&source, &track_id).await {
    Ok(t) => t,
    Err(e) => {
      teardown_subsonic(spine).await;
      status(spine, format!("Cannot download Subsonic track: {e}"), 6).await;
      return;
    }
  };
  let path = tmp.path().to_path_buf();
  let decode_player = Arc::clone(&player);
  let result = tokio::task::spawn_blocking(move || decode_player.play_file(&path)).await;

  match result {
    Ok(Ok(())) => {
      emit(
        spine,
        Fact::SubsonicIndex {
          target,
          tempfile: tmp,
          display,
        },
      )
      .await;
    }
    Ok(Err(e)) => {
      teardown_subsonic(spine).await;
      status(spine, format!("Cannot play Subsonic track: {e}"), 6).await;
    }
    Err(e) => {
      teardown_subsonic(spine).await;
      status(spine, format!("Subsonic playback task failed: {e}"), 6).await;
    }
  }
}

/// Commit `target` as the live index, clear the auto-advance guard, and swap in
/// the new track's tempfile (dropping the previous one). Ordering is safe: the
/// blocking `play_file` already cleared the old source from the sink, so rodio
/// no longer holds the old file by the time it is dropped here.
/// End the subsonic session, releasing the output device and cleaning up the
/// current tempfile.
async fn teardown_subsonic(spine: &Spine) {
  emit(spine, Fact::SubsonicPlaybackCleared).await;
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::core::app::App;
  use tokio::sync::Mutex;

  fn track(uri: &str, name: &str) -> TrackInfo {
    TrackInfo {
      uri: Some(uri.to_string()),
      name: name.to_string(),
      artists: vec!["Artist".to_string()],
      album: "Album".to_string(),
      duration_ms: 1000,
      id: None,
      album_id: None,
      artist_refs: vec![],
      is_playable: true,
      is_local: false,
      track_number: 0,
      explicit: false,
      image_url: None,
    }
  }

  #[test]
  fn snapshot_finds_tracks_in_table_preserving_order() {
    let table = vec![
      track("subsonic:track:a", "A"),
      track("subsonic:track:b", "B"),
    ];
    let snap = snapshot_tracks(
      &table,
      None,
      &[
        "subsonic:track:b".to_string(),
        "subsonic:track:a".to_string(),
      ],
    );
    assert_eq!(snap.len(), 2);
    assert_eq!(snap[0].name, "B");
    assert_eq!(snap[1].name, "A");
  }

  #[test]
  fn snapshot_falls_back_to_search_results_for_search_to_play() {
    // The track table holds a previously-browsed playlist; the played uris come
    // from the search results. The lookup must consult both. (Regression: the
    // search->play path looked only at the table and found nothing.)
    let table = vec![track("subsonic:track:browsed", "Browsed")];
    let search = vec![track("subsonic:track:searched", "Searched")];
    let snap = snapshot_tracks(
      &table,
      Some(&search),
      &["subsonic:track:searched".to_string()],
    );
    assert_eq!(snap.len(), 1, "search-sourced uri must resolve");
    assert_eq!(snap[0].name, "Searched");
  }

  #[test]
  fn snapshot_drops_unknown_uris() {
    let table = vec![track("subsonic:track:a", "A")];
    let snap = snapshot_tracks(&table, None, &["subsonic:track:missing".to_string()]);
    assert!(snap.is_empty());
  }

  /// End-to-end dispatch test: drive `route_subsonic_event` exactly as the
  /// runtime pump does — browse, play, advance the queue, and tear down on a
  /// foreign start. Exercises the dispatch glue/state machine the direct-client
  /// smoke tests bypass. Ignored (needs network **and** an audio device); run:
  /// `cargo test --features subsonic -- --ignored live_dispatch`
  #[tokio::test]
  #[ignore = "hits the live demo server AND requires an audio output device"]
  async fn live_dispatch_browse_play_and_advance() {
    use crate::core::user_config::UserConfig;
    use std::sync::mpsc::channel;
    use std::time::SystemTime;

    let (tx, _rx) = channel();
    let mut app = App::new(tx, UserConfig::new(), Some(SystemTime::now()));
    app.user_config.behavior.subsonic_url = Some("https://demo.navidrome.org".to_string());
    app.user_config.behavior.subsonic_username = Some("demo".to_string());
    app.user_config.behavior.subsonic_password = Some("demo".to_string());
    let app = Arc::new(Mutex::new(app));
    let (spine, _) = crate::runtime::frontend::spawn_fold(&app).await;

    // Browse playlists, then a playlist's tracks into the shared table.
    assert!(route_subsonic_event(&spine, &IoEvent::GetSubsonicPlaylists).await);
    let playlist_uri = app
      .lock()
      .await
      .subsonic_playlists
      .first()
      .expect("demo has playlists")
      .uri
      .clone();
    assert!(route_subsonic_event(&spine, &IoEvent::GetSubsonicTracks(playlist_uri)).await);
    let uris: Vec<String> = app
      .lock()
      .await
      .track_table
      .tracks
      .iter()
      .filter_map(|t| t.uri.clone())
      .collect();
    assert!(
      uris.len() >= 2,
      "need a multi-track playlist to test advance"
    );

    // Start the queue at index 0 — downloads + plays the first track.
    assert!(
      route_subsonic_event(&spine, &IoEvent::StartPlayback(None, Some(uris), Some(0))).await,
      "subsonic StartPlayback must be consumed"
    );
    {
      let guard = app.lock().await;
      let s = guard.subsonic_playback.as_ref().expect("session published");
      assert_eq!(s.index, 0);
      assert!(!s.player.is_paused(), "should be playing");
    }

    // Advance — downloads + plays the next track, moving the index.
    assert!(route_subsonic_event(&spine, &IoEvent::NextTrack).await);
    {
      let guard = app.lock().await;
      let s = guard
        .subsonic_playback
        .as_ref()
        .expect("session still active");
      assert_eq!(s.index, 1, "Next should advance the queue index");
    }

    // A foreign (Spotify) start tears the session down and falls through.
    assert!(
      !route_subsonic_event(
        &spine,
        &IoEvent::StartPlayback(Some("spotify:track:x".to_string()), None, None)
      )
      .await,
      "a non-subsonic start must fall through to the network"
    );
    assert!(
      app.lock().await.subsonic_playback.is_none(),
      "a foreign start must tear down the subsonic session (device handoff)"
    );
  }

  /// Regression for the search->play path: search, then play a result **without**
  /// browsing it into the track table first, so the metadata can only come from
  /// the search results. Before the `snapshot_tracks` fix this published no
  /// session ("No Subsonic tracks to play").
  #[tokio::test]
  #[ignore = "hits the live demo server AND requires an audio output device"]
  async fn live_dispatch_search_to_play() {
    use crate::core::user_config::UserConfig;
    use std::sync::mpsc::channel;
    use std::time::SystemTime;

    let (tx, _rx) = channel();
    let mut app = App::new(tx, UserConfig::new(), Some(SystemTime::now()));
    app.user_config.behavior.subsonic_url = Some("https://demo.navidrome.org".to_string());
    app.user_config.behavior.subsonic_username = Some("demo".to_string());
    app.user_config.behavior.subsonic_password = Some("demo".to_string());
    let app = Arc::new(Mutex::new(app));
    let (spine, _) = crate::runtime::frontend::spawn_fold(&app).await;

    assert!(
      route_subsonic_event(
        &spine,
        &IoEvent::GetSubsonicSearchResults("love".to_string())
      )
      .await
    );
    let uris: Vec<String> = app
      .lock()
      .await
      .search_results
      .tracks
      .as_ref()
      .expect("search populated the songs block")
      .items
      .iter()
      .filter_map(|t| t.uri.clone())
      .collect();
    assert!(!uris.is_empty(), "search should return tracks");
    // Track table is empty (never browsed) — metadata must resolve from search.
    assert!(app.lock().await.track_table.tracks.is_empty());

    assert!(route_subsonic_event(&spine, &IoEvent::StartPlayback(None, Some(uris), Some(0))).await);
    let guard = app.lock().await;
    let s = guard
      .subsonic_playback
      .as_ref()
      .expect("search->play must publish a session");
    assert!(
      s.current().is_some_and(|t| !t.name.is_empty()),
      "metadata must resolve from the search results"
    );
  }
}
