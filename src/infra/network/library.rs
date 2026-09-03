use super::mapping::{map_page, playlist_items_page};
use super::requests::{spotify_api_request_json_for_with_refresh, spotify_get_typed_for};
use super::Network;
use crate::core::app::{
  ActiveBlock, App, PlaylistFolder, PlaylistFolderItem, PlaylistFolderNode, PlaylistFolderNodeType,
  RouteId,
};
use crate::core::catalog::{CatalogStateEvent, LibrarySection, PlaylistSelection, SavedSetKind};
use crate::core::plugin_api::{PlaylistInfo, ShowInfo, TrackInfo};
use crate::core::source::Source;
use crate::core::state::PersistedRuntimeState;
use crate::runtime::engine::{FoldAck, FoldAnswer, FoldQuery, SessionFact};
use anyhow::anyhow;
use reqwest::Method;
use rspotify::model::{
  idtypes::{AlbumId, LibraryId, PlaylistId, ShowId, TrackId, UserId},
  page::Page,
  playlist::{PlaylistItem, SimplifiedPlaylist},
  track::SavedTrack,
  PlayableItem,
};
use rspotify::{prelude::*, AuthCodePkceSpotify};
use serde_json::json;
use std::time::Duration;

// Spotify's `me/library/contains` endpoint accepts at most 40 uris per
// request; anything larger fails with a 400 "Too many uris".
const LIBRARY_CONTAINS_MAX_URIS: usize = 40;

async fn library_generation(network: &Network, section: LibrarySection, reset: bool) -> u64 {
  let Some(spine) = network.spine.as_ref() else {
    return 0;
  };
  if reset {
    return spine
      .emit_acked(crate::runtime::engine::EngineEvent::Catalog(
        CatalogStateEvent::LibraryGenerationStarted { section },
      ))
      .await
      .and_then(|ack| ack.library_generation)
      .unwrap_or(0);
  }
  match spine.query(FoldQuery::LibraryGeneration { section }).await {
    Ok(FoldAnswer::U64(generation)) => generation,
    _ => 0,
  }
}

#[cfg(test)]
fn next_saved_tracks_offset(page: &Page<SavedTrack>) -> Option<u32> {
  page.next.as_ref().map(|_| page.offset + page.limit)
}

fn uri_batches(uris: &[String]) -> impl Iterator<Item = &[String]> {
  uris.chunks(LIBRARY_CONTAINS_MAX_URIS)
}

#[cfg_attr(not(test), allow(dead_code))] // the fold merges ids now; tests keep this
fn populate_liked_song_ids_from_saved_tracks(
  liked_song_ids_set: &mut std::collections::HashSet<String>,
  page: &Page<SavedTrack>,
) {
  for item in &page.items {
    if let Some(track_id) = &item.track.id {
      liked_song_ids_set.insert(track_id.id().to_string());
    }
  }
}

/// Build the saved-tracks page-upsert event (ticket 16 batch 2). The page's
/// raw track ids ride along: the fold merges them into the liked set, which
/// is what the old inline write did under the same lock.
fn saved_tracks_page_event(page: &Page<SavedTrack>, generation: u64) -> CatalogStateEvent {
  let mut liked_ids = Vec::with_capacity(page.items.len());
  for item in &page.items {
    if let Some(track_id) = &item.track.id {
      liked_ids.push(track_id.id().to_string());
    }
  }
  CatalogStateEvent::SavedTracksPageUpserted {
    offset: page.offset,
    total: page.total,
    limit: page.limit,
    next: page.next.clone(),
    page: map_page(page, |st| TrackInfo::from(&st.track)).items,
    liked_ids_from_page: liked_ids,
    generation,
  }
}

fn playlist_track_search_terms(query: &str) -> Vec<String> {
  query
    .split_whitespace()
    .map(|term| term.to_lowercase())
    .filter(|term| !term.is_empty())
    .collect()
}

fn playlist_track_search_haystack(track: &rspotify::model::track::FullTrack) -> String {
  let mut haystack = format!("{} {}", track.name, track.album.name);
  for artist in &track.artists {
    haystack.push(' ');
    haystack.push_str(&artist.name);
  }
  haystack.to_lowercase()
}

fn playlist_track_matches_terms(
  track: &rspotify::model::track::FullTrack,
  terms: &[String],
) -> bool {
  let haystack = playlist_track_search_haystack(track);
  terms.iter().all(|term| haystack.contains(term))
}

/// Detached saved-tracks page prefetch (ticket 16 batch 2): a pure producer.
/// Every decision runs through a fold query; every outcome is an event. The
/// worker holds only the spine — it never locks `App`.
///
/// The loop shape (claim → fetch → emit → chain to the next offset) mirrors
/// the old lock-holding version; the fold owns the claim set, the cache, and
/// the visible table, so a page that lands stale is dropped fold-side.
pub async fn run_saved_tracks_prefetch(
  spine: &crate::runtime::engine::Spine,
  spotify: AuthCodePkceSpotify,
  token_cache_path: std::path::PathBuf,
  limit: u32,
  mut offset: u32,
  generation: u64,
) {
  loop {
    // Claim the offset atomically in the fold: two workers racing at the
    // same offset produce exactly one fetch.
    let claimed = matches!(
      spine
        .query(FoldQuery::ClaimLibraryOffset {
          section: LibrarySection::SavedTracks { offset },
          generation,
        })
        .await,
      Ok(FoldAnswer::Bool(true))
    );
    let request_live = matches!(
      spine
        .query(FoldQuery::LibraryRequestLive {
          generation,
          section: LibrarySection::SavedTracks { offset }
        })
        .await,
      Ok(FoldAnswer::Bool(true))
    );
    if !claimed || !request_live {
      return;
    }

    let query = vec![("limit", limit.to_string()), ("offset", offset.to_string())];
    let Ok(page) = super::requests::spotify_get_typed_for::<Page<rspotify::model::SavedTrack>>(
      &spotify,
      "me/tracks",
      &query,
      &token_cache_path,
    )
    .await
    else {
      let _ = spine
        .query(FoldQuery::ReleaseLibraryOffset {
          section: LibrarySection::SavedTracks { offset },
          generation,
        })
        .await;
      return;
    };
    if page.items.is_empty() {
      let _ = spine
        .query(FoldQuery::ReleaseLibraryOffset {
          section: LibrarySection::SavedTracks { offset },
          generation,
        })
        .await;
      return;
    }

    let next_offset = page.next.as_ref().map(|_| page.offset + page.limit);
    spine
      .emit(crate::runtime::engine::EngineEvent::Catalog(
        saved_tracks_page_event(&page, generation),
      ))
      .await;

    let Some(candidate_next_offset) = next_offset else {
      return;
    };
    // Chain to the next page; the claim loop re-checks liveness fold-side.
    let next_missing = matches!(
      spine
        .query(FoldQuery::NextLibraryOffset {
          section: LibrarySection::SavedTracks {
            offset: candidate_next_offset
          },
        })
        .await,
      Ok(FoldAnswer::Offset(Some(_)))
    );
    if !next_missing {
      return;
    }
    offset = candidate_next_offset;
  }
}

#[allow(clippy::never_loop)]
pub async fn prefetch_playlist_tracks_page_task(
  spine: &crate::runtime::engine::Spine,
  spotify: AuthCodePkceSpotify,
  token_cache_path: std::path::PathBuf,
  limit: u32,
  playlist_id: PlaylistId<'static>,
  offset: u32,
  generation: u64,
) {
  let playlist_id_string = playlist_id.id().to_string();
  loop {
    // Claim the offset in the fold (atomic with the liveness check, which
    // covers both the generation and the still-open playlist).
    let claimed = matches!(
      spine
        .query(FoldQuery::ClaimPlaylistTracksOffset {
          playlist_id: playlist_id_string.clone(),
          offset,
          generation,
        })
        .await,
      Ok(FoldAnswer::Bool(true))
    );
    if !claimed {
      return;
    }

    let path = format!("playlists/{}/items", playlist_id.id());
    let query = vec![("limit", limit.to_string()), ("offset", offset.to_string())];
    let Ok(page) =
      spotify_get_typed_for::<Page<PlaylistItem>>(&spotify, &path, &query, &token_cache_path).await
    else {
      let _ = spine
        .query(FoldQuery::ReleasePlaylistTracksOffset {
          playlist_id: playlist_id_string.clone(),
          offset,
          generation,
        })
        .await;
      return;
    };

    if page.items.is_empty() {
      let _ = spine
        .query(FoldQuery::ReleasePlaylistTracksOffset {
          playlist_id: playlist_id_string.clone(),
          offset,
          generation,
        })
        .await;
      return;
    }

    // Publish the page through the fold; the ack returns the next missing
    // offset (None when the fold dropped the page as stale or the cache is
    // complete), which decides whether the chain continues.
    let ack = spine
      .emit_acked(crate::runtime::engine::EngineEvent::Catalog(
        CatalogStateEvent::PlaylistTracksPageUpserted {
          playlist_id: playlist_id_string.clone(),
          generation,
          page: playlist_items_page(&page),
          track_ids_for_like_check: page
            .items
            .iter()
            .filter_map(|item| match &item.item {
              Some(PlayableItem::Track(track)) => track.id.as_ref().map(|id| id.id().to_string()),
              _ => None,
            })
            .collect(),
        },
      ))
      .await;
    let _ = ack;
    return;
  }
}

pub trait LibraryNetwork {
  async fn get_current_user_playlists(&mut self);
  async fn get_playlist_tracks(&mut self, playlist_id: PlaylistId<'static>, playlist_offset: u32);
  async fn search_playlist_tracks(&mut self, playlist_id: PlaylistId<'static>, query: String);
  async fn get_current_user_saved_tracks(&mut self, offset: Option<u32>);
  async fn get_current_user_saved_albums(&mut self, offset: Option<u32>);
  async fn current_user_saved_albums_contains(&mut self, album_ids: Vec<AlbumId<'static>>);
  async fn current_user_saved_album_delete(&mut self, album_id: AlbumId<'static>);
  async fn current_user_saved_album_add(&mut self, album_id: AlbumId<'static>);
  async fn current_user_saved_shows_contains(&mut self, show_ids: Vec<ShowId<'static>>);
  async fn current_user_saved_shows_delete(&mut self, show_id: ShowId<'static>);
  async fn current_user_saved_shows_add(&mut self, show_id: ShowId<'static>);
  async fn get_current_user_saved_shows(&mut self, offset: Option<u32>);
  async fn user_follow_playlist(
    &mut self,
    playlist_owner_id: UserId<'static>,
    playlist_id: PlaylistId<'static>,
    is_public: Option<bool>,
  );
  async fn user_unfollow_playlist(
    &mut self,
    user_id: UserId<'static>,
    playlist_id: PlaylistId<'static>,
  );
  async fn add_track_to_playlist(
    &mut self,
    playlist_id: PlaylistId<'static>,
    track_id: TrackId<'static>,
  );
  async fn remove_track_from_playlist_at_position(
    &mut self,
    playlist_id: PlaylistId<'static>,
    track_id: TrackId<'static>,
    position: usize,
  );
  async fn toggle_save_track(&mut self, track_id: rspotify::model::idtypes::PlayableId<'static>);
  async fn current_user_saved_tracks_contains(&mut self, ids: Vec<TrackId<'static>>);
  async fn fetch_all_playlist_tracks_and_sort(&mut self, playlist_id: PlaylistId<'static>);
  async fn create_new_playlist(&mut self, name: String, track_ids: Vec<TrackId<'static>>);
}

// Private helper methods
impl Network {
  async fn library_contains_uris(&self, uris: &[String]) -> anyhow::Result<Vec<bool>> {
    if uris.is_empty() {
      return Ok(Vec::new());
    }

    let mut all_results = Vec::with_capacity(uris.len());
    for batch in uri_batches(uris) {
      let batch_results = spotify_get_typed_for::<Vec<bool>>(
        self.spotify(),
        "me/library/contains",
        &[("uris", batch.join(","))],
        &self.token_cache_path,
      )
      .await?;
      all_results.extend(batch_results);
    }

    Ok(all_results)
  }

  /// Resolve liked state for bare track ids inline and merge it: the CLI's
  /// synchronous path. The merge runs through a fold query so the CLI never
  /// locks `App` (ticket 16); failures propagate so the CLI reports them
  /// instead of acting on stale membership.
  pub(crate) async fn resolve_liked_state_now(
    &mut self,
    ids: &[String],
  ) -> anyhow::Result<Vec<bool>> {
    let uris: Vec<String> = ids.iter().map(|id| format!("spotify:track:{id}")).collect();
    let is_saved_vec = self.library_contains_uris(&uris).await?;
    if let Some(spine) = self.spine.as_ref() {
      let _ = spine
        .query(FoldQuery::MergeLikedState {
          ids: ids.to_vec(),
          is_saved: is_saved_vec.clone(),
        })
        .await;
    }
    Ok(is_saved_vec)
  }

  async fn library_save_uris(&self, uris: &[String]) -> anyhow::Result<()> {
    if uris.is_empty() {
      return Ok(());
    }

    let query = vec![("uris", uris.join(","))];
    spotify_api_request_json_for_with_refresh(
      self.spotify(),
      Method::PUT,
      "me/library",
      &query,
      Some(json!({ "uris": uris })),
      &self.token_cache_path,
    )
    .await?;
    Ok(())
  }

  async fn library_remove_uris(&self, uris: &[String]) -> anyhow::Result<()> {
    if uris.is_empty() {
      return Ok(());
    }

    let query = vec![("uris", uris.join(","))];
    spotify_api_request_json_for_with_refresh(
      self.spotify(),
      Method::DELETE,
      "me/library",
      &query,
      Some(json!({ "uris": uris })),
      &self.token_cache_path,
    )
    .await?;
    Ok(())
  }

  pub fn spawn_saved_tracks_prefetch(&self, offset: u32, generation: u64) {
    // Ticket 16 batch 2: the worker needs no App handle — the spine plus
    // the fetch inputs. Without a spine (tests) there is no fold to claim
    // from or publish to, so nothing spawns.
    let Some(spine) = self.spine.clone() else {
      return;
    };
    let spotify = self.spotify().clone();
    let token_cache_path = self.token_cache_path.clone();
    let large_search_limit = self.large_search_limit;
    tokio::spawn(async move {
      run_saved_tracks_prefetch(
        &spine,
        spotify,
        token_cache_path,
        large_search_limit,
        offset,
        generation,
      )
      .await;
    });
  }

  pub fn spawn_playlist_tracks_prefetch(
    &self,
    playlist_id: PlaylistId<'static>,
    offset: u32,
    generation: u64,
  ) {
    // Ticket 16 batch 2: no App handle; the fold owns the claim set and the
    // cache. Without a spine (tests) nothing spawns.
    let Some(spine) = self.spine.clone() else {
      return;
    };
    let spotify = self.spotify().clone();
    let token_cache_path = self.token_cache_path.clone();
    let large_search_limit = self.large_search_limit;
    tokio::spawn(async move {
      prefetch_playlist_tracks_page_task(
        &spine,
        spotify,
        token_cache_path,
        large_search_limit,
        playlist_id,
        offset,
        generation,
      )
      .await;
    });
  }
}

/// Detached body of `fetch_all_playlist_tracks_and_sort`. On a page failure
/// the error routes through `App::handle_error` exactly as the inline version
/// did.
/// Single detached worker draining [`App::liked_lookup_pending`] in 40-uri
/// batches, merging each batch into `liked_song_ids_set` as it lands. Runs off
/// the IoEvent pump: a big playlist's liked-state sweep is 8-15 batches at
/// 1-2s each, which used to head-of-line-block playback controls (a
/// skip-to-queued-track waited ~20s behind it, #386). One worker at a time
/// (`liked_lookup_worker_running`), so rapid navigation coalesces into the
/// deduped pending set instead of parallel request floods.
async fn liked_lookup_worker_task(
  spine: crate::runtime::engine::Spine,
  spotify: AuthCodePkceSpotify,
  token_cache_path: std::path::PathBuf,
) {
  const MAX_CONSECUTIVE_FAILURES: u32 = 3;
  let mut consecutive_failures = 0u32;
  loop {
    // Claim a batch in the fold (atomic drain of the pending set + running
    // flag); `None` means the pending set went empty and the worker exits.
    let Some((batch, epoch)) = (match spine.query(FoldQuery::ClaimLikedLookupBatch).await {
      Ok(FoldAnswer::LikedBatch(batch)) => batch,
      _ => None,
    }) else {
      return;
    };
    let uris: Vec<String> = batch
      .iter()
      .map(|id| format!("spotify:track:{id}"))
      .collect();
    let result = spotify_get_typed_for::<Vec<bool>>(
      &spotify,
      "me/library/contains",
      &[("uris", uris.join(","))],
      &token_cache_path,
    )
    .await;
    match result {
      Ok(is_saved_vec) => {
        consecutive_failures = 0;
        // The fold merges only when the epoch still matches; a stale batch
        // is re-pended inside the fold (old inline logic, single owner).
        let _ = spine
          .query(FoldQuery::ApplyLikedLookup {
            epoch,
            batch,
            is_saved: is_saved_vec,
          })
          .await;
      }
      Err(_) if consecutive_failures + 1 < MAX_CONSECUTIVE_FAILURES => {
        // Transient failure: back off and retry from THIS worker, so ids
        // enqueued while the request was in flight (which saw the running
        // flag and did not spawn) are not stranded without a worker.
        let _ = spine.query(FoldQuery::RependLikedLookup { batch }).await;
        consecutive_failures += 1;
        tokio::time::sleep(std::time::Duration::from_secs(u64::from(
          2 * consecutive_failures,
        )))
        .await;
      }
      Err(e) => {
        // Persistently failing: stop rather than hammer the endpoint. The
        // ids stay pending and the next dispatch restarts the worker.
        let _ = spine.query(FoldQuery::RependLikedLookup { batch }).await;
        let _ = spine.query(FoldQuery::StopLikedLookupWorker).await;
        let _ = spine
          .emit(crate::runtime::engine::EngineEvent::Session(
            crate::runtime::engine::SessionFact::Status {
              message: format!("Could not check liked track state: {e}"),
              ttl_secs: 5,
              is_error: false,
            },
          ))
          .await;
        return;
      }
    }
  }
}

async fn fetch_all_playlist_tracks_and_sort_task(
  spine: crate::runtime::engine::Spine,
  spotify: AuthCodePkceSpotify,
  token_cache_path: std::path::PathBuf,
  playlist_id: PlaylistId<'static>,
) {
  let playlist_id_string = playlist_id.id().to_string();
  let mut all_tracks = Vec::new();
  let mut offset = 0u32;
  let limit = 50u32;
  let path = format!("playlists/{}/items", playlist_id.id());

  loop {
    // Still the active view? The fold answers; a stale fetch exits and
    // releases its in-flight slot through the same query path.
    let live = matches!(
      spine
        .query(FoldQuery::PlaylistTrackTableActive {
          playlist_id: playlist_id_string.clone(),
        })
        .await,
      Ok(FoldAnswer::Bool(true))
    );
    if !live {
      let _ = spine
        .query(FoldQuery::ReleasePlaylistSortFetch {
          playlist_id: playlist_id_string,
        })
        .await;
      return;
    }
    let query = vec![("limit", limit.to_string()), ("offset", offset.to_string())];
    match spotify_get_typed_for::<Page<PlaylistItem>>(&spotify, &path, &query, &token_cache_path)
      .await
    {
      Ok(page) => {
        if page.items.is_empty() {
          break;
        }

        for item in page.items {
          if let Some(PlayableItem::Track(full_track)) = item.item {
            all_tracks.push(full_track);
          }
        }

        if page.next.is_none() {
          break;
        }
        offset += limit;
      }
      Err(e) => {
        let _ = spine
          .query(FoldQuery::ReleasePlaylistSortFetch {
            playlist_id: playlist_id_string.clone(),
          })
          .await;
        let _ = spine
          .emit(crate::runtime::engine::EngineEvent::Session(
            crate::runtime::engine::SessionFact::Error(format!("{e}")),
          ))
          .await;
        return;
      }
    }
  }

  // Ticket 16: the sort application is one fold query — it reads the live
  // sort state, replaces the table if the same playlist is still active,
  // and releases the in-flight slot atomically.
  let _ = spine
    .query(FoldQuery::ApplySortedPlaylistTracks {
      playlist_id: playlist_id_string,
      tracks: all_tracks,
    })
    .await;
}

/// Background half of `get_current_user_playlists`: fetch the remaining pages
/// and the librespot rootlist folder structure, then publish the complete list
/// through a fold query (unless a newer refresh superseded this one — the
/// fold checks the generation). A page failure here keeps the
/// already-published pages instead of error-routing the UI.
#[allow(clippy::too_many_arguments)]
async fn finish_playlists_fetch(
  spine: crate::runtime::engine::Spine,
  spotify: AuthCodePkceSpotify,
  token_cache_path: std::path::PathBuf,
  mut all_playlists: Vec<PlaylistInfo>,
  limit: u32,
  generation: u64,
  original_selection: PlaylistSelection,
) {
  let mut pagination_error = None;
  let mut offset = limit;
  loop {
    let live = matches!(
      spine
        .query(FoldQuery::PlaylistRefreshLive { generation })
        .await,
      Ok(FoldAnswer::Bool(true))
    );
    if !live {
      return;
    }
    let mut attempts = 0u8;
    let page = loop {
      match spotify_get_typed_for::<Page<SimplifiedPlaylist>>(
        &spotify,
        "me/playlists",
        &[("limit", limit.to_string()), ("offset", offset.to_string())],
        &token_cache_path,
      )
      .await
      {
        Ok(page) => break Some(page),
        Err(e) if attempts < 2 => {
          attempts += 1;
          log::warn!("playlist page fetch failed at offset {offset}; retry {attempts}/2: {e}");
          tokio::time::sleep(Duration::from_millis(250 * u64::from(attempts))).await;
        }
        Err(e) => {
          pagination_error = Some(e.to_string());
          break None;
        }
      }
    };
    let Some(page) = page else { break };
    if page.items.is_empty() {
      break;
    }
    all_playlists.extend(page.items.iter().map(PlaylistInfo::from_simplified));
    if page.next.is_none() {
      break;
    }
    offset += limit;
  }

  // The rootlist folder structure is fetched here (async, off the fold) so
  // the publish query can stay synchronous. A failure keeps the previous
  // nodes (None), exactly like the old inline flow.
  #[cfg(feature = "streaming")]
  let folder_nodes: crate::runtime::engine::FolderNodes = {
    match spine.query(FoldQuery::StreamingPlayerHandle).await {
      Ok(FoldAnswer::StreamingPlayer(player)) => fetch_rootlist_folders_from(player).await,
      _ => None,
    }
  };
  #[cfg(not(feature = "streaming"))]
  let folder_nodes = ();

  // One fold query publishes the outcome — complete list or restore-the-
  // previous-list on pagination failure. The fold owns the generation
  // check, the folder structure, the selection reconcile, and the
  // community-pin prompt (all the state the old task locked for).
  let _ = spine
    .query(FoldQuery::PublishPlaylistRefresh {
      generation,
      playlists: all_playlists,
      original_selection,
      pagination_error,
      folder_nodes,
    })
    .await;
}

/// Offer the one-time community-pin hide prompt once the full playlist list is
/// known. A user who already follows the community playlist never sees it (no
/// pin to explain); if they later unfollow, the pin appears for the first time
/// and this can fire then — intended, not a bug.
pub(crate) fn maybe_prompt_community_pin(app: &mut App) {
  if app.active_source == Source::Spotify
    && !app.runtime_state.community_pin_prompt_shown
    && app.user_config.behavior.pin_community_playlist
    && !app.follows_community_playlist()
    && app.get_current_route().id != RouteId::CommunityPinPrompt
  {
    // Mark shown at push time, not only on dismiss, so paths that close the
    // prompt without going through its handler (the back key, mouse-interactive
    // layout dismissal) can never make it reappear.
    app.runtime_state.community_pin_prompt_shown = true;
    if let Err(e) = app.save_runtime_state(&PersistedRuntimeState::community_pin_prompt_shown(true))
    {
      log::warn!("failed to persist community pin prompt state: {}", e);
    }
    app.push_navigation_stack(RouteId::CommunityPinPrompt, ActiveBlock::CommunityPinPrompt);
  }
}

impl Network {
  /// Emit one catalog fact through the spine (ticket 16 batch 2). A no-op
  /// when no fold is installed (tests).
  async fn emit_catalog(&self, event: CatalogStateEvent) {
    if let Some(spine) = self.spine.as_ref() {
      spine
        .emit(crate::runtime::engine::EngineEvent::Catalog(event))
        .await;
    }
  }

  /// Emit one catalog fact and await the fold's ack (post-fold facts).
  pub(super) async fn emit_catalog_acked(&self, event: CatalogStateEvent) -> Option<FoldAck> {
    let spine = self.spine.as_ref()?;
    spine
      .emit_acked(crate::runtime::engine::EngineEvent::Catalog(event))
      .await
  }
}

impl LibraryNetwork for Network {
  async fn get_current_user_playlists(&mut self) {
    let Some(spine) = self.spine.clone() else {
      return;
    };
    let limit = 50u32;

    // Page 1 first: publish it immediately so the sidebar is usable, then
    // fetch the remaining pages and the rootlist folder structure in a
    // background task instead of blocking the pump for the whole pagination
    // (each page pays the pacing floor; 500 playlists used to block startup
    // dispatches for seconds).
    //
    // Always use the compat path: parses raw JSON to serde_json::Value first,
    // which silently deduplicates keys (last-wins). This handles the known Spotify
    // API bug where "items" appears twice in the same JSON object.
    let first_page = match spotify_get_typed_for::<Page<SimplifiedPlaylist>>(
      self.spotify(),
      "me/playlists",
      &[("limit", limit.to_string()), ("offset", "0".to_string())],
      &self.token_cache_path,
    )
    .await
    {
      Ok(page) => page,
      Err(e) => {
        self
          .emit_session(SessionFact::ClearPendingPlaylistOpen(None))
          .await;
        self.handle_error(anyhow!(e)).await;
        return;
      }
    };

    // Convert to source-agnostic domain types at the network boundary.
    let first_items: Vec<PlaylistInfo> = first_page
      .items
      .iter()
      .map(PlaylistInfo::from_simplified)
      .collect();
    let mapped_first = map_page(&first_page, PlaylistInfo::from_simplified);

    // Ticket 16: page-1 publication is a fold query — it snapshots the
    // previous complete list, bumps the generation, installs the first
    // page, reconciles the selection, and returns what the background task
    // needs. One atomic step instead of three lock sections.
    let Ok(FoldAnswer::PlaylistRefreshRegistered {
      generation,
      original_selection,
      ..
    }) = spine
      .query(FoldQuery::RegisterPlaylistRefresh {
        first_page: mapped_first,
        first_items: first_items.clone(),
      })
      .await
    else {
      return;
    };

    let spotify = self.spotify().clone();
    let token_cache_path = self.token_cache_path.clone();
    tokio::spawn(async move {
      finish_playlists_fetch(
        spine,
        spotify,
        token_cache_path,
        first_items,
        limit,
        generation,
        original_selection,
      )
      .await;
    });
  }

  async fn get_playlist_tracks(&mut self, playlist_id: PlaylistId<'static>, playlist_offset: u32) {
    let generation = match self.spine.as_ref() {
      Some(spine) => match spine.query(FoldQuery::PlaylistTracksGeneration).await {
        Ok(FoldAnswer::U64(generation)) => generation,
        _ => return,
      },
      None => 0,
    };
    let path = format!("playlists/{}/items", playlist_id.id());
    match spotify_get_typed_for::<Page<PlaylistItem>>(
      self.spotify(),
      &path,
      &[
        ("limit", self.large_search_limit.to_string()),
        ("offset", playlist_offset.to_string()),
      ],
      &self.token_cache_path,
    )
    .await
    {
      Ok(playlist_tracks) => {
        // Ticket 16 batch 2: the page lands as an acked event; the fold
        // releases the in-flight slot, clears the pending open, enforces
        // staleness (active playlist + generation), upserts, rebuilds the
        // table, and answers with the next missing offset.
        let ack = self
          .emit_catalog_acked(CatalogStateEvent::PlaylistTracksPageUpserted {
            playlist_id: playlist_id.id().to_string(),
            generation,
            page: playlist_items_page(&playlist_tracks),
            track_ids_for_like_check: playlist_tracks
              .items
              .iter()
              .filter_map(|item| match &item.item {
                Some(PlayableItem::Track(track)) => track.id.as_ref().map(|id| id.id().to_string()),
                _ => None,
              })
              .collect(),
          })
          .await;
        if let Some(FoldAck {
          next_playlist_tracks_offset: Some(next_offset),
          ..
        }) = ack
        {
          self.spawn_playlist_tracks_prefetch(playlist_id, next_offset, generation);
        }
      }
      Err(e) => {
        if let Some(spine) = self.spine.as_ref() {
          let _ = spine
            .query(FoldQuery::ReleasePlaylistTracksPage {
              playlist_id: playlist_id.id().to_string(),
              offset: playlist_offset,
              generation,
            })
            .await;
        }
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn search_playlist_tracks(&mut self, playlist_id: PlaylistId<'static>, query: String) {
    let terms = playlist_track_search_terms(&query);
    if terms.is_empty() {
      // The empty query clears the filter — a fold query (it rebuilds the
      // unfiltered table from the cached pages).
      if let Some(spine) = self.spine.as_ref() {
        let _ = spine.query(FoldQuery::ClearPlaylistTrackFilter).await;
      }
      return;
    }

    let limit = self.large_search_limit;
    let mut offset = 0u32;
    let mut matches = Vec::new();

    loop {
      let path = format!("playlists/{}/items", playlist_id.id());
      let page = match spotify_get_typed_for::<Page<PlaylistItem>>(
        self.spotify(),
        &path,
        &[("limit", limit.to_string()), ("offset", offset.to_string())],
        &self.token_cache_path,
      )
      .await
      {
        Ok(page) => page,
        Err(e) => {
          self.handle_error(anyhow!(e)).await;
          return;
        }
      };

      if page.items.is_empty() {
        break;
      }

      for (index, item) in page.items.iter().enumerate() {
        if let Some(PlayableItem::Track(track)) = item.item.as_ref() {
          if playlist_track_matches_terms(track, &terms) {
            matches.push((track.clone(), page.offset as usize + index));
          }
        }
      }

      if page.next.is_none() {
        break;
      }
      offset = page.offset.saturating_add(page.limit);
    }

    // Ticket 16: the result application is one fold query — it checks the
    // playlist is still active, sorts, replaces the rows, and clears the
    // pending search if the playlist changed mid-search.
    let match_count = matches.len();
    let applied = if let Some(spine) = self.spine.as_ref() {
      matches!(
        spine
          .query(FoldQuery::PlaylistTrackSearchApplied {
            playlist_id: playlist_id.id().to_string(),
            query: query.clone(),
            matches,
          })
          .await,
        Ok(FoldAnswer::Bool(true))
      )
    } else {
      false
    };
    if applied {
      self
        .emit_status(
          format!("{match_count} playlist tracks match \"{query}\""),
          3,
        )
        .await;
    }
  }

  async fn get_current_user_saved_tracks(&mut self, offset: Option<u32>) {
    // Ticket 16 batch 2: a pure producer. The generation is stamped by a
    // fold query; the page lands as an acked event whose reply carries the
    // next missing offset (the read the old code did under its own lock
    // after the upsert). Errors release the claimed offset and surface as
    // an error fact.
    let requested_offset = offset.unwrap_or(0);
    let Some(spine) = self.spine.clone() else {
      return;
    };
    let generation = match spine.query(FoldQuery::SavedTracksGeneration).await {
      Ok(FoldAnswer::U64(generation)) => generation,
      _ => return,
    };

    let mut query = vec![("limit", self.large_search_limit.to_string())];
    if let Some(offset) = offset {
      query.push(("offset", offset.to_string()));
    }

    match spotify_get_typed_for::<Page<rspotify::model::SavedTrack>>(
      self.spotify(),
      "me/tracks",
      &query,
      &self.token_cache_path,
    )
    .await
    {
      Ok(saved_tracks) => {
        let page_event = saved_tracks_page_event(&saved_tracks, generation);
        let ack = spine
          .emit_acked(crate::runtime::engine::EngineEvent::Catalog(page_event))
          .await;
        let Some(ack) = ack else {
          return;
        };
        if let Some(next_offset) = ack.next_saved_tracks_offset {
          self.spawn_saved_tracks_prefetch(next_offset, generation);
        }
      }
      Err(e) => {
        let _ = spine
          .query(FoldQuery::ReleaseLibraryOffset {
            section: LibrarySection::SavedTracks {
              offset: requested_offset,
            },
            generation,
          })
          .await;
        spine
          .emit(crate::runtime::engine::EngineEvent::Session(
            crate::runtime::engine::SessionFact::Error(format!("{e}")),
          ))
          .await;
      }
    }
  }

  async fn get_current_user_saved_albums(&mut self, offset: Option<u32>) {
    let generation = library_generation(self, LibrarySection::SavedAlbums, offset.is_none()).await;
    let mut query = vec![("limit", self.large_search_limit.to_string())];
    if let Some(offset) = offset {
      query.push(("offset", offset.to_string()));
    }

    match spotify_get_typed_for::<Page<rspotify::model::SavedAlbum>>(
      self.spotify(),
      "me/albums",
      &query,
      &self.token_cache_path,
    )
    .await
    {
      Ok(saved_albums) => {
        let page = (!saved_albums.items.is_empty()).then(|| {
          crate::infra::network::mapping::map_page(
            &saved_albums,
            crate::infra::network::mapping::saved_album_info,
          )
        });
        // Ticket 16 batch 2: the page (and the always-bump completion
        // signal) is one event; the fold writes the cache.
        self
          .emit_catalog(CatalogStateEvent::SavedAlbumsPageUpserted { generation, page })
          .await;
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn current_user_saved_albums_contains(&mut self, album_ids: Vec<AlbumId<'static>>) {
    let uris: Vec<String> = album_ids
      .iter()
      .map(|id| format!("spotify:album:{}", id.id()))
      .collect();

    match self.library_contains_uris(&uris).await {
      Ok(is_saved_vec) => {
        let saved_ids: Vec<String> = album_ids
          .iter()
          .enumerate()
          .filter(|(i, _)| is_saved_vec.get(*i).copied().unwrap_or(false))
          .map(|(_, id)| id.id().to_string())
          .collect();
        let unsaved_ids: Vec<String> = album_ids
          .iter()
          .enumerate()
          .filter(|(i, _)| !is_saved_vec.get(*i).copied().unwrap_or(true))
          .map(|(_, id)| id.id().to_string())
          .collect();
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::Albums,
            saved_ids,
            unsaved_ids,
          })
          .await;
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn current_user_saved_album_delete(&mut self, album_id: AlbumId<'static>) {
    let uris = vec![format!("spotify:album:{}", album_id.id())];
    match self.library_remove_uris(&uris).await {
      Ok(_) => {
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::Albums,
            saved_ids: Vec::new(),
            unsaved_ids: vec![album_id.id().to_string()],
          })
          .await;
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn current_user_saved_album_add(&mut self, album_id: AlbumId<'static>) {
    let uris = vec![format!("spotify:album:{}", album_id.id())];
    match self.library_save_uris(&uris).await {
      Ok(_) => {
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::Albums,
            saved_ids: vec![album_id.id().to_string()],
            unsaved_ids: Vec::new(),
          })
          .await;
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn current_user_saved_shows_contains(&mut self, show_ids: Vec<ShowId<'static>>) {
    let uris: Vec<String> = show_ids
      .iter()
      .map(|id| format!("spotify:show:{}", id.id()))
      .collect();
    match self.library_contains_uris(&uris).await {
      Ok(is_saved_vec) => {
        let saved_ids: Vec<String> = show_ids
          .iter()
          .enumerate()
          .filter(|(i, _)| is_saved_vec.get(*i).copied().unwrap_or(false))
          .map(|(_, id)| id.id().to_string())
          .collect();
        let unsaved_ids: Vec<String> = show_ids
          .iter()
          .enumerate()
          .filter(|(i, _)| !is_saved_vec.get(*i).copied().unwrap_or(true))
          .map(|(_, id)| id.id().to_string())
          .collect();
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::Shows,
            saved_ids,
            unsaved_ids,
          })
          .await;
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn current_user_saved_shows_delete(&mut self, show_id: ShowId<'static>) {
    let uris = vec![format!("spotify:show:{}", show_id.id())];
    match self.library_remove_uris(&uris).await {
      Ok(_) => {
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::Shows,
            saved_ids: Vec::new(),
            unsaved_ids: vec![show_id.id().to_string()],
          })
          .await;
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn current_user_saved_shows_add(&mut self, show_id: ShowId<'static>) {
    let uris = vec![format!("spotify:show:{}", show_id.id())];
    match self.library_save_uris(&uris).await {
      Ok(_) => {
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::Shows,
            saved_ids: vec![show_id.id().to_string()],
            unsaved_ids: Vec::new(),
          })
          .await;
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn get_current_user_saved_shows(&mut self, offset: Option<u32>) {
    let generation = library_generation(self, LibrarySection::SavedShows, offset.is_none()).await;
    let mut query = vec![("limit", self.large_search_limit.to_string())];
    if let Some(offset) = offset {
      query.push(("offset", offset.to_string()));
    }

    match spotify_get_typed_for::<Page<rspotify::model::show::Show>>(
      self.spotify(),
      "me/shows",
      &query,
      &self.token_cache_path,
    )
    .await
    {
      Ok(saved_shows) => {
        let page = (!saved_shows.items.is_empty()).then(|| {
          crate::infra::network::mapping::map_page(&saved_shows, |s| ShowInfo::from(&s.show))
        });
        self
          .emit_catalog(CatalogStateEvent::SavedShowsPageUpserted { generation, page })
          .await;
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn user_follow_playlist(
    &mut self,
    _playlist_owner_id: UserId<'static>,
    playlist_id: PlaylistId<'static>,
    _is_public: Option<bool>,
  ) {
    match self
      .spotify()
      .library_add([LibraryId::Playlist(playlist_id)])
      .await
    {
      Ok(_) => {
        // Optimistic update handled in handler or next refresh
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn user_unfollow_playlist(
    &mut self,
    _user_id: UserId<'static>,
    playlist_id: PlaylistId<'static>,
  ) {
    match self
      .spotify()
      .library_remove([LibraryId::Playlist(playlist_id)])
      .await
    {
      Ok(_) => {
        // Handled
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn add_track_to_playlist(
    &mut self,
    playlist_id: PlaylistId<'static>,
    track_id: TrackId<'static>,
  ) {
    match self
      .spotify()
      .playlist_add_items(playlist_id.clone(), vec![PlayableId::Track(track_id)], None)
      .await
    {
      Ok(_) => {
        // Ticket 16: the name lookup + conditional refresh is one fold
        // query; the status message comes back with it.
        let status_message = if let Some(spine) = self.spine.as_ref() {
          match spine
            .query(FoldQuery::PlaylistTrackAdded {
              playlist_id: playlist_id.id().to_string(),
            })
            .await
          {
            Ok(FoldAnswer::StatusText(text)) => text,
            _ => "Added to playlist".to_string(),
          }
        } else {
          "Added to playlist".to_string()
        };
        self.show_status_message(status_message, 3).await;
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn remove_track_from_playlist_at_position(
    &mut self,
    playlist_id: PlaylistId<'static>,
    track_id: TrackId<'static>,
    position: usize,
  ) {
    let body = json!({
        "tracks": [{
            "uri": format!("spotify:track:{}", track_id.id()),
            "positions": [position]
        }]
    });

    match spotify_api_request_json_for_with_refresh(
      self.spotify(),
      Method::DELETE,
      &format!("playlists/{}/tracks", playlist_id.id()),
      &[],
      Some(body),
      &self.token_cache_path,
    )
    .await
    {
      Ok(_) => {
        self
          .show_status_message("Removed from playlist".to_string(), 3)
          .await;
      }
      Err(e) => self.handle_error(anyhow!(e)).await,
    }
  }

  async fn toggle_save_track(&mut self, track_id: rspotify::model::idtypes::PlayableId<'static>) {
    let id_str = match &track_id {
      PlayableId::Track(id) => id.id(),
      PlayableId::Episode(id) => id.id(),
    };
    let uri = match &track_id {
      PlayableId::Track(id) => format!("spotify:track:{}", id.id()),
      PlayableId::Episode(id) => format!("spotify:episode:{}", id.id()),
    };

    // Ticket 16: the membership read is a fold query; the epoch bump rides
    // the membership-merge fact (the fold stamps it when the mutation
    // lands, which is exactly the ordering the old single lock gave).
    let is_liked = if let Some(spine) = self.spine.as_ref() {
      matches!(
        spine
          .query(FoldQuery::LikedState {
            ids: vec![id_str.to_string()],
          })
          .await,
        Ok(FoldAnswer::Liked(liked)) if liked.first().copied().unwrap_or(false)
      )
    } else {
      false
    };

    if is_liked {
      if let Err(e) = self.library_remove_uris(&[uri]).await {
        self.handle_error(anyhow!(e)).await;
      } else {
        self
          .emit_catalog(CatalogStateEvent::SavedSetMerged {
            kind: SavedSetKind::LikedTracks,
            saved_ids: Vec::new(),
            unsaved_ids: vec![id_str.to_string()],
          })
          .await;
        self.emit_catalog(CatalogStateEvent::LikedEpochBumped).await;
      }
    } else if let Err(e) = self.library_save_uris(&[uri]).await {
      self.handle_error(anyhow!(e)).await;
    } else {
      self
        .emit_catalog(CatalogStateEvent::SavedSetMerged {
          kind: SavedSetKind::LikedTracks,
          saved_ids: vec![id_str.to_string()],
          unsaved_ids: Vec::new(),
        })
        .await;
      self.emit_catalog(CatalogStateEvent::LikedEpochBumped).await;
    }
  }

  async fn current_user_saved_tracks_contains(&mut self, ids: Vec<TrackId<'static>>) {
    // Enqueue and let the single detached worker resolve: these lookups are
    // UI garnish and must never head-of-line-block playback events on the
    // serial pump (see `liked_lookup_worker_task`). Ticket 16: the enqueue +
    // worker-claim runs in the fold; this handler only spawns the worker
    // when the fold says a claim actually happened.
    let Some(spine) = self.spine.clone() else {
      return;
    };
    let ids: Vec<String> = ids.iter().map(|id| id.id().to_string()).collect();
    let spawn = matches!(
      spine.query(FoldQuery::EnqueueLikedLookup { ids }).await,
      Ok(FoldAnswer::Bool(true))
    );
    if !spawn {
      return;
    }
    let spotify = self.spotify().clone();
    let token_cache_path = self.token_cache_path.clone();
    tokio::spawn(async move {
      liked_lookup_worker_task(spine, spotify, token_cache_path).await;
    });
  }

  async fn fetch_all_playlist_tracks_and_sort(&mut self, playlist_id: PlaylistId<'static>) {
    // The full pagination pays the pacing floor per page (a 3,000-track
    // playlist is 60 pages, 15s+), so it runs detached instead of
    // head-of-line-blocking playback controls and polling on the serial pump.
    // The sort application (fold query) guards a stale result.
    let Some(spine) = self.spine.clone() else {
      return;
    };
    let claimed = matches!(
      spine
        .query(FoldQuery::ClaimPlaylistSortFetch {
          playlist_id: playlist_id.id().to_string(),
        })
        .await,
      Ok(FoldAnswer::Bool(true))
    );
    if !claimed {
      return;
    }
    let spotify = self.spotify().clone();
    let token_cache_path = self.token_cache_path.clone();
    tokio::spawn(async move {
      fetch_all_playlist_tracks_and_sort_task(spine, spotify, token_cache_path, playlist_id).await;
    });
  }

  async fn create_new_playlist(&mut self, name: String, track_ids: Vec<TrackId<'static>>) {
    // Ticket 16: the user id is a fold query.
    let user_id = if let Some(spine) = self.spine.as_ref() {
      match spine.query(FoldQuery::UserId).await {
        Ok(FoldAnswer::User(id)) => id,
        _ => None,
      }
    } else {
      None
    };

    let user_id = match user_id {
      Some(id) => id,
      None => {
        self
          .show_status_message("Cannot create playlist: not logged in".to_string(), 4)
          .await;
        return;
      }
    };

    // Use raw API call to avoid rspotify deserializing FullPlaylist, which crashes when
    // Spotify returns a duplicate "items" key in the response (known API migration bug).
    let user_id_str = user_id;
    let create_path = format!("users/{}/playlists", user_id_str);
    let create_body = json!({
      "name": name,
      "public": false,
      "collaborative": false,
      "description": "Created with spotatui"
    });
    let playlist_value = match spotify_api_request_json_for_with_refresh(
      self.spotify(),
      Method::POST,
      &create_path,
      &[],
      Some(create_body),
      &self.token_cache_path,
    )
    .await
    {
      Ok(v) => v,
      Err(e) => {
        self.handle_error(e).await;
        return;
      }
    };

    let playlist_id_str = match playlist_value.get("id").and_then(|v| v.as_str()) {
      Some(id) => id.to_string(),
      None => {
        self
          .show_status_message("Playlist created but could not get its ID".to_string(), 4)
          .await;
        return;
      }
    };

    let playlist_id = match PlaylistId::from_id(playlist_id_str) {
      Ok(id) => id.into_static(),
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
        return;
      }
    };

    if !track_ids.is_empty() {
      let items: Vec<rspotify::model::idtypes::PlayableId> = track_ids
        .iter()
        .map(|id| rspotify::model::idtypes::PlayableId::Track(id.clone()))
        .collect();
      if let Err(e) = self
        .spotify()
        .playlist_add_items(playlist_id, items, None)
        .await
      {
        self.handle_error(anyhow!(e)).await;
        return;
      }
    }

    // Refresh playlists: dispatched from the fold so the spinner semantics
    // stay with the single writer.
    self
      .emit_session(SessionFact::DispatchPlaylistRefresh)
      .await;

    let status = format!("Playlist \"{}\" created!", name);
    self.show_status_message(status, 4).await;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use chrono::{Duration as ChronoDuration, Utc};
  use rspotify::model::{artist::SimplifiedArtist, track::FullTrack};
  use std::collections::{HashMap, HashSet};

  #[allow(deprecated)]
  fn full_track(id: &str) -> FullTrack {
    FullTrack {
      album: rspotify::model::album::SimplifiedAlbum {
        name: "Album".to_string(),
        ..Default::default()
      },
      artists: vec![SimplifiedArtist {
        name: "Artist".to_string(),
        ..Default::default()
      }],
      available_markets: Vec::new(),
      disc_number: 1,
      duration: ChronoDuration::milliseconds(180_000),
      explicit: false,
      external_ids: HashMap::new(),
      external_urls: HashMap::new(),
      href: None,
      id: Some(TrackId::from_id(id).unwrap().into_static()),
      is_local: false,
      is_playable: Some(true),
      linked_from: None,
      restrictions: None,
      name: format!("Track {id}"),
      popularity: 50,
      preview_url: None,
      track_number: 1,
      r#type: rspotify::model::Type::Track,
    }
  }

  fn saved_track(id: &str) -> SavedTrack {
    SavedTrack {
      added_at: Utc::now(),
      track: full_track(id),
    }
  }

  fn saved_tracks_page(offset: u32, limit: u32, has_next: bool) -> Page<SavedTrack> {
    let ids = match offset {
      0 => vec!["0000000000000000000001", "0000000000000000000002"],
      20 => vec!["0000000000000000000003", "0000000000000000000004"],
      40 => vec!["0000000000000000000005", "0000000000000000000006"],
      _ => vec!["0000000000000000000007", "0000000000000000000008"],
    };

    Page {
      href: "https://example.com/me/tracks".to_string(),
      items: ids.into_iter().map(saved_track).collect(),
      limit,
      next: has_next.then(|| "https://example.com/me/tracks?next".to_string()),
      offset,
      previous: None,
      total: 60,
    }
  }

  #[test]
  fn community_pin_prompt_marks_shown_at_push_time() {
    let dir = tempfile::tempdir().unwrap();
    // Trigger conditions: Spotify source, flag false, toggle on, not following.
    let mut app = App::default();
    app.state_path = Some(dir.path().join("state.yml"));
    assert!(app.active_source == Source::Spotify);
    assert!(app.user_config.behavior.pin_community_playlist);
    assert!(!app.follows_community_playlist());
    assert!(!app.runtime_state.community_pin_prompt_shown);

    maybe_prompt_community_pin(&mut app);

    // Shown is marked at push time so a non-handler dismissal can't reappear it.
    assert!(app.runtime_state.community_pin_prompt_shown);
    assert_eq!(app.get_current_route().id, RouteId::CommunityPinPrompt);
  }

  #[test]
  fn next_saved_tracks_offset_uses_page_limit() {
    let page = saved_tracks_page(20, 20, true);
    assert_eq!(next_saved_tracks_offset(&page), Some(40));
  }

  #[test]
  fn next_saved_tracks_offset_returns_none_without_next_link() {
    let page = saved_tracks_page(20, 20, false);
    assert_eq!(next_saved_tracks_offset(&page), None);
  }

  #[test]
  fn uri_batches_split_large_contains_requests() {
    let uris = (0..120)
      .map(|index| format!("spotify:track:{index:022}"))
      .collect::<Vec<_>>();
    let batches = uri_batches(&uris)
      .map(|batch| batch.len())
      .collect::<Vec<_>>();

    assert_eq!(batches, vec![40, 40, 40]);
  }

  #[test]
  fn populate_liked_song_ids_from_saved_tracks_uses_raw_track_ids() {
    let page = saved_tracks_page(0, 20, false);
    let mut liked_song_ids_set = HashSet::new();

    populate_liked_song_ids_from_saved_tracks(&mut liked_song_ids_set, &page);

    assert!(liked_song_ids_set.contains("0000000000000000000001"));
    assert!(liked_song_ids_set.contains("0000000000000000000002"));
    assert!(!liked_song_ids_set.contains("spotify:track:0000000000000000000001"));
  }

  #[test]
  fn playlist_track_filter_matches_title_artist_album_case_insensitively() {
    let mut track = full_track("0000000000000000000001");
    track.name = "Midnight City".to_string();
    track.artists[0].name = "M83".to_string();
    track.album.name = "Hurry Up".to_string();

    assert!(playlist_track_matches_terms(
      &track,
      &playlist_track_search_terms("midnight")
    ));
    assert!(playlist_track_matches_terms(
      &track,
      &playlist_track_search_terms("m83")
    ));
    assert!(playlist_track_matches_terms(
      &track,
      &playlist_track_search_terms("hurry")
    ));
    assert!(playlist_track_matches_terms(
      &track,
      &playlist_track_search_terms("MIDNIGHT m83")
    ));
  }

  #[test]
  fn playlist_track_filter_requires_every_query_term() {
    let mut track = full_track("0000000000000000000001");
    track.name = "Midnight City".to_string();
    track.artists[0].name = "M83".to_string();
    track.album.name = "Hurry Up".to_string();

    assert!(playlist_track_matches_terms(
      &track,
      &playlist_track_search_terms("city hurry")
    ));
    assert!(!playlist_track_matches_terms(
      &track,
      &playlist_track_search_terms("city missing")
    ));
  }
}

#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
#[cfg(feature = "streaming")]
async fn fetch_rootlist_folders_from(
  player: Option<std::sync::Arc<crate::infra::player::StreamingPlayer>>,
) -> Option<Vec<PlaylistFolderNode>> {
  let player = player?;
  let session = player.session();

  let bytes = match session.spclient().get_rootlist(0, Some(100_000)).await {
    Ok(bytes) => bytes,
    Err(_) => return None,
  };

  use protobuf::Message;
  let selected: librespot_protocol::playlist4_external::SelectedListContent =
    Message::parse_from_bytes(&bytes).ok()?;

  let contents = selected.contents.as_ref()?;
  Some(parse_rootlist_items(&contents.items))
}

// NOTE: on the slim leg this helper is only reached through the fold's
// PublishPlaylistRefresh path, which itself is compiled unconditionally —
// the dead-code warning on this arm is silenced by the caller below.

pub(crate) fn build_flat_playlist_items(playlists: &[PlaylistInfo]) -> Vec<PlaylistFolderItem> {
  playlists
    .iter()
    .enumerate()
    .map(|(index, _)| PlaylistFolderItem::Playlist {
      index,
      current_id: 0,
    })
    .collect()
}

pub(crate) fn reconcile_playlist_selection(
  app: &mut App,
  preferred_playlist_id: Option<&str>,
  preferred_folder_id: usize,
  preferred_selected_index: Option<usize>,
) {
  if app.playlist_folder_items.is_empty() {
    app.current_playlist_folder_id = 0;
    return;
  }

  let folder_has_visible = |folder_id: usize, app: &App| {
    app.playlist_folder_items.iter().any(|item| match item {
      PlaylistFolderItem::Folder(folder) => folder.current_id == folder_id,
      PlaylistFolderItem::Playlist { current_id, .. } => *current_id == folder_id,
      PlaylistFolderItem::CommunityPin => false,
    })
  };

  app.current_playlist_folder_id = if folder_has_visible(preferred_folder_id, app) {
    preferred_folder_id
  } else {
    0
  };

  if let Some(playlist_id) = preferred_playlist_id {
    let visible_playlist_index = app
      .get_playlist_display_items()
      .into_iter()
      .enumerate()
      .find_map(|(display_idx, item)| match item {
        PlaylistFolderItem::Playlist { index, .. } => app
          .all_playlists
          .get(*index)
          .filter(|playlist| playlist.id.as_deref() == Some(playlist_id))
          .map(|_| display_idx),
        PlaylistFolderItem::Folder(_) | PlaylistFolderItem::CommunityPin => None,
      });

    let _ = visible_playlist_index;

    let mut target_folder: Option<usize> = None;
    for item in &app.playlist_folder_items {
      if let PlaylistFolderItem::Playlist { index, current_id } = item {
        if let Some(playlist) = app.all_playlists.get(*index) {
          if playlist.id.as_deref() == Some(playlist_id) {
            target_folder = Some(*current_id);
            break;
          }
        }
      }
    }

    if let Some(folder_id) = target_folder {
      app.current_playlist_folder_id = folder_id;
    }
  }

  // The sidebar cursor was TUI presentation state; the folder jump above is
  // all that remains meaningful here.
  let _ = preferred_selected_index;
}

#[cfg(feature = "streaming")]
#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
fn parse_rootlist_items(
  items: &[librespot_protocol::playlist4_external::Item],
) -> Vec<PlaylistFolderNode> {
  let mut root: Vec<PlaylistFolderNode> = Vec::new();
  let mut stack: Vec<Vec<PlaylistFolderNode>> = Vec::new();
  let mut name_stack: Vec<(String, String)> = Vec::new();

  for item in items {
    let uri = item.uri();

    if let Some(rest) = uri.strip_prefix("spotify:start-group:") {
      let (group_id, name) = match rest.find(':') {
        Some(pos) => (rest[..pos].to_string(), rest[pos + 1..].to_string()),
        None => (rest.to_string(), String::new()),
      };
      name_stack.push((group_id, name));
      stack.push(std::mem::take(&mut root));
      root = Vec::new();
    } else if uri.starts_with("spotify:end-group:") {
      if let Some((group_id, name)) = name_stack.pop() {
        let children = std::mem::take(&mut root);
        root = stack.pop().unwrap_or_default();
        root.push(PlaylistFolderNode {
          name: Some(name),
          node_type: PlaylistFolderNodeType::Folder,
          uri: format!("spotify:folder:{}", group_id),
          children,
        });
      }
    } else {
      root.push(PlaylistFolderNode {
        name: None,
        node_type: PlaylistFolderNodeType::Playlist,
        uri: uri.to_string(),
        children: Vec::new(),
      });
    }
  }

  while let Some((group_id, name)) = name_stack.pop() {
    let children = std::mem::take(&mut root);
    root = stack.pop().unwrap_or_default();
    root.push(PlaylistFolderNode {
      name: Some(name),
      node_type: PlaylistFolderNodeType::Folder,
      uri: format!("spotify:folder:{}", group_id),
      children,
    });
  }

  root
}

#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
pub(crate) fn structurize_playlist_folders(
  nodes: &[PlaylistFolderNode],
  playlists: &[PlaylistInfo],
) -> Vec<PlaylistFolderItem> {
  use std::collections::{HashMap, HashSet};

  let playlist_map: HashMap<String, usize> = playlists
    .iter()
    .enumerate()
    .filter_map(|(idx, playlist)| playlist.id.clone().map(|id| (id, idx)))
    .collect();

  let mut items: Vec<PlaylistFolderItem> = Vec::new();
  let mut next_folder_id: usize = 1;
  let mut used_playlist_indices: HashSet<usize> = HashSet::new();

  fn walk(
    nodes: &[PlaylistFolderNode],
    current_folder_id: usize,
    items: &mut Vec<PlaylistFolderItem>,
    next_folder_id: &mut usize,
    playlist_map: &std::collections::HashMap<String, usize>,
    used_playlist_indices: &mut std::collections::HashSet<usize>,
  ) {
    for node in nodes {
      match node.node_type {
        PlaylistFolderNodeType::Folder => {
          let folder_id = *next_folder_id;
          *next_folder_id += 1;

          let name = node.name.as_deref().unwrap_or("Unnamed Folder").to_string();

          items.push(PlaylistFolderItem::Folder(PlaylistFolder {
            name: name.clone(),
            current_id: current_folder_id,
            target_id: folder_id,
          }));

          items.push(PlaylistFolderItem::Folder(PlaylistFolder {
            name: format!("\u{2190} {}", name),
            current_id: folder_id,
            target_id: current_folder_id,
          }));

          walk(
            &node.children,
            folder_id,
            items,
            next_folder_id,
            playlist_map,
            used_playlist_indices,
          );
        }
        PlaylistFolderNodeType::Playlist => {
          let playlist_id = node
            .uri
            .strip_prefix("spotify:playlist:")
            .unwrap_or(&node.uri);

          if let Some(&index) = playlist_map.get(playlist_id) {
            items.push(PlaylistFolderItem::Playlist {
              index,
              current_id: current_folder_id,
            });
            used_playlist_indices.insert(index);
          }
        }
      }
    }
  }

  walk(
    nodes,
    0,
    &mut items,
    &mut next_folder_id,
    &playlist_map,
    &mut used_playlist_indices,
  );

  for (index, _) in playlists.iter().enumerate() {
    if !used_playlist_indices.contains(&index) {
      items.push(PlaylistFolderItem::Playlist {
        index,
        current_id: 0,
      });
    }
  }

  items
}
