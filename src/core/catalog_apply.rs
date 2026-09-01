//! The fold task's write path for catalog facts (ADR 0016 ticket 15).
//!
//! Gated to nothing: every field it touches exists in every build (search
//! results, track table, native queue are core state). Producers emit
//! [`CatalogStateEvent`]s on the spine; only the fold calls
//! [`apply_catalog_state`], which enforces the stale-request contract through
//! [`crate::core::catalog::CatalogState::absorb`] before any mirror write.

use crate::core::app::{App, SearchResultBlock, TrackTableContext};
use crate::core::catalog::{CatalogOutcome, CatalogStateEvent, SavedSetKind};
use crate::infra::network::IoEvent;

/// The visible length of an optional search page (fold-local copy of the
/// handler's helper: the clamp needs it, and the helper should not be
/// reachable from a producer's import graph).
fn page_len_of<T>(page: &Option<crate::core::pagination::Paged<T>>) -> usize {
  page.as_ref().map(|p| p.items.len()).unwrap_or(0)
}

fn clamp_selected_index(index: &mut Option<usize>, len: usize) {
  *index = match (*index, len) {
    (_, 0) => None,
    (Some(i), len) => Some(i.min(len - 1)),
    (None, _) => None,
  };
}

/// The fold's single write path for catalog facts.
pub fn apply_catalog_state(app: &mut App, event: &CatalogStateEvent) -> CatalogOutcome {
  match event {
    CatalogStateEvent::SearchRequested {
      source,
      query,
      scoped_loading,
    } => {
      // Accept as latest; a replaced query surfaces Loading via the
      // search-scoped flag the dispatcher already raised on dispatch. Rows
      // stay visible until results land or an error replaces them: ticket
      // 10's visible-replacement predicate lives in the domain.
      app.catalog_state.accept_request(*source, query.clone());
      app.search_loading = *scoped_loading;
      CatalogOutcome {
        search_rows_replaced: false,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SearchCompleted {
      source,
      query,
      pages,
    } => {
      let outcome = app.catalog_state.absorb(event);
      if outcome.stale_event_dropped {
        log::info!(
          "catalog fold dropped stale search completion: source={source:?} query={query:?}"
        );
        return outcome;
      }
      // Ticket 17: the completion carries every page, so the fold is the
      // single writer of the visible search surface (the old handler wrote
      // the non-track pages directly under the lock).
      if let Some(page) = &pages.tracks {
        let total = page.total;
        let items = page.items.clone();
        app.search_results.tracks = Some(crate::core::pagination::Paged {
          items,
          total,
          ..Default::default()
        });
        // Focus the songs block so the first hit is selectable immediately
        // (parity with the per-source dispatchers' inline writes).
        app.search_results.selected_tracks_index = Some(0);
        app.search_results.hovered_block = SearchResultBlock::SongSearch;
        app.search_results.selected_block = SearchResultBlock::Empty;
      }
      // A completion with no track rows is the search-failure fact: it
      // retires the spinner but must not wipe the previous surface (the old
      // fold only replaced pages alongside a fresh track page).
      if pages.tracks.is_some() {
        app.search_results.artists = pages.artists.clone();
        app.search_results.albums = pages.albums.clone();
        app.search_results.playlists = pages.playlists.clone();
        app.search_results.shows = pages.shows.clone();
      }
      // A replaced page can be shorter than the previous one (or empty),
      // which would otherwise leave a stale selected-index pointing past the
      // end of the new results (panic-1: unchecked `.items[selected_index]`
      // indexing downstream).
      let tracks_len = page_len_of(&app.search_results.tracks);
      let artists_len = page_len_of(&app.search_results.artists);
      let albums_len = page_len_of(&app.search_results.albums);
      let playlists_len = page_len_of(&app.search_results.playlists);
      let shows_len = page_len_of(&app.search_results.shows);
      clamp_selected_index(&mut app.search_results.selected_tracks_index, tracks_len);
      clamp_selected_index(&mut app.search_results.selected_artists_index, artists_len);
      clamp_selected_index(&mut app.search_results.selected_album_index, albums_len);
      clamp_selected_index(
        &mut app.search_results.selected_playlists_index,
        playlists_len,
      );
      clamp_selected_index(&mut app.search_results.selected_shows_index, shows_len);
      outcome
    }
    CatalogStateEvent::TrackTableReplaced {
      tracks,
      context_label,
    } => {
      // Context labels map onto `TrackTableContext` at the fold boundary:
      // the producer names its surface in plain text and stays decoupled
      // from the enum (which grows with each source).
      let context = match *context_label {
        "youtube_playlist" => Some(TrackTableContext::YouTubePlaylist),
        "subsonic_playlist" => Some(TrackTableContext::SubsonicPlaylist),
        "local_playlist" => Some(TrackTableContext::LocalPlaylist),
        _ => None,
      };
      app.track_table.tracks = tracks.clone();
      app.track_table.selected_index = 0;
      app.track_table.context = context;
      CatalogOutcome {
        track_table_replaced: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::QueueMutated { upcoming } => {
      // The producer sampled the post-mutation queue under its own lock; the
      // fold publishes the same value. Idempotent for direct readers (the
      // Vec contents are already mutated) but the snapshot rebuild it
      // triggers keeps the watch channel honest.
      app.native_queue = upcoming.clone();
      CatalogOutcome {
        queue_changed: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::LibraryPageUpserted { section } => {
      // Legacy arm from batch 1's placeholder shape: producers now carry
      // full payloads through the dedicated variants below. Nothing to do
      // beyond acknowledging the surface moved.
      let _ = section;
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SavedTracksReset => {
      // Ticket 16 batch 2: the liked-songs view re-opened. Bump the
      // generation (invalidating in-flight page fetches), clear the claimed
      // offsets and pages, and drop the visible rows — the exact writes
      // `App::reset_saved_tracks_view` made under the handler's lock, minus
      // the dispatch (the producer emits the fetch event itself).
      app.saved_tracks_prefetch_generation = app.saved_tracks_prefetch_generation.wrapping_add(1);
      app.saved_tracks_prefetch_in_flight.clear();
      app.library.saved_tracks.clear();
      app.pending_track_table_selection = None;
      app.track_table.selected_index = 0;
      app.track_table.tracks.clear();
      app.track_table.context = Some(TrackTableContext::SavedTracks);
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SavedTracksPageUpserted {
      offset,
      total,
      limit,
      next,
      page,
      liked_ids_from_page,
      generation,
    } => {
      let (offset, total, limit, next, generation) =
        (*offset, *total, *limit, next.clone(), *generation);
      // Stale-page enforcement (ticket 10 as enforcement): a page fetched
      // under an older request-generation never lands — the cache it would
      // extend was cleared after this fetch started.
      if app.saved_tracks_prefetch_generation != generation {
        log::info!(
          "catalog fold dropped stale saved-tracks page: offset={offset} fetched-gen={generation} live-gen={}",
          app.saved_tracks_prefetch_generation
        );
        return CatalogOutcome {
          stale_event_dropped: true,
          ..CatalogOutcome::default()
        };
      }
      for id in liked_ids_from_page.clone() {
        app.liked_song_ids_set.insert(id);
      }
      let domain_page = crate::core::pagination::Paged {
        items: page.clone(),
        total,
        limit,
        next,
        offset,
        previous: None,
      };
      app.library.saved_tracks.upsert_page_by_offset(domain_page);
      app.set_saved_tracks_to_table_continuous();
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::LikedEpochBumped => {
      // A local like/unlike landed: in-flight contains-reads that predate
      // the mutation must not clobber it. The fold stamps the epoch here;
      // the worker compares the token it captured before its request.
      app.liked_state_epoch = app.liked_state_epoch.wrapping_add(1);
      CatalogOutcome::default()
    }
    CatalogStateEvent::PlaylistTracksPageUpserted {
      playlist_id,
      page,
      track_ids_for_like_check,
    } => {
      // Stale-page enforcement: only the playlist whose track table is the
      // active browse surface may land pages (the old inline generation +
      // `is_playlist_track_table_active_for` guard, now fold-side).
      let active = rspotify::model::idtypes::PlaylistId::from_id(playlist_id.as_str())
        .ok()
        .map(|id| id.into_static())
        .is_some_and(|id| app.is_playlist_track_table_active_for(&id));
      if !active {
        return CatalogOutcome {
          stale_event_dropped: true,
          ..CatalogOutcome::default()
        };
      }
      app.playlist_tracks_prefetch_in_flight.remove(&page.offset);
      if app.pending_playlist_open.as_deref() == Some(playlist_id.as_str()) {
        app.pending_playlist_open = None;
      }
      let track_ids = track_ids_for_like_check.clone();
      app.playlist_track_pages.upsert_page_by_offset(page.clone());
      app.set_playlist_tracks_to_table_continuous();
      if !track_ids.is_empty() {
        app.dispatch(IoEvent::CurrentUserSavedTracksContains(track_ids));
      }
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SavedAlbumsPageUpserted { page } => {
      if let Some(page) = page {
        app.library.saved_albums.add_pages(page.clone());
      }
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SavedShowsPageUpserted { page } => {
      if let Some(page) = page {
        app.library.saved_shows.add_pages(page.clone());
      }
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SavedArtistsPageUpserted { page } => {
      if let Some(page) = page {
        app.library.saved_artists.add_pages(page.clone());
      }
      // No plugin-data bump: followed artists never had a kind (the old
      // inline write bumped nothing either); scripting reads it via the
      // artists cache refresh instead.
      CatalogOutcome {
        library_page_upserted: true,
        ..CatalogOutcome::default()
      }
    }
    CatalogStateEvent::SavedSetMerged {
      kind,
      saved_ids,
      unsaved_ids,
    } => {
      for id in saved_ids {
        match kind {
          SavedSetKind::LikedTracks => {
            app.liked_song_ids_set.insert(id.clone());
          }
          SavedSetKind::FollowedArtists => {
            app.followed_artist_ids_set.insert(id.clone());
          }
          SavedSetKind::Albums => {
            app.saved_album_ids_set.insert(id.clone());
          }
          SavedSetKind::Shows => {
            app.saved_show_ids_set.insert(id.clone());
          }
        }
      }
      for id in unsaved_ids {
        match kind {
          SavedSetKind::LikedTracks => {
            app.liked_song_ids_set.remove(id);
          }
          SavedSetKind::FollowedArtists => {
            app.followed_artist_ids_set.remove(id);
          }
          SavedSetKind::Albums => {
            app.saved_album_ids_set.remove(id);
          }
          SavedSetKind::Shows => {
            app.saved_show_ids_set.remove(id);
          }
        }
      }
      CatalogOutcome::default()
    }
  }
}

#[cfg(all(test, feature = "streaming"))]
mod tests {
  use super::*;
  use crate::core::plugin_api::TrackInfo;
  use crate::core::source::Source;
  use crate::core::user_config::UserConfig;
  use std::sync::mpsc::channel;

  fn app_with_io() -> App {
    let (tx, rx) = channel();
    let _keep = rx;
    App::new(tx, UserConfig::new(), None)
  }

  #[test]
  fn requested_then_completed_populates_rows_and_focuses_songs() {
    let mut app = app_with_io();
    apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SearchRequested {
        source: Source::YouTube,
        query: "lofi".to_string(),
        scoped_loading: true,
      },
    );
    let mut paged = crate::core::pagination::Paged::<TrackInfo>::default();
    paged.items = vec![track("youtube:v1", "Gone Home")];
    paged.total = 1;
    let outcome = apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SearchCompleted {
        source: Source::YouTube,
        query: "lofi".to_string(),
        pages: Box::new(crate::core::catalog::SearchCompletionPages {
          tracks: Some(paged),
          ..Default::default()
        }),
      },
    );
    assert!(outcome.search_rows_replaced);
    assert_eq!(app.search_results.tracks.as_ref().unwrap().items.len(), 1);
    assert!(matches!(
      app.search_results.hovered_block,
      SearchResultBlock::SongSearch
    ));
  }

  #[test]
  fn completion_for_a_superseded_request_is_dropped() {
    // Two dispatchers racing: YouTube "a", then Subsonic "b" requested.
    // YouTube's completion lands late — it must not replace "b"'s rows.
    let mut app = app_with_io();
    apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SearchRequested {
        source: Source::YouTube,
        query: "a".to_string(),
        scoped_loading: true,
      },
    );
    apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SearchRequested {
        source: Source::Subsonic,
        query: "b".to_string(),
        scoped_loading: true,
      },
    );
    let mut paged = crate::core::pagination::Paged::<TrackInfo>::default();
    paged.items = vec![track("youtube:a1", "Old Request Row")];
    paged.total = 1;
    let outcome = apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SearchCompleted {
        source: Source::YouTube,
        query: "a".to_string(),
        pages: Box::new(crate::core::catalog::SearchCompletionPages {
          tracks: Some(paged),
          ..Default::default()
        }),
      },
    );
    assert!(outcome.stale_event_dropped);
    assert!(
      app.search_results.tracks.is_none()
        || app
          .search_results
          .tracks
          .as_ref()
          .unwrap()
          .items
          .iter()
          .all(|t| t.name != "Old Request Row"),
      "superseded rows must never land"
    );
  }

  #[test]
  fn track_table_replacement_sets_context_and_resets_cursor() {
    let mut app = app_with_io();
    app.track_table.tracks = vec![track("old:1", "older")];
    app.track_table.selected_index = 5;
    let outcome = apply_catalog_state(
      &mut app,
      &CatalogStateEvent::TrackTableReplaced {
        tracks: vec![track("subsonic:new", "new row")],
        context_label: "subsonic_playlist",
      },
    );
    assert!(outcome.track_table_replaced);
    assert_eq!(app.track_table.tracks.len(), 1);
    assert_eq!(app.track_table.selected_index, 0);
    assert!(matches!(
      app.track_table.context,
      Some(TrackTableContext::SubsonicPlaylist)
    ));
  }

  fn track(uri: &str, name: &str) -> TrackInfo {
    TrackInfo {
      uri: Some(uri.to_string()),
      name: name.to_string(),
      artists: Vec::new(),
      album: String::new(),
      duration_ms: 0,
      id: None,
      album_id: None,
      artist_refs: Vec::new(),
      is_playable: true,
      is_local: false,
      track_number: 0,
      explicit: false,
      image_url: None,
    }
  }

  // ── Ticket 16 batch 2: library pages fold-side ────────────────────────────

  fn saved_tracks_page_event(offset: u32, generation: u64, name: &str) -> CatalogStateEvent {
    CatalogStateEvent::SavedTracksPageUpserted {
      offset,
      total: 2,
      limit: 2,
      next: None,
      page: vec![track("spotify:track:x", name)],
      liked_ids_from_page: vec!["liked-id".to_string()],
      generation,
    }
  }

  #[test]
  fn saved_tracks_page_under_a_live_generation_lands() {
    let mut app = app_with_io();
    apply_catalog_state(&mut app, &CatalogStateEvent::SavedTracksReset);
    let generation = app.saved_tracks_prefetch_generation;
    let outcome = apply_catalog_state(&mut app, &saved_tracks_page_event(0, generation, "row"));
    assert!(!outcome.stale_event_dropped);
    assert!(outcome.library_page_upserted);
    assert_eq!(app.track_table.tracks.len(), 1);
    assert!(app.liked_song_ids_set.contains("liked-id"));
  }

  #[test]
  fn saved_tracks_page_under_a_superseded_generation_never_lands() {
    // The view was re-opened (generation bumped) while a fetch was in
    // flight: its page must be dropped wholesale, rows and liked ids alike.
    let mut app = app_with_io();
    apply_catalog_state(&mut app, &CatalogStateEvent::SavedTracksReset);
    let outcome = apply_catalog_state(&mut app, &saved_tracks_page_event(0, 999, "stale"));
    assert!(outcome.stale_event_dropped);
    assert!(app.track_table.tracks.is_empty());
    assert!(!app.liked_song_ids_set.contains("liked-id"));
  }

  #[test]
  fn saved_tracks_reset_clears_pages_and_visible_rows() {
    let mut app = app_with_io();
    // Land a page, then re-open the view: the cache and the table reset.
    apply_catalog_state(&mut app, &saved_tracks_page_event(0, 0, "first"));
    assert!(!app.track_table.tracks.is_empty());
    let before = app.saved_tracks_prefetch_generation;
    apply_catalog_state(&mut app, &CatalogStateEvent::SavedTracksReset);
    assert_eq!(app.saved_tracks_prefetch_generation, before + 1);
    assert!(app.library.saved_tracks.pages.is_empty());
    assert!(app.track_table.tracks.is_empty());
    assert!(app.saved_tracks_prefetch_in_flight.is_empty());
  }

  #[test]
  fn liked_epoch_bump_advances_the_re_read_counter() {
    let mut app = app_with_io();
    let before = app.liked_state_epoch;
    apply_catalog_state(&mut app, &CatalogStateEvent::LikedEpochBumped);
    assert_eq!(app.liked_state_epoch, before + 1);
  }

  #[test]
  fn playlist_page_for_an_inactive_table_is_dropped() {
    let mut app = app_with_io();
    let outcome = apply_catalog_state(
      &mut app,
      &CatalogStateEvent::PlaylistTracksPageUpserted {
        playlist_id: "not-open".to_string(),
        page: crate::core::pagination::Paged::default(),
        track_ids_for_like_check: vec![],
      },
    );
    assert!(outcome.stale_event_dropped);
  }

  #[test]
  fn saved_set_merge_inserts_and_removes() {
    use crate::core::catalog::SavedSetKind;
    let mut app = app_with_io();
    apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SavedSetMerged {
        kind: SavedSetKind::Albums,
        saved_ids: vec!["a1".to_string()],
        unsaved_ids: vec![],
      },
    );
    assert!(app.saved_album_ids_set.contains("a1"));
    apply_catalog_state(
      &mut app,
      &CatalogStateEvent::SavedSetMerged {
        kind: SavedSetKind::Albums,
        saved_ids: vec![],
        unsaved_ids: vec!["a1".to_string()],
      },
    );
    assert!(!app.saved_album_ids_set.contains("a1"));
  }
}
