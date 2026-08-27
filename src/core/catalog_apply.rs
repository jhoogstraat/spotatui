//! The fold task's write path for catalog facts (ADR 0016 ticket 15).
//!
//! Gated to nothing: every field it touches exists in every build (search
//! results, track table, native queue are core state). Producers emit
//! [`CatalogStateEvent`]s on the spine; only the fold calls
//! [`apply_catalog_state`], which enforces the stale-request contract through
//! [`crate::core::catalog::CatalogState::absorb`] before any mirror write.

use crate::core::app::{App, SearchResultBlock, TrackTableContext};
use crate::core::catalog::{CatalogOutcome, CatalogStateEvent};

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
      tracks,
    } => {
      let outcome = app.catalog_state.absorb(event);
      if outcome.stale_event_dropped {
        log::info!(
          "catalog fold dropped stale search completion: source={source:?} query={query:?}"
        );
        return outcome;
      }
      if let Some(page) = tracks {
        let total = page.total;
        let items = page.items.clone();
        app.search_results.tracks = Some(crate::core::pagination::Paged {
          items,
          total,
          ..Default::default()
        });
        app.search_results.albums = None;
        app.search_results.artists = None;
        app.search_results.playlists = None;
        app.search_results.shows = None;
        // Focus the songs block so the first hit is selectable immediately
        // (parity with the per-source dispatchers' inline writes).
        app.search_results.selected_tracks_index = Some(0);
        app.search_results.hovered_block = SearchResultBlock::SongSearch;
        app.search_results.selected_block = SearchResultBlock::Empty;
      }
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
    _ => CatalogOutcome::default(),
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
        tracks: Some(paged),
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
        tracks: Some(paged),
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
}
