//! The catalog domain (ADR 0016 stage 3, ticket 15): search results, the
//! shared browse table, library pages, and the native queue arrive as typed
//! spine events and are folded by one owner.
//!
//! The stale-visibility contract from ticket 10 becomes *enforcement* here:
//! [`CatalogState::absorb`] refuses to publish rows for a query other than
//! the latest requested one, so two dispatchers completing out of order can
//! never clobber the newer request's rows. Producers emit
//! [`CatalogStateEvent`]s on the Event Spine; the fold task is the single
//! writer of `search_results`, `track_table`, the library page caches, and
//! `native_queue` mutations that flow through events.
//!
//! On slim CI legs (no producers compiled) this module's surface stays alive
//! only through its tests, hence the blanket dead-code allow.

#![allow(dead_code)]

use crate::core::plugin_api::{PlayableInfo, TrackInfo};

/// A selection snapshot for the playlist refresh flow (ticket 16 batch 2):
/// which playlist, which folder, and the folder's display index. The fold
/// carries these between the page-1 install and the background completion.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaylistSelection {
  pub playlist_id: Option<String>,
  pub folder_id: usize,
  pub selected_index: Option<usize>,
}
use crate::core::source::Source;

use crate::core::pagination::Paged;

/// Which catalog query a result belongs to. The fold drops results whose
/// request token no longer matches the latest accepted request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestToken {
  /// The search text (empty for non-search requests).
  pub query: String,
  /// Monotonic per-fold counter: every new request bumps it, so retries of
  /// the same query still get a fresh token.
  pub seq: u64,
}

/// All five search pages a completion carries. Spotify fills every field;
/// single-source engines pass track rows only and the fold clears the
/// remaining pages on a fresh completion.
#[derive(Debug, Clone, Default)]
pub struct SearchCompletionPages {
  pub tracks: Option<Paged<TrackInfo>>,
  pub artists: Option<Paged<crate::core::plugin_api::ArtistInfo>>,
  pub albums: Option<Paged<crate::core::plugin_api::AlbumInfo>>,
  pub playlists: Option<Paged<crate::core::plugin_api::PlaylistInfo>>,
  pub shows: Option<Paged<crate::core::plugin_api::ShowInfo>>,
}

/// One observed catalog fact.
#[derive(Debug, Clone)]
pub enum CatalogStateEvent {
  /// A dispatcher is about to fetch: the fold accepts this request as latest
  /// (bumping the token) and may clear visible rows for replaced queries.
  SearchRequested {
    source: Source,
    query: String,
    scoped_loading: bool,
  },
  /// Rows for a source's search landed.
  SearchCompleted {
    source: Source,
    query: String,
    /// Request generation assigned by the fold when the search began.
    /// Completions with an older generation are stale even when the query text
    /// is identical (for example, a retry racing its first attempt).
    generation: u64,
    /// All five pages: the fold is the single writer of the visible search
    /// surface (ticket 17). Boxed — five `Paged` payloads dwarf the enum's
    /// other variants.
    pages: Box<SearchCompletionPages>,
  },
  /// Begin a fresh saved-library page-one request and invalidate older pages.
  LibraryGenerationStarted { section: LibrarySection },
  /// A playlist/album listing landed in the shared browse table.
  TrackTableReplaced {
    tracks: Vec<TrackInfo>,
    context_label: &'static str,
  },
  /// The native queue was mutated (enqueue/remove/clear/advance).
  QueueMutated {
    /// The authoritative post-mutation queue, sampled by the producer under
    /// its existing lock (the mutation method itself).
    upcoming: Vec<TrackInfo>,
  },
  /// Library pages landed in an owned cache (`upsert_page_by_offset` runs in
  /// the fold; the event carries everything needed to build one page).
  LibraryPageUpserted { section: LibrarySection },
  /// The saved-tracks view was re-opened: drop cached pages, claimed
  /// offsets, and visible rows; invalidate in-flight fetches (ticket 16
  /// batch 2; the exact writes of the old `reset_saved_tracks_view`).
  SavedTracksReset,
  /// One saved-tracks page landed (ticket 16 batch 2). The fold upserts it
  /// by offset, rebuilds the continuous visible table, merges the page's
  /// liked-state ids, and drops the whole event when the cache epoch moved
  /// since this fetch was stamped.
  SavedTracksPageUpserted {
    offset: u32,
    total: u32,
    limit: u32,
    /// Spotify's next-page link, resolved to "there is a next page" (the
    /// domain page carries the link but nothing reads its text).
    next: Option<String>,
    page: Vec<TrackInfo>,
    /// Bare track ids from the page, merged into `liked_song_ids_set` by
    /// the fold (the old inline `populate_liked_song_ids_from_saved_tracks`).
    liked_ids_from_page: Vec<String>,
    /// The cache epoch this fetch was stamped with at dispatch time.
    generation: u64,
  },
  /// A local like/unlike landed: in-flight liked-state reads predating the
  /// mutation re-read instead of clobbering it.
  LikedEpochBumped,
  /// A saved-albums page landed (index-keyed cache; `None` = empty page —
  /// the completion bump still fires, plugin data waits on it).
  SavedAlbumsPageUpserted {
    generation: u64,
    page: Option<crate::core::pagination::Paged<crate::core::plugin_api::SavedAlbumInfo>>,
  },
  /// A saved-shows page landed (same completion contract as albums).
  SavedShowsPageUpserted {
    generation: u64,
    page: Option<crate::core::pagination::Paged<crate::core::plugin_api::ShowInfo>>,
  },
  /// A saved-albums page landed (same completion contract as albums).
  SavedArtistsPageUpserted {
    generation: u64,
    page: Option<crate::core::pagination::CursorPaged<crate::core::plugin_api::ArtistInfo>>,
  },
  /// Merge membership changes into one of the saved-id sets (contains
  /// responses and optimistic add/remove outcomes).
  SavedSetMerged {
    kind: SavedSetKind,
    saved_ids: Vec<String>,
    unsaved_ids: Vec<String>,
  },
  /// A playlist's track pages landed (offset-keyed cache), with everything
  /// the fold needs to rebuild the visible table: the owning playlist id
  /// (staleness), the upserted page, and the liked-state ids the old inline
  /// path dispatched a contains-check for.
  PlaylistTracksPageUpserted {
    playlist_id: String,
    generation: u64,
    page: crate::core::pagination::Paged<(u32, PlayableInfo)>,
    track_ids_for_like_check: Vec<String>,
  },
}

/// Which library cache a page belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LibrarySection {
  SavedTracks { offset: u32 },
  SavedAlbums,
  SavedShows,
  SavedArtists,
}

/// Which saved-id set a membership merge applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavedSetKind {
  LikedTracks,
  FollowedArtists,
  Albums,
  Shows,
}

/// Authoritative catalog facts folded in stream order.
#[derive(Debug, Clone, Default)]
pub struct CatalogState {
  /// Latest accepted request token; earlier tokens are stale.
  pub latest: Option<(Source, RequestToken)>,
}

/// What folding one event produced.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogOutcome {
  pub search_rows_replaced: bool,
  pub track_table_replaced: bool,
  pub queue_changed: bool,
  pub library_page_upserted: bool,
  /// The event carried rows for a superseded request: dropped wholesale.
  pub stale_event_dropped: bool,
}

impl CatalogOutcome {
  pub fn any(&self) -> bool {
    self.search_rows_replaced
      || self.track_table_replaced
      || self.queue_changed
      || self.library_page_upserted
      || self.stale_event_dropped
  }
}

impl CatalogState {
  /// Pure decision: does `incoming` belong to the latest accepted request?
  ///
  /// This is ticket 10's contract as enforcement. Two dispatchers completing
  /// out of order cannot clobber the newer request's rows because the fold
  /// refuses anything whose token predates the latest accepted request.
  pub fn completes_latest(
    &self,
    incoming_source: &Source,
    incoming_query: &str,
    generation: u64,
  ) -> bool {
    match &self.latest {
      // Nothing requested yet: a completion cannot be stale (defensive —
      // producers always emit Requested first).
      None => true,
      Some((source, token)) => {
        source == incoming_source && token.query == incoming_query && token.seq == generation
      }
    }
  }

  /// Accept a new request as latest. Returns the fresh token producers don't
  /// need to know (the fold owns sequencing).
  pub fn accept_request(&mut self, source: Source, query: String) -> RequestToken {
    let seq = self.latest.as_ref().map_or(0, |(_, t)| t.seq + 1);
    let token = RequestToken { query, seq };
    self.latest = Some((source, token.clone()));
    token
  }

  /// Reduce one event against pure state. Mirror writes happen in
  /// `core::catalog_apply` after this returns.
  pub fn absorb(&mut self, event: &CatalogStateEvent) -> CatalogOutcome {
    match event {
      CatalogStateEvent::SearchRequested { source, query, .. } => {
        self.accept_request(*source, query.clone());
        CatalogOutcome {
          search_rows_replaced: false,
          ..CatalogOutcome::default()
        }
      }
      CatalogStateEvent::SearchCompleted {
        source,
        query,
        generation,
        ..
      } => {
        if self.completes_latest(source, query, *generation) {
          CatalogOutcome {
            search_rows_replaced: true,
            ..CatalogOutcome::default()
          }
        } else {
          CatalogOutcome {
            stale_event_dropped: true,
            ..CatalogOutcome::default()
          }
        }
      }
      _ => CatalogOutcome::default(),
    }
  }
}

#[cfg(test)]
mod catalog_domain_tests {
  use super::*;

  fn spotify() -> Source {
    Source::Spotify
  }

  fn youtube() -> Source {
    Source::YouTube
  }

  #[test]
  fn newer_request_makes_older_completion_stale() {
    // Ticket 15's cross-source race: Spotify search "a" starts, then YouTube
    // search "b" starts, then Spotify's slow response lands first-in-flight.
    let mut state = CatalogState::default();
    state.accept_request(spotify(), "a".to_string());
    state.accept_request(youtube(), "b".to_string());
    // Spotify's late completion references the superseded request.
    assert!(!state.completes_latest(&spotify(), "a", 0));
    // YouTube's is current.
    assert!(state.completes_latest(&youtube(), "b", 1));
  }

  #[test]
  fn retry_of_the_same_query_gets_a_fresh_token_and_wins() {
    // Retries must replace too (ticket 10): same query, later request.
    let mut state = CatalogState::default();
    state.accept_request(Source::Subsonic, "coltrane".to_string());
    state.accept_request(Source::Subsonic, "coltrane".to_string());
    assert!(state.completes_latest(&Source::Subsonic, "coltrane", 1));
  }

  #[test]
  fn different_sources_are_never_confused_by_equal_queries() {
    // Both sources searched "radio hits": the newer YouTube request makes
    // Subsonic's equal-query completion stale anyway.
    let mut state = CatalogState::default();
    state.accept_request(Source::Subsonic, "radio hits".to_string());
    state.accept_request(youtube(), "radio hits".to_string());
    assert!(!state.completes_latest(&Source::Subsonic, "radio hits", 0));
    assert!(state.completes_latest(&youtube(), "radio hits", 1));
  }
}
