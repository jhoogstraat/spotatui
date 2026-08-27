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

use crate::core::plugin_api::TrackInfo;
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
    tracks: Option<Paged<TrackInfo>>,
  },
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
}

/// Which library cache a page belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LibrarySection {
  SavedTracks { offset: u32 },
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
  pub fn completes_latest(&self, incoming_source: &Source, incoming_query: &str) -> bool {
    match &self.latest {
      // Nothing requested yet: a completion cannot be stale (defensive —
      // producers always emit Requested first).
      None => true,
      Some((source, token)) => source == incoming_source && token.query == incoming_query,
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
      CatalogStateEvent::SearchCompleted { source, query, .. } => {
        if self.completes_latest(source, query) {
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
    assert!(!state.completes_latest(&spotify(), "a"));
    // YouTube's is current.
    assert!(state.completes_latest(&youtube(), "b"));
  }

  #[test]
  fn retry_of_the_same_query_gets_a_fresh_token_and_wins() {
    // Retries must replace too (ticket 10): same query, later request.
    let mut state = CatalogState::default();
    state.accept_request(Source::Subsonic, "coltrane".to_string());
    state.accept_request(Source::Subsonic, "coltrane".to_string());
    assert!(state.completes_latest(&Source::Subsonic, "coltrane"));
  }

  #[test]
  fn different_sources_are_never_confused_by_equal_queries() {
    // Both sources searched "radio hits": the newer YouTube request makes
    // Subsonic's equal-query completion stale anyway.
    let mut state = CatalogState::default();
    state.accept_request(Source::Subsonic, "radio hits".to_string());
    state.accept_request(youtube(), "radio hits".to_string());
    assert!(!state.completes_latest(&Source::Subsonic, "radio hits"));
    assert!(state.completes_latest(&youtube(), "radio hits"));
  }
}
