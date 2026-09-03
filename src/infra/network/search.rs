use super::{IoEvent, Network};
use crate::core::plugin_api::{AlbumInfo, ArtistInfo, PlaylistInfo, ShowInfo, TrackInfo};
use crate::infra::network::mapping::map_page;
use crate::runtime::engine::SessionFact;
use anyhow::anyhow;
use rspotify::model::{
  artist::FullArtist, enums::Country, page::Page, playlist::SimplifiedPlaylist,
  show::SimplifiedShow, track::FullTrack, SimplifiedAlbum,
};
use rspotify::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ArtistSearchResponse {
  artists: Page<FullArtist>,
}

/// `pub(crate)` so the DJ resolver can reuse the track-search shape: unlike the
/// handlers in this module, it needs a *returned* result rather than a write into
/// `App` (see `crate::infra::dj::resolve`).
#[derive(Deserialize, Debug)]
pub(crate) struct TrackSearchResponse {
  pub(crate) tracks: Page<FullTrack>,
}

#[derive(Deserialize, Debug)]
struct AlbumSearchResponse {
  albums: Page<SimplifiedAlbum>,
}

#[derive(Deserialize, Debug)]
struct PlaylistSearchResponse {
  playlists: Page<SimplifiedPlaylist>,
}

#[derive(Deserialize, Debug)]
struct ShowSearchResponse {
  shows: Page<SimplifiedShow>,
}

pub trait SearchNetwork {
  async fn get_search_results(
    &mut self,
    search_term: String,
    country: Option<Country>,
    generation: u64,
  );
  async fn search_tracks_for_playlist(&mut self, search_term: String);
}

impl Network {
  /// Ticket 15: mark a search as finished-with-error so the fold retires the
  /// loading flag even though no rows landed. Emits a completion carrying no
  /// rows; per-source error messages continue through `handle_error` unchanged.
  async fn search_failed_spotify(
    &mut self,
    query: &str,
    generation: u64,
    tracks: Option<crate::core::pagination::Paged<TrackInfo>>,
  ) {
    crate::runtime::engine::emit_catalog_event(
      &self.app,
      crate::core::catalog::CatalogStateEvent::SearchCompleted {
        source: crate::core::source::Source::Spotify,
        query: query.to_string(),
        generation,
        pages: Box::new(crate::core::catalog::SearchCompletionPages {
          tracks,
          ..Default::default()
        }),
      },
    )
    .await;
  }
}

impl SearchNetwork for Network {
  async fn get_search_results(
    &mut self,
    search_term: String,
    country: Option<Country>,
    generation: u64,
  ) {
    // Don't pass market to search - when market is specified, Spotify doesn't return
    // available_markets field, but rspotify 0.14 models require it for tracks/albums.
    // We'll handle null playlist fields by searching playlists separately without requiring all fields.
    let _country = country;

    let base_query = |search_type: &str| {
      vec![
        ("q", search_term.clone()),
        ("type", search_type.to_string()),
        ("limit", self.small_search_limit.to_string()),
        ("offset", "0".to_string()),
      ]
    };

    let track_query = base_query("track");
    let album_query = base_query("album");
    let playlist_query = base_query("playlist");
    let show_query = base_query("show");
    let artist_query = vec![
      ("q", search_term.clone()),
      ("type", "artist".to_string()),
      ("limit", self.small_search_limit.to_string()),
      ("offset", "0".to_string()),
    ];

    let (track_search, album_search, show_search, playlist_search, artist_search) = tokio::join!(
      self.spotify_get_typed::<TrackSearchResponse>("search", &track_query),
      self.spotify_get_typed::<AlbumSearchResponse>("search", &album_query),
      self.spotify_get_typed::<ShowSearchResponse>("search", &show_query),
      self.spotify_get_typed::<PlaylistSearchResponse>("search", &playlist_query),
      self.spotify_get_typed::<ArtistSearchResponse>("search", &artist_query)
    );

    let track_result = match track_search {
      Ok(res) => Some(res.tracks),
      Err(e) => {
        self
          .search_failed_spotify(&search_term, generation, None)
          .await;
        self.handle_error(anyhow!(e)).await;
        return;
      }
    };
    let track_page = track_result
      .as_ref()
      .map(|p| map_page(p, |t| TrackInfo::from(t)));
    let album_result = match album_search {
      Ok(res) => Some(res.albums),
      Err(e) => {
        self
          .search_failed_spotify(&search_term, generation, track_page)
          .await;
        self.handle_error(anyhow!(e)).await;
        return;
      }
    };
    let show_result = match show_search {
      Ok(res) => Some(res.shows),
      Err(e) => {
        self
          .search_failed_spotify(&search_term, generation, track_page)
          .await;
        self.handle_error(anyhow!(e)).await;
        return;
      }
    };

    let artist_result = artist_search.ok().map(|res| res.artists);

    // Handle playlist search separately since it can fail with null fields from Spotify API
    // Silently ignore playlist errors - this is a known Spotify API issue
    let playlist_result = playlist_search.ok().map(|res| res.playlists);

    // Ticket 17: the completion event carries every page, so the fold is
    // the single writer of the visible search surface (pages, focus, and
    // the stale-index clamps all moved into the fold's reducer). The
    // liked/follow/saved membership checks still ride the IoEvent lane,
    // dispatched by the fold to preserve the old spinner semantics.
    let mut checks: Vec<IoEvent> = Vec::new();
    if let Some(ref track_results) = track_result {
      let track_ids = track_results
        .items
        .iter()
        .filter_map(|track| track.id.as_ref().map(|id| id.id().to_string()))
        .collect();
      checks.push(IoEvent::CurrentUserSavedTracksContains(track_ids));
    }
    if let Some(ref album_results) = album_result {
      let artist_ids = album_results
        .items
        .iter()
        .flat_map(|item| {
          item
            .artists
            .iter()
            .filter_map(|artist| artist.id.as_ref().map(|id| id.id().to_string()))
        })
        .collect();
      checks.push(IoEvent::UserArtistFollowCheck(artist_ids));
      let album_ids = album_results
        .items
        .iter()
        .filter_map(|album| album.id.as_ref().map(|id| id.id().to_string()))
        .collect();
      checks.push(IoEvent::CurrentUserSavedAlbumsContains(album_ids));
    }
    if let Some(ref show_results) = show_result {
      let show_ids = show_results
        .items
        .iter()
        .map(|show| show.id.id().to_string())
        .collect();
      checks.push(IoEvent::CurrentUserSavedShowsContains(show_ids));
    }
    if !checks.is_empty() {
      self.emit_session(SessionFact::DispatchIo(checks)).await;
    }

    // Convert rspotify pages to domain Paged<T> and publish the completion
    // (the ticket 15 event shape, extended in ticket 17 to carry every page).
    let tracks_page = track_page;
    let search_term_snapshot = search_term.clone();
    let artists_page = artist_result
      .as_ref()
      .map(|p| map_page(p, |a| ArtistInfo::from(a)));
    let albums_page = album_result
      .as_ref()
      .map(|p| map_page(p, |a| AlbumInfo::from(a)));
    let playlists_page = playlist_result
      .as_ref()
      .map(|p| map_page(p, PlaylistInfo::from_simplified));
    let shows_page = show_result
      .as_ref()
      .map(|p| map_page(p, |s| ShowInfo::from(s)));

    crate::runtime::engine::emit_catalog_event(
      &self.app,
      crate::core::catalog::CatalogStateEvent::SearchCompleted {
        source: crate::core::source::Source::Spotify,
        query: search_term_snapshot,
        generation,
        pages: Box::new(crate::core::catalog::SearchCompletionPages {
          tracks: tracks_page,
          artists: artists_page,
          albums: albums_page,
          playlists: playlists_page,
          shows: shows_page,
        }),
      },
    )
    .await;
  }

  async fn search_tracks_for_playlist(&mut self, search_term: String) {
    let query = vec![
      ("q", search_term),
      ("type", "track".to_string()),
      ("limit", self.large_search_limit.to_string()),
      ("offset", "0".to_string()),
    ];

    let tracks = match self
      .spotify_get_typed::<TrackSearchResponse>("search", &query)
      .await
    {
      Ok(res) => res
        .tracks
        .items
        .iter()
        .filter(|t| t.id.is_some())
        .map(TrackInfo::from)
        .collect::<Vec<_>>(),
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
        return;
      }
    };

    self
      .emit_session(SessionFact::CreatePlaylistSearchResults(tracks))
      .await;
  }
}

/// Keep a `search_results.selected_*_index` field in range of its
/// freshly-replaced sibling page. A new search page can be shorter than (or
/// empty relative to) the page it replaces; without this, a stale index can
/// point past the end of the new `items` Vec, which panics wherever a caller
/// indexes directly (panic-1: `src/tui/handlers/search_results.rs`, the `D`
/// / unfollow-playlist handler).
#[cfg(test)]
mod tests {
  use super::*;
  use crate::core::pagination::Paged;

  fn page_len<T>(page: &Option<crate::core::pagination::Paged<T>>) -> usize {
    page.as_ref().map(|p| p.items.len()).unwrap_or(0)
  }

  fn clamp_selected_index(index: &mut Option<usize>, len: usize) {
    *index = match (*index, len) {
      (_, 0) => None,
      (Some(i), len) => Some(i.min(len - 1)),
      (None, _) => None,
    };
  }

  fn page_of(len: usize) -> Option<Paged<u32>> {
    Some(Paged {
      items: (0..len as u32).collect(),
      total: len as u32,
      ..Default::default()
    })
  }

  /// Replacing a long page (e.g. 21 playlists, selected index 20) with a
  /// shorter one (e.g. 2 playlists) must clamp the selected index into range
  /// instead of leaving it pointing past the end — this is the root cause of
  /// panic-1 (unchecked `.items[selected_index]` indexing downstream).
  #[test]
  fn clamp_selects_last_valid_index_when_new_page_is_shorter() {
    let mut index = Some(20);
    let new_page = page_of(2);
    clamp_selected_index(&mut index, page_len(&new_page));
    assert_eq!(index, Some(1));
  }

  #[test]
  fn clamp_resets_to_none_when_new_page_is_empty() {
    let mut index = Some(20);
    let empty_page: Option<Paged<u32>> = page_of(0);
    clamp_selected_index(&mut index, page_len(&empty_page));
    assert_eq!(index, None);

    let mut index = Some(0);
    let none_page: Option<Paged<u32>> = None;
    clamp_selected_index(&mut index, page_len(&none_page));
    assert_eq!(index, None);
  }

  #[test]
  fn clamp_leaves_in_range_index_untouched() {
    let mut index = Some(1);
    let new_page = page_of(5);
    clamp_selected_index(&mut index, page_len(&new_page));
    assert_eq!(index, Some(1));
  }

  #[test]
  fn clamp_leaves_none_as_none_when_page_is_nonempty() {
    let mut index = None;
    let new_page = page_of(5);
    clamp_selected_index(&mut index, page_len(&new_page));
    assert_eq!(index, None);
  }
}
