//! Internet-radio browse/search/playback routing.
//!
//! [`route_radio_event`] is called from the runtime IoEvent pump *after* the
//! local-files and Subsonic dispatches and *before* `handle_network_event`.
//! When an event targets the radio source (a station-list/search request, or a
//! `radio:` playback URI) it is handled here and consumed; otherwise it falls
//! through to the normal Spotify dispatch.
//!
//! ## Decoupling & device ownership
//!
//! Radio playback owns a single piece of state, [`App::radio_playback`], and
//! never writes Spotify/librespot fields. Only one backend holds the audio
//! device at a time: starting radio pauses librespot **and** tears down any
//! local/Subsonic session; the reciprocal teardowns live in those sources'
//! start paths.
//!
//! ## Live-stream semantics
//!
//! A station is infinite, so several transport events are **consumed as
//! no-ops** while radio owns the session — `Seek` (nothing to seek within),
//! `Repeat`, and `NextTrack`/`PreviousTrack` (no queue). Consuming them is load-bearing:
//! falling through would hand them to the Spotify dispatch, which would try to
//! act on a Spotify session that isn't playing.

use std::sync::Arc;

use super::{
  config_station_to_track_info, is_radio_uri, open_radio_stream, stream_url_from_uri,
  uri_for_stream_url, RadioPlaybackState, RadioSource,
};
use crate::core::catalog::CatalogStateEvent;
use crate::core::pagination::Paged;
use crate::core::plugin_api::TrackInfo;
use crate::core::source::Searcher;
use crate::core::source::Source;
use crate::core::state::merged_radio_stations;
use crate::infra::audio::LocalPlayer;
use crate::infra::network::IoEvent;
use crate::runtime::engine::{
  DecodedSourceAnswer as Answer, DecodedSourceFact as Fact, DecodedSourceQuery as Query,
  EngineEvent, FoldAnswer, FoldQuery, Spine,
};

/// Intercept events that target the radio source.
///
/// Returns `true` if the event was handled (and must **not** be forwarded to
/// the Spotify network), `false` to let the normal dispatch run.
pub async fn route_radio_event(spine: &Spine, event: &IoEvent) -> bool {
  match event {
    IoEvent::GetRadioStations => {
      load_radio_stations(spine).await;
      true
    }
    IoEvent::GetRadioSearchResults(query) => {
      run_radio_search(spine, query).await;
      true
    }
    // A station play. Radio "queues" collapse to the one station at the offset:
    // stations are infinite, so queueing the rest is meaningless.
    IoEvent::StartPlayback(None, Some(uris), offset)
      if uris.first().is_some_and(|u| is_radio_uri(u)) =>
    {
      let index = offset.unwrap_or(0).min(uris.len().saturating_sub(1));
      start_radio(spine, &uris[index]).await;
      true
    }
    IoEvent::StartPlayback(Some(uri), _, _) if is_radio_uri(uri) => {
      start_radio(spine, uri).await;
      true
    }
    // Bare "resume current" — ours only while radio owns the session.
    IoEvent::StartPlayback(None, None, None) => match player(spine).await {
      Some(p) => {
        p.resume();
        true
      }
      None => false,
    },
    // Any other start is a local/Subsonic/Spotify play: relinquish the device,
    // then let the normal dispatch run.
    IoEvent::StartPlayback(..) => {
      teardown_radio(spine).await;
      false
    }
    IoEvent::PausePlayback => match player(spine).await {
      Some(p) => {
        p.pause();
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
    // Meaningless on a live stream — consume so they never reach Spotify.
    IoEvent::Seek(_) => player(spine).await.is_some(),
    IoEvent::Repeat(_) => player(spine).await.is_some(),
    IoEvent::NextTrack | IoEvent::PreviousTrack | IoEvent::ForcePreviousTrack
      if player(spine).await.is_some() =>
    {
      status(spine, "Live radio: no track skipping", 6).await;
      true
    }
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
    Fact::RadioStatus {
      message: message.into(),
      ttl_secs,
    },
  )
  .await;
}

// ---------------------------------------------------------------------------
// Browse + search
// ---------------------------------------------------------------------------

/// Load configured and saved stations into `app.radio_stations` (the sidebar's
/// Stations panel). No network.
async fn load_radio_stations(spine: &Spine) {
  let (configured, saved) = match decoded(spine, Query::RadioConfig).await {
    Some(Answer::RadioConfig { configured, saved }) => (configured, saved),
    _ => return,
  };
  let stations: Vec<TrackInfo> = merged_radio_stations(&configured, &saved)
    .into_iter()
    .map(|station| config_station_to_track_info(&station.name, &station.url))
    .collect();
  let empty = stations.is_empty();
  emit(spine, Fact::RadioStations(stations)).await;
  if empty {
    status(
      spine,
      "No radio stations configured or saved; search to add some",
      6,
    )
    .await;
  }
}

/// Search the radio-browser.info directory and populate `app.search_results`.
///
/// Like the Subsonic search, only the songs block is populated — each row is a
/// station; Enter on one dispatches its `radio:` URI back through this module.
async fn run_radio_search(spine: &Spine, query: &str) {
  spine
    .emit(EngineEvent::Catalog(CatalogStateEvent::SearchRequested {
      source: Source::Radio,
      query: query.to_string(),
      scoped_loading: true,
    }))
    .await;
  let source = RadioSource::new();
  match source.search(query).await {
    Ok(results) => {
      let total = results.tracks.len() as u32;
      spine
        .emit(EngineEvent::Catalog(CatalogStateEvent::SearchCompleted {
          source: Source::Radio,
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
    Err(e) => status(spine, format!("Radio search failed: {e}"), 6).await,
  }
}

// ---------------------------------------------------------------------------
// Playback
// ---------------------------------------------------------------------------

/// The live radio player, if a radio session is active.
async fn player(spine: &Spine) -> Option<Arc<LocalPlayer>> {
  match decoded(spine, Query::RadioPlayer).await? {
    Answer::RadioPlayer(player) => player,
    _ => None,
  }
}

/// Find the station row for `uri` in the browse views a play request can
/// originate from: the sidebar station list, the shared track table, and the
/// search results. Falls back to a synthetic row built from the URL itself, so
/// a `radio:` URI with no backing row still plays (it just has no nice name).
pub(crate) fn snapshot_station(app: &crate::core::app::App, uri: &str) -> TrackInfo {
  let matches = |t: &&TrackInfo| t.uri.as_deref() == Some(uri);
  app
    .radio_stations
    .iter()
    .find(matches)
    .or_else(|| app.track_table.tracks.iter().find(matches))
    .or_else(|| {
      app
        .search_results
        .tracks
        .as_ref()
        .and_then(|p| p.items.iter().find(matches))
    })
    .cloned()
    .unwrap_or_else(|| {
      let url = stream_url_from_uri(uri).unwrap_or(uri);
      config_station_to_track_info(url, url)
    })
}

/// Release the other backends so only radio holds the output device.
async fn release_other_backends(spine: &Spine, clear_radio: bool) {
  let backends = spine
    .emit_acked(EngineEvent::Decoded(Fact::ClaimBackendPlayers {
      keep: (!clear_radio).then_some(crate::core::source::Source::Radio),
    }))
    .await
    .and_then(|ack| ack.backend_players);
  if let Some(backends) = backends {
    #[cfg(feature = "streaming")]
    if let Some(player) = backends.streaming {
      player.pause();
    }
    #[cfg(feature = "local-files")]
    if let Some(player) = backends.local {
      player.stop();
    }
    #[cfg(feature = "subsonic")]
    if let Some(player) = backends.subsonic {
      player.stop();
    }
    #[cfg(feature = "youtube")]
    if let Some(player) = backends.youtube {
      player.stop();
    }
  }
}

/// Reuse the live radio player, or open a fresh output device for one. A
/// freshly opened player is **not** published to `App` here — the caller
/// publishes the session only on a successful first play.
async fn acquire_player(spine: &Spine) -> Option<Arc<LocalPlayer>> {
  match tokio::task::spawn_blocking(LocalPlayer::new).await {
    Ok(Ok(p)) => Some(Arc::new(p)),
    Ok(Err(e)) => {
      status(spine, format!("No audio output for radio playback: {e}"), 6).await;
      None
    }
    Err(e) => {
      status(spine, format!("Audio output init failed: {e}"), 6).await;
      None
    }
  }
}

/// Begin playing the station at `uri`, taking over the session.
async fn start_radio(spine: &Spine, uri: &str) {
  let url = match stream_url_from_uri(uri) {
    Ok(u) => u.to_string(),
    Err(_) => {
      status(spine, "Invalid radio URI", 6).await;
      return;
    }
  };
  let mut station = match decoded(spine, Query::RadioStationsSnapshot(uri.to_string())).await {
    Some(Answer::RadioStationsSnapshot(station)) => station,
    _ => return,
  };

  // Only one backend owns the device at a time.
  let existing_player = player(spine).await;
  release_other_backends(spine, existing_player.is_none()).await;

  let Some(player) = (match existing_player {
    Some(player) => Some(player),
    None => acquire_player(spine).await,
  }) else {
    return;
  };

  // Connect + prefetch off the lock; this can take seconds on a slow network.
  let opened = match open_radio_stream(&url).await {
    Ok(o) => o,
    Err(e) => {
      status(spine, format!("Cannot open radio stream: {e}"), 6).await;
      return;
    }
  };

  // Count the click for directory-sourced stations (usage-policy nicety).
  // Fire-and-forget: it must never delay or fail playback.
  if let Some(uuid) = station.id.clone() {
    tokio::spawn(async move { RadioSource::new().click(&uuid).await });
  }

  // A synthetic row (raw URL as name) gets upgraded to the station's
  // self-reported icy-name.
  if station.name == url {
    if let Some(icy_name) = &opened.station_name {
      station.name = icy_name.clone();
      station.uri = Some(uri_for_stream_url(&url));
    }
  }

  let now_playing = Arc::clone(&opened.now_playing);
  let (reader, mime) = (opened.reader, opened.content_type);
  let decode_player = Arc::clone(&player);
  let result =
    tokio::task::spawn_blocking(move || decode_player.play_stream(reader, mime.as_deref())).await;

  match result {
    Ok(Ok(())) => {
      if let Some(Answer::Volume(volume)) = decoded(spine, Query::Volume).await {
        player.set_volume(volume as f32 / 100.0);
      }

      let display = station.name.clone();
      // Publish the session exactly once, now that the stream is decoding.
      let state = RadioPlaybackState {
        player,
        station,
        now_playing,
      };
      emit(spine, Fact::RadioPlayback(state)).await;
      status(spine, format!("\u{266a} {display} (live)"), 4).await;
    }
    Ok(Err(e)) => status(spine, format!("Cannot play radio stream: {e}"), 6).await,
    Err(e) => status(spine, format!("Radio playback task failed: {e}"), 6).await,
  }
}

/// End the radio session, releasing the output device. Stopping the sink drops
/// the decoder, which drops the stream reader, which cancels the background
/// download task (`cancel_on_drop`).
pub async fn teardown_radio(spine: &Spine) {
  emit(spine, Fact::RadioPlaybackCleared).await;
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::core::app::App;
  use crate::core::user_config::UserConfig;
  use std::sync::mpsc::channel;
  use std::time::SystemTime;
  use tokio::sync::Mutex;

  fn test_app() -> App {
    let (tx, _rx) = channel();
    App::new(tx, UserConfig::new(), Some(SystemTime::now()))
  }

  fn station_row(uri: &str, name: &str) -> TrackInfo {
    TrackInfo {
      uri: Some(uri.to_string()),
      name: name.to_string(),
      artists: vec![],
      album: String::new(),
      duration_ms: 0,
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
  fn snapshot_prefers_sidebar_station_list() {
    let mut app = test_app();
    let uri = "radio:https://ice1.somafm.com/groovesalad-128-mp3";
    app.radio_stations = vec![station_row(uri, "Groove Salad")];
    app.track_table.tracks = vec![station_row(uri, "Wrong Name")];
    assert_eq!(snapshot_station(&app, uri).name, "Groove Salad");
  }

  #[test]
  fn snapshot_falls_back_to_search_results() {
    let mut app = test_app();
    let uri = "radio:https://example.com/stream";
    app.search_results.tracks = Some(Paged {
      items: vec![station_row(uri, "Searched FM")],
      total: 1,
      ..Default::default()
    });
    assert_eq!(snapshot_station(&app, uri).name, "Searched FM");
  }

  #[test]
  fn snapshot_synthesizes_a_row_for_unknown_uris() {
    let app = test_app();
    let station = snapshot_station(&app, "radio:https://example.com/live");
    assert_eq!(station.name, "https://example.com/live");
    assert_eq!(
      station.uri.as_deref(),
      Some("radio:https://example.com/live")
    );
    assert_eq!(station.duration_ms, 0);
  }

  /// Browse/search/transport events that are not radio's must fall through.
  #[tokio::test]
  async fn foreign_events_fall_through_without_a_session() {
    let app = Arc::new(Mutex::new(test_app()));
    let (spine, _rx) = crate::runtime::frontend::spawn_fold(&app).await;
    // No radio session: transport events are not ours.
    assert!(!route_radio_event(&spine, &IoEvent::PausePlayback).await);
    assert!(!route_radio_event(&spine, &IoEvent::NextTrack).await);
    assert!(!route_radio_event(&spine, &IoEvent::Seek(1000)).await);
    assert!(
      !route_radio_event(
        &spine,
        &IoEvent::Repeat(rspotify::model::enums::RepeatState::Off)
      )
      .await
    );
    assert!(!route_radio_event(&spine, &IoEvent::StartPlayback(None, None, None)).await);
    // A Spotify start falls through (and there is nothing to tear down).
    assert!(
      !route_radio_event(
        &spine,
        &IoEvent::StartPlayback(Some("spotify:track:x".to_string()), None, None)
      )
      .await
    );
  }

  /// Loading the station list is consumed and merges config + saved stations.
  #[tokio::test]
  async fn get_radio_stations_merges_configured_and_saved_lists() {
    let mut seeded = test_app();
    seeded.user_config.behavior.radio_stations = vec![crate::core::state::RadioStationConfig {
      name: "Configured Groove".to_string(),
      url: "https://ice1.somafm.com/groovesalad-128-mp3".to_string(),
    }];
    seeded.runtime_state.radio_stations = vec![
      crate::core::state::RadioStationConfig {
        name: "Groove Salad".to_string(),
        url: "https://ice1.somafm.com/groovesalad-128-mp3".to_string(),
      },
      crate::core::state::RadioStationConfig {
        name: "Secret Agent".to_string(),
        url: "https://ice1.somafm.com/secretagent-128-mp3".to_string(),
      },
    ];
    let app = Arc::new(Mutex::new(seeded));
    let (spine, _rx) = crate::runtime::frontend::spawn_fold(&app).await;
    assert!(route_radio_event(&spine, &IoEvent::GetRadioStations).await);
    let guard = app.lock().await;
    assert_eq!(guard.radio_stations.len(), 2);
    assert_eq!(guard.radio_stations[0].name, "Configured Groove");
    assert_eq!(
      guard.radio_stations[0].uri.as_deref(),
      Some("radio:https://ice1.somafm.com/groovesalad-128-mp3")
    );
    assert_eq!(guard.radio_stations[1].name, "Secret Agent");
  }

  /// End-to-end dispatch test: drive `route_radio_event` exactly as the runtime
  /// pump does — load stations, start one, exercise the live-stream no-op
  /// transport semantics, and tear down on a foreign start. Ignored (needs
  /// network **and** an audio output device); run:
  /// `cargo test --features internet-radio -- --ignored live_dispatch`
  #[tokio::test(flavor = "multi_thread")]
  #[ignore = "hits the live SomaFM stream AND requires an audio output device"]
  async fn live_dispatch_play_and_teardown() {
    let mut seeded = test_app();
    seeded.runtime_state.radio_stations = vec![crate::core::state::RadioStationConfig {
      name: "Groove Salad".to_string(),
      url: "https://ice1.somafm.com/groovesalad-128-mp3".to_string(),
    }];
    let app = Arc::new(Mutex::new(seeded));
    let (spine, _rx) = crate::runtime::frontend::spawn_fold(&app).await;
    assert!(route_radio_event(&spine, &IoEvent::GetRadioStations).await);
    let uri = app.lock().await.radio_stations[0].uri.clone().unwrap();

    // Start the station.
    assert!(
      route_radio_event(&spine, &IoEvent::StartPlayback(Some(uri), None, None)).await,
      "radio StartPlayback must be consumed"
    );
    {
      let guard = app.lock().await;
      let s = guard.radio_playback.as_ref().expect("session published");
      assert_eq!(s.station.name, "Groove Salad");
      assert!(!s.player.is_paused(), "should be playing");
    }

    // Live-stream semantics: Seek and Next/Prev are consumed as no-ops.
    assert!(route_radio_event(&spine, &IoEvent::Seek(30_000)).await);
    assert!(
      route_radio_event(
        &spine,
        &IoEvent::Repeat(rspotify::model::enums::RepeatState::Off)
      )
      .await
    );
    assert!(route_radio_event(&spine, &IoEvent::NextTrack).await);
    assert!(
      app.lock().await.radio_playback.is_some(),
      "no-op transport must not tear the session down"
    );

    // Pause/resume are ours while the session lives.
    assert!(route_radio_event(&spine, &IoEvent::PausePlayback).await);
    assert!(app
      .lock()
      .await
      .radio_playback
      .as_ref()
      .is_some_and(|s| s.player.is_paused()));
    assert!(route_radio_event(&spine, &IoEvent::StartPlayback(None, None, None)).await);

    // A foreign (Spotify) start tears the session down and falls through.
    assert!(
      !route_radio_event(
        &spine,
        &IoEvent::StartPlayback(Some("spotify:track:x".to_string()), None, None)
      )
      .await,
      "a non-radio start must fall through to the network"
    );
    assert!(
      app.lock().await.radio_playback.is_none(),
      "a foreign start must tear down the radio session (device handoff)"
    );
  }
}
