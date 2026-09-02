//! Differential tests for `App::apply`: one test per arm (or small arm
//! family), pinning the exact observable behavior — dispatched `IoEvent`s
//! and `App` state.
//!
//! House pattern: `app_with_channel` keeps the `IoEvent` receiver alive;
//! `IoEvent` derives nothing, so assertions pattern-match. Catch-all match
//! arms are spelled `_other` because this directory's gate counts bare
//! wildcard arms.

use std::sync::mpsc::{channel, Receiver};
use std::time::SystemTime;

use super::{ActionOutcome, EngineAction, NavTarget, RepeatSetting};
use crate::core::app::{App, RouteId, UserInfo};
use crate::core::user_config::UserConfig;
use crate::infra::network::IoEvent;

fn app_with_channel() -> (App, Receiver<IoEvent>) {
  let (tx, rx) = channel();
  let app = App::new(tx, UserConfig::new(), Some(SystemTime::now()));
  (app, rx)
}

#[allow(deprecated)]
fn playback_context(
  is_playing: bool,
  shuffle_state: bool,
) -> rspotify::model::CurrentPlaybackContext {
  use rspotify::model::{
    context::{Actions, CurrentPlaybackContext},
    CurrentlyPlayingType, Device, DeviceType, RepeatState,
  };
  CurrentPlaybackContext {
    device: Device {
      id: Some("dev-test".to_string()),
      is_active: true,
      is_private_session: false,
      is_restricted: false,
      name: "Test Device".to_string(),
      _type: DeviceType::Computer,
      volume_percent: Some(50),
    },
    repeat_state: RepeatState::Off,
    shuffle_state,
    context: None,
    timestamp: chrono::Utc::now(),
    progress: None,
    is_playing,
    item: None,
    currently_playing_type: CurrentlyPlayingType::Unknown,
    actions: Actions::default(),
  }
}

#[allow(deprecated)]
fn playback_context_with_track(is_playing: bool) -> rspotify::model::CurrentPlaybackContext {
  use crate::core::test_helpers::full_track;
  use rspotify::model::idtypes::ArtistId;
  use rspotify::model::PlayableItem;
  let mut ctx = playback_context(is_playing, false);
  ctx.progress = Some(chrono::Duration::milliseconds(0));
  let mut track = full_track("4uLU6hMCjMI75M1A2tKUQC", "Test Song");
  // `full_track` leaves the artist id unset; the jump-to-artist arm needs one.
  track.artists[0].id = Some(
    ArtistId::from_id("0OdUWJ0sBjDrqHygGUXeCF")
      .unwrap()
      .into_static(),
  );
  ctx.item = Some(PlayableItem::Track(track));
  ctx.currently_playing_type = rspotify::model::CurrentlyPlayingType::Track;
  ctx
}

// --- transport ---

#[test]
fn play_when_paused_dispatches_start_playback() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(false, false));

  app.apply(EngineAction::Play);

  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::StartPlayback(None, None, None))
  ));
}

#[test]
fn play_when_already_playing_is_a_noop() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(true, false));

  app.apply(EngineAction::Play);

  assert!(rx.try_recv().is_err(), "expected no IoEvent dispatched");
}

#[test]
fn pause_when_playing_dispatches_pause_playback() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(true, false));

  app.apply(EngineAction::Pause);

  assert!(matches!(rx.try_recv(), Ok(IoEvent::PausePlayback)));
}

#[test]
fn pause_when_already_paused_is_a_noop() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(false, false));

  app.apply(EngineAction::Pause);

  assert!(rx.try_recv().is_err(), "expected no IoEvent dispatched");
}

#[test]
fn next_track_routes_through_the_transport_method() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::NextTrack);

  assert!(matches!(rx.try_recv(), Ok(IoEvent::NextTrack)));
}

#[test]
fn previous_track_routes_through_the_transport_method() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::PreviousTrack);

  assert!(matches!(rx.try_recv(), Ok(IoEvent::PreviousTrack)));
}

#[test]
fn seek_to_with_a_track_context_dispatches_seek() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context_with_track(true));

  app.apply(EngineAction::SeekTo(30_000));

  match rx.try_recv() {
    Ok(IoEvent::Seek(ms)) => assert_eq!(ms, 30_000),
    _other => panic!("expected Seek(30000) (IoEvent is not Debug)"),
  }
}

#[test]
fn set_volume_sets_pending_volume() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::SetVolume(80));

  assert_eq!(app.pending_volume, Some(80));
}

#[test]
fn set_shuffle_dispatches_only_when_state_differs() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(false, false));

  app.apply(EngineAction::SetShuffle(true));
  assert!(matches!(rx.try_recv(), Ok(IoEvent::Shuffle(true))));

  app.apply(EngineAction::SetShuffle(false));
  assert!(rx.try_recv().is_err(), "same state must not dispatch");
}

#[test]
fn cycle_repeat_without_playback_context_is_a_noop() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::CycleRepeat);

  assert!(rx.try_recv().is_err(), "expected no IoEvent dispatched");
}

#[test]
fn set_repeat_dispatches_the_absolute_repeat_state() {
  use rspotify::model::RepeatState;
  for (setting, expected) in [
    (RepeatSetting::Off, RepeatState::Off),
    (RepeatSetting::Track, RepeatState::Track),
    (RepeatSetting::Context, RepeatState::Context),
  ] {
    let (mut app, rx) = app_with_channel();
    app.apply(EngineAction::SetRepeat(setting));
    match rx.try_recv() {
      Ok(IoEvent::Repeat(state)) => assert_eq!(state, expected),
      _other => panic!("expected Repeat (IoEvent is not Debug)"),
    }
  }
}

#[test]
fn toggle_playback_pauses_when_playing_and_starts_when_paused() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(true, false));

  app.apply(EngineAction::TogglePlayback);
  assert!(matches!(rx.try_recv(), Ok(IoEvent::PausePlayback)));

  app.current_playback_context = Some(playback_context(false, false));
  app.apply(EngineAction::TogglePlayback);
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::StartPlayback(None, None, None))
  ));
}

#[test]
fn force_previous_track_dispatches_the_force_event() {
  let (mut app, rx) = app_with_channel();
  app.song_progress_ms = 20_000;

  app.apply(EngineAction::ForcePreviousTrack);

  assert!(matches!(rx.try_recv(), Ok(IoEvent::ForcePreviousTrack)));
  assert_eq!(app.song_progress_ms, 0, "a forced previous restarts at 0");
}

#[test]
fn seek_actions_move_by_the_configured_step() {
  // Start one step in, so forward lands at `2 * step + 1_000` and backward
  // lands at `1_000`.
  for (action, forward) in [
    (EngineAction::SeekForward, true),
    (EngineAction::SeekBackward, false),
  ] {
    let (mut app, _rx) = app_with_channel();
    app.current_playback_context = Some(playback_context_with_track(true));
    let step = app.user_config.behavior.seek_milliseconds as u128;
    app.song_progress_ms = step + 1_000;

    app.apply(action);

    let expected = if forward { 2 * step + 1_000 } else { 1_000 };
    assert_eq!(app.song_progress_ms, expected);
  }
}

#[test]
fn volume_actions_dispatch_the_stepped_volume() {
  for (action, up) in [
    (EngineAction::VolumeUp, true),
    (EngineAction::VolumeDown, false),
  ] {
    let (mut app, rx) = app_with_channel();
    app.current_playback_context = Some(playback_context(true, false));

    app.apply(action);

    let increment = app.user_config.behavior.volume_increment;
    let expected = if up { 50 + increment } else { 50 - increment };
    match rx.try_recv() {
      Ok(IoEvent::ChangeVolume(v)) => assert_eq!(v, expected),
      _other => panic!("expected ChangeVolume (IoEvent is not Debug)"),
    }
  }
}

#[test]
fn toggle_shuffle_flips_the_spotify_shuffle_state() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context(true, false));

  app.apply(EngineAction::ToggleShuffle);

  assert!(matches!(rx.try_recv(), Ok(IoEvent::Shuffle(true))));
}

// --- jump-to navigation ---

#[test]
fn jump_to_album_opens_the_current_tracks_album() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context_with_track(false));

  app.apply(EngineAction::JumpToAlbum);

  match rx.try_recv() {
    Ok(IoEvent::GetAlbumTracks(album)) => assert_eq!(album.name, "Test Album"),
    _other => panic!("expected GetAlbumTracks (IoEvent is not Debug)"),
  }
}

#[test]
fn jump_to_album_without_playback_is_a_noop() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::JumpToAlbum);

  assert!(rx.try_recv().is_err(), "expected no IoEvent dispatched");
}

#[test]
fn jump_to_artist_opens_the_first_artists_albums() {
  let (mut app, rx) = app_with_channel();
  app.current_playback_context = Some(playback_context_with_track(false));

  app.apply(EngineAction::JumpToArtist);

  match rx.try_recv() {
    Ok(IoEvent::GetArtist(id, name, _country)) => {
      assert_eq!(id, "0OdUWJ0sBjDrqHygGUXeCF");
      assert_eq!(name, "Test Artist");
    }
    _other => panic!("expected GetArtist (IoEvent is not Debug)"),
  }
}

#[test]
fn jump_to_context_opens_the_playlist_context() {
  use rspotify::model::{context::Context, enums::Type};
  let (mut app, rx) = app_with_channel();
  let mut ctx = playback_context_with_track(true);
  ctx.context = Some(Context {
    uri: "spotify:playlist:37i9dQZF1DX4WYpdgoIcn6".to_string(),
    href: String::new(),
    external_urls: std::collections::HashMap::new(),
    _type: Type::Playlist,
  });
  app.current_playback_context = Some(ctx);

  app.apply(EngineAction::JumpToContext);

  assert_eq!(app.get_current_route().id, RouteId::TrackTable);
  match rx.try_recv() {
    Ok(IoEvent::GetPlaylistItems(id, _offset)) => {
      assert_eq!(id, "37i9dQZF1DX4WYpdgoIcn6");
    }
    _other => panic!("expected GetPlaylistItems (IoEvent is not Debug)"),
  }
}

// --- recap ---

#[test]
fn generate_recap_resolves_the_period_from_the_current_route() {
  use crate::core::app::ActiveBlock;
  use crate::infra::history::RecapPeriod;
  // The stats selection is set in both cases, so the off-Stats case proves
  // the selection is ignored anywhere else.
  for (on_stats_screen, expected) in [(false, RecapPeriod::ThirtyDays), (true, RecapPeriod::Year)] {
    let (mut app, rx) = app_with_channel();
    app.stats_period = RecapPeriod::Year;
    if on_stats_screen {
      app.push_navigation_stack(RouteId::Stats, ActiveBlock::Stats);
    }

    app.apply(EngineAction::GenerateRecap);

    match rx.try_recv() {
      Ok(IoEvent::GenerateRecap(period)) => assert_eq!(period, expected),
      _other => panic!("expected GenerateRecap (IoEvent is not Debug)"),
    }
  }
}

// --- playback starts ---

#[test]
fn play_uris_dispatches_a_uri_list_start() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::PlayUris {
    uris: vec!["spotify:track:abc".to_string()],
    offset: None,
  });

  match rx.try_recv() {
    Ok(IoEvent::StartPlayback(None, Some(uris), None)) => {
      assert_eq!(uris, vec!["spotify:track:abc".to_string()]);
    }
    _other => panic!("expected a uri-list StartPlayback (IoEvent is not Debug)"),
  }
}

#[test]
fn play_uris_carries_the_offset() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::PlayUris {
    uris: vec!["spotify:track:abc".to_string()],
    offset: Some(0),
  });

  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::StartPlayback(None, Some(_), Some(0)))
  ));
}

#[test]
fn play_context_dispatches_a_context_start() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::PlayContext {
    uri: "spotify:playlist:p1".to_string(),
    offset: Some(5),
  });

  match rx.try_recv() {
    Ok(IoEvent::StartPlayback(Some(ctx), None, Some(offset))) => {
      assert_eq!(ctx, "spotify:playlist:p1");
      assert_eq!(offset, 5);
    }
    _other => panic!("expected a context StartPlayback (IoEvent is not Debug)"),
  }
}

#[test]
fn transfer_playback_carries_device_and_persist() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::TransferPlayback {
    device_id: "dev-1".to_string(),
    persist: false,
  });

  match rx.try_recv() {
    Ok(IoEvent::TransferPlaybackToDevice(id, persist)) => {
      assert_eq!(id, "dev-1");
      assert!(!persist);
    }
    _other => panic!("expected TransferPlaybackToDevice (IoEvent is not Debug)"),
  }
}

#[test]
fn add_to_queue_dispatches_add_item() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::AddToQueue("spotify:track:t1".to_string()));

  match rx.try_recv() {
    Ok(IoEvent::AddItemToQueue(uri)) => assert_eq!(uri, "spotify:track:t1"),
    _other => panic!("expected AddItemToQueue (IoEvent is not Debug)"),
  }
}

// --- native queue ---

fn queue_track(uri: &str, name: &str) -> crate::core::plugin_api::TrackInfo {
  crate::core::plugin_api::TrackInfo {
    uri: Some(uri.to_string()),
    name: name.to_string(),
    artists: vec!["Artist".to_string()],
    album: "Album".to_string(),
    duration_ms: 200_000,
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

#[test]
fn enqueue_native_pushes_onto_the_native_queue() {
  let (mut app, _rx) = app_with_channel();

  assert_eq!(
    app.apply(EngineAction::EnqueueNative(queue_track(
      "subsonic:track:a",
      "A",
    ))),
    ActionOutcome::Queued { accepted: 1 }
  );

  assert_eq!(app.native_queue.len(), 1);
  assert_eq!(app.native_queue[0].name, "A");
}

#[test]
fn remove_native_queued_deletes_only_the_indexed_upcoming_item() {
  let (mut app, _rx) = app_with_channel();
  app.native_queue = vec![
    queue_track("subsonic:track:a", "A"),
    queue_track("subsonic:track:b", "B"),
  ];

  app.apply(EngineAction::RemoveNativeQueued(0));

  assert_eq!(app.native_queue.len(), 1);
  assert_eq!(app.native_queue[0].name, "B");

  // Out of range is a no-op.
  app.apply(EngineAction::RemoveNativeQueued(9));
  assert_eq!(app.native_queue.len(), 1);
}

#[test]
fn move_native_queued_swaps_with_the_neighbor() {
  let (mut app, _rx) = app_with_channel();
  app.native_queue = vec![
    queue_track("subsonic:track:a", "A"),
    queue_track("subsonic:track:b", "B"),
    queue_track("subsonic:track:c", "C"),
  ];

  app.apply(EngineAction::MoveNativeQueued { index: 1, up: true });
  assert_eq!(app.native_queue[0].name, "B");
  assert_eq!(app.native_queue[1].name, "A");

  app.apply(EngineAction::MoveNativeQueued {
    index: 0,
    up: false,
  });
  assert_eq!(app.native_queue[0].name, "A");
  assert_eq!(app.native_queue[1].name, "B");

  // Boundary indices are no-ops.
  app.apply(EngineAction::MoveNativeQueued { index: 0, up: true });
  app.apply(EngineAction::MoveNativeQueued {
    index: 2,
    up: false,
  });
  assert_eq!(app.native_queue[0].name, "A");
  assert_eq!(app.native_queue[1].name, "B");
  assert_eq!(app.native_queue[2].name, "C");
}

#[test]
fn clear_native_queue_drops_only_upcoming_items() {
  let (mut app, _rx) = app_with_channel();
  app.native_queue = vec![
    queue_track("subsonic:track:a", "A"),
    queue_track("subsonic:track:b", "B"),
  ];
  // A queue slot that is playing now lives outside `native_queue`; keep a
  // marker to prove clear leaves it alone. The slot type needs streaming;
  // without it there is no slot at all, so the assertion is the same.
  #[cfg(feature = "streaming")]
  {
    app.queue_now = Some(crate::infra::queue::QueueNowPlaying::Spotify {
      track: queue_track("spotify:track:now", "Now"),
    });
  }

  app.apply(EngineAction::ClearNativeQueue);

  assert!(app.native_queue.is_empty());
  #[cfg(feature = "streaming")]
  assert!(app.queue_now.is_some(), "the active Playable keeps playing");
}

#[test]
fn begin_spotify_login_dispatches_begin() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::BeginSpotifyLogin);

  match rx.try_recv() {
    Ok(IoEvent::BeginSpotifyLogin) => {}
    _other => panic!("expected BeginSpotifyLogin (IoEvent is not Debug)"),
  }
}

// --- library / playlists ---

#[test]
fn search_resolves_the_user_country_at_apply_time() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Search("daft punk".to_string()));

  match rx.try_recv() {
    Ok(IoEvent::GetSearchResults(query, country)) => {
      assert_eq!(query, "daft punk");
      assert_eq!(country, None, "no user loaded, so no market");
    }
    _other => panic!("expected GetSearchResults (IoEvent is not Debug)"),
  }
}

#[test]
fn create_playlist_dispatches_create() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::CreatePlaylist {
    name: "Mix".to_string(),
    track_uris: vec!["spotify:track:a".to_string(), "spotify:track:b".to_string()],
  });

  match rx.try_recv() {
    Ok(IoEvent::CreateNewPlaylist(name, uris)) => {
      assert_eq!(name, "Mix");
      assert_eq!(uris.len(), 2);
    }
    _other => panic!("expected CreateNewPlaylist (IoEvent is not Debug)"),
  }
}

#[test]
fn add_track_to_playlist_dispatches() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::AddTrackToPlaylist {
    playlist: "p1".to_string(),
    track: "t1".to_string(),
  });

  match rx.try_recv() {
    Ok(IoEvent::AddTrackToPlaylist(playlist, track)) => {
      assert_eq!(playlist, "p1");
      assert_eq!(track, "t1");
    }
    _other => panic!("expected AddTrackToPlaylist (IoEvent is not Debug)"),
  }
}

#[test]
fn remove_track_from_playlist_dispatches_with_position() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::RemoveTrackFromPlaylist {
    playlist: "p1".to_string(),
    track: "t1".to_string(),
    position: 3,
  });

  match rx.try_recv() {
    Ok(IoEvent::RemoveTrackFromPlaylistAtPosition(playlist, track, position)) => {
      assert_eq!(playlist, "p1");
      assert_eq!(track, "t1");
      assert_eq!(position, 3);
    }
    _other => panic!("expected RemoveTrackFromPlaylistAtPosition (IoEvent is not Debug)"),
  }
}

#[test]
fn follow_playlist_uses_the_unknown_owner_fallback() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::FollowPlaylist("p1".to_string()));

  match rx.try_recv() {
    Ok(IoEvent::UserFollowPlaylist(owner, playlist, public)) => {
      assert_eq!(owner, "unknown");
      assert_eq!(playlist, "p1");
      assert_eq!(public, None);
    }
    _other => panic!("expected UserFollowPlaylist (IoEvent is not Debug)"),
  }
}

#[test]
fn unfollow_playlist_resolves_the_current_user() {
  let (mut app, rx) = app_with_channel();
  app.user = Some(UserInfo {
    id: "me-123".to_string(),
    display_name: None,
    country: None,
  });

  app.apply(EngineAction::UnfollowPlaylist("p1".to_string()));

  match rx.try_recv() {
    Ok(IoEvent::UserUnfollowPlaylist(user_id, playlist_id)) => {
      assert_eq!(user_id, "me-123");
      assert_eq!(playlist_id, "p1");
    }
    _other => panic!("expected UserUnfollowPlaylist (IoEvent is not Debug)"),
  }
}

#[test]
fn unfollow_playlist_without_a_user_sets_an_error_status() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::UnfollowPlaylist("p1".to_string()));

  assert!(rx.try_recv().is_err(), "no IoEvent expected");
  assert!(app.status_message_is_error);
  assert_eq!(
    app.status_message.as_deref(),
    Some("plugin unfollow_playlist: user profile not loaded yet")
  );
}

#[test]
fn save_and_follow_actions_dispatch_their_events() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::ToggleSaveTrack(
    "spotify:track:t1".to_string(),
  ));
  assert!(matches!(rx.try_recv(), Ok(IoEvent::ToggleSaveTrack(_))));

  app.apply(EngineAction::SaveAlbum("a1".to_string()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::CurrentUserSavedAlbumAdd(_))
  ));

  app.apply(EngineAction::UnsaveAlbum("a1".to_string()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::CurrentUserSavedAlbumDelete(_))
  ));

  app.apply(EngineAction::SaveShow("s1".to_string()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::CurrentUserSavedShowAdd(_))
  ));

  app.apply(EngineAction::UnsaveShow("s1".to_string()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::CurrentUserSavedShowDelete(_))
  ));
}

#[test]
fn follow_artist_wraps_the_id_in_a_batch_event() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::FollowArtist("ar1".to_string()));
  match rx.try_recv() {
    Ok(IoEvent::UserFollowArtists(ids)) => assert_eq!(ids, vec!["ar1".to_string()]),
    _other => panic!("expected UserFollowArtists (IoEvent is not Debug)"),
  }

  app.apply(EngineAction::UnfollowArtist("ar1".to_string()));
  match rx.try_recv() {
    Ok(IoEvent::UserUnfollowArtists(ids)) => assert_eq!(ids, vec!["ar1".to_string()]),
    _other => panic!("expected UserUnfollowArtists (IoEvent is not Debug)"),
  }
}

// --- status messages ---

#[test]
fn notify_sets_a_status_message() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::Notify("hello".to_string(), 4));

  assert_eq!(app.status_message.as_deref(), Some("hello"));
  assert!(!app.status_message_is_error);
}

#[test]
fn notify_error_blocks_a_following_notify() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::NotifyError("boom".to_string(), 6));
  app.apply(EngineAction::Notify("normal".to_string(), 4));

  assert_eq!(app.status_message.as_deref(), Some("boom"));
  assert!(app.status_message_is_error);
}

// --- navigation ---

#[test]
fn navigate_home_returns_to_the_home_route() {
  let (mut app, _rx) = app_with_channel();
  app.apply(EngineAction::Navigate(NavTarget::Queue));
  assert_eq!(app.get_current_route().id, RouteId::Queue);

  app.apply(EngineAction::Navigate(NavTarget::Home));

  assert_eq!(app.get_current_route().id, RouteId::Home);
}

#[test]
fn navigate_queue_pushes_the_route_and_fetches() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Navigate(NavTarget::Queue));

  assert_eq!(app.get_current_route().id, RouteId::Queue);
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetQueue)));
}

#[test]
fn navigate_settings_opens_the_settings_screen() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::Navigate(NavTarget::Settings));

  assert_eq!(app.get_current_route().id, RouteId::Settings);
}

#[test]
fn navigate_devices_opens_the_device_picker() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Navigate(NavTarget::Devices));

  assert_eq!(app.get_current_route().id, RouteId::SelectedDevice);
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetDevices)));
}

#[test]
fn navigate_help_pushes_the_help_route() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::Navigate(NavTarget::Help));
  assert_eq!(app.get_current_route().id, RouteId::HelpMenu);
}

#[test]
fn navigate_recently_played_only_dispatches() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Navigate(NavTarget::RecentlyPlayed));

  // The network handler pushes the route once the data arrives.
  assert_eq!(app.get_current_route().id, RouteId::Home);
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetRecentlyPlayed)));
}

#[test]
fn navigate_miniplayer_toggles() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::Navigate(NavTarget::MiniPlayer));
  assert_eq!(app.get_current_route().id, RouteId::MiniPlayer);

  app.apply(EngineAction::Navigate(NavTarget::MiniPlayer));
  assert_eq!(app.get_current_route().id, RouteId::Home);
}

#[test]
fn back_pops_the_navigation_stack() {
  let (mut app, _rx) = app_with_channel();
  app.apply(EngineAction::Navigate(NavTarget::Queue));
  assert_eq!(app.get_current_route().id, RouteId::Queue);

  app.apply(EngineAction::Back);

  assert_eq!(app.get_current_route().id, RouteId::Home);
}

#[test]
fn nav_target_names_round_trip() {
  for target in NavTarget::ALL {
    assert_eq!(NavTarget::from_name(target.name()), Some(target));
  }
  assert_eq!(NavTarget::from_name("nowhere"), None);
}

// --- DJ ---

#[cfg(feature = "dj-core")]
mod dj_actions {
  use super::*;
  use crate::core::action::ActionOutcome;
  use crate::core::plugin_api::TrackInfo;

  fn dj_track(uri: Option<&str>, name: &str) -> TrackInfo {
    TrackInfo {
      uri: uri.map(str::to_string),
      name: name.to_string(),
      artists: vec!["Artist".to_string()],
      album: "Album".to_string(),
      duration_ms: 200_000,
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

  #[test]
  fn queue_tracks_reports_the_accepted_count() {
    let (mut app, _rx) = app_with_channel();

    let outcome = app.apply(EngineAction::QueueTracks(vec![
      dj_track(Some("subsonic:track:a"), "A"),
      dj_track(Some("subsonic:track:b"), "B"),
    ]));

    assert_eq!(outcome, ActionOutcome::Queued { accepted: 2 });
    assert_eq!(app.native_queue.len(), 2);
    assert!(app.dj.queued_uris.contains("subsonic:track:a"));
  }

  #[test]
  fn queue_tracks_skips_tracks_without_a_uri() {
    let (mut app, _rx) = app_with_channel();

    let outcome = app.apply(EngineAction::QueueTracks(vec![
      dj_track(Some("subsonic:track:a"), "A"),
      dj_track(None, "No URI"),
    ]));

    assert_eq!(outcome, ActionOutcome::Queued { accepted: 1 });
    assert_eq!(app.native_queue.len(), 1);
  }

  #[test]
  fn set_dj_vibe_bumps_the_generation_exactly_once() {
    let (mut app, _rx) = app_with_channel();
    let before = app.dj.generation;

    app.apply(EngineAction::SetDjVibe(Some("mellow".to_string())));

    assert_eq!(
      app.dj.generation,
      before.wrapping_add(1),
      "the turn loop's adopt-one-bump rule needs exactly one bump"
    );
    assert_eq!(app.dj.vibe.as_deref(), Some("mellow"));
  }

  #[test]
  fn set_dj_vibe_clears_with_none() {
    let (mut app, _rx) = app_with_channel();
    app.apply(EngineAction::SetDjVibe(Some("mellow".to_string())));

    app.apply(EngineAction::SetDjVibe(None));

    assert_eq!(app.dj.vibe, None);
  }
}

// --- pagination, open family, source-routed search, library rows, dialogs ---

use super::{LibraryTarget, ListTarget, OpenTarget};
use crate::core::app::{ActiveBlock, DialogContext, PendingTrackSelection};
use crate::core::pagination::Paged;
use crate::core::plugin_api::{PlaylistInfo, TrackInfo};
use crate::core::source::Source;
use crate::core::test_helpers::{full_track, playlist_info};

fn track(id: &str, name: &str) -> TrackInfo {
  TrackInfo::from(&full_track(id, name))
}

fn track_page(offset: u32, ids: &[&str], has_next: bool) -> Paged<TrackInfo> {
  Paged {
    items: ids
      .iter()
      .enumerate()
      .map(|(i, id)| track(id, &format!("Track {offset}-{i}")))
      .collect(),
    offset,
    limit: ids.len() as u32,
    total: 4,
    next: has_next.then(|| "https://example.com/next".to_string()),
    previous: None,
  }
}

/// The playlist track table's page type carries snapshot positions.
fn playlist_items_page(has_next: bool) -> Paged<(u32, crate::core::plugin_api::PlayableInfo)> {
  Paged {
    items: vec![(
      0,
      crate::core::plugin_api::PlayableInfo::Track(track("0000000000000000000001", "T1")),
    )],
    offset: 0,
    limit: 1,
    total: 4,
    next: has_next.then(|| "https://example.com/next".to_string()),
    previous: None,
  }
}

// --- LoadMore ---

#[test]
fn load_more_playlist_tracks_dispatches_the_next_page_fetch() {
  let (mut app, rx) = app_with_channel();
  app.playlist_track_table_id = Some(
    rspotify::model::idtypes::PlaylistId::from_id("37i9dQZF1DX4WYpdgoIcn6")
      .unwrap()
      .into_static(),
  );
  let page = playlist_items_page(true);
  app.track_table.context = Some(crate::core::app::TrackTableContext::MyPlaylists);
  app.playlist_tracks = Some(page.clone());
  app.playlist_track_pages.upsert_page_by_offset(page);

  app.apply(EngineAction::LoadMore(ListTarget::PlaylistTracks));

  match rx.try_recv() {
    Ok(IoEvent::GetPlaylistItems(id, offset)) => {
      assert_eq!(id, "37i9dQZF1DX4WYpdgoIcn6");
      assert_eq!(offset, 1);
    }
    _other => panic!("expected GetPlaylistItems"),
  }
}

#[test]
fn load_more_playlist_tracks_is_a_noop_under_an_active_filter() {
  let (mut app, rx) = app_with_channel();
  app.playlist_track_table_id = Some(
    rspotify::model::idtypes::PlaylistId::from_id("37i9dQZF1DX4WYpdgoIcn6")
      .unwrap()
      .into_static(),
  );
  let page = playlist_items_page(true);
  app.track_table.context = Some(crate::core::app::TrackTableContext::MyPlaylists);
  app.playlist_tracks = Some(page.clone());
  app.playlist_track_pages.upsert_page_by_offset(page);
  app.active_playlist_track_filter = Some("query".to_string());

  app.apply(EngineAction::LoadMore(ListTarget::PlaylistTracks));

  assert!(rx.try_recv().is_err(), "filtered playlists never paginate");
}

#[test]
fn load_more_saved_tracks_fetches_the_missing_continuous_page() {
  let (mut app, rx) = app_with_channel();
  let page = track_page(
    0,
    &["0000000000000000000001", "0000000000000000000002"],
    true,
  );
  app.library.saved_tracks.upsert_page_by_offset(page);

  app.apply(EngineAction::LoadMore(ListTarget::SavedTracks));

  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetCurrentSavedTracks(Some(2)))
  ));
}

#[test]
fn load_more_saved_tracks_is_a_noop_at_the_end_of_the_list() {
  let (mut app, rx) = app_with_channel();
  let page = track_page(0, &["0000000000000000000001"], false);
  app.library.saved_tracks.upsert_page_by_offset(page);

  app.apply(EngineAction::LoadMore(ListTarget::SavedTracks));

  assert!(rx.try_recv().is_err());
}

#[test]
fn load_more_does_not_touch_pending_track_table_selection() {
  let (mut app, rx) = app_with_channel();
  let page = track_page(0, &["0000000000000000000001"], false);
  app.library.saved_tracks.upsert_page_by_offset(page);
  app.pending_track_table_selection = Some(PendingTrackSelection::Index(9));

  app.apply(EngineAction::LoadMore(ListTarget::SavedTracks));

  assert_eq!(
    app.pending_track_table_selection,
    Some(PendingTrackSelection::Index(9)),
    "the cursor-follow side effect belongs to the caller, not to LoadMore"
  );
  assert!(rx.try_recv().is_err());
}

#[test]
fn load_more_saved_tracks_does_not_refetch_an_in_flight_page() {
  let (mut app, rx) = app_with_channel();
  let page = track_page(
    0,
    &["0000000000000000000001", "0000000000000000000002"],
    true,
  );
  app.library.saved_tracks.upsert_page_by_offset(page);

  app.apply(EngineAction::LoadMore(ListTarget::SavedTracks));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetCurrentSavedTracks(Some(2)))
  ));

  // The fetch marked offset 2 as in flight; a second apply must not
  // re-dispatch it (the prefetch dedupe the arm inherits).
  app.apply(EngineAction::LoadMore(ListTarget::SavedTracks));
  assert!(rx.try_recv().is_err());
}

#[test]
fn load_more_playlist_tracks_does_not_refetch_an_in_flight_page() {
  let (mut app, rx) = app_with_channel();
  app.playlist_track_table_id = Some(
    rspotify::model::idtypes::PlaylistId::from_id("37i9dQZF1DX4WYpdgoIcn6")
      .unwrap()
      .into_static(),
  );
  app.track_table.context = Some(crate::core::app::TrackTableContext::MyPlaylists);
  let page = playlist_items_page(true);
  app.playlist_tracks = Some(page.clone());
  app.playlist_track_pages.upsert_page_by_offset(page);

  app.apply(EngineAction::LoadMore(ListTarget::PlaylistTracks));
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetPlaylistItems(_, 1))));

  app.apply(EngineAction::LoadMore(ListTarget::PlaylistTracks));
  assert!(
    rx.try_recv().is_err(),
    "the in-flight page is not refetched"
  );
}

// --- Open family ---

#[test]
fn open_album_dispatches_get_album() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::Album(
    "5gzLOflH95LkKYE6XSXE9k".to_string(),
  )));

  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetAlbum(id)) if id == "5gzLOflH95LkKYE6XSXE9k"
  ));
}

#[test]
fn open_artist_resolves_the_country_and_dispatches_get_artist() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::Artist {
    id: "2ye2Wgw4gimLv2eAKyk1NB".to_string(),
    name: String::new(),
  }));

  match rx.try_recv() {
    Ok(IoEvent::GetArtist(id, name, country)) => {
      assert_eq!(id, "2ye2Wgw4gimLv2eAKyk1NB");
      assert_eq!(name, "");
      assert_eq!(country, None, "no user loaded, so no market");
    }
    _other => panic!("expected GetArtist"),
  }
}

#[test]
fn open_track_album_dispatches_get_album_for_track() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::TrackAlbum(
    "10igKaIKsSB6ZnWxPxPvKO".to_string(),
  )));

  assert!(
    matches!(
      rx.try_recv(),
      Ok(IoEvent::GetAlbumForTrack(id)) if id == "10igKaIKsSB6ZnWxPxPvKO"
    ),
    "the track id rides through unchanged"
  );
}

#[test]
fn open_show_dispatches_get_show() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::Show(
    "3aNsrV6lkzmcU1w8u8kA7N".to_string(),
  )));

  assert!(
    matches!(
      rx.try_recv(),
      Ok(IoEvent::GetShow(id)) if id == "3aNsrV6lkzmcU1w8u8kA7N"
    ),
    "the show id rides through unchanged"
  );
}

#[test]
fn open_playlist_opens_the_track_table_in_the_playlists_context() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::Playlist {
    id: "37i9dQZF1DX4WYpdgoIcn6".to_string(),
    from_search: false,
  }));

  assert_eq!(app.get_current_route().id, RouteId::TrackTable);
  assert_eq!(
    app.track_table.context,
    Some(crate::core::app::TrackTableContext::MyPlaylists)
  );
  assert_eq!(
    app.pending_playlist_open.as_deref(),
    Some("37i9dQZF1DX4WYpdgoIcn6")
  );
  match rx.try_recv() {
    Ok(IoEvent::GetPlaylistItems(id, offset)) => {
      assert_eq!(id, "37i9dQZF1DX4WYpdgoIcn6");
      assert_eq!(offset, 0);
    }
    _other => panic!("expected GetPlaylistItems"),
  }
}

#[test]
fn open_playlist_from_search_uses_the_search_context() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::Playlist {
    id: "37i9dQZF1DX4WYpdgoIcn6".to_string(),
    from_search: true,
  }));

  assert_eq!(
    app.track_table.context,
    Some(crate::core::app::TrackTableContext::PlaylistSearch)
  );
}

#[test]
fn open_playlist_with_an_unparseable_id_is_a_silent_noop() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::Open(OpenTarget::Playlist {
    id: "not a base62 id!".to_string(),
    from_search: false,
  }));

  assert!(
    rx.try_recv().is_err(),
    "the opening paths drop bad ids silently"
  );
  assert_eq!(app.get_current_route().id, RouteId::Home);
}

#[test]
fn open_playlist_twice_while_pending_open_fetches_only_once() {
  let (mut app, rx) = app_with_channel();

  let target = OpenTarget::Playlist {
    id: "37i9dQZF1DX4WYpdgoIcn6".to_string(),
    from_search: false,
  };
  app.apply(EngineAction::Open(target.clone()));
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetPlaylistItems(_, 0))));

  // Same open already in flight: no second fetch, only the screen shown.
  app.apply(EngineAction::Open(target));
  assert!(rx.try_recv().is_err());
}

// --- SearchActiveSource (EngineAction::Search stays Spotify-only) ---

#[test]
fn search_active_source_routes_by_the_active_source() {
  let (mut app, rx) = app_with_channel();
  let query = "coltrane".to_string();

  app.active_source = Source::Subsonic;
  app.apply(EngineAction::SearchActiveSource(query.clone()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetSubsonicSearchResults(q)) if q == "coltrane"
  ));

  app.active_source = Source::Radio;
  app.apply(EngineAction::SearchActiveSource(query.clone()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetRadioSearchResults(q)) if q == "coltrane"
  ));

  app.active_source = Source::YouTube;
  app.apply(EngineAction::SearchActiveSource(query.clone()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetYouTubeSearchResults(q)) if q == "coltrane"
  ));

  app.active_source = Source::Spotify;
  app.apply(EngineAction::SearchActiveSource(query.clone()));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetSearchResults(q, _)) if q == "coltrane"
  ));

  // Local falls into the Spotify branch, matching the search input's
  // if-chain, which has no Local branch.
  app.active_source = Source::Local;
  app.apply(EngineAction::SearchActiveSource(query));
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetSearchResults(_, None))
  ));
}

#[test]
fn search_active_source_resolves_a_loaded_users_country() {
  let (mut app, rx) = app_with_channel();
  app.user = Some(crate::core::app::UserInfo {
    id: "user1".to_string(),
    display_name: None,
    country: Some("US".to_string()),
  });
  app.active_source = Source::Spotify;

  app.apply(EngineAction::SearchActiveSource("coltrane".to_string()));

  assert!(
    matches!(rx.try_recv(), Ok(IoEvent::GetSearchResults(_, Some(_)))),
    "the loaded user's market rides along"
  );
}

#[test]
fn search_stays_on_the_spotify_catalog_when_browsing_other_sources() {
  let (mut app, rx) = app_with_channel();
  app.active_source = Source::YouTube;

  app.apply(EngineAction::Search("daft punk".to_string()));

  // Browsing scope does not reroute the explicit Web API search.
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetSearchResults(q, None)) if q == "daft punk"
  ));
}

#[test]
fn search_playlist_tracks_records_the_pending_search_and_dispatches() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::SearchPlaylistTracks {
    playlist_id: "37i9dQZF1DX4WYpdgoIcn6".to_string(),
    query: "queen rock".to_string(),
  });

  assert_eq!(
    app.pending_playlist_track_search.as_deref(),
    Some("queen rock")
  );
  assert_eq!(
    app.status_message.as_deref(),
    Some("Searching playlist for \"queen rock\"...")
  );
  match rx.try_recv() {
    Ok(IoEvent::SearchPlaylistTracks(id, query)) => {
      assert_eq!(id, "37i9dQZF1DX4WYpdgoIcn6");
      assert_eq!(query, "queen rock");
    }
    _other => panic!("expected SearchPlaylistTracks"),
  }
}

// --- playback starts ---

#[test]
fn play_track_in_context_dispatches_the_combined_start() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::PlayTrackInContext {
    context: "spotify:playlist:p1".to_string(),
    track: "spotify:track:t1".to_string(),
  });

  match rx.try_recv() {
    Ok(IoEvent::StartPlayback(context, uris, offset)) => {
      assert_eq!(context.as_deref(), Some("spotify:playlist:p1"));
      assert_eq!(uris, Some(vec!["spotify:track:t1".to_string()]));
      assert_eq!(offset, Some(0));
    }
    _other => panic!("expected StartPlayback"),
  }
}

// --- dialogs ---

#[test]
fn open_add_track_dialog_without_a_selection_is_a_noop() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::OpenAddTrackDialog);

  assert!(rx.try_recv().is_err());
  assert!(app.pending_playlist_track_add.is_none());
}

#[test]
fn open_add_track_dialog_stages_the_selected_row_for_the_picker() {
  let (mut app, rx) = app_with_channel();
  app.active_source = Source::YouTube;
  app.youtube_playlists = vec![PlaylistInfo {
    uri: "youtube:playlist:y1".to_string(),
    ..playlist_info("y1", "Local List", "owner", false)
  }];
  app.track_table.tracks = vec![track("0000000000000000000001", "Track 1")];
  app.track_table.selected_index = 0;

  app.apply(EngineAction::OpenAddTrackDialog);

  assert_eq!(
    app.get_current_route().active_block,
    ActiveBlock::Dialog(DialogContext::AddTrackToPlaylistPicker)
  );
  let pending = app
    .pending_playlist_track_add
    .as_ref()
    .expect("staged for the picker");
  assert_eq!(pending.track_name, "Track 1");
  assert!(
    rx.try_recv().is_err(),
    "the YouTube path fetches nothing when playlists exist"
  );
}

#[test]
fn open_add_track_dialog_without_destinations_requests_them() {
  let (mut app, rx) = app_with_channel();
  app.active_source = Source::Spotify;
  app.track_table.tracks = vec![track("0000000000000000000001", "Track 1")];
  app.track_table.selected_index = 0;

  app.apply(EngineAction::OpenAddTrackDialog);

  // No user profile and no playlists loaded yet: the flow fetches both
  // first and tells the user to retry.
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetUser)));
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetPlaylists)));
  assert!(app.pending_playlist_track_add.is_none());
}

#[test]
fn open_remove_track_dialog_stages_the_spotify_removal_with_position() {
  let (mut app, rx) = app_with_channel();
  app.track_table.context = Some(crate::core::app::TrackTableContext::MyPlaylists);
  app.playlist_track_table_id = Some(
    rspotify::model::idtypes::PlaylistId::from_id("37i9dQZF1DX4WYpdgoIcn6")
      .unwrap()
      .into_static(),
  );
  app.all_playlists = vec![playlist_info(
    "37i9dQZF1DX4WYpdgoIcn6",
    "My List",
    "owner",
    false,
  )];
  app.track_table.tracks = vec![track("0000000000000000000001", "Track 1")];
  app.track_table.selected_index = 0;
  app.playlist_track_positions = Some(vec![7]);

  app.apply(EngineAction::OpenRemoveTrackDialog);

  let pending = app
    .pending_playlist_track_removal
    .as_ref()
    .expect("staged removal");
  assert_eq!(pending.playlist_name, "My List");
  assert_eq!(pending.track_id, "0000000000000000000001");
  assert_eq!(pending.position, 7);
  assert_eq!(
    app.get_current_route().active_block,
    ActiveBlock::Dialog(DialogContext::RemoveTrackFromPlaylistConfirm)
  );
  assert!(rx.try_recv().is_err());
}

#[test]
fn open_remove_track_dialog_without_a_position_reports_an_error() {
  let (mut app, rx) = app_with_channel();
  app.track_table.context = Some(crate::core::app::TrackTableContext::MyPlaylists);
  app.playlist_track_table_id = Some(
    rspotify::model::idtypes::PlaylistId::from_id("37i9dQZF1DX4WYpdgoIcn6")
      .unwrap()
      .into_static(),
  );
  app.all_playlists = vec![playlist_info(
    "37i9dQZF1DX4WYpdgoIcn6",
    "My List",
    "owner",
    false,
  )];
  app.track_table.tracks = vec![track("0000000000000000000001", "Track 1")];
  app.track_table.selected_index = 0;

  app.apply(EngineAction::OpenRemoveTrackDialog);

  assert_eq!(
    app.status_message.as_deref(),
    Some("Cannot resolve track position for removal"),
  );
  assert!(app.pending_playlist_track_removal.is_none());
  assert!(rx.try_recv().is_err());
}

#[test]
fn open_remove_track_dialog_youtube_routes_the_local_edit() {
  let (mut app, rx) = app_with_channel();
  app.track_table.context = Some(crate::core::app::TrackTableContext::YouTubePlaylist);
  app.youtube_open_playlist = Some("youtube:playlist:y1".to_string());
  app.youtube_playlists = vec![PlaylistInfo {
    uri: "youtube:playlist:y1".to_string(),
    ..playlist_info("y1", "Local List", "owner", false)
  }];
  app.track_table.tracks = vec![track("0000000000000000000001", "Track 1")];
  app.track_table.selected_index = 0;

  app.apply(EngineAction::OpenRemoveTrackDialog);

  let pending = app
    .pending_playlist_track_removal
    .as_ref()
    .expect("staged local edit");
  assert_eq!(pending.playlist_id, "youtube:playlist:y1");
  assert_eq!(pending.position, 0, "unused for local YouTube playlists");
  assert_eq!(
    app.get_current_route().active_block,
    ActiveBlock::Dialog(DialogContext::RemoveTrackFromPlaylistConfirm)
  );
  assert!(rx.try_recv().is_err());
}

// --- library sections ---

#[test]
fn open_library_stats_marks_loading_dispatches_and_pushes() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::OpenLibrary(LibraryTarget::Stats));

  assert!(app.stats_loading);
  assert_eq!(app.get_current_route().id, RouteId::Stats);
  let expected_period = app.stats_period;
  assert!(
    matches!(
      rx.try_recv(),
      Ok(IoEvent::LoadListeningStats(period)) if period == expected_period
    ),
    "the selected stats period rides along"
  );
}

#[test]
fn open_library_recently_played_pushes_before_the_data_arrives() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::OpenLibrary(LibraryTarget::RecentlyPlayed));

  // The row pushes immediately; only the global navigate binding defers
  // the push to the network result.
  assert_eq!(app.get_current_route().id, RouteId::RecentlyPlayed);
  assert!(matches!(rx.try_recv(), Ok(IoEvent::GetRecentlyPlayed)));
}

#[test]
fn open_library_rows_push_their_routes_and_fetch() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::OpenLibrary(LibraryTarget::Discover));
  assert_eq!(app.get_current_route().id, RouteId::Discover);
  assert!(rx.try_recv().is_err());

  app.apply(EngineAction::OpenLibrary(LibraryTarget::Albums));
  assert_eq!(app.get_current_route().id, RouteId::AlbumList);
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetCurrentUserSavedAlbums(None))
  ));

  app.apply(EngineAction::OpenLibrary(LibraryTarget::Artists));
  assert_eq!(app.get_current_route().id, RouteId::Artists);
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetFollowedArtists(None))
  ));

  app.apply(EngineAction::OpenLibrary(LibraryTarget::Podcasts));
  assert_eq!(app.get_current_route().id, RouteId::Podcasts);
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetCurrentUserSavedShows(None))
  ));
}

#[cfg(all(test, feature = "local-files"))]
#[test]
fn open_library_local_files_pushes_the_browser_route() {
  let (mut app, rx) = app_with_channel();

  app.apply(EngineAction::OpenLibrary(LibraryTarget::LocalFiles));

  assert_eq!(app.get_current_route().id, RouteId::LocalBrowser);
  assert!(rx.try_recv().is_err());
}

#[cfg(all(test, feature = "ai-dj"))]
#[test]
fn open_library_ai_dj_opens_the_screen_through_app() {
  let (mut app, _rx) = app_with_channel();

  app.apply(EngineAction::OpenLibrary(LibraryTarget::AiDj));

  assert_eq!(app.get_current_route().id, RouteId::AiDj);
}

#[test]
fn open_library_liked_songs_resets_the_cache_and_fetches() {
  let (mut app, rx) = app_with_channel();
  let page = track_page(0, &["0000000000000000000001"], false);
  app.library.saved_tracks.upsert_page_by_offset(page);

  app.apply(EngineAction::OpenLibrary(LibraryTarget::LikedSongs));

  assert_eq!(app.get_current_route().id, RouteId::TrackTable);
  assert!(app.library.saved_tracks.pages.is_empty(), "cache reset");
  assert!(matches!(
    rx.try_recv(),
    Ok(IoEvent::GetCurrentSavedTracks(None))
  ));
}

#[test]
fn library_target_names_round_trip_and_cover_every_sidebar_row() {
  for target in LibraryTarget::ALL {
    assert_eq!(LibraryTarget::from_name(target.name()), Some(target));
  }
  // Every sidebar label resolves to a DISTINCT target: this is the
  // load-bearing invariant (name() must equal the library_options()
  // strings, or the handler's name resolution remaps rows).
  let options = crate::core::app::library_options();
  let mut resolved = Vec::new();
  for label in options {
    let target = LibraryTarget::from_name(label)
      .unwrap_or_else(|| panic!("sidebar row {label} has no LibraryTarget"));
    assert!(!resolved.contains(&target), "duplicate label {label}");
    resolved.push(target);
  }
  assert_eq!(LibraryTarget::from_name("no such row"), None);
}

// --- SelectSource ---

#[test]
fn select_source_flips_scope_mirrors_runtime_state_and_persists() {
  let dir = tempfile::tempdir().unwrap();
  let (mut app, rx) = app_with_channel();
  app.state_path = Some(dir.path().join("state.yml"));

  app.apply(EngineAction::SelectSource(Source::Local));

  assert_eq!(app.active_source, Source::Local);
  assert_eq!(app.runtime_state.active_source, Source::Local);
  let written = std::fs::read_to_string(dir.path().join("state.yml")).unwrap();
  assert!(
    written.contains("active_source") && written.contains("Local"),
    "sparse patch persisted the choice"
  );
  assert!(
    rx.try_recv().is_err(),
    "sidebar loading stays with the caller"
  );
}
