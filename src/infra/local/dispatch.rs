//! Local-file playback routing. State is owned by the event fold; this module
//! only queries snapshots and performs decoder/player effects.

use std::sync::Arc;
use std::time::Duration;

use super::{file_uri_to_path, track_info_from_path, LocalPlaybackState, LocalSource};
use crate::core::source::MediaSource;
use crate::infra::audio::LocalPlayer;
use crate::infra::network::IoEvent;
use crate::infra::queue::replay_file;
use crate::runtime::engine::{
  DecodedSourceAnswer as Answer, DecodedSourceFact as Fact, DecodedSourceQuery as Query,
  EngineEvent, FoldAnswer, FoldQuery, Spine,
};

fn is_file_uri(uri: &str) -> bool {
  uri.starts_with("file:")
}

/// Intercept events owned by local-files. `Spine` is the sole state seam:
/// producers never retain or lock `App`.
pub async fn route_local_event(spine: &Spine, event: &IoEvent) -> bool {
  match event {
    IoEvent::GetLocalPlaylists => {
      load_local_playlists(spine).await;
      true
    }
    IoEvent::GetLocalTracks(uri) => {
      load_local_tracks(spine, uri).await;
      true
    }
    IoEvent::StartPlayback(None, Some(uris), offset)
      if uris.first().is_some_and(|uri| is_file_uri(uri)) =>
    {
      start_local_queue(spine, uris.clone(), offset.unwrap_or(0)).await;
      true
    }
    IoEvent::StartPlayback(Some(uri), _, _) if is_file_uri(uri) => {
      start_local_queue(spine, vec![uri.clone()], 0).await;
      true
    }
    IoEvent::StartPlayback(None, None, None) => match local_player(spine).await {
      Some(player) => {
        player.resume();
        true
      }
      None => false,
    },
    // A non-local start must release this device before the next dispatcher acquires it.
    IoEvent::StartPlayback(..) => {
      teardown_local(spine).await;
      false
    }
    IoEvent::PausePlayback => match local_player(spine).await {
      Some(player) => {
        player.pause();
        true
      }
      None => false,
    },
    IoEvent::Seek(position_ms) => match local_player(spine).await {
      Some(player) => {
        let _ = player.seek(Duration::from_millis(*position_ms as u64));
        true
      }
      None => false,
    },
    IoEvent::ChangeVolume(volume) => match local_player(spine).await {
      Some(player) => {
        player.set_volume(*volume as f32 / 100.0);
        emit(spine, Fact::Volume(*volume)).await;
        true
      }
      None => false,
    },
    IoEvent::NextTrack => skip(spine, true).await,
    IoEvent::PreviousTrack | IoEvent::ForcePreviousTrack => skip(spine, false).await,
    IoEvent::ReplayCurrentTrack => replay_current(spine).await,
    IoEvent::Repeat(state) => matches!(
      decoded(spine, Query::LocalRepeat(*state)).await,
      Some(Answer::LocalRepeat(true))
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
  // Ownership and RMW changes must be visible before the next player effect.
  let _ = spine.emit_acked(EngineEvent::Decoded(fact)).await;
}

async fn status(spine: &Spine, message: impl Into<String>, ttl_secs: u64) {
  emit(
    spine,
    Fact::LocalStatus {
      message: message.into(),
      ttl_secs,
    },
  )
  .await;
}

async fn local_player(spine: &Spine) -> Option<Arc<LocalPlayer>> {
  match decoded(spine, Query::LocalPlayer).await? {
    Answer::LocalPlayer(player) => player,
    _ => None,
  }
}

async fn skip(spine: &Spine, forward: bool) -> bool {
  if local_player(spine).await.is_none() {
    return false;
  }
  let target = match decoded(spine, Query::LocalSkip { forward }).await {
    Some(Answer::LocalSkip(target)) => target,
    _ => return false,
  };
  match target {
    Some(index) => play_index(spine, index).await,
    None => emit(spine, Fact::LocalAdvancing(false)).await,
  }
  true
}

async fn replay_current(spine: &Spine) -> bool {
  if local_player(spine).await.is_none() {
    return false;
  }
  let target = match decoded(spine, Query::LocalReplay).await {
    Some(Answer::LocalReplay(target)) => target,
    _ => return false,
  };
  let replayed = match target {
    Some((player, path)) => replay_file(player, path).await,
    None => false,
  };
  if replayed {
    emit(spine, Fact::LocalReplaySucceeded).await;
  } else {
    teardown_local(spine).await;
    status(spine, "Cannot replay local file", 6).await;
  }
  true
}

async fn start_local_queue(spine: &Spine, queue: Vec<String>, start_idx: usize) {
  if queue.is_empty() {
    status(spine, "No local tracks to play", 6).await;
    return;
  }
  let index = start_idx.min(queue.len() - 1);
  let path = match file_uri_to_path(&queue[index]) {
    Ok(path) => path,
    Err(error) => {
      status(spine, format!("Invalid local file URI: {error}"), 6).await;
      return;
    }
  };
  // Reuse a live local player; opening another output would fail on exclusive
  // devices and would unnecessarily leak the existing session's sink.
  let existing_player = local_player(spine).await;
  teardown_backends(spine, existing_player.is_none()).await;
  let Some(player) = (match existing_player {
    Some(player) => Some(player),
    None => acquire_player(spine).await,
  }) else {
    return;
  };
  let decode_player = Arc::clone(&player);
  let result = tokio::task::spawn_blocking(move || {
    let info = track_info_from_path(&path);
    decode_player.play_file(&path).map(|()| info)
  })
  .await;
  let info = match result {
    Ok(Ok(info)) => info,
    Ok(Err(error)) => {
      status(spine, format!("Cannot play local file: {error}"), 6).await;
      return;
    }
    Err(error) => {
      status(spine, format!("Local playback task failed: {error}"), 6).await;
      return;
    }
  };
  if let Some(Answer::Volume(volume)) = decoded(spine, Query::Volume).await {
    player.set_volume(volume as f32 / 100.0);
  }
  let shuffle = matches!(
    decoded(spine, Query::LocalShuffle).await,
    Some(Answer::LocalShuffle(true))
  );
  let display_name = info.name.clone();
  let mut state = LocalPlaybackState {
    player,
    queue,
    index,
    name: info.name,
    artists: info.artists.join(", "),
    album: info.album,
    duration_ms: info.duration_ms,
    advancing: false,
    shuffle_backup: None,
    failed_since_played: std::collections::HashSet::new(),
  };
  if shuffle {
    state.set_shuffle(true);
  }
  emit(spine, Fact::LocalPlayback(state)).await;
  status(spine, format!("\u{266a} {display_name}"), 4).await;
}

async fn teardown_backends(spine: &Spine, clear_local: bool) {
  let backends = decoded(
    spine,
    Query::ClaimBackendPlayers {
      keep: (!clear_local).then_some(crate::core::source::Source::Local),
    },
  )
  .await;
  if let Some(Answer::BackendPlayers {
    #[cfg(feature = "streaming")]
    streaming,
    #[cfg(feature = "local-files")]
      local: _,
    #[cfg(feature = "subsonic")]
    subsonic,
    #[cfg(feature = "internet-radio")]
    radio,
    #[cfg(feature = "youtube")]
    youtube,
  }) = backends
  {
    #[cfg(feature = "streaming")]
    if let Some(player) = streaming {
      player.pause();
    }
    #[cfg(feature = "subsonic")]
    if let Some(player) = subsonic {
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

#[cfg_attr(not(feature = "local-files"), allow(dead_code))]
pub(crate) async fn play_single_file(
  player: &Arc<LocalPlayer>,
  uri: &str,
) -> anyhow::Result<crate::core::plugin_api::TrackInfo> {
  let path = file_uri_to_path(uri)?;
  let player = Arc::clone(player);
  tokio::task::spawn_blocking(move || {
    let info = track_info_from_path(&path);
    player.play_file(&path).map(|()| info)
  })
  .await?
}

/// The fold atomically claims the live player, URI and advancing guard before decode.
pub(crate) async fn play_index(spine: &Spine, target: usize) {
  let (player, uri) = match decoded(spine, Query::LocalPlayIndex(target)).await {
    Some(Answer::LocalPlayIndex(Some(value))) => value,
    _ => return,
  };
  let path = match file_uri_to_path(&uri) {
    Ok(path) => path,
    Err(error) => {
      fail_index(spine, target, format!("Invalid local file URI: {error}")).await;
      return;
    }
  };
  let decode_player = Arc::clone(&player);
  let result = tokio::task::spawn_blocking(move || {
    let info = track_info_from_path(&path);
    decode_player.play_file(&path).map(|()| info)
  })
  .await;
  match result {
    Ok(Ok(info)) => {
      let name = info.name.clone();
      emit(
        spine,
        Fact::LocalIndex {
          target,
          info: Some(info),
        },
      )
      .await;
      status(spine, format!("\u{266a} {name}"), 4).await;
    }
    Ok(Err(error)) => {
      player.stop();
      fail_index(spine, target, format!("Cannot play local file: {error}")).await;
    }
    Err(error) => {
      player.stop();
      fail_index(
        spine,
        target,
        format!("Local playback task failed: {error}"),
      )
      .await;
    }
  }
}

async fn fail_index(spine: &Spine, target: usize, message: String) {
  emit(spine, Fact::LocalFailedIndex { target, message }).await;
}

async fn acquire_player(spine: &Spine) -> Option<Arc<LocalPlayer>> {
  match tokio::task::spawn_blocking(LocalPlayer::new).await {
    Ok(Ok(player)) => Some(Arc::new(player)),
    Ok(Err(error)) => {
      status(
        spine,
        format!("No audio output for local playback: {error}"),
        6,
      )
      .await;
      None
    }
    Err(error) => {
      status(spine, format!("Audio output init failed: {error}"), 6).await;
      None
    }
  }
}

async fn teardown_local(spine: &Spine) {
  emit(spine, Fact::LocalPlaybackCleared).await;
}

async fn music_root(spine: &Spine) -> Option<String> {
  match decoded(spine, Query::LocalConfig).await {
    Some(Answer::LocalConfig(Some(root))) => Some(root),
    _ => {
      status(
        spine,
        "No local music folder configured (set behavior.local_music_path)",
        6,
      )
      .await;
      None
    }
  }
}

async fn load_local_playlists(spine: &Spine) {
  let Some(root) = music_root(spine).await else {
    return;
  };
  match tokio::task::spawn_blocking(move || {
    futures::executor::block_on(LocalSource::new(root).playlists())
  })
  .await
  {
    Ok(Ok(playlists)) => emit(spine, Fact::LocalPlaylists(playlists)).await,
    Ok(Err(error)) => status(spine, format!("Cannot scan music folder: {error}"), 6).await,
    Err(error) => status(spine, format!("Local folder scan failed: {error}"), 6).await,
  }
}

async fn load_local_tracks(spine: &Spine, playlist_uri: &str) {
  let Some(root) = music_root(spine).await else {
    return;
  };
  let uri = playlist_uri.to_string();
  match tokio::task::spawn_blocking(move || {
    futures::executor::block_on(LocalSource::new(root).tracks(&uri))
  })
  .await
  {
    Ok(Ok(tracks)) => emit(spine, Fact::LocalTracks(tracks)).await,
    Ok(Err(error)) => status(spine, format!("Cannot read folder: {error}"), 6).await,
    Err(error) => status(spine, format!("Local track scan failed: {error}"), 6).await,
  }
}
