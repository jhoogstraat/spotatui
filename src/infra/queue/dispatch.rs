//! Fold-facing native queue dispatcher.
//!
//! Queue playback still has source-specific async effects (decode, download,
//! or a direct librespot load). Those effects live behind [`QueueRuntime`]; the
//! public router itself owns no `App` handle and asks the engine spine for the
//! published queue state before running an effect. This keeps the pump's
//! routing seam independent of the state owner while the final source effects
//! migrate to typed queue facts.

use std::future::Future;
use std::pin::Pin;

use crate::infra::network::IoEvent;
#[cfg(any(feature = "subsonic", feature = "youtube"))]
use anyhow::anyhow;

#[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
use crate::infra::audio::LocalPlayer;
#[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
use std::sync::Arc;
#[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
static QUEUE_FETCH_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Runtime effect bridge for queue playback. Production callers use the
/// engine [`Spine`]; the App-backed implementation remains available to
/// isolated unit tests and to boot paths that have no fold yet.
pub trait QueueRuntime {
  fn route_queue_event<'a>(
    &'a self,
    event: &'a IoEvent,
  ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

/// Route one IoEvent through the queue domain before source dispatchers.
pub async fn route_queue_event<R: QueueRuntime + ?Sized>(runtime: &R, event: &IoEvent) -> bool {
  runtime.route_queue_event(event).await
}

/// Engine-spine implementation. The query is deliberately performed before
/// invoking the effect bridge: it gives queue-owned controls an ordered,
/// fold-authoritative claim and makes a closed fold fail closed.
impl QueueRuntime for crate::runtime::engine::Spine {
  fn route_queue_event<'a>(
    &'a self,
    event: &'a IoEvent,
  ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
    Box::pin(async move {
      let owned = matches!(
        event,
        IoEvent::AdvanceNativeQueue
          | IoEvent::PausePlayback
          | IoEvent::StartPlayback(None, None, None)
          | IoEvent::Seek(_)
          | IoEvent::ChangeVolume(_)
          | IoEvent::NextTrack
          | IoEvent::PreviousTrack
          | IoEvent::ForcePreviousTrack
      );
      #[cfg(feature = "streaming")]
      let owned = owned || matches!(event, IoEvent::ReplayPublishedSpotifyQueueSlot);
      if owned {
        // QueueTracks is answered by the fold under its App lock. It is a
        // read barrier, not a second source of truth in this producer.
        if self
          .query(crate::runtime::engine::FoldQuery::QueueTracks)
          .await
          .is_err()
        {
          return false;
        }
      }
      if matches!(
        event,
        IoEvent::StartPlayback(Some(_), _, _) | IoEvent::StartPlayback(_, Some(_), _)
      ) {
        // Clear the slot in the fold before source teardown/start effects run;
        // this preserves the queue ownership ordering across concurrent
        // recovery and playback events.
        #[cfg(any(
          feature = "streaming",
          feature = "local-files",
          feature = "subsonic",
          feature = "youtube"
        ))]
        if self
          .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
            crate::runtime::engine::DecodedSourceFact::QueuePlaybackCleared,
          ))
          .await
          .is_none()
        {
          return false;
        }
      }
      // Transport I/O uses only handles claimed from the fold. No App handle
      // crosses this seam; if no queue slot owns the device, normal source
      // routing remains responsible for the event.
      match event {
        #[cfg(any(
          feature = "streaming",
          feature = "local-files",
          feature = "subsonic",
          feature = "youtube"
        ))]
        IoEvent::AdvanceNativeQueue | IoEvent::NextTrack => {
          advance_queue(self, matches!(event, IoEvent::NextTrack)).await
        }
        #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
        IoEvent::PausePlayback
        | IoEvent::StartPlayback(None, None, None)
        | IoEvent::Seek(_)
        | IoEvent::PreviousTrack
        | IoEvent::ForcePreviousTrack
        | IoEvent::ChangeVolume(_) => {
          let decoded = self
            .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
            .await
            .ok()
            .and_then(|answer| match answer {
              crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(player) => player,
              _ => None,
            });
          if let Some(player) = decoded {
            match event {
              IoEvent::PausePlayback => player.pause(),
              IoEvent::StartPlayback(None, None, None) => player.resume(),
              IoEvent::Seek(position_ms) => {
                let _ = player.seek(std::time::Duration::from_millis(*position_ms as u64));
              }
              IoEvent::PreviousTrack | IoEvent::ForcePreviousTrack => {
                let _ = player.seek(std::time::Duration::ZERO);
              }
              IoEvent::ChangeVolume(volume) => {
                player.set_volume(*volume as f32 / 100.0);
                let _ = self
                  .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
                    crate::runtime::engine::DecodedSourceFact::Volume(*volume),
                  ))
                  .await;
              }
              _ => unreachable!(),
            }
            return true;
          }
          #[cfg(feature = "streaming")]
          {
            let Ok(crate::runtime::engine::FoldAnswer::Bool(true)) = self
              .query(crate::runtime::engine::FoldQuery::QueueSlotActive)
              .await
            else {
              return false;
            };
            let Ok(crate::runtime::engine::FoldAnswer::StreamingPlayer(Some(player))) = self
              .query(crate::runtime::engine::FoldQuery::StreamingPlayerHandle)
              .await
            else {
              return false;
            };
            match event {
              IoEvent::PausePlayback => player.pause(),
              IoEvent::StartPlayback(None, None, None) => player.play(),
              IoEvent::PreviousTrack | IoEvent::ForcePreviousTrack => player.seek(0),
              IoEvent::ChangeVolume(volume) => {
                player.set_volume(*volume);
                let generation = match self
                  .query(crate::runtime::engine::FoldQuery::QueueSlotGeneration)
                  .await
                {
                  Ok(crate::runtime::engine::FoldAnswer::U64(generation)) => generation,
                  _ => return false,
                };
                let _ = self
                  .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
                    crate::runtime::engine::DecodedSourceFact::QueueTransportIntent {
                      generation,
                      is_playing: true,
                    },
                  ))
                  .await;
              }
              _ => unreachable!(),
            }
            return true;
          }
          false
        }
        _ => false,
      }
    })
  }
}

#[cfg(any(
  feature = "streaming",
  feature = "internet-radio",
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube"
))]
async fn resume_suspended(spine: &crate::runtime::engine::Spine) {
  let suspended = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueTakeSuspended,
    ))
    .await
    .and_then(|ack| ack.queue_suspended)
    .flatten();
  let Some(suspended) = suspended else { return };
  let tx = spine
    .query(crate::runtime::engine::FoldQuery::IoTx)
    .await
    .ok()
    .and_then(|answer| match answer {
      crate::runtime::engine::FoldAnswer::IoTx(tx) => tx,
      _ => None,
    });
  #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
  let position_ms = match &suspended {
    #[cfg(feature = "local-files")]
    crate::core::queue::SuspendedContext::Local {
      resume_position_ms, ..
    } => Some(*resume_position_ms),
    #[cfg(feature = "subsonic")]
    crate::core::queue::SuspendedContext::Subsonic {
      resume_position_ms, ..
    } => Some(*resume_position_ms),
    #[cfg(feature = "youtube")]
    crate::core::queue::SuspendedContext::YouTube {
      resume_position_ms, ..
    } => Some(*resume_position_ms),
    _ => None,
  };
  let Some(tx) = tx else { return };
  let event = match suspended {
    #[cfg(feature = "streaming")]
    crate::core::queue::SuspendedContext::Spotify {
      context_uri,
      resume_track_uri,
    } => crate::infra::network::IoEvent::ResumeSpotifyContext(context_uri, resume_track_uri),
    #[cfg(feature = "streaming")]
    crate::core::queue::SuspendedContext::SpotifyShuffled {
      resume_index,
      generation,
      ..
    } => crate::infra::network::IoEvent::ResumeNativeShuffleSession(resume_index, generation),
    #[cfg(feature = "internet-radio")]
    crate::core::queue::SuspendedContext::Radio { station } => {
      crate::infra::network::IoEvent::StartPlayback(station.uri, None, None)
    }
    #[cfg(feature = "local-files")]
    crate::core::queue::SuspendedContext::Local { resume_index, .. } => {
      let Some(index) = resume_index else {
        let _ = spine
          .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
            crate::runtime::engine::DecodedSourceFact::LocalPlaybackCleared,
          ))
          .await;
        return;
      };
      if let Ok(crate::runtime::engine::FoldAnswer::Decoded(
        crate::runtime::engine::DecodedSourceAnswer::LocalPlayIndex(Some((player, uri))),
      )) = spine
        .query(crate::runtime::engine::FoldQuery::Decoded(
          crate::runtime::engine::DecodedSourceQuery::LocalPlayIndex(index),
        ))
        .await
      {
        if crate::infra::local::dispatch::play_single_file(&player, &uri)
          .await
          .is_ok()
        {
          if let Some(ms) = position_ms {
            let _ = player.seek(std::time::Duration::from_millis(ms));
          }
        }
      }
      return;
    }
    #[cfg(feature = "subsonic")]
    crate::core::queue::SuspendedContext::Subsonic { resume_index, .. } => {
      let Some(index) = resume_index else {
        let _ = spine
          .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
            crate::runtime::engine::DecodedSourceFact::SubsonicPlaybackCleared,
          ))
          .await;
        return;
      };
      crate::infra::subsonic::dispatch::play_index(spine, index).await;
      if let Some(ms) = position_ms {
        if let Ok(crate::runtime::engine::FoldAnswer::Decoded(
          crate::runtime::engine::DecodedSourceAnswer::SubsonicPlayer(Some(player)),
        )) = spine
          .query(crate::runtime::engine::FoldQuery::Decoded(
            crate::runtime::engine::DecodedSourceQuery::SubsonicPlayer,
          ))
          .await
        {
          let _ = player.seek(std::time::Duration::from_millis(ms));
        }
      }
      return;
    }
    #[cfg(feature = "youtube")]
    crate::core::queue::SuspendedContext::YouTube { resume_index, .. } => {
      let Some(index) = resume_index else {
        let _ = spine
          .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
            crate::runtime::engine::DecodedSourceFact::YouTubePlaybackCleared,
          ))
          .await;
        return;
      };
      crate::infra::youtube::dispatch::play_index(spine, index).await;
      if let Some(ms) = position_ms {
        if let Ok(crate::runtime::engine::FoldAnswer::Decoded(
          crate::runtime::engine::DecodedSourceAnswer::YouTubePlayer(Some(player)),
        )) = spine
          .query(crate::runtime::engine::FoldQuery::Decoded(
            crate::runtime::engine::DecodedSourceQuery::YouTubePlayer,
          ))
          .await
        {
          let _ = player.seek(std::time::Duration::from_millis(ms));
        }
      }
      return;
    }
    #[allow(unreachable_patterns)]
    _ => return,
  };
  let _ = tx.send(event);
}

#[cfg(any(
  feature = "streaming",
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube"
))]
async fn queue_status(spine: &crate::runtime::engine::Spine, message: &str) {
  let _ = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Session(
      crate::runtime::engine::SessionFact::TransientStatus {
        message: message.to_string(),
        ttl_secs: 4,
      },
    ))
    .await;
}

/// Atomically claim the queue head before doing any source I/O. This is the
/// linearization point for queue workers: a second Advance cannot start a
/// duplicate download, and a stale/mismatched producer cannot consume a later
/// item.
#[cfg(any(
  feature = "streaming",
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube"
))]
async fn claim_queue_head(
  spine: &crate::runtime::engine::Spine,
) -> Option<crate::core::plugin_api::TrackInfo> {
  let head = match spine
    .query(crate::runtime::engine::FoldQuery::QueueTracks)
    .await
    .ok()?
  {
    crate::runtime::engine::FoldAnswer::QueueTracks(mut tracks) => tracks.drain(..).next(),
    _ => None,
  }?;
  spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueNativeTrackPopped { track: head },
    ))
    .await
    .and_then(|ack| ack.queue_track)
}

#[cfg(any(
  feature = "streaming",
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube"
))]
async fn advance_queue(spine: &crate::runtime::engine::Spine, explicit_next: bool) -> bool {
  if explicit_next {
    let has_upcoming = matches!(
      spine.query(crate::runtime::engine::FoldQuery::QueueTracks).await,
      Ok(crate::runtime::engine::FoldAnswer::QueueTracks(ref tracks)) if !tracks.is_empty()
    );
    if !has_upcoming {
      let mut has_slot = false;
      #[cfg(any(feature = "local-files", feature = "subsonic", feature = "youtube"))]
      {
        has_slot = spine
          .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
          .await
          .ok()
          .is_some_and(|answer| {
            matches!(
              answer,
              crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(Some(_))
            )
          });
      }
      #[cfg(feature = "streaming")]
      {
        has_slot |= matches!(
          spine
            .query(crate::runtime::engine::FoldQuery::QueueSlotActive)
            .await,
          Ok(crate::runtime::engine::FoldAnswer::Bool(true))
        );
      }
      if !has_slot {
        return false;
      }
    }
  }
  let Some(track) = claim_queue_head(spine).await else {
    let _ = spine
      .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
        crate::runtime::engine::DecodedSourceFact::QueueSlotCleared,
      ))
      .await;
    resume_suspended(spine).await;
    return true;
  };
  let Some(uri) = track.uri.clone() else {
    queue_status(spine, "Skipped a queued track with no URI").await;
    return true;
  };
  match crate::core::queue::queue_item_source(&uri) {
    #[cfg(feature = "local-files")]
    crate::core::queue::QueueItemSource::LocalFile => play_local_claimed(spine, track, uri).await,
    #[cfg(feature = "subsonic")]
    crate::core::queue::QueueItemSource::Subsonic => {
      start_downloaded_queue(spine, track, false).await
    }
    #[cfg(feature = "youtube")]
    crate::core::queue::QueueItemSource::YouTube => {
      start_downloaded_queue(spine, track, true).await
    }
    #[cfg(feature = "streaming")]
    crate::core::queue::QueueItemSource::Spotify => play_spotify_claimed(spine, track, uri).await,
    _ => {
      queue_status(spine, "Queued source isn't available in this build").await;
      true
    }
  }
}

#[cfg(feature = "local-files")]
async fn play_local_claimed(
  spine: &crate::runtime::engine::Spine,
  track: crate::core::plugin_api::TrackInfo,
  uri: String,
) -> bool {
  let player = spine
    .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
    .await
    .ok()
    .and_then(|answer| match answer {
      crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(player) => player,
      _ => None,
    });
  let player = if player.is_some() {
    player
  } else {
    spine
      .query(crate::runtime::engine::FoldQuery::Decoded(
        crate::runtime::engine::DecodedSourceQuery::LocalPlayer,
      ))
      .await
      .ok()
      .and_then(|answer| match answer {
        crate::runtime::engine::FoldAnswer::Decoded(
          crate::runtime::engine::DecodedSourceAnswer::LocalPlayer(player),
        ) => player,
        _ => None,
      })
  };
  let player = match player {
    Some(player) => player,
    None => match tokio::task::spawn_blocking(LocalPlayer::new).await {
      Ok(Ok(player)) => Arc::new(player),
      _ => {
        queue_status(
          spine,
          "Cannot play queued local track: audio output unavailable",
        )
        .await;
        return true;
      }
    },
  };
  let fetch_id = QUEUE_FETCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  player.pause();
  let _ = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueuePendingDecoded {
        player: Arc::clone(&player),
        track,
        fetch_id,
      },
    ))
    .await;
  if crate::infra::local::dispatch::play_single_file(&player, &uri)
    .await
    .is_ok()
  {
    let _ = spine
      .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
        crate::runtime::engine::DecodedSourceFact::QueueDecodedLocalReady { fetch_id },
      ))
      .await;
  } else {
    let _ = spine
      .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
        crate::runtime::engine::DecodedSourceFact::QueuePlaybackCleared,
      ))
      .await;
  }
  true
}

#[cfg(feature = "streaming")]
async fn play_spotify_claimed(
  spine: &crate::runtime::engine::Spine,
  track: crate::core::plugin_api::TrackInfo,
  uri: String,
) -> bool {
  let Ok(crate::runtime::engine::FoldAnswer::StreamingPlayer(Some(player))) = spine
    .query(crate::runtime::engine::FoldQuery::StreamingPlayerHandle)
    .await
  else {
    return true;
  };
  player.activate();
  if player.play_uri(&uri).await.is_ok() {
    let _ = spine
      .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
        crate::runtime::engine::DecodedSourceFact::QueueSpotifyPublished(track),
      ))
      .await;
  }
  true
}

#[cfg(any(feature = "subsonic", feature = "youtube"))]
async fn start_downloaded_queue(
  spine: &crate::runtime::engine::Spine,
  track: crate::core::plugin_api::TrackInfo,
  youtube: bool,
) -> bool {
  let Some(uri) = track.uri.clone() else {
    return true;
  };
  let expected = if youtube {
    crate::core::queue::QueueItemSource::YouTube
  } else {
    crate::core::queue::QueueItemSource::Subsonic
  };
  if crate::core::queue::queue_item_source(&uri) != expected {
    queue_status(spine, "Queued source isn't available in this build").await;
    return true;
  }
  // Reuse the suspended source sink where available. Pausing it before the
  // pending fact prevents two players from writing the same output device.
  let player = spine
    .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
    .await
    .ok()
    .and_then(|answer| match answer {
      crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(player) => player,
      _ => None,
    });
  let player = if player.is_some() {
    player
  } else if youtube {
    #[cfg(feature = "youtube")]
    {
      spine
        .query(crate::runtime::engine::FoldQuery::Decoded(
          crate::runtime::engine::DecodedSourceQuery::YouTubePlayer,
        ))
        .await
        .ok()
        .and_then(|answer| match answer {
          crate::runtime::engine::FoldAnswer::Decoded(
            crate::runtime::engine::DecodedSourceAnswer::YouTubePlayer(player),
          ) => player,
          _ => None,
        })
    }
    #[cfg(not(feature = "youtube"))]
    {
      None
    }
  } else {
    #[cfg(feature = "subsonic")]
    {
      spine
        .query(crate::runtime::engine::FoldQuery::Decoded(
          crate::runtime::engine::DecodedSourceQuery::SubsonicPlayer,
        ))
        .await
        .ok()
        .and_then(|answer| match answer {
          crate::runtime::engine::FoldAnswer::Decoded(
            crate::runtime::engine::DecodedSourceAnswer::SubsonicPlayer(player),
          ) => player,
          _ => None,
        })
    }
    #[cfg(not(feature = "subsonic"))]
    {
      None
    }
  };
  let player = match player {
    Some(player) => player,
    None => match tokio::task::spawn_blocking(LocalPlayer::new).await {
      Ok(Ok(player)) => Arc::new(player),
      _ => {
        queue_status(spine, "Cannot play queued track: audio output unavailable").await;
        return true;
      }
    },
  };
  player.pause();
  let fetch_id = QUEUE_FETCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  if spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueuePendingDecoded {
        player: Arc::clone(&player),
        track,
        fetch_id,
      },
    ))
    .await
    .is_none()
  {
    return true;
  }
  // Claim the lease before detached I/O. This is what prevents duplicate
  // workers and lets QueueFetchCompleted reject superseded work.
  let Some(claim) = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueFetchClaimed { fetch_id },
    ))
    .await
    .and_then(|ack| ack.queue_fetch_claim)
  else {
    return true;
  };
  let worker_spine = spine.clone();
  tokio::spawn(async move {
    let result: anyhow::Result<tempfile::NamedTempFile> = if youtube {
      #[cfg(feature = "youtube")]
      {
        let path = match worker_spine
          .query(crate::runtime::engine::FoldQuery::Decoded(
            crate::runtime::engine::DecodedSourceQuery::YouTubeConfig,
          ))
          .await
          .ok()
        {
          Some(crate::runtime::engine::FoldAnswer::Decoded(
            crate::runtime::engine::DecodedSourceAnswer::YouTubeConfig(path),
          )) => path,
          _ => None,
        };
        let source = crate::infra::youtube::YouTubeSource::new(path);
        crate::infra::youtube::dispatch::download_for_queue(&source, &uri).await
      }
      #[cfg(not(feature = "youtube"))]
      {
        Err(anyhow!("youtube source unavailable"))
      }
    } else {
      #[cfg(feature = "subsonic")]
      {
        match crate::infra::subsonic::dispatch::build_source(&worker_spine).await {
          Some(source) => crate::infra::subsonic::dispatch::download_for_queue(&source, &uri).await,
          None => Err(anyhow!("subsonic source unavailable")),
        }
      }
      #[cfg(not(feature = "subsonic"))]
      {
        Err(anyhow!("subsonic source unavailable"))
      }
    };
    finish_downloaded_queue(&worker_spine, fetch_id, claim, result).await;
  });
  true
}

#[cfg(any(feature = "subsonic", feature = "youtube"))]
async fn finish_downloaded_queue(
  spine: &crate::runtime::engine::Spine,
  fetch_id: u64,
  claim: crate::runtime::engine::QueueFetchClaim,
  result: anyhow::Result<tempfile::NamedTempFile>,
) {
  let Ok(tmp) = result else {
    let _ = spine
      .emit(crate::runtime::engine::EngineEvent::Decoded(
        crate::runtime::engine::DecodedSourceFact::QueueFetchCompleted {
          fetch_id,
          result: crate::runtime::engine::QueueFetchResult::Failure("queue download failed".into()),
        },
      ))
      .await;
    return;
  };
  if tmp.path().metadata().map(|m| m.len() == 0).unwrap_or(true) {
    let _ = spine
      .emit(crate::runtime::engine::EngineEvent::Decoded(
        crate::runtime::engine::DecodedSourceFact::QueueFetchCompleted {
          fetch_id,
          result: crate::runtime::engine::QueueFetchResult::Empty,
        },
      ))
      .await;
    return;
  }
  let path = tmp.path().to_path_buf();
  let player = Arc::clone(&claim.player);
  let lease = Arc::clone(&claim.lease);
  let outcome = tokio::task::spawn_blocking(move || lease.play_file(&player, &path)).await;
  let result = match outcome {
    Ok(Ok(())) => crate::runtime::engine::QueueFetchResult::Success(tmp),
    Ok(Err(error)) => crate::runtime::engine::QueueFetchResult::Failure(error.to_string()),
    Err(_) => crate::runtime::engine::QueueFetchResult::Cancelled,
  };
  let _ = spine
    .emit(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueFetchCompleted { fetch_id, result },
    ))
    .await;
}
