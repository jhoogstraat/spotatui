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

#[cfg(feature = "local-files")]
async fn play_local_queue(spine: &crate::runtime::engine::Spine) -> bool {
  let track = match spine
    .query(crate::runtime::engine::FoldQuery::QueueTracks)
    .await
  {
    Ok(crate::runtime::engine::FoldAnswer::QueueTracks(mut tracks)) => tracks.drain(..).next(),
    _ => None,
  };
  let Some(track) = track else { return true };
  let Some(uri) = track.uri.clone() else {
    return true;
  };
  if crate::core::queue::queue_item_source(&uri) != crate::core::queue::QueueItemSource::LocalFile {
    return false;
  }
  let _ = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueNativeTrackPopped {
        track: track.clone(),
      },
    ))
    .await;
  let player = spine
    .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
    .await
    .ok()
    .and_then(|answer| match answer {
      crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(player) => player,
      _ => None,
    });
  let player = match player {
    Some(player) => player,
    None => match tokio::task::spawn_blocking(LocalPlayer::new).await {
      Ok(Ok(player)) => Arc::new(player),
      _ => return true,
    },
  };
  let fetch_id = QUEUE_FETCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        #[cfg(all(
          feature = "subsonic",
          not(feature = "streaming"),
          not(feature = "local-files")
        ))]
        IoEvent::AdvanceNativeQueue | IoEvent::NextTrack => {
          play_downloaded_queue(self, false).await
        }
        #[cfg(all(
          feature = "youtube",
          not(feature = "streaming"),
          not(feature = "local-files")
        ))]
        IoEvent::AdvanceNativeQueue | IoEvent::NextTrack => play_downloaded_queue(self, true).await,
        #[cfg(all(feature = "local-files", not(feature = "streaming")))]
        IoEvent::AdvanceNativeQueue | IoEvent::NextTrack => {
          let track = match self
            .query(crate::runtime::engine::FoldQuery::QueueTracks)
            .await
          {
            Ok(crate::runtime::engine::FoldAnswer::QueueTracks(mut tracks)) => {
              tracks.drain(..).next()
            }
            _ => None,
          };
          let Some(track) = track else { return true };
          let Some(uri) = track.uri.clone() else {
            return true;
          };
          if crate::core::queue::queue_item_source(&uri)
            != crate::core::queue::QueueItemSource::LocalFile
          {
            return true;
          }
          self
            .emit(crate::runtime::engine::EngineEvent::Decoded(
              crate::runtime::engine::DecodedSourceFact::QueueNativeTrackPopped {
                track: track.clone(),
              },
            ))
            .await;
          let player = self
            .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
            .await
            .ok()
            .and_then(|answer| match answer {
              crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(player) => player,
              _ => None,
            });
          let player = match player {
            Some(player) => Some(player),
            None => tokio::task::spawn_blocking(LocalPlayer::new)
              .await
              .ok()
              .and_then(Result::ok)
              .map(Arc::new),
          };
          let Some(player) = player else { return true };
          if let Ok(crate::runtime::engine::FoldAnswer::Decoded(
            crate::runtime::engine::DecodedSourceAnswer::Volume(volume),
          )) = self
            .query(crate::runtime::engine::FoldQuery::Decoded(
              crate::runtime::engine::DecodedSourceQuery::Volume,
            ))
            .await
          {
            player.set_volume(volume as f32 / 100.0);
          }
          let fetch_id = QUEUE_FETCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let _ = self
            .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
              crate::runtime::engine::DecodedSourceFact::QueuePendingDecoded {
                player: Arc::clone(&player),
                track: track.clone(),
                fetch_id,
              },
            ))
            .await;
          if crate::infra::local::dispatch::play_single_file(&player, &uri)
            .await
            .is_ok()
          {
            let _ = self
              .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
                crate::runtime::engine::DecodedSourceFact::QueueDecodedLocalReady { fetch_id },
              ))
              .await;
          } else {
            let _ = self
              .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
                crate::runtime::engine::DecodedSourceFact::QueuePlaybackCleared,
              ))
              .await;
          }
          true
        }
        #[cfg(feature = "streaming")]
        IoEvent::AdvanceNativeQueue | IoEvent::NextTrack => {
          // In an all-feature build the native queue can contain every
          // queueable source. Dispatch by URI before the Spotify fast path;
          // otherwise this arm would consume decoded entries and report them
          // as unavailable.
          let source = self
            .query(crate::runtime::engine::FoldQuery::QueueTracks)
            .await
            .ok()
            .and_then(|answer| match answer {
              crate::runtime::engine::FoldAnswer::QueueTracks(tracks) => tracks
                .first()
                .and_then(|track| track.uri.as_deref())
                .map(crate::core::queue::queue_item_source),
              _ => None,
            });
          #[cfg(feature = "local-files")]
          if source == Some(crate::core::queue::QueueItemSource::LocalFile) {
            return play_local_queue(self).await;
          }
          #[cfg(feature = "subsonic")]
          if source == Some(crate::core::queue::QueueItemSource::Subsonic) {
            let spine = self.clone();
            tokio::spawn(async move {
              play_downloaded_queue(&spine, false).await;
            });
            return true;
          }
          #[cfg(feature = "youtube")]
          if source == Some(crate::core::queue::QueueItemSource::YouTube) {
            let spine = self.clone();
            tokio::spawn(async move {
              play_downloaded_queue(&spine, true).await;
            });
            return true;
          }
          let track = match self
            .query(crate::runtime::engine::FoldQuery::QueueTracks)
            .await
          {
            Ok(crate::runtime::engine::FoldAnswer::QueueTracks(mut tracks)) => {
              tracks.drain(..).next()
            }
            _ => None,
          };
          let Some(track) = track else {
            resume_suspended(self).await;
            return true;
          };
          let Some(uri) = track.uri.clone() else {
            let _ = self
              .emit_acked(crate::runtime::engine::EngineEvent::Session(
                crate::runtime::engine::SessionFact::TransientStatus {
                  message: "Skipped a queued track with no URI".to_string(),
                  ttl_secs: 4,
                },
              ))
              .await;
            return true;
          };
          let popped = self
            .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
              crate::runtime::engine::DecodedSourceFact::QueueNativeTrackPopped {
                track: track.clone(),
              },
            ))
            .await
            .and_then(|ack| ack.queue_track);
          let Some(track) = popped else {
            return true;
          };
          if crate::core::queue::queue_item_source(&uri)
            != crate::core::queue::QueueItemSource::Spotify
          {
            let _ = self
              .emit_acked(crate::runtime::engine::EngineEvent::Session(
                crate::runtime::engine::SessionFact::TransientStatus {
                  message: "Queued source isn't available in this build".to_string(),
                  ttl_secs: 4,
                },
              ))
              .await;
            return true;
          }
          let Ok(crate::runtime::engine::FoldAnswer::StreamingPlayer(Some(player))) = self
            .query(crate::runtime::engine::FoldQuery::StreamingPlayerHandle)
            .await
          else {
            return true;
          };
          player.activate();
          if player.play_uri(&uri).await.is_ok() {
            let _ = self
              .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
                crate::runtime::engine::DecodedSourceFact::QueueSpotifyPublished(track),
              ))
              .await;
          }
          true
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
        _ => true,
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

#[cfg(any(feature = "subsonic", feature = "youtube"))]
async fn play_downloaded_queue(spine: &crate::runtime::engine::Spine, youtube: bool) -> bool {
  let track = match spine
    .query(crate::runtime::engine::FoldQuery::QueueTracks)
    .await
  {
    Ok(crate::runtime::engine::FoldAnswer::QueueTracks(mut tracks)) => tracks.drain(..).next(),
    _ => None,
  };
  let Some(track) = track else { return true };
  let Some(uri) = track.uri.clone() else {
    return true;
  };
  let expected = if youtube {
    crate::core::queue::QueueItemSource::YouTube
  } else {
    crate::core::queue::QueueItemSource::Subsonic
  };
  if crate::core::queue::queue_item_source(&uri) != expected {
    return true;
  }
  spine
    .emit(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueNativeTrackPopped {
        track: track.clone(),
      },
    ))
    .await;
  let player = spine
    .query(crate::runtime::engine::FoldQuery::QueueDecodedPlayer)
    .await
    .ok()
    .and_then(|answer| match answer {
      crate::runtime::engine::FoldAnswer::QueueDecodedPlayer(player) => player,
      _ => None,
    });
  let player = match player {
    Some(player) => Some(player),
    None => tokio::task::spawn_blocking(LocalPlayer::new)
      .await
      .ok()
      .and_then(Result::ok)
      .map(Arc::new),
  };
  let Some(player) = player else { return true };
  let fetch_id = QUEUE_FETCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
  spine
    .emit(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueuePendingDecoded {
        player,
        track,
        fetch_id,
      },
    ))
    .await;
  let result = if youtube {
    #[cfg(feature = "youtube")]
    {
      let path = match spine
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
      return true;
    }
  } else {
    #[cfg(feature = "subsonic")]
    {
      let Some(source) = crate::infra::subsonic::dispatch::build_source(spine).await else {
        return true;
      };
      crate::infra::subsonic::dispatch::download_for_queue(&source, &uri).await
    }
    #[cfg(not(feature = "subsonic"))]
    {
      return true;
    }
  };
  finish_downloaded_queue(spine, fetch_id, result).await;
  true
}

#[cfg(any(feature = "subsonic", feature = "youtube"))]
async fn finish_downloaded_queue(
  spine: &crate::runtime::engine::Spine,
  fetch_id: u64,
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
  let Some(claim) = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Decoded(
      crate::runtime::engine::DecodedSourceFact::QueueFetchClaimed { fetch_id },
    ))
    .await
    .and_then(|ack| ack.queue_fetch_claim)
  else {
    return;
  };
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
