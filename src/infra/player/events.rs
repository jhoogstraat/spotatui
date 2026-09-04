use crate::core::app::{self, NativeTrackKind};
use crate::core::config::ClientConfig;
use crate::infra::network::IoEvent;
use crate::infra::player::{
  get_default_cache_path, PlayerEvent, SessionDisconnectReason, StreamingConfig,
  StreamingConnectionState, StreamingPlayer,
};
use log::{info, warn};
use std::sync::{
  atomic::{AtomicBool, AtomicU64, Ordering},
  Arc,
};
use std::time::{Duration, Instant};

const STALLED_PLAYBACK_RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const END_OF_TRACK_CONTINUATION_DELAY: Duration = Duration::from_millis(500);
const END_OF_TRACK_RECONNECT_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionDisconnectRecovery {
  Stop,
  RebuildIdle,
  RestorePlayback,
}

fn session_disconnect_recovery(reason: SessionDisconnectReason) -> SessionDisconnectRecovery {
  match reason {
    SessionDisconnectReason::ExternalDeviceHandoff => SessionDisconnectRecovery::RebuildIdle,
    SessionDisconnectReason::UnexpectedShutdown => SessionDisconnectRecovery::RestorePlayback,
    SessionDisconnectReason::LocalCommand => SessionDisconnectRecovery::Stop,
  }
}

#[derive(Clone, Default)]
pub struct StreamingRecoveryRequest {
  /// Playback generation that requested recovery. A completion from an older
  /// generation must not replace or clear a newer native session.
  pub generation: u64,
  pub reselect_device: bool,
  pub restore_playback: bool,
  pub continue_after_track: Option<String>,
}

/// Restore intent alone decides the replay: an idle-app queue never matches
/// the native context (`reselect_device` stays false), and a handoff teardown
/// must not replay at all (#437).
#[cfg_attr(not(test), allow(dead_code))]
fn should_replay_published_queue_slot(
  request: &StreamingRecoveryRequest,
  queue_now_is_spotify: bool,
) -> bool {
  request.restore_playback && queue_now_is_spotify
}

#[cfg_attr(not(test), allow(dead_code))]
fn recovery_needs_native_selection(
  request: &StreamingRecoveryRequest,
  replay_queue_slot: bool,
) -> bool {
  request.reselect_device || replay_queue_slot
}

/// Bundled context for player event handling tasks.
/// Groups all shared state and managers needed by event handlers.
pub struct PlayerEventContext {
  pub player: Arc<StreamingPlayer>,
  pub shared_position: Arc<AtomicU64>,
  pub shared_is_playing: Arc<AtomicBool>,
  /// Typed streaming facts are emitted here (ticket 14); the fold task is
  /// their only consumer and the single writer of the mirrored fields.
  pub spine: crate::runtime::engine::Spine,
  pub recovery_tx: tokio::sync::mpsc::UnboundedSender<StreamingRecoveryRequest>,
  /// Runtime shutdown signal. End-of-track recovery continuations must stop
  /// before they can publish a late `EnsurePlaybackContinues` event.
  pub shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

pub struct StreamingRecoveryContext {
  pub shared_position: Arc<AtomicU64>,
  pub shared_is_playing: Arc<AtomicBool>,
  /// Typed streaming facts are emitted here (ticket 14): the recovery
  /// teardown publishes its full state reset as one `Teardown` event instead
  /// of writing ten fields under the lock.
  pub spine: crate::runtime::engine::Spine,
  pub recovery_rx: tokio::sync::mpsc::UnboundedReceiver<StreamingRecoveryRequest>,
  pub recovery_tx: tokio::sync::mpsc::UnboundedSender<StreamingRecoveryRequest>,
  pub client_config: ClientConfig,
  pub redirect_uri: String,
  /// Shutdown cancellation owned by the runtime. Recovery must not continue
  /// rebuilding a native session after the fold has begun quiescing.
  pub shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

pub fn spawn_streaming_recovery_handler(
  ctx: StreamingRecoveryContext,
) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    let mut shutdown_rx = ctx.shutdown_rx.clone();
    tokio::select! {
      _ = shutdown_rx.changed() => {}
      _ = handle_streaming_recovery(ctx) => {}
    }
  })
}

/// Intent ORs across coalesced transport-drop requests, but a handoff
/// (`restore_playback == false`) stickily vetoes restore/reselect: restoring
/// over the user's handoff would steal playback back (#437).
fn merge_recovery_requests(request: &mut StreamingRecoveryRequest, next: StreamingRecoveryRequest) {
  // A handoff is a user veto, not a generation-local hint. Preserve it when
  // a newer restore request supersedes the request carrying that veto.
  let handoff_vetoed = !request.restore_playback || !next.restore_playback;
  if request.generation != next.generation {
    *request = next;
    if handoff_vetoed {
      request.reselect_device = false;
      request.restore_playback = false;
      request.continue_after_track = None;
    }
    return;
  }
  if !request.restore_playback || !next.restore_playback {
    request.reselect_device = false;
    request.restore_playback = false;
    request.continue_after_track = None;
    return;
  }
  request.reselect_device |= next.reselect_device;
  if next.continue_after_track.is_some() {
    request.continue_after_track = next.continue_after_track;
  }
}

async fn handle_streaming_recovery(mut ctx: StreamingRecoveryContext) {
  while let Some(mut request) = ctx.recovery_rx.recv().await {
    while let Ok(next_request) = ctx.recovery_rx.try_recv() {
      merge_recovery_requests(&mut request, next_request);
    }

    let active_player = match ctx
      .spine
      .query(crate::runtime::engine::FoldQuery::StreamingPlayerHandle)
      .await
      .ok()
    {
      Some(crate::runtime::engine::FoldAnswer::StreamingPlayer(player)) => player,
      _ => None,
    };
    if active_player.as_ref().is_some_and(|p| p.is_available()) {
      // A live player already exists (e.g. a queued duplicate request): the
      // pending window is over, so replay anything parked against it.
      info!(
        "recovery over a live player: reselect={} continue_after={:?}",
        request.reselect_device, request.continue_after_track
      );
      let _ = ctx
        .spine
        .emit_acked(crate::runtime::engine::EngineEvent::Native(
          crate::runtime::engine::NativePlaybackFact::RecoveryRoute {
            player: active_player.expect("active player checked above"),
            request,
            device_name: ctx.client_config.streaming_device_name.clone(),
          },
        ))
        .await;
      continue;
    }

    let initial_volume = match ctx
      .spine
      .query(crate::runtime::engine::FoldQuery::NativeRuntimeVolume)
      .await
      .ok()
    {
      Some(crate::runtime::engine::FoldAnswer::NativeRuntimeVolume(volume)) => volume,
      _ => return,
    };

    let streaming_config = StreamingConfig {
      device_name: ctx.client_config.streaming_device_name.clone(),
      bitrate: ctx.client_config.streaming_bitrate,
      audio_cache: ctx.client_config.streaming_audio_cache,
      cache_path: get_default_cache_path(),
      initial_volume,
    };

    info!("attempting native streaming recovery");

    match StreamingPlayer::new_cache_only(
      &ctx.client_config.client_id,
      &ctx.redirect_uri,
      streaming_config,
    )
    .await
    {
      Ok(recovered_player) => {
        let recovered_player = Arc::new(recovered_player);
        let installed = ctx
          .spine
          .emit_acked(crate::runtime::engine::EngineEvent::Native(
            crate::runtime::engine::NativePlaybackFact::Install {
              player: Arc::clone(&recovered_player),
              generation: Some(request.generation),
            },
          ))
          .await
          .and_then(|ack| ack.native)
          .and_then(|ack| match ack {
            crate::runtime::engine::NativePlaybackAck::Installed { old, accepted } => {
              Some((old, accepted))
            }
            _ => None,
          });
        let Some((old, accepted)) = installed else {
          recovered_player.shutdown();
          continue;
        };
        if !accepted {
          continue;
        }
        if let Some(old) = old {
          if !Arc::ptr_eq(&old, &recovered_player) {
            old.shutdown();
          }
        }

        spawn_player_event_handler(PlayerEventContext {
          player: Arc::clone(&recovered_player),
          shared_position: Arc::clone(&ctx.shared_position),
          shared_is_playing: Arc::clone(&ctx.shared_is_playing),
          spine: ctx.spine.clone(),
          recovery_tx: ctx.recovery_tx.clone(),
          shutdown_rx: ctx.shutdown_rx.clone(),
        });

        info!(
          "recovery rebuilt the backend: restore={} reselect={} continue_after={:?}",
          request.restore_playback, request.reselect_device, request.continue_after_track
        );
        let _ = ctx
          .spine
          .emit_acked(crate::runtime::engine::EngineEvent::Native(
            crate::runtime::engine::NativePlaybackFact::RecoveryRoute {
              player: Arc::clone(&recovered_player),
              request,
              device_name: ctx.client_config.streaming_device_name.clone(),
            },
          ))
          .await;
      }
      Err(e) => {
        info!("native streaming recovery failed: {}", e);
        let _ = ctx
          .spine
          .emit_acked(crate::runtime::engine::EngineEvent::Native(
            crate::runtime::engine::NativePlaybackFact::RecoveryFailed {
              generation: request.generation,
              error: e.to_string(),
            },
          ))
          .await;
      }
    }
  }
}

/// Get the currently active streaming player (if any).
#[allow(dead_code)]
pub async fn active_streaming_player(
  spine: &crate::runtime::engine::Spine,
) -> Option<Arc<StreamingPlayer>> {
  match spine
    .query(crate::runtime::engine::FoldQuery::StreamingPlayerHandle)
    .await
    .ok()?
  {
    crate::runtime::engine::FoldAnswer::StreamingPlayer(player) => {
      player.filter(|p| p.is_available())
    }
    _ => None,
  }
}

/// Handle one player event through the spine (ticket 14): every App-visible
/// fact becomes a typed [`StreamingStateEvent`]. The handler keeps only the
/// producer-local state it needs (watchdog timers, stale-write guards); all
/// shared-state writes go through `apply_streaming_state` in the fold.
async fn apply_streaming_facts(
  spine: &crate::runtime::engine::Spine,
  player: &Arc<StreamingPlayer>,
  facts: Vec<crate::core::streaming::StreamingStateEvent>,
) -> bool {
  let mut applied = true;
  for fact in facts {
    applied &= matches!(
      spine
        .emit_acked(crate::runtime::engine::EngineEvent::Native(
          crate::runtime::engine::NativePlaybackFact::State {
            player: Arc::clone(player),
            event: fact,
          },
        ))
        .await,
      Some(crate::runtime::engine::FoldAck {
        native: Some(crate::runtime::engine::NativePlaybackAck::Applied(true)),
        ..
      })
    );
  }
  // Recovery-observer side effects still need synchronous execution against
  // the pre-absorb state (queue continuation etc.) — none of those remain on
  // this path; everything rides the fold now. Keep the lock acquisition only
  // for actions that must dispatch IoEvents from event order (handled inside
  // `apply_streaming_state`).
  applied
}

pub fn spawn_player_event_handler(ctx: PlayerEventContext) -> tokio::task::JoinHandle<()> {
  let event_rx = ctx.player.get_event_channel();
  info!("spawning native player event handler");

  let player = ctx.player.clone();
  let shared_position = Arc::clone(&ctx.shared_position);
  let shared_is_playing = Arc::clone(&ctx.shared_is_playing);
  let spine = ctx.spine.clone();
  let recovery_tx = ctx.recovery_tx.clone();
  let shutdown_rx = ctx.shutdown_rx.clone();

  tokio::spawn(async move {
    handle_player_events(
      event_rx,
      player,
      shared_position,
      shared_is_playing,
      spine,
      recovery_tx,
      shutdown_rx,
    )
    .await;
  })
}

#[allow(clippy::too_many_arguments)]
/// Handle player events from librespot. Ticket 14: emits typed
/// [`StreamingStateEvent`]s on the spine instead of writing App fields; the
/// fold task is the single writer of the shared playback mirrors.
async fn handle_player_events(
  mut event_rx: librespot_playback::player::PlayerEventChannel,
  player: Arc<StreamingPlayer>,
  shared_position: Arc<AtomicU64>,
  shared_is_playing: Arc<AtomicBool>,
  spine: crate::runtime::engine::Spine,
  recovery_tx: tokio::sync::mpsc::UnboundedSender<StreamingRecoveryRequest>,
  mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
  // Count consecutive failed (Unavailable) loads so we can escalate the message
  // when an account is hit by the upstream Spotify audio-key block (#282). A
  // single genuinely-unavailable track only trips the mild message and resets on
  // the next successful Playing.
  let mut consecutive_unavailable: u32 = 0;
  const UNAVAILABLE_ESCALATION_THRESHOLD: u32 = 3;

  // The StreamingPlayer first reconnects its Session/Spirc in place, retaining
  // the Player and buffered audio. Only replace the whole backend when that
  // attempt fails or desired playback stops making progress.
  let mut connection_state_rx = player.connection_state_receiver();
  let mut session_lost = false;
  let mut fast_reconnect_failed = false;
  let mut progress_watchdog_armed = false;
  let mut transport_recovery_pending = false;
  let mut pending_end_of_track = None;
  let mut observed_connection_generation = 0_u64;
  let mut audibly_playing = shared_is_playing.load(Ordering::Relaxed);
  let mut last_position = shared_position.load(Ordering::Relaxed);
  let mut last_progress_at = Instant::now();
  // Recovery is a one-shot deadline, re-armed after each check. It never
  // creates a background heartbeat while the native session is healthy.
  let mut watchdog = Box::pin(tokio::time::sleep(Duration::from_secs(1)));
  let playback_transition_generation = Arc::new(AtomicU64::new(0));

  loop {
    let event = tokio::select! {
      maybe_event = event_rx.recv() => match maybe_event {
        Some(event) => event,
        None => break,
      },
      changed = connection_state_rx.changed() => {
        if changed.is_err() {
          return;
        }
        let connection_state = *connection_state_rx.borrow_and_update();
        match connection_state {
          StreamingConnectionState::Reconnecting { generation } => {
            observed_connection_generation = generation;
            session_lost = true;
            fast_reconnect_failed = false;
            progress_watchdog_armed = true;
            transport_recovery_pending = true;
            last_progress_at = Instant::now();
            info!("native streaming fast reconnect generation {} started", generation);
            let _ = spine.emit(crate::runtime::engine::EngineEvent::Native(crate::runtime::engine::NativePlaybackFact::Reconnect { player: Arc::clone(&player), connected: false })).await;
          }
          StreamingConnectionState::Connected { generation }
            if session_lost || generation != observed_connection_generation =>
          {
            observed_connection_generation = generation;
            session_lost = false;
            fast_reconnect_failed = false;
            progress_watchdog_armed = true;
            transport_recovery_pending = true;
            last_progress_at = Instant::now();
            info!("native streaming fast reconnect generation {} completed", generation);
            let _ = spine.emit(crate::runtime::engine::EngineEvent::Native(crate::runtime::engine::NativePlaybackFact::Reconnect { player: Arc::clone(&player), connected: true })).await;
          }
          StreamingConnectionState::Connected { .. } => {}
          StreamingConnectionState::Failed { generation } => {
            session_lost = true;
            fast_reconnect_failed = true;
            progress_watchdog_armed = true;
            transport_recovery_pending = true;
            info!("native streaming fast reconnect generation {} failed", generation);
            if audibly_playing {
              continue;
            }
            request_full_streaming_recovery(
              &player,
              &shared_position,
              &shared_is_playing,
              &spine,
              &recovery_tx,
              "Native streaming connection lost; attempting recovery.",
              pending_end_of_track.clone(),
            )
            .await;
            return;
          }
          StreamingConnectionState::Shutdown => return,
        }
        continue;
      }
      _ = &mut watchdog, if progress_watchdog_armed => {
        watchdog
          .as_mut()
          .reset(tokio::time::Instant::now() + Duration::from_secs(1));
        let position = shared_position.load(Ordering::Relaxed);
        if position != last_position {
          last_position = position;
          last_progress_at = Instant::now();
        }
        let desired_playing = match spine.query(crate::runtime::engine::FoldQuery::NativePlayerState { player: Arc::clone(&player) }).await {
          Ok(crate::runtime::engine::FoldAnswer::NativePlayerState(state)) => state.desired_playing,
          _ => shared_is_playing.load(Ordering::Relaxed),
        };
        if !desired_playing && !session_lost {
          progress_watchdog_armed = false;
          transport_recovery_pending = false;
          continue;
        }
        if recovery_watchdog_should_escalate(desired_playing, last_progress_at.elapsed()) {
          info!("native playback made no progress for {}s during session recovery; replacing backend", STALLED_PLAYBACK_RECOVERY_TIMEOUT.as_secs());
          request_full_streaming_recovery(
            &player,
            &shared_position,
            &shared_is_playing,
            &spine,
            &recovery_tx,
            "Native streaming stalled; attempting recovery.",
            pending_end_of_track.clone(),
          )
          .await;
          return;
        }
        continue;
      }
      changed = shutdown_rx.changed() => {
        if changed.is_err() || *shutdown_rx.borrow() {
          return;
        }
        continue;
      }
    };

    if !is_current_streaming_player(&spine, &player).await {
      continue;
    }

    match &event {
      PlayerEvent::Playing { .. } => {
        audibly_playing = true;
        pending_end_of_track = None;
        last_progress_at = Instant::now();
        if !session_lost {
          progress_watchdog_armed = false;
          transport_recovery_pending = false;
        }
      }
      PlayerEvent::Paused { .. } | PlayerEvent::Stopped { .. } | PlayerEvent::EndOfTrack { .. } => {
        audibly_playing = false;
      }
      PlayerEvent::Seeked { .. }
      | PlayerEvent::TrackChanged { .. }
      | PlayerEvent::Loading { .. } => {
        last_progress_at = Instant::now();
      }
      PlayerEvent::PositionChanged { position_ms, .. } => {
        let position = u64::from(*position_ms);
        if position != last_position {
          last_position = position;
          pending_end_of_track = None;
          last_progress_at = Instant::now();
          if !session_lost {
            progress_watchdog_armed = false;
            transport_recovery_pending = false;
          }
        }
      }
      _ => {}
    }

    match event {
      PlayerEvent::Playing {
        play_request_id: _,
        track_id,
        position_ms,
      } => {
        // While the native queue is mid-handoff or playing a *decoded* track,
        // librespot must stay paused. The handoff pauses Spirc, but a
        // self-advance load (or a stale-slot reissue) already in flight at that
        // moment can complete afterwards and start audio over the queue slot —
        // re-pause instead of accepting the state update. Librespot playing is
        // legitimate here only when the slot itself is a Spotify track; with a
        // decoded slot, or with a context suspended under the queue and no
        // Spotify slot (the between-items window: the old slot is cleared, the
        // next one not yet published), it never is. One-shot: a paused Spirc
        // emits no further Playing events, so this can't ping-pong.
        {
          let stray_over_queue = match spine
            .query(crate::runtime::engine::FoldQuery::NativePlayerState {
              player: Arc::clone(&player),
            })
            .await
          {
            Ok(crate::runtime::engine::FoldAnswer::NativePlayerState(state)) => {
              !state.queue_now_is_spotify && (state.decoded_queue_slot || state.queue_suspended)
            }
            _ => false,
          };
          if stray_over_queue {
            player.pause();
            continue;
          }
        }

        // Playback is actually working: reset the failure streak.
        consecutive_unavailable = 0;
        let track_uri = track_id.to_string();

        // Reaction half (synchronous, under one lock): recovery confirmation,
        // parked-request drop, identity refresh dispatch, stop-after-current.
        // State-fact half (typed event): transport flag + progress + context
        // progress land via the fold task instead of bare field writes.
        let current = matches!(
          spine
            .emit_acked(crate::runtime::engine::EngineEvent::Native(
              crate::runtime::engine::NativePlaybackFact::Playing {
                player: Arc::clone(&player),
                track_uri: track_uri.clone(),
                position_ms,
              }
            ))
            .await,
          Some(crate::runtime::engine::FoldAck {
            native: Some(crate::runtime::engine::NativePlaybackAck::Playing { current: true }),
            ..
          })
        );
        if current {
          shared_is_playing.store(true, Ordering::Relaxed);
        }
      }
      PlayerEvent::Paused {
        play_request_id: _,
        track_id,
        position_ms,
      } => {
        let track_uri = track_id.to_string();

        // Reaction half: recovery confirmation in the paused state.
        let current = matches!(
          spine
            .emit_acked(crate::runtime::engine::EngineEvent::Native(
              crate::runtime::engine::NativePlaybackFact::Paused {
                player: Arc::clone(&player),
                track_uri: track_uri.clone(),
                position_ms,
              }
            ))
            .await,
          Some(crate::runtime::engine::FoldAck {
            native: Some(crate::runtime::engine::NativePlaybackAck::Paused { current: true }),
            ..
          })
        );
        if current {
          shared_is_playing.store(false, Ordering::Relaxed);
        }
      }
      PlayerEvent::Seeked {
        play_request_id: _,
        track_id: _,
        position_ms,
      } => {
        let current = apply_streaming_facts(
          &spine,
          &player,
          vec![crate::core::streaming::StreamingStateEvent::Seeked { position_ms }],
        )
        .await;
        if !current {
          continue;
        }
        // State fact only: `seek_ms` clear + recovery-position stamp ride the
        // event through the fold's mirror write.
      }
      PlayerEvent::TrackChanged { audio_item } => {
        playback_transition_generation.fetch_add(1, Ordering::Relaxed);
        use librespot_metadata::audio::UniqueFields;

        let (artists, album, kind) = match &audio_item.unique_fields {
          UniqueFields::Track { artists, album, .. } => {
            let artist_names: Vec<String> = artists.0.iter().map(|a| a.name.clone()).collect();
            (artist_names, album.clone(), NativeTrackKind::Track)
          }
          UniqueFields::Episode { show_name, .. } => (
            vec![show_name.clone()],
            String::new(),
            NativeTrackKind::Episode,
          ),
          UniqueFields::Local { artists, album, .. } => {
            let artist_vec = artists
              .as_ref()
              .map(|a| vec![a.clone()])
              .unwrap_or_default();
            let album_str = album.clone().unwrap_or_default();
            (artist_vec, album_str, NativeTrackKind::Track)
          }
        };

        // Reaction half (synchronous): watchdog disarm, restore observation,
        // shuffle-index sync, identity-driven dispatches, spirc self-advance
        // guard. State-fact half: metadata + progress reset via the fold.
        // librespot 0.8's `SpotifyUri` Display is the full `spotify:track:<id>`
        // URI; app-side ids (shuffle index sync, the queue guard,
        // `last_track_id`) are bare base62, so normalize at the event boundary.
        // The restore observer keeps the full URI (it canonicalizes internally).
        let playing_uri = audio_item.track_id.to_string();
        let decision = spine
          .emit_acked(crate::runtime::engine::EngineEvent::Native(
            crate::runtime::engine::NativePlaybackFact::TrackChanged {
              player: Arc::clone(&player),
              track_uri: playing_uri.clone(),
              info: crate::core::app::NativeTrackInfo {
                name: audio_item.name.clone(),
                artists: artists.clone(),
                album: album.clone(),
                duration_ms: audio_item.duration_ms,
                kind,
                image_url: audio_item
                  .covers
                  .first()
                  .map(|c| c.url.clone())
                  .filter(|u| !u.is_empty()),
              },
              previous_track_id: pending_end_of_track.clone(),
            },
          ))
          .await;
        let (_current, reload_uri, transition_advanced) = match decision.and_then(|a| a.native) {
          Some(crate::runtime::engine::NativePlaybackAck::TrackChanged {
            current,
            reload_uri,
            transition_advanced,
          }) => (current, reload_uri, transition_advanced),
          _ => (false, None, false),
        };
        if transition_advanced {
          pending_end_of_track = None;
        }
        if let Some((uri, desired)) = reload_uri {
          info!("spirc advanced off the queued track; reissuing {}", uri);
          reissue_published_queue_slot(&player, &uri, desired).await;
        }
      }
      PlayerEvent::Stopped { .. } => {
        let current = apply_streaming_facts(
          &spine,
          &player,
          vec![crate::core::streaming::StreamingStateEvent::Stopped],
        )
        .await;
        if !current {
          continue;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        spine
          .emit(crate::runtime::engine::EngineEvent::Session(
            crate::runtime::engine::SessionFact::DispatchIo(vec![IoEvent::GetCurrentPlayback]),
          ))
          .await;
      }
      PlayerEvent::EndOfTrack { track_id, .. } => {
        // Recovery and queue continuation compare against the bare Web API
        // id, while librespot formats this event as a full Spotify URI.
        let ended_uri = track_id.to_string();
        let previous_track_id = app::base62_id_of(&ended_uri).to_string();

        // The fold answers this atomic decision before the progress fact lands;
        // dropping it would strand the queue (nothing advances, nothing continues).
        //
        // The context/progress/metadata clears that used to sit at the top of
        // this block are the Stopped-shaped facts the fold now receives via
        // `EndOfTrack { reset_progress: true }` (below); the decision logic
        // itself stays synchronous because it must observe pre-clear state.
        let decision = spine
          .emit_acked(crate::runtime::engine::EngineEvent::Native(
            crate::runtime::engine::NativePlaybackFact::EndOfTrack {
              player: Arc::clone(&player),
              previous_track_id: previous_track_id.clone(),
            },
          ))
          .await;
        let (_current, should_ensure_playback, reissue_queue_slot) =
          match decision.and_then(|a| a.native) {
            Some(crate::runtime::engine::NativePlaybackAck::EndOfTrack {
              current,
              should_ensure_playback,
              reissue_queue_slot,
              ..
            }) => (current, should_ensure_playback, reissue_queue_slot),
            _ => (false, false, None),
          };
        if let Some((uri, desired_playing)) = reissue_queue_slot {
          info!(
            "queue slot did not start before the previous track ended; reissuing {}",
            uri
          );
          reissue_published_queue_slot(&player, &uri, desired_playing).await;
        }

        if should_ensure_playback {
          pending_end_of_track = Some(previous_track_id.clone());
          progress_watchdog_armed = true;
          last_progress_at = Instant::now();
          spawn_end_of_track_continuation(
            spine.clone(),
            Arc::clone(&player),
            previous_track_id,
            Arc::clone(&playback_transition_generation),
            shutdown_rx.clone(),
          );
        }
      }
      PlayerEvent::VolumeChanged { volume } => {
        let volume_percent = ((volume as f64 / 65535.0) * 100.0).round() as u8;
        let current = apply_streaming_facts(
          &spine,
          &player,
          vec![crate::core::streaming::StreamingStateEvent::VolumeChanged { volume_percent }],
        )
        .await;
        if !current {
          continue;
        }
      }
      PlayerEvent::PositionChanged {
        play_request_id: _,
        track_id: _,
        position_ms,
      } => {
        let current = apply_streaming_facts(
          &spine,
          &player,
          vec![crate::core::streaming::StreamingStateEvent::PositionChanged { position_ms }],
        )
        .await;
        if !current {
          continue;
        }
        shared_position.store(position_ms as u64, Ordering::Relaxed);
      }
      PlayerEvent::SessionDisconnected { reason, .. } => {
        let recovery = session_disconnect_recovery(reason);
        let (status_message, restore_playback) = match recovery {
          SessionDisconnectRecovery::Stop => ("Native streaming stopped.", false),
          SessionDisconnectRecovery::RebuildIdle => {
            ("Playback moved to another Spotify device.", false)
          }
          SessionDisconnectRecovery::RestorePlayback => {
            ("Native streaming disconnected; attempting recovery.", true)
          }
        };
        if let Some(request) = disconnect_streaming_player(
          &spine,
          &player,
          &shared_position,
          &shared_is_playing,
          status_message,
          restore_playback,
        )
        .await
        {
          if recovery == SessionDisconnectRecovery::Stop {
            // No rebuild follows a Stop; release the pending latch so later
            // requests can't park forever. (Presently unreachable: local
            // shutdowns remove the player before this event, failing the
            // ptr_eq guard above.)
            let _ = spine
              .emit(crate::runtime::engine::EngineEvent::Native(
                crate::runtime::engine::NativePlaybackFact::Reconnect {
                  player: Arc::clone(&player),
                  connected: true,
                },
              ))
              .await;
          } else {
            let _ = recovery_tx.send(request);
          }
        }
        return;
      }
      PlayerEvent::Unavailable { track_id, .. } => {
        if unavailable_is_transport_related(transport_recovery_pending, player.connection_state()) {
          info!(
            "native playback unavailable for track {} while the session transport is down; deferring to connection recovery",
            track_id
          );
          continue;
        }

        // librespot emits Unavailable when a track can't be loaded — including
        // when Spotify rejects the audio key (`error audio key 0 1`), which makes
        // decryption fail. This was previously dropped by the `_` arm, so the
        // failure was completely silent (#282). Surface it to the user.
        consecutive_unavailable += 1;
        last_progress_at = Instant::now();

        // Clear the ghost native track and the request/watchdog together so an
        // unavailable track cannot be replayed as a supposed session failure.
        // Reaction half keeps the request/watchdog drops; the progress and
        // metadata clears become the fold's `Stopped` fact (identical mirror
        // effect, no separate Unavailable variant needed).
        let current = matches!(
          spine
            .emit_acked(crate::runtime::engine::EngineEvent::Native(
              crate::runtime::engine::NativePlaybackFact::Unavailable {
                player: Arc::clone(&player),
              },
            ))
            .await,
          Some(crate::runtime::engine::FoldAck {
            native: Some(crate::runtime::engine::NativePlaybackAck::Applied(true)),
            ..
          })
        );
        if !current {
          continue;
        }

        info!(
          "native playback unavailable (track {}, consecutive {})",
          track_id, consecutive_unavailable
        );

        // Once several consecutive tracks fail to load (genuinely unavailable,
        // e.g. region-locked or the audio-key block), halt playback instead of
        // letting librespot auto-skip through the entire queue at machine speed;
        // that stampede hammers Spotify and can get the account rate-limited.
        if consecutive_unavailable >= UNAVAILABLE_ESCALATION_THRESHOLD {
          player.pause();
          progress_watchdog_armed = false;
          pending_end_of_track = None;
        }

        // Emit on the threshold transitions only (== not >=) so we don't spam the
        // same message on every auto-skip during an account-wide failure.
        if consecutive_unavailable == 1 {
          spine
            .emit(crate::runtime::engine::EngineEvent::Session(
              crate::runtime::engine::SessionFact::Status {
                message: "Couldn't play this track natively (unavailable or blocked); skipping."
                  .to_string(),
                ttl_secs: 6,
                is_error: false,
              },
            ))
            .await;
        } else if consecutive_unavailable == UNAVAILABLE_ESCALATION_THRESHOLD {
          spine.emit(crate::runtime::engine::EngineEvent::Session(crate::runtime::engine::SessionFact::Status { message: "Several tracks in a row couldn't be played natively, so playback was stopped. They may be unavailable on your account or region. Press 'd' to switch to an official Spotify Connect device.".to_string(), ttl_secs: 20, is_error: false })).await;
        }
      }
      PlayerEvent::Loading {
        track_id,
        position_ms,
        ..
      } => {
        // Reaction half: watchdog re-arm stays synchronous. State-fact half:
        // the restore-observer half of Loading rides the event through the
        // fold (same stale-load rejection semantics).
        let current = matches!(
          spine
            .emit_acked(crate::runtime::engine::EngineEvent::Native(
              crate::runtime::engine::NativePlaybackFact::Loading {
                player: Arc::clone(&player),
                track_uri: track_id.to_string(),
                position_ms,
              },
            ))
            .await,
          Some(crate::runtime::engine::FoldAck {
            native: Some(crate::runtime::engine::NativePlaybackAck::Applied(true)),
            ..
          })
        );
        if !current {
          continue;
        }
      }
      PlayerEvent::Preloading { .. } => {}
      _ => {}
    }

    // A failed in-place reconnect owns no viable Spirc. Runs after event
    // bookkeeping so EndOfTrack still records queue/stop-after-current state.
    if session_lost && fast_reconnect_failed && !audibly_playing {
      info!("buffered playback ended after fast reconnect failure; replacing backend");
      request_full_streaming_recovery(
        &player,
        &shared_position,
        &shared_is_playing,
        &spine,
        &recovery_tx,
        "Native streaming connection lost; attempting recovery.",
        pending_end_of_track.clone(),
      )
      .await;
      return;
    }
  }

  if matches!(
    player.connection_state(),
    StreamingConnectionState::Shutdown
  ) {
    return;
  }

  request_full_streaming_recovery(
    &player,
    &shared_position,
    &shared_is_playing,
    &spine,
    &recovery_tx,
    "Native streaming stopped; attempting recovery.",
    pending_end_of_track,
  )
  .await;
}

fn recovery_watchdog_should_escalate(desired_playing: bool, stalled_for: Duration) -> bool {
  desired_playing && stalled_for >= STALLED_PLAYBACK_RECOVERY_TIMEOUT
}

/// Reissue a published Spotify queue slot only when it should be playing. A
/// paused slot stays logical-only; explicit resume owns the next load/play.
async fn reissue_published_queue_slot(
  player: &Arc<StreamingPlayer>,
  uri: &str,
  desired_playing: bool,
) {
  if desired_playing {
    if let Err(e) = player.play_uri(uri).await {
      info!("failed to reissue queued Spotify track: {}", e);
    }
  }
}

fn unavailable_is_transport_related(
  transport_recovery_pending: bool,
  connection_state: StreamingConnectionState,
) -> bool {
  transport_recovery_pending
    || !matches!(connection_state, StreamingConnectionState::Connected { .. })
}

#[allow(clippy::too_many_arguments)]
async fn request_full_streaming_recovery(
  player: &Arc<StreamingPlayer>,
  shared_position: &Arc<AtomicU64>,
  shared_is_playing: &Arc<AtomicBool>,
  spine: &crate::runtime::engine::Spine,
  recovery_tx: &tokio::sync::mpsc::UnboundedSender<StreamingRecoveryRequest>,
  status_message: &str,
  continue_after_track: Option<String>,
) {
  if let Some(mut request) = disconnect_streaming_player(
    spine,
    player,
    shared_position,
    shared_is_playing,
    status_message,
    true,
  )
  .await
  {
    request.continue_after_track = continue_after_track.or(request.continue_after_track);
    let _ = recovery_tx.send(request);
  }
}

fn spawn_end_of_track_continuation(
  spine: crate::runtime::engine::Spine,
  player: Arc<StreamingPlayer>,
  previous_track_id: String,
  playback_transition_generation: Arc<AtomicU64>,
  mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
  let observed_transition_generation = playback_transition_generation.load(Ordering::Relaxed);
  let mut connection_state_rx = player.connection_state_receiver();

  tokio::spawn(async move {
    if *shutdown_rx.borrow() {
      return;
    }
    let wait_for_stable_connection = async {
      loop {
        let connection_state = *connection_state_rx.borrow_and_update();
        match connection_state {
          StreamingConnectionState::Connected { generation } => {
            tokio::time::sleep(END_OF_TRACK_CONTINUATION_DELAY).await;
            if matches!(
              *connection_state_rx.borrow(),
              StreamingConnectionState::Connected {
                generation: current_generation
              } if current_generation == generation
            ) {
              return true;
            }
          }
          StreamingConnectionState::Reconnecting { .. } => {
            if connection_state_rx.changed().await.is_err() {
              return false;
            }
          }
          StreamingConnectionState::Failed { .. } | StreamingConnectionState::Shutdown => {
            return false;
          }
        }
      }
    };

    let stable = tokio::select! {
      _ = shutdown_rx.changed() => false,
      stable = tokio::time::timeout(END_OF_TRACK_RECONNECT_TIMEOUT, wait_for_stable_connection) => stable.unwrap_or(false),
    };
    if !stable {
      warn!(
        "end-of-track continuation for {} abandoned: no stable connection within {}s",
        previous_track_id,
        END_OF_TRACK_RECONNECT_TIMEOUT.as_secs()
      );
      return;
    }

    // TrackChanged is proof that Spirc already advanced. The fallback must only
    // run when no transition was observed, otherwise it skips twice. Loading
    // alone is not sufficient: a dead connection can stall after that event.
    if playback_transition_generation.load(Ordering::Relaxed) != observed_transition_generation
      || !is_current_streaming_player(&spine, &player).await
      || *shutdown_rx.borrow()
    {
      return;
    }

    spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::DispatchIo(vec![IoEvent::EnsurePlaybackContinues(
          previous_track_id,
        )]),
      ))
      .await;
  });
}

async fn is_current_streaming_player(
  spine: &crate::runtime::engine::Spine,
  player: &Arc<StreamingPlayer>,
) -> bool {
  matches!(spine.query(crate::runtime::engine::FoldQuery::NativePlayerState { player: Arc::clone(player) }).await, Ok(crate::runtime::engine::FoldAnswer::NativePlayerState(state)) if state.current)
}

async fn disconnect_streaming_player(
  spine: &crate::runtime::engine::Spine,
  player: &Arc<StreamingPlayer>,
  shared_position: &Arc<AtomicU64>,
  shared_is_playing: &Arc<AtomicBool>,
  status_message: &str,
  restore_playback: bool,
) -> Option<StreamingRecoveryRequest> {
  let answer = spine
    .emit_acked(crate::runtime::engine::EngineEvent::Native(
      crate::runtime::engine::NativePlaybackFact::Disconnect {
        player: Arc::clone(player),
        position_ms: u32::try_from(shared_position.load(Ordering::Relaxed)).unwrap_or(u32::MAX),
        is_playing: shared_is_playing.load(Ordering::Relaxed),
        status_message: status_message.to_string(),
        restore_playback,
      },
    ))
    .await?;

  match answer.native {
    Some(crate::runtime::engine::NativePlaybackAck::Disconnect(request)) => {
      if request.is_some() {
        player.shutdown();
        shared_position.store(0, Ordering::Relaxed);
        shared_is_playing.store(false, Ordering::Relaxed);
      }
      request
    }
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use super::{
    merge_recovery_requests, recovery_needs_native_selection, recovery_watchdog_should_escalate,
    session_disconnect_recovery, should_replay_published_queue_slot,
    unavailable_is_transport_related, SessionDisconnectReason, SessionDisconnectRecovery,
    StreamingConnectionState, StreamingRecoveryRequest, STALLED_PLAYBACK_RECOVERY_TIMEOUT,
  };
  use std::time::Duration;

  #[test]
  fn recovery_watchdog_waits_for_the_full_stall_window() {
    assert!(!recovery_watchdog_should_escalate(
      true,
      STALLED_PLAYBACK_RECOVERY_TIMEOUT - Duration::from_millis(1)
    ));
    assert!(recovery_watchdog_should_escalate(
      true,
      STALLED_PLAYBACK_RECOVERY_TIMEOUT
    ));
  }

  #[test]
  fn recovery_watchdog_never_rebuilds_explicitly_paused_playback() {
    assert!(!recovery_watchdog_should_escalate(
      false,
      STALLED_PLAYBACK_RECOVERY_TIMEOUT * 2
    ));
  }

  #[test]
  fn unavailable_is_transport_related_until_recovery_progress_is_verified() {
    assert!(unavailable_is_transport_related(
      true,
      StreamingConnectionState::Connected { generation: 1 }
    ));
    assert!(unavailable_is_transport_related(
      false,
      StreamingConnectionState::Reconnecting { generation: 2 }
    ));
    assert!(!unavailable_is_transport_related(
      false,
      StreamingConnectionState::Connected { generation: 2 }
    ));
  }

  #[test]
  fn session_disconnect_recovery_respects_device_handoffs_and_local_shutdowns() {
    assert_eq!(
      session_disconnect_recovery(SessionDisconnectReason::ExternalDeviceHandoff),
      SessionDisconnectRecovery::RebuildIdle
    );
    assert_eq!(
      session_disconnect_recovery(SessionDisconnectReason::UnexpectedShutdown),
      SessionDisconnectRecovery::RestorePlayback
    );
    assert_eq!(
      session_disconnect_recovery(SessionDisconnectReason::LocalCommand),
      SessionDisconnectRecovery::Stop
    );
  }

  #[test]
  fn queue_slot_replay_follows_restore_intent_not_reselect() {
    // A queue playing over an idle app recovers with restore intent but no
    // native context match (reselect stays false); the slot must still replay.
    let idle_app_restore = StreamingRecoveryRequest {
      restore_playback: true,
      ..StreamingRecoveryRequest::default()
    };
    assert!(should_replay_published_queue_slot(&idle_app_restore, true));
    assert!(!should_replay_published_queue_slot(
      &idle_app_restore,
      false
    ));

    // A handoff rebuild never replays, regardless of reselect.
    let handoff = StreamingRecoveryRequest::default();
    assert!(!should_replay_published_queue_slot(&handoff, true));
    let handoff_with_reselect = StreamingRecoveryRequest {
      reselect_device: true,
      ..StreamingRecoveryRequest::default()
    };
    assert!(!should_replay_published_queue_slot(
      &handoff_with_reselect,
      true
    ));
  }

  #[test]
  fn idle_app_queue_recovery_still_checks_device_ownership_before_replay() {
    let idle_app_restore = StreamingRecoveryRequest {
      restore_playback: true,
      ..StreamingRecoveryRequest::default()
    };

    assert!(recovery_needs_native_selection(&idle_app_restore, true));
    assert!(!recovery_needs_native_selection(&idle_app_restore, false));
  }

  #[test]
  fn recovery_request_merge_ors_intent_across_transport_drops() {
    let mut request = StreamingRecoveryRequest {
      restore_playback: true,
      continue_after_track: Some("older".to_string()),
      ..StreamingRecoveryRequest::default()
    };
    merge_recovery_requests(
      &mut request,
      StreamingRecoveryRequest {
        generation: 0,
        reselect_device: true,
        restore_playback: true,
        continue_after_track: None,
      },
    );
    assert!(request.reselect_device);
    assert!(request.restore_playback);
    // The newest non-None continuation wins; None does not erase it.
    assert_eq!(request.continue_after_track.as_deref(), Some("older"));

    merge_recovery_requests(
      &mut request,
      StreamingRecoveryRequest {
        generation: 0,
        reselect_device: false,
        restore_playback: true,
        continue_after_track: Some("newer".to_string()),
      },
    );
    assert!(request.reselect_device);
    assert_eq!(request.continue_after_track.as_deref(), Some("newer"));
  }

  #[test]
  fn recovery_request_merge_handoff_vetoes_restore_intent() {
    let restore = StreamingRecoveryRequest {
      generation: 0,
      reselect_device: true,
      restore_playback: true,
      continue_after_track: Some("track".to_string()),
    };
    let handoff = StreamingRecoveryRequest::default();

    // Handoff queued behind a restore request.
    let mut request = restore.clone();
    merge_recovery_requests(&mut request, handoff.clone());
    assert!(!request.reselect_device);
    assert!(!request.restore_playback);
    assert!(request.continue_after_track.is_none());

    // Restore queued behind a handoff request.
    let mut request = handoff;
    merge_recovery_requests(&mut request, restore.clone());
    assert!(!request.reselect_device);
    assert!(!request.restore_playback);
    assert!(request.continue_after_track.is_none());

    // The veto is sticky: a later restore request cannot resurrect intent.
    merge_recovery_requests(&mut request, restore);
    assert!(!request.reselect_device);
    assert!(!request.restore_playback);
    assert!(request.continue_after_track.is_none());
  }

  #[test]
  fn recovery_request_merge_keeps_handoff_veto_across_generations() {
    let mut request = StreamingRecoveryRequest {
      generation: 2,
      restore_playback: false,
      ..StreamingRecoveryRequest::default()
    };
    merge_recovery_requests(
      &mut request,
      StreamingRecoveryRequest {
        generation: 3,
        restore_playback: true,
        reselect_device: true,
        continue_after_track: Some("track".to_string()),
      },
    );
    assert_eq!(request.generation, 3);
    assert!(!request.restore_playback);
    assert!(!request.reselect_device);
    assert!(request.continue_after_track.is_none());
  }
}
