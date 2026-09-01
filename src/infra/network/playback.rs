use super::{IoEvent, Network};
use crate::core::app::App;
#[cfg(feature = "streaming")]
use crate::core::app::{NativePlaybackOrigin, NativePlaybackRecoverySnapshot};
#[cfg(feature = "streaming")]
use crate::infra::player::{select_native, PlaybackBackend};
#[cfg(feature = "streaming")]
use crate::runtime::engine::NativeDevicePreferenceUpdate;
use crate::runtime::engine::{FoldAnswer, FoldQuery, SessionFact};
use anyhow::anyhow;
use chrono::TimeDelta;
use log::{info, warn};
use reqwest::Method;
#[cfg(feature = "streaming")]
use rspotify::model::device::DevicePayload;
use rspotify::model::{
  context::CurrentUserQueue,
  enums::RepeatState,
  idtypes::{PlayContextId, PlayableId},
  PlayableItem,
};
use rspotify::prelude::*;
use serde_json::{json, Value};
#[cfg(feature = "streaming")]
use std::time::Duration;
use std::time::Instant;

#[cfg(feature = "streaming")]
use librespot_connect::{
  LoadContextOptions, LoadRequest, LoadRequestOptions, Options as LoadOptions, PlayingTrack,
};
#[cfg(feature = "streaming")]
use std::sync::Arc;

const MAX_API_PLAYBACK_URIS: usize = 100;

/// Playback polls that must fail 401 back-to-back before the full-screen error
/// route is shown. Below the threshold the poll simply tries again on its next
/// tick, which is what actually recovers the intermittent player-service 401s
/// of issue #395 (users were pressing Esc and finding playback untouched).
const MAX_CONSECUTIVE_PLAYBACK_AUTH_FAILURES: u8 = 4;

#[cfg(feature = "streaming")]
const MAX_NATIVE_IDLE_RECOVERY_ATTEMPTS: u8 = 2;

#[cfg(feature = "streaming")]
const NATIVE_IDLE_RECOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub trait PlaybackNetwork {
  async fn get_current_playback(&mut self);
  async fn start_playback(
    &mut self,
    context_id: Option<PlayContextId<'static>>,
    uris: Option<Vec<PlayableId<'static>>>,
    offset: Option<usize>,
  );
  #[cfg(feature = "streaming")]
  async fn restore_native_playback(&mut self, generation: u64);
  async fn pause_playback(&mut self);
  async fn next_track(&mut self);
  async fn previous_track(&mut self);
  async fn force_previous_track(&mut self);
  async fn seek(&mut self, position_ms: u32);
  async fn shuffle(&mut self, shuffle_state: bool);
  async fn repeat(&mut self, repeat_state: RepeatState);
  async fn change_volume(&mut self, volume: u8);
  async fn transfert_playback_to_device(&mut self, device_id: String, persist_device_id: bool);
  #[cfg(feature = "streaming")]
  async fn auto_select_streaming_device(
    &mut self,
    device_name: String,
    persist_device_id: bool,
    yield_to_external_playback: bool,
  );
  async fn ensure_playback_continues(&mut self, previous_track_id: String);
  /// Resume a native-Spotify context suspended under the native queue, targeting
  /// the resume track via an offset URI. Falls back to playing just the track
  /// when there is no context, and to a "Queue finished" status when neither is
  /// known.
  async fn resume_spotify_context(
    &mut self,
    context_uri: Option<String>,
    resume_track_uri: Option<String>,
  );
  #[allow(dead_code)]
  async fn add_item_to_queue(&mut self, item: PlayableId<'static>);
  async fn get_queue(&mut self);
}

fn trim_api_playback_uris(
  track_uris: Vec<PlayableId<'static>>,
  offset: Option<usize>,
) -> (Vec<PlayableId<'static>>, Option<usize>) {
  if track_uris.len() <= MAX_API_PLAYBACK_URIS {
    return (track_uris, offset);
  }

  let selected_index = offset.unwrap_or(0).min(track_uris.len().saturating_sub(1));
  let preferred_history = MAX_API_PLAYBACK_URIS / 5;
  let mut start = selected_index.saturating_sub(preferred_history);
  let end = (start + MAX_API_PLAYBACK_URIS).min(track_uris.len());

  if end - start < MAX_API_PLAYBACK_URIS {
    start = end.saturating_sub(MAX_API_PLAYBACK_URIS);
  }

  // Spotify rejects oversized URI payloads, so URI-list playback is capped
  // to a window that still contains the selected track.
  let trimmed_uris = track_uris[start..end]
    .iter()
    .map(PlayableId::clone_static)
    .collect::<Vec<_>>();

  (trimmed_uris, Some(selected_index - start))
}

fn api_playback_offset_json(
  context_uris: Option<&[PlayableId<'static>]>,
  offset: Option<usize>,
) -> Option<Value> {
  if let Some(first_uri) = context_uris.and_then(|uris| uris.first()) {
    return Some(json!({ "uri": first_uri.uri() }));
  }

  offset.map(|index| json!({ "position": index }))
}

fn api_playback_body(
  context_id: Option<&PlayContextId<'static>>,
  uris: Option<&[PlayableId<'static>]>,
  offset: Option<usize>,
) -> Option<Value> {
  match (context_id, uris) {
    (Some(context), track_uris) => {
      let mut body = json!({ "context_uri": context.uri() });
      if let Some(offset) = api_playback_offset_json(track_uris, offset) {
        body["offset"] = offset;
      }
      Some(body)
    }
    (None, Some(track_uris)) => {
      let mut body = json!({
        "uris": track_uris.iter().map(|uri| uri.uri()).collect::<Vec<_>>()
      });
      if let Some(offset) = api_playback_offset_json(None, offset) {
        body["offset"] = offset;
      }
      Some(body)
    }
    (None, None) => None,
  }
}

#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
fn playable_item_id(item: &PlayableItem) -> Option<String> {
  match item {
    PlayableItem::Track(track) => track.id.as_ref().map(|id| id.id().to_string()),
    PlayableItem::Episode(episode) => Some(episode.id.id().to_string()),
    PlayableItem::Unknown(_) => None,
  }
}

#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
fn playable_item_name(item: &PlayableItem) -> Option<&str> {
  match item {
    PlayableItem::Track(track) => Some(&track.name),
    PlayableItem::Episode(episode) => Some(&episode.name),
    PlayableItem::Unknown(_) => None,
  }
}

fn spotify_item_identity(value: &str) -> &str {
  value.rsplit(':').next().unwrap_or(value)
}

#[cfg(feature = "streaming")]
#[derive(Debug, PartialEq, Eq)]
enum NativePlaybackRoute {
  ContextApi { device_id: String },
  NativeLoad,
}

#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug, Default)]
struct NativeActivationContext {
  player_connected: bool,
  current_device_id_present: bool,
  current_device_is_confirmed_native: bool,
  current_device_name_matches_native: bool,
  native_has_fresh_activity: bool,
  saved_device_matches_native: bool,
  saved_external_confirmed_available: bool,
}

#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum NativeIdleRecoveryPhase {
  #[default]
  Armed,
  Idle {
    attempts: u8,
    last_attempt: Instant,
  },
}

#[cfg(feature = "streaming")]
#[derive(Debug, Default)]
pub(super) struct NativeIdleRecoveryState {
  player_instance: Option<usize>,
  phase: NativeIdleRecoveryPhase,
}

#[cfg(feature = "streaming")]
impl NativeIdleRecoveryState {
  fn observe_player_instance(&mut self, player_instance: Option<usize>) {
    if self.player_instance != player_instance {
      self.player_instance = player_instance;
      self.phase = NativeIdleRecoveryPhase::Armed;
    }
  }

  fn should_attempt_idle_recovery(&mut self, now: Instant) -> bool {
    match self.phase {
      NativeIdleRecoveryPhase::Armed => {
        self.phase = NativeIdleRecoveryPhase::Idle {
          attempts: 1,
          last_attempt: now,
        };
        true
      }
      NativeIdleRecoveryPhase::Idle {
        attempts,
        last_attempt,
      } if attempts < MAX_NATIVE_IDLE_RECOVERY_ATTEMPTS
        && now.duration_since(last_attempt) >= NATIVE_IDLE_RECOVERY_RETRY_INTERVAL =>
      {
        self.phase = NativeIdleRecoveryPhase::Idle {
          attempts: attempts + 1,
          last_attempt: now,
        };
        true
      }
      NativeIdleRecoveryPhase::Idle { .. } => false,
    }
  }

  fn settle_current_episode(&mut self, now: Instant) {
    self.phase = NativeIdleRecoveryPhase::Idle {
      attempts: MAX_NATIVE_IDLE_RECOVERY_ATTEMPTS,
      last_attempt: now,
    };
  }
}

#[cfg(feature = "streaming")]
fn should_activate_native_for_playback(context: NativeActivationContext) -> bool {
  if !context.player_connected {
    return false;
  }

  let current_device_is_stale_native_name =
    context.current_device_name_matches_native && !context.native_has_fresh_activity;
  let current_device_is_usable_external = context.current_device_id_present
    && !context.current_device_is_confirmed_native
    && !current_device_is_stale_native_name;

  if current_device_is_usable_external {
    return false;
  }

  if context.saved_device_matches_native {
    return true;
  }

  !context.saved_external_confirmed_available
}

#[cfg(feature = "streaming")]
pub(crate) fn should_yield_native_recovery(
  yield_to_external_playback: bool,
  current_is_playing: bool,
  current_device_id: Option<&str>,
  native_device_id: &str,
) -> bool {
  yield_to_external_playback && current_is_playing && current_device_id != Some(native_device_id)
}

#[cfg(feature = "streaming")]
pub(crate) fn clear_native_recovery_intent_for_external_playback(app: &mut App) {
  app.is_streaming_active = false;
  app.native_activation_pending = false;
  app.last_device_activation = None;
  app.clear_native_playback_recovery();
  if app.queue_now_is_spotify() {
    app.queue_now = None;
    app.spotify_queue_guard_reloads = 0;
  }
}

#[cfg(feature = "streaming")]
pub(crate) fn native_device_preference_update(
  saved_device_id: Option<&str>,
  explicit_persist: bool,
  saved_device_matches_native: bool,
) -> NativeDevicePreferenceUpdate {
  if explicit_persist || saved_device_id.is_none() || saved_device_matches_native {
    NativeDevicePreferenceUpdate::Persist
  } else {
    NativeDevicePreferenceUpdate::KeepExistingPreference
  }
}

#[cfg(feature = "streaming")]
fn native_idle_device_preference_update(
  saved_device_id: Option<&str>,
  saved_device_matches_native: bool,
) -> Option<NativeDevicePreferenceUpdate> {
  let update = native_device_preference_update(saved_device_id, false, saved_device_matches_native);
  (update != NativeDevicePreferenceUpdate::KeepExistingPreference).then_some(update)
}

#[cfg(feature = "streaming")]
pub(crate) fn saved_device_matches_native_player(
  saved_device_id: Option<&str>,
  native_device_id: Option<&str>,
  devices: Option<&DevicePayload>,
  native_device_name: &str,
) -> bool {
  saved_device_id.is_some_and(|saved| {
    native_device_id == Some(saved)
      || devices.is_some_and(|payload| {
        payload.devices.iter().any(|device| {
          device.id.as_deref() == Some(saved)
            && device.name.eq_ignore_ascii_case(native_device_name)
        })
      })
  })
}

#[cfg(feature = "streaming")]
fn spotify_payload_confirms_native_device(payload: &DevicePayload, native_device_id: &str) -> bool {
  payload
    .devices
    .iter()
    .any(|device| device.id.as_deref() == Some(native_device_id))
}

fn is_no_active_device_error(e: &anyhow::Error) -> bool {
  let text = e.to_string().to_ascii_lowercase();
  text.contains("no_active_device") || text.contains("no active device")
}

/// Whether a native backend is positioned to claim a failed player command:
/// an available player (`start_playback` activates it on `NO_ACTIVE_DEVICE`),
/// or a backend/activation still materializing (the `suppressed_*` handlers
/// classify on the original error).
async fn native_can_claim_player_command(network: &Network) -> bool {
  #[cfg(feature = "streaming")]
  {
    matches!(
      network
        .fold_query(FoldQuery::NativeCanClaimPlayerCommand)
        .await,
      Some(FoldAnswer::Bool(true))
    )
  }
  #[cfg(not(feature = "streaming"))]
  {
    let _ = network;
    false
  }
}

/// The saved device to retry a player command on, or `None` to surface the
/// original error. Sending `device_id` with `me/player/*` both targets and
/// activates the device, which a bare command cannot do — without it every
/// command 404s once all devices are idle, the steady state of CLI mode
/// (#464). Never retries while a native backend could claim the command.
fn saved_device_retry(
  error_is_no_active_device: bool,
  saved_device_id: Option<&str>,
  native_can_claim: bool,
) -> Option<String> {
  if !error_is_no_active_device || native_can_claim {
    return None;
  }
  saved_device_id.map(str::to_string)
}

/// Run a player command against the Web API, retrying once on the saved
/// device (`client_config.device_id`) when Spotify rejects the bare command
/// with `NO_ACTIVE_DEVICE`. A failed retry surfaces the *original* error so
/// downstream classification (native fallback, transient suppression) is
/// unchanged.
async fn player_command_with_saved_device_retry(
  network: &Network,
  method: Method,
  path: &str,
  query: &[(&str, String)],
  body: Option<Value>,
) -> anyhow::Result<Value> {
  let result = network
    .spotify_api_request_json(method.clone(), path, query, body.clone())
    .await;
  let Err(e) = result else {
    return result;
  };

  let Some(device_id) = saved_device_retry(
    is_no_active_device_error(&e),
    network.client_config.device_id.as_deref(),
    native_can_claim_player_command(network).await,
  ) else {
    return Err(e);
  };

  info!("{path}: no active device; retrying on the saved device {device_id}");
  let mut query = query.to_vec();
  query.push(("device_id", device_id));
  match network
    .spotify_api_request_json(method, path, &query, body)
    .await
  {
    Ok(value) => Ok(value),
    Err(retry_err) => {
      warn!("saved-device retry failed: {retry_err}");
      Err(e)
    }
  }
}

/// While a native backend is expected to materialize (recovery in flight, or
/// the deferred startup init still running), a transport command that 404s
/// with NO_ACTIVE_DEVICE is expected noise — surface a status message instead
/// of routing to the full-screen Error. Returns true when the error was
/// handled that way.
#[cfg(feature = "streaming")]
async fn suppressed_no_device_error_while_pending(network: &Network, e: &anyhow::Error) -> bool {
  matches!(
    network
      .fold_query(FoldQuery::ClaimNativeReconnectingStatus {
        is_no_active_device: is_no_active_device_error(e),
      })
      .await,
    Some(FoldAnswer::Bool(true))
  )
}

#[cfg(feature = "streaming")]
fn is_restriction_violated_error(e: &anyhow::Error) -> bool {
  let text = e.to_string().to_ascii_lowercase();
  text.contains("403") && text.contains("restriction violated")
}

/// De-escalate a `403 Restriction violated` to a status message while the
/// native session is still settling; a steady-state restriction stays fatal.
#[cfg(feature = "streaming")]
async fn suppressed_restriction_violation_while_pending(
  network: &Network,
  e: &anyhow::Error,
) -> bool {
  matches!(
    network
      .fold_query(FoldQuery::ClaimNativeSettlingStatus {
        is_restriction_violated: is_restriction_violated_error(e),
      })
      .await,
    Some(FoldAnswer::Bool(true))
  )
}

/// Transient native-session errors (`NO_ACTIVE_DEVICE` 404 or `Restriction
/// violated` 403) a command can hit while the native backend is coming up.
#[cfg(feature = "streaming")]
async fn suppressed_transient_native_command_error(network: &Network, e: &anyhow::Error) -> bool {
  suppressed_no_device_error_while_pending(network, e).await
    || suppressed_restriction_violation_while_pending(network, e).await
}

#[cfg_attr(not(feature = "streaming"), allow(dead_code))]
fn api_confirms_native_info_is_current(
  native_name: &str,
  item: &PlayableItem,
  last_track_id: Option<&str>,
) -> bool {
  if playable_item_name(item) == Some(native_name) {
    return true;
  }

  playable_item_id(item)
    .as_deref()
    .is_some_and(|api_id| Some(api_id) == last_track_id)
}

#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
struct StaleApiItemContext {
  native_info_present: bool,
  api_item_present: bool,
  api_confirms_native_info: bool,
  native_track_id_present: bool,
  api_item_matches_native_track: bool,
  native_streaming_was_active: bool,
  native_activation_pending: bool,
  api_device_is_native: bool,
}

#[cfg(feature = "streaming")]
fn stale_api_item_should_preserve_native_context(context: StaleApiItemContext) -> bool {
  context.api_item_present
    && !context.api_confirms_native_info
    && (context.native_info_present
      || (context.native_track_id_present && !context.api_item_matches_native_track))
    && (context.native_streaming_was_active
      || context.native_activation_pending
      || context.api_device_is_native)
}

/// Get the currently active streaming player, if any.
/// Note: This logic is duplicated in `main.rs` as `active_streaming_player()`.
/// Both are identical; the difference is input type (Network vs. App Arc).
/// A future refactor could consolidate to a shared location like `src/core/app.rs`.
#[cfg(feature = "streaming")]
async fn current_streaming_player(
  network: &Network,
) -> Option<Arc<crate::infra::player::StreamingPlayer>> {
  match network.fold_query(FoldQuery::StreamingPlayerHandle).await {
    Some(FoldAnswer::StreamingPlayer(player)) => player,
    _ => None,
  }
}

#[cfg(feature = "streaming")]
#[derive(Clone)]
pub struct PlaybackPollState {
  pub player: Option<Arc<crate::infra::player::StreamingPlayer>>,
  pub native_device_id: Option<String>,
  pub native_streaming_was_active: bool,
  pub native_activation_pending: bool,
  pub native_track_id: Option<String>,
  pub native_info_name: Option<String>,
  pub has_fresh_native_activity: bool,
  pub native_is_playing: Option<bool>,
  pub persisted_shuffle: bool,
  pub local_state: Option<(Option<u8>, bool, RepeatState, Option<bool>)>,
  pub current_context: Option<rspotify::model::context::CurrentPlaybackContext>,
}

#[cfg(feature = "streaming")]
pub struct NativeRestoreClaim {
  pub player: Arc<crate::infra::player::StreamingPlayer>,
  pub snapshot: NativePlaybackRecoverySnapshot,
}

#[cfg(not(feature = "streaming"))]
#[derive(Default)]
pub struct PlaybackPollState;

#[cfg(feature = "streaming")]
pub(crate) fn apply_playback_poll(
  app: &mut App,
  mut c: rspotify::model::context::CurrentPlaybackContext,
  state: PlaybackPollState,
) {
  app.instant_since_last_current_playback_poll = Instant::now();
  let is_native_device = state.player.as_ref().is_some_and(|p| {
    if let (Some(current_id), Some(native_id)) =
      (c.device.id.as_ref(), state.native_device_id.as_ref())
    {
      return current_id == native_id;
    }
    c.device.name.eq_ignore_ascii_case(p.device_name()) && state.has_fresh_native_activity
  });
  if is_native_device && app.native_device_id.is_none() {
    app.native_device_id = c.device.id.clone();
  }
  let api_item_confirms_native_info = app
    .native_track_info
    .as_ref()
    .zip(c.item.as_ref())
    .is_some_and(|(info, item)| {
      api_confirms_native_info_is_current(
        info.name.as_str(),
        item,
        state.native_track_id.as_deref(),
      )
    });
  let api_item_matches_native_track = c
    .item
    .as_ref()
    .and_then(playable_item_id)
    .as_deref()
    .is_some_and(|id| Some(id) == state.native_track_id.as_deref());
  let stale_api_item = stale_api_item_should_preserve_native_context(StaleApiItemContext {
    native_info_present: state.native_info_name.is_some(),
    api_item_present: c.item.is_some(),
    api_confirms_native_info: api_item_confirms_native_info,
    native_track_id_present: state.native_track_id.is_some(),
    api_item_matches_native_track,
    native_streaming_was_active: state.native_streaming_was_active,
    native_activation_pending: state.native_activation_pending,
    api_device_is_native: is_native_device,
  });

  if !stale_api_item {
    if let Some(PlayableItem::Track(track)) = c.item.as_ref() {
      if let Some(track_id) = track.id.as_ref() {
        let id = track_id.id().to_string();
        if app.last_track_id.as_ref() != Some(&id) {
          if app.user_config.behavior.enable_global_song_count {
            app.dispatch(IoEvent::IncrementGlobalSongCount);
          }
          app.dispatch(IoEvent::CurrentUserSavedTracksContains(vec![id.clone()]));
        }
        app.last_track_id = Some(id);
      }
    }
  }
  if is_native_device {
    if let Some((volume, shuffle, repeat, native_playing)) = state.local_state {
      if let Some(volume) = volume {
        c.device.volume_percent = Some(volume.into());
      }
      c.shuffle_state = shuffle;
      c.repeat_state = repeat;
      if let Some(playing) = native_playing {
        c.is_playing = playing;
      }
    }
  }
  if let Some(pending) = app.pending_volume {
    let api_volume = c.device.volume_percent.unwrap_or(0) as u8;
    if api_volume == pending {
      app.pending_volume = None;
      app.last_dispatched_volume = None;
    } else if let Some(ctx) = app.current_playback_context.as_ref() {
      c.device.volume_percent = ctx.device.volume_percent;
    }
  }
  if state.local_state.is_none() && is_native_device {
    c.shuffle_state = state.persisted_shuffle;
  }
  if !stale_api_item {
    app.current_playback_context = Some(c);
  }
  app.is_streaming_active = if stale_api_item {
    true
  } else {
    is_native_device
  };
  if is_native_device {
    app.native_activation_pending = false;
  }
  if app.native_track_info.is_some()
    && !stale_api_item
    && (!is_native_device || api_item_confirms_native_info)
  {
    app.native_track_info = None;
  }
  app.seek_ms.take();
  app.is_fetching_current_playback = false;
}

#[cfg(not(feature = "streaming"))]
pub(crate) fn apply_playback_poll(
  app: &mut App,
  c: Option<rspotify::model::context::CurrentPlaybackContext>,
) {
  app.instant_since_last_current_playback_poll = Instant::now();
  if let Some(c) = c {
    if let Some(PlayableItem::Track(track)) = c.item.as_ref() {
      if let Some(id) = track.id.as_ref() {
        let id = id.id().to_string();
        if app.last_track_id.as_ref() != Some(&id) {
          app.dispatch(IoEvent::CurrentUserSavedTracksContains(vec![id.clone()]));
        }
        app.last_track_id = Some(id);
      }
    }
    app.current_playback_context = Some(c);
  }
  app.seek_ms.take();
  app.is_fetching_current_playback = false;
}

// --- Ticket 17: fold-side transport-backend resolution. The fold's
// `answer_query` arms call these so the whole backend decision (context
// reads, device matching, activation heuristics) stays one atomic read under
// the fold's lock, and the producer never locks App to route a command.

/// The symmetric-operation native check, fold-side.
#[cfg(feature = "streaming")]
pub(crate) fn fold_is_native_streaming_active_for_playback(app: &App) -> bool {
  let Some(streaming_player) = app.streaming_player.as_ref() else {
    return false;
  };
  let player_connected = streaming_player.is_available();
  if !player_connected {
    return false;
  }

  let native_device_name = streaming_player.device_name().to_lowercase();

  // If no context yet (e.g., at startup), use the app state flag which is
  // set when the native streaming device is activated/selected.
  let Some(ctx) = app.current_playback_context.as_ref() else {
    return app.is_streaming_active;
  };

  // First, check if the current playback device matches the native streaming device ID
  if let (Some(current_id), Some(native_id)) =
    (ctx.device.id.as_ref(), app.native_device_id.as_ref())
  {
    if current_id == native_id {
      return true;
    }
  }

  // Fallback: strict name match (case-insensitive), but only while we have
  // fresh native activity or a recent explicit activation. After a recovery,
  // Spotify can keep returning the old "spotatui" device while the new native
  // player is connected but stopped/not active.
  let current_device_name = ctx.device.name.to_lowercase();
  if current_device_name == native_device_name && app.has_fresh_native_activity() {
    return true;
  }

  // The user explicitly selected the native device very recently; honor that
  // intent even when the API context hasn't caught up yet (the brief pre-poll
  // window). `is_streaming_active` is re-derived from real Spotify state on the
  // next poll, so this cannot reintroduce the #254 device hijack. (#282)
  app.is_streaming_active
    && app
      .last_device_activation
      .is_some_and(|instant| instant.elapsed() < Duration::from_secs(5))
}

/// Symmetric transport backend, fold-side: the native player when native is
/// selected (player present), else `None` for the Web API route.
#[cfg(feature = "streaming")]
pub(crate) fn fold_symmetric_playback_backend(
  app: &App,
) -> Option<Arc<crate::infra::player::StreamingPlayer>> {
  let is_native_active = fold_is_native_streaming_active_for_playback(app);
  // Only look up the player when native is active, mirroring the original
  // short-circuit so the Connect path does not take the lock it never took.
  let player = if is_native_active {
    app.streaming_player.clone()
  } else {
    None
  };
  if select_native(is_native_active, player.is_some()) {
    player
  } else {
    None
  }
}

/// Start-playback activation heuristic, fold-side. `saved_device_id` is the
/// Web API client's saved device (lives on the producer's client config).
#[cfg(feature = "streaming")]
pub(crate) fn fold_should_activate_native_streaming_for_playback(
  app: &App,
  saved_device_id: Option<&str>,
) -> bool {
  let Some(player) = app.streaming_player.as_ref() else {
    return false;
  };

  if !player.is_available() {
    return false;
  }

  let native_name = player.device_name();
  let native_device_id = app.native_device_id.as_deref();
  let current_device = app.current_playback_context.as_ref().map(|ctx| &ctx.device);
  let current_device_id = current_device.and_then(|device| device.id.as_deref());
  let current_device_name_matches_native =
    current_device.is_some_and(|device| device.name.eq_ignore_ascii_case(native_name));
  let native_has_fresh_activity = app.has_fresh_native_activity();

  let saved_device_matches_native = saved_device_matches_native_player(
    saved_device_id,
    native_device_id,
    app.devices.as_ref(),
    native_name,
  );

  let saved_external_confirmed_available = saved_device_id.is_some_and(|saved| {
    app.devices.as_ref().is_some_and(|payload| {
      payload.devices.iter().any(|device| {
        device.id.as_deref() == Some(saved) && !device.name.eq_ignore_ascii_case(native_name)
      })
    })
  });

  should_activate_native_for_playback(NativeActivationContext {
    player_connected: true,
    current_device_id_present: current_device_id.is_some(),
    current_device_is_confirmed_native: native_device_id
      .is_some_and(|id| current_device_id == Some(id)),
    current_device_name_matches_native,
    native_has_fresh_activity,
    saved_device_matches_native,
    saved_external_confirmed_available,
  })
}

/// Start-playback backend, fold-side.
#[cfg(feature = "streaming")]
pub(crate) fn fold_start_playback_backend(
  app: &App,
  saved_device_id: Option<&str>,
) -> Option<Arc<crate::infra::player::StreamingPlayer>> {
  let is_native_active = fold_is_native_streaming_active_for_playback(app)
    || fold_should_activate_native_streaming_for_playback(app, saved_device_id);
  let player = if is_native_active {
    app.streaming_player.clone()
  } else {
    None
  };
  if select_native(is_native_active, player.is_some()) {
    player
  } else {
    None
  }
}

/// Transfer backend, fold-side: native only when the transfer target refers
/// to the native streaming device.
#[cfg(feature = "streaming")]
pub(crate) fn fold_transfer_playback_backend(
  app: &App,
  device_id: &str,
) -> Option<Arc<crate::infra::player::StreamingPlayer>> {
  let player = app.streaming_player.clone();
  let is_native_transfer = if let Some(ref player) = player {
    let native_name = player.device_name().to_lowercase();
    let matches_cached_device = app.devices.as_ref().is_some_and(|payload| {
      payload
        .devices
        .iter()
        .any(|d| d.id.as_deref() == Some(device_id) && d.name.to_lowercase() == native_name)
    });
    matches_cached_device || app.native_device_id.as_deref() == Some(device_id)
  } else {
    false
  };

  if select_native(is_native_transfer, player.is_some()) {
    player
  } else {
    None
  }
}

#[cfg(feature = "streaming")]
async fn is_native_streaming_active_for_playback(network: &Network) -> bool {
  matches!(
    network
      .fold_query(FoldQuery::IsNativeStreamingActiveForPlayback)
      .await,
    Some(FoldAnswer::Bool(true))
  )
}

/// Resolve the transport backend for a *symmetric* playback operation
/// (pause/next/previous/seek/shuffle/repeat/volume).
///
/// Native streaming is chosen only when it is the active device *and* a player
/// handle is present; otherwise the operation falls through to the Spotify Web
/// API. This wraps the existing selection logic without changing it: the two
/// awaited lookups happen in the same order as the original inline
/// `if is_native_streaming_active_for_playback(..).await { if let Some(player) =
/// current_streaming_player(..).await { .. } }` guard, so behaviour is identical.
#[cfg(feature = "streaming")]
async fn symmetric_playback_backend(network: &Network) -> PlaybackBackend {
  match network
    .fold_query(FoldQuery::SymmetricPlaybackBackend)
    .await
  {
    Some(FoldAnswer::StreamingPlayer(Some(player))) => PlaybackBackend::Native(player),
    _ => PlaybackBackend::Connect,
  }
}

/// Resolve the transport backend for `start_playback`.
///
/// Unlike the symmetric operations, native streaming is selected when it is
/// already active *or* when the activation heuristics say it should be
/// activated for this playback. The `||` short-circuit and "fetch the player
/// only when native applies" ordering are preserved exactly; a true predicate
/// with a missing player still falls through to the Web API.
#[cfg(feature = "streaming")]
async fn start_playback_backend(network: &Network) -> PlaybackBackend {
  match network
    .fold_query(FoldQuery::StartPlaybackBackend {
      saved_device_id: network.client_config.device_id.clone(),
    })
    .await
  {
    Some(FoldAnswer::StreamingPlayer(Some(player))) => PlaybackBackend::Native(player),
    _ => PlaybackBackend::Connect,
  }
}

/// Resolve the transport backend for `transfert_playback_to_device`.
///
/// The native player is selected only when the *transfer target* `device_id`
/// refers to the native streaming device, identified either by matching a
/// cached device whose name equals the native device name, or by matching the
/// recorded `native_device_id`. This mirrors the previous inline
/// `is_native_transfer` computation exactly; an unrelated target falls through
/// to the Web API transfer.
#[cfg(feature = "streaming")]
async fn transfer_playback_backend(network: &Network, device_id: &str) -> PlaybackBackend {
  match network
    .fold_query(FoldQuery::TransferPlaybackBackend {
      device_id: device_id.to_string(),
    })
    .await
  {
    Some(FoldAnswer::StreamingPlayer(Some(player))) => PlaybackBackend::Native(player),
    _ => PlaybackBackend::Connect,
  }
}

#[cfg(feature = "streaming")]
async fn request_native_streaming_recovery_if_disconnected(network: &Network) -> bool {
  matches!(
    network
      .fold_query(FoldQuery::ClaimNativeRecoveryIfDisconnected)
      .await,
    Some(FoldAnswer::Bool(true))
  )
}

#[cfg(feature = "streaming")]
async fn requested_native_playback_origin(
  network: &Network,
  context_id: &Option<PlayContextId<'static>>,
  uris: &Option<Vec<PlayableId<'static>>>,
) -> NativePlaybackOrigin {
  if context_id.is_some() {
    return NativePlaybackOrigin::Context;
  }

  if uris.is_some() {
    return NativePlaybackOrigin::RawList;
  }

  match network
    .fold_query(FoldQuery::NativePlaybackOriginFallback)
    .await
  {
    Some(FoldAnswer::NativeOrigin(origin)) => origin,
    _ => NativePlaybackOrigin::RawList,
  }
}

#[cfg(feature = "streaming")]
async fn resolve_native_playback_route(
  network: &Network,
  context_id: &Option<PlayContextId<'static>>,
) -> NativePlaybackRoute {
  if context_id.is_none() {
    return NativePlaybackRoute::NativeLoad;
  }

  match network.fold_query(FoldQuery::NativeDeviceId).await {
    Some(FoldAnswer::NativeDeviceId(Some(device_id))) => {
      NativePlaybackRoute::ContextApi { device_id }
    }
    _ => NativePlaybackRoute::NativeLoad,
  }
}

/// Start a context on the native device through the Web API (`me/player/play`
/// with `device_id`), mirroring shuffle state. The direct spirc load is the
/// primary route (#386); this covers a load the player rejected outright and
/// the watchdog-recovery replay of a load that was accepted but silently
/// failed to resolve its context.
#[cfg(feature = "streaming")]
async fn start_native_context_via_api(
  network: &mut Network,
  device_id: String,
  context: PlayContextId<'static>,
  uris: Option<&[PlayableId<'static>]>,
  offset: Option<usize>,
  desired_shuffle_state: bool,
) {
  let body = api_playback_body(Some(&context), uris, offset);
  match network
    .spotify_api_request_json(
      Method::PUT,
      "me/player/play",
      &[("device_id", device_id.clone())],
      body,
    )
    .await
  {
    Ok(_) => {
      // Playback started; a rejected shuffle toggle is non-fatal and must not
      // be recorded as applied (native polling won't self-correct it).
      let shuffle_applied = match network
        .spotify_api_request_json(
          Method::PUT,
          "me/player/shuffle",
          &[
            ("state", desired_shuffle_state.to_string()),
            ("device_id", device_id),
          ],
          None,
        )
        .await
      {
        Ok(_) => true,
        Err(e) => {
          warn!("native start succeeded but shuffle toggle was rejected: {e}");
          network
            .show_status_message("Playback started; couldn't apply shuffle.".to_string(), 6)
            .await;
          false
        }
      };

      // Keep the recovery chain alive: if the API accepted the start but the
      // native device never emits a player event, the watchdog fires again and
      // the bounded attempt counter eventually drops the request with a
      // message instead of leaving it parked forever.
      network
        .emit_session_acked(SessionFact::NativeApiContextStarted {
          context_uri: context.uri(),
          uris: uris.map(|list| list.iter().map(|u| u.uri()).collect()),
          offset,
          shuffle_applied,
          desired_shuffle_state,
        })
        .await;
    }
    Err(e) => {
      if suppressed_restriction_violation_while_pending(network, &e).await {
        return;
      }
      network
        .emit_session_acked(SessionFact::ClearStartPlaybackReplay)
        .await;
      // Both routes failed for this request; drop the parked copy so an
      // unrelated later recovery can't replay it.
      network
        .emit_session_acked(SessionFact::Error(format!(
          "Failed to start native playback: {}",
          e
        )))
        .await;
    }
  }
}

#[cfg(feature = "streaming")]
fn native_load_request(
  context_id: Option<PlayContextId<'static>>,
  uris: Option<Vec<PlayableId<'static>>>,
  offset: Option<usize>,
) -> Option<LoadRequest> {
  let mut options = LoadRequestOptions {
    start_playing: true,
    seek_to: 0,
    context_options: None,
    playing_track: None,
  };

  match (context_id, uris) {
    (Some(context), Some(track_uris)) => {
      if let Some(first_uri) = track_uris.first() {
        options.playing_track = Some(PlayingTrack::Uri(first_uri.uri()));
      } else if let Some(i) = offset.and_then(|i| u32::try_from(i).ok()) {
        options.playing_track = Some(PlayingTrack::Index(i));
      }
      Some(LoadRequest::from_context_uri(context.uri(), options))
    }
    (Some(context), None) => {
      if let Some(i) = offset.and_then(|i| u32::try_from(i).ok()) {
        options.playing_track = Some(PlayingTrack::Index(i));
      }
      Some(LoadRequest::from_context_uri(context.uri(), options))
    }
    (None, Some(track_uris)) => {
      if let Some(i) = offset.and_then(|i| u32::try_from(i).ok()) {
        options.playing_track = Some(PlayingTrack::Index(i));
      }
      let uris = track_uris.into_iter().map(|u| u.uri()).collect::<Vec<_>>();
      Some(LoadRequest::from_tracks(uris, options))
    }
    (None, None) => None,
  }
}

#[cfg(feature = "streaming")]
fn native_restore_load_request(snapshot: &NativePlaybackRecoverySnapshot) -> Option<LoadRequest> {
  let mut options = LoadRequestOptions {
    start_playing: snapshot.desired_playing,
    seek_to: snapshot.restore_position_ms(),
    context_options: Some(LoadContextOptions::Options(LoadOptions {
      shuffle: snapshot.shuffle,
      repeat: matches!(snapshot.repeat, RepeatState::Context | RepeatState::Track),
      repeat_track: snapshot.repeat == RepeatState::Track,
    })),
    playing_track: snapshot
      .expected_track_uri()
      .map(|uri| PlayingTrack::Uri(uri.to_string())),
  };

  if options.playing_track.is_none() {
    options.playing_track = snapshot
      .offset
      .and_then(|i| u32::try_from(i).ok())
      .map(PlayingTrack::Index);
  }

  if let Some(context_uri) = snapshot.context_uri.clone() {
    Some(LoadRequest::from_context_uri(context_uri, options))
  } else {
    let uris = snapshot.uris.clone()?;
    (!uris.is_empty()).then(|| LoadRequest::from_tracks(uris, options))
  }
}

impl PlaybackNetwork for Network {
  async fn get_current_playback(&mut self) {
    #[cfg(feature = "streaming")]
    let poll_state = match self.fold_query(FoldQuery::PlaybackPollState).await {
      Some(FoldAnswer::PlaybackPoll(state)) => *state,
      _ => return,
    };
    #[cfg(feature = "streaming")]
    {
      self.native_idle_recovery.observe_player_instance(
        poll_state
          .player
          .as_ref()
          .filter(|player| player.is_available())
          .map(|player| Arc::as_ptr(player) as usize),
      );
    }

    let context = self
      .spotify_get_typed::<Option<rspotify::model::CurrentPlaybackContext>>(
        "me/player",
        &[("additional_types", "episode,track".to_string())],
      )
      .await;

    match context {
      Ok(Some(c)) => {
        self.consecutive_playback_auth_failures = 0;
        #[cfg(feature = "streaming")]
        {
          let poll_state = match self.fold_query(FoldQuery::PlaybackPollState).await {
            Some(FoldAnswer::PlaybackPoll(state)) => *state,
            _ => return,
          };
          if poll_state.local_state.is_none() {
            if let Some(player) = poll_state.player.as_ref() {
              let shuffle = poll_state
                .current_context
                .as_ref()
                .map(|ctx| ctx.shuffle_state)
                .unwrap_or(poll_state.persisted_shuffle);
              let _ = player.set_shuffle(shuffle);
            }
          }
          self
            .emit_session_acked(SessionFact::PlaybackPolled {
              context: Some(Box::new(c)),
              state: Box::new(poll_state),
            })
            .await;
        }
        #[cfg(not(feature = "streaming"))]
        self
          .emit_session_acked(SessionFact::PlaybackPolled {
            context: Some(Box::new(c)),
          })
          .await;
      }
      Ok(None) => {
        self.consecutive_playback_auth_failures = 0;
        #[cfg(feature = "streaming")]
        let poll_state = match self.fold_query(FoldQuery::PlaybackPollState).await {
          Some(FoldAnswer::PlaybackPoll(state)) => *state,
          _ => return,
        };
        #[cfg(feature = "streaming")]
        if let Some(player) = poll_state
          .player
          .as_ref()
          .filter(|player| player.is_connected())
        {
          let native_device_id = player.device_id();
          let saved_matches = matches!(
            self
              .fold_query(FoldQuery::NativeIdleDeviceState {
                saved_device_id: self.client_config.device_id.clone(),
                native_device_id: native_device_id.clone(),
                native_device_name: player.device_name().to_string(),
              })
              .await,
            Some(FoldAnswer::NativeIdleDevice {
              saved_device_matches_native: true
            })
          );
          if let Some(update) = native_idle_device_preference_update(
            self.client_config.device_id.as_deref(),
            saved_matches,
          ) {
            let activated_at = if self
              .native_idle_recovery
              .should_attempt_idle_recovery(Instant::now())
            {
              let now = Instant::now();
              let _ = player.transfer(None);
              player.activate();
              Some(now)
            } else {
              None
            };
            self
              .emit_session_acked(SessionFact::NativeIdleDeviceReconciled {
                native_device_id: native_device_id.clone(),
                device_name: player.device_name().to_string(),
                volume: player.get_volume(),
                activated_at,
              })
              .await;
            if update == NativeDevicePreferenceUpdate::Persist
              && self.client_config.device_id.as_deref() != Some(native_device_id.as_str())
            {
              if let Err(e) = self.client_config.set_device_id(native_device_id) {
                self.emit_error(anyhow!(e)).await;
              }
            }
          }
        }
        self
          .emit_session_acked(SessionFact::PlaybackPollIdle {
            clear_context: false,
          })
          .await;
      }
      Err(e) => {
        let err = anyhow!(e);

        if err.to_string().contains("429")
          || err.to_string().contains("Too Many Requests")
          || err.to_string().contains("Too many requests")
        {
          self
            .emit_session(SessionFact::TransientStatus {
              message: "Spotify rate limit hit. Retrying automatically; please wait a few seconds."
                .to_string(),
              ttl_secs: 6,
            })
            .await;
          self
            .emit_session_acked(SessionFact::PlaybackPollIdle {
              clear_context: false,
            })
            .await;
          return;
        }

        if err
          .to_string()
          .to_lowercase()
          .contains("error sending request for url")
          || err.to_string().contains("connection reset")
          || err.to_string().contains("connection refused")
          || err.to_string().contains("timed out")
          || err.to_string().contains("temporary failure")
          || err.to_string().contains("dns")
        {
          self
            .emit_session(SessionFact::TransientStatus {
              message:
                "Temporary Spotify network error while polling playback; retrying automatically."
                  .to_string(),
              ttl_secs: 5,
            })
            .await;
          self
            .emit_session_acked(SessionFact::PlaybackPollIdle {
              clear_context: false,
            })
            .await;
          return;
        }

        if err.to_string().contains("504")
          || err.to_string().contains("503")
          || err.to_string().contains("502")
          || err.to_string().contains("Gateway Timeout")
          || err.to_string().contains("Service Unavailable")
          || err.to_string().contains("Bad Gateway")
        {
          self
            .emit_session(SessionFact::TransientStatus {
              message: "Spotify server temporarily unavailable (5xx); retrying automatically."
                .to_string(),
              ttl_secs: 10,
            })
            .await;
          self
            .emit_session_acked(SessionFact::PlaybackPollIdle {
              clear_context: false,
            })
            .await;
          return;
        }

        // A 401 here is usually not a broken session: Spotify's player service
        // intermittently rejects a valid token (issue #395, typically right at a
        // track transition) and the very next poll succeeds with the same token.
        // Escalating the first one to the full-screen error route interrupted
        // playback the user could see was fine. Tolerate a short run, then
        // surface it — a genuinely dead session fails every poll.
        if err.to_string().contains("401")
          || err.to_string().to_lowercase().contains("unauthorized")
        {
          self.consecutive_playback_auth_failures =
            self.consecutive_playback_auth_failures.saturating_add(1);

          log::warn!(
            "playback poll rejected with 401 ({}/{}): {}",
            self.consecutive_playback_auth_failures,
            MAX_CONSECUTIVE_PLAYBACK_AUTH_FAILURES,
            err
          );

          if self.consecutive_playback_auth_failures < MAX_CONSECUTIVE_PLAYBACK_AUTH_FAILURES {
            // Stay quiet on the first one (the self-healing case); only say
            // something once it starts looking persistent.
            if self.consecutive_playback_auth_failures > 1 {
              self
                .emit_session(SessionFact::Status {
                  message: "Spotify keeps rejecting the playback poll; retrying automatically."
                    .to_string(),
                  ttl_secs: 5,
                  is_error: false,
                })
                .await;
            }
            self
              .emit_session_acked(SessionFact::PlaybackPollIdle {
                clear_context: false,
              })
              .await;
            return;
          }

          // Sustained failure: surface it, and start counting again so a still
          // broken session re-reports periodically instead of every poll.
          self.consecutive_playback_auth_failures = 0;
        }

        // 404 = no active device/player; treat as idle, not an error
        if err.to_string().contains("404") || err.to_string().contains("Not Found") {
          // The API answered, so the token is good.
          self.consecutive_playback_auth_failures = 0;
          #[cfg(feature = "streaming")]
          let poll_state = match self.fold_query(FoldQuery::PlaybackPollState).await {
            Some(FoldAnswer::PlaybackPoll(state)) => *state,
            _ => return,
          };
          #[cfg(feature = "streaming")]
          if let Some(player) = poll_state
            .player
            .as_ref()
            .filter(|player| player.is_connected())
          {
            let native_device_id = player.device_id();
            let saved_matches = matches!(
              self
                .fold_query(FoldQuery::NativeIdleDeviceState {
                  saved_device_id: self.client_config.device_id.clone(),
                  native_device_id: native_device_id.clone(),
                  native_device_name: player.device_name().to_string(),
                })
                .await,
              Some(FoldAnswer::NativeIdleDevice {
                saved_device_matches_native: true
              })
            );
            if let Some(update) = native_idle_device_preference_update(
              self.client_config.device_id.as_deref(),
              saved_matches,
            ) {
              self
                .emit_session_acked(SessionFact::NativeIdleDeviceReconciled {
                  native_device_id: native_device_id.clone(),
                  device_name: player.device_name().to_string(),
                  volume: player.get_volume(),
                  activated_at: None,
                })
                .await;
              if update == NativeDevicePreferenceUpdate::Persist
                && self.client_config.device_id.as_deref() != Some(native_device_id.as_str())
              {
                if let Err(e) = self.client_config.set_device_id(native_device_id) {
                  self.emit_error(anyhow!(e)).await;
                }
              }
            }
          }
          self
            .emit_session_acked(SessionFact::PlaybackPollIdle {
              clear_context: true,
            })
            .await;
          return;
        }

        self
          .emit_session_acked(SessionFact::PlaybackPollIdle {
            clear_context: false,
          })
          .await;
        self.emit_error(err).await;
      }
    }
  }

  async fn start_playback(
    &mut self,
    context_id: Option<PlayContextId<'static>>,
    uris: Option<Vec<PlayableId<'static>>>,
    offset: Option<usize>,
  ) {
    let (uris, offset) = if context_id.is_none() {
      match uris {
        Some(track_uris) => {
          let (trimmed_uris, trimmed_offset) = trim_api_playback_uris(track_uris, offset);
          (Some(trimmed_uris), trimmed_offset)
        }
        None => (None, offset),
      }
    } else {
      (uris, offset)
    };

    let desired_shuffle_state = match self.fold_query(FoldQuery::DesiredShuffleState).await {
      Some(FoldAnswer::Bool(state)) => state,
      _ => false,
    };

    // Any explicit new playback target invalidates the app-owned shuffle
    // session; the interception below re-creates one when it applies. A bare
    // resume (no context, no uris) keeps the session.
    #[cfg(feature = "streaming")]
    if context_id.is_some() || uris.is_some() {
      self
        .emit_session_acked(SessionFact::ClearNativeShuffleSession)
        .await;
    }

    // Check if we should use native streaming for playback
    #[cfg(feature = "streaming")]
    if request_native_streaming_recovery_if_disconnected(self).await {
      // Park the request instead of dropping it: the recovery handler replays
      // it once the new session and device selection are in place, so the
      // press that detected the disconnect still plays.
      self
        .emit_session_acked(SessionFact::ParkStartPlayback {
          context_uri: context_id.as_ref().map(|c| c.uri()),
          uris: uris
            .as_ref()
            .map(|list| list.iter().map(|u| u.uri()).collect()),
          offset,
        })
        .await;
      return;
    }

    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = start_playback_backend(self).await {
      let requested_origin = requested_native_playback_origin(self, &context_id, &uris).await;
      let activation_time = Instant::now();
      let native_device_id = player.device_id();
      let request = if context_id.is_none() && uris.is_none() {
        None
      } else {
        Some(crate::runtime::engine::NativePlaybackRequestSpec {
          context_uri: context_id.as_ref().map(|context| context.uri()),
          uris: uris
            .as_ref()
            .map(|items| items.iter().map(|item| item.uri()).collect()),
          offset,
          desired_playing: true,
          shuffle: desired_shuffle_state,
        })
      };
      let activation = crate::runtime::engine::NativePlaybackActivation {
        native_device_id: native_device_id.clone(),
        native_device_name: player.device_name().to_string(),
        saved_device_id: self.client_config.device_id.clone(),
        explicit_persist: false,
        origin: Some(requested_origin),
        activation_time,
        backdate_poll: false,
        clear_context: false,
        bare_resume: context_id.is_none() && uris.is_none(),
        request,
      };
      let (should_transfer, native_preference_update) = match self
        .fold_query(FoldQuery::NativePlaybackActivationDecision(
          activation.clone(),
        ))
        .await
      {
        Some(FoldAnswer::NativeActivation(decision)) => {
          (decision.should_transfer, decision.preference)
        }
        _ => (false, NativeDevicePreferenceUpdate::KeepExistingPreference),
      };

      if should_transfer {
        // A failed transfer on a zombie session used to vanish silently; the
        // load watchdog below is the recovery net, but keep the evidence.
        if let Err(e) = player.transfer(None) {
          warn!("native transfer failed: {e}");
        }
      }

      player.activate();
      self
        .native_idle_recovery
        .settle_current_episode(activation_time);
      self
        .emit_session_acked(SessionFact::NativePlaybackActivated(activation))
        .await;
      if native_preference_update == NativeDevicePreferenceUpdate::Persist
        && self.client_config.device_id.as_deref() != Some(native_device_id.as_str())
      {
        if let Err(e) = self.client_config.set_device_id(native_device_id.clone()) {
          self.emit_error(anyhow!(e)).await;
        }
      }
      // Client-side shuffle: when shuffle is on and the target is a playlist,
      // album, or Liked Songs, the app owns the play order (a pre-shuffled
      // flat list loaded via `from_tracks`) instead of delegating shuffle to
      // Spirc/Spotify — see `native_shuffle`.
      if desired_shuffle_state
        && self
          .try_start_native_shuffled_playback(&player, &context_id, &uris, offset)
          .await
      {
        return;
      }

      let native_route = resolve_native_playback_route(self, &context_id).await;

      // For resume playback (no context, no uris)
      if context_id.is_none() && uris.is_none() {
        let can_resume_direct_native = matches!(
          self.fold_query(FoldQuery::NativeResumeDirectPossible).await,
          Some(FoldAnswer::Bool(true))
        );

        if can_resume_direct_native {
          info!("starting native resume playback via direct player route");
          player.play();
          self
            .emit_session_acked(SessionFact::TransportStateMirrored {
              is_playing: Some(true),
              shuffle: None,
              repeat: None,
              volume: None,
            })
            .await;
        } else {
          info!(
            "starting native resume playback via Spotify API on device {}",
            native_device_id
          );
          match self
            .spotify_api_request_json(
              Method::PUT,
              "me/player/play",
              &[("device_id", native_device_id.clone())],
              None,
            )
            .await
          {
            Ok(_) => {
              self
                .emit_session_acked(SessionFact::NativeResumeApiStarted {
                  native_device_id: Some(native_device_id),
                })
                .await;
            }
            Err(e) => {
              self
                .emit_status(
                  format!("No playback to resume on {}.", player.device_name()),
                  4,
                )
                .await;
              info!("native resume via Spotify API failed: {}", e);
            }
          }
        }
        return;
      }

      // Keep the string form for the load watchdog: if the session turns out
      // to be a zombie (load accepted, no player event follows), recovery
      // replays this exact request.
      let parked_context = context_id.as_ref().map(|c| c.uri());
      let parked_uris = uris
        .as_ref()
        .map(|list| list.iter().map(|u| u.uri()).collect::<Vec<_>>());

      // Native context starts take the direct spirc load first
      // (`from_context_uri`): context resolution happens inside librespot's
      // connect task, so a slow or degraded Spotify session cannot stall the
      // serial IoEvent pump the way a `me/player/play` round trip can (one
      // stalled call blocks every queued playback command for the full
      // timeout-and-retry cycle, #386). The watchdog covers silent failures;
      // the Web API context route remains as fallback for a load the player
      // rejects outright.
      let api_fallback = match (&native_route, &context_id) {
        (NativePlaybackRoute::ContextApi { device_id }, Some(context)) => {
          Some((device_id.clone(), context.clone()))
        }
        _ => None,
      };

      // A watchdog-recovery replay means a direct load of this same request
      // was already accepted once and failed silently (the context never
      // resolved); take the Web API context route this time instead of
      // looping through the same silent failure until the request is dropped.
      let retry_via_api = matches!(
        self
          .fold_query(FoldQuery::StartPlaybackReplayMatches {
            context_uri: parked_context.clone(),
            uris: parked_uris.clone(),
            offset,
          })
          .await,
        Some(FoldAnswer::Bool(true))
      );
      if retry_via_api {
        if let Some((device_id, context)) = api_fallback.clone() {
          info!(
            "recovery replay: starting native playback via Spotify context route on device {device_id}"
          );
          start_native_context_via_api(
            self,
            device_id,
            context,
            uris.as_deref(),
            offset,
            desired_shuffle_state,
          )
          .await;
          return;
        }
      }

      let Some(request) = native_load_request(context_id, uris.clone(), offset) else {
        return;
      };

      info!("starting native playback via direct load route");
      match player.load(request) {
        Ok(()) => {
          let _ = player.set_shuffle(desired_shuffle_state);
          // Optimistic UI update; the watchdog corrects it if no player event
          // ever confirms the load (zombie session).
          self
            .emit_session_acked(SessionFact::NativeLoadAccepted {
              context_uri: parked_context,
              uris: parked_uris,
              offset,
              desired_shuffle_state,
              record: None,
            })
            .await;
        }
        Err(load_err) => {
          let Some((device_id, context)) = api_fallback else {
            self
              .emit_error(anyhow!("Failed to start native playback: {}", load_err))
              .await;
            return;
          };
          info!(
            "direct native load failed ({load_err}); falling back to Spotify context route on device {device_id}"
          );
          start_native_context_via_api(
            self,
            device_id,
            context,
            uris.as_deref(),
            offset,
            desired_shuffle_state,
          )
          .await;
        }
      }
      return;
    }

    let body = api_playback_body(context_id.as_ref(), uris.as_deref(), offset);
    let result =
      player_command_with_saved_device_retry(self, Method::PUT, "me/player/play", &[], body).await;

    match result {
      Ok(_) => {
        let shuffle_applied = match self
          .spotify_api_request_json(
            Method::PUT,
            "me/player/shuffle",
            &[("state", desired_shuffle_state.to_string())],
            None,
          )
          .await
        {
          Ok(_) => true,
          Err(e) => {
            log::warn!("start succeeded but shuffle toggle was rejected: {e}");
            self
              .show_status_message("Playback started; couldn't apply shuffle.".to_string(), 6)
              .await;
            false
          }
        };

        self
          .emit_session_acked(SessionFact::TransportStateMirrored {
            is_playing: Some(true),
            shuffle: shuffle_applied.then_some(desired_shuffle_state),
            repeat: None,
            volume: None,
          })
          .await;
      }
      Err(e) => {
        #[cfg(feature = "streaming")]
        if is_no_active_device_error(&e) {
          if let Some(player) = current_streaming_player(self).await {
            if player.is_available() {
              let requested_origin =
                requested_native_playback_origin(self, &context_id, &uris).await;
              let activation_time = Instant::now();
              let native_device_id = player.device_id();
              player.activate();
              self
                .native_idle_recovery
                .settle_current_episode(activation_time);
              let activation = crate::runtime::engine::NativePlaybackActivation {
                native_device_id: native_device_id.clone(),
                native_device_name: player.device_name().to_string(),
                saved_device_id: self.client_config.device_id.clone(),
                explicit_persist: false,
                origin: Some(requested_origin),
                activation_time,
                backdate_poll: true,
                clear_context: false,
                bare_resume: context_id.is_none() && uris.is_none(),
                // The recovery snapshot is recorded only after the fallback
                // load is accepted, preserving the old failure behavior.
                request: None,
              };
              let native_preference_update = match self
                .fold_query(FoldQuery::NativePlaybackActivationDecision(
                  activation.clone(),
                ))
                .await
              {
                Some(FoldAnswer::NativeActivation(decision)) => decision.preference,
                _ => NativeDevicePreferenceUpdate::KeepExistingPreference,
              };
              self
                .emit_session_acked(SessionFact::NativePlaybackActivated(activation))
                .await;
              if native_preference_update == NativeDevicePreferenceUpdate::Persist
                && self.client_config.device_id.as_deref() != Some(native_device_id.as_str())
              {
                if let Err(e) = self.client_config.set_device_id(native_device_id.clone()) {
                  self.emit_error(anyhow!(e)).await;
                }
              }

              let parked_context = context_id.as_ref().map(|c| c.uri());
              let parked_uris = uris
                .as_ref()
                .map(|list| list.iter().map(|u| u.uri()).collect::<Vec<_>>());
              if let Some(request) = native_load_request(context_id.clone(), uris.clone(), offset) {
                info!("default Spotify playback had no active device; falling back to native load");
                if let Err(load_err) = player.load(request) {
                  self
                    .emit_error(anyhow!("Failed to start native playback: {}", load_err))
                    .await;
                } else {
                  let _ = player.set_shuffle(desired_shuffle_state);
                  self
                    .emit_session_acked(SessionFact::NativeLoadAccepted {
                      context_uri: parked_context,
                      uris: parked_uris,
                      offset,
                      desired_shuffle_state,
                      record: Some(crate::runtime::engine::NativePlaybackRequestSpec {
                        context_uri: context_id.as_ref().map(|context| context.uri()),
                        uris: uris
                          .as_ref()
                          .map(|list| list.iter().map(|item| item.uri()).collect::<Vec<_>>()),
                        offset,
                        desired_playing: true,
                        shuffle: desired_shuffle_state,
                      }),
                    })
                    .await;
                }
                return;
              }

              info!(
                "default Spotify resume had no active device; retrying on native device {}",
                native_device_id
              );
              match self
                .spotify_api_request_json(
                  Method::PUT,
                  "me/player/play",
                  &[("device_id", native_device_id.clone())],
                  None,
                )
                .await
              {
                Ok(_) => {
                  self
                    .emit_session_acked(SessionFact::NativeResumeApiStarted {
                      native_device_id: Some(native_device_id),
                    })
                    .await;
                }
                Err(resume_err) => {
                  self
                    .emit_status(
                      format!("No playback to resume on {}.", player.device_name()),
                      4,
                    )
                    .await;
                  info!("native resume fallback failed: {}", resume_err);
                }
              }
              return;
            }
          }

          // No usable backend right now, but one may materialize shortly
          // (recovery in flight, or deferred startup init still running):
          // park the request for replay instead of routing to the
          // full-screen error, which is what made every press during the
          // recovery window cost a paced round trip ending on Error.
          if matches!(
            self
              .fold_query(FoldQuery::ClaimNativeBackendPendingPark {
                context_uri: context_id.as_ref().map(|c| c.uri()),
                uris: uris
                  .as_ref()
                  .map(|list| list.iter().map(|u| u.uri()).collect()),
                offset,
              })
              .await,
            Some(FoldAnswer::Bool(true))
          ) {
            return;
          }
        }

        // De-escalate a native-settling restriction violation; don't park it
        // (no replay-owner guarantee). External plays succeed rather than 403.
        #[cfg(feature = "streaming")]
        if suppressed_restriction_violation_while_pending(self, &e).await {
          return;
        }

        self.emit_error(e).await;
      }
    }
  }

  #[cfg(feature = "streaming")]
  async fn restore_native_playback(&mut self, generation: u64) {
    let (player, snapshot) = match self
      .fold_query(FoldQuery::ClaimNativeRestore { generation })
      .await
    {
      Some(FoldAnswer::NativeRestore(Some(claim))) => {
        (claim.player.clone(), claim.snapshot.clone())
      }
      _ => return,
    };

    let Some(request) = native_restore_load_request(&snapshot) else {
      warn!(
        "native restore {generation} skipped: the snapshot carries no loadable context or uris"
      );
      self
        .emit_session(SessionFact::NativeRestoreNoState { generation })
        .await;
      return;
    };

    info!(
      "restoring native playback generation {} track {:?} position {} playing {}",
      generation,
      snapshot.expected_track_uri(),
      snapshot.restore_position_ms(),
      snapshot.desired_playing
    );
    player.activate();
    if let Err(e) = player.load(request) {
      warn!(
        "failed to issue native playback restore generation {}: {}",
        generation, e
      );
      self
        .emit_session(SessionFact::NativeRestoreFailed { generation })
        .await;
      return;
    }

    self
      .emit_session(SessionFact::NativeRestoreApplied {
        generation,
        desired_playing: snapshot.desired_playing,
        position_ms: u64::from(snapshot.restore_position_ms()),
        shuffle: snapshot.shuffle,
        repeat: snapshot.repeat,
      })
      .await;
  }

  async fn pause_playback(&mut self) {
    #[cfg(feature = "streaming")]
    {
      self
        .emit_session(SessionFact::ClearStartPlaybackReplay)
        .await;
    }
    // Check if using native streaming
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      player.pause();
      // Update UI state immediately
      self.emit_session(SessionFact::NativePauseMirrored).await;
      return;
    }

    match player_command_with_saved_device_retry(self, Method::PUT, "me/player/pause", &[], None)
      .await
    {
      Ok(_) => {
        self
          .emit_session(SessionFact::TransportStateMirrored {
            is_playing: Some(false),
            shuffle: None,
            repeat: None,
            volume: None,
          })
          .await;
      }
      Err(e) => {
        #[cfg(feature = "streaming")]
        if suppressed_transient_native_command_error(self, &e).await {
          return;
        }
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn next_track(&mut self) {
    #[cfg(feature = "streaming")]
    {
      self
        .emit_session(SessionFact::ClearStartPlaybackReplay)
        .await;
    }
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      player.next();
      return;
    }

    if let Err(e) =
      player_command_with_saved_device_retry(self, Method::POST, "me/player/next", &[], None).await
    {
      #[cfg(feature = "streaming")]
      if suppressed_transient_native_command_error(self, &e).await {
        return;
      }
      self.handle_error(anyhow!(e)).await;
    }
  }

  async fn previous_track(&mut self) {
    #[cfg(feature = "streaming")]
    {
      self
        .emit_session(SessionFact::ClearStartPlaybackReplay)
        .await;
    }
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      player.prev();
      return;
    }

    if let Err(e) =
      player_command_with_saved_device_retry(self, Method::POST, "me/player/previous", &[], None)
        .await
    {
      #[cfg(feature = "streaming")]
      if suppressed_transient_native_command_error(self, &e).await {
        return;
      }
      self.handle_error(anyhow!(e)).await;
    }
  }

  async fn force_previous_track(&mut self) {
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      player.prev();
      // The second prev (which actually skips back once the position has reset
      // to 0) runs on a detached task so the intentional 500ms gap doesn't
      // block every other IoEvent on the serial pump.
      tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        player.prev();
      });
      return;
    }

    // First previous_track restarts the current track (if past Spotify's ~3s
    // threshold). After a short delay the second call actually skips to the
    // previous track, since the position is now back at 0.
    if let Err(e) =
      player_command_with_saved_device_retry(self, Method::POST, "me/player/previous", &[], None)
        .await
    {
      #[cfg(feature = "streaming")]
      if suppressed_transient_native_command_error(self, &e).await {
        return;
      }
      self.handle_error(anyhow!(e)).await;
      return;
    }

    // Re-dispatch the second call after the delay instead of sleeping on the
    // pump: a plain PreviousTrack with the position back at 0 skips to the
    // previous track, which is exactly the second half of the double-press
    // semantics.
    let io_tx = match self.fold_query(FoldQuery::IoTx).await {
      Some(FoldAnswer::IoTx(io_tx)) => io_tx,
      _ => None,
    };
    tokio::spawn(async move {
      tokio::time::sleep(std::time::Duration::from_millis(500)).await;
      if let Some(io_tx) = io_tx {
        let _ = io_tx.send(IoEvent::PreviousTrack);
      }
    });
  }

  async fn seek(&mut self, position_ms: u32) {
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      player.seek(position_ms);
      self
        .emit_session(SessionFact::NativeSeekMirrored { position_ms })
        .await;
      return;
    }

    if let Err(e) = player_command_with_saved_device_retry(
      self,
      Method::PUT,
      "me/player/seek",
      &[("position_ms", position_ms.to_string())],
      None,
    )
    .await
    {
      #[cfg(feature = "streaming")]
      if suppressed_transient_native_command_error(self, &e).await {
        return;
      }
      self.handle_error(anyhow!(e)).await;
    }
  }

  async fn shuffle(&mut self, shuffle_state: bool) {
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      let _ = player.set_shuffle(shuffle_state);
      self
        .emit_session(SessionFact::NativeShuffleMirrored {
          state: shuffle_state,
        })
        .await;
      return;
    }

    match player_command_with_saved_device_retry(
      self,
      Method::PUT,
      "me/player/shuffle",
      &[("state", shuffle_state.to_string())],
      None,
    )
    .await
    {
      Ok(_) => {
        self
          .emit_session(SessionFact::TransportStateMirrored {
            is_playing: None,
            shuffle: Some(shuffle_state),
            repeat: None,
            volume: None,
          })
          .await;
      }
      Err(e) => {
        #[cfg(feature = "streaming")]
        if suppressed_transient_native_command_error(self, &e).await {
          return;
        }
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn repeat(&mut self, repeat_state: RepeatState) {
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      let _ = player.set_repeat(repeat_state);
      self
        .emit_session(SessionFact::NativeRepeatMirrored {
          state: repeat_state,
        })
        .await;
      return;
    }

    let repeat_state_param: &'static str = repeat_state.into();
    match player_command_with_saved_device_retry(
      self,
      Method::PUT,
      "me/player/repeat",
      &[("state", repeat_state_param.to_string())],
      None,
    )
    .await
    {
      Ok(_) => {
        self
          .emit_session(SessionFact::TransportStateMirrored {
            is_playing: None,
            shuffle: None,
            repeat: Some(repeat_state),
            volume: None,
          })
          .await;
      }
      Err(e) => {
        #[cfg(feature = "streaming")]
        if suppressed_transient_native_command_error(self, &e).await {
          return;
        }
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  /// Sends the volume change to Spotify, either through the native streaming
  /// player or the Web API depending on which device is active.
  ///
  /// On success we clear the in-flight flag but keep `pending_volume` around.
  /// It only gets cleared when `get_current_playback` comes back with a matching
  /// volume — that's our signal that Spotify actually caught up.
  ///
  /// On error we bail and clear everything so the UI falls back to whatever
  /// the API last reported.
  async fn change_volume(&mut self, volume: u8) {
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = symmetric_playback_backend(self).await {
      player.set_volume(volume);
      self
        .emit_session(SessionFact::NativeVolumeMirrored { volume })
        .await;
      return;
    }

    match player_command_with_saved_device_retry(
      self,
      Method::PUT,
      "me/player/volume",
      &[("volume_percent", volume.to_string())],
      None,
    )
    .await
    {
      Ok(_) => {
        self
          .emit_session(SessionFact::TransportStateMirrored {
            is_playing: None,
            shuffle: None,
            repeat: None,
            volume: Some(volume),
          })
          .await;
      }
      Err(e) => {
        self.emit_session(SessionFact::VolumeChangeDropped).await;
        #[cfg(feature = "streaming")]
        if suppressed_transient_native_command_error(self, &e).await {
          return;
        }
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn transfert_playback_to_device(&mut self, device_id: String, persist_device_id: bool) {
    // A device change moves playback off the session's `from_tracks` load;
    // the app-owned shuffle order no longer describes what plays.
    #[cfg(feature = "streaming")]
    self
      .emit_session(SessionFact::ClearNativeShuffleSession)
      .await;
    #[cfg(feature = "streaming")]
    if let PlaybackBackend::Native(player) = transfer_playback_backend(self, &device_id).await {
      let activation_time = Instant::now();
      let native_device_id = player.device_id();
      let _ = player.transfer(None);
      player.activate();
      self
        .native_idle_recovery
        .settle_current_episode(activation_time);
      self
        .emit_session_acked(SessionFact::NativeTransferActivated {
          native_device_id: native_device_id.clone(),
          activation_time,
        })
        .await;
      if persist_device_id
        && self.client_config.device_id.as_deref() != Some(native_device_id.as_str())
      {
        if let Err(e) = self.client_config.set_device_id(native_device_id) {
          self.emit_error(anyhow!(e)).await;
        }
      }
      return;
    }

    if let Err(e) = self
      .spotify_api_request_json(
        Method::PUT,
        "me/player",
        &[],
        Some(json!({
          "device_ids": [device_id.clone()],
          "play": true
        })),
      )
      .await
    {
      self.emit_error(anyhow!(e)).await;
    } else {
      if persist_device_id {
        if let Err(e) = self.client_config.set_device_id(device_id.clone()) {
          self.emit_error(anyhow!(e)).await;
        }
      }
      self
        .emit_session_acked(SessionFact::ExternalTransferCompleted { device_id })
        .await;
    }
  }

  #[cfg(feature = "streaming")]
  async fn auto_select_streaming_device(
    &mut self,
    device_name: String,
    persist_device_id: bool,
    yield_to_external_playback: bool,
  ) {
    if let Some(player) = current_streaming_player(self).await {
      let activation_time = Instant::now();
      let native_device_id = player.device_id();
      let (should_transfer, external_playback_active, native_preference_update) = match self
        .fold_query(FoldQuery::AutoSelectDecision {
          native_device_id: native_device_id.clone(),
          native_device_name: player.device_name().to_string(),
          saved_device_id: self.client_config.device_id.clone(),
          persist_device_id,
          yield_to_external_playback,
        })
        .await
      {
        Some(FoldAnswer::AutoSelect {
          should_transfer,
          external_playback_active,
          preference,
        }) => (should_transfer, external_playback_active, preference),
        _ => (
          false,
          false,
          NativeDevicePreferenceUpdate::KeepExistingPreference,
        ),
      };

      if !external_playback_active {
        if should_transfer {
          let _ = player.transfer(None);
        }
        player.activate();
      }
      self
        .native_idle_recovery
        .settle_current_episode(activation_time);

      self
        .emit_session_acked(SessionFact::AutoSelectApplied {
          native_device_id: native_device_id.clone(),
          activation_time,
          external_playback_active,
        })
        .await;
      if native_preference_update == NativeDevicePreferenceUpdate::Persist
        && self.client_config.device_id.as_deref() != Some(native_device_id.as_str())
      {
        if let Err(e) = self.client_config.set_device_id(native_device_id.clone()) {
          self.emit_error(anyhow!(e)).await;
        }
      }

      for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(200)).await;

        match self
          .spotify_get_typed::<DevicePayload>("me/player/devices", &[])
          .await
        {
          Ok(payload) => {
            let native_confirmed =
              spotify_payload_confirms_native_device(&payload, &native_device_id);
            let name_seen = payload
              .devices
              .iter()
              .any(|device| device.name.eq_ignore_ascii_case(&device_name));

            if native_confirmed || name_seen {
              self
                .emit_session(SessionFact::DevicesLanded {
                  devices: payload.devices,
                  navigate: false,
                })
                .await;
            }

            if native_confirmed {
              return;
            }
          }
          Err(_) => continue,
        }
      }
    }
  }

  async fn ensure_playback_continues(&mut self, previous_track_id: String) {
    #[cfg(feature = "streaming")]
    let native_active = is_native_streaming_active_for_playback(self).await;
    #[cfg(feature = "streaming")]
    info!("continuation for {previous_track_id}: handling, native_active={native_active}");
    #[cfg(feature = "streaming")]
    if native_active {
      let (transition_advanced, raw_next, raw_list_exhausted) = match self
        .fold_query(FoldQuery::NativeContinuationState {
          previous_track_id: previous_track_id.clone(),
        })
        .await
      {
        Some(FoldAnswer::NativeContinuation {
          transition_advanced,
          raw_next,
          raw_list_exhausted,
          ..
        }) => (transition_advanced, raw_next, raw_list_exhausted),
        _ => (false, None, false),
      };
      if transition_advanced {
        info!("continuation for {previous_track_id}: playback already advanced");
        return;
      }
      if let Some(next) = raw_next {
        info!("continuation for {previous_track_id}: starting next raw-list track");
        let uris = next
          .uris
          .map(|items| crate::infra::network::ids::playable_ids(&items));
        self.start_playback(None, uris, next.offset).await;
        return;
      }
      if raw_list_exhausted {
        // A raw URI list with repeat off has no track after the one that just
        // ended, so stopping is the correct outcome. Record stopped intent so
        // the progress watchdog does not read the silence as a stall and
        // rebuild the backend, and so a later recovery cannot replay the
        // final track.
        info!("continuation for {previous_track_id}: raw uri list exhausted; staying stopped");
        self
          .emit_session_acked(SessionFact::NativePlaybackIntentStopped)
          .await;
        return;
      }
    }

    // Check if we are paused/stopped
    let context = self
      .spotify_get_typed::<Option<rspotify::model::CurrentPlaybackContext>>("me/player", &[])
      .await;

    if let Ok(Some(ctx)) = context {
      if !ctx.is_playing {
        let current_id = ctx.item.as_ref().and_then(|item| match item {
          PlayableItem::Track(t) => t.id.as_ref().map(|id| id.id().to_string()),
          PlayableItem::Episode(e) => Some(e.id.id().to_string()),
          _ => None,
        });
        let still_on_finished_track = current_id.as_deref().is_some_and(|current| {
          spotify_item_identity(current) == spotify_item_identity(&previous_track_id)
        }) && ctx
          .progress
          .map(|d: TimeDelta| d.num_milliseconds())
          .unwrap_or(0)
          == 0;

        if still_on_finished_track {
          log::info!(
            "continuation for {previous_track_id}: still on finished track, skipping ahead"
          );
          #[cfg(feature = "streaming")]
          if native_active {
            let native_device_id = match self.fold_query(FoldQuery::NativeDeviceId).await {
              Some(FoldAnswer::NativeDeviceId(native_device_id)) => native_device_id,
              _ => None,
            };
            let Some(native_device_id) = native_device_id else {
              self
                .emit_session_acked(SessionFact::ForceNativeRecovery)
                .await;
              return;
            };
            if let Err(e) = self
              .spotify_api_request_json(
                Method::POST,
                "me/player/next",
                &[("device_id", native_device_id)],
                None,
              )
              .await
            {
              warn!("failed to advance stalled native playback via Web API: {e}");
              self
                .emit_session_acked(SessionFact::ForceNativeRecovery)
                .await;
            }
            return;
          }

          self.next_track().await;
        } else if current_id.is_some() {
          // Spirc may already have selected the next item but left it paused.
          // Resume that item instead of issuing another skip and losing a track.
          log::info!(
            "continuation for {previous_track_id}: resuming the already-selected next item"
          );
          self.start_playback(None, None, None).await;
        } else {
          warn!(
            "continuation for {previous_track_id}: paused with no current item; nothing to resume"
          );
        }
      } else {
        info!("continuation for {previous_track_id}: the API already reports playback running");
      }
    } else {
      warn!(
        "continuation for {previous_track_id}: no playback context from the API; nothing to resume"
      );
    }
  }

  async fn resume_spotify_context(
    &mut self,
    context_uri: Option<String>,
    resume_track_uri: Option<String>,
  ) {
    use crate::infra::network::ids;
    let context = context_uri.as_deref().and_then(ids::play_context_id);
    let track = resume_track_uri.as_deref().and_then(ids::playable_id);

    // Reuse the existing `start_playback` machinery (device activation/transfer
    // included). Passing the resume track as a single-item `uris` alongside the
    // context yields an offset-by-uri start (see `api_playback_offset_json` /
    // `native_load_request`), i.e. the context resumes at that track. A plain
    // `spirc.play()` can't do this after a direct `player.load`, which is why
    // the context is re-loaded here.
    match (context, track) {
      (Some(context), Some(track)) => {
        self
          .start_playback(Some(context), Some(vec![track]), None)
          .await;
      }
      (Some(context), None) => {
        self.start_playback(Some(context), None, None).await;
      }
      (None, Some(track)) => {
        self.start_playback(None, Some(vec![track]), None).await;
      }
      (None, None) => {
        self.emit_status("Queue finished".to_string(), 3).await;
      }
    }
  }

  async fn add_item_to_queue(&mut self, item: PlayableId<'static>) {
    match self
      .spotify_api_request_json(
        Method::POST,
        "me/player/queue",
        &[("uri", item.uri())],
        None,
      )
      .await
    {
      Ok(_) => {
        // Raw write for parity: the old handler stored the message directly
        // (unscaled TTL), not through the status sink.
        self
          .emit_session(SessionFact::TransientStatus {
            message: "Added to queue".to_string(),
            ttl_secs: 3,
          })
          .await;
      }
      Err(e) => {
        #[cfg(feature = "streaming")]
        if suppressed_transient_native_command_error(self, &e).await {
          return;
        }
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn get_queue(&mut self) {
    match self
      .spotify_get_typed::<CurrentUserQueue>("me/player/queue", &[])
      .await
    {
      Ok(q) => {
        use crate::core::app::QueueState;
        use crate::infra::network::mapping;
        let domain_queue = QueueState {
          currently_playing: q
            .currently_playing
            .as_ref()
            .and_then(mapping::playable_info),
          queue: q.queue.iter().filter_map(mapping::playable_info).collect(),
        };
        self
          .emit_session(SessionFact::QueueLanded(Some(domain_queue)))
          .await;
      }
      Err(e) => {
        // Bump on failure too: completion (not success) is what plugin data
        // requests wait on.
        self.emit_session(SessionFact::QueueLanded(None)).await;
        self
          .emit_session(SessionFact::TransientStatus {
            message: "Could not load queue (no active device?)".to_string(),
            ttl_secs: 3,
          })
          .await;
        log::warn!("get_queue failed: {}", e);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use rspotify::model::{
    artist::SimplifiedArtist, idtypes::TrackId, track::FullTrack, SimplifiedAlbum,
  };
  #[cfg(feature = "streaming")]
  use rspotify::model::{device::Device, DeviceType};
  use rspotify::prelude::Id;
  use std::collections::HashMap;

  fn playable_track(id: &str) -> PlayableId<'static> {
    PlayableId::Track(TrackId::from_id(id).unwrap().into_static())
  }

  #[test]
  fn no_active_device_error_matches_spotify_404_payload() {
    let e = anyhow!(
      "{}",
      r#"Spotify API 404 Not Found failed: {
  "error" : {
    "status" : 404,
    "message" : "Player command failed: No active device found",
    "reason" : "NO_ACTIVE_DEVICE"
  }
}"#
    );
    assert!(is_no_active_device_error(&e));
    assert!(!is_no_active_device_error(&anyhow!(
      "Spotify API 404 Not Found failed: Device not found"
    )));
  }

  #[test]
  fn saved_device_retry_needs_the_error_and_a_device_and_no_native_claim() {
    assert_eq!(
      saved_device_retry(true, Some("dev1"), false),
      Some("dev1".to_string())
    );
    assert_eq!(saved_device_retry(true, Some("dev1"), true), None);
    assert_eq!(saved_device_retry(true, None, false), None);
    assert_eq!(saved_device_retry(false, Some("dev1"), false), None);
  }

  #[allow(deprecated)]
  fn full_track(id: &str, name: &str) -> PlayableItem {
    PlayableItem::Track(FullTrack {
      album: SimplifiedAlbum {
        name: "Album".to_string(),
        ..Default::default()
      },
      artists: vec![SimplifiedArtist {
        name: "Artist".to_string(),
        ..Default::default()
      }],
      available_markets: Vec::new(),
      disc_number: 1,
      duration: TimeDelta::milliseconds(180_000),
      explicit: false,
      external_ids: HashMap::new(),
      external_urls: HashMap::new(),
      href: None,
      id: Some(TrackId::from_id(id).unwrap().into_static()),
      is_local: false,
      is_playable: Some(true),
      linked_from: None,
      restrictions: None,
      name: name.to_string(),
      popularity: 50,
      preview_url: None,
      track_number: 1,
      r#type: rspotify::model::Type::Track,
    })
  }

  #[cfg(feature = "streaming")]
  #[allow(deprecated)]
  fn playback_device(id: &str, name: &str) -> Device {
    Device {
      id: Some(id.to_string()),
      is_active: false,
      is_private_session: false,
      is_restricted: false,
      name: name.to_string(),
      _type: DeviceType::Computer,
      volume_percent: Some(50),
    }
  }

  #[cfg(feature = "streaming")]
  fn queued_track(uri: &str) -> crate::core::plugin_api::TrackInfo {
    crate::core::plugin_api::TrackInfo {
      uri: Some(uri.to_string()),
      name: "Queued".to_string(),
      artists: vec!["Artist".to_string()],
      album: "Album".to_string(),
      duration_ms: 180_000,
      id: None,
      album_id: None,
      artist_refs: Vec::new(),
      is_playable: true,
      is_local: false,
      track_number: 1,
      explicit: false,
      image_url: None,
    }
  }

  #[test]
  fn trim_api_playback_uris_leaves_small_requests_unchanged() {
    let uris = vec![
      playable_track("0000000000000000000001"),
      playable_track("0000000000000000000002"),
    ];

    let (trimmed, offset) = trim_api_playback_uris(uris.clone(), Some(1));

    assert_eq!(trimmed, uris);
    assert_eq!(offset, Some(1));
  }

  #[test]
  fn trim_api_playback_uris_keeps_selected_track_inside_window() {
    let uris = (0..150)
      .map(|index| playable_track(&format!("{index:022}")))
      .collect::<Vec<_>>();

    let (trimmed, offset) = trim_api_playback_uris(uris.clone(), Some(60));

    assert_eq!(trimmed.len(), MAX_API_PLAYBACK_URIS);
    assert_eq!(offset, Some(20));
    assert_eq!(trimmed[offset.unwrap()].uri(), uris[60].uri());
  }

  #[test]
  fn trim_api_playback_uris_slides_window_near_end() {
    let uris = (0..150)
      .map(|index| playable_track(&format!("{index:022}")))
      .collect::<Vec<_>>();

    let (trimmed, offset) = trim_api_playback_uris(uris.clone(), Some(149));

    assert_eq!(trimmed.len(), MAX_API_PLAYBACK_URIS);
    assert_eq!(offset, Some(99));
    assert_eq!(trimmed[offset.unwrap()].uri(), uris[149].uri());
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_restore_request_preserves_paused_position_and_options() {
    let snapshot = NativePlaybackRecoverySnapshot {
      generation: 7,
      context_uri: Some("spotify:playlist:context".to_string()),
      uris: None,
      offset: Some(3),
      current_track_uri: Some("spotify:track:current".to_string()),
      loading_track_uri: None,
      track_duration_ms: Some(60_000),
      position_ms: 42_000,
      desired_playing: false,
      shuffle: true,
      repeat: RepeatState::Track,
      recovery_attempts: 1,
    };

    let request = native_restore_load_request(&snapshot).unwrap();

    assert!(!request.start_playing);
    assert_eq!(request.seek_to, 42_000);
    assert!(matches!(
      request.playing_track.as_ref(),
      Some(PlayingTrack::Uri(uri)) if uri == "spotify:track:current"
    ));
    assert!(matches!(
      request.context_options.as_ref(),
      Some(LoadContextOptions::Options(options))
        if options.shuffle && options.repeat && options.repeat_track
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_restore_request_clamps_position_to_track_duration() {
    let snapshot = NativePlaybackRecoverySnapshot {
      generation: 8,
      context_uri: None,
      uris: Some(vec!["spotify:track:finished".to_string()]),
      offset: Some(0),
      current_track_uri: Some("spotify:track:finished".to_string()),
      loading_track_uri: None,
      track_duration_ms: Some(389_094),
      position_ms: 395_562,
      desired_playing: true,
      shuffle: false,
      repeat: RepeatState::Off,
      recovery_attempts: 1,
    };

    let request = native_restore_load_request(&snapshot).unwrap();

    assert_eq!(request.seek_to, 389_093);
  }

  #[test]
  fn api_playback_offset_uses_track_uri_for_context_playback() {
    let uris = vec![
      playable_track("0000000000000000000001"),
      playable_track("0000000000000000000002"),
    ];

    let offset = api_playback_offset_json(Some(&uris), Some(1));

    assert_eq!(
      offset,
      Some(json!({ "uri": "spotify:track:0000000000000000000001" }))
    );
  }

  #[test]
  fn api_playback_offset_uses_position_for_uri_list_playback() {
    let offset = api_playback_offset_json(None, Some(1));

    assert_eq!(offset, Some(json!({ "position": 1 })));
  }

  #[test]
  fn api_playback_offset_falls_back_to_position_when_context_has_no_uri() {
    let offset = api_playback_offset_json(None, Some(3));

    assert_eq!(offset, Some(json!({ "position": 3 })));
  }

  #[test]
  fn api_confirms_native_info_when_names_match() {
    let item = full_track("0000000000000000000001", "Current Song");

    assert!(api_confirms_native_info_is_current(
      "Current Song",
      &item,
      Some("different-id")
    ));
  }

  #[test]
  fn api_confirms_native_info_when_current_id_matches_even_if_name_differs() {
    let item = full_track("0000000000000000000001", "Stranger Thing");

    assert!(api_confirms_native_info_is_current(
      "Greater Together",
      &item,
      Some("0000000000000000000001")
    ));
  }

  #[test]
  fn api_does_not_confirm_stale_api_item_for_different_native_track() {
    let item = full_track("0000000000000000000001", "Old API Song");

    assert!(!api_confirms_native_info_is_current(
      "New Native Song",
      &item,
      Some("0000000000000000000002")
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn no_active_device_error_matches_spotify_no_device_signals() {
    assert!(is_no_active_device_error(&anyhow!(
      "{}",
      r#"Spotify API 404 Not Found failed: {"error":{"reason":"NO_ACTIVE_DEVICE"}}"#
    )));
    assert!(is_no_active_device_error(&anyhow!(
      "Spotify API 404 Not Found failed: No active device found"
    )));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn no_active_device_error_does_not_match_generic_not_found() {
    assert!(!is_no_active_device_error(&anyhow!(
      "Spotify API 404 Not Found failed: playlist not found"
    )));
    assert!(!is_no_active_device_error(&anyhow!(
      "Spotify API 404 failed for https://example.test/404"
    )));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn restriction_violated_error_matches_player_command_signal() {
    assert!(is_restriction_violated_error(&anyhow!(
      "{}",
      r#"Spotify API 403 Forbidden failed: {"error":{"status":403,"message":"Player command failed: Restriction violated","reason":"UNKNOWN"}}"#
    )));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn restriction_violated_error_does_not_match_other_forbidden() {
    assert!(!is_restriction_violated_error(&anyhow!(
      "Spotify API 403 Forbidden failed: Premium required"
    )));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn restriction_violated_error_requires_403_status() {
    // The phrase alone must not classify a non-403 error as a player-command
    // restriction violation.
    assert!(!is_restriction_violated_error(&anyhow!(
      "Some unrelated failure: restriction violated on resource"
    )));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_device_preference_update_persists_when_no_saved_device() {
    assert_eq!(
      native_device_preference_update(None, false, false),
      NativeDevicePreferenceUpdate::Persist
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_device_preference_update_persists_when_explicitly_requested() {
    assert_eq!(
      native_device_preference_update(Some("phone-device"), true, false),
      NativeDevicePreferenceUpdate::Persist
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_device_preference_update_keeps_existing_saved_device_for_fallback() {
    assert_eq!(
      native_device_preference_update(Some("phone-device"), false, false),
      NativeDevicePreferenceUpdate::KeepExistingPreference
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_device_preference_update_refreshes_saved_native_device() {
    assert_eq!(
      native_device_preference_update(Some("old-native-device"), false, true),
      NativeDevicePreferenceUpdate::Persist
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn idle_poll_exposes_native_device_when_it_is_preferred() {
    assert_eq!(
      native_idle_device_preference_update(None, false),
      Some(NativeDevicePreferenceUpdate::Persist)
    );
    assert_eq!(
      native_idle_device_preference_update(Some("old-native-device"), true),
      Some(NativeDevicePreferenceUpdate::Persist)
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn idle_poll_preserves_saved_external_device() {
    assert_eq!(
      native_idle_device_preference_update(Some("phone-device"), false),
      None
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn idle_recovery_is_limited_to_two_spaced_attempts() {
    let mut recovery = NativeIdleRecoveryState::default();
    recovery.observe_player_instance(Some(1));
    let started_at = Instant::now();

    assert!(recovery.should_attempt_idle_recovery(started_at));
    assert!(!recovery.should_attempt_idle_recovery(
      started_at + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL - Duration::from_millis(1)
    ));
    assert!(recovery.should_attempt_idle_recovery(started_at + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL));
    assert!(!recovery.should_attempt_idle_recovery(
      started_at + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn stale_api_playback_does_not_rearm_idle_recovery() {
    let mut recovery = NativeIdleRecoveryState::default();
    recovery.observe_player_instance(Some(1));
    let started_at = Instant::now();

    assert!(recovery.should_attempt_idle_recovery(started_at));
    assert!(recovery.should_attempt_idle_recovery(started_at + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL));
    recovery.observe_player_instance(Some(1));
    assert!(
      !recovery.should_attempt_idle_recovery(started_at + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL * 2)
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn replacement_player_rearms_idle_recovery() {
    let mut recovery = NativeIdleRecoveryState::default();
    recovery.observe_player_instance(Some(1));
    let started_at = Instant::now();
    recovery.settle_current_episode(started_at);

    recovery.observe_player_instance(Some(2));

    assert!(recovery.should_attempt_idle_recovery(started_at + Duration::from_millis(1)));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn explicit_activation_settles_current_idle_episode() {
    let mut recovery = NativeIdleRecoveryState::default();
    recovery.observe_player_instance(Some(1));
    let started_at = Instant::now();

    recovery.settle_current_episode(started_at);

    assert!(
      !recovery.should_attempt_idle_recovery(started_at + NATIVE_IDLE_RECOVERY_RETRY_INTERVAL)
    );
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn spotify_payload_confirms_native_device_by_id() {
    let payload = DevicePayload {
      devices: vec![playback_device("native-device", "spotatui")],
    };

    assert!(spotify_payload_confirms_native_device(
      &payload,
      "native-device"
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn spotify_payload_does_not_confirm_stale_native_name_with_different_id() {
    let payload = DevicePayload {
      devices: vec![playback_device("stale-device", "spotatui")],
    };

    assert!(!spotify_payload_confirms_native_device(
      &payload,
      "native-device"
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_uses_native_when_no_current_device_or_saved_device() {
    assert!(should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: true,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_uses_native_when_saved_device_is_unavailable() {
    assert!(should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: true,
        saved_external_confirmed_available: false,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_keeps_confirmed_external_device() {
    assert!(!should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: true,
        current_device_id_present: true,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_keeps_confirmed_saved_external_device() {
    assert!(!should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: true,
        saved_external_confirmed_available: true,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_uses_native_for_saved_native_device() {
    assert!(should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: true,
        saved_device_matches_native: true,
        saved_external_confirmed_available: true,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_uses_native_for_stale_native_name_match() {
    assert!(should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: true,
        current_device_id_present: true,
        current_device_name_matches_native: true,
        native_has_fresh_activity: false,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_activation_ignores_disconnected_player() {
    assert!(!should_activate_native_for_playback(
      NativeActivationContext {
        player_connected: false,
        saved_device_matches_native: true,
        ..Default::default()
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn recovery_yields_to_an_active_external_device() {
    assert!(should_yield_native_recovery(
      true,
      true,
      Some("phone-device"),
      "native-device"
    ));
    assert!(!should_yield_native_recovery(
      true,
      true,
      Some("native-device"),
      "native-device"
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn startup_and_paused_external_playback_do_not_trigger_recovery_yield() {
    assert!(!should_yield_native_recovery(
      false,
      true,
      Some("phone-device"),
      "native-device"
    ));
    assert!(!should_yield_native_recovery(
      true,
      false,
      Some("phone-device"),
      "native-device"
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn yielding_clears_stale_native_intent_but_keeps_explicit_playback() {
    use crate::infra::queue::QueueNowPlaying;

    let mut app = App::default();
    app.record_native_playback_request(
      None,
      Some(vec!["spotify:track:recover".to_string()]),
      Some(0),
      true,
      false,
      RepeatState::Off,
    );
    app.park_start_playback(Some("spotify:playlist:explicit".to_string()), None, None);
    app.queue_now = Some(QueueNowPlaying::Spotify {
      track: queued_track("spotify:track:queued"),
    });
    app.spotify_queue_guard_reloads = 2;
    app.is_streaming_active = true;
    app.native_activation_pending = true;
    app.last_device_activation = Some(Instant::now());

    clear_native_recovery_intent_for_external_playback(&mut app);

    assert!(!app.is_streaming_active);
    assert!(!app.native_activation_pending);
    assert!(app.last_device_activation.is_none());
    assert!(app.native_playback_recovery.is_none());
    assert!(app.queue_now.is_none());
    assert_eq!(app.spotify_queue_guard_reloads, 0);
    assert!(app.pending_start_playback.is_some());
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn stale_api_item_keeps_native_metadata_when_native_was_active() {
    assert!(stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: true,
        api_item_present: true,
        api_confirms_native_info: false,
        native_track_id_present: true,
        api_item_matches_native_track: false,
        native_streaming_was_active: true,
        native_activation_pending: false,
        api_device_is_native: false,
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn stale_api_item_keeps_native_metadata_during_activation() {
    assert!(stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: true,
        api_item_present: true,
        api_confirms_native_info: false,
        native_track_id_present: true,
        api_item_matches_native_track: false,
        native_streaming_was_active: false,
        native_activation_pending: true,
        api_device_is_native: false,
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn stale_api_item_keeps_native_context_before_native_metadata_arrives() {
    assert!(stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: false,
        api_item_present: true,
        api_confirms_native_info: false,
        native_track_id_present: true,
        api_item_matches_native_track: false,
        native_streaming_was_active: true,
        native_activation_pending: false,
        api_device_is_native: false,
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn stale_native_metadata_clears_after_playback_leaves_native_device() {
    assert!(!stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: true,
        api_item_present: true,
        api_confirms_native_info: false,
        native_track_id_present: true,
        api_item_matches_native_track: false,
        native_streaming_was_active: false,
        native_activation_pending: false,
        api_device_is_native: false,
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn confirmed_api_item_no_longer_keeps_native_metadata() {
    assert!(!stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: true,
        api_item_present: true,
        api_confirms_native_info: true,
        native_track_id_present: true,
        api_item_matches_native_track: true,
        native_streaming_was_active: true,
        native_activation_pending: false,
        api_device_is_native: true,
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn matching_api_item_without_native_metadata_can_update_context() {
    assert!(!stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: false,
        api_item_present: true,
        api_confirms_native_info: false,
        native_track_id_present: true,
        api_item_matches_native_track: true,
        native_streaming_was_active: true,
        native_activation_pending: false,
        api_device_is_native: false,
      },
    ));
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn api_item_without_native_track_id_can_update_context() {
    assert!(!stale_api_item_should_preserve_native_context(
      StaleApiItemContext {
        native_info_present: false,
        api_item_present: true,
        api_confirms_native_info: false,
        native_track_id_present: false,
        api_item_matches_native_track: false,
        native_streaming_was_active: true,
        native_activation_pending: false,
        api_device_is_native: false,
      },
    ));
  }

  /// With neither a context uri nor a resume track, resuming has nothing to do:
  /// it reports the queue as finished rather than issuing a playback request.
  #[tokio::test]
  async fn resume_spotify_context_with_nothing_known_finishes_the_queue() {
    use crate::core::app::App;
    use crate::core::config::ClientConfig;
    use crate::core::user_config::UserConfig;
    use std::sync::mpsc::channel;
    use std::time::SystemTime;

    let (io_tx, _rx) = channel();
    let app = std::sync::Arc::new(tokio::sync::Mutex::new(App::new(
      io_tx,
      UserConfig::new(),
      Some(SystemTime::now()),
    )));
    // No Spotify client is needed: the both-None arm never reaches `spotify()`.
    // Ticket 17: the handler emits the status as a fact; the test plays the
    // fold by draining the events through the same reducer the fold task uses.
    let (spine, mut events_rx, _) = crate::runtime::engine::Spine::open(Default::default());
    let mut network = Network::new(
      None,
      ClientConfig::new(),
      &app,
      std::env::temp_dir().join("spotatui_resume_context_test.json"),
    );
    network.spine = Some(spine);

    network.resume_spotify_context(None, None).await;

    while let Ok(event) = events_rx.try_recv() {
      if let crate::runtime::engine::EngineEvent::Session(fact) = event {
        let mut guard = app.lock().await;
        crate::runtime::frontend::apply_session_fact(&mut guard, fact);
      }
    }

    let guard = app.lock().await;
    assert_eq!(guard.status_message.as_deref(), Some("Queue finished"));
  }

  #[test]
  fn playback_poll_idle_fact_resets_fetching_without_clearing_context_by_default() {
    use crate::core::app::App;
    use crate::core::user_config::UserConfig;
    use std::sync::mpsc::channel;
    use std::time::SystemTime;

    let (io_tx, _rx) = channel();
    let mut app = App::new(io_tx, UserConfig::new(), Some(SystemTime::now()));
    app.is_fetching_current_playback = true;
    app.seek_ms = Some(12_345);
    crate::runtime::frontend::apply_session_fact(
      &mut app,
      SessionFact::PlaybackPollIdle {
        clear_context: false,
      },
    );
    assert!(!app.is_fetching_current_playback);
    assert_eq!(app.seek_ms, None);
  }

  #[cfg(feature = "streaming")]
  #[test]
  fn native_restore_claim_checks_parked_replay_before_recovery() {
    use crate::core::app::{App, PendingStartPlayback};
    use crate::core::user_config::UserConfig;
    use std::sync::mpsc::channel;
    use std::time::{Instant, SystemTime};

    let (io_tx, _rx) = channel();
    let mut app = App::new(io_tx, UserConfig::new(), Some(SystemTime::now()));
    app.pending_start_playback = Some(PendingStartPlayback {
      context_uri: None,
      uris: None,
      offset: None,
      parked_at: Instant::now(),
      recovery_attempts: 0,
    });
    let answer = crate::runtime::frontend::answer_query(
      &mut app,
      FoldQuery::ClaimNativeRestore { generation: 1 },
    );
    assert!(matches!(answer, FoldAnswer::NativeRestore(None)));
    assert!(!app.native_backend_pending);
  }
}
