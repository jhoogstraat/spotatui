//! Launching the interactive frontend: OS media integrations, the deferred
//! native-streaming startup, persisted-session restore, and the handoff to
//! the terminal UI's event loop.

use super::bootstrap::Boot;
use super::pump::start_tokio;
use crate::core::app::App;
#[cfg(feature = "streaming")]
use crate::core::config::ClientConfig;
use crate::core::user_config::StartupBehavior;
use crate::core::user_config::UserConfig;
#[cfg(feature = "discord-rpc")]
use crate::infra::discord_rpc;
#[cfg(all(feature = "macos-media", target_os = "macos"))]
use crate::infra::macos_media;
#[cfg(all(feature = "mpris", target_os = "linux"))]
use crate::infra::mpris;
#[cfg(feature = "streaming")]
use crate::infra::network::requests::spotify_get_typed_compat_for_with_refresh;
use crate::infra::network::{IoEvent, Network};
#[cfg(feature = "streaming")]
use crate::infra::player;
use crate::runtime::engine::{EngineEvent, SessionFact, Spine};
use anyhow::Result;
use log::info;
#[cfg(feature = "streaming")]
use log::warn;
#[cfg(feature = "streaming")]
use rspotify::{model::user::PrivateUser, AuthCodePkceSpotify};
#[cfg(feature = "streaming")]
use std::path::Path;
#[cfg(feature = "streaming")]
use std::path::PathBuf;
#[cfg(any(feature = "streaming", all(feature = "mpris", target_os = "linux")))]
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
// Used by the streaming OAuth timeout and by `restore_playback_session`'s
// per-source position seeks.
#[cfg(any(
  feature = "streaming",
  feature = "local-files",
  feature = "subsonic",
  feature = "youtube"
))]
use std::time::Duration;
use tokio::sync::Mutex;

/// Queue an OS-originated control through the fold's ordered event spine.
/// Integrations do not mutate `App` or bypass producer ordering.
async fn emit_control(spine: &Spine, event: IoEvent) {
  spine
    .emit(EngineEvent::Session(SessionFact::DispatchIo(vec![event])))
    .await;
}

#[cfg(feature = "discord-rpc")]
type DiscordRpcHandle = Option<discord_rpc::DiscordRpcManager>;
#[cfg(not(feature = "discord-rpc"))]
type DiscordRpcHandle = Option<()>;

#[cfg(feature = "discord-rpc")]
const DEFAULT_DISCORD_CLIENT_ID: &str = "1464235043462447166";

#[cfg(all(feature = "macos-media", target_os = "macos"))]
#[derive(Default, PartialEq)]
struct MacosMetadata {
  title: String,
  artists: Vec<String>,
  album: String,
  duration_ms: u32,
  art_url: Option<String>,
}

#[cfg(all(feature = "windows-media", target_os = "windows"))]
#[derive(Default, PartialEq)]
struct WindowsMetadata {
  title: String,
  artists: Vec<String>,
  album: String,
  duration: u64,
  art_url: Option<String>,
}

#[cfg(feature = "discord-rpc")]
fn resolve_discord_app_id(user_config: &UserConfig) -> Option<String> {
  std::env::var("SPOTATUI_DISCORD_APP_ID")
    .ok()
    .filter(|value| !value.trim().is_empty())
    .or_else(|| user_config.behavior.discord_rpc_client_id.clone())
    .or_else(|| Some(DEFAULT_DISCORD_CLIENT_ID.to_string()))
}

#[cfg(feature = "discord-rpc")]
fn discord_playback(
  snapshot: &crate::infra::media_metadata::PlaybackSnapshot,
) -> discord_rpc::DiscordPlayback {
  discord_rpc::DiscordPlayback {
    title: snapshot.metadata.title.clone(),
    artist: snapshot.primary_artist(),
    album: snapshot.metadata.album.clone(),
    state: if snapshot.is_live {
      "Live".to_string()
    } else if snapshot.is_playing {
      "Playing".to_string()
    } else {
      "Paused".to_string()
    },
    image_url: snapshot.metadata.image_url.clone(),
    duration_ms: snapshot.metadata.duration_ms,
    progress_ms: snapshot.progress_ms,
    is_playing: snapshot.is_playing,
  }
}

#[cfg(all(feature = "mpris", target_os = "linux"))]
fn update_mpris_metadata(
  manager: &mpris::MprisManager,
  last_metadata: &mut Option<(String, Vec<String>, String, u32, Option<String>)>,
  snapshot: Option<crate::infra::media_metadata::PlaybackSnapshot>,
  volume: Option<u8>,
) {
  if let Some(ref snapshot) = snapshot {
    let metadata = (
      snapshot.metadata.title.clone(),
      snapshot.metadata.artists.clone(),
      snapshot.metadata.album.clone(),
      snapshot.metadata.duration_ms,
      snapshot.metadata.image_url.clone(),
    );
    if last_metadata.as_ref() != Some(&metadata) {
      manager.set_metadata(
        &metadata.0,
        &metadata.1,
        &metadata.2,
        metadata.3,
        metadata.4.clone(),
      );
      *last_metadata = Some(metadata);
    }
    manager.set_playback_status(snapshot.is_playing);
    manager.set_position(snapshot.progress_ms as u64);
    manager.set_shuffle(snapshot.shuffle);
    if let Some(repeat) = snapshot.repeat {
      manager.set_loop_status(match repeat {
        rspotify::model::enums::RepeatState::Off => mpris::LoopStatusEvent::None,
        rspotify::model::enums::RepeatState::Track => mpris::LoopStatusEvent::Track,
        rspotify::model::enums::RepeatState::Context => mpris::LoopStatusEvent::Playlist,
      });
    } else {
      manager.set_loop_status(mpris::LoopStatusEvent::None);
    }
  } else {
    last_metadata.take();
    manager.set_stopped();
  }
  if let Some(volume) = volume {
    manager.set_volume(volume);
  }
}

#[cfg(all(feature = "macos-media", target_os = "macos"))]
fn update_macos_metadata(
  manager: &macos_media::MacMediaManager,
  last_metadata: &mut Option<MacosMetadata>,
  snapshot: Option<crate::infra::media_metadata::PlaybackSnapshot>,
) {
  if let Some(ref snapshot) = snapshot {
    let new_metadata = MacosMetadata {
      title: snapshot.metadata.title.clone(),
      artists: snapshot.metadata.artists.clone(),
      album: snapshot.metadata.album.clone(),
      duration_ms: snapshot.metadata.duration_ms,
      art_url: snapshot.metadata.image_url.clone(),
    };

    // Only update if metadata changed to avoid repeated artwork fetches.
    if last_metadata.as_ref() != Some(&new_metadata) {
      manager.set_metadata(
        &snapshot.metadata.title,
        &snapshot.metadata.artists,
        &snapshot.metadata.album,
        snapshot.metadata.duration_ms,
        snapshot.metadata.image_url.clone(),
      );
      *last_metadata = Some(new_metadata);
    }
  } else {
    *last_metadata = None;
    manager.set_stopped();
  }
  if let Some(ref snapshot) = snapshot {
    manager.set_playback_status(snapshot.is_playing);
    manager.set_position(snapshot.progress_ms as u64);
  }
}

#[cfg(all(feature = "windows-media", target_os = "windows"))]
fn update_windows_metadata(
  manager: &smtc_tokio::WindowsMediaManager,
  last_metadata: &mut Option<WindowsMetadata>,
  snapshot: Option<crate::infra::media_metadata::PlaybackSnapshot>,
) {
  if let Some(ref snapshot) = snapshot {
    let new_metadata = WindowsMetadata {
      title: snapshot.metadata.title.clone(),
      artists: snapshot.metadata.artists.clone(),
      album: snapshot.metadata.album.clone(),
      duration: snapshot.metadata.duration_ms as u64,
      art_url: snapshot.metadata.image_url.clone(),
    };

    if last_metadata.as_ref() != Some(&new_metadata) {
      manager.set_metadata(
        &snapshot.metadata.title,
        &snapshot.metadata.artists,
        &snapshot.metadata.album,
        snapshot.metadata.duration_ms as u64,
        snapshot.metadata.image_url,
      );
      *last_metadata = Some(new_metadata);
    }
  } else {
    *last_metadata = None;
    manager.set_stopped();
  }
  if let Some(ref snapshot) = snapshot {
    manager.set_playback_status(snapshot.is_playing);
    manager.set_position(snapshot.progress_ms as u64);
  }
}

#[cfg(feature = "streaming")]
fn subscription_level_label(level: rspotify::model::SubscriptionLevel) -> &'static str {
  match level {
    rspotify::model::SubscriptionLevel::Premium => "premium",
    rspotify::model::SubscriptionLevel::Free => "free",
  }
}

/// Runs after the UI is up (see `deferred_streaming_startup`), so outcomes are
/// reported via `info!` + the returned status message only — never `println!`,
/// which would corrupt the TUI. Reuses the `/me` captured during token
/// validation when available instead of paying a second round trip.
#[cfg(feature = "streaming")]
async fn account_supports_native_streaming(
  spotify: &AuthCodePkceSpotify,
  cached_me: Option<PrivateUser>,
  token_cache_path: &Path,
  app: &Arc<Mutex<App>>,
) -> (bool, Option<&'static str>) {
  let user_result = match cached_me {
    Some(user) => Ok(user),
    None => {
      spotify_get_typed_compat_for_with_refresh::<PrivateUser>(
        spotify,
        "me",
        &[],
        token_cache_path,
        app,
      )
      .await
    }
  };
  match user_result {
    #[allow(deprecated)]
    Ok(user) => match user.product {
      Some(rspotify::model::SubscriptionLevel::Premium) => (true, None),
      Some(level) => {
        let plan = subscription_level_label(level);
        info!(
          "spotify {} account detected: playback is unavailable (native streaming and Web API playback controls require premium)",
          plan
        );
        (
          false,
          Some("Spotify Free account: playback controls unavailable (Premium required)"),
        )
      }
      None => {
        info!("spotify account level unknown: native streaming disabled to avoid librespot exit");
        (
          false,
          Some("Could not verify Spotify plan: native streaming disabled"),
        )
      }
    },
    Err(e) => {
      info!(
        "spotify account level check failed ({}); native streaming disabled to avoid librespot exit",
        e
      );
      (
        false,
        Some("Could not verify Spotify plan: native streaming disabled"),
      )
    }
  }
}

#[cfg(any(feature = "streaming", test))]
#[derive(Debug, PartialEq, Eq)]
enum StartupDeviceEvent {
  Transfer {
    device_id: String,
    persist_device_id: bool,
  },
  AutoSelectStreaming {
    device_name: String,
    persist_device_id: bool,
  },
}

#[cfg(any(feature = "streaming", test))]
#[derive(Debug, PartialEq, Eq)]
struct StartupDeviceDecision {
  event: Option<StartupDeviceEvent>,
  status_message: Option<String>,
}

#[cfg(feature = "streaming")]
impl StartupDeviceEvent {
  fn into_io_event(self) -> IoEvent {
    match self {
      StartupDeviceEvent::Transfer {
        device_id,
        persist_device_id,
      } => IoEvent::TransferPlaybackToDevice(device_id, persist_device_id),
      StartupDeviceEvent::AutoSelectStreaming {
        device_name,
        persist_device_id,
      } => IoEvent::AutoSelectStreamingDevice(device_name, persist_device_id, false),
    }
  }
}

#[cfg(any(feature = "streaming", test))]
fn startup_device_decision(
  startup_behavior: StartupBehavior,
  saved_device_id: Option<String>,
  devices_snapshot: Option<&[rspotify::model::device::Device]>,
  native_device_name: &str,
) -> StartupDeviceDecision {
  if startup_behavior != StartupBehavior::Play {
    return StartupDeviceDecision {
      event: None,
      status_message: None,
    };
  }

  let event = match saved_device_id {
    Some(saved_device_id) => {
      if let Some(devices) = devices_snapshot {
        let mut saved_device_available = false;
        let mut native_device_id = None;

        for device in devices {
          if device.id.as_ref() == Some(&saved_device_id) {
            saved_device_available = true;
            break;
          }

          if native_device_id.is_none() && device.name.eq_ignore_ascii_case(native_device_name) {
            native_device_id = device.id.clone();
          }
        }

        if saved_device_available {
          Some(StartupDeviceEvent::Transfer {
            device_id: saved_device_id,
            persist_device_id: true,
          })
        } else {
          native_device_id.map_or_else(
            || {
              Some(StartupDeviceEvent::AutoSelectStreaming {
                device_name: native_device_name.to_string(),
                persist_device_id: false,
              })
            },
            |device_id| {
              Some(StartupDeviceEvent::Transfer {
                device_id,
                persist_device_id: false,
              })
            },
          )
        }
      } else {
        Some(StartupDeviceEvent::Transfer {
          device_id: saved_device_id,
          persist_device_id: true,
        })
      }
    }
    None => Some(StartupDeviceEvent::AutoSelectStreaming {
      device_name: native_device_name.to_string(),
      persist_device_id: true,
    }),
  };

  let status_message = matches!(
    event,
    Some(
      StartupDeviceEvent::Transfer {
        persist_device_id: false,
        ..
      } | StartupDeviceEvent::AutoSelectStreaming {
        persist_device_id: false,
        ..
      }
    )
  )
  .then(|| format!("Saved device unavailable; using {}", native_device_name));

  StartupDeviceDecision {
    event,
    status_message,
  }
}

/// Everything native streaming needs that used to gate the first frame:
/// account probe, librespot session handshake, player event handler, and the
/// saved-device startup decision. Bundled so `deferred_streaming_startup` can
/// run it all on a background task after the UI is already up.
#[cfg(feature = "streaming")]
struct DeferredStreamingContext {
  app: Arc<Mutex<App>>,
  spotify: AuthCodePkceSpotify,
  cached_me: Option<PrivateUser>,
  token_cache_path: PathBuf,
  client_config: ClientConfig,
  redirect_uri: String,
  volume_percent: u8,
  device_startup_behavior: StartupBehavior,
  /// Spotify startup Play/Pause, run after the device decision so it lands on
  /// the selected device instead of 404ing with NO_ACTIVE_DEVICE while init is
  /// still in flight. `None` when a non-Spotify session restore owns startup.
  spotify_startup_behavior: Option<StartupBehavior>,
  initial_shuffle_enabled: bool,
  recovery_tx: tokio::sync::mpsc::UnboundedSender<player::StreamingRecoveryRequest>,
  shared_position: Arc<AtomicU64>,
  shared_is_playing: Arc<std::sync::atomic::AtomicBool>,
  /// Typed streaming facts from the player event handler go here (ticket 14).
  spine: crate::runtime::engine::Spine,
  /// Cancels deferred startup and its player-event listener during shutdown.
  shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

/// Initialize native streaming in the background (D1). The UI renders its
/// first frame immediately; this task probes the account (reusing the auth
/// `/me` when available), performs the librespot handshake with the same
/// double-timeout as before, stores the player in `App`, spawns the player
/// event handler, and finally makes the saved-device startup decision —
/// dispatching its outcome through the normal IoEvent pump.
#[cfg(feature = "streaming")]
fn deferred_streaming_startup(ctx: DeferredStreamingContext) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    let spine = ctx.spine.clone();
    let spotify_startup_behavior = ctx.spotify_startup_behavior;
    let initial_shuffle_enabled = ctx.initial_shuffle_enabled;
    let mut shutdown_rx = ctx.shutdown_rx.clone();
    let player_task = tokio::select! {
      _ = shutdown_rx.changed() => None,
      task = deferred_streaming_startup_inner(ctx) => task,
    };
    if let Some(mut task) = player_task {
      tokio::select! {
        _ = shutdown_rx.changed() => task.abort(),
        _ = &mut task => {},
      }
    }
    // Whatever happened above (backend up, unsupported account, failed or
    // timed-out init), the pending window is over. Keep startup ordering in
    // the fold so the worker never mutates native App state directly.
    let _ = spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::NativeStartupFinished {
          spotify_startup_behavior,
          initial_shuffle_enabled,
        },
      ))
      .await;
  })
}

#[cfg(feature = "streaming")]
async fn deferred_streaming_startup_inner(ctx: DeferredStreamingContext) -> Option<tokio::task::JoinHandle<()>> {
  let (supported, status_message) =
    account_supports_native_streaming(&ctx.spotify, ctx.cached_me, &ctx.token_cache_path, &ctx.app)
      .await;
  if let Some(message) = status_message {
    let _ = ctx
      .spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::Status {
          message: message.to_string(),
          ttl_secs: 12,
          is_error: false,
        },
      ))
      .await;
  }
  if !supported {
    return None;
  }

  info!("initializing native streaming player");
  let streaming_config = player::StreamingConfig {
    device_name: ctx.client_config.streaming_device_name.clone(),
    bitrate: ctx.client_config.streaming_bitrate,
    audio_cache: ctx.client_config.streaming_audio_cache,
    cache_path: player::get_default_cache_path(),
    initial_volume: ctx.volume_percent,
  };
  let client_id = ctx.client_config.client_id.clone();
  let redirect_uri = ctx.redirect_uri.clone();

  // Internal Spirc timeout defaults to 30s (configurable via
  // SPOTATUI_STREAMING_INIT_TIMEOUT_SECS). The outer timeout here is a safety net
  // that catches hangs *outside* Spirc init (e.g. OAuth callback never arriving,
  // blocking I/O in credential retrieval). Set it above the internal timeout.
  let internal_timeout_secs: u64 = std::env::var("SPOTATUI_STREAMING_INIT_TIMEOUT_SECS")
    .ok()
    .and_then(|v| v.parse().ok())
    .filter(|&v: &u64| v > 0)
    .unwrap_or(30);
  let outer_timeout = Duration::from_secs(internal_timeout_secs.saturating_add(15));

  let init_task = tokio::spawn(async move {
    player::StreamingPlayer::new_cache_only(&client_id, &redirect_uri, streaming_config).await
  });
  let abort_handle = init_task.abort_handle();

  let streaming_player = match tokio::time::timeout(outer_timeout, init_task).await {
    Ok(Ok(Ok(p))) => {
      info!(
        "native streaming player initialized as '{}'",
        p.device_name()
      );
      // Note: We don't activate() here - that's handled by AutoSelectStreamingDevice
      // which respects the user's saved device preference (e.g., spotifyd)
      Arc::new(p)
    }
    Ok(Ok(Err(e))) => {
      info!(
        "failed to initialize streaming: {} - falling back to web api",
        e
      );
      let _ = ctx
        .spine
        .emit(crate::runtime::engine::EngineEvent::Session(
          crate::runtime::engine::SessionFact::Status {
            message: "Native streaming didn't start; using Spotify Connect for now. Restart spotatui to reconnect native playback.".to_string(),
            ttl_secs: 12,
            is_error: false,
          },
        ))
        .await;
      return None;
    }
    Ok(Err(e)) => {
      info!(
        "streaming initialization panicked: {} - falling back to web api",
        e
      );
      return None;
    }
    Err(_) => {
      abort_handle.abort();
      warn!(
        "streaming initialization hung unexpectedly (outer timeout {}s) - falling back to web api",
        outer_timeout.as_secs()
      );
      return None;
    }
  };

  info!("native playback enabled - spotatui is available as a spotify connect device");

  // Install the player and refresh playlists through the fold. The player
  // itself was constructed immutably above; only the fold publishes it.
  let installed = ctx
    .spine
    .emit_acked(crate::runtime::engine::EngineEvent::Native(
      crate::runtime::engine::NativePlaybackFact::Install {
        player: Arc::clone(&streaming_player),
        generation: None,
      },
    ))
    .await
    .and_then(|ack| ack.native)
    .is_some_and(|ack| {
      matches!(
        ack,
        crate::runtime::engine::NativePlaybackAck::Installed { accepted: true, .. }
      )
    });
  if !installed {
    streaming_player.shutdown();
    return None;
  }
  let _ = ctx
    .spine
    .emit(crate::runtime::engine::EngineEvent::Session(
      crate::runtime::engine::SessionFact::DispatchIo(vec![IoEvent::GetPlaylists]),
    ))
    .await;

  // Spawn player event listener (updates app state from native player events)
  let player_task = player::spawn_player_event_handler(player::PlayerEventContext {
    player: Arc::clone(&streaming_player),
    shared_position: ctx.shared_position,
    shared_is_playing: ctx.shared_is_playing,
    spine: ctx.spine.clone(),
    recovery_tx: ctx.recovery_tx,
  });

  // Auto-select the saved playback device when available (fallback to native
  // streaming). This used to run inline in the network task before the pump
  // started; the decision's outcome now dispatches through the pump.
  let device_name = streaming_player.device_name().to_string();
  let saved_device_id = ctx.client_config.device_id.clone();
  let mut devices_snapshot = None;
  if let Ok(devices) =
    spotify_get_typed_compat_for_with_refresh::<rspotify::model::device::DevicePayload>(
      &ctx.spotify,
      "me/player/devices",
      &[],
      &ctx.token_cache_path,
      &ctx.app,
    )
    .await
  {
    let devices_vec = devices.devices;
    let _ = ctx
      .spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::DevicesLanded {
          devices: devices_vec.clone(),
          navigate: false,
        },
      ))
      .await;
    devices_snapshot = Some(devices_vec);
  }

  let startup_decision = startup_device_decision(
    ctx.device_startup_behavior,
    saved_device_id,
    devices_snapshot.as_deref(),
    &device_name,
  );

  if let Some(message) = startup_decision.status_message {
    let _ = ctx
      .spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::Status {
          message,
          ttl_secs: 5,
          is_error: false,
        },
      ))
      .await;
  }
  if let Some(event) = startup_decision.event {
    let _ = ctx
      .spine
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::DispatchIo(vec![event.into_io_event()]),
      ))
      .await;
  }
  Some(player_task)
}

/// What a running set of shared services hands to the frontend that started
/// them. Every frontend consumes the same handles; the windowed `frontend`
/// module drives its own loop on top.
pub(super) struct Services {
  pub app: Arc<Mutex<App>>,
  /// The engine's event spine, opened by `start_services` so every producer
  /// (player events, recovery teardown, catalog dispatchers) shares one
  /// channel; the frontend's fold task takes the receiving half.
  pub spine: crate::runtime::engine::Spine,
  pub spine_rx: tokio::sync::mpsc::Receiver<crate::runtime::engine::SpineMessage>,
  /// The fold-query receive half: the fold task answers producer reads here
  /// (ticket 16), replacing every lock-then-read outside the fold.
  pub spine_queries: crate::runtime::engine::QueryReceiver,
  #[allow(dead_code)] // TEMPORARY (ticket 01): only the deleted TUI runner read it.
  pub user_config: UserConfig,
  #[allow(dead_code)] // TEMPORARY (ticket 01): only the deleted TUI runner read it.
  pub discord_rpc_manager: DiscordRpcHandle,
  pub history_collector: crate::infra::history::HistoryCollectorHandle,
  /// Handles for the long-lived producer tasks. Shutdown cancels the pump and
  /// joins these before asking the fold to flush durable state.
  pub producer_tasks: Vec<tokio::task::JoinHandle<()>>,
  pub shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Start every service a frontend shares: OS media integrations, the
/// deferred native-streaming startup, persisted-session restore, and the
/// network task driving the IoEvent pump. Returns once the services are
/// running; the caller then drives its own event loop until shutdown.
///
/// Split out of the former single `launch_ui` at the handoff to the terminal
/// runner so a second frontend boots through exactly the same sequence.
pub(super) async fn start_services(boot: Boot) -> Result<Services> {
  let app = boot.app;
  // Open the fold spine before any asynchronous startup work so even
  // account/credential notices use typed facts rather than App lock writes.
  let (spine_for_events, spine_rx, spine_queries) =
    crate::runtime::engine::Spine::open(Default::default());
  let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
  app.lock().await.spine = Some(spine_for_events.clone());
  let sync_io_rx = boot.sync_io_rx;
  let user_config = boot.user_config;
  let client_config = boot.client_config;
  let spotify = boot.spotify;
  let final_token_cache_path = boot.token_cache_path;
  let restore_playback = boot.restore_playback;
  #[cfg(feature = "streaming")]
  let restore_queue = boot.restore_queue;
  let initial_shuffle_enabled = boot.initial_shuffle_enabled;
  let initial_startup_behavior = boot.initial_startup_behavior;
  let client_id_notice_message = boot.client_id_notice_message;
  #[cfg(feature = "streaming")]
  let runtime_state = boot.runtime_state;
  #[cfg(feature = "streaming")]
  let onboarding = boot.onboarding;
  #[cfg(feature = "streaming")]
  let cached_me = boot.cached_me;
  #[cfg(feature = "streaming")]
  let selected_redirect_uri = boot.selected_redirect_uri;

  info!("launching interactive terminal ui");
  #[cfg(feature = "streaming")]
  if client_config.enable_streaming && !player::streaming_credentials_are_cached().unwrap_or(false)
  {
    if let Some(spotify) = spotify.as_ref() {
      let (supported, status_message) = account_supports_native_streaming(
        spotify,
        cached_me.clone(),
        &final_token_cache_path,
        &app,
      )
      .await;
      if let Some(message) = status_message {
        let _ = spine_for_events
          .emit(crate::runtime::engine::EngineEvent::Session(
            crate::runtime::engine::SessionFact::Status {
              message: message.to_string(),
              ttl_secs: 12,
              is_error: false,
            },
          ))
          .await;
      }
      if supported {
        // The OAuth flow spins up a blocking local callback server and waits on
        // the browser; keep it off the async reactor so it never ties up a
        // worker thread while the user completes sign-in.
        let onboarding_for_streaming = onboarding.clone();
        let cached = tokio::task::spawn_blocking(move || {
          player::ensure_streaming_credentials_cached(onboarding_for_streaming.as_ref())
        })
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("credential caching task panicked: {e}")));
        if let Err(error) = cached {
          warn!("native streaming authentication unavailable: {error}");
          // Name the actual failure. The generic message left issue #414's
          // reporter with a browser "unable to connect" page, a login that
          // repeated every launch, and nothing on screen or in reach that
          // said which half of startup had failed or why.
          let _ = spine_for_events
            .emit(crate::runtime::engine::EngineEvent::Session(
              crate::runtime::engine::SessionFact::Status {
                message: format!(
                  "Native streaming authentication failed ({error}); using Spotify Connect."
                ),
                ttl_secs: 10,
                is_error: false,
              },
            ))
            .await;
        }
      }
    }
  }
  // Shown here rather than at authentication time: the streaming credential
  // flow above can block on a browser round trip, which would burn the
  // message's TTL before the first frame ever renders. Never displaces a
  // message something else just set (they are more urgent than this notice).
  if let Some(message) = client_id_notice_message {
    let mut app_mut = app.lock().await;
    if app_mut.status_message.is_none() {
      app_mut.set_status_message(message, 15);
    }
  }

  // Opt-in MCP control socket. Nothing listens unless the user asked for it;
  // a bind failure is reported and shrugged off rather than blocking startup,
  // since the player is perfectly usable without it.
  #[cfg(feature = "mcp-server")]
  {
    let (enabled, io_tx) = {
      let app_ref = app.lock().await;
      (
        app_ref.user_config.behavior.mcp_enabled,
        app_ref.io_tx_clone(),
      )
    };
    if enabled {
      match io_tx {
        Some(io_tx) => {
          match crate::infra::mcp::spawn_listener(io_tx, spine_for_events.clone()).await {
            Ok(port) => {
              let _ = spine_for_events
                .emit(crate::runtime::engine::EngineEvent::Session(
                  crate::runtime::engine::SessionFact::Status {
                    message: format!("MCP server listening on 127.0.0.1:{port}"),
                    ttl_secs: 6,
                    is_error: false,
                  },
                ))
                .await;
            }
            Err(e) => {
              log::warn!("MCP: could not start the control socket: {e}");
              let _ = spine_for_events
                .emit(crate::runtime::engine::EngineEvent::Session(
                  crate::runtime::engine::SessionFact::Status {
                    message: format!("MCP server failed to start: {e}"),
                    ttl_secs: 8,
                    is_error: true,
                  },
                ))
                .await;
            }
          }
        }
        None => log::warn!("MCP: no IoEvent sender available; control socket not started"),
      }
    }
  }
  // Native streaming needs a Spotify session; when it will be attempted, the
  // account probe and librespot handshake run in a background task after the
  // UI is up (see `deferred_streaming_startup`) instead of gating the first
  // frame for seconds (worst case tens of seconds).
  #[cfg(feature = "streaming")]
  let streaming_attempted = client_config.enable_streaming && spotify.is_some();

  // Create shared atomic for real-time position updates from native player
  // This avoids lock contention - the player event handler can update position
  // without needing to acquire the app mutex
  #[cfg(any(feature = "streaming", all(feature = "mpris", target_os = "linux")))]
  let shared_position = Arc::new(AtomicU64::new(0));
  #[cfg(feature = "streaming")]
  let shared_position_for_events = Arc::clone(&shared_position);

  // Create shared atomic for playing state (lock-free for MPRIS toggle)
  #[cfg(any(feature = "streaming", all(feature = "mpris", target_os = "linux")))]
  let shared_is_playing = Arc::new(std::sync::atomic::AtomicBool::new(false));
  #[cfg(feature = "streaming")]
  let shared_is_playing_for_events = Arc::clone(&shared_is_playing);
  #[cfg(feature = "streaming")]
  let (streaming_recovery_tx, streaming_recovery_rx) =
    tokio::sync::mpsc::unbounded_channel::<player::StreamingRecoveryRequest>();
  // One spine for every producer: streaming (player events, recovery) plus
  // catalog (dispatchers, network layer). The fold task in `frontend` takes
  // the receiving half via Services.
  let history_collector = crate::infra::history::spawn_history_collector(spine_for_events.clone());
  let mut producer_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
  #[cfg(feature = "streaming")]
  {
    let mut app_mut = app.lock().await;
    app_mut.streaming_recovery_tx = Some(streaming_recovery_tx.clone());
  }

  // Initialize MPRIS D-Bus integration for desktop media control
  // This registers spotatui as a controllable media player on the session bus
  #[cfg(all(feature = "mpris", target_os = "linux"))]
  let mpris_manager: Option<Arc<mpris::MprisManager>> = match mpris::MprisManager::new() {
    Ok(mgr) => {
      info!("mpris d-bus interface registered - media keys and playerctl enabled");
      Some(Arc::new(mgr))
    }
    Err(e) => {
      info!(
        "failed to initialize mpris: {} - media key control disabled",
        e
      );
      None
    }
  };

  // Initialize macOS Now Playing integration for media key control
  // This registers with MPRemoteCommandCenter for media key events
  // Gated on whether streaming will be attempted (the player itself now
  // initializes in the background): registering media keys for a session
  // whose native init later fails is harmless — the handlers just no-op.
  #[cfg(all(feature = "macos-media", target_os = "macos"))]
  let macos_media_manager: Option<Arc<macos_media::MacMediaManager>> = if streaming_attempted {
    match macos_media::MacMediaManager::new() {
      Ok(mgr) => {
        info!("macos now playing interface registered - media keys enabled");
        Some(Arc::new(mgr))
      }
      Err(e) => {
        info!(
          "failed to initialize macos media control: {} - media keys disabled",
          e
        );
        None
      }
    }
  } else {
    None
  };

  #[cfg(all(feature = "windows-media", target_os = "windows"))]
  let windows_media_manager: Option<Arc<smtc_tokio::WindowsMediaManager>> = if streaming_attempted {
    match smtc_tokio::WindowsMediaManager::new() {
      Ok(mgr) => {
        info!("windows smtc com registered - media keys enabled");
        Some(Arc::new(mgr))
      }
      Err(e) => {
        info!(
          "failed to initialize windows smtc com: {} - media keys disabled",
          e
        );
        None
      }
    }
  } else {
    None
  };

  #[cfg(feature = "discord-rpc")]
  let discord_rpc_manager: DiscordRpcHandle = if user_config.behavior.enable_discord_rpc {
    match resolve_discord_app_id(&user_config)
      .and_then(|app_id| discord_rpc::DiscordRpcManager::new(app_id).ok())
    {
      Some(mgr) => {
        info!("discord rich presence enabled");
        Some(mgr)
      }
      None => {
        info!("discord rich presence failed to initialize");
        None
      }
    }
  } else {
    info!("discord rich presence disabled");
    None
  };
  #[cfg(not(feature = "discord-rpc"))]
  let discord_rpc_manager: DiscordRpcHandle = None;

  #[cfg(feature = "discord-rpc")]
  if let Some(ref discord) = discord_rpc_manager {
    let discord = discord.clone();
    let spine_for_discord = spine_for_events.clone();
    let mut activity_rx = spine_for_discord.subscribe_activity();
    tokio::spawn(async move {
      let mut last_snapshot = None;
      loop {
        if activity_rx.changed().await.is_err() {
          break;
        }
        let snapshot = match spine_for_discord
          .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
          .await
        {
          Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
          _ => None,
        };
        if snapshot == last_snapshot {
          continue;
        }
        match snapshot.as_ref() {
          Some(snapshot) => discord.set_activity(&discord_playback(snapshot)),
          None => discord.clear(),
        }
        last_snapshot = snapshot;
      }
    });
  }

  #[cfg(all(feature = "mpris", target_os = "linux"))]
  if let Some(ref mpris) = mpris_manager {
    let mpris_for_metadata = Arc::clone(mpris);
    let spine_for_mpris_metadata = spine_for_events.clone();
    let mut activity_rx = spine_for_mpris_metadata.subscribe_activity();
    tokio::spawn(async move {
      let mut last_metadata = None;
      loop {
        if activity_rx.changed().await.is_err() {
          break;
        }
        let snapshot = match spine_for_mpris_metadata
          .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
          .await
        {
          Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
          _ => None,
        };
        let volume = match spine_for_mpris_metadata
          .query(crate::runtime::engine::FoldQuery::Decoded(
            crate::runtime::engine::DecodedSourceQuery::Volume,
          ))
          .await
        {
          Ok(crate::runtime::engine::FoldAnswer::Decoded(
            crate::runtime::engine::DecodedSourceAnswer::Volume(volume),
          )) => Some(volume),
          _ => None,
        };
        update_mpris_metadata(&mpris_for_metadata, &mut last_metadata, snapshot, volume);
      }
    });
  }

  // Spawn MPRIS event handler to process external control requests (media keys, playerctl)
  #[cfg(all(feature = "mpris", target_os = "linux"))]
  if let Some(ref mpris) = mpris_manager {
    if let Some(event_rx) = mpris.take_event_rx() {
      let mpris_for_seek = Arc::clone(mpris);
      let spine_for_mpris = spine_for_events.clone();
      tokio::spawn(async move {
        handle_mpris_events(event_rx, mpris_for_seek, spine_for_mpris).await;
      });
    }
  }

  // Spawn macOS media event handler to process external control requests (media keys, Control Center)
  #[cfg(all(feature = "macos-media", target_os = "macos"))]
  if let Some(ref macos_media) = macos_media_manager {
    if let Some(event_rx) = macos_media.take_event_rx() {
      let spine_for_macos = spine_for_events.clone();
      tokio::spawn(async move {
        handle_macos_media_events(event_rx, spine_for_macos).await;
      });
    }
  }

  // Keep Now Playing metadata (including artwork URL from Web API playback state)
  // synchronized with Control Center.
  #[cfg(all(feature = "macos-media", target_os = "macos"))]
  if let Some(ref macos_media) = macos_media_manager {
    let macos_media_for_metadata = Arc::clone(macos_media);
    let spine_for_macos_metadata = spine_for_events.clone();
    let mut activity_rx = spine_for_events.subscribe_activity();
    tokio::spawn(async move {
      let mut last_metadata: Option<MacosMetadata> = None;

      loop {
        if activity_rx.changed().await.is_err() {
          break;
        }
        let snapshot = match spine_for_macos_metadata
          .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
          .await
        {
          Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
          _ => None,
        };
        update_macos_metadata(&macos_media_for_metadata, &mut last_metadata, snapshot);
      }
    });
  }

  #[cfg(all(feature = "windows-media", target_os = "windows"))]
  if let Some(ref windows_media) = windows_media_manager {
    if let Some(event_rx) = windows_media.take_event_rx() {
      let spine_for_windows = spine_for_events.clone();
      tokio::spawn(async move {
        handle_windows_media_events(event_rx, spine_for_windows).await;
      });
    }
  }

  #[cfg(all(feature = "windows-media", target_os = "windows"))]
  if let Some(ref windows_media) = windows_media_manager {
    let windows_media_for_metadata = Arc::clone(windows_media);
    let spine_for_windows_metadata = spine_for_events.clone();
    let mut activity_rx = spine_for_events.subscribe_activity();
    tokio::spawn(async move {
      let mut last_metadata: Option<WindowsMetadata> = None;

      loop {
        if activity_rx.changed().await.is_err() {
          break;
        }
        let snapshot = match spine_for_windows_metadata
          .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
          .await
        {
          Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
          _ => None,
        };
        update_windows_metadata(&windows_media_for_metadata, &mut last_metadata, snapshot);
      }
    });
  }

  // Kick off the deferred native-streaming startup: account probe, librespot
  // handshake, player event handler, and saved-device startup decision all
  // run on a background task while the UI renders (see
  // `deferred_streaming_startup`).
  #[cfg(feature = "streaming")]
  if streaming_attempted {
    // When resuming a non-Spotify session, never transfer Spotify playback
    // to a device on startup — that would fight the restored source for the
    // audio output. Treat the device decision as passive (Continue); the
    // device list is still fetched for the UI.
    let device_startup_behavior = if restore_playback.is_some() {
      StartupBehavior::Continue
    } else {
      initial_startup_behavior
    };
    // While init is running, a playback request that finds no active device
    // parks itself for replay instead of erroring (the task clears this and
    // replays whatever parked when it finishes, whatever the outcome).
    let _ = spine_for_events
      .emit(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::NativeBackendPending(true),
      ))
      .await;
    producer_tasks.push(deferred_streaming_startup(DeferredStreamingContext {
      app: Arc::clone(&app),
      spotify: spotify
        .clone()
        .expect("streaming_attempted implies a Spotify session"),
      cached_me,
      token_cache_path: final_token_cache_path.clone(),
      client_config: client_config.clone(),
      redirect_uri: selected_redirect_uri.clone(),
      volume_percent: runtime_state.volume_percent,
      device_startup_behavior,
      // A restored non-Spotify session owns the startup play/pause decision;
      // otherwise the deferred task fires it once the device is selected.
      spotify_startup_behavior: if restore_playback.is_some() {
        None
      } else {
        Some(initial_startup_behavior)
      },
      initial_shuffle_enabled,
      recovery_tx: streaming_recovery_tx.clone(),
      spine: spine_for_events.clone(),
      shared_position: shared_position_for_events,
      shared_is_playing: shared_is_playing_for_events,
      shutdown_rx: shutdown_tx.subscribe(),
    }));
  }

  #[cfg(feature = "streaming")]
  {
    producer_tasks.push(player::spawn_streaming_recovery_handler(player::StreamingRecoveryContext {
      shared_position: Arc::clone(&shared_position),
      shared_is_playing: Arc::clone(&shared_is_playing),
      spine: spine_for_events.clone(),
      recovery_rx: streaming_recovery_rx,
      recovery_tx: streaming_recovery_tx.clone(),
      client_config: client_config.clone(),
      redirect_uri: selected_redirect_uri.clone(),
      shutdown_rx: shutdown_tx.subscribe(),
    }));
  }

  let network_app = Arc::clone(&app);
  let network_spine = spine_for_events.clone();
  info!("spawning spotify network event handler");
  let network_shutdown_rx = shutdown_tx.subscribe();
  let network_task = tokio::spawn(async move {
    let mut network = Network::new(spotify, client_config, &network_app, final_token_cache_path);
    network.spine = Some(network_spine.clone());

    // The saved-device startup decision moved into
    // `deferred_streaming_startup` (it needs the native player's device
    // name, which now materializes in the background).

    // Resume a persisted non-Spotify session if there is one; it honors the
    // startup behavior for its own play/pause decision. Otherwise fall back to
    // the Spotify startup play behavior. Continue is passive and must not
    // transfer devices, change shuffle, or otherwise activate Spotatui.
    // Restore the persisted native queue into app state before the runner
    // starts, independent of whether a source playback is resumed.
    #[cfg(feature = "streaming")]
    if !restore_queue.is_empty() {
      let _ = network_spine
        .emit(crate::runtime::engine::EngineEvent::Session(
          crate::runtime::engine::SessionFact::NativeQueueLanded(restore_queue),
        ))
        .await;
    }
    if let Some(session) = restore_playback {
      // Resume off the event pump: a slow source (yt-dlp download, remote
      // fetch) must not stall the Spotify startup events the UI's first render
      // queues (user, playlists, current playback). The restore drives the
      // source's own start path, which serializes on the `App` lock like every
      // other event, so running it concurrently is safe.
      let restore_app = Arc::clone(&network.app);
      let restore_spine = network_spine.clone();
      tokio::spawn(async move {
        restore_playback_session(
          &restore_app,
          &restore_spine,
          session,
          initial_startup_behavior,
        )
        .await;
      });
    } else if network.spotify.is_some() {
      // Spotify startup play/pause only applies with a Spotify session; a
      // free-source launch has nothing to activate here. When native
      // streaming init is deferred, `deferred_streaming_startup` fires this
      // after the device decision instead — running it here would race the
      // init and 404 with NO_ACTIVE_DEVICE onto the Error screen.
      #[cfg(feature = "streaming")]
      let startup_behavior_runs_here = !streaming_attempted;
      #[cfg(not(feature = "streaming"))]
      let startup_behavior_runs_here = true;
      if startup_behavior_runs_here {
        match initial_startup_behavior {
          StartupBehavior::Continue => {}
          StartupBehavior::Play => {
            network
              .handle_network_event(IoEvent::Shuffle(initial_shuffle_enabled))
              .await;
            network
              .handle_network_event(IoEvent::StartPlayback(None, None, None))
              .await;
          }
          StartupBehavior::Pause => {
            network.handle_network_event(IoEvent::PausePlayback).await;
          }
        }
      }
    }

    start_tokio(sync_io_rx, &mut network, network_shutdown_rx).await;
  });

  Ok(Services {
    spine: spine_for_events.clone(),
    spine_rx,
    spine_queries,
    app,
    user_config,
    discord_rpc_manager,
    history_collector,
    producer_tasks: {
      producer_tasks.push(network_task);
      producer_tasks
    },
    shutdown_tx,
  })
}

/// Pause native playback on the way out so the Connect device does not keep
/// reporting this session as playing after the process exits. Shared by the
/// terminal runner and the `frontend` module.
#[cfg(feature = "streaming")]
pub(crate) async fn pause_native_playback_before_exit(app: &Arc<Mutex<App>>) {
  let player = {
    let mut app = app.lock().await;
    if !app.is_streaming_active {
      return;
    }

    let Some(player) = app.streaming_player.clone() else {
      return;
    };

    let is_playing = app.native_is_playing.unwrap_or_else(|| {
      app
        .current_playback_context
        .as_ref()
        .map(|context| context.is_playing)
        .unwrap_or(false)
    });

    if !is_playing {
      return;
    }

    app.native_is_playing = Some(false);
    if let Some(context) = app.current_playback_context.as_mut() {
      context.is_playing = false;
    }

    player
  };

  player.pause();
  tokio::time::sleep(std::time::Duration::from_millis(150)).await;
}

/// Resume a persisted non-Spotify playback session at launch.
///
/// Drives the source's existing, tested start path (seeding the browse table
/// with the persisted metadata so the snapshot resolves, then dispatching the
/// same `StartPlayback` the keyboard would), and afterwards applies the saved
/// position and the play/pause decision. That decision follows `startup_behavior`:
/// `Play` forces playing, `Pause` forces paused, and `Continue` restores the
/// exact state the session had when it was saved.
///
/// A failed start (removed video, dead network, macOS local playback, missing
/// yt-dlp) publishes no session, so the resume step simply finds nothing and
/// no-ops — startup is never blocked or crashed by a stale session. Any variant
/// whose source feature is disabled in this build is a no-op.
#[allow(unused_variables)]
async fn restore_playback_session(
  app: &Arc<Mutex<App>>,
  spine: &crate::runtime::engine::Spine,
  session: crate::core::persisted_playback::PersistedPlayback,
  startup_behavior: StartupBehavior,
) {
  #[cfg(any(
    feature = "youtube",
    feature = "subsonic",
    feature = "local-files",
    feature = "internet-radio"
  ))]
  use crate::core::persisted_playback::PersistedPlayback;

  // Resolve whether the restored track should end up paused.
  let resolve_paused = |saved_paused: bool| match startup_behavior {
    StartupBehavior::Play => false,
    StartupBehavior::Pause => true,
    StartupBehavior::Continue => saved_paused,
  };

  match session {
    #[cfg(feature = "local-files")]
    PersistedPlayback::Local {
      queue,
      index,
      position_ms,
      paused,
      repeat,
      shuffle_on,
      shuffle,
    } => {
      if queue.is_empty() {
        return;
      }
      // Local reads tags from disk, so the URI queue alone drives the start.
      crate::infra::local::dispatch::route_local_event(
        spine,
        &IoEvent::StartPlayback(None, Some(queue), Some(index)),
      )
      .await;
      let mut guard = app.lock().await;
      // Only adopt the persisted modes if the source actually started; a decode
      // failure would otherwise leave the player-global flags set with no source.
      // Trust the persisted intent (`shuffle_on`) for the flag and restore the
      // backup as-is (the start path above did not re-shuffle because
      // `decoded_shuffle` is still off). When the two disagree — a toggle deferred
      // at exit — the driver's `reconcile_decoded_shuffle` re-syncs the queue
      // order to the flag on the next tick.
      if guard.local_playback.is_some() {
        guard.decoded_repeat = repeat;
        guard.decoded_shuffle = shuffle_on;
      }
      if let Some(s) = guard.local_playback.as_mut() {
        if position_ms > 0 {
          let _ = s.player.seek(Duration::from_millis(position_ms));
        }
        if resolve_paused(paused) {
          s.player.pause();
        }
        s.shuffle_backup = shuffle;
      }
    }
    #[cfg(feature = "subsonic")]
    PersistedPlayback::Subsonic {
      tracks,
      index,
      position_ms,
      paused,
      repeat,
      shuffle_on,
      shuffle,
    } => {
      let uris: Vec<String> = tracks.iter().filter_map(|t| t.uri.clone()).collect();
      if uris.is_empty() {
        return;
      }
      // Seed the browse table so the start path's snapshot resolves metadata.
      app.lock().await.track_table.tracks = tracks;
      crate::infra::subsonic::dispatch::route_subsonic_event(
        spine,
        &IoEvent::StartPlayback(None, Some(uris), Some(index)),
      )
      .await;
      let mut guard = app.lock().await;
      if guard.subsonic_playback.is_some() {
        guard.decoded_repeat = repeat;
        guard.decoded_shuffle = shuffle_on;
      }
      if let Some(s) = guard.subsonic_playback.as_mut() {
        if position_ms > 0 {
          let _ = s.player.seek(Duration::from_millis(position_ms));
        }
        if resolve_paused(paused) {
          s.player.pause();
        }
        s.shuffle_backup = shuffle;
      }
    }
    #[cfg(feature = "youtube")]
    PersistedPlayback::YouTube {
      tracks,
      index,
      position_ms,
      paused,
      repeat,
      shuffle_on,
      shuffle,
    } => {
      let uris: Vec<String> = tracks.iter().filter_map(|t| t.uri.clone()).collect();
      if uris.is_empty() {
        return;
      }
      app.lock().await.track_table.tracks = tracks;
      crate::infra::youtube::dispatch::route_youtube_event(
        app,
        &IoEvent::StartPlayback(None, Some(uris), Some(index)),
      )
      .await;
      let mut guard = app.lock().await;
      if guard.youtube_playback.is_some() {
        guard.decoded_repeat = repeat;
        guard.decoded_shuffle = shuffle_on;
      }
      if let Some(s) = guard.youtube_playback.as_mut() {
        if position_ms > 0 {
          let _ = s.player.seek(Duration::from_millis(position_ms));
        }
        if resolve_paused(paused) {
          s.player.pause();
        }
        s.shuffle_backup = shuffle;
      }
    }
    #[cfg(feature = "internet-radio")]
    PersistedPlayback::Radio { station, paused } => {
      let Some(uri) = station.uri.clone() else {
        return;
      };
      // Seed the browse table so the start path's station snapshot resolves.
      app.lock().await.track_table.tracks = vec![station];
      crate::infra::radio::dispatch::route_radio_event(
        spine,
        &IoEvent::StartPlayback(Some(uri), None, None),
      )
      .await;
      // A live stream has no seekable position; only apply the pause decision.
      let guard = app.lock().await;
      if let Some(s) = guard.radio_playback.as_ref() {
        if resolve_paused(paused) {
          s.player.pause();
        }
      }
    }
    // Any variant whose source feature is disabled in this build.
    #[allow(unreachable_patterns)]
    _ => {}
  }
}

/// Handle MPRIS events from external clients (media keys, playerctl, etc.)
/// Routes to native streaming player when available, or dispatches IoEvents as fallback
#[cfg(all(feature = "mpris", target_os = "linux"))]
async fn handle_mpris_events(
  mut event_rx: tokio::sync::mpsc::UnboundedReceiver<mpris::MprisEvent>,
  mpris_manager: Arc<mpris::MprisManager>,
  spine: Spine,
) {
  use mpris::MprisEvent;

  while let Some(event) = event_rx.recv().await {
    if !matches!(
      spine
        .query(crate::runtime::engine::FoldQuery::MediaKeysEnabled)
        .await,
      Ok(crate::runtime::engine::FoldAnswer::Bool(true))
    ) {
      continue;
    }
    let snapshot = match spine
      .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
      .await
    {
      Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
      _ => None,
    };

    match event {
      MprisEvent::PlayPause => {
        emit_control(
          &spine,
          if snapshot.as_ref().is_some_and(|s| s.is_playing) {
            IoEvent::PausePlayback
          } else {
            IoEvent::StartPlayback(None, None, None)
          },
        )
        .await;
      }
      MprisEvent::Play => emit_control(&spine, IoEvent::StartPlayback(None, None, None)).await,
      MprisEvent::Pause | MprisEvent::Stop => emit_control(&spine, IoEvent::PausePlayback).await,
      MprisEvent::Next => emit_control(&spine, IoEvent::NextTrack).await,
      MprisEvent::Previous => emit_control(&spine, IoEvent::PreviousTrack).await,
      MprisEvent::Seek(offset_micros) => {
        let offset_ms = offset_micros / 1000;
        spine
          .emit(crate::runtime::engine::EngineEvent::Session(
            crate::runtime::engine::SessionFact::RelativeSeek(offset_ms),
          ))
          .await;
      }
      MprisEvent::SetPosition(position_micros) => {
        let new_position_ms = (position_micros / 1000).max(0) as u32;
        emit_control(&spine, IoEvent::Seek(new_position_ms)).await;
        mpris_manager.emit_seeked(new_position_ms as u64);
      }
      MprisEvent::SetShuffle(shuffle) => {
        mpris_manager.set_shuffle(shuffle);
        emit_control(&spine, IoEvent::Shuffle(shuffle)).await;
      }
      MprisEvent::SetLoopStatus(loop_status) => {
        use mpris::LoopStatusEvent;
        use rspotify::model::enums::RepeatState;

        let repeat_state = match loop_status {
          LoopStatusEvent::None => RepeatState::Off,
          LoopStatusEvent::Track => RepeatState::Track,
          LoopStatusEvent::Playlist => RepeatState::Context,
        };
        mpris_manager.set_loop_status(loop_status);
        emit_control(&spine, IoEvent::Repeat(repeat_state)).await;
      }
      MprisEvent::SetVolume(volume_percent) => {
        emit_control(&spine, IoEvent::ChangeVolume(volume_percent)).await
      }
    }
  }
}

/// Handle macOS media events from external sources (media keys, Control Center, AirPods, etc.)
/// Routes control requests to the native streaming player
#[cfg(all(feature = "macos-media", target_os = "macos"))]
async fn handle_macos_media_events(
  mut event_rx: tokio::sync::mpsc::UnboundedReceiver<macos_media::MacMediaEvent>,
  spine: crate::runtime::engine::Spine,
) {
  use macos_media::MacMediaEvent;

  while let Some(event) = event_rx.recv().await {
    if !matches!(
      spine
        .query(crate::runtime::engine::FoldQuery::MediaKeysEnabled)
        .await,
      Ok(crate::runtime::engine::FoldAnswer::Bool(true))
    ) {
      continue;
    }
    let snapshot = match spine
      .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
      .await
    {
      Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
      _ => None,
    };

    match event {
      MacMediaEvent::PlayPause => {
        emit_control(
          &spine,
          if snapshot.as_ref().is_some_and(|s| s.is_playing) {
            IoEvent::PausePlayback
          } else {
            IoEvent::StartPlayback(None, None, None)
          },
        )
        .await;
      }
      MacMediaEvent::Play => emit_control(&spine, IoEvent::StartPlayback(None, None, None)).await,
      MacMediaEvent::Pause | MacMediaEvent::Stop => {
        emit_control(&spine, IoEvent::PausePlayback).await
      }
      MacMediaEvent::Next => emit_control(&spine, IoEvent::NextTrack).await,
      MacMediaEvent::Previous => emit_control(&spine, IoEvent::PreviousTrack).await,
    }
  }
}

#[cfg(all(feature = "windows-media", target_os = "windows"))]
async fn handle_windows_media_events(
  mut event_rx: tokio::sync::mpsc::UnboundedReceiver<smtc_tokio::WindowsMediaEvent>,
  spine: crate::runtime::engine::Spine,
) {
  use smtc_tokio::WindowsMediaEvent;

  while let Some(event) = event_rx.recv().await {
    if !matches!(
      spine
        .query(crate::runtime::engine::FoldQuery::MediaKeysEnabled)
        .await,
      Ok(crate::runtime::engine::FoldAnswer::Bool(true))
    ) {
      continue;
    }
    let snapshot = match spine
      .query(crate::runtime::engine::FoldQuery::MediaSnapshot)
      .await
    {
      Ok(crate::runtime::engine::FoldAnswer::Media(snapshot)) => snapshot,
      _ => None,
    };

    match event {
      WindowsMediaEvent::Play => {
        emit_control(&spine, IoEvent::StartPlayback(None, None, None)).await
      }
      WindowsMediaEvent::Pause | WindowsMediaEvent::Stop => {
        emit_control(&spine, IoEvent::PausePlayback).await
      }
      WindowsMediaEvent::Next => emit_control(&spine, IoEvent::NextTrack).await,
      WindowsMediaEvent::Previous => emit_control(&spine, IoEvent::PreviousTrack).await,
      WindowsMediaEvent::SetPosition(pos) => emit_control(&spine, IoEvent::Seek(pos as u32)).await,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{startup_device_decision, StartupDeviceEvent};
  use crate::core::user_config::StartupBehavior;
  use rspotify::model::{device::Device, DeviceType};

  const NATIVE_NAME: &str = "spotatui";
  const NATIVE_ID: &str = "native-device";
  const EXTERNAL_ID: &str = "phone-device";

  #[allow(deprecated)]
  fn device(id: &str, name: &str) -> Device {
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

  fn startup_device_event(
    startup_behavior: StartupBehavior,
    saved_device_id: Option<String>,
    devices_snapshot: Option<&[Device]>,
  ) -> Option<StartupDeviceEvent> {
    startup_device_decision(
      startup_behavior,
      saved_device_id,
      devices_snapshot,
      NATIVE_NAME,
    )
    .event
  }

  #[test]
  fn continue_without_saved_device_does_not_transfer() {
    let devices = vec![device(NATIVE_ID, NATIVE_NAME)];

    assert_eq!(
      startup_device_event(StartupBehavior::Continue, None, Some(&devices)),
      None
    );
  }

  #[test]
  fn continue_with_saved_native_device_does_not_transfer() {
    let devices = vec![device(NATIVE_ID, NATIVE_NAME)];

    assert_eq!(
      startup_device_event(
        StartupBehavior::Continue,
        Some(NATIVE_ID.to_string()),
        Some(&devices),
      ),
      None
    );
  }

  #[test]
  fn continue_with_saved_external_device_does_not_transfer() {
    let devices = vec![
      device(EXTERNAL_ID, "Jay's phone"),
      device(NATIVE_ID, NATIVE_NAME),
    ];

    assert_eq!(
      startup_device_event(
        StartupBehavior::Continue,
        Some(EXTERNAL_ID.to_string()),
        Some(&devices),
      ),
      None
    );
  }

  #[test]
  fn play_with_saved_available_device_transfers_to_saved_device() {
    let devices = vec![
      device(EXTERNAL_ID, "Jay's phone"),
      device(NATIVE_ID, NATIVE_NAME),
    ];

    assert_eq!(
      startup_device_event(
        StartupBehavior::Play,
        Some(EXTERNAL_ID.to_string()),
        Some(&devices),
      ),
      Some(StartupDeviceEvent::Transfer {
        device_id: EXTERNAL_ID.to_string(),
        persist_device_id: true,
      })
    );
  }

  #[test]
  fn play_without_saved_device_auto_selects_native_fallback() {
    let devices = vec![device(NATIVE_ID, NATIVE_NAME)];

    assert_eq!(
      startup_device_event(StartupBehavior::Play, None, Some(&devices)),
      Some(StartupDeviceEvent::AutoSelectStreaming {
        device_name: NATIVE_NAME.to_string(),
        persist_device_id: true,
      })
    );
  }

  #[test]
  fn continue_with_unavailable_saved_device_does_not_fall_back_to_native() {
    let devices = vec![device(NATIVE_ID, NATIVE_NAME)];

    assert_eq!(
      startup_device_event(
        StartupBehavior::Continue,
        Some(EXTERNAL_ID.to_string()),
        Some(&devices),
      ),
      None
    );
  }

  #[test]
  fn play_with_unavailable_saved_device_transfers_to_native_without_persisting() {
    let devices = vec![device(NATIVE_ID, NATIVE_NAME)];

    let decision = startup_device_decision(
      StartupBehavior::Play,
      Some(EXTERNAL_ID.to_string()),
      Some(&devices),
      NATIVE_NAME,
    );

    assert_eq!(
      decision.event,
      Some(StartupDeviceEvent::Transfer {
        device_id: NATIVE_ID.to_string(),
        persist_device_id: false,
      })
    );
    assert_eq!(
      decision.status_message,
      Some(format!("Saved device unavailable; using {}", NATIVE_NAME))
    );
  }

  #[test]
  fn play_with_unavailable_saved_device_auto_selects_native_without_persisting() {
    let devices = vec![device("other-device", "Other speaker")];

    let decision = startup_device_decision(
      StartupBehavior::Play,
      Some(EXTERNAL_ID.to_string()),
      Some(&devices),
      NATIVE_NAME,
    );

    assert_eq!(
      decision.event,
      Some(StartupDeviceEvent::AutoSelectStreaming {
        device_name: NATIVE_NAME.to_string(),
        persist_device_id: false,
      })
    );
    assert_eq!(
      decision.status_message,
      Some(format!("Saved device unavailable; using {}", NATIVE_NAME))
    );
  }

  #[test]
  fn play_with_saved_device_and_no_snapshot_transfers_to_saved_device() {
    let decision = startup_device_decision(
      StartupBehavior::Play,
      Some(EXTERNAL_ID.to_string()),
      None,
      NATIVE_NAME,
    );

    assert_eq!(
      decision.event,
      Some(StartupDeviceEvent::Transfer {
        device_id: EXTERNAL_ID.to_string(),
        persist_device_id: true,
      })
    );
    assert_eq!(decision.status_message, None);
  }
}
