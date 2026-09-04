//! The serial IoEvent pump: source routing by URI scheme and the service lane.

use crate::infra::network::{IoEvent, Network};

// CLI mode never starts the pump; only a frontend launch does.
#[allow(dead_code)] // TEMPORARY (ticket 01): the windowed frontend still drives this pump.
pub(super) async fn start_tokio(
  io_rx: std::sync::mpsc::Receiver<IoEvent>,
  network: &mut Network,
  mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
  // Bridge the sync dispatch channel onto the async runtime: a dedicated
  // thread does blocking recv() and forwards into a tokio channel the pump can
  // await, replacing the old try_recv() + 5ms sleep poll (which added 0-5ms
  // latency to every event and busy-woke the task while idle).
  let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::unbounded_channel::<IoEvent>();
  std::thread::spawn(move || {
    while let Ok(io_event) = io_rx.recv() {
      if bridge_tx.send(io_event).is_err() {
        break;
      }
    }
  });

  loop {
    let io_event = tokio::select! {
      _ = shutdown_rx.changed() => break,
      maybe_event = bridge_rx.recv() => match maybe_event {
        Some(io_event) => io_event,
        None => break,
      }
    };

    // Source-agnostic service events (telemetry,
    // announcements, friends, stats/recap) only touch `App` and their own HTTP
    // clients — never the Spotify client, pacing, or `Network` state — so they
    // run concurrently on a detached task instead of queueing behind
    // rate-limited Spotify calls on this serial pump (and vice versa).
    if Network::runs_on_service_lane(&io_event) {
      let mut service_network = Network::new(
        None,
        network.client_config.clone(),
        network.spine.clone(),
        network.ytdlp_path.clone(),
        network.token_cache_path.clone(),
      );
      tokio::spawn(async move {
        service_network.handle_network_event(io_event).await;
      });
      continue;
    }

    {
      // The native queue router runs first: it owns `AdvanceNativeQueue` and
      // the queue slot's transport controls, and relinquishes the slot on an
      // unrelated `StartPlayback` (returning false so the per-source
      // teardowns/starts still run). Compiled unconditionally.
      let handled_queue =
        crate::infra::queue::dispatch::route_queue_event(&network.app, &io_event).await;
      // Local-file playback is intercepted before the Spotify network so the
      // network stays Spotify-only (see infra::local::dispatch).
      #[cfg(feature = "local-files")]
      let handled_locally = if !handled_queue {
        match network.spine.as_ref() {
          Some(spine) => crate::infra::local::dispatch::route_local_event(spine, &io_event).await,
          None => false,
        }
      } else {
        false
      };
      #[cfg(not(feature = "local-files"))]
      let handled_locally = false;
      // Subsonic is intercepted after local and before the Spotify network. A
      // `subsonic:` URI falls through the local dispatch (its `is_file_uri` is
      // false) and is caught here (see infra::subsonic::dispatch). Skipped when
      // local already consumed the event.
      #[cfg(feature = "subsonic")]
      let handled_subsonic = if !handled_queue && !handled_locally {
        match network.spine.as_ref() {
          Some(spine) => {
            crate::infra::subsonic::dispatch::route_subsonic_event(spine, &io_event).await
          }
          None => false,
        }
      } else {
        false
      };
      #[cfg(not(feature = "subsonic"))]
      let handled_subsonic = false;
      // Internet radio is intercepted last before the Spotify network. A
      // `radio:` URI falls through both earlier dispatches and is caught here
      // (see infra::radio::dispatch). Skipped when already consumed.
      #[cfg(feature = "internet-radio")]
      let handled_radio = !handled_queue
        && !handled_locally
        && !handled_subsonic
        && if let Some(spine) = network.spine.as_ref() {
          crate::infra::radio::dispatch::route_radio_event(spine, &io_event).await
        } else {
          false
        };
      #[cfg(not(feature = "internet-radio"))]
      let handled_radio = false;
      // YouTube is intercepted last before the Spotify network. A `youtube:`
      // URI falls through the three earlier dispatches and is caught here
      // (see infra::youtube::dispatch). Skipped when already consumed.
      #[cfg(feature = "youtube")]
      let handled_youtube = !handled_queue
        && !handled_locally
        && !handled_subsonic
        && !handled_radio
        && crate::infra::youtube::dispatch::route_youtube_event(&network.app, &io_event).await;
      #[cfg(not(feature = "youtube"))]
      let handled_youtube = false;
      if !handled_queue
        && !handled_locally
        && !handled_subsonic
        && !handled_radio
        && !handled_youtube
      {
        network.handle_network_event(io_event).await;
      } else {
        // A source router consumed the event and returned without touching
        // `is_loading`, which `App::dispatch` set to true. Only
        // `handle_network_event` resets it, and we skipped that path, so clear
        // it here — otherwise selecting/loading Local, Subsonic, Radio, or
        // YouTube content leaves the UI stuck on the loading indicator.
        let _ = network
          .emit_session_acked(crate::runtime::engine::SessionFact::LoadingCleared)
          .await;
      }
    }
  }
}
