//! The serial IoEvent pump: source routing by URI scheme and the service lane.

use crate::infra::network::{IoEvent, Network};

fn spawn_event_bridge(
  io_rx: std::sync::mpsc::Receiver<IoEvent>,
  bridge_tx: tokio::sync::mpsc::UnboundedSender<IoEvent>,
) -> std::thread::JoinHandle<()> {
  std::thread::spawn(move || loop {
    match io_rx.recv() {
      Ok(IoEvent::ShutdownPump) | Err(_) => break,
      Ok(io_event) => {
        if bridge_tx.send(io_event).is_err() {
          break;
        }
      }
    }
  })
}

struct EventBridge {
  shutdown_tx: std::sync::mpsc::Sender<IoEvent>,
  thread: Option<std::thread::JoinHandle<()>>,
}

impl EventBridge {
  fn new(
    io_rx: std::sync::mpsc::Receiver<IoEvent>,
    bridge_tx: tokio::sync::mpsc::UnboundedSender<IoEvent>,
    shutdown_tx: std::sync::mpsc::Sender<IoEvent>,
  ) -> Self {
    Self {
      shutdown_tx,
      thread: Some(spawn_event_bridge(io_rx, bridge_tx)),
    }
  }
}

impl Drop for EventBridge {
  fn drop(&mut self) {
    let _ = self.shutdown_tx.send(IoEvent::ShutdownPump);
    if let Some(thread) = self.thread.take() {
      let _ = thread.join();
    }
  }
}

// CLI mode never starts the pump; only a frontend launch does.
#[allow(dead_code)] // TEMPORARY (ticket 01): the windowed frontend still drives this pump.
pub(super) async fn start_tokio(
  io_rx: std::sync::mpsc::Receiver<IoEvent>,
  shutdown_wake_tx: std::sync::mpsc::Sender<IoEvent>,
  network: &mut Network,
  mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
  // Bridge the sync dispatch channel onto the async runtime: a dedicated
  // thread does blocking recv() and forwards into a tokio channel the pump can
  // await. Shutdown sends a final private event to wake the blocking receive.
  let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::unbounded_channel::<IoEvent>();
  let _bridge = EventBridge::new(io_rx, bridge_tx, shutdown_wake_tx);
  let mut service_tasks = tokio::task::JoinSet::new();

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
      service_tasks.spawn(async move {
        service_network.handle_network_event(io_event).await;
      });
      continue;
    }

    {
      // The native queue router runs first: it owns `AdvanceNativeQueue` and
      // the queue slot's transport controls, and relinquishes the slot on an
      // unrelated `StartPlayback` (returning false so the per-source
      // teardowns/starts still run). Compiled unconditionally.
      let handled_queue = match network.spine.as_ref() {
        Some(spine) => crate::infra::queue::dispatch::route_queue_event(spine, &io_event).await,
        None => false,
      };
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
        && if let Some(spine) = network.spine.as_ref() {
          crate::infra::youtube::dispatch::route_youtube_event_spine(spine, &io_event).await
        } else {
          false
        };
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
  service_tasks.abort_all();
  while service_tasks.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn event_bridge_preserves_order_and_wakes_on_shutdown() {
    let (io_tx, io_rx) = std::sync::mpsc::channel();
    let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::unbounded_channel();
    let bridge = EventBridge::new(io_rx, bridge_tx, io_tx.clone());

    io_tx.send(IoEvent::GetCurrentPlayback).unwrap();
    io_tx.send(IoEvent::PausePlayback).unwrap();
    assert!(matches!(
      bridge_rx.recv().await,
      Some(IoEvent::GetCurrentPlayback)
    ));
    assert!(matches!(
      bridge_rx.recv().await,
      Some(IoEvent::PausePlayback)
    ));

    let external_sender = io_tx.clone();
    let mut drop_task = tokio::task::spawn_blocking(move || drop(bridge));
    match tokio::time::timeout(std::time::Duration::from_secs(1), &mut drop_task).await {
      Ok(result) => result.unwrap(),
      Err(_) => {
        external_sender.send(IoEvent::ShutdownPump).unwrap();
        drop_task.await.unwrap();
        panic!("bridge drop should join promptly");
      }
    }
  }

  #[tokio::test]
  async fn event_bridge_drop_wakes_with_external_sender_alive() {
    let (io_tx, io_rx) = std::sync::mpsc::channel();
    let external_sender = io_tx.clone();
    let (bridge_tx, _bridge_rx) = tokio::sync::mpsc::unbounded_channel();
    let bridge = EventBridge::new(io_rx, bridge_tx, io_tx);

    let mut drop_task = tokio::task::spawn_blocking(move || drop(bridge));
    match tokio::time::timeout(std::time::Duration::from_secs(1), &mut drop_task).await {
      Ok(result) => result.unwrap(),
      Err(_) => {
        external_sender.send(IoEvent::ShutdownPump).unwrap();
        drop_task.await.unwrap();
        panic!("bridge drop should join promptly");
      }
    }
    drop(external_sender);
  }
}
