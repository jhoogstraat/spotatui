//! Throwaway audible proof that Spotatui can be driven by a second frontend
//! without the terminal UI compiled in: boots the `frontend` runtime with
//! console onboarding, plays one explicitly supplied `spotify:track:` URI,
//! prints every published snapshot change, pauses, resumes, and shuts down.
//!
//! Run against a Premium account with streaming compiled in:
//!
//! ```sh
//! cargo run --example frontend_demo --no-default-features --features streaming \
//!   -- spotify:track:4uLU6hMCjMI75M1A2tKUQC
//! ```
//!
//! Credentials cache under the data root (override with
//! `SPOTATUI_FRONTEND_DEMO_DATA_ROOT`), so a second run skips both consents.

use std::time::Duration;

use spotatui::frontend::{Action, ConsoleOnboarding, Options, Runtime};

fn main() -> anyhow::Result<()> {
  let uri = std::env::args()
    .nth(1)
    .expect("usage: frontend_demo <spotify:track:URI>");

  let data_root = std::env::var_os("SPOTATUI_FRONTEND_DEMO_DATA_ROOT")
    .map(std::path::PathBuf::from)
    .unwrap_or_else(|| {
      dirs::home_dir()
        .expect("no home directory")
        .join(".cache")
        .join("spotatui-frontend-demo")
    });

  Runtime::install_panic_hook();

  println!("booting runtime (data root: {})", data_root.display());
  let runtime = Runtime::boot(Options::new(data_root), Arc::new(ConsoleOnboarding))?;
  println!("runtime booted");

  {
    let mut snapshots = runtime.subscribe();
    runtime.handle().spawn(async move {
      while snapshots.changed().await.is_ok() {
        let snapshot = snapshots.borrow_and_update().clone();
        let track = snapshot
          .playback
          .as_ref()
          .and_then(|p| p.track.as_ref())
          .map(|t| format!("{} — {}", t.name, t.artists.join(", ")))
          .unwrap_or_else(|| "nothing".to_string());
        println!(
          "[snapshot] track={track} playing={} position={:?}ms queued={} audio_ready={} notice={:?}",
          snapshot.playback.as_ref().map(|p| p.is_playing).unwrap_or(false),
          snapshot.position_ms,
          snapshot.queue_upcoming.len(),
          snapshot.audio_ready,
          snapshot.notice,
        );
      }
      println!("[snapshot] channel closed");
    });
  }

  println!("starting playback of {uri}");
  runtime.apply(Action::PlayUris {
    uris: vec![uri],
    offset: None,
  });
  std::thread::sleep(Duration::from_secs(15));

  println!("pausing");
  runtime.apply(Action::Pause);
  std::thread::sleep(Duration::from_secs(3));

  println!("resuming");
  runtime.apply(Action::Play);
  std::thread::sleep(Duration::from_secs(5));

  println!("shutting down");
  runtime.shutdown()?;
  println!("shutdown complete");
  Ok(())
}

use std::sync::Arc;
