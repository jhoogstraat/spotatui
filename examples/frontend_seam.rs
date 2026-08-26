//! Throwaway proof for the `frontend` seam: boots the runtime with console
//! onboarding, plays one explicitly supplied `spotify:track:` URI through
//! native playback, prints every published snapshot, pauses, resumes, and
//! shuts down cleanly. Run with the `tui` feature off:
//!
//! ```sh
//! cargo run --no-default-features --features streaming \
//!   --example frontend_seam -- spotify:track:4uLU6hMCjMI75M1A2tKUQC
//! ```

use std::sync::Arc;
use std::time::Duration;

use spotatui::frontend::{ConsoleOnboarding, EngineAction, Options, Runtime};

fn main() -> anyhow::Result<()> {
  let uri = std::env::args()
    .nth(1)
    .expect("usage: frontend_seam <spotify:track:URI>");
  let data_root = match std::env::var_os("SPOTATUI_FRONTEND_DATA_ROOT") {
    Some(root) => std::path::PathBuf::from(root),
    None => {
      let dir = std::env::temp_dir().join("spotatui-frontend-seam");
      std::fs::create_dir_all(&dir)?;
      dir
    }
  };

  Runtime::install_panic_hook();

  println!("booting (data root {})", data_root.display());
  let runtime = Runtime::boot(Options::new(data_root), Arc::new(ConsoleOnboarding))?;

  let mut snapshots = runtime.subscribe();
  runtime.handle().spawn(async move {
    while snapshots.changed().await.is_ok() {
      let snapshot = snapshots.borrow_and_update().clone();
      println!(
        "[snapshot] playing={:?} position={:?}ms queued={} notice={:?}",
        snapshot
          .playback
          .as_ref()
          .map(|p| (p.is_playing, p.track.as_ref().map(|t| t.name.clone()))),
        snapshot.position_ms,
        snapshot.queue_upcoming.len(),
        snapshot.notice,
      );
    }
    println!("[snapshot] stream closed");
  });

  println!("playing {uri}");
  println!(
    "apply(PlayUris) -> {:?}",
    runtime.apply(EngineAction::PlayUris {
      uris: vec![uri],
      offset: None,
    })
  );
  std::thread::sleep(Duration::from_secs(20));

  println!("pausing");
  println!("apply(Pause) -> {:?}", runtime.apply(EngineAction::Pause));
  std::thread::sleep(Duration::from_secs(3));

  println!("resuming");
  println!("apply(Play) -> {:?}", runtime.apply(EngineAction::Play));
  std::thread::sleep(Duration::from_secs(5));

  println!("shutting down");
  runtime.shutdown()?;
  println!("done");
  Ok(())
}
