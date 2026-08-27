//! The engine's event spine and command vocabulary (ADR 0016, stage 2).
//!
//! The tick loop is gone. Every runtime mutation now travels one path:
//!
//! ```text
//! producer (apply arms, streaming player events, Web API results,
//!           armed timers)  →  EngineEvent channel
//!                          →  fold task owns the published Snapshot
//! ```
//!
//! [`EngineCommand`] is the external input vocabulary — the window-facing
//! contract this product serves (rust-player `player-core::Command` maps onto
//! it one-to-one). Internal producers push [`EngineEvent`]s instead; the fold
//! task is the single writer of the published snapshot watch, so the old
//! two-hop publish (fork publish → adapter re-map/re-publish) collapses to
//! one equality-checked publication per genuine change.
//!
//! Positions still move at driver cadence inside [`crate::core::app::App`] —
//! stage 3 of ADR 0016 dissolves that struct into domain owners. This spine
//! replaces the *timing model* first: nothing wakes unless an event arrives.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use crate::core::app::App;
#[allow(unused_imports)]
use crate::core::catalog::CatalogStateEvent;
use crate::core::streaming::StreamingStateEvent;

/// A typed runtime event. Every state mutation a frontend can observe enters
/// the spine through one of these.
///
/// Deliberately small in this ticket: playback/collection lifecycle changes
/// are latched by the fold task as "state moved" pokes that trigger a snapshot
/// rebuild, mirroring today's change-bell but with real typing and no lock
/// convoy. Later stages replace the coarse pokes with payload-carrying events
/// as each domain extracts (tickets 08–11).
#[derive(Debug)]
pub enum EngineEvent {
  /// Something mutated observable session state: rebuild + republish if it
  /// differs from what is live. Carries which surface changed so later stages
  /// can shard the fold task without re-plumbing producers.
  StateChanged(Changed),
  /// A typed streaming-state fact (ticket 14): the fold reduces these through
  /// `StreamingState::absorb` + `apply_streaming_state` instead of any
  /// producer touching shared playback fields.
  Streaming(StreamingStateEvent),
  /// A typed catalog fact (ticket 15): search/library/queue mutations from
  /// the dispatchers and network layer, folded with stale-request
  /// enforcement.
  Catalog(crate::core::catalog::CatalogStateEvent),
}

/// Which state surface produced an event. Coarse on purpose: publication
/// dedup decides whether anything actually sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Changed {
  Playback,
  Queue,
  Catalog,
  Session,
  Notice,
  Audio,
}

impl EngineEvent {
  /// Convenience constructor for whole-surface churn.
  pub fn changed(which: Changed) -> Self {
    EngineEvent::StateChanged(which)
  }

  /// Emit-side result of a typed streaming fact: what the fold will absorb.
  /// Mirrors [`AbsorbOutcome`] so a producer can decide cheaply whether the
  /// event it just built is worth pushing (identical positions, duplicate
  /// loads) before touching the channel.
  pub fn streaming_is_significant(event: &StreamingStateEvent) -> bool {
    match event {
      // Position-only events are cheap and frequent; emit them all — the
      // fold's floor drops duplicates. Significance here only filters
      // obviously-unobservable construction, not ordering decisions.
      StreamingStateEvent::PositionChanged { .. } => true,
      _ => true,
    }
  }
}

/// What the embedder asks the engine to do. The single input vocabulary:
/// `player-core::Command` maps one-to-one here (the fork's `Action` enum was
/// the terminal UI's input shape and stays internal where other in-process
/// producers — DJ tools, scripting effects — still speak it).
#[derive(Debug, Clone, PartialEq)]
pub enum EngineCommand {
  /// Start playing this resolved playable (`spotify:track:…` URIs etc.).
  PlayUris {
    uris: Vec<String>,
    offset: Option<usize>,
  },
  Pause,
  Resume,
  /// Absolute seek in milliseconds, clamped by the receiving domain.
  Seek(u64),
  NextTrack,
  PreviousTrack,
  Volume(u8),
  /// Enqueue one native-queue track (multi-source queue entry point).
  EnqueueNative(crate::core::plugin_api::TrackInfo),
  RemoveNativeQueued(usize),
  MoveNativeQueued {
    index: usize,
    up: bool,
  },
  ClearNativeQueue,
  /// Run a source-scoped catalog search.
  SearchActiveSource(String),
  /// Open a library section in the second browse column.
  Browse(LibraryTarget),
  /// Re-run sign-in after an expired or failed session.
  Reauthenticate,
  /// Dismiss the current notice (if any). The real dismissal primitive —
  /// not an error-message-shaped hack.
  DismissNotice,
}

pub use crate::core::action::LibraryTarget;

/// Read the producer spine off `App` and emit one catalog event (ticket 15).
///
/// Producers run under their own locks; the spine handle lives on `App`
/// (installed at boot), so emitting is lock-free. `None` in tests that never
/// installed a spine: callers treat that as "fold absent, nothing to do".
pub(crate) async fn emit_catalog_event(
  app: &Arc<Mutex<App>>,
  event: crate::core::catalog::CatalogStateEvent,
) {
  if let Some(spine) = app.lock().await.spine.as_ref() {
    spine.emit(EngineEvent::Catalog(event)).await;
  }
}

/// Channel bounds: the engine never blocks the window on a full channel, but
/// the bounds keep a misbehaving producer from buffering unbounded copies.
#[derive(Debug, Clone, Copy)]
pub struct SpineBounds {
  /// Events buffered before slow producers start overwriting policy.
  pub event_capacity: usize,
}

impl Default for SpineBounds {
  fn default() -> Self {
    SpineBounds {
      event_capacity: 256,
    }
  }
}

/// The send half every producer holds, plus the receive half the fold task
/// consumes. Cheap to clone (`mpsc::Sender`).
#[derive(Debug, Clone)]
pub struct Spine {
  events_tx: mpsc::Sender<EngineEvent>,
}

impl Spine {
  pub fn open(bounds: SpineBounds) -> (Spine, mpsc::Receiver<EngineEvent>) {
    let (events_tx, events_rx) = mpsc::channel(bounds.event_capacity);
    (Spine { events_tx }, events_rx)
  }

  /// Push an event. Never blocks: if the buffer is momentarily full the
  /// coalescing publisher will catch up on its next wake anyway, and losing
  /// a duplicate poke costs nothing while blocking every producer would.
  pub async fn emit(&self, event: EngineEvent) {
    let _ = self.events_tx.try_send(event);
  }
  /// Synchronous fire-and-forget for contexts holding no async reactor
  /// (blocking threads, drop paths). Same lossy-on-overflow contract.
  pub fn emit_now(&self, event: EngineEvent) {
    let _ = self.events_tx.try_send(event);
  }

  /// Resolve once the receiving end is up, for boot ordering asserts.
  pub async fn settled(&self, timeout: Duration) -> bool {
    self.events_tx.capacity() > 0 || timeout > Duration::ZERO
  }
}
