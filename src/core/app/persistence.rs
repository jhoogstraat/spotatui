use super::*;

/// Debounce window for coalescing hot-path runtime-state saves (volume,
/// shuffle) into one disk write, and the retry delay after a failed save.
const STATE_SAVE_DEBOUNCE_MS: u64 = 500;
const SESSION_SAVE_INTERVAL: Duration = Duration::from_secs(3);

type SessionCompletion = (
  crate::runtime::engine::Spine,
  tokio::runtime::Handle,
  crate::runtime::engine::SessionPersistenceOperation,
);

enum SessionWrite {
  Save {
    path: anyhow::Result<std::path::PathBuf>,
    session: crate::core::persisted_playback::PersistedSession,
    done: Option<std::sync::mpsc::Sender<anyhow::Result<()>>>,
    completion: Option<SessionCompletion>,
  },
  Clear {
    path: anyhow::Result<std::path::PathBuf>,
    done: Option<std::sync::mpsc::Sender<anyhow::Result<()>>>,
    completion: Option<SessionCompletion>,
  },
  Barrier(std::sync::mpsc::Sender<anyhow::Result<()>>),
}

static SESSION_WRITER: std::sync::OnceLock<std::sync::mpsc::Sender<SessionWrite>> =
  std::sync::OnceLock::new();
static SESSION_COMPLETIONS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Mutex<()>>> =
  std::sync::OnceLock::new();

fn session_completion_lock() -> std::sync::Arc<tokio::sync::Mutex<()>> {
  SESSION_COMPLETIONS
    .get_or_init(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
    .clone()
}

fn session_writer() -> &'static std::sync::mpsc::Sender<SessionWrite> {
  SESSION_WRITER.get_or_init(|| {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
      .name("spotatui-session-writer".into())
      .spawn(move || {
        while let Ok(write) = rx.recv() {
          match write {
            SessionWrite::Save {
              path,
              session,
              done,
              completion,
            } => {
              let result =
                path.and_then(|path| crate::core::persisted_playback::save(&path, &session));
              let success = result.is_ok();
              if let Some(done) = done {
                let _ = done.send(result);
              } else if let Err(error) = &result {
                log::warn!("[session] failed to persist playback session: {error}");
              }
              if let Some((spine, handle, operation)) = completion {
                let completion_lock = session_completion_lock();
                // Do not block this ordered writer on the bounded fold queue:
                // shutdown can be waiting for the writer's barrier on that
                // same fold thread.
                handle.spawn(async move {
                  let _guard = completion_lock.lock().await;
                  spine
                    .emit(crate::runtime::engine::EngineEvent::Session(
                      crate::runtime::engine::SessionFact::PersistenceCompleted {
                        operation,
                        success,
                      },
                    ))
                    .await;
                });
              }
            }
            SessionWrite::Clear {
              path,
              done,
              completion,
            } => {
              let result = path.and_then(|path| crate::core::persisted_playback::clear(&path));
              let success = result.is_ok();
              if let Some(done) = done {
                let _ = done.send(result);
              } else if let Err(error) = &result {
                log::warn!("[session] failed to clear playback session: {error}");
              }
              if let Some((spine, handle, operation)) = completion {
                let completion_lock = session_completion_lock();
                handle.spawn(async move {
                  let _guard = completion_lock.lock().await;
                  spine
                    .emit(crate::runtime::engine::EngineEvent::Session(
                      crate::runtime::engine::SessionFact::PersistenceCompleted {
                        operation,
                        success,
                      },
                    ))
                    .await;
                });
              }
            }
            SessionWrite::Barrier(done) => {
              let _ = done.send(Ok(()));
            }
          }
        }
      })
      .expect("spawn persisted-session writer");
    tx
  })
}

fn enqueue_session_write(write: SessionWrite) {
  if session_writer().send(write).is_err() {
    log::warn!("[session] persisted-session writer stopped");
  }
}

impl App {
  /// Return the next explicit deadline for persisted playback. The first save
  /// is immediate; subsequent saves retain the historical three-second
  /// cadence. A transition to no session gets one immediate clear deadline.
  pub(crate) fn persisted_session_deadline(&self) -> Option<Instant> {
    if self.has_persistable_session() {
      Some(self.session_save_due.unwrap_or_else(Instant::now))
    } else if self.session_was_present {
      Some(Instant::now())
    } else {
      None
    }
  }

  /// Handle the named persisted-session deadline. Disk work runs on the
  /// ordered writer thread; the fold only snapshots state and arms deadlines.
  pub(crate) fn flush_persisted_session(&mut self) {
    let Some(deadline) = self.persisted_session_deadline() else {
      return;
    };
    if Instant::now() < deadline {
      return;
    }
    if let Some(session) = self.current_persisted_session() {
      self.session_save_due = Some(Instant::now() + SESSION_SAVE_INTERVAL);
      self.session_was_present = true;
      enqueue_session_write(SessionWrite::Save {
        path: crate::core::persisted_playback::default_session_path(),
        session,
        done: None,
        completion: self
          .persistence_completion(crate::runtime::engine::SessionPersistenceOperation::Save),
      });
    } else if self.session_was_present {
      // Keep a retry deadline while the asynchronous clear is in flight;
      // otherwise the no-session branch would enqueue duplicate clears every
      // scheduler turn before its completion fact arrives.
      self.session_save_due = Some(Instant::now() + SESSION_SAVE_INTERVAL);
      let completion =
        self.persistence_completion(crate::runtime::engine::SessionPersistenceOperation::Clear);
      if completion.is_some() {
        enqueue_session_write(SessionWrite::Clear {
          path: crate::core::persisted_playback::default_session_path(),
          done: None,
          completion,
        });
      } else {
        // App instances without a running fold retain a synchronous fallback.
        self.flush_persisted_session_force();
      }
    }
  }

  /// Synchronously flush the persisted session during shutdown. Unlike the
  /// deadline path this must complete before the runtime is dropped.
  pub(crate) fn flush_persisted_session_force(&mut self) {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    match self.current_persisted_session() {
      Some(session) => enqueue_session_write(SessionWrite::Save {
        path: crate::core::persisted_playback::default_session_path(),
        session,
        done: Some(done_tx),
        completion: None,
      }),
      None if self.session_was_present => enqueue_session_write(SessionWrite::Clear {
        path: crate::core::persisted_playback::default_session_path(),
        done: Some(done_tx),
        completion: None,
      }),
      None => enqueue_session_write(SessionWrite::Barrier(done_tx)),
    }
    let result = done_rx.recv().unwrap_or_else(|_| {
      Err(anyhow!(
        "persisted-session writer stopped before flush completed"
      ))
    });
    if let Err(error) = result {
      log::warn!("[session] failed to flush playback session on shutdown: {error}");
    }
    self.session_save_due = None;
    self.session_was_present = self.current_persisted_session().is_some();
  }

  fn persistence_completion(
    &self,
    operation: crate::runtime::engine::SessionPersistenceOperation,
  ) -> Option<SessionCompletion> {
    Some((
      self.spine.clone()?,
      tokio::runtime::Handle::try_current().ok()?,
      operation,
    ))
  }

  /// Snapshot the currently-playing non-Spotify session for persistence, or
  /// `None` when Spotify (or nothing) owns playback. Reads the live position and
  /// pause state straight from whichever source's player is active, mirroring
  /// the source-ownership order the runner tick uses. Spotify playback is not
  /// persisted here — its resume is handled by `startup_behavior` device logic.
  /// The resume point (`resume_index`, `resume_position_ms`) recorded when the
  /// active decoded context was suspended under the native queue, or `None` when
  /// no decoded context is suspended. Only one context is ever active, so this
  /// unambiguously describes it.
  #[cfg(any(feature = "youtube", feature = "subsonic", feature = "local-files"))]
  fn suspended_resume(&self) -> Option<(Option<usize>, u64)> {
    match self.queue_suspended.as_ref()? {
      #[cfg(feature = "local-files")]
      crate::core::queue::SuspendedContext::Local {
        resume_index,
        resume_position_ms,
      } => Some((*resume_index, *resume_position_ms)),
      #[cfg(feature = "subsonic")]
      crate::core::queue::SuspendedContext::Subsonic {
        resume_index,
        resume_position_ms,
      } => Some((*resume_index, *resume_position_ms)),
      #[cfg(feature = "youtube")]
      crate::core::queue::SuspendedContext::YouTube {
        resume_index,
        resume_position_ms,
      } => Some((*resume_index, *resume_position_ms)),
      #[allow(unreachable_patterns)]
      _ => None,
    }
  }

  pub fn current_persisted_playback(
    &self,
  ) -> Option<crate::core::persisted_playback::PersistedPlayback> {
    if !self.has_persistable_playback() {
      return None;
    }
    #[cfg(any(
      feature = "youtube",
      feature = "subsonic",
      feature = "local-files",
      feature = "internet-radio"
    ))]
    use crate::core::persisted_playback::PersistedPlayback;
    #[cfg(feature = "youtube")]
    if let Some(s) = self.youtube_playback.as_ref() {
      // While suspended under the queue, the context's player is playing a
      // *queued* track, so read the resume point from `queue_suspended` instead
      // of the (repurposed) live player. A `None` resume_index means the context
      // was exhausted — don't persist it (the queue itself still persists).
      match self.suspended_resume() {
        Some((Some(index), position_ms)) => {
          return Some(PersistedPlayback::YouTube {
            tracks: s.tracks.clone(),
            index,
            position_ms,
            paused: false,
            repeat: self.decoded_repeat,
            shuffle_on: self.decoded_shuffle,
            shuffle: s.shuffle_backup.clone(),
          });
        }
        Some((None, _)) => {}
        None => {
          return Some(PersistedPlayback::YouTube {
            tracks: s.tracks.clone(),
            index: s.index,
            position_ms: s.player.position().as_millis() as u64,
            paused: s.player.is_paused(),
            repeat: self.decoded_repeat,
            shuffle_on: self.decoded_shuffle,
            shuffle: s.shuffle_backup.clone(),
          });
        }
      }
    }
    #[cfg(feature = "subsonic")]
    if let Some(s) = self.subsonic_playback.as_ref() {
      match self.suspended_resume() {
        Some((Some(index), position_ms)) => {
          return Some(PersistedPlayback::Subsonic {
            tracks: s.tracks.clone(),
            index,
            position_ms,
            paused: false,
            repeat: self.decoded_repeat,
            shuffle_on: self.decoded_shuffle,
            shuffle: s.shuffle_backup.clone(),
          });
        }
        Some((None, _)) => {}
        None => {
          return Some(PersistedPlayback::Subsonic {
            tracks: s.tracks.clone(),
            index: s.index,
            position_ms: s.player.position().as_millis() as u64,
            paused: s.player.is_paused(),
            repeat: self.decoded_repeat,
            shuffle_on: self.decoded_shuffle,
            shuffle: s.shuffle_backup.clone(),
          });
        }
      }
    }
    #[cfg(feature = "local-files")]
    if let Some(s) = self.local_playback.as_ref() {
      match self.suspended_resume() {
        Some((Some(index), position_ms)) => {
          return Some(PersistedPlayback::Local {
            queue: s.queue.clone(),
            index,
            position_ms,
            paused: false,
            repeat: self.decoded_repeat,
            shuffle_on: self.decoded_shuffle,
            shuffle: s.shuffle_backup.clone(),
          });
        }
        Some((None, _)) => {}
        None => {
          return Some(PersistedPlayback::Local {
            queue: s.queue.clone(),
            index: s.index,
            position_ms: s.player.position().as_millis() as u64,
            paused: s.player.is_paused(),
            repeat: self.decoded_repeat,
            shuffle_on: self.decoded_shuffle,
            shuffle: s.shuffle_backup.clone(),
          });
        }
      }
    }
    #[cfg(feature = "internet-radio")]
    if let Some(s) = self.radio_playback.as_ref() {
      return Some(PersistedPlayback::Radio {
        station: s.station.clone(),
        paused: s.player.is_paused(),
      });
    }
    None
  }

  /// Snapshot the full session to persist: the active non-Spotify playback (if
  /// any) plus the native queue. Returns `None` only when there is nothing to
  /// save (no active source *and* an empty queue), so the caller clears the
  /// session file — preserving the existing Some→None clear semantics.
  pub fn current_persisted_session(
    &self,
  ) -> Option<crate::core::persisted_playback::PersistedSession> {
    let playback = self.current_persisted_playback();
    let queue_now_track = self.queue_now_track();
    if playback.is_none() && self.native_queue.is_empty() && queue_now_track.is_none() {
      return None;
    }
    // A track playing through the queue slot has been popped off `native_queue`;
    // prepend it so a mid-queue quit resumes it (it re-enters the queue on the
    // next launch).
    let mut queue = self.native_queue.clone();
    if let Some(track) = queue_now_track {
      queue.insert(0, track.clone());
    }
    Some(crate::core::persisted_playback::PersistedSession { playback, queue })
  }

  /// Cheap presence check for `current_persisted_session`: same decision
  /// logic, no snapshot allocation. The deadline scheduler uses this to
  /// avoid building persistence state until a save is due.
  pub fn has_persistable_session(&self) -> bool {
    self.has_persistable_playback()
      || !self.native_queue.is_empty()
      || self.queue_now_track().is_some()
  }

  fn has_persistable_playback(&self) -> bool {
    // Mirrors `current_persisted_playback`: a decoded context suspended past
    // its end (resume_index == None) does not persist.
    #[cfg(any(feature = "youtube", feature = "subsonic", feature = "local-files"))]
    {
      let context_resumable = !matches!(self.suspended_resume(), Some((None, _)));
      #[cfg(feature = "youtube")]
      if self.youtube_playback.is_some() && context_resumable {
        return true;
      }
      #[cfg(feature = "subsonic")]
      if self.subsonic_playback.is_some() && context_resumable {
        return true;
      }
      #[cfg(feature = "local-files")]
      if self.local_playback.is_some() && context_resumable {
        return true;
      }
    }
    #[cfg(feature = "internet-radio")]
    if self.radio_playback.is_some() {
      return true;
    }
    false
  }

  /// Schedule a debounced state save. Hot paths (volume, shuffle) call this
  /// instead of `save_runtime_state()` so a held key doesn't pay
  /// disk + YAML work on every auto-repeat; the save lands once, shortly
  /// after the last change.
  pub fn schedule_state_save(&mut self, patch: PersistedRuntimeState) {
    if patch.is_empty() {
      return;
    }
    self.pending_state_save_patch.merge_patch(&patch);
    self.state_save_due = Some(Instant::now() + Duration::from_millis(STATE_SAVE_DEBOUNCE_MS));
  }

  pub fn save_runtime_state(&self, patch: &PersistedRuntimeState) -> anyhow::Result<()> {
    if patch.is_empty() {
      return Ok(());
    }
    let path = match &self.state_path {
      Some(path) => path.clone(),
      None => crate::core::state::default_state_path()?,
    };
    crate::core::state::save(&path, patch)
  }

  fn save_removed_radio_station(&self, url: &str) -> anyhow::Result<()> {
    let path = match &self.state_path {
      Some(path) => path.clone(),
      None => crate::core::state::default_state_path()?,
    };
    crate::core::state::save_removing_radio_station(&path, url)
  }

  pub fn add_radio_station(
    &mut self,
    name: impl AsRef<str>,
    url: impl AsRef<str>,
  ) -> anyhow::Result<RadioStationAddOutcome> {
    if self.is_configured_radio_station_url(url.as_ref()) {
      return Ok(RadioStationAddOutcome::AlreadyExists);
    }
    let before_len = self.runtime_state.radio_stations.len();
    let outcome = self.runtime_state.add_radio_station(name, url)?;
    if outcome == RadioStationAddOutcome::Added {
      let Some(station) = self.runtime_state.radio_stations.get(before_len).cloned() else {
        // `Added` promises the station was appended at `before_len`; if it is
        // not there, don't report success for a station that was never saved.
        self.runtime_state.radio_stations.truncate(before_len);
        return Err(anyhow!(
          "added radio station was not found at the expected position; not persisted"
        ));
      };
      if let Err(error) = self.save_runtime_state(&PersistedRuntimeState::radio_station(station)) {
        self.runtime_state.radio_stations.truncate(before_len);
        return Err(error);
      }
    }
    Ok(outcome)
  }

  pub fn is_configured_radio_station_url(&self, url: &str) -> bool {
    let url = url.trim();
    self.is_config_owned_radio_station_url(url) && !self.is_state_owned_radio_station_url(url)
  }

  pub fn is_config_owned_radio_station_url(&self, url: &str) -> bool {
    let url = url.trim();
    !url.is_empty()
      && self
        .user_config
        .behavior
        .radio_stations
        .iter()
        .any(|station| station.url.trim() == url)
  }

  pub fn is_state_owned_radio_station_url(&self, url: &str) -> bool {
    let url = url.trim();
    !url.is_empty()
      && self
        .runtime_state
        .radio_stations
        .iter()
        .any(|station| station.url.trim() == url)
  }

  pub fn remove_radio_station_by_url(
    &mut self,
    url: impl AsRef<str>,
  ) -> anyhow::Result<Option<RadioStationConfig>> {
    let url = url.as_ref().trim().to_string();
    let Some(index) = self
      .runtime_state
      .radio_stations
      .iter()
      .position(|station| station.url.trim() == url)
    else {
      return Ok(None);
    };
    let removed = self.runtime_state.remove_radio_station_by_url(&url)?;
    if let Err(error) = self.save_removed_radio_station(&url) {
      if let Some(removed) = removed {
        self.runtime_state.radio_stations.insert(index, removed);
      }
      return Err(error);
    }
    Ok(removed)
  }

  /// Flush a scheduled state save once its debounce window has passed, or
  /// immediately when `force` is set (shutdown).
  pub fn flush_state_save(&mut self, force: bool) {
    let Some(due) = self.state_save_due else {
      return;
    };
    if force || Instant::now() >= due {
      let patch = std::mem::take(&mut self.pending_state_save_patch);
      self.state_save_due = None;
      match self.save_runtime_state(&patch) {
        Ok(()) => self.state_save_error_reported = false,
        Err(e) => {
          self.pending_state_save_patch.merge_patch(&patch);
          // Keep the save scheduled: with the deadline cleared the retained patch
          // would never be retried (this fn returns early on a `None` deadline),
          // and the forced shutdown flush would silently drop it.
          self.state_save_due =
            Some(Instant::now() + Duration::from_millis(STATE_SAVE_DEBOUNCE_MS));
          // Reported to the status bar, not as an error page: the tick calls
          // this every frame and the retry above re-arms twice a second, so as
          // a modal an unwritable state dir is an unusable app, and as an
          // `api_error` it is one no frontend can clear because every retry
          // restamps the lifetime. Latched to one report per failure run, or
          // the same rate would pin the status bar into error mode and drop
          // every ordinary message for the rest of the session.
          if !self.state_save_error_reported {
            self.state_save_error_reported = true;
            self.set_error_status_message(format!("Failed to save state: {}", e), 8);
          }
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::core::app::test_support::*;

  /// An `App` whose state saves always fail: the save path's parent is a
  /// regular file, so creating the directory for it cannot succeed on any
  /// platform.
  fn app_with_unwritable_state_path() -> (App, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"not a directory").unwrap();

    let mut app = make_app_simple();
    app.state_path = Some(blocker.join("state.yml"));
    (app, dir)
  }

  fn schedule_a_save(app: &mut App) {
    app.pending_state_save_patch = PersistedRuntimeState {
      volume_percent: Some(42),
      ..PersistedRuntimeState::default()
    };
    app.state_save_due = Some(Instant::now());
  }

  #[test]
  fn a_failed_state_save_reports_to_the_status_bar_instead_of_the_error_page() {
    let (mut app, _dir) = app_with_unwritable_state_path();
    schedule_a_save(&mut app);

    app.flush_state_save(false);

    assert!(app
      .status_message
      .as_deref()
      .is_some_and(|message| message.starts_with("Failed to save state")));
    assert!(app.status_message_is_error);
    // Not the modal: the tick retries twice a second, so an error page here
    // is an app the user cannot escape.
    assert!(app.api_error.is_empty());
    assert_ne!(app.get_current_route().id, RouteId::Error);
  }

  // The regression the latch exists to prevent: an unlatched report re-fires
  // at the tick's retry rate, and a live error message holds
  // `status_message_is_error`, which makes `set_status_message` drop every
  // ordinary message for the rest of the session.
  //
  // Asserted through a sentinel rather than by expiring the message and
  // watching the bar recover: backdating `status_message_expires_at` opens the
  // priority guard whether or not the latch is there, so that version of this
  // test passes with the latch deleted. A second report would replace the
  // sentinel, because an error always overwrites a live error.
  #[test]
  fn repeated_state_save_failures_report_only_once() {
    let (mut app, _dir) = app_with_unwritable_state_path();
    schedule_a_save(&mut app);
    app.flush_state_save(false);
    app.set_error_status_message("sentinel", 8);

    // The retry the failed flush re-armed, as the tick would drive it.
    app.state_save_due = Some(Instant::now());
    app.flush_state_save(false);

    assert_eq!(app.status_message.as_deref(), Some("sentinel"));
  }

  #[test]
  fn a_successful_state_save_re_arms_the_failure_report() {
    let (mut app, dir) = app_with_unwritable_state_path();
    schedule_a_save(&mut app);
    app.flush_state_save(false);
    assert!(app.state_save_error_reported);

    app.state_path = Some(dir.path().join("state.yml"));
    app.state_save_due = Some(Instant::now());
    app.flush_state_save(false);

    assert!(!app.state_save_error_reported);
  }

  #[test]
  fn an_inactive_session_only_arms_a_clear_after_one_was_saved() {
    let mut app = make_app_simple();
    assert!(app.persisted_session_deadline().is_none());

    app.session_was_present = true;
    assert!(app.persisted_session_deadline().is_some());
  }

  #[test]
  fn stale_session_timer_does_not_bypass_the_armed_interval() {
    let mut app = make_app_simple();
    app
      .native_queue
      .push(queue_track(Some("track:queued"), "queued"));
    let deadline = Instant::now() + Duration::from_secs(30);
    app.session_save_due = Some(deadline);

    app.flush_persisted_session();

    assert_eq!(app.session_save_due, Some(deadline));
  }

  #[test]
  fn session_writer_serializes_save_before_clear() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("last_session.yml");
    let session = crate::core::persisted_playback::PersistedSession {
      playback: None,
      queue: vec![queue_track(Some("track:queued"), "queued")],
    };
    let (save_done_tx, save_done_rx) = std::sync::mpsc::channel();
    let (clear_done_tx, clear_done_rx) = std::sync::mpsc::channel();
    enqueue_session_write(SessionWrite::Save {
      path: Ok(path.clone()),
      session,
      done: Some(save_done_tx),
      completion: None,
    });
    enqueue_session_write(SessionWrite::Clear {
      path: Ok(path.clone()),
      done: Some(clear_done_tx),
      completion: None,
    });

    assert!(save_done_rx.recv().unwrap().is_ok());
    assert!(clear_done_rx.recv().unwrap().is_ok());
    assert!(!path.exists());
  }

  #[test]
  fn session_writer_barrier_waits_for_final_save() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("last_session.yml");
    enqueue_session_write(SessionWrite::Save {
      path: Ok(path.clone()),
      session: crate::core::persisted_playback::PersistedSession::default(),
      done: None,
      completion: None,
    });
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    enqueue_session_write(SessionWrite::Barrier(done_tx));
    assert!(done_rx.recv().unwrap().is_ok());
    assert!(path.exists());
  }

  #[tokio::test]
  async fn flush_persistence_does_not_wait_on_completion_before_barrier() {
    let app = std::sync::Arc::new(tokio::sync::Mutex::new(App::default()));
    let (spine, _snapshots) = crate::runtime::frontend::spawn_fold(&app).await;
    let directory = tempfile::tempdir().unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    enqueue_session_write(SessionWrite::Save {
      path: Ok(directory.path().join("last_session.yml")),
      session: crate::core::persisted_playback::PersistedSession::default(),
      done: Some(done_tx),
      completion: Some((
        spine.clone(),
        tokio::runtime::Handle::current(),
        crate::runtime::engine::SessionPersistenceOperation::Save,
      )),
    });
    spine
      .emit_acked(crate::runtime::engine::EngineEvent::Session(
        crate::runtime::engine::SessionFact::FlushPersistence,
      ))
      .await
      .expect("flush should complete while prior completion is pending");
    assert!(done_rx.recv().unwrap().is_ok());
  }
}

// ── Persistence domain interface (ADR 0016 stage 3, ticket 11) ──────────────
//
// Save scheduling/debouncing is the persistence domain's policy, exercised at
// its own seam below. The shared `App` fields remain until the mutex
// dissolution lands; these helpers already express the domain's contract:
// debounce windows coalesce hot-path writes, failures retry with latched
// reporting, and quit-time flushes are forced.

/// The current debounce window for runtime-state saves.
pub const STATE_SAVE_DEBOUNCE: Duration = Duration::from_millis(STATE_SAVE_DEBOUNCE_MS);

#[cfg(test)]
mod persistence_domain_tests {
  use super::*;

  /// A save scheduled twice inside one window coalesces to one deadline —
  /// the debounced-write guarantee the volume/shuffle fast paths rely on.
  fn domain_app() -> App {
    let (tx, _rx) = channel();
    App::new(tx, UserConfig::new(), Some(SystemTime::now()))
  }

  #[test]
  fn schedules_inside_one_window_coalesce() {
    let mut app = domain_app();
    app.schedule_state_save(PersistedRuntimeState {
      volume_percent: Some(10),
      ..PersistedRuntimeState::default()
    });
    assert!(app.state_save_due.is_some());

    std::thread::sleep(Duration::from_millis(5));
    app.schedule_state_save(PersistedRuntimeState {
      volume_percent: Some(20),
      ..PersistedRuntimeState::default()
    });
    // Either re-armed or kept the earlier deadline; never zero deadlines.
    assert!(app.state_save_due.is_some());
  }

  /// The forced flush persists even when no debounce window is pending —
  /// the quit path's durability backstop.
  #[test]
  fn forced_flush_survives_a_missing_deadline() {
    let mut app = domain_app();
    app.state_save_due = None;
    app.flush_state_save(true); // must not panic or schedule work
    assert!(app.state_save_due.is_none());
  }
}
