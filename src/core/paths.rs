use anyhow::{Context, Result};
use std::{
  ffi::OsString,
  path::{Path, PathBuf},
  sync::OnceLock,
};

const APP_DIR: &str = "spotatui";
const FALLBACK_CONFIG_DIR: [&str; 1] = [".config"];
#[cfg(any(feature = "streaming", test))]
const FALLBACK_CACHE_DIR: [&str; 1] = [".cache"];
const FALLBACK_STATE_DIR: [&str; 2] = [".local", "state"];
#[cfg(feature = "streaming")]
const XDG_CACHE_HOME_ENV: &str = "XDG_CACHE_HOME";
const XDG_CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";
const XDG_STATE_HOME_ENV: &str = "XDG_STATE_HOME";

const DATA_ROOT_CONFIG_DIR: &str = "config";
#[cfg(any(feature = "streaming", test))]
const DATA_ROOT_CACHE_DIR: &str = "cache";
const DATA_ROOT_STATE_DIR: &str = "state";

static DATA_ROOT: OnceLock<PathBuf> = OnceLock::new();

/// Override every directory `paths.rs` resolves (config, cache, state) with
/// subdirectories of `root`, for the lifetime of this process.
///
/// A host application (a second frontend embedding spotatui as a library)
/// calls this once, before anything reads a path, so nothing spotatui
/// persists ever lands under spotatui's own XDG directories. `app_log_dir`
/// is deliberately untouched: a host that sets a data root is expected to
/// own its own logging.
///
/// `OnceLock` is process-global, so a second call cannot silently move
/// already-resolved paths out from under earlier callers. It is a no-op with
/// a `log::warn!` rather than a panic, because failing loudly here would
/// crash a host for a mistake that changes nothing (the first root already
/// won).
///
/// `pub` but unreachable from outside the crate today because `core` is a
/// private module: nothing calls this yet. The planned `frontend` module is
/// its production caller (`pub use crate::core::paths::set_data_root;`), so
/// this stays `#[allow(dead_code)]` rather than `pub(crate)` in the meantime.
#[allow(dead_code)]
pub fn set_data_root(root: PathBuf) {
  if DATA_ROOT.set(root).is_err() {
    log::warn!(
      "spotatui::core::paths::set_data_root called more than once; ignoring the later call"
    );
  }
}

fn data_root() -> Option<&'static PathBuf> {
  DATA_ROOT.get()
}

/// Pure derivation of a data-root subdirectory, kept separate from
/// [`DATA_ROOT`] so tests can exercise the resolution logic with an explicit
/// root instead of relying on the process-global `OnceLock`, which can only
/// be set once per test binary.
fn data_root_subdir(root: &Path, subdir: &str) -> PathBuf {
  root.join(subdir)
}

/// Resolve spotatui's app config directory.
///
/// When [`set_data_root`] has been called, this is `<root>/config`.
/// Otherwise uses `$XDG_CONFIG_HOME/spotatui` when XDG_CONFIG_HOME is set to
/// an absolute path, or preserves the historical `$HOME/.config/spotatui`
/// fallback.
pub(crate) fn app_config_dir() -> Option<PathBuf> {
  if let Some(root) = data_root() {
    return Some(data_root_subdir(root, DATA_ROOT_CONFIG_DIR));
  }

  app_dir_from(
    std::env::var_os(XDG_CONFIG_HOME_ENV),
    dirs::home_dir(),
    &FALLBACK_CONFIG_DIR,
  )
}

/// Resolve spotatui's app cache directory.
///
/// When [`set_data_root`] has been called, this is `<root>/cache`.
/// Otherwise uses `$XDG_CACHE_HOME/spotatui` when XDG_CACHE_HOME is set to an
/// absolute path, or falls back to `$HOME/.cache/spotatui`.
#[cfg(feature = "streaming")]
pub(crate) fn app_cache_dir() -> Option<PathBuf> {
  if let Some(root) = data_root() {
    return Some(data_root_subdir(root, DATA_ROOT_CACHE_DIR));
  }

  app_dir_from(
    std::env::var_os(XDG_CACHE_HOME_ENV),
    dirs::home_dir(),
    &FALLBACK_CACHE_DIR,
  )
}

/// Resolve spotatui's app state directory.
///
/// When [`set_data_root`] has been called, this is `<root>/state`.
/// Otherwise uses `$XDG_STATE_HOME/spotatui` when XDG_STATE_HOME is set to an
/// absolute path, or falls back to `$HOME/.local/state/spotatui`.
pub(crate) fn app_state_dir() -> Option<PathBuf> {
  if let Some(root) = data_root() {
    return Some(data_root_subdir(root, DATA_ROOT_STATE_DIR));
  }

  app_dir_from(
    std::env::var_os(XDG_STATE_HOME_ENV),
    dirs::home_dir(),
    &FALLBACK_STATE_DIR,
  )
}

/// Directory holding this run's log file.
///
/// The OS temp directory rather than the state dir: a log file is written per
/// process id, so they accumulate, and temp is the one location the platform
/// clears on its own. Resolved through `std::env::temp_dir` so Windows lands in
/// `%TEMP%` instead of the literal `/tmp` this used to hard-code, which is
/// drive-relative there and left Windows users looking for a path the app
/// reported but their shell could not find.
pub(crate) fn app_log_dir() -> PathBuf {
  std::env::temp_dir().join("spotatui_logs")
}

/// Path of this process's log file. Callers on both the writing side
/// (`setup_logging`) and the reporting side (the help screen) use this so the
/// path shown to a user is always the path actually written.
pub(crate) fn app_log_path() -> PathBuf {
  app_log_dir().join(format!("spotatuilog{}", std::process::id()))
}

/// Ensure a directory that stores credentials or other private app data exists
/// and is owner-only where supported.
pub(crate) fn ensure_private_dir(dir: &Path) -> Result<()> {
  std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;

  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
      .with_context(|| format!("setting private permissions on {}", dir.display()))?;
  }

  Ok(())
}

fn app_dir_from(
  xdg_home: Option<OsString>,
  home: Option<PathBuf>,
  fallback_dir: &[&str],
) -> Option<PathBuf> {
  xdg_home
    .and_then(valid_xdg_home)
    .or_else(|| {
      home.map(|home| {
        fallback_dir
          .iter()
          .fold(home, |home, component| home.join(component))
      })
    })
    .map(|dir| dir.join(APP_DIR))
}

fn valid_xdg_home(value: OsString) -> Option<PathBuf> {
  if value.is_empty() {
    return None;
  }

  let path = PathBuf::from(value);
  if path.is_absolute() {
    Some(path)
  } else {
    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn test_home() -> PathBuf {
    std::env::temp_dir().join("spotatui-home-alice")
  }

  fn test_xdg_home(name: &str) -> PathBuf {
    std::env::temp_dir().join(name)
  }

  #[test]
  fn app_config_dir_uses_absolute_xdg_config_home() {
    let xdg_home = test_xdg_home("xdg-config");
    let path = app_dir_from(
      Some(xdg_home.clone().into_os_string()),
      Some(test_home()),
      &FALLBACK_CONFIG_DIR,
    );

    assert_eq!(path, Some(xdg_home.join(APP_DIR)));
  }

  #[test]
  fn app_config_dir_falls_back_to_home_when_xdg_is_unset() {
    let home = test_home();
    let path = app_dir_from(None, Some(home.clone()), &FALLBACK_CONFIG_DIR);

    assert_eq!(path, Some(home.join(".config").join(APP_DIR)));
  }

  #[test]
  fn app_config_dir_ignores_empty_or_relative_xdg_config_home() {
    let home = test_home();

    assert_eq!(
      app_dir_from(
        Some(OsString::new()),
        Some(home.clone()),
        &FALLBACK_CONFIG_DIR
      ),
      Some(home.clone().join(".config").join(APP_DIR))
    );
    assert_eq!(
      app_dir_from(
        Some(OsString::from("relative")),
        Some(home.clone()),
        &FALLBACK_CONFIG_DIR
      ),
      Some(home.join(".config").join(APP_DIR))
    );
  }

  #[test]
  fn app_cache_dir_uses_xdg_cache_home_or_home_fallback() {
    let xdg_home = test_xdg_home("xdg-cache");
    assert_eq!(
      app_dir_from(
        Some(xdg_home.clone().into_os_string()),
        Some(test_home()),
        &FALLBACK_CACHE_DIR,
      ),
      Some(xdg_home.join(APP_DIR))
    );

    let home = test_home();
    assert_eq!(
      app_dir_from(None, Some(home.clone()), &FALLBACK_CACHE_DIR,),
      Some(home.join(".cache").join(APP_DIR))
    );
  }

  #[test]
  fn app_state_dir_uses_xdg_state_home_or_home_fallback() {
    let xdg_home = test_xdg_home("xdg-state");
    assert_eq!(
      app_dir_from(
        Some(xdg_home.clone().into_os_string()),
        Some(test_home()),
        &FALLBACK_STATE_DIR,
      ),
      Some(xdg_home.join(APP_DIR))
    );

    let home = test_home();
    assert_eq!(
      app_dir_from(None, Some(home.clone()), &FALLBACK_STATE_DIR,),
      Some(home.join(".local").join("state").join(APP_DIR))
    );
  }

  // `set_data_root`'s backing `OnceLock` is process-global, so it can only be
  // set once per test binary without one test's root leaking into another's
  // assertions. These tests exercise the pure `data_root_subdir` resolution
  // instead of calling `set_data_root`, proving config, cache, and state all
  // derive from an explicit root the same way the real override would.
  #[test]
  fn data_root_subdir_derives_config_cache_and_state_from_the_same_root() {
    let root = std::env::temp_dir().join("spotatui-data-root-alice");

    assert_eq!(
      data_root_subdir(&root, DATA_ROOT_CONFIG_DIR),
      root.join("config")
    );
    assert_eq!(
      data_root_subdir(&root, DATA_ROOT_CACHE_DIR),
      root.join("cache")
    );
    assert_eq!(
      data_root_subdir(&root, DATA_ROOT_STATE_DIR),
      root.join("state")
    );
  }

  #[test]
  fn data_root_subdir_is_scoped_to_the_given_root() {
    let alice = std::env::temp_dir().join("spotatui-data-root-alice");
    let bob = std::env::temp_dir().join("spotatui-data-root-bob");

    assert_ne!(
      data_root_subdir(&alice, DATA_ROOT_CONFIG_DIR),
      data_root_subdir(&bob, DATA_ROOT_CONFIG_DIR)
    );
  }

  // One process-wide assertion that `set_data_root` actually wires
  // `app_config_dir`/`app_cache_dir`/`app_state_dir` to the override, backed
  // by a single call to the real `OnceLock` so it cannot collide with any
  // other test setting it.
  #[test]
  fn set_data_root_overrides_config_cache_and_state_dirs() {
    let root = std::env::temp_dir().join("spotatui-data-root-set-once");
    set_data_root(root.clone());

    assert_eq!(app_config_dir(), Some(root.join("config")));
    #[cfg(feature = "streaming")]
    assert_eq!(app_cache_dir(), Some(root.join("cache")));
    assert_eq!(app_state_dir(), Some(root.join("state")));

    // A second call must not move the paths already resolved above.
    set_data_root(std::env::temp_dir().join("spotatui-data-root-should-be-ignored"));
    assert_eq!(app_config_dir(), Some(root.join("config")));
  }
}
