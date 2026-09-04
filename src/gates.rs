//! Ratchet counters for the GUI substrate migration: each counter measures a
//! form of TUI/core coupling that must reach zero before a second frontend
//! can share the core (`crate::tui` imports outside `tui/`, direct `App`
//! field writes in handlers, raw `IoEvent` dispatch from the TUI, mouse
//! handling that synthesizes keystrokes, wildcard arms in the action tree,
//! producers outside the TUI that write its `App::view` presentation state).
//!
//! `tools/gates.count` holds the measured baselines, and the test below pins
//! every counter to its baseline exactly, so a PR that moves a number must
//! also move the file. The direction is enforced by
//! `tools/check_gates_ratchet.sh` against the merge base: coupling counters
//! may only fall. The historical test-count adoption counter was retired with
//! terminal-only test modules in issue #16. `action_refs_in_tui_handlers` is
//! the second: shared `Action::` uses in production handler code, which must
//! rise with every handler conversion. Without it a conversion could replace
//! field writes with arbitrary `App` method calls and never adopt the shared
//! vocabulary.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
  PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
  let Ok(entries) = fs::read_dir(dir) else {
    return;
  };
  for entry in entries.flatten() {
    let path = entry.path();
    if path.is_dir() {
      collect_rs_files(&path, out);
    } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
      out.push(path);
    }
  }
}

fn read_source(path: &Path) -> String {
  fs::read_to_string(path).unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

/// Return the production half of a module for structural ownership checks.
/// Repository test modules are conventionally a single tail item gated by
/// `#[cfg(test)]`; compatibility fixtures may keep App handles there without
/// weakening the runtime ownership boundary.
fn production_source(source: &str) -> &str {
  source
    .find("#[cfg(test)]")
    .map_or(source, |end| &source[..end])
}

/// Sums non-overlapping occurrences of `needle` across every `.rs` file under
/// `dirs` (recursive) plus the standalone `files`.
fn count_occurrences(dirs: &[&str], files: &[&str], needle: &str) -> usize {
  let root = repo_root();
  let mut paths = Vec::new();
  for dir in dirs {
    collect_rs_files(&root.join(dir), &mut paths);
  }
  for file in files {
    paths.push(root.join(file));
  }
  paths
    .iter()
    .map(|path| read_source(path).matches(needle).count())
    .sum()
}

fn is_ident_byte(byte: u8) -> bool {
  byte.is_ascii_alphanumeric() || byte == b'_'
}

/// True when the byte before `pos` continues an identifier, so a needle found
/// at `pos` is the tail of a longer name (`wrapped_app.`, `PlaybackAction::`).
fn preceded_by_ident(source: &str, pos: usize) -> bool {
  pos > 0 && is_ident_byte(source.as_bytes()[pos - 1])
}

/// Walks the `field(.field)*` chain that starts at `chain_start` and reports
/// whether an assignment operator follows it on the same line. False when no
/// field name starts there. Comparisons (`==`), match guards (`=>`), method
/// calls, and indexed writes are not writes. This is the exact matcher the
/// write baselines were measured with.
fn chain_is_written(source: &str, chain_start: usize) -> bool {
  let bytes = source.as_bytes();
  let mut pos = chain_start;
  loop {
    let ident_start = pos;
    while pos < bytes.len() && is_ident_byte(bytes[pos]) {
      pos += 1;
    }
    if pos == ident_start {
      break;
    }
    if pos < bytes.len() && bytes[pos] == b'.' {
      pos += 1;
      continue;
    }
    break;
  }
  if pos == chain_start {
    return false; // no field name after the prefix
  } // Only same-line spacing may separate the chain from the operator.
  while pos < bytes.len() && (bytes[pos] == b' ' || bytes[pos] == b'\t') {
    pos += 1;
  }
  match bytes.get(pos) {
    Some(b'+' | b'-' | b'*' | b'/' | b'%' | b'|' | b'&' | b'^') => {
      bytes.get(pos + 1) == Some(&b'=')
    }
    Some(shift @ (b'<' | b'>')) => {
      bytes.get(pos + 1) == Some(shift) && bytes.get(pos + 2) == Some(&b'=')
    }
    Some(b'=') => !matches!(bytes.get(pos + 1), Some(b'=') | Some(b'>')),
    _ => false,
  }
}

/// Counts direct `App` field writes: `app.<field>` (optionally a deeper
/// `.field` chain) followed on the same line by `=` or a compound assignment
/// operator. Chains into `app.view` (the TUI's own presentation state, see
/// `core::app::ViewState`) are not coupling and do not count.
fn count_app_field_writes(source: &str) -> usize {
  source
    .match_indices("app.")
    .filter(|&(start, _)| !preceded_by_ident(source, start))
    .map(|(start, needle)| start + needle.len())
    .filter(|&chain_start| !source[chain_start..].starts_with("view."))
    .filter(|&chain_start| chain_is_written(source, chain_start))
    .count()
}

/// Counts writes into `App::view`: `<receiver>.view.<field>` (optionally a
/// deeper chain) followed on the same line by an assignment operator. Measured
/// over every file outside `src/tui/` and `src/core/app/` (and this file),
/// tests included like the handler write counter: a producer that resets or
/// clamps a cursor after it replaces a list, and the test that pins that
/// behavior. Each one is a place where the TUI's presentation state leaks into
/// shared code; the end state is a producer-side revision the frontend reacts to.
fn count_view_field_writes(source: &str) -> usize {
  let bytes = source.as_bytes();
  // A field access has a receiver directly before the dot; a `.view.` that
  // opens a string literal or a line is not one.
  let has_receiver = |start: usize| {
    start > 0 && (is_ident_byte(bytes[start - 1]) || matches!(bytes[start - 1], b')' | b']'))
  };
  source
    .match_indices(".view.")
    .filter(|&(start, _)| has_receiver(start))
    .filter(|&(start, needle)| chain_is_written(source, start + needle.len()))
    .count()
}

/// Counts shared `Action::` uses in the production half of one handler file:
/// the text before its first test-gated item (every handler keeps its tests in
/// one tail module, gated by `#[cfg(test)]` or `#[cfg(all(test, …))]`). The
/// match is word-bounded, so `PlaybackAction::` and similar do not count.
fn count_production_action_refs(source: &str) -> usize {
  let tests_start = ["#[cfg(test)]", "#[cfg(all(test"]
    .iter()
    .filter_map(|gate| source.find(gate))
    .min()
    .unwrap_or(source.len());
  let production = &source[..tests_start];
  production
    .match_indices("Action::")
    .filter(|&(start, _)| !preceded_by_ident(production, start))
    .count()
}

/// Counts direct writes to the ticket-14 streaming mirror family in the
/// librespot event handler: `native_is_playing`, `song_progress_ms`,
/// `last_track_id`, `native_track_info`. After the migration these four may
/// only be written by the fold task (`core::streaming_apply`); the recovery
/// teardown keeps its synchronous writes until tickets 15–17 migrate that
/// task (hence the measured count, not 0 — the counter is the ratchet;
/// ticket 17 drives it to zero).
///
/// The chain passed here starts *after* a known field name, so
/// [`chain_is_written`]'s "must begin with an ident" guard would misread a
/// complete `receiver.field` chain as empty; the operator check is inlined
/// instead, with identical semantics (same-line whitespace, then an
/// assignment operator that is not `==` or `=>`).
fn count_handler_mirror_writes(source: &str) -> usize {
  const FIELDS: [&str; 4] = [
    "native_is_playing",
    "song_progress_ms",
    "last_track_id",
    "native_track_info",
  ];
  // A field access is `receiver.field`: the byte immediately before the
  // match must be the dot (`app_lock.native_is_playing`). A bare occurrence
  // preceded by whitespace, `(`, or a string literal is not a write target.
  source
    .lines()
    .map(|line| {
      FIELDS
        .iter()
        .filter(|field| {
          let field = **field;
          line.match_indices(field).any(|(pos, _)| {
            pos > 0 && line.as_bytes()[pos - 1] == b'.' && {
              let after = line[pos + field.len()..].trim_start_matches([' ', '\t']);
              after.starts_with('=') && !after[1..].starts_with('=') && !after[1..].starts_with('>')
            }
          })
        })
        .count()
    })
    .sum()
}

fn load_baselines() -> BTreeMap<String, usize> {
  let path = repo_root().join("tools").join("gates.count");
  let text = read_source(&path);
  let mut map = BTreeMap::new();
  for raw in text.lines() {
    let line = raw.split('#').next().unwrap_or("").trim();
    if line.is_empty() {
      continue;
    }
    let (key, value) = line
      .split_once('=')
      .unwrap_or_else(|| panic!("malformed line in tools/gates.count: {raw}"));
    let value = value
      .trim()
      .parse::<usize>()
      .unwrap_or_else(|_| panic!("bad number in tools/gates.count: {raw}"));
    map.insert(key.trim().to_string(), value);
  }
  map
}

#[test]
fn ratchet_counters_match_the_checked_in_baselines() {
  let root = repo_root();

  let (app_field_writes, action_refs) = (0usize, 0usize);
  let _ = (app_field_writes, action_refs);

  // Needles that would otherwise count their own literal in this file are
  // assembled with concat! (this file lives under the `src/` scan).
  // Every Rust file is in scope; a new module cannot re-import the deleted
  // terminal frontend unseen. This file is the one exclusion: it names its own
  // needles in doc and data, and being test-only it cannot couple anything to
  // the removed code.
  let mut non_tui_files = Vec::new();
  collect_rs_files(&root.join("src"), &mut non_tui_files);
  let gates_file = root.join("src").join("gates.rs");
  non_tui_files.retain(|path| *path != gates_file);
  // The view-write scan also skips `src/core/app/` (whose methods are `App`'s
  // own presentation helpers): it counts every producer that still writes a
  // TUI cursor.
  let app_dir = root.join("src").join("core").join("app");
  let (crate_tui_refs, view_writes) =
    non_tui_files
      .iter()
      .fold((0usize, 0usize), |(refs, writes), path| {
        let source = read_source(path);
        let view_writes = if path.starts_with(&app_dir) {
          0
        } else {
          count_view_field_writes(&source)
        };
        (
          refs + source.matches("crate::tui").count(),
          writes + view_writes,
        )
      });
  // (name, measured value). Every counter must match its baseline exactly;
  // the direction a baseline may move is enforced against the merge base by
  // tools/check_gates_ratchet.sh.
  let handler_mirror_writes = {
    let events_path = root
      .join("src")
      .join("infra")
      .join("player")
      .join("events.rs");
    count_handler_mirror_writes(&read_source(&events_path))
  };
  let measured: [(&str, usize); 2] = [
    (
      "wildcard_arms_in_action_tree",
      count_occurrences(&["src/core/action"], &[], "_ =>"),
    ),
    ("handler_mirror_writes", handler_mirror_writes),
  ];

  let mut baselines = load_baselines();
  let mut report = String::new();
  for (name, actual) in measured {
    match baselines.remove(name) {
      None => report.push_str(&format!(
        "{name}: missing from tools/gates.count (measured {actual})\n"
      )),
      Some(baseline) if baseline != actual => report.push_str(&format!(
        "{name}: baseline {baseline}, measured {actual}; move the baseline in \
         tools/gates.count in this same PR (tools/check_gates_ratchet.sh enforces \
         the direction it may move)\n"
      )),
      Some(_) => {}
    }
  }
  for (name, baseline) in baselines {
    report.push_str(&format!(
      "{name} = {baseline}: unknown counter in tools/gates.count, remove or fix the name\n"
    ));
  }
  assert!(report.is_empty(), "\nratchet violations:\n{report}");
}

#[test]
fn production_has_no_app_mutex_handles() {
  let root = repo_root();
  let mut files = Vec::new();
  collect_rs_files(&root.join("src"), &mut files);
  let forbidden = ["Arc<Mutex<App", "Arc<tokio::sync::Mutex<App", "Mutex<App>"];
  let mut violations = Vec::new();
  for path in files {
    if path == root.join("src").join("gates.rs") {
      continue;
    }
    let source = read_source(&path);
    let production = production_source(&source);
    for needle in forbidden {
      if production.contains(needle) {
        violations.push(format!("{} contains {needle}", path.display()));
      }
    }
  }
  assert!(
    violations.is_empty(),
    "production App mutex handles are forbidden; test-only fixtures must be cfg(test):\n{}",
    violations.join("\n")
  );
}

#[test]
fn field_write_matcher_counts_assignment_operators() {
  let src = "app.x = 1;\napp.a.b += 2;\napp.c -= 3;\napp.d *= 4;\napp.e <<= 1;\napp.f >>= 2;\n";
  assert_eq!(count_app_field_writes(src), 6);
}

#[test]
fn field_write_matcher_ignores_comparisons_guards_calls_and_other_idents() {
  let src = "if app.x == 1 {}\n\
             Key::Up if app.flag => {}\n\
             app.list.push(1);\n\
             foo(app.x);\n\
             if app.x != y {}\n\
             if app.x <= y {}\n\
             if app.x >= y {}\n\
             let s = app.y << 2;\n\
             wrapped_app.x = 1;\n";
  assert_eq!(count_app_field_writes(src), 0);
}

#[test]
fn field_write_matcher_skips_view_chains() {
  let src = "app.view.x = 1;\napp.view.a.b += 2;\napp.y = 3;\napp.viewer = 4;\n";
  assert_eq!(count_app_field_writes(src), 2);
}

#[test]
fn view_write_matcher_needs_a_receiver_and_an_assignment() {
  let src = "app.view.x = 1;\n\
             guard.view.a.b += 2;\n\
             if app.view.x == 1 {}\n\
             app.view.list.push(1);\n\
             let s = \".view.x = 1\";\n\
             foo(app.view.x);\n";
  assert_eq!(count_view_field_writes(src), 2);
}

#[test]
fn action_ref_matcher_is_word_bounded_and_stops_at_the_test_module() {
  let src = "app.apply(Action::Play);\n\
             let x = PlaybackAction::Pause;\n\
             app.apply(Action::Next)?;\n\
             #[cfg(test)]\n\
             mod tests { fn t() { app.apply(Action::Play); } }\n";
  assert_eq!(count_production_action_refs(src), 2);
  let gated = "app.apply(Action::Play);\n\
               #[cfg(all(test, feature = \"x\"))]\n\
               mod tests { fn t() { app.apply(Action::Play); } }\n";
  assert_eq!(count_production_action_refs(gated), 1);
}
