use super::*;

/// Compatibility data-generation counters for future frontend extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginDataKind {
  Playlists,
  Queue,
  Search,
  SavedTracks,
  SavedAlbums,
  SavedShows,
  RecentlyPlayed,
  Devices,
  Lyrics,
}

impl PluginDataKind {
  pub const COUNT: usize = 9;
  pub fn index(self) -> usize {
    self as usize
  }
}

#[derive(Debug, Default)]
pub struct PluginDataGenerations {
  counters: [u64; PluginDataKind::COUNT],
}

impl PluginDataGenerations {
  pub fn bump(&mut self, kind: PluginDataKind) {
    self.counters[kind.index()] = self.counters[kind.index()].wrapping_add(1);
  }
  pub fn get(&self, kind: PluginDataKind) -> u64 {
    self.counters[kind.index()]
  }
}

impl App {
  pub fn queue_plugin_command(&mut self, name: String) {
    self.pending_plugin_commands.push(name);
  }
  pub(crate) fn show_plugin_popup(&mut self, popup: crate::core::plugin_api::PluginPopup) {
    self.plugin_popup = Some(popup);
  }
  pub(crate) fn close_plugin_popup(&mut self) {
    self.plugin_popup = None;
  }
  pub(crate) fn open_plugin_screen(&mut self, name: String) {
    if self.get_current_route().id != RouteId::PluginScreen(name.clone()) {
      self.push_navigation_stack(RouteId::PluginScreen(name), ActiveBlock::PluginScreen);
    }
  }
}
