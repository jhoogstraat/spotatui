#[derive(Clone, PartialEq, Debug, Default)]
pub enum LyricsStatus {
  #[default]
  NotStarted,
  Loading,
  Found,
  NotFound,
}
