use super::Network;
use crate::core::app::{Announcement, AnnouncementLevel};
use crate::runtime::engine::{FoldAnswer, FoldQuery, SessionFact};
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer};
use std::collections::HashSet;
use std::env;
use std::time::{Duration, Instant};

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct GlobalSongCountResponse {
  #[serde(deserialize_with = "deserialize_global_song_count")]
  count: u64,
}

const TELEMETRY_ENDPOINT: &str = "https://spotatui-counter.spotatui.workers.dev";

fn deserialize_global_song_count<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
  D: Deserializer<'de>,
{
  #[derive(Deserialize)]
  #[serde(untagged)]
  enum CountValue {
    Number(u64),
    String(String),
  }

  match CountValue::deserialize(deserializer)? {
    CountValue::Number(value) => Ok(value),
    CountValue::String(value) => {
      let sanitized = value.replace(',', "");
      sanitized
        .parse::<u64>()
        .map_err(|_| D::Error::custom("invalid global song count"))
    }
  }
}

#[derive(Deserialize, Debug)]
struct AnnouncementFeedResponse {
  #[allow(dead_code)]
  version: Option<u8>,
  #[serde(default)]
  announcements: Vec<AnnouncementRecord>,
}

#[derive(Deserialize, Debug)]
struct AnnouncementRecord {
  id: String,
  title: Option<String>,
  body: String,
  level: Option<String>,
  url: Option<String>,
  starts_at: Option<String>,
  ends_at: Option<String>,
}

pub trait UtilsNetwork {
  async fn increment_global_song_count(&mut self);
  async fn fetch_global_song_count(&mut self);
  async fn fetch_announcements(&mut self);
}

impl UtilsNetwork for Network {
  async fn increment_global_song_count(&mut self) {
    let client = super::requests::shared_http_client();
    // Fire and forget
    let _ = client
      .post(TELEMETRY_ENDPOINT)
      .header(reqwest::header::ACCEPT, "application/json")
      .timeout(Duration::from_secs(5))
      .send()
      .await;
  }

  async fn fetch_global_song_count(&mut self) {
    let client = super::requests::shared_http_client();
    match client
      .get(TELEMETRY_ENDPOINT)
      .header(reqwest::header::ACCEPT, "application/json")
      .timeout(Duration::from_secs(5))
      .send()
      .await
    {
      Ok(resp) => {
        if let Ok(data) = resp.json::<GlobalSongCountResponse>().await {
          self
            .emit_session(SessionFact::GlobalSongCount {
              count: Some(data.count),
              failed: false,
            })
            .await;
        } else {
          self
            .emit_session(SessionFact::GlobalSongCount {
              count: None,
              failed: true,
            })
            .await;
        }
      }
      Err(_) => {
        self
          .emit_session(SessionFact::GlobalSongCount {
            count: None,
            failed: true,
          })
          .await;
      }
    }
  }

  async fn fetch_announcements(&mut self) {
    const MAX_ANNOUNCEMENT_FEED_BYTES: usize = 256 * 1024;
    const ANNOUNCEMENTS_ENV_KEY: &str = "SPOTATUI_ANNOUNCEMENTS_URL";
    const DEFAULT_ANNOUNCEMENTS_URL: &str =
      "https://raw.githubusercontent.com/LargeModGames/spotatui/main/announcements.json";

    let (announcements_enabled, feed_url, seen_ids) = {
      match self
        .fold_query(FoldQuery::AnnouncementSources { seen_ids: true })
        .await
      {
        Some(FoldAnswer::AnnouncementSources {
          enabled,
          feed_url,
          seen_ids,
        }) => (enabled, feed_url, seen_ids),
        _ => return,
      }
    };

    if !announcements_enabled {
      return;
    }

    let env_feed_url = env::var(ANNOUNCEMENTS_ENV_KEY)
      .ok()
      .map(|v| v.trim().to_string())
      .filter(|v| !v.is_empty());

    let resolved_url = env_feed_url
      .or(feed_url)
      .filter(|url| !url.trim().is_empty())
      .unwrap_or_else(|| DEFAULT_ANNOUNCEMENTS_URL.to_string());

    if !resolved_url.starts_with("https://") {
      return;
    }

    let client = super::requests::shared_http_client();

    let response = match client
      .get(&resolved_url)
      .header(reqwest::header::ACCEPT, "application/json")
      .timeout(Duration::from_secs(5))
      .send()
      .await
    {
      Ok(response) => response,
      Err(_) => return,
    };

    if !response.status().is_success() {
      return;
    }

    if response
      .content_length()
      .is_some_and(|length| length > MAX_ANNOUNCEMENT_FEED_BYTES as u64)
    {
      return;
    }

    let body = match response.bytes().await {
      Ok(bytes) if bytes.len() <= MAX_ANNOUNCEMENT_FEED_BYTES => bytes,
      _ => return,
    };

    let feed: AnnouncementFeedResponse = match serde_json::from_slice(&body) {
      Ok(feed) => feed,
      Err(_) => return,
    };

    let now = Utc::now();
    let seen_ids = seen_ids
      .unwrap_or_default()
      .into_iter()
      .collect::<HashSet<String>>();
    let mut feed_ids_seen = HashSet::new();
    let mut announcements = Vec::new();

    for record in feed.announcements {
      let id = record.id.trim().to_string();
      if id.is_empty() || seen_ids.contains(&id) || !feed_ids_seen.insert(id.clone()) {
        continue;
      }

      let body = record.body.trim().to_string();
      if body.is_empty() {
        continue;
      }

      let starts_at = match record.starts_at.as_deref().map(parse_announcement_datetime) {
        Some(Some(value)) => Some(value),
        Some(None) => continue,
        None => None,
      };

      let ends_at = match record.ends_at.as_deref().map(parse_announcement_datetime) {
        Some(Some(value)) => Some(value),
        Some(None) => continue,
        None => None,
      };

      if let Some(start) = starts_at {
        if now < start {
          continue;
        }
      }

      if let Some(end) = ends_at {
        if now > end {
          continue;
        }
      }

      let url = record
        .url
        .map(|url| url.trim().to_string())
        .filter(|url| !url.is_empty() && url.starts_with("https://"));

      announcements.push(Announcement {
        id,
        title: record
          .title
          .map(|title| title.trim().to_string())
          .filter(|title| !title.is_empty())
          .unwrap_or_else(|| "Announcement".to_string()),
        body,
        level: parse_announcement_level(record.level.as_deref()),
        url,
        received_at: Instant::now(),
      });
    }

    if announcements.is_empty() {
      return;
    }

    self
      .emit_session(SessionFact::Announcements(announcements))
      .await;
  }
}

fn parse_announcement_datetime(value: &str) -> Option<DateTime<Utc>> {
  DateTime::parse_from_rfc3339(value)
    .ok()
    .map(|dt| dt.with_timezone(&Utc))
}

fn parse_announcement_level(level: Option<&str>) -> AnnouncementLevel {
  match level.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
    Some("critical") => AnnouncementLevel::Critical,
    Some("warning") => AnnouncementLevel::Warning,
    _ => AnnouncementLevel::Info,
  }
}
