use crate::core::user_config::UserConfig;
use crate::infra::network::{IoEvent, Network};
use crate::runtime::engine::{FoldAnswer, FoldQuery, SessionFact};

use super::util::{Flag, Format, FormatType, JumpDirection, Type};

use crate::core::plugin_api::{PlaylistInfo, TrackInfo};
use crate::runtime::engine::DeviceList;
use anyhow::{anyhow, Result};
use rspotify::model::{
  context::CurrentPlaybackContext, idtypes::Id, playlist::FullPlaylist, PlayableItem,
};

pub struct CliApp {
  pub net: Network,
  pub config: UserConfig,
}

// Non-concurrent functions
// I feel that async in a cli is not working
// I just .await all processes and directly interact
// by calling network.handle_network_event
impl CliApp {
  pub fn new(net: Network, config: UserConfig) -> Self {
    Self { net, config }
  }

  // Ticket 17: every read below goes through the fold's published state
  // (the CLI mode runs its own fold task); nothing here locks App.
  async fn playback_context(&self) -> Option<CurrentPlaybackContext> {
    match self.net.fold_query(FoldQuery::PlaybackContext).await? {
      FoldAnswer::Playback(context) => context.map(|boxed| *boxed),
      _ => None,
    }
  }

  async fn devices(&self) -> Option<DeviceList> {
    match self.net.fold_query(FoldQuery::Devices).await? {
      FoldAnswer::Devices(payload) => payload,
      _ => None,
    }
  }

  async fn search_results(&self) -> Option<crate::core::app::SearchResult> {
    match self.net.fold_query(FoldQuery::SearchResults).await? {
      FoldAnswer::Search(results) => Some(*results),
      _ => None,
    }
  }

  async fn track_info_playlists(&self) -> Option<Vec<PlaylistInfo>> {
    match self.net.fold_query(FoldQuery::Playlists).await? {
      FoldAnswer::Playlists(paged) => paged.map(|p| p.items),
      _ => None,
    }
  }

  async fn table_tracks(&self) -> Vec<TrackInfo> {
    match self.net.fold_query(FoldQuery::TableTracks).await {
      Some(FoldAnswer::Tracks(tracks)) => tracks,
      _ => Vec::new(),
    }
  }

  async fn is_a_saved_track(&mut self, id: &str) -> Result<bool> {
    // Tracks without an ID (e.g. local files) can't be saved; asking the API
    // about `spotify:track:` would only fail the whole status output.
    if id.is_empty() {
      return Ok(false);
    }
    // The IoEvent handler defers to a detached worker (it must not block the
    // window's serial pump); the CLI needs the answer before returning, so
    // it resolves now and takes the API's own answer (the fold's liked set
    // is updated by the same merge).
    let saved = self.net.resolve_liked_state_now(&[id.to_string()]).await?;
    Ok(saved.first().copied().unwrap_or(false))
  }

  pub fn format_output(&self, mut format: String, values: Vec<Format>) -> String {
    for val in values {
      format = format.replace(val.get_placeholder(), &val.inner(self.config.clone()));
    }
    // Replace unsupported flags with 'None'
    for p in &["%a", "%b", "%t", "%p", "%h", "%u", "%d", "%v", "%f", "%s"] {
      format = format.replace(p, "None");
    }
    format.trim().to_string()
  }

  // spt playback -t
  pub async fn toggle_playback(&mut self) {
    if let Some(c) = self.playback_context().await {
      if c.is_playing {
        self.net.handle_network_event(IoEvent::PausePlayback).await;
        return;
      }
    }
    self
      .net
      .handle_network_event(IoEvent::StartPlayback(None, None, None))
      .await;
  }

  // spt pb --share-track (share the current playing song)
  // Basically copy-pasted the 'copy_song_url' function
  pub async fn share_track_or_episode(&mut self) -> Result<String> {
    if let Some(CurrentPlaybackContext {
      item: Some(item), ..
    }) = self.playback_context().await
    {
      match item {
        PlayableItem::Track(track) => {
          if let Some(id) = &track.id {
            Ok(format!("https://open.spotify.com/track/{}", id.id()))
          } else {
            Err(anyhow!("track has no ID"))
          }
        }
        PlayableItem::Episode(episode) => Ok(format!(
          "https://open.spotify.com/episode/{}",
          episode.id.id()
        )),
        _ => Err(anyhow!("unknown playable item type")),
      }
    } else {
      Err(anyhow!(
        "failed to generate a shareable url for the current song"
      ))
    }
  }

  // spt pb --share-album (share the current album)
  // Basically copy-pasted the 'copy_album_url' function
  pub async fn share_album_or_show(&mut self) -> Result<String> {
    if let Some(CurrentPlaybackContext {
      item: Some(item), ..
    }) = self.playback_context().await
    {
      match item {
        PlayableItem::Track(track) => {
          if let Some(id) = &track.album.id {
            Ok(format!("https://open.spotify.com/album/{}", id.id()))
          } else {
            Err(anyhow!("album has no ID"))
          }
        }
        PlayableItem::Episode(episode) => Ok(format!(
          "https://open.spotify.com/show/{}",
          episode.show.id.id()
        )),
        _ => Err(anyhow!("unknown playable item type")),
      }
    } else {
      Err(anyhow!(
        "failed to generate a shareable url for the current song"
      ))
    }
  }

  // spt ... -d ... (specify device to control)
  pub async fn set_device(&mut self, name: String) -> Result<()> {
    // Change the device if specified by user
    if let Some(dp) = self.devices().await {
      for d in &dp.devices {
        if d.name == name {
          // Save the id of the device
          if let Some(id) = d.id.clone() {
            self
              .net
              .client_config
              .set_device_id(id)
              .map_err(|_e| anyhow!("failed to use device with name '{}'", d.name))?;
          }
        }
      }
    } else {
      // Error out if no device is available
      return Err(anyhow!("no device available"));
    }
    Ok(())
  }

  // spt query ... --limit LIMIT (set max search limit)
  pub async fn update_query_limits(&mut self, max: String) -> Result<()> {
    let num = max
      .parse::<u32>()
      .map_err(|_e| anyhow!("limit must be between 1 and 50"))?;

    // 50 seems to be the maximum limit
    if num > 50 || num == 0 {
      return Err(anyhow!("limit must be between 1 and 50"));
    };

    self
      .net
      .handle_network_event(IoEvent::UpdateSearchLimits(num, num))
      .await;
    Ok(())
  }

  pub async fn volume(&mut self, vol: String) -> Result<()> {
    let num = vol
      .parse::<u32>()
      .map_err(|_e| anyhow!("volume must be between 0 and 100"))?;

    // Check if it's in range
    if num > 100 {
      return Err(anyhow!("volume must be between 0 and 100"));
    };

    self
      .net
      .handle_network_event(IoEvent::ChangeVolume(num as u8))
      .await;
    Ok(())
  }

  // spt playback --next / --previous
  pub async fn jump(&mut self, d: &JumpDirection) {
    match d {
      JumpDirection::Next => self.net.handle_network_event(IoEvent::NextTrack).await,
      JumpDirection::Previous => self.net.handle_network_event(IoEvent::PreviousTrack).await,
    }
  }

  // spt query -l ...
  pub async fn list(&mut self, item: Type, format: &str) -> String {
    match item {
      Type::Device => {
        if let Some(devices) = self.devices().await {
          devices
            .devices
            .iter()
            .map(|d| {
              self.format_output(
                format.to_string(),
                vec![
                  Format::Device(d.name.clone()),
                  Format::Volume(d.volume_percent.unwrap_or(0)),
                ],
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          "No devices available".to_string()
        }
      }
      Type::Playlist => {
        self.net.handle_network_event(IoEvent::GetPlaylists).await;
        if let Some(playlists) = self.track_info_playlists().await {
          playlists
            .iter()
            .map(|p| {
              self.format_output(
                format.to_string(),
                Format::from_type(FormatType::PlaylistInfo(Box::new(p.clone()))),
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          "No playlists found".to_string()
        }
      }
      Type::Liked => {
        self
          .net
          .handle_network_event(IoEvent::GetCurrentSavedTracks(None))
          .await;
        let liked_songs = self
          .table_tracks()
          .await
          .iter()
          .map(|t| {
            self.format_output(
              format.to_string(),
              Format::from_type(FormatType::TrackInfo(Box::new(t.clone()))),
            )
          })
          .collect::<Vec<String>>();
        // Check if there are any liked songs
        if liked_songs.is_empty() {
          "No liked songs found".to_string()
        } else {
          liked_songs.join("\n")
        }
      }
      // Enforced by clap
      _ => unreachable!(),
    }
  }

  // spt playback --transfer DEVICE
  pub async fn transfer_playback(&mut self, device: &str) -> Result<()> {
    // Get the device id by name
    let mut id = String::new();
    if let Some(devices) = self.devices().await {
      for d in &devices.devices {
        if d.name == device {
          if let Some(device_id) = &d.id {
            id.push_str(device_id);
            break;
          }
          break;
        }
      }
    };

    if id.is_empty() {
      Err(anyhow!("no device with name '{}'", device))
    } else {
      self
        .net
        .handle_network_event(IoEvent::TransferPlaybackToDevice(id.to_string(), true))
        .await;
      Ok(())
    }
  }

  pub async fn seek(&mut self, seconds_str: String) -> Result<()> {
    let seconds = match seconds_str.parse::<i32>() {
      Ok(s) => s.unsigned_abs(),
      Err(_) => return Err(anyhow!("failed to convert seconds to i32")),
    };

    let (current_pos, duration) = {
      self
        .net
        .handle_network_event(IoEvent::GetCurrentPlayback)
        .await;
      if let Some(CurrentPlaybackContext {
        progress: Some(ms),
        item: Some(item),
        ..
      }) = self.playback_context().await
      {
        let duration = match item {
          PlayableItem::Track(track) => track.duration.num_milliseconds() as u32,
          PlayableItem::Episode(episode) => episode.duration.num_milliseconds() as u32,
          _ => return Err(anyhow!("unknown playable item type")),
        };

        (ms.num_milliseconds() as u32, duration)
      } else {
        return Err(anyhow!("no context available"));
      }
    };

    // Convert secs to ms
    let ms = seconds * 1000;
    // Calculate new positon
    let position_to_seek = if seconds_str.starts_with('+') {
      current_pos + ms
    } else if seconds_str.starts_with('-') {
      // Jump to the beginning if the position_to_seek would be
      // negative, must be checked before the calculation to avoid
      // an 'underflow'
      current_pos.saturating_sub(ms)
    } else {
      // Absolute value of the track
      seconds * 1000
    };

    // Check if position_to_seek is greater than duration (next track)
    if position_to_seek > duration {
      self.jump(&JumpDirection::Next).await;
    } else {
      // This seeks to a position in the current song
      self
        .net
        .handle_network_event(IoEvent::Seek(position_to_seek))
        .await;
    }

    Ok(())
  }

  // spt playback --like / --dislike / --shuffle / --repeat
  pub async fn mark(&mut self, flag: Flag) -> Result<()> {
    let c = self
      .playback_context()
      .await
      .ok_or_else(|| anyhow!("no context available"))?;

    match flag {
      Flag::Like(s) => {
        // Get the id of the current song
        let id = match c.item {
          Some(i) => match i {
            PlayableItem::Track(t) => t.id.ok_or_else(|| anyhow!("item has no id")),
            PlayableItem::Episode(_) => Err(anyhow!("saving episodes not yet implemented")),
            _ => Err(anyhow!("unknown playable item type")),
          },
          None => Err(anyhow!("no item playing")),
        }?;

        let id_string = id.id().to_string();
        // Want to like but is already liked -> do nothing
        // Want to like and is not liked yet -> like
        if s && !self.is_a_saved_track(&id_string).await? {
          self
            .net
            .handle_network_event(IoEvent::ToggleSaveTrack(id.uri()))
            .await;
        // Want to dislike but is already disliked -> do nothing
        // Want to dislike and is liked currently -> remove like
        } else if !s && self.is_a_saved_track(&id_string).await? {
          self
            .net
            .handle_network_event(IoEvent::ToggleSaveTrack(id.uri()))
            .await;
        }
      }
      Flag::Shuffle => {
        self
          .net
          .handle_network_event(IoEvent::Shuffle(!c.shuffle_state))
          .await
      }
      Flag::Repeat => {
        self
          .net
          .handle_network_event(IoEvent::Repeat(c.repeat_state))
          .await;
      }
    }

    Ok(())
  }

  // spt playback -s
  pub async fn get_status(&mut self, format: String) -> Result<String> {
    // Update info on current playback
    self
      .net
      .handle_network_event(IoEvent::GetCurrentPlayback)
      .await;
    self
      .net
      .handle_network_event(IoEvent::GetCurrentSavedTracks(None))
      .await;

    let context = self
      .playback_context()
      .await
      .ok_or_else(|| anyhow!("no context available"))?;

    let playing_item = context.item.ok_or_else(|| anyhow!("no track playing"))?;

    let mut hs = match playing_item {
      PlayableItem::Track(track) => {
        let id = track
          .id
          .clone()
          .map(|track_id| track_id.id().to_string())
          .unwrap_or_default();
        let mut hs = Format::from_type(FormatType::Track(Box::new(track.clone())));
        if let Some(ms) = &context.progress {
          hs.push(Format::Position((
            ms.num_milliseconds() as u32,
            track.duration.num_milliseconds() as u32,
          )))
        }
        hs.push(Format::Flags((
          context.repeat_state,
          context.shuffle_state,
          self.is_a_saved_track(&id).await?,
        )));
        hs
      }
      PlayableItem::Episode(episode) => {
        let mut hs = Format::from_type(FormatType::Episode(Box::new(episode.clone())));
        if let Some(ms) = &context.progress {
          hs.push(Format::Position((
            ms.num_milliseconds() as u32,
            episode.duration.num_milliseconds() as u32,
          )))
        }
        hs.push(Format::Flags((
          context.repeat_state,
          context.shuffle_state,
          false,
        )));
        hs
      }
      _ => return Err(anyhow!("unknown playable item type")),
    };

    hs.push(Format::Device(context.device.name));
    hs.push(Format::Volume(context.device.volume_percent.unwrap_or(0)));
    hs.push(Format::Playing(context.is_playing));

    Ok(self.format_output(format, hs))
  }

  // spt play -u URI
  pub async fn play_uri(&mut self, uri: String, queue: bool, random: bool) {
    let offset = if random {
      // Only works with playlists for now
      if uri.contains("spotify:playlist:") {
        let id_str = uri.split(':').next_back().unwrap();
        if let Ok(playlist_id) = rspotify::model::idtypes::PlaylistId::from_id(id_str) {
          match self
            .net
            .spotify_get_typed::<FullPlaylist>(&format!("playlists/{}", playlist_id.id()), &[])
            .await
          {
            Ok(p) => {
              let num = p.items.total;
              Some(rand::random_range(0..num) as usize)
            }
            Err(e) => {
              // Acked: the command's success/failure verdict below reads the
              // fold's error surface, so a dropped event must not lie.
              self
                .net
                .emit_session_acked(SessionFact::Error(e.to_string()))
                .await;
              return;
            }
          }
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    };

    // The network boundary reconstructs the typed rspotify id from the URI.
    if uri.contains("spotify:track:") {
      if queue {
        self
          .net
          .handle_network_event(IoEvent::AddItemToQueue(uri))
          .await;
      } else {
        self
          .net
          .handle_network_event(IoEvent::StartPlayback(None, Some(vec![uri]), Some(0)))
          .await;
      }
    } else if uri.contains("spotify:playlist:")
      || uri.contains("spotify:album:")
      || uri.contains("spotify:artist:")
      || uri.contains("spotify:show:")
    {
      // Context URIs (playlist, album, artist, show)
      self
        .net
        .handle_network_event(IoEvent::StartPlayback(Some(uri), None, offset))
        .await;
    }
  }

  // spt play -n NAME ...
  pub async fn play(&mut self, name: String, item: Type, queue: bool, random: bool) -> Result<()> {
    self
      .net
      .handle_network_event(IoEvent::GetSearchResults(name.clone(), None))
      .await;
    // Get the uri of the first found
    // item + the offset or return an error message
    let results = self.search_results().await.unwrap_or_default();
    let uri = {
      match item {
        Type::Track => {
          if let Some(r) = &results.tracks {
            if let Some(ref id) = r.items[0].id {
              format!("spotify:track:{}", id)
            } else {
              return Err(anyhow!("track has no id"));
            }
          } else {
            return Err(anyhow!("no tracks with name '{}'", name));
          }
        }
        Type::Album => {
          if let Some(r) = &results.albums {
            let album = &r.items[0];
            if let Some(ref id) = album.id {
              format!("spotify:album:{}", id)
            } else {
              return Err(anyhow!("album {} has no id", album.name));
            }
          } else {
            return Err(anyhow!("no albums with name '{}'", name));
          }
        }
        Type::Artist => {
          if let Some(r) = &results.artists {
            if let Some(ref id) = r.items[0].id {
              format!("spotify:artist:{}", id)
            } else {
              return Err(anyhow!("artist has no id"));
            }
          } else {
            return Err(anyhow!("no artists with name '{}'", name));
          }
        }
        Type::Show => {
          if let Some(r) = &results.shows {
            if let Some(ref id) = r.items[0].id {
              format!("spotify:show:{}", id)
            } else {
              return Err(anyhow!("show has no id"));
            }
          } else {
            return Err(anyhow!("no shows with name '{}'", name));
          }
        }
        Type::Playlist => {
          if let Some(r) = &results.playlists {
            let p = &r.items[0];
            if let Some(ref id) = p.id {
              format!("spotify:playlist:{}", id)
            } else {
              return Err(anyhow!("playlist has no id"));
            }
          } else {
            return Err(anyhow!("no playlists with name '{}'", name));
          }
        }
        _ => unreachable!(),
      }
    };

    // Play or queue the uri
    self.play_uri(uri, queue, random).await;

    Ok(())
  }

  // spt query -s SEARCH ...
  pub async fn query(&mut self, search: String, format: String, item: Type) -> String {
    self
      .net
      .handle_network_event(IoEvent::GetSearchResults(search.clone(), None))
      .await;

    let results = self.search_results().await.unwrap_or_default();
    match item {
      Type::Playlist => {
        if let Some(results) = &results.playlists {
          results
            .items
            .iter()
            .map(|r| {
              self.format_output(
                format.clone(),
                Format::from_type(FormatType::PlaylistInfo(Box::new(r.clone()))),
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          format!("no playlists with name '{}'", search)
        }
      }
      Type::Track => {
        if let Some(results) = &results.tracks {
          results
            .items
            .iter()
            .map(|r| {
              self.format_output(
                format.clone(),
                Format::from_type(FormatType::TrackInfo(Box::new(r.clone()))),
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          format!("no tracks with name '{}'", search)
        }
      }
      Type::Artist => {
        if let Some(results) = &results.artists {
          results
            .items
            .iter()
            .map(|r| {
              self.format_output(
                format.clone(),
                Format::from_type(FormatType::ArtistInfo(Box::new(r.clone()))),
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          format!("no artists with name '{}'", search)
        }
      }
      Type::Show => {
        if let Some(results) = &results.shows {
          results
            .items
            .iter()
            .map(|r| {
              self.format_output(
                format.clone(),
                Format::from_type(FormatType::ShowInfo(Box::new(r.clone()))),
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          format!("no shows with name '{}'", search)
        }
      }
      Type::Album => {
        if let Some(results) = &results.albums {
          results
            .items
            .iter()
            .map(|r| {
              self.format_output(
                format.clone(),
                Format::from_type(FormatType::AlbumInfo(Box::new(r.clone()))),
              )
            })
            .collect::<Vec<String>>()
            .join("\n")
        } else {
          format!("no albums with name '{}'", search)
        }
      }
      // Enforced by clap
      _ => unreachable!(),
    }
  }
}
