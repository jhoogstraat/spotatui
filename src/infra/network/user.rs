use super::requests::is_rate_limited_error;
use super::{ids, IoEvent, Network};
use crate::core::app::{ActiveBlock, RouteId, UserInfo};
use crate::core::plugin_api::TrackInfo;
use anyhow::anyhow;

use crate::infra::network::mapping::map_cursor_page;
use rspotify::model::{
  device::DevicePayload, page::CursorBasedPage, playing::PlayHistory, user::PrivateUser,
};
#[cfg(feature = "streaming")]
use rspotify::model::{enums::DeviceType, Device};
use rspotify::prelude::*;
use std::time::{Duration, Instant};

#[cfg(feature = "streaming")]
fn include_native_streaming_device(app: &crate::core::app::App, payload: &mut DevicePayload) {
  let Some(player) = app.streaming_player.as_ref() else {
    return;
  };

  if !player.is_available() {
    return;
  }

  let device_name = player.device_name();
  let device_id = app
    .native_device_id
    .clone()
    .unwrap_or_else(|| player.device_id());

  if let Some(device) = payload
    .devices
    .iter_mut()
    .find(|device| device.name.eq_ignore_ascii_case(device_name))
  {
    if device.id.is_none() {
      device.id = Some(device_id);
    }
    return;
  }

  payload.devices.push(Device {
    id: Some(device_id),
    is_active: app.is_streaming_active,
    is_private_session: false,
    is_restricted: false,
    name: device_name.to_string(),
    _type: DeviceType::Computer,
    volume_percent: Some(player.get_volume().into()),
  });
}

pub trait UserNetwork {
  async fn get_user(&mut self);
  /// `navigate: false` refreshes the device list without opening the device
  /// picker (used by plugin data reads).
  async fn get_devices(&mut self, navigate: bool);
  /// `navigate: false` refreshes the data without opening the screen (used by
  /// plugin data reads).
  #[allow(dead_code)]
  async fn get_recently_played(&mut self, navigate: bool);
}

impl UserNetwork for Network {
  async fn get_user(&mut self) {
    match self.spotify_get_typed::<PrivateUser>("me", &[]).await {
      Ok(user) => {
        let mut app = self.app.lock().await;
        // `PrivateUser::country` is deprecated upstream but still the only
        // market signal available; mirror the existing read in `get_user_country`.
        #[allow(deprecated)]
        let country = user.country.map(|c| <&'static str>::from(c).to_string());
        app.user = Some(UserInfo {
          id: user.id.id().to_string(),
          display_name: user.display_name.clone(),
          // Store the ISO 3166-1 alpha-2 code as a plain string so no rspotify
          // type leaks into App state; `App::get_user_country` re-derives it.
          country,
        });
      }
      Err(e) => {
        let err = anyhow!(e);
        if is_rate_limited_error(&err) {
          let mut app = self.app.lock().await;
          app.status_message = Some(
            "Spotify rate limit hit while loading profile. Retrying automatically.".to_string(),
          );
          app.status_message_expires_at = Some(Instant::now() + Duration::from_secs(6));
          return;
        }
        self.handle_error(err).await;
      }
    }
  }

  async fn get_devices(&mut self, navigate: bool) {
    match self
      .spotify_get_typed::<DevicePayload>("me/player/devices", &[])
      .await
    {
      Ok(result) => {
        let mut app = self.app.lock().await;
        if navigate {
          app.push_navigation_stack(RouteId::SelectedDevice, ActiveBlock::SelectDevice);
        }

        #[cfg(feature = "streaming")]
        let mut result = result;
        #[cfg(feature = "streaming")]
        {
          let recovering = app.request_native_streaming_recovery_if_disconnected(true);
          if !recovering {
            include_native_streaming_device(&app, &mut result);
          }
        }

        let _ = &result;
        app.devices = Some(result);
        app
          .plugin_data_generations
          .bump(crate::core::app::PluginDataKind::Devices);
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }

  async fn get_recently_played(&mut self, navigate: bool) {
    let limit = self.large_search_limit;
    match self
      .spotify_get_typed::<CursorBasedPage<PlayHistory>>(
        "me/player/recently-played",
        &[("limit", limit.to_string())],
      )
      .await
    {
      Ok(recently_played) => {
        let domain_page = map_cursor_page(&recently_played, |ph| TrackInfo::from(&ph.track));
        let mut app = self.app.lock().await;
        // Check if these tracks are liked.
        let track_check =
          ids::track_check_ids(recently_played.items.iter().map(|ph| ph.track.id.as_ref()));
        if !track_check.is_empty() {
          app.dispatch(IoEvent::CurrentUserSavedTracksContains(track_check));
        }
        app.recently_played.result = Some(domain_page);
        app.sort_recently_played_items();
        app
          .plugin_data_generations
          .bump(crate::core::app::PluginDataKind::RecentlyPlayed);
        if navigate {
          app.push_navigation_stack(RouteId::RecentlyPlayed, ActiveBlock::RecentlyPlayed);
        }
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }
}
