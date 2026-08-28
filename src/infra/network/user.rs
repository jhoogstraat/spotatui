use super::requests::is_rate_limited_error;
use super::{ids, IoEvent, Network};
use crate::core::app::UserInfo;
use crate::core::plugin_api::TrackInfo;
use crate::runtime::engine::SessionFact;
use anyhow::anyhow;

use crate::infra::network::mapping::map_cursor_page;
use rspotify::model::{
  device::DevicePayload, page::CursorBasedPage, playing::PlayHistory, user::PrivateUser,
};
use rspotify::prelude::*;

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
        // `PrivateUser::country` is deprecated upstream but still the only
        // market signal available; mirror the existing read in `get_user_country`.
        #[allow(deprecated)]
        let country = user.country.map(|c| <&'static str>::from(c).to_string());
        self
          .emit_session(SessionFact::User(Ok(UserInfo {
            id: user.id.id().to_string(),
            display_name: user.display_name.clone(),
            // Store the ISO 3166-1 alpha-2 code as a plain string so no rspotify
            // type leaks into App state; `App::get_user_country` re-derives it.
            country,
          })))
          .await;
      }
      Err(e) => {
        let err = anyhow!(e);
        if is_rate_limited_error(&err) {
          // A benign toast, not the error page: the old inline write was a
          // bare status message, and the 429 recovers on the next poll.
          self
            .emit_session(SessionFact::User(Err(
              "Spotify rate limit hit while loading profile. Retrying automatically.".to_string(),
            )))
            .await;
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
        // One fact, one fold step: the navigation, the native recovery
        // interlock (a check-and-set that gates the device merge), the cache
        // write, and the plugin generation bump all reduce together.
        self
          .emit_session(SessionFact::DevicesLanded {
            devices: result.devices,
            navigate,
          })
          .await;
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
        // Liked checks ride the fold's dispatch fact (the producer cannot
        // reach the IoEvent channel without the App lock it no longer takes).
        let track_check =
          ids::track_check_ids(recently_played.items.iter().map(|ph| ph.track.id.as_ref()));
        if !track_check.is_empty() {
          self
            .emit_session(SessionFact::DispatchIo(vec![
              IoEvent::CurrentUserSavedTracksContains(track_check),
            ]))
            .await;
        }
        self
          .emit_session(SessionFact::RecentlyPlayed {
            page: domain_page,
            navigate,
          })
          .await;
      }
      Err(e) => {
        self.handle_error(anyhow!(e)).await;
      }
    }
  }
}
