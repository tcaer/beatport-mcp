use std::env;

use beatport_mcp::{BeatportClient, Config};
use reqwest::Method;
use serde_json::json;

fn live_enabled() -> bool {
    env::var("BEATPORT_RUN_LIVE_TESTS").ok().as_deref() == Some("1")
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}

fn live_client() -> Option<BeatportClient> {
    if !live_enabled() {
        return None;
    }
    let config = Config::from_env().ok()?;
    BeatportClient::new(config).ok()
}

#[tokio::test]
#[ignore = "requires BEATPORT_RUN_LIVE_TESTS=1 and valid Beatport credentials/token"]
async fn live_search_and_catalog_reads() {
    let Some(client) = live_client() else {
        return;
    };

    let search = client
        .request(
            Method::GET,
            "/v4/catalog/search/",
            Some(&std::collections::BTreeMap::from([
                ("q".into(), json!("drum")),
                ("per_page".into(), json!(1)),
            ])),
            None,
        )
        .await
        .expect("search should succeed");

    assert_eq!(search.status, 200);
    assert!(search.data.get("tracks").is_some());

    let tracks = client
        .request(
            Method::GET,
            "/v4/catalog/tracks/",
            Some(&std::collections::BTreeMap::from([(
                "per_page".into(),
                json!(1),
            )])),
            None,
        )
        .await
        .expect("catalog tracks should succeed");

    assert_eq!(tracks.status, 200);
    assert!(tracks.data.get("results").is_some());
}

#[tokio::test]
#[ignore = "requires BEATPORT_RUN_LIVE_TESTS=1, a valid token, and BEATPORT_TEST_GENRE_ID"]
async fn live_genre_subscription_round_trip() {
    let Some(client) = live_client() else {
        return;
    };
    let Some(genre_id) = env_u64("BEATPORT_TEST_GENRE_ID") else {
        return;
    };

    let subscribe = client
        .request(
            Method::POST,
            "/v4/my/genres/",
            None,
            Some(&json!({ "id": genre_id })),
        )
        .await
        .expect("genre subscribe should succeed");
    assert!(subscribe.status == 200 || subscribe.status == 201);

    let unsubscribe = client
        .request(
            Method::DELETE,
            &format!("/v4/my/genres/{genre_id}/"),
            None,
            None,
        )
        .await
        .expect("genre unsubscribe should succeed");
    assert!(unsubscribe.status == 200 || unsubscribe.status == 204);
}

#[tokio::test]
#[ignore = "requires BEATPORT_RUN_LIVE_TESTS=1, a valid token, and BEATPORT_TEST_TRACK_ID"]
async fn live_playlist_lifecycle_round_trip() {
    let Some(client) = live_client() else {
        return;
    };
    let Some(track_id) = env_u64("BEATPORT_TEST_TRACK_ID") else {
        return;
    };

    let created = client
        .request(
            Method::POST,
            "/v4/my/playlists/",
            None,
            Some(&json!({ "name": "beatport-mcp integration test" })),
        )
        .await
        .expect("playlist should be created");

    let playlist_id = created
        .data
        .get("id")
        .and_then(|value| value.as_u64())
        .expect("playlist response should include an id");

    let renamed = client
        .request(
            Method::PATCH,
            &format!("/v4/my/playlists/{playlist_id}/"),
            None,
            Some(&json!({ "name": "beatport-mcp integration test renamed" })),
        )
        .await
        .expect("playlist should rename");
    assert_eq!(renamed.status, 200);

    let added = client
        .request(
            Method::POST,
            &format!("/v4/my/playlists/{playlist_id}/tracks/"),
            None,
            Some(&json!({ "track_id": track_id })),
        )
        .await
        .expect("track should be added");
    assert!(added.status == 200 || added.status == 201);

    let playlist_tracks = client
        .request(
            Method::GET,
            &format!("/v4/my/playlists/{playlist_id}/tracks/"),
            None,
            None,
        )
        .await
        .expect("playlist tracks should load");

    let playlist_track_id = playlist_tracks
        .data
        .get("results")
        .and_then(|value| value.as_array())
        .and_then(|items| items.first())
        .and_then(|value| value.get("id"))
        .and_then(|value| value.as_u64())
        .expect("playlist track entry should include an id");

    let reordered = client
        .request(
            Method::PATCH,
            &format!("/v4/my/playlists/{playlist_id}/tracks/{playlist_track_id}/"),
            None,
            Some(&json!({ "position": 1 })),
        )
        .await
        .expect("playlist track should reorder");
    assert_eq!(reordered.status, 200);

    let removed = client
        .request(
            Method::DELETE,
            &format!("/v4/my/playlists/{playlist_id}/tracks/{playlist_track_id}/"),
            None,
            None,
        )
        .await
        .expect("playlist track should delete");
    assert!(removed.status == 200 || removed.status == 204);

    let deleted = client
        .request(
            Method::DELETE,
            &format!("/v4/my/playlists/{playlist_id}/"),
            None,
            None,
        )
        .await
        .expect("playlist should delete");
    assert!(deleted.status == 200 || deleted.status == 204);
}

#[tokio::test]
#[ignore = "interactive browser test; requires BEATPORT_RUN_LIVE_TESTS=1"]
async fn live_interactive_connect_bootstrap() {
    let Some(client) = live_client() else {
        return;
    };

    let result = client
        .connect()
        .await
        .expect("interactive connect should succeed");
    assert!(
        result
            .authorization_url
            .starts_with("https://api.beatport.com/")
    );
}
