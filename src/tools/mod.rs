use std::collections::BTreeMap;

use reqwest::Method;
use rmcp::{
    ErrorData, Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AuthMode;
use crate::beatport::{
    ApiResponse, BeatportClient, CompactTrackSummary, DescribeEndpointOutput, SearchResults,
    extract_compact_track_summaries, merge_paging, sanitize_relation,
};
use crate::crate_sync::{
    CrateSyncEngine, PlaylistSyncApplyOutput, PlaylistSyncApplyRequest, PlaylistSyncDryRunOutput,
    PlaylistSyncDryRunRequest, PlaylistSyncResolveOutput, PlaylistSyncResolveRequest,
    PlaylistSyncReviewOutput, PlaylistSyncReviewRequest, TrendingCandidatesOutput,
    TrendingCandidatesRequest,
};

pub struct BeatportMcp {
    client: BeatportClient,
    sync_engine: CrateSyncEngine,
    tool_router: ToolRouter<Self>,
}

impl BeatportMcp {
    pub fn new(client: BeatportClient) -> Self {
        let sync_engine = CrateSyncEngine::new(client.clone(), client.sync_state_path());
        Self {
            client,
            sync_engine,
            tool_router: Self::tool_router(),
        }
    }

    fn tool_error(error: impl Into<ErrorData>) -> ErrorData {
        error.into()
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for BeatportMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(
                "Authenticate with beatport_connect, then prefer the curated Beatport sync tools for automation and playlist curation. Use beatport_request only for long-tail endpoints.",
            )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectOutput {
    pub authorization_url: String,
    pub opened_browser: bool,
    pub expires_at: String,
    pub scope: Option<String>,
    pub token_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AuthStatusOutput {
    pub connected: bool,
    pub token_path: String,
    pub expires_at: Option<String>,
    pub scope: Option<String>,
    pub account: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DisconnectOutput {
    pub revoked: bool,
    pub token_file_removed: bool,
    pub token_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchOutput {
    pub path: String,
    pub status: u16,
    pub data: SearchResults,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompactSearchRequest {
    pub query: String,
    pub per_page: Option<u64>,
    pub include_tracks_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CompactSearchOutput {
    pub path: String,
    pub status: u16,
    pub candidates: Vec<CompactTrackSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistTracksCompactRequest {
    pub playlist_id: u64,
    pub page: Option<u64>,
    pub per_page: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistTracksCompactOutput {
    pub path: String,
    pub status: u16,
    pub playlist_id: u64,
    pub tracks: Vec<CompactTrackSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ServerInfoOutput {
    pub server_version: String,
    pub auth_mode: String,
    pub token_path: String,
    pub sync_state_path: String,
    pub curated_tools: Vec<String>,
    pub requires_reconnect_after_tool_changes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchInclude {
    Tracks,
    Artists,
    Releases,
    Charts,
    Labels,
    Playlists,
}

impl SearchInclude {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Tracks => "tracks",
            Self::Artists => "artists",
            Self::Releases => "releases",
            Self::Charts => "charts",
            Self::Labels => "labels",
            Self::Playlists => "playlists",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchRequest {
    pub query: String,
    pub page: Option<u64>,
    pub per_page: Option<u64>,
    pub include: Option<Vec<SearchInclude>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CatalogResource {
    Artists,
    Charts,
    Genres,
    Labels,
    Playlists,
    Releases,
    SubGenres,
    Tracks,
}

impl CatalogResource {
    fn segment(&self) -> &'static str {
        match self {
            Self::Artists => "artists",
            Self::Charts => "charts",
            Self::Genres => "genres",
            Self::Labels => "labels",
            Self::Playlists => "playlists",
            Self::Releases => "releases",
            Self::SubGenres => "sub-genres",
            Self::Tracks => "tracks",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CatalogListRequest {
    pub resource: CatalogResource,
    pub filters: Option<BTreeMap<String, Value>>,
    pub page: Option<u64>,
    pub per_page: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CatalogGetRequest {
    pub resource: CatalogResource,
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CatalogRelatedRequest {
    pub resource: CatalogResource,
    pub id: String,
    pub relation: String,
    pub filters: Option<BTreeMap<String, Value>>,
    pub page: Option<u64>,
    pub per_page: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MyListResource {
    Beatport,
    BeatportArtists,
    BeatportLabels,
    BeatportPlaylists,
    BeatportTracks,
    Charts,
    Downloads,
    Genres,
    Playlists,
}

impl MyListResource {
    fn segment(&self) -> &'static str {
        match self {
            Self::Beatport => "beatport",
            Self::BeatportArtists => "beatport/artists",
            Self::BeatportLabels => "beatport/labels",
            Self::BeatportPlaylists => "beatport/playlists",
            Self::BeatportTracks => "beatport/tracks",
            Self::Charts => "charts",
            Self::Downloads => "downloads",
            Self::Genres => "genres",
            Self::Playlists => "playlists",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MyListRequest {
    pub resource: MyListResource,
    pub filters: Option<BTreeMap<String, Value>>,
    pub page: Option<u64>,
    pub per_page: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MyGetResource {
    Account,
    Chart,
    EmailPreference,
    Playlist,
    StreamingQuality,
}

impl MyGetResource {
    fn segment(&self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Chart => "charts",
            Self::EmailPreference => "email-preferences",
            Self::Playlist => "playlists",
            Self::StreamingQuality => "streaming-quality",
        }
    }

    fn requires_id(&self) -> bool {
        !matches!(self, Self::Account)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MyGetRequest {
    pub resource: MyGetResource,
    pub id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistCreateRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistUpdateRequest {
    pub id: u64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistDeleteRequest {
    pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistAddTracksRequest {
    pub playlist_id: u64,
    pub track_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistReorderTrackRequest {
    pub playlist_id: u64,
    pub playlist_track_id: u64,
    pub position: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistRemoveTrackRequest {
    pub playlist_id: u64,
    pub playlist_track_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GenreIdRequest {
    pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DescribeEndpointRequest {
    pub path_or_tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum RawMethod {
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
}

impl RawMethod {
    fn as_reqwest(&self) -> Method {
        match self {
            Self::GET => Method::GET,
            Self::POST => Method::POST,
            Self::PUT => Method::PUT,
            Self::PATCH => Method::PATCH,
            Self::DELETE => Method::DELETE,
        }
    }

    fn is_get(&self) -> bool {
        matches!(self, Self::GET)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RawRequest {
    pub method: RawMethod,
    pub path: String,
    pub query: Option<BTreeMap<String, Value>>,
    pub body: Option<Value>,
    pub confirm_write: Option<bool>,
}

#[tool_router(router = tool_router)]
impl BeatportMcp {
    #[tool(
        name = "beatport_connect",
        description = "Authenticate the MCP server with Beatport using the configured OAuth application."
    )]
    async fn beatport_connect(&self) -> std::result::Result<Json<ConnectOutput>, ErrorData> {
        let result = self.client.connect().await.map_err(Self::tool_error)?;
        Ok(Json(ConnectOutput {
            authorization_url: result.authorization_url,
            opened_browser: result.opened_browser,
            expires_at: result.token.expires_at.to_rfc3339(),
            scope: result.token.scope,
            token_path: self.client.token_path_string(),
        }))
    }

    #[tool(
        name = "beatport_auth_status",
        description = "Report whether Beatport is connected and, if so, return the current account summary."
    )]
    async fn beatport_auth_status(&self) -> std::result::Result<Json<AuthStatusOutput>, ErrorData> {
        let (connected, token, account) =
            self.client.auth_status().await.map_err(Self::tool_error)?;
        Ok(Json(AuthStatusOutput {
            connected,
            token_path: self.client.token_path_string(),
            expires_at: token.as_ref().map(|token| token.expires_at.to_rfc3339()),
            scope: token.and_then(|token| token.scope),
            account,
        }))
    }

    #[tool(
        name = "beatport_disconnect",
        description = "Revoke the current Beatport token when possible and clear the local token cache."
    )]
    async fn beatport_disconnect(&self) -> std::result::Result<Json<DisconnectOutput>, ErrorData> {
        let result = self.client.disconnect().await.map_err(Self::tool_error)?;
        Ok(Json(DisconnectOutput {
            revoked: result.revoked,
            token_file_removed: result.token_file_removed,
            token_path: self.client.token_path_string(),
        }))
    }

    #[tool(
        name = "beatport_search",
        description = "Search Beatport catalog content and return grouped search results."
    )]
    async fn beatport_search(
        &self,
        Parameters(request): Parameters<SearchRequest>,
    ) -> std::result::Result<Json<SearchOutput>, ErrorData> {
        let mut query = merge_paging(None, request.page, request.per_page);
        query.insert("q".into(), json!(request.query));
        let response = self
            .client
            .request(Method::GET, "/v4/catalog/search/", Some(&query), None)
            .await
            .map_err(Self::tool_error)?;
        let mut data = serde_json::from_value::<SearchResults>(response.data)
            .map_err(|error| Self::tool_error(crate::error::AppError::from(error)))?;
        if let Some(include) = request.include.as_ref() {
            let keep = include
                .iter()
                .map(SearchInclude::as_str)
                .collect::<Vec<_>>();
            data.filter_to(&keep);
        }
        Ok(Json(SearchOutput {
            path: response.path,
            status: response.status,
            data,
        }))
    }

    #[tool(
        name = "beatport_search_compact",
        description = "Search Beatport tracks and return compact typed summaries that are easier to diff and review."
    )]
    async fn beatport_search_compact(
        &self,
        Parameters(request): Parameters<CompactSearchRequest>,
    ) -> std::result::Result<Json<CompactSearchOutput>, ErrorData> {
        let mut query = merge_paging(None, Some(1), request.per_page);
        query.insert("q".into(), json!(request.query));
        if request.include_tracks_only.unwrap_or(true) {
            query.insert("type".into(), json!("tracks"));
        }
        let response = self
            .client
            .request(Method::GET, "/v4/catalog/search/", Some(&query), None)
            .await
            .map_err(Self::tool_error)?;
        let candidates = response
            .data
            .get("tracks")
            .map(extract_compact_track_summaries)
            .transpose()
            .map_err(Self::tool_error)?
            .unwrap_or_default();
        Ok(Json(CompactSearchOutput {
            path: response.path,
            status: response.status,
            candidates,
        }))
    }

    #[tool(
        name = "beatport_trending_candidates",
        description = "Collect recent Beatport chart candidates for one or more genres as normalized sync rows."
    )]
    async fn beatport_trending_candidates(
        &self,
        Parameters(request): Parameters<TrendingCandidatesRequest>,
    ) -> std::result::Result<Json<TrendingCandidatesOutput>, ErrorData> {
        let output = self
            .sync_engine
            .trending_candidates(request)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(output))
    }

    #[tool(
        name = "beatport_playlist_sync_dry_run",
        description = "Resolve candidate rows against Beatport, diff them against a playlist and prior seen state, and persist a reviewable sync plan."
    )]
    async fn beatport_playlist_sync_dry_run(
        &self,
        Parameters(request): Parameters<PlaylistSyncDryRunRequest>,
    ) -> std::result::Result<Json<PlaylistSyncDryRunOutput>, ErrorData> {
        let output = self
            .sync_engine
            .playlist_sync_dry_run(request)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(output))
    }

    #[tool(
        name = "beatport_playlist_sync_apply",
        description = "Apply a previously persisted playlist sync dry-run plan by plan_id."
    )]
    async fn beatport_playlist_sync_apply(
        &self,
        Parameters(request): Parameters<PlaylistSyncApplyRequest>,
    ) -> std::result::Result<Json<PlaylistSyncApplyOutput>, ErrorData> {
        let output = self
            .sync_engine
            .playlist_sync_apply(request)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(output))
    }

    #[tool(
        name = "beatport_playlist_sync_review",
        description = "Load the remaining compact review items for a previously persisted sync plan."
    )]
    async fn beatport_playlist_sync_review(
        &self,
        Parameters(request): Parameters<PlaylistSyncReviewRequest>,
    ) -> std::result::Result<Json<PlaylistSyncReviewOutput>, ErrorData> {
        let output = self
            .sync_engine
            .playlist_sync_review(request)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(output))
    }

    #[tool(
        name = "beatport_playlist_sync_resolve",
        description = "Resolve ambiguous or not-found sync review items by choosing a track id or skipping them before apply."
    )]
    async fn beatport_playlist_sync_resolve(
        &self,
        Parameters(request): Parameters<PlaylistSyncResolveRequest>,
    ) -> std::result::Result<Json<PlaylistSyncResolveOutput>, ErrorData> {
        let output = self
            .sync_engine
            .playlist_sync_resolve(request)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(output))
    }

    #[tool(
        name = "beatport_playlist_tracks_compact",
        description = "List compact typed track summaries for a Beatport playlist page."
    )]
    async fn beatport_playlist_tracks_compact(
        &self,
        Parameters(request): Parameters<PlaylistTracksCompactRequest>,
    ) -> std::result::Result<Json<PlaylistTracksCompactOutput>, ErrorData> {
        let path = format!("/v4/my/playlists/{}/tracks/", request.playlist_id);
        let filters = merge_paging(None, request.page.or(Some(1)), request.per_page);
        let response = self
            .client
            .request(Method::GET, &path, Some(&filters), None)
            .await
            .map_err(Self::tool_error)?;
        let tracks = extract_compact_track_summaries(&response.data).map_err(Self::tool_error)?;
        Ok(Json(PlaylistTracksCompactOutput {
            path: response.path,
            status: response.status,
            playlist_id: request.playlist_id,
            tracks,
        }))
    }

    #[tool(
        name = "beatport_server_info",
        description = "Return Beatport MCP version, auth mode, sync state path, and the curated tool set expected after reconnect."
    )]
    async fn beatport_server_info(&self) -> std::result::Result<Json<ServerInfoOutput>, ErrorData> {
        let auth_mode = match self.client.auth_mode() {
            AuthMode::OAuthApp => "oauth_app",
            AuthMode::DocsFrontend => "docs_frontend",
        };
        Ok(Json(ServerInfoOutput {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            auth_mode: auth_mode.to_string(),
            token_path: self.client.token_path_string(),
            sync_state_path: self.client.sync_state_path_string(),
            curated_tools: vec![
                "beatport_search_compact".into(),
                "beatport_playlist_tracks_compact".into(),
                "beatport_trending_candidates".into(),
                "beatport_playlist_sync_dry_run".into(),
                "beatport_playlist_sync_review".into(),
                "beatport_playlist_sync_resolve".into(),
                "beatport_playlist_sync_apply".into(),
            ],
            requires_reconnect_after_tool_changes: true,
        }))
    }

    #[tool(
        name = "beatport_catalog_list",
        description = "List Beatport catalog resources with optional filters and pagination."
    )]
    async fn beatport_catalog_list(
        &self,
        Parameters(request): Parameters<CatalogListRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!("/v4/catalog/{}/", request.resource.segment());
        let filters = merge_paging(request.filters, request.page, request.per_page);
        let response = self
            .client
            .request(Method::GET, &path, Some(&filters), None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_catalog_get",
        description = "Fetch a single Beatport catalog object by resource type and id."
    )]
    async fn beatport_catalog_get(
        &self,
        Parameters(request): Parameters<CatalogGetRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!("/v4/catalog/{}/{}/", request.resource.segment(), request.id);
        let response = self
            .client
            .request(Method::GET, &path, None, None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_catalog_related",
        description = "Fetch a related Beatport catalog subresource such as release tracks or playlist tracks."
    )]
    async fn beatport_catalog_related(
        &self,
        Parameters(request): Parameters<CatalogRelatedRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let relation = sanitize_relation(&request.relation).map_err(Self::tool_error)?;
        let path = format!(
            "/v4/catalog/{}/{}/{}/",
            request.resource.segment(),
            request.id,
            relation
        );
        let filters = merge_paging(request.filters, request.page, request.per_page);
        let response = self
            .client
            .request(Method::GET, &path, Some(&filters), None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_me_list",
        description = "List the authenticated user's Beatport resources such as playlists, downloads, genres, and saved objects."
    )]
    async fn beatport_me_list(
        &self,
        Parameters(request): Parameters<MyListRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!("/v4/my/{}/", request.resource.segment());
        let filters = merge_paging(request.filters, request.page, request.per_page);
        let response = self
            .client
            .request(Method::GET, &path, Some(&filters), None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_me_get",
        description = "Fetch a direct authenticated Beatport object such as the account profile, a playlist, or a chart."
    )]
    async fn beatport_me_get(
        &self,
        Parameters(request): Parameters<MyGetRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = match (request.resource.requires_id(), request.id) {
            (false, None) => format!("/v4/my/{}/", request.resource.segment()),
            (false, Some(_)) => {
                return Err(ErrorData::from(crate::error::AppError::InvalidConfig(
                    "this Beatport me resource does not take an id".into(),
                )));
            }
            (true, Some(id)) => format!("/v4/my/{}/{}/", request.resource.segment(), id),
            (true, None) => {
                return Err(ErrorData::from(crate::error::AppError::InvalidConfig(
                    "this Beatport me resource requires an id".into(),
                )));
            }
        };
        let response = self
            .client
            .request(Method::GET, &path, None, None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_playlist_create",
        description = "Create a Beatport playlist owned by the authenticated user."
    )]
    async fn beatport_playlist_create(
        &self,
        Parameters(request): Parameters<PlaylistCreateRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let response = self
            .client
            .request(
                Method::POST,
                "/v4/my/playlists/",
                None,
                Some(&json!({ "name": request.name })),
            )
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_playlist_update",
        description = "Rename a Beatport playlist owned by the authenticated user."
    )]
    async fn beatport_playlist_update(
        &self,
        Parameters(request): Parameters<PlaylistUpdateRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!("/v4/my/playlists/{}/", request.id);
        let response = self
            .client
            .request(
                Method::PATCH,
                &path,
                None,
                Some(&json!({ "name": request.name })),
            )
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_playlist_delete",
        description = "Delete a Beatport playlist owned by the authenticated user."
    )]
    async fn beatport_playlist_delete(
        &self,
        Parameters(request): Parameters<PlaylistDeleteRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!("/v4/my/playlists/{}/", request.id);
        let response = self
            .client
            .request(Method::DELETE, &path, None, None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_playlist_add_tracks",
        description = "Add one or more tracks to a Beatport playlist. Uses the single-track or bulk endpoint automatically."
    )]
    async fn beatport_playlist_add_tracks(
        &self,
        Parameters(request): Parameters<PlaylistAddTracksRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        if request.track_ids.is_empty() {
            return Err(ErrorData::from(crate::error::AppError::InvalidConfig(
                "track_ids must contain at least one Beatport track id".into(),
            )));
        }
        let (path, body) = if request.track_ids.len() == 1 {
            (
                format!("/v4/my/playlists/{}/tracks/", request.playlist_id),
                json!({ "track_id": request.track_ids[0] }),
            )
        } else {
            (
                format!("/v4/my/playlists/{}/tracks/bulk/", request.playlist_id),
                json!({ "track_ids": request.track_ids }),
            )
        };
        let response = self
            .client
            .request(Method::POST, &path, None, Some(&body))
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_playlist_reorder_track",
        description = "Reorder an existing track entry inside a Beatport playlist."
    )]
    async fn beatport_playlist_reorder_track(
        &self,
        Parameters(request): Parameters<PlaylistReorderTrackRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!(
            "/v4/my/playlists/{}/tracks/{}/",
            request.playlist_id, request.playlist_track_id
        );
        let response = self
            .client
            .request(
                Method::PATCH,
                &path,
                None,
                Some(&json!({ "position": request.position })),
            )
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_playlist_remove_track",
        description = "Remove a track entry from a Beatport playlist."
    )]
    async fn beatport_playlist_remove_track(
        &self,
        Parameters(request): Parameters<PlaylistRemoveTrackRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!(
            "/v4/my/playlists/{}/tracks/{}/",
            request.playlist_id, request.playlist_track_id
        );
        let response = self
            .client
            .request(Method::DELETE, &path, None, None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_genre_subscribe",
        description = "Subscribe the authenticated user to a Beatport genre."
    )]
    async fn beatport_genre_subscribe(
        &self,
        Parameters(request): Parameters<GenreIdRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let response = self
            .client
            .request(
                Method::POST,
                "/v4/my/genres/",
                None,
                Some(&json!({ "id": request.id })),
            )
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_genre_unsubscribe",
        description = "Unsubscribe the authenticated user from a Beatport genre."
    )]
    async fn beatport_genre_unsubscribe(
        &self,
        Parameters(request): Parameters<GenreIdRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        let path = format!("/v4/my/genres/{}/", request.id);
        let response = self
            .client
            .request(Method::DELETE, &path, None, None)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }

    #[tool(
        name = "beatport_describe_endpoint",
        description = "Inspect the authenticated Beatport schema by exact path, path fragment, or tag."
    )]
    async fn beatport_describe_endpoint(
        &self,
        Parameters(request): Parameters<DescribeEndpointRequest>,
    ) -> std::result::Result<Json<DescribeEndpointOutput>, ErrorData> {
        let output = self
            .client
            .describe_endpoint(&request.path_or_tag)
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(output))
    }

    #[tool(
        name = "beatport_request",
        description = "Perform a guarded raw Beatport API request for endpoints not yet covered by curated tools."
    )]
    async fn beatport_request(
        &self,
        Parameters(request): Parameters<RawRequest>,
    ) -> std::result::Result<Json<ApiResponse>, ErrorData> {
        if request.path.starts_with("/v4/auth/") {
            return Err(ErrorData::from(crate::error::AppError::UnsafePath(
                "raw requests may not target Beatport auth endpoints".into(),
            )));
        }
        if !request.method.is_get() && request.confirm_write != Some(true) {
            return Err(ErrorData::from(crate::error::AppError::InvalidConfig(
                "non-GET raw requests require confirm_write=true".into(),
            )));
        }
        let response = self
            .client
            .request(
                request.method.as_reqwest(),
                &request.path,
                request.query.as_ref(),
                request.body.as_ref(),
            )
            .await
            .map_err(Self::tool_error)?;
        Ok(Json(response))
    }
}
