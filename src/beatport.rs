use std::{collections::BTreeMap, sync::Arc, time::Duration};

use reqwest::{
    Method, StatusCode,
    header::{AUTHORIZATION, RETRY_AFTER},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::RwLock;
use url::Url;

use crate::{
    auth::{AuthManager, ConnectResult, DisconnectResult, TokenSet},
    config::Config,
    error::{AppError, Result},
};

const API_ROOT: &str = "https://api.beatport.com";
const SCHEMA_PATH: &str = "/v4/swagger-ui/json/";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TRANSIENT_RETRIES: usize = 3;

#[derive(Debug, Clone)]
pub struct BeatportClient {
    http: reqwest::Client,
    config: Config,
    auth: Arc<AuthManager>,
    schema_cache: Arc<RwLock<Option<Value>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApiResponse {
    pub method: String,
    pub path: String,
    pub status: u16,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchResults {
    pub tracks: Option<Value>,
    pub order: Option<Value>,
    pub artists: Option<Value>,
    pub releases: Option<Value>,
    pub charts: Option<Value>,
    pub labels: Option<Value>,
    pub playlists: Option<Value>,
}

impl SearchResults {
    pub fn filter_to(&mut self, keep: &[&str]) {
        if !keep.contains(&"tracks") {
            self.tracks = None;
        }
        if !keep.contains(&"artists") {
            self.artists = None;
        }
        if !keep.contains(&"releases") {
            self.releases = None;
        }
        if !keep.contains(&"charts") {
            self.charts = None;
        }
        if !keep.contains(&"labels") {
            self.labels = None;
        }
        if !keep.contains(&"playlists") {
            self.playlists = None;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EndpointDescriptionParameter {
    pub name: String,
    pub location: String,
    pub required: bool,
    pub schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EndpointDescriptionMethod {
    pub method: String,
    pub operation_id: Option<String>,
    pub summary: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub parameters: Vec<EndpointDescriptionParameter>,
    pub request_body: Option<Value>,
    pub responses: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EndpointDescriptionMatch {
    pub path: String,
    pub methods: Vec<EndpointDescriptionMethod>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DescribeEndpointOutput {
    pub query: String,
    pub match_kind: String,
    pub matches: Vec<EndpointDescriptionMatch>,
}

impl BeatportClient {
    pub fn new(config: Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("beatport-mcp/0.1.0")
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()?;
        let auth = Arc::new(AuthManager::new(config.clone(), http.clone())?);
        Ok(Self {
            http,
            config,
            auth,
            schema_cache: Arc::new(RwLock::new(None)),
        })
    }

    pub fn token_path_string(&self) -> String {
        self.config.token_path_string()
    }

    pub fn sync_state_path(&self) -> std::path::PathBuf {
        self.config.sync_state_path.clone()
    }

    pub fn sync_state_path_string(&self) -> String {
        self.config.sync_state_path_string()
    }

    pub async fn snapshot_token(&self) -> Option<TokenSet> {
        self.auth.snapshot().await
    }

    pub async fn connect(&self) -> Result<ConnectResult> {
        self.auth.connect().await
    }

    pub async fn disconnect(&self) -> Result<DisconnectResult> {
        let result = self.auth.disconnect().await?;
        let mut schema = self.schema_cache.write().await;
        *schema = None;
        Ok(result)
    }

    pub async fn auth_status(&self) -> Result<(bool, Option<TokenSet>, Option<Value>)> {
        let token = self.snapshot_token().await;
        if token.is_none() {
            return Ok((false, None, None));
        }

        match self
            .request(Method::GET, "/v4/my/account/", None, None)
            .await
        {
            Ok(response) => Ok((true, token, Some(response.data))),
            Err(AppError::AuthenticationRequired)
            | Err(AppError::RefreshUnavailable)
            | Err(AppError::BeatportApi { status: 401, .. }) => Ok((false, token, None)),
            Err(error) => Err(error),
        }
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        query: Option<&BTreeMap<String, Value>>,
        body: Option<&Value>,
    ) -> Result<ApiResponse> {
        self.request_inner(method, path, query, body, true).await
    }

    pub async fn load_schema(&self) -> Result<Value> {
        if let Some(schema) = self.schema_cache.read().await.clone() {
            return Ok(schema);
        }

        let response = self
            .request(Method::GET, SCHEMA_PATH, None, None)
            .await?
            .data;
        let mut guard = self.schema_cache.write().await;
        *guard = Some(response.clone());
        Ok(response)
    }

    pub async fn describe_endpoint(&self, query: &str) -> Result<DescribeEndpointOutput> {
        let schema = self.load_schema().await?;
        let paths = schema
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AppError::InvalidConfig("Beatport schema did not include paths".into())
            })?;

        let mut matches = Vec::new();
        let trimmed_query = query.trim();

        let match_kind = if trimmed_query.starts_with('/') {
            if let Some(item) = paths.get(trimmed_query) {
                matches.push(build_description_match(trimmed_query, item)?);
                "path".to_string()
            } else {
                "path".to_string()
            }
        } else {
            for (path, item) in paths {
                if path.contains(trimmed_query) || has_matching_tag(item, trimmed_query) {
                    matches.push(build_description_match(path, item)?);
                }
            }
            if matches.iter().any(|m| m.path == trimmed_query) {
                "path".to_string()
            } else if matches.iter().any(|m| {
                m.methods
                    .iter()
                    .any(|method| method.tags.iter().any(|tag| tag == trimmed_query))
            }) {
                "tag".to_string()
            } else {
                "search".to_string()
            }
        };

        matches.sort_by(|left, right| left.path.cmp(&right.path));

        Ok(DescribeEndpointOutput {
            query: trimmed_query.to_string(),
            match_kind,
            matches,
        })
    }

    async fn request_inner(
        &self,
        method: Method,
        path: &str,
        query: Option<&BTreeMap<String, Value>>,
        body: Option<&Value>,
        allow_retry: bool,
    ) -> Result<ApiResponse> {
        let url = build_api_url(path, query)?;
        let mut transient_attempt = 0usize;
        let mut auth_retry_available = allow_retry;

        loop {
            let token = self.auth.access_token().await?;
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .header(AUTHORIZATION, format!("Bearer {token}"));
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(error)
                    if should_retry_transport_error(&error)
                        && transient_attempt < MAX_TRANSIENT_RETRIES =>
                {
                    let delay = retry_delay_for_attempt(transient_attempt, None);
                    transient_attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let status = response.status();

            if status == StatusCode::UNAUTHORIZED && auth_retry_available {
                let _ = self.auth.force_refresh().await?;
                auth_retry_available = false;
                continue;
            }

            if should_retry_status(status) && transient_attempt < MAX_TRANSIENT_RETRIES {
                let delay =
                    retry_delay_for_attempt(transient_attempt, response.headers().get(RETRY_AFTER));
                transient_attempt += 1;
                tokio::time::sleep(delay).await;
                continue;
            }

            let data = parse_response_body(response).await?;
            if !status.is_success() {
                return Err(AppError::beatport_api(status, Some(data)));
            }

            return Ok(ApiResponse {
                method: method.as_str().to_string(),
                path: path.to_string(),
                status: status.as_u16(),
                data,
            });
        }
    }
}

fn should_retry_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn retry_delay_for_attempt(
    attempt: usize,
    retry_after: Option<&reqwest::header::HeaderValue>,
) -> Duration {
    if let Some(delay) = retry_after
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| raw.parse::<u64>().ok())
    {
        return Duration::from_secs(delay.min(30));
    }
    let seconds = match attempt {
        0 => 1,
        1 => 2,
        _ => 4,
    };
    Duration::from_secs(seconds)
}

fn build_api_url(path: &str, query: Option<&BTreeMap<String, Value>>) -> Result<Url> {
    if !path.starts_with("/v4/") {
        return Err(AppError::UnsafePath(path.to_string()));
    }
    if path.contains("..") || path.contains('?') || path.contains('#') {
        return Err(AppError::UnsafePath(path.to_string()));
    }

    let mut url = Url::parse(API_ROOT)?.join(path.trim_start_matches('/'))?;
    if let Some(query) = query {
        let pairs = serialize_query_pairs(query)?;
        let mut serializer = url.query_pairs_mut();
        for (key, value) in pairs {
            serializer.append_pair(&key, &value);
        }
    }
    Ok(url)
}

pub fn serialize_query_pairs(query: &BTreeMap<String, Value>) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    for (key, value) in query {
        serialize_query_value(key, value, &mut pairs)?;
    }
    Ok(pairs)
}

fn serialize_query_value(
    key: &str,
    value: &Value,
    pairs: &mut Vec<(String, String)>,
) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Bool(boolean) => {
            pairs.push((key.to_string(), boolean.to_string()));
            Ok(())
        }
        Value::Number(number) => {
            pairs.push((key.to_string(), number.to_string()));
            Ok(())
        }
        Value::String(text) => {
            pairs.push((key.to_string(), text.clone()));
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                match item {
                    Value::Object(_) | Value::Array(_) => {
                        return Err(AppError::QuerySerialization(format!(
                            "query field `{key}` contains a nested value that Beatport cannot serialize"
                        )));
                    }
                    _ => serialize_query_value(key, item, pairs)?,
                }
            }
            Ok(())
        }
        Value::Object(_) => Err(AppError::QuerySerialization(format!(
            "query field `{key}` cannot be a JSON object"
        ))),
    }
}

async fn parse_response_body(response: reqwest::Response) -> Result<Value> {
    if response.status() == StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let text = response.text().await?;
    if text.trim().is_empty() {
        return Ok(Value::Null);
    }
    if content_type.contains("application/json") || looks_like_json(&text) {
        serde_json::from_str(&text).map_err(Into::into)
    } else {
        Ok(Value::String(text))
    }
}

fn looks_like_json(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with('{') || trimmed.starts_with('[')
}

fn has_matching_tag(path_item: &Value, query: &str) -> bool {
    path_item
        .as_object()
        .into_iter()
        .flat_map(|methods| methods.values())
        .any(|method| {
            method
                .get("tags")
                .and_then(Value::as_array)
                .into_iter()
                .flat_map(|tags| tags.iter())
                .any(|tag| tag.as_str() == Some(query))
        })
}

fn build_description_match(path: &str, path_item: &Value) -> Result<EndpointDescriptionMatch> {
    let methods = path_item
        .as_object()
        .ok_or_else(|| {
            AppError::InvalidConfig("Beatport schema path item was not an object".into())
        })?
        .iter()
        .filter_map(|(method, definition)| match method.as_str() {
            "get" | "post" | "put" | "patch" | "delete" => {
                Some(build_description_method(method, definition))
            }
            _ => None,
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(EndpointDescriptionMatch {
        path: path.to_string(),
        methods,
    })
}

fn build_description_method(method: &str, definition: &Value) -> Result<EndpointDescriptionMethod> {
    let parameters = definition
        .get("parameters")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| EndpointDescriptionParameter {
                    name: item
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    location: item
                        .get("in")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    required: item
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    schema: item.get("schema").map(schema_excerpt),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let request_body = definition
        .get("requestBody")
        .and_then(|request_body| request_body.get("content"))
        .and_then(Value::as_object)
        .map(|content| {
            let mut out = Map::new();
            for (content_type, schema) in content {
                if let Some(schema) = schema.get("schema") {
                    out.insert(content_type.clone(), schema_excerpt(schema));
                }
            }
            Value::Object(out)
        });

    let responses = definition
        .get("responses")
        .and_then(Value::as_object)
        .map(|responses| {
            responses
                .iter()
                .map(|(status, value)| {
                    let excerpt = value
                        .get("content")
                        .and_then(Value::as_object)
                        .map(|content| {
                            let mut out = Map::new();
                            for (content_type, schema) in content {
                                if let Some(schema) = schema.get("schema") {
                                    out.insert(content_type.clone(), schema_excerpt(schema));
                                }
                            }
                            Value::Object(out)
                        })
                        .unwrap_or(Value::Null);
                    (status.clone(), excerpt)
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let tags = definition
        .get("tags")
        .and_then(Value::as_array)
        .map(|tags| {
            tags.iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(EndpointDescriptionMethod {
        method: method.to_uppercase(),
        operation_id: definition
            .get("operationId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        summary: definition
            .get("summary")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        description: definition
            .get("description")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        tags,
        parameters,
        request_body,
        responses,
    })
}

fn schema_excerpt(schema: &Value) -> Value {
    match schema {
        Value::Object(map) => {
            let mut out = Map::new();
            for key in ["$ref", "type", "format", "description", "nullable"] {
                if let Some(value) = map.get(key) {
                    out.insert(key.to_string(), value.clone());
                }
            }
            if let Some(value) = map.get("enum") {
                out.insert("enum".to_string(), value.clone());
            }
            if let Some(value) = map.get("required") {
                out.insert("required".to_string(), value.clone());
            }
            if let Some(items) = map.get("items") {
                out.insert("items".to_string(), schema_excerpt(items));
            }
            if let Some(properties) = map.get("properties").and_then(Value::as_object) {
                let mut prop_out = Map::new();
                for (index, (key, value)) in properties.iter().enumerate() {
                    if index >= 12 {
                        break;
                    }
                    prop_out.insert(key.clone(), schema_excerpt(value));
                }
                out.insert("properties".to_string(), Value::Object(prop_out));
            }
            Value::Object(out)
        }
        _ => schema.clone(),
    }
}

pub fn merge_paging(
    filters: Option<BTreeMap<String, Value>>,
    page: Option<u64>,
    per_page: Option<u64>,
) -> BTreeMap<String, Value> {
    let mut out = filters.unwrap_or_default();
    if let Some(page) = page {
        out.insert("page".into(), json!(page));
    }
    if let Some(per_page) = per_page {
        out.insert("per_page".into(), json!(per_page));
    }
    out
}

pub fn sanitize_relation(relation: &str) -> Result<String> {
    let trimmed = relation.trim_matches('/');
    if trimmed.is_empty()
        || trimmed.contains("..")
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.starts_with("v4/")
    {
        return Err(AppError::InvalidRelation(relation.to_string()));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use reqwest::StatusCode;
    use serde_json::json;

    use super::{
        merge_paging, retry_delay_for_attempt, sanitize_relation, serialize_query_pairs,
        should_retry_status,
    };

    #[test]
    fn serializes_scalar_and_array_queries() {
        let mut query = BTreeMap::new();
        query.insert("page".into(), json!(2));
        query.insert("enabled".into(), json!(true));
        query.insert("genre_id".into(), json!([1, 2]));

        let pairs = serialize_query_pairs(&query).expect("query should serialize");
        assert!(pairs.contains(&("page".into(), "2".into())));
        assert!(pairs.contains(&("enabled".into(), "true".into())));
        assert!(pairs.contains(&("genre_id".into(), "1".into())));
        assert!(pairs.contains(&("genre_id".into(), "2".into())));
    }

    #[test]
    fn rejects_object_queries() {
        let mut query = BTreeMap::new();
        query.insert("filters".into(), json!({ "unexpected": true }));
        let error = serialize_query_pairs(&query).expect_err("object queries should fail");
        assert!(
            error
                .to_string()
                .contains("query field `filters` cannot be a JSON object")
        );
    }

    #[test]
    fn merges_paging_fields() {
        let filters = merge_paging(None, Some(3), Some(20));
        assert_eq!(filters.get("page"), Some(&json!(3)));
        assert_eq!(filters.get("per_page"), Some(&json!(20)));
    }

    #[test]
    fn relation_must_be_safe() {
        let error = sanitize_relation("../bad").expect_err("unsafe relation should fail");
        assert!(error.to_string().contains("invalid Beatport relation"));
    }

    #[test]
    fn retries_transient_status_codes() {
        assert!(should_retry_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(should_retry_status(StatusCode::BAD_GATEWAY));
        assert!(!should_retry_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn respects_retry_after_when_present() {
        let header = reqwest::header::HeaderValue::from_static("7");
        assert_eq!(
            retry_delay_for_attempt(0, Some(&header)),
            Duration::from_secs(7)
        );
    }
}
