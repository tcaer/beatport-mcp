use std::{fs, path::Path, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::random;
use reqwest::{
    header::{AUTHORIZATION, CONTENT_LENGTH, COOKIE, LOCATION, SET_COOKIE},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::RwLock,
    time::timeout,
};
use url::Url;

use crate::{
    config::{AuthMode, Config},
    error::{AppError, Result},
};

const API_ROOT: &str = "https://api.beatport.com";
const DOCS_URL: &str = "https://api.beatport.com/v4/docs/";
const AUTHORIZATION_URL: &str = "https://api.beatport.com/v4/auth/o/authorize/";
const LOGIN_URL: &str = "https://api.beatport.com/v4/auth/login/";
const TOKEN_URL: &str = "https://api.beatport.com/v4/auth/o/token/";
const REVOKE_URL: &str = "https://api.beatport.com/v4/auth/o/revoke/";
const USER_AGENT: &str = "beatport-mcp/0.1.0";
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);
const REFRESH_SKEW_SECONDS: i64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenSet {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_type: String,
    pub scope: Option<String>,
    pub expires_at: DateTime<Utc>,
}

impl TokenSet {
    pub fn is_expiring_soon(&self) -> bool {
        self.expires_at <= Utc::now() + ChronoDuration::seconds(REFRESH_SKEW_SECONDS)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectResult {
    pub authorization_url: String,
    pub opened_browser: bool,
    pub token: TokenSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisconnectResult {
    pub revoked: bool,
    pub token_file_removed: bool,
}

#[derive(Debug)]
pub struct AuthManager {
    config: Config,
    http: reqwest::Client,
    resolved_client_id: RwLock<Option<String>>,
    tokens: RwLock<Option<TokenSet>>,
}

impl AuthManager {
    pub fn new(config: Config, http: reqwest::Client) -> Result<Self> {
        let tokens = load_tokens(&config.token_path)?;
        Ok(Self {
            resolved_client_id: RwLock::new(config.client_id.clone()),
            config,
            http,
            tokens: RwLock::new(tokens),
        })
    }

    pub fn token_path(&self) -> &Path {
        &self.config.token_path
    }

    pub fn token_path_string(&self) -> String {
        self.config.token_path_string()
    }

    pub async fn snapshot(&self) -> Option<TokenSet> {
        self.tokens.read().await.clone()
    }

    pub async fn connect(&self) -> Result<ConnectResult> {
        match self.config.auth_mode {
            AuthMode::OAuthApp => self.connect_with_oauth_app().await,
            AuthMode::DocsFrontend => self.connect_with_docs_frontend().await,
        }
    }

    async fn connect_with_oauth_app(&self) -> Result<ConnectResult> {
        let state = generate_random_urlsafe(32);
        let verifier = generate_random_urlsafe(48);
        let challenge = pkce_challenge(&verifier);
        let listener = TcpListener::bind(self.config.callback_bind_addr()?).await?;
        let authorization_url = build_authorization_url(&self.config, &state, &challenge)?;

        let opened_browser = webbrowser::open(authorization_url.as_str()).is_ok();
        if !opened_browser {
            eprintln!(
                "Open this URL in your browser to finish Beatport login: {}",
                authorization_url
            );
        }

        let code = wait_for_callback(
            listener,
            self.config.callback_path(),
            &state,
            &authorization_url,
        )
        .await?;
        let token = self.exchange_authorization_code(&code, &verifier).await?;
        self.store_token(Some(token.clone())).await?;

        Ok(ConnectResult {
            authorization_url: authorization_url.into(),
            opened_browser,
            token,
        })
    }

    async fn connect_with_docs_frontend(&self) -> Result<ConnectResult> {
        let client_id = self.resolve_client_id().await?;
        let authorization_url =
            build_docs_authorization_url(&client_id, &self.config.redirect_uri)?;
        let token = self.exchange_docs_frontend_credentials(&client_id).await?;
        self.store_token(Some(token.clone())).await?;

        Ok(ConnectResult {
            authorization_url: authorization_url.into(),
            opened_browser: false,
            token,
        })
    }

    pub async fn disconnect(&self) -> Result<DisconnectResult> {
        let token = self.snapshot().await;
        let revoked = if let Some(token) = token {
            self.revoke_token(&token.access_token).await.is_ok()
        } else {
            false
        };
        let token_file_removed = self.clear_tokens().await?;
        Ok(DisconnectResult {
            revoked,
            token_file_removed,
        })
    }

    pub async fn access_token(&self) -> Result<String> {
        let snapshot = self
            .snapshot()
            .await
            .ok_or(AppError::AuthenticationRequired)?;
        if snapshot.is_expiring_soon() {
            let refreshed = self.refresh_or_reconnect(&snapshot).await?;
            return Ok(refreshed.access_token);
        }
        Ok(snapshot.access_token)
    }

    pub async fn force_refresh(&self) -> Result<TokenSet> {
        let snapshot = self
            .snapshot()
            .await
            .ok_or(AppError::AuthenticationRequired)?;
        self.refresh_or_reconnect(&snapshot).await
    }

    async fn refresh_or_reconnect(&self, current: &TokenSet) -> Result<TokenSet> {
        match self.refresh_tokens(current).await {
            Ok(token) => Ok(token),
            Err(error)
                if self.config.can_password_bootstrap()
                    && should_retry_with_docs_frontend_credentials(&error) =>
            {
                let result = self.connect_with_docs_frontend().await?;
                Ok(result.token)
            }
            Err(error) => Err(error),
        }
    }

    async fn refresh_tokens(&self, current: &TokenSet) -> Result<TokenSet> {
        let refresh_token = current
            .refresh_token
            .clone()
            .ok_or(AppError::RefreshUnavailable)?;
        let client_id = self.resolve_client_id().await?;
        let mut form = vec![
            ("client_id".to_string(), client_id),
            ("refresh_token".to_string(), refresh_token),
            ("grant_type".to_string(), "refresh_token".to_string()),
        ];
        if let Some(client_secret) = self.config.client_secret() {
            form.push(("client_secret".to_string(), client_secret.to_string()));
        }
        let request = self
            .http
            .post(TOKEN_URL)
            .header(AUTHORIZATION, format!("Bearer {}", current.access_token))
            .form(&form);
        let response = request.send().await?;
        let token = parse_token_response(response).await?;
        self.store_token(Some(token.clone())).await?;
        Ok(token)
    }

    async fn exchange_authorization_code(
        &self,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenSet> {
        let client_id = self.resolve_client_id().await?;
        let mut form = vec![
            ("client_id".to_string(), client_id),
            (
                "redirect_uri".to_string(),
                self.config.redirect_uri.as_str().to_string(),
            ),
            ("code".to_string(), code.to_string()),
            ("grant_type".to_string(), "authorization_code".to_string()),
            ("code_verifier".to_string(), code_verifier.to_string()),
        ];
        if let Some(client_secret) = self.config.client_secret() {
            form.push(("client_secret".to_string(), client_secret.to_string()));
        }
        let response = self.http.post(TOKEN_URL).form(&form).send().await?;
        parse_token_response(response).await
    }

    async fn revoke_token(&self, access_token: &str) -> Result<()> {
        let client_id = self.resolve_client_id().await?;
        let response = self
            .http
            .post(REVOKE_URL)
            .query(&[("client_id", client_id.as_str()), ("token", access_token)])
            .header(CONTENT_LENGTH, "0")
            .body("")
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.json::<Value>().await.ok();
            return Err(AppError::beatport_api(status, body));
        }
        Ok(())
    }

    async fn store_token(&self, token: Option<TokenSet>) -> Result<()> {
        let serialized = token.clone();
        {
            let mut guard = self.tokens.write().await;
            *guard = token;
        }
        match serialized {
            Some(tokens) => persist_tokens(&self.config.token_path, &tokens),
            None => delete_tokens(&self.config.token_path).map(|_| ()),
        }
    }

    async fn clear_tokens(&self) -> Result<bool> {
        {
            let mut guard = self.tokens.write().await;
            *guard = None;
        }
        delete_tokens(&self.config.token_path)
    }

    async fn resolve_client_id(&self) -> Result<String> {
        if let Some(client_id) = self.resolved_client_id.read().await.clone() {
            return Ok(client_id);
        }

        match self.config.auth_mode {
            AuthMode::OAuthApp => Err(AppError::MissingConfig("BEATPORT_CLIENT_ID")),
            AuthMode::DocsFrontend => {
                let client_id = self.fetch_docs_frontend_client_id().await?;
                let mut guard = self.resolved_client_id.write().await;
                *guard = Some(client_id.clone());
                Ok(client_id)
            }
        }
    }

    async fn fetch_docs_frontend_client_id(&self) -> Result<String> {
        let html = self.http.get(DOCS_URL).send().await?.text().await?;
        let docs_url = Url::parse(DOCS_URL)?;
        for script_src in extract_script_sources(&html) {
            let script_url = docs_url.join(&script_src)?;
            let script = self.http.get(script_url).send().await?.text().await?;
            if let Some(client_id) = extract_client_id_from_script(&script) {
                return Ok(client_id);
            }
        }

        Err(AppError::AuthenticationFailed(
            "could not fetch Beatport docs frontend client id".into(),
        ))
    }

    async fn exchange_docs_frontend_credentials(&self, client_id: &str) -> Result<TokenSet> {
        let username = self
            .config
            .username()
            .ok_or(AppError::MissingConfig("BEATPORT_USERNAME"))?;
        let password = self
            .config
            .password()
            .ok_or(AppError::MissingConfig("BEATPORT_PASSWORD"))?;
        let session = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .redirect(Policy::none())
            .build()?;

        let login_response = session
            .post(LOGIN_URL)
            .json(&json!({
                "username": username,
                "password": password,
            }))
            .send()
            .await?;
        debug_auth(format_args!(
            "docs_frontend login status {}",
            login_response.status()
        ));
        let cookie_header = extract_cookie_header(login_response.headers());
        let login_status = login_response.status();
        let login_body = login_response.json::<Value>().await?;
        if !login_status.is_success() {
            return Err(AppError::beatport_api(login_status, Some(login_body)));
        }
        if login_body.get("username").is_none() || login_body.get("email").is_none() {
            let message = extract_error_message_value(&login_body)
                .unwrap_or_else(|| "Beatport login did not return an authenticated user".into());
            return Err(AppError::AuthenticationFailed(message));
        }

        let authorization_url = build_docs_authorization_url(client_id, &self.config.redirect_uri)?;
        let mut authorize_request = session.get(authorization_url.clone());
        if let Some(cookie_header) = cookie_header.as_deref() {
            authorize_request = authorize_request.header(COOKIE, cookie_header);
        }
        let authorize_response = authorize_request.send().await?;
        debug_auth(format_args!(
            "docs_frontend authorize status {}",
            authorize_response.status()
        ));
        let authorize_status = authorize_response.status();
        let authorize_location = authorize_response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let authorize_body = authorize_response.text().await?;

        let code = match authorize_location {
            Some(location) => extract_authorization_code(&location)?,
            None => {
                let message = extract_html_paragraph(&authorize_body)
                    .unwrap_or_else(|| authorize_body.trim().to_string());
                let message = if message.is_empty() {
                    format!(
                        "Beatport OAuth redirect missing Location header (status {authorize_status})"
                    )
                } else {
                    message
                };
                return Err(AppError::AuthenticationFailed(message));
            }
        };

        let mut token_request = session
            .post(TOKEN_URL)
            .query(&[
                ("code", code.as_str()),
                ("grant_type", "authorization_code"),
                ("redirect_uri", self.config.redirect_uri.as_str()),
                ("client_id", client_id),
            ])
            .header(CONTENT_LENGTH, "0")
            .body("");
        if let Some(cookie_header) = cookie_header.as_deref() {
            token_request = token_request.header(COOKIE, cookie_header);
        }
        let response = token_request.send().await?;
        debug_auth(format_args!(
            "docs_frontend token status {}",
            response.status()
        ));
        parse_token_response(response).await
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    scope: Option<String>,
    token_type: Option<String>,
}

async fn parse_token_response(response: reqwest::Response) -> Result<TokenSet> {
    let status = response.status();
    if !status.is_success() {
        let body = response.json::<Value>().await.ok();
        return Err(AppError::beatport_api(status, body));
    }
    let payload = response.json::<TokenResponse>().await?;
    let expires_in = payload.expires_in.unwrap_or(3600);
    Ok(TokenSet {
        access_token: payload.access_token,
        refresh_token: payload.refresh_token,
        token_type: payload.token_type.unwrap_or_else(|| "Bearer".into()),
        scope: payload.scope,
        expires_at: Utc::now() + ChronoDuration::seconds(expires_in),
    })
}

fn build_authorization_url(config: &Config, state: &str, code_challenge: &str) -> Result<Url> {
    let client_id = config
        .client_id()
        .ok_or(AppError::MissingConfig("BEATPORT_CLIENT_ID"))?;
    build_oauth_authorization_url(config, client_id, state, code_challenge)
}

fn build_oauth_authorization_url(
    config: &Config,
    client_id: &str,
    state: &str,
    code_challenge: &str,
) -> Result<Url> {
    let mut url = Url::parse(AUTHORIZATION_URL)?;
    url.query_pairs_mut()
        .append_pair("client_id", client_id)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", config.redirect_uri.as_str())
        .append_pair("state", state)
        .append_pair("scope", &config.scope)
        .append_pair("code_challenge", code_challenge)
        .append_pair("code_challenge_method", "S256");
    Ok(url)
}

fn build_docs_authorization_url(client_id: &str, redirect_uri: &Url) -> Result<Url> {
    let mut url = Url::parse(AUTHORIZATION_URL)?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", redirect_uri.as_str());
    Ok(url)
}

async fn wait_for_callback(
    listener: TcpListener,
    expected_path: &str,
    expected_state: &str,
    authorization_url: &Url,
) -> Result<String> {
    let (stream, _) = timeout(CALLBACK_TIMEOUT, listener.accept())
        .await
        .map_err(|_| AppError::OAuthTimeout {
            authorization_url: authorization_url.to_string(),
        })??;

    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    let target = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| AppError::InvalidCallback("missing request target".into()))?;

    loop {
        let mut header = String::new();
        let bytes = reader.read_line(&mut header).await?;
        if bytes == 0 || header == "\r\n" {
            break;
        }
    }

    let parsed = Url::parse(&format!("http://localhost{target}"))?;
    let mut stream = reader.into_inner();

    if parsed.path() != expected_path {
        respond_with_html(
            &mut stream,
            404,
            "Beatport callback path mismatch. You can close this tab.",
        )
        .await?;
        return Err(AppError::InvalidCallback(format!(
            "unexpected callback path: {}",
            parsed.path()
        )));
    }

    let query = parsed.query_pairs().collect::<Vec<_>>();
    if let Some(error) = query.iter().find(|(key, _)| key == "error") {
        respond_with_html(
            &mut stream,
            400,
            "Beatport authorization failed. You can close this tab.",
        )
        .await?;
        return Err(AppError::InvalidCallback(format!(
            "Beatport returned an OAuth error: {}",
            error.1
        )));
    }

    let code = query
        .iter()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.to_string())
        .ok_or_else(|| AppError::InvalidCallback("missing authorization code".into()))?;
    let state = query
        .iter()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value.to_string())
        .ok_or(AppError::OAuthStateMismatch)?;

    if state != expected_state {
        respond_with_html(
            &mut stream,
            400,
            "Beatport authorization state mismatch. You can close this tab.",
        )
        .await?;
        return Err(AppError::OAuthStateMismatch);
    }

    respond_with_html(
        &mut stream,
        200,
        "Beatport authorization completed. You can close this tab and return to your MCP client.",
    )
    .await?;

    Ok(code)
}

async fn respond_with_html(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn generate_random_urlsafe(bytes: usize) -> String {
    let buf = (0..bytes).map(|_| random::<u8>()).collect::<Vec<_>>();
    URL_SAFE_NO_PAD.encode(buf)
}

fn pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn should_retry_with_docs_frontend_credentials(error: &AppError) -> bool {
    matches!(
        error,
        AppError::RefreshUnavailable
            | AppError::AuthenticationRequired
            | AppError::BeatportApi {
                status: 400 | 401 | 403,
                ..
            }
    )
}

fn debug_auth(args: std::fmt::Arguments<'_>) {
    if std::env::var("BEATPORT_DEBUG_AUTH").ok().as_deref() == Some("1") {
        eprintln!("beatport-auth-debug: {args}");
    }
}

fn extract_cookie_header(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let cookies = headers
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|cookie| cookie.split(';').next())
        .map(str::trim)
        .filter(|cookie| !cookie.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    if cookies.is_empty() {
        None
    } else {
        Some(cookies.join("; "))
    }
}

fn extract_script_sources(html: &str) -> Vec<String> {
    let mut sources = Vec::new();
    let mut remaining = html;

    while let Some(index) = remaining.find("src=") {
        let after = &remaining[index + 4..];
        let Some(quote) = after.chars().next() else {
            break;
        };
        if quote != '"' && quote != '\'' {
            remaining = after;
            continue;
        }
        let after_quote = &after[1..];
        let Some(end) = after_quote.find(quote) else {
            break;
        };
        let candidate = &after_quote[..end];
        if candidate.ends_with(".js") {
            sources.push(candidate.to_string());
        }
        remaining = &after_quote[end + 1..];
    }

    sources
}

fn extract_client_id_from_script(script: &str) -> Option<String> {
    let marker = "API_CLIENT_ID";
    let index = script.find(marker)?;
    let after = &script[index + marker.len()..];
    let separator = after.find([':', '='])?;
    let value = after[separator + 1..].trim_start();
    let quote = value.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let end = value[1..].find(quote)?;
    Some(value[1..1 + end].to_string())
}

fn extract_authorization_code(location: &str) -> Result<String> {
    let url = build_v4_url(location)?;
    let code = url
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.to_string())
        .ok_or_else(|| {
            AppError::AuthenticationFailed(format!(
                "Beatport authorization redirect did not include a code: {location}"
            ))
        })?;
    Ok(code)
}

fn build_v4_url(path_or_url: &str) -> Result<Url> {
    if path_or_url.starts_with("http://") || path_or_url.starts_with("https://") {
        return Ok(Url::parse(path_or_url)?);
    }

    let relative = path_or_url.trim_start_matches('/');
    let relative = if relative.starts_with("v4/") {
        relative.to_string()
    } else {
        format!("v4/{relative}")
    };
    Ok(Url::parse(API_ROOT)?.join(&relative)?)
}

fn extract_html_paragraph(body: &str) -> Option<String> {
    let start = body.find("<p>")?;
    let after = &body[start + 3..];
    let end = after.find("</p>")?;
    Some(after[..end].trim().to_string())
}

fn extract_error_message_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => {
            for key in ["detail", "error", "message"] {
                if let Some(message) = map.get(key).and_then(Value::as_str) {
                    return Some(message.to_string());
                }
            }
            Some(value.to_string())
        }
        _ => Some(value.to_string()),
    }
}

fn load_tokens(path: &Path) -> Result<Option<TokenSet>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let token = serde_json::from_slice::<TokenSet>(&bytes)?;
    Ok(Some(token))
}

fn persist_tokens(path: &Path, token: &TokenSet) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec_pretty(token)?;
    fs::write(path, payload)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let permissions = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, permissions)?;
    }

    Ok(())
}

fn delete_tokens(path: &Path) -> Result<bool> {
    if path.exists() {
        fs::remove_file(path)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use tempfile::TempDir;
    use url::Url;

    use super::{
        TokenSet, build_authorization_url, build_docs_authorization_url,
        extract_client_id_from_script, load_tokens, persist_tokens, pkce_challenge,
    };
    use crate::config::{AuthMode, Config};
    use std::time::SystemTime;

    fn test_config(dir: &TempDir) -> Config {
        Config::from_lookup(|key| match key {
            "BEATPORT_CLIENT_ID" => Some("client".into()),
            "BEATPORT_CLIENT_SECRET" => Some("secret".into()),
            "BEATPORT_TOKEN_PATH" => Some(dir.path().join("tokens.json").display().to_string()),
            _ => None,
        })
        .expect("config should be valid")
    }

    #[test]
    fn pkce_challenge_is_urlsafe() {
        let challenge = pkce_challenge("verifier-value");
        assert!(!challenge.contains('='));
        assert!(!challenge.contains('+'));
        assert!(!challenge.contains('/'));
    }

    #[test]
    fn authorization_url_contains_state_and_scope() {
        let dir = TempDir::new().expect("temp dir should be created");
        let config = test_config(&dir);
        let url =
            build_authorization_url(&config, "state-123", "challenge-123").expect("valid url");
        let query = url.query_pairs().collect::<Vec<_>>();

        assert!(query.iter().any(|(k, v)| k == "state" && v == "state-123"));
        assert!(
            query
                .iter()
                .any(|(k, v)| k == "scope" && v == "app:external user:dj")
        );
        assert!(
            query
                .iter()
                .any(|(k, v)| k == "code_challenge" && v == "challenge-123")
        );
    }

    #[test]
    fn docs_authorization_url_contains_redirect_uri() {
        let redirect_uri =
            Url::parse("https://api.beatport.com/v4/auth/o/post-message/").expect("valid url");
        let url = build_docs_authorization_url("frontend-client", &redirect_uri)
            .expect("valid docs authorization url");
        let query = url.query_pairs().collect::<Vec<_>>();

        assert!(
            query
                .iter()
                .any(|(k, v)| k == "client_id" && v == "frontend-client")
        );
        assert!(
            query
                .iter()
                .any(|(k, v)| k == "response_type" && v == "code")
        );
        assert!(
            query.iter().any(|(k, v)| k == "redirect_uri"
                && v == "https://api.beatport.com/v4/auth/o/post-message/")
        );
    }

    #[test]
    fn extracts_client_id_from_docs_script() {
        let script = "var config = { API_CLIENT_ID: 'public-client-id' };";
        let client_id = extract_client_id_from_script(script).expect("client id should be found");
        assert_eq!(client_id, "public-client-id");
    }

    #[test]
    fn persists_and_loads_tokens() {
        let dir = TempDir::new().expect("temp dir should be created");
        let config = test_config(&dir);
        let token = TokenSet {
            access_token: "access".into(),
            refresh_token: Some("refresh".into()),
            token_type: "Bearer".into(),
            scope: Some("app:external user:dj".into()),
            expires_at: DateTime::<Utc>::from(SystemTime::UNIX_EPOCH),
        };

        persist_tokens(&config.token_path, &token).expect("tokens should persist");
        let loaded = load_tokens(&config.token_path)
            .expect("tokens should load")
            .expect("token should be present");

        assert_eq!(loaded, token);
    }

    #[test]
    fn auth_test_config_uses_oauth_mode() {
        let dir = TempDir::new().expect("temp dir should be created");
        let config = test_config(&dir);
        assert_eq!(config.auth_mode, AuthMode::OAuthApp);
    }
}
