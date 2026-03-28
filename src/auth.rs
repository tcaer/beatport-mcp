use std::{fs, path::Path, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::random;
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::RwLock,
    time::timeout,
};
use url::Url;

use crate::{
    config::Config,
    error::{AppError, Result},
};

const AUTHORIZATION_URL: &str = "https://api.beatport.com/v4/auth/o/authorize/";
const TOKEN_URL: &str = "https://api.beatport.com/v4/auth/o/token/";
const REVOKE_URL: &str = "https://api.beatport.com/v4/auth/o/revoke/";
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
    tokens: RwLock<Option<TokenSet>>,
}

impl AuthManager {
    pub fn new(config: Config, http: reqwest::Client) -> Result<Self> {
        let tokens = load_tokens(&config.token_path)?;
        Ok(Self {
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
            let refreshed = self.refresh_tokens(&snapshot).await?;
            return Ok(refreshed.access_token);
        }
        Ok(snapshot.access_token)
    }

    pub async fn force_refresh(&self) -> Result<TokenSet> {
        let snapshot = self
            .snapshot()
            .await
            .ok_or(AppError::AuthenticationRequired)?;
        self.refresh_tokens(&snapshot).await
    }

    async fn refresh_tokens(&self, current: &TokenSet) -> Result<TokenSet> {
        let refresh_token = current
            .refresh_token
            .clone()
            .ok_or(AppError::RefreshUnavailable)?;
        let request = self
            .http
            .post(TOKEN_URL)
            .header(AUTHORIZATION, format!("Bearer {}", current.access_token))
            .form(&[
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.as_str()),
                ("refresh_token", refresh_token.as_str()),
                ("grant_type", "refresh_token"),
            ]);
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
        let response = self
            .http
            .post(TOKEN_URL)
            .form(&[
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.as_str()),
                ("code", code),
                ("grant_type", "authorization_code"),
                ("redirect_uri", self.config.redirect_uri.as_str()),
                ("code_verifier", code_verifier),
            ])
            .send()
            .await?;
        parse_token_response(response).await
    }

    async fn revoke_token(&self, access_token: &str) -> Result<()> {
        let response = self
            .http
            .post(REVOKE_URL)
            .query(&[
                ("client_id", self.config.client_id.as_str()),
                ("token", access_token),
            ])
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
    let mut url = Url::parse(AUTHORIZATION_URL)?;
    url.query_pairs_mut()
        .append_pair("client_id", &config.client_id)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", config.redirect_uri.as_str())
        .append_pair("state", state)
        .append_pair("scope", &config.scope)
        .append_pair("code_challenge", code_challenge)
        .append_pair("code_challenge_method", "S256");
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

    use super::{TokenSet, build_authorization_url, load_tokens, persist_tokens, pkce_challenge};
    use crate::config::Config;
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
}
