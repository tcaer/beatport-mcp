use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use url::Url;

use crate::error::{AppError, Result};

const DEFAULT_OAUTH_REDIRECT_URI: &str = "http://127.0.0.1:8765/callback";
const DEFAULT_DOCS_REDIRECT_URI: &str = "https://api.beatport.com/v4/auth/o/post-message/";
const DEFAULT_SCOPE: &str = "app:external user:dj";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    OAuthApp,
    DocsFrontend,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub auth_mode: AuthMode,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub redirect_uri: Url,
    pub scope: String,
    pub token_path: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup<F>(mut lookup: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let client_id = lookup("BEATPORT_CLIENT_ID").filter(|value| !value.trim().is_empty());
        let client_secret =
            lookup("BEATPORT_CLIENT_SECRET").filter(|value| !value.trim().is_empty());
        let username = lookup("BEATPORT_USERNAME").filter(|value| !value.trim().is_empty());
        let password = lookup("BEATPORT_PASSWORD").filter(|value| !value.trim().is_empty());
        let auth_mode = match parse_auth_mode(lookup("BEATPORT_AUTH_MODE").as_deref())? {
            Some(auth_mode) => auth_mode,
            None => infer_auth_mode(&client_id, &client_secret, &username, &password)?,
        };
        validate_credentials(auth_mode, &client_id, &client_secret, &username, &password)?;

        let default_redirect_uri = match auth_mode {
            AuthMode::OAuthApp => DEFAULT_OAUTH_REDIRECT_URI,
            AuthMode::DocsFrontend => DEFAULT_DOCS_REDIRECT_URI,
        };
        let redirect_uri =
            lookup("BEATPORT_REDIRECT_URI").unwrap_or_else(|| default_redirect_uri.to_string());
        let redirect_uri = Url::parse(&redirect_uri)?;
        validate_redirect_uri(auth_mode, &redirect_uri)?;
        let scope = lookup("BEATPORT_SCOPE").unwrap_or_else(|| DEFAULT_SCOPE.to_string());
        let token_path = lookup("BEATPORT_TOKEN_PATH")
            .map(PathBuf::from)
            .unwrap_or(default_token_path()?);

        Ok(Self {
            auth_mode,
            client_id,
            client_secret,
            username,
            password,
            redirect_uri,
            scope,
            token_path,
        })
    }

    pub fn callback_bind_addr(&self) -> Result<SocketAddr> {
        if self.auth_mode != AuthMode::OAuthApp {
            return Err(AppError::InvalidConfig(
                "docs_frontend auth does not use a local callback listener".into(),
            ));
        }
        let port = self
            .redirect_uri
            .port_or_known_default()
            .ok_or_else(|| AppError::InvalidConfig("redirect URI must include a port".into()))?;
        let host = self
            .redirect_uri
            .host_str()
            .ok_or_else(|| AppError::InvalidConfig("redirect URI must include a host".into()))?;
        let ip = match host {
            "127.0.0.1" | "localhost" => IpAddr::V4(Ipv4Addr::LOCALHOST),
            _ => {
                return Err(AppError::InvalidConfig(
                    "redirect URI host must be localhost or 127.0.0.1".into(),
                ));
            }
        };
        Ok(SocketAddr::new(ip, port))
    }

    pub fn callback_path(&self) -> &str {
        let path = self.redirect_uri.path();
        if path.is_empty() { "/" } else { path }
    }

    pub fn token_path_string(&self) -> String {
        self.token_path.display().to_string()
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn client_secret(&self) -> Option<&str> {
        self.client_secret.as_deref()
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn can_password_bootstrap(&self) -> bool {
        self.auth_mode == AuthMode::DocsFrontend
            && self.username.is_some()
            && self.password.is_some()
    }
}

fn parse_auth_mode(raw: Option<&str>) -> Result<Option<AuthMode>> {
    match raw {
        None => Ok(None),
        Some("oauth_app") => Ok(Some(AuthMode::OAuthApp)),
        Some("docs_frontend") => Ok(Some(AuthMode::DocsFrontend)),
        Some(other) => Err(AppError::InvalidConfig(format!(
            "BEATPORT_AUTH_MODE must be one of oauth_app or docs_frontend, got {other}"
        ))),
    }
}

fn infer_auth_mode(
    client_id: &Option<String>,
    client_secret: &Option<String>,
    username: &Option<String>,
    password: &Option<String>,
) -> Result<AuthMode> {
    if username.is_some() || password.is_some() {
        return Ok(AuthMode::DocsFrontend);
    }
    if client_id.is_some() || client_secret.is_some() {
        return Ok(AuthMode::OAuthApp);
    }
    Err(AppError::InvalidConfig(
        "configure either BEATPORT_USERNAME/BEATPORT_PASSWORD for docs_frontend auth or BEATPORT_CLIENT_ID/BEATPORT_CLIENT_SECRET for oauth_app auth".into(),
    ))
}

fn validate_credentials(
    auth_mode: AuthMode,
    client_id: &Option<String>,
    client_secret: &Option<String>,
    username: &Option<String>,
    password: &Option<String>,
) -> Result<()> {
    match auth_mode {
        AuthMode::OAuthApp => {
            if client_id.is_none() {
                return Err(AppError::MissingConfig("BEATPORT_CLIENT_ID"));
            }
            if client_secret.is_none() {
                return Err(AppError::MissingConfig("BEATPORT_CLIENT_SECRET"));
            }
        }
        AuthMode::DocsFrontend => {
            if username.is_none() {
                return Err(AppError::MissingConfig("BEATPORT_USERNAME"));
            }
            if password.is_none() {
                return Err(AppError::MissingConfig("BEATPORT_PASSWORD"));
            }
        }
    }
    Ok(())
}

fn validate_redirect_uri(auth_mode: AuthMode, uri: &Url) -> Result<()> {
    match auth_mode {
        AuthMode::OAuthApp => validate_loopback_redirect_uri(uri),
        AuthMode::DocsFrontend => validate_docs_redirect_uri(uri),
    }
}

fn validate_loopback_redirect_uri(uri: &Url) -> Result<()> {
    if uri.scheme() != "http" {
        return Err(AppError::InvalidConfig(
            "BEATPORT_REDIRECT_URI must use http for the local loopback listener".into(),
        ));
    }
    let host = uri
        .host_str()
        .ok_or_else(|| AppError::InvalidConfig("redirect URI must include a host".into()))?;
    if host != "localhost" && host != "127.0.0.1" {
        return Err(AppError::InvalidConfig(
            "redirect URI host must be localhost or 127.0.0.1".into(),
        ));
    }
    if uri.port().is_none() {
        return Err(AppError::InvalidConfig(
            "redirect URI must include an explicit port".into(),
        ));
    }
    Ok(())
}

fn validate_docs_redirect_uri(uri: &Url) -> Result<()> {
    if uri.scheme() != "http" && uri.scheme() != "https" {
        return Err(AppError::InvalidConfig(
            "BEATPORT_REDIRECT_URI must use http or https".into(),
        ));
    }
    if uri.host_str().is_none() {
        return Err(AppError::InvalidConfig(
            "redirect URI must include a host".into(),
        ));
    }
    Ok(())
}

fn default_token_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir().ok_or_else(|| {
        AppError::InvalidConfig("unable to resolve a default config directory".into())
    })?;
    Ok(config_dir.join("beatport-mcp").join("tokens.json"))
}

#[cfg(test)]
mod tests {
    use super::{AuthMode, Config};

    #[test]
    fn builds_oauth_app_config_from_lookup() {
        let config = Config::from_lookup(|key| match key {
            "BEATPORT_CLIENT_ID" => Some("client".into()),
            "BEATPORT_CLIENT_SECRET" => Some("secret".into()),
            _ => None,
        })
        .expect("config should be valid");

        assert_eq!(config.auth_mode, AuthMode::OAuthApp);
        assert_eq!(config.client_id(), Some("client"));
        assert_eq!(config.client_secret(), Some("secret"));
        assert_eq!(
            config.redirect_uri.as_str(),
            "http://127.0.0.1:8765/callback"
        );
        assert_eq!(config.scope, "app:external user:dj");
        assert!(config.token_path.ends_with("beatport-mcp/tokens.json"));
    }

    #[test]
    fn builds_docs_frontend_config_from_lookup() {
        let config = Config::from_lookup(|key| match key {
            "BEATPORT_USERNAME" => Some("dj@example.com".into()),
            "BEATPORT_PASSWORD" => Some("secret".into()),
            _ => None,
        })
        .expect("config should be valid");

        assert_eq!(config.auth_mode, AuthMode::DocsFrontend);
        assert_eq!(config.username(), Some("dj@example.com"));
        assert_eq!(config.password(), Some("secret"));
        assert_eq!(
            config.redirect_uri.as_str(),
            "https://api.beatport.com/v4/auth/o/post-message/"
        );
    }

    #[test]
    fn rejects_non_loopback_redirects_for_oauth_app() {
        let error = Config::from_lookup(|key| match key {
            "BEATPORT_CLIENT_ID" => Some("client".into()),
            "BEATPORT_CLIENT_SECRET" => Some("secret".into()),
            "BEATPORT_REDIRECT_URI" => Some("http://example.com/callback".into()),
            _ => None,
        })
        .expect_err("expected config validation to fail");

        assert!(
            error
                .to_string()
                .contains("redirect URI host must be localhost or 127.0.0.1")
        );
    }

    #[test]
    fn rejects_partial_docs_frontend_credentials() {
        let error = Config::from_lookup(|key| match key {
            "BEATPORT_USERNAME" => Some("dj@example.com".into()),
            _ => None,
        })
        .expect_err("expected docs_frontend validation to fail");

        assert_eq!(
            error.to_string(),
            "missing required configuration: BEATPORT_PASSWORD"
        );
    }
}
