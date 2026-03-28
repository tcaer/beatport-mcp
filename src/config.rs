use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use url::Url;

use crate::error::{AppError, Result};

const DEFAULT_REDIRECT_URI: &str = "http://127.0.0.1:8765/callback";
const DEFAULT_SCOPE: &str = "app:external user:dj";

#[derive(Debug, Clone)]
pub struct Config {
    pub client_id: String,
    pub client_secret: String,
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
        let client_id = lookup("BEATPORT_CLIENT_ID")
            .filter(|value| !value.trim().is_empty())
            .ok_or(AppError::MissingConfig("BEATPORT_CLIENT_ID"))?;
        let client_secret = lookup("BEATPORT_CLIENT_SECRET")
            .filter(|value| !value.trim().is_empty())
            .ok_or(AppError::MissingConfig("BEATPORT_CLIENT_SECRET"))?;
        let redirect_uri =
            lookup("BEATPORT_REDIRECT_URI").unwrap_or_else(|| DEFAULT_REDIRECT_URI.to_string());
        let redirect_uri = Url::parse(&redirect_uri)?;
        validate_redirect_uri(&redirect_uri)?;
        let scope = lookup("BEATPORT_SCOPE").unwrap_or_else(|| DEFAULT_SCOPE.to_string());
        let token_path = lookup("BEATPORT_TOKEN_PATH")
            .map(PathBuf::from)
            .unwrap_or(default_token_path()?);

        Ok(Self {
            client_id,
            client_secret,
            redirect_uri,
            scope,
            token_path,
        })
    }

    pub fn callback_bind_addr(&self) -> Result<SocketAddr> {
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
}

fn validate_redirect_uri(uri: &Url) -> Result<()> {
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

fn default_token_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir().ok_or_else(|| {
        AppError::InvalidConfig("unable to resolve a default config directory".into())
    })?;
    Ok(config_dir.join("beatport-mcp").join("tokens.json"))
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn builds_config_from_lookup() {
        let config = Config::from_lookup(|key| match key {
            "BEATPORT_CLIENT_ID" => Some("client".into()),
            "BEATPORT_CLIENT_SECRET" => Some("secret".into()),
            _ => None,
        })
        .expect("config should be valid");

        assert_eq!(config.client_id, "client");
        assert_eq!(config.client_secret, "secret");
        assert_eq!(
            config.redirect_uri.as_str(),
            "http://127.0.0.1:8765/callback"
        );
        assert_eq!(config.scope, "app:external user:dj");
        assert!(config.token_path.ends_with("beatport-mcp/tokens.json"));
    }

    #[test]
    fn rejects_non_loopback_redirects() {
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
}
