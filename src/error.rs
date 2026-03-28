use std::borrow::Cow;

use reqwest::StatusCode;
use rmcp::ErrorData;
use serde_json::{Value, json};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("missing required configuration: {0}")]
    MissingConfig(&'static str),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("unsupported Beatport resource: {0}")]
    UnsupportedResource(String),
    #[error("unsupported Beatport method: {0}")]
    UnsupportedMethod(String),
    #[error("invalid Beatport relation: {0}")]
    InvalidRelation(String),
    #[error("unsafe Beatport path: {0}")]
    UnsafePath(String),
    #[error("query serialization error: {0}")]
    QuerySerialization(String),
    #[error("beatport authentication required")]
    AuthenticationRequired,
    #[error("Beatport authentication failed: {0}")]
    AuthenticationFailed(String),
    #[error("no Beatport refresh token is available; reconnect required")]
    RefreshUnavailable,
    #[error("Beatport OAuth callback timed out")]
    OAuthTimeout { authorization_url: String },
    #[error("Beatport OAuth callback was invalid: {0}")]
    InvalidCallback(String),
    #[error("Beatport OAuth state mismatch")]
    OAuthStateMismatch,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),
    #[error("time parse error: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("Beatport API returned {status}: {message}")]
    BeatportApi {
        status: u16,
        message: String,
        body: Option<Value>,
    },
}

impl AppError {
    pub fn beatport_api(status: StatusCode, body: Option<Value>) -> Self {
        let message = body
            .as_ref()
            .and_then(extract_error_message)
            .unwrap_or_else(|| status.to_string());
        Self::BeatportApi {
            status: status.as_u16(),
            message,
            body,
        }
    }
}

impl From<AppError> for ErrorData {
    fn from(value: AppError) -> Self {
        match value {
            AppError::MissingConfig(name) => {
                ErrorData::invalid_params(format!("missing required configuration: {name}"), None)
            }
            AppError::InvalidConfig(message)
            | AppError::UnsupportedResource(message)
            | AppError::UnsupportedMethod(message)
            | AppError::InvalidRelation(message)
            | AppError::UnsafePath(message)
            | AppError::QuerySerialization(message)
            | AppError::InvalidCallback(message) => ErrorData::invalid_params(message, None),
            AppError::AuthenticationFailed(message) => ErrorData::invalid_request(message, None),
            AppError::AuthenticationRequired | AppError::RefreshUnavailable => {
                ErrorData::invalid_request(
                    "Beatport authentication is required. Run beatport_connect first.",
                    Some(json!({ "hint": "Run beatport_connect first." })),
                )
            }
            AppError::OAuthTimeout { authorization_url } => ErrorData::url_elicitation_required(
                "Beatport login is waiting for browser authorization.",
                Some(json!({ "url": authorization_url })),
            ),
            AppError::OAuthStateMismatch => {
                ErrorData::invalid_request("Beatport OAuth state mismatch", None)
            }
            AppError::BeatportApi {
                status,
                message,
                body,
            } => match status {
                400 => ErrorData::invalid_params(message, body),
                401 => ErrorData::invalid_request(
                    format!("{message}. Run beatport_connect if the token is no longer valid."),
                    body,
                ),
                403 => ErrorData::invalid_request(message, body),
                404 => ErrorData::resource_not_found(message, body),
                _ => ErrorData::internal_error(message, body),
            },
            AppError::Io(error) => ErrorData::internal_error(error.to_string(), None),
            AppError::Http(error) => ErrorData::internal_error(error.to_string(), None),
            AppError::Json(error) => ErrorData::internal_error(error.to_string(), None),
            AppError::Url(error) => ErrorData::internal_error(error.to_string(), None),
            AppError::Chrono(error) => ErrorData::internal_error(error.to_string(), None),
        }
    }
}

fn extract_error_message(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => {
            if let Some(detail) = map.get("detail").and_then(Value::as_str) {
                return Some(detail.to_string());
            }
            if let Some(error) = map.get("error").and_then(Value::as_str) {
                return Some(error.to_string());
            }
            if let Some(message) = map.get("message").and_then(Value::as_str) {
                return Some(message.to_string());
            }
            Some(Value::Object(map.clone()).to_string())
        }
        _ => Some(value.to_string()),
    }
}

pub fn owned_message(message: impl Into<String>) -> Cow<'static, str> {
    Cow::Owned(message.into())
}
