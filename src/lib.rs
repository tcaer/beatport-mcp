pub mod auth;
pub mod beatport;
pub mod config;
pub mod crate_sync;
pub mod error;
pub mod tools;

pub use auth::{AuthManager, ConnectResult, DisconnectResult, TokenSet};
pub use beatport::{
    ApiResponse, BeatportClient, CompactTrackSummary, DescribeEndpointOutput,
    EndpointDescriptionMatch, EndpointDescriptionMethod, EndpointDescriptionParameter,
    SearchResults,
};
pub use config::{AuthMode, Config};
pub use error::{AppError, Result};
pub use tools::BeatportMcp;
