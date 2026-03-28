use beatport_mcp::{BeatportClient, BeatportMcp, Config};
use rmcp::{ServiceExt, transport::stdio};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let server = BeatportMcp::new(BeatportClient::new(config)?);
    let running = server.serve(stdio()).await?;
    let _ = running.waiting().await?;
    Ok(())
}
