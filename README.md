# Beatport MCP

Local Rust MCP server for Beatport v4, built on `rmcp` over `stdio`.

## Features

- OAuth2 authorization-code login with a loopback callback listener
- Token refresh, token revocation, and on-disk token persistence
- Curated tools for Beatport search, catalog reads, account reads, playlist writes, and genre subscriptions
- `beatport_describe_endpoint` to inspect the authenticated Beatport OpenAPI schema
- `beatport_request` as a guarded raw escape hatch for the rest of the API surface

## Required Environment

Set these before launching the server:

```bash
export BEATPORT_CLIENT_ID="your-client-id"
export BEATPORT_CLIENT_SECRET="your-client-secret"
```

Optional overrides:

```bash
export BEATPORT_REDIRECT_URI="http://127.0.0.1:8765/callback"
export BEATPORT_SCOPE="app:external user:dj"
export BEATPORT_TOKEN_PATH="$HOME/.config/beatport-mcp/tokens.json"
```

## Running

```bash
cargo run
```

The server speaks MCP over `stdio`, so it is intended to be launched by an MCP client.

## Auth Notes

- `beatport_connect` starts a temporary loopback listener on the configured redirect URI.
- The tool attempts to open the authorization URL in the default browser and waits for the callback.
- Tokens are persisted to the configured token path and refreshed automatically when possible.

## Testing

```bash
cargo test
```

Live Beatport tests are opt-in:

```bash
export BEATPORT_RUN_LIVE_TESTS=1
cargo test --test live_integration -- --ignored
```
