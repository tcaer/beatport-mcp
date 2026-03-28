# Beatport MCP

Local Rust MCP server for Beatport v4, built on `rmcp` over `stdio`.

## Features

- OAuth2 authorization-code login with a loopback callback listener
- Token refresh, token revocation, and on-disk token persistence
- Curated tools for Beatport search, catalog reads, account reads, playlist writes, and genre subscriptions
- `beatport_describe_endpoint` to inspect the authenticated Beatport OpenAPI schema
- `beatport_request` as a guarded raw escape hatch for the rest of the API surface

## Required Environment

Choose one auth mode before launching the server.

### Option 1: Beatport docs frontend auth

This matches the `beets-beatport4` workaround and only requires your Beatport login:

```bash
export BEATPORT_AUTH_MODE="docs_frontend"
export BEATPORT_USERNAME="your-beatport-email-or-username"
export BEATPORT_PASSWORD="your-beatport-password"
```

Optional override if you want to pin the docs frontend client id instead of scraping it:

```bash
export BEATPORT_CLIENT_ID="public-docs-client-id"
```

### Option 2: Your own OAuth application

Set these if you have official Beatport API credentials:

```bash
export BEATPORT_AUTH_MODE="oauth_app"
export BEATPORT_CLIENT_ID="your-client-id"
export BEATPORT_CLIENT_SECRET="your-client-secret"
```

Optional overrides:

```bash
export BEATPORT_REDIRECT_URI="http://127.0.0.1:8765/callback" # oauth_app default
export BEATPORT_SCOPE="app:external user:dj"
export BEATPORT_TOKEN_PATH="$HOME/.config/beatport-mcp/tokens.json"
```

For `docs_frontend`, the default redirect URI is:

```bash
export BEATPORT_REDIRECT_URI="https://api.beatport.com/v4/auth/o/post-message/"
```

## Running

```bash
cargo run
```

The server speaks MCP over `stdio`, so it is intended to be launched by an MCP client.

## Auth Notes

- In `oauth_app` mode, `beatport_connect` starts a temporary loopback listener on the configured redirect URI, opens the browser, and waits for the callback.
- In `docs_frontend` mode, `beatport_connect` logs in with `BEATPORT_USERNAME` and `BEATPORT_PASSWORD`, scrapes Beatport's public docs frontend client id if needed, and exchanges the auth code automatically without opening a browser.
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
