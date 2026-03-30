# Beatport MCP

Local Rust MCP server for Beatport v4, built on `rmcp` over `stdio`.

## Features

- OAuth2 authorization-code login with a loopback callback listener
- Token refresh, token revocation, and on-disk token persistence
- Curated tools for Beatport search, catalog reads, account reads, playlist writes, and genre subscriptions
- Higher-level crate sync tools for trending candidates, dry-run playlist diffs, review/resolve flows, and plan-based apply flows
- Compact typed search and playlist-track tools for low-noise agent workflows
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
export BEATPORT_SYNC_STATE_PATH="$HOME/.config/beatport-mcp/sync-state.json"
export BEATPORT_SYNC_SEARCH_CONCURRENCY="6"
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

## Automation-Oriented Tools

- `beatport_trending_candidates`: collect recent chart-driven candidate rows for one or more genres
- `beatport_playlist_sync_dry_run`: resolve candidate rows, diff them against a playlist, and persist a reviewable sync plan
- `beatport_playlist_sync_review`: load the remaining ambiguous or not-found review items for a plan
- `beatport_playlist_sync_resolve`: resolve review items into addable tracks or manual skips
- `beatport_playlist_sync_apply`: apply a prior dry-run plan by `plan_id`
- `beatport_search_compact`: search tracks and return compact typed summaries with identity fields
- `beatport_playlist_tracks_compact`: list compact typed playlist-track summaries for a playlist page
- `beatport_server_info`: report server version, auth mode, sync state path, and the curated tool surface

Dry-runs persist lightweight state in `sync-state.json` so repeated weekly runs can avoid resurfacing the same tracks as brand-new forever.

The intended default workflow is:

1. `beatport_trending_candidates` or external candidate rows
2. `beatport_playlist_sync_dry_run`
3. `beatport_playlist_sync_review` and `beatport_playlist_sync_resolve` if anything remains ambiguous
4. `beatport_playlist_sync_apply`

The compact tools expose the same identity fields used by the sync engine for semantic dedupe: Beatport track id, ISRC, label track identifier, normalized artist/title/mix, duration, and BPM.

## Reconnect Note

When you add or rename MCP tools in the server code, your MCP client must restart the server and reconnect before those new tools are callable in the live session. Use `beatport_server_info` after reconnect to confirm the curated tool set that is actually exposed.

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
