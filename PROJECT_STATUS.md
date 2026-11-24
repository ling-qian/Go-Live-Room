# Go Live Room Service V3.0 - Verification Status

## Locally verified (Go 1.23.2)

- `go test ./...` - PASS
- `go test -race ./...` - PASS
- `go vet ./...` - PASS
- `go build ./cmd/server` - PASS
- REST smoke test: list / create / join - PASS
- WebSocket same-origin handshake + welcome + text broadcast over local TCP test server - PASS
- Redis RESP PING over mock TCP server - PASS

## Implemented but not integration-verified

- `database/sql` MySQL repository adapter and `migrations/001_create_rooms.sql` compile. No MySQL driver is bundled/registered, so live MySQL integration and transaction behavior have not been verified against a MySQL server.
- `compose.yaml` provides MySQL 8.4 and Redis 7 services. Docker was unavailable in the authoring environment, so the Compose services and Dockerfile were not launched.
- Redis client can connect to Redis RESP2; unit/mock server tests pass, but no live Redis container test or load test was run.

## Deliberate scope limits

- The default application wiring still uses the in-memory room repository.
- WebSocket Hub only broadcasts within one process. No Redis subscriber bridge is wired.
- The WebSocket endpoint uses `user_id` as a demo query parameter; no JWT/session authentication is implemented.
- Only a bounded RFC 6455 subset is supported: final text/control frames, ping/pong, close; no fragmentation, extensions or binary media frames.
- No RTMP, WebRTC, HLS, transcoding or media forwarding.
- No throughput, latency or capacity benchmark is claimed.

## Recommended resume wording

Describe this as a personal Go backend engineering project. State that MySQL adapter/schema code is implemented but live driver wiring/integration remains pending. Do not present it as production Go work experience or as a complete audio/video streaming server.
