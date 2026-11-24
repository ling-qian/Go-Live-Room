# Go Live Room Service

Go 1.23 practice backend for live-room business workflows. This is a portfolio/learning project, not a production media server.

## Features

- REST API for room creation, listing, lookup, join, leave and host-only close.
- In-memory repository protected by `sync.RWMutex`.
- `database/sql` MySQL repository adapter and schema migration (live driver integration not yet verified).
- Dependency-free RESP2 Redis client for PING, GET, SET with TTL, DEL and PUBLISH; optional best-effort presence writes.
- WebSocket text-message handshake and in-process room broadcast (bounded RFC 6455 subset).
- Request validation, structured errors, logging, panic recovery, HTTP timeouts and graceful shutdown.
- Dockerfile and Compose definition for MySQL 8.4 and Redis 7.

## Run

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/server
go run ./cmd/server
```

The default runtime uses the in-memory repository. Configure `REDIS_ADDR` to enable optional Redis presence writes. Before claiming MySQL runtime support, register a MySQL driver and wire `NewMySQLStore(db)` in the application bootstrap.

## Verification boundaries

The authoring environment verified unit tests, the Go race detector, `go vet`, build, WebSocket local TCP handshake/message round-trip, and Redis RESP mock-TCP PING. It did not run Docker, a live MySQL server/driver integration, a live Redis container, load benchmarks, or production deployment.

The WebSocket implementation supports a limited text/control-frame subset. The hub is process-local; no cross-instance Redis subscription bridge or JWT authentication is included. `user_id` is a demo query parameter. This project does not implement RTMP, WebRTC, HLS, transcoding, media forwarding, or a complete audio/video streaming server.

See [PROJECT_STATUS.md](PROJECT_STATUS.md) for verification details and known limitations.