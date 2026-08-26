# Bsky Firehose

[![Go Version](https://img.shields.io/badge/go-1.21+-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Bluesky](https://img.shields.io/badge/bluesky-AT%20Protocol-0085FF?style=flat-square)](https://atproto.com/)
[![NATS](https://img.shields.io/badge/nats-io-4285F4?style=flat-square)](https://nats.io/)

**Author**: Vincent Gauthier <vincent.gauthier@telecom-sudparis.eu>

**A real-time bridge that consumes the Bluesky AT Protocol firehose and publishes events to NATS topics.**

---

## 📋 Features

| Feature | Description |
|---------|-------------|
| **Firehose Consumption** | Subscribes to Bluesky's `SyncSubscribeRepos` WebSocket endpoint |
| **Event Processing** | Parses commit operations, extracts records from CAR blocks, converts to JSON |
| **NATS JetStream** | Publishes to NATS with JetStream for reliable message delivery |
| **Record Filtering** | Routes posts, likes, reposts to dedicated NATS topics |
| **Graceful Shutdown** | Handles SIGINT/SIGTERM for clean disconnection |
| **Error Resilience** | Built-in error handling and structured logging with `slog` |

---

## 🏗️ Architecture

```
Bluesky Firehose (WebSocket)
        ↓
  [FirehoseConnection] → Subscribes to SyncSubscribeRepos
        ↓
  [BskyMessageHandler] → Processes commits, extracts records
        ↓
  [NATS JetStream] → Publishes to bsky_test.* topics
```

- **FirehoseConnection**: Manages WebSocket connection to Bluesky relay using Gorilla WebSocket
- **BskyMessageHandler**: Processes AT Protocol events and routes to NATS with subject-based filtering
- **NATS Stream**: JetStream-enabled publisher with pre-configured streams and consumers

---

## ⚙️ Prerequisites

- **Go** 1.21 or higher
- **NATS Server** 2.0+ (with JetStream enabled)
- **Bluesky PDS** access or public relay URL (default: `bsky.network`)

---

## 🚀 Installation

### 1. Clone the repository
```bash
git clone https://github.com/vgauthier/Bsky/bsky-firehose.git
cd bsky-firehose
```

### 2. Install dependencies
```bash
go mod download
```

### 3. Build
```bash
make build
# Output binary: ./out/bsky-firehose
```

---

## 📂 Configuration

### Command Line Flags

| Flag | Description | Required | Default | Environment Variable |
|------|-------------|----------|---------|--------------------|
| `-relay` | Bluesky WebSocket relay host (without protocol) | Yes | `bsky.network` | `BSKY_RELAY` |
| `-nats` | NATS server URL | Yes | - | `BSKY_NATS` |
| `-debug` | Enable debug logging | No | `false` | `BSKY_DEBUG` |
| `-log-format` | Log output format (text/json) | No | `text` | `BSKY_LOG_FORMAT` |
| `-stream-name` | NATS JetStream stream name | No | `bsky_test` | `BSKY_STREAM_NAME` |
| `-max-messages` | Maximum messages in NATS stream | No | `100000` | `BSKY_MAX_MESSAGES` |
| `-reconnect-interval` | WebSocket reconnect interval (seconds) | No | `30` | `BSKY_RECONNECT_INTERVAL` |

> **Note**: The relay URL is constructed as `wss://{relay}/xrpc/com.atproto.sync.subscribeRepos`

### Example Usage

```bash
# Using command line flags
./out/bsky-firehose -relay bsky.network -nats nats://localhost:4222

# Using environment variables
BSKY_RELAY=bsky.network BSKY_NATS=nats://localhost:4222 ./out/bsky-firehose

# Using Makefile with environment variables
RELAY=bsky.network NATS_URL=nats://localhost:4222 make run
```

---

## 📦 NATS Topics

Processed events are published to the following NATS JetStream subjects:

| Bluesky Record Type | NATS Subject | Consumer |
|---------------------|--------------|----------|
| `app.bsky.feed.post` | `bsky_test.posts` | `posts_subscriber` |
| `app.bsky.feed.like` | `bsky_test.likes` | `likes_subscriber` |
| `app.bsky.feed.repost` | `bsky_test.reposts` | `reposts_subscriber` |

The NATS stream is configured as:
- **Stream Name**: `bskt_test`
- **Subjects**: `bsky_test.*`
- **Max Messages**: 100,000

> **Note**: To add support for additional record types, extend the `subjectMap` in `internal/firehose/handler.go`.

---

## 🏃 Usage

### Run directly
```bash
# Build and run with defaults
make run

# With custom configuration
./out/bsky-firehose -relay bsky.network -nats nats://localhost:4222
```

### Run with Docker
```bash
# Build image
docker build -t bsky-firehose .

# Run container
docker run \
  -e BSKY_RELAY=bsky.network \
  -e BSKY_NATS=nats://host.docker.internal:4222 \
  bsky-firehose
```

---

## 📁 Project Structure

```
bsky-firehose/
├── go.mod                          # Go module definition
├── go.sum                          # Dependency checksums
├── Makefile                       # Build automation
├── Dockerfile                     # Docker configuration
├── cmd/
│   └── bsky-firehose/
│       ├── main.go                # Application entry point with graceful shutdown
│       └── flags.go               # CLI flags and environment variable parsing
├── internal/
│   ├── firehose/
│   │   ├── connection.go           # WebSocket connection management to Bluesky
│   │   ├── handler.go              # Message processing and NATS publishing
│   │   └── message.go              # Data structures (Op, BskyMessage)
│   └── nats/
│       └── stream.go               # NATS JetStream client wrapper
└── pkg/
    └── config/                  # Configuration utilities
        ├── config.go               # Configuration loading and validation
        └── types.go                # Configuration data types
```

---

## 🔧 Development

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make` or `make all` | Build the project (default) |
| `make build` | Build binary to `./out/` |
| `make run` | Build and run the application |
| `make vet` | Run `go vet` for static analysis |
| `make fmt` | Format Go source code |
| `make tidy` | Update `go.mod` dependencies |
| `make clean` | Remove build artifacts and output directory |

### Running Tests
```bash
go test -v ./...
```

### Code Quality
```bash
# Run all checks
go vet ./...
gofmt -w .
golangci-lint run ./...
```

---

## 🌐 Bluesky AT Protocol

This project uses the official [`indigo`](https://github.com/bluesky-social/indigo) library from Bluesky to interact with the AT Protocol.

- **Firehose Endpoint**: `com.atproto.sync.subscribeRepos`
- **Protocol**: WebSocket (wss://)
- **Authentication**: Not required for public relays
- **Message Types**: Handles `Commit` operations with create/update/delete actions
- **CAR Blocks**: Content-addressable archives containing repository data

The firehose provides real-time access to all public repository operations on the Bluesky network.

---

## 📦 Dependencies

| Package | Purpose | Version |
|---------|---------|---------|
| [`github.com/bluesky-social/indigo`](https://github.com/bluesky-social/indigo) | Official AT Protocol client library | v0.0.0-20260730171912 |
| [`github.com/gorilla/websocket`](https://github.com/gorilla/websocket) | WebSocket connection management | v1.5.3 |
| [`github.com/nats-io/nats.go`](https://github.com/nats-io/nats.go) | NATS client with JetStream support | v1.52.0 |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing`)
5. Open a Pull Request

### Code Quality Standards
- All code must pass `make vet`
- All code must be formatted with `make fmt`
- New features require tests
- Follow Go conventions and best practices
- Use `slog` for structured logging

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/vgauthier/Bsky/bsky-firehose/issues)
- **Discussions**: [GitHub Discussions](https://github.com/vgauthier/Bsky/bsky-firehose/discussions)
- **Bluesky**: [@vgauthier.bsky.social](https://bsky.app/profile/vgauthier.bsky.social)
