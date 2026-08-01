package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
)

// FirehoseConnection manages the WebSocket connection and stream.
type FirehoseConnection struct {
	bskyUrl   string
	conn      *websocket.Conn
	scheduler *sequential.Scheduler
	cancel    context.CancelFunc
	isRunning bool
	handler   *BskyMessageHandler
}

// NewFirehoseConnection creates a new connection manager.
func NewFirehoseConnection(ctx context.Context, bskyUrl string, handler *BskyMessageHandler) *FirehoseConnection {
	return &FirehoseConnection{
		bskyUrl: bskyUrl,
		handler: handler,
	}
}

// Connect establishes the WebSocket connection.
func (fc *FirehoseConnection) Connect(ctx context.Context) error {

	slog.Info("dialing", "url", fc.bskyUrl)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, fc.bskyUrl, http.Header{})
	if err != nil {
		metrics.WebSocketConnectionStatus.Set(0)
		return fmt.Errorf("websocket dial failed: %w", err)
	}
	fc.conn = conn
	metrics.WebSocketConnectionStatus.Set(1)
	return nil
}

// Close shuts down the connection.
func (fc *FirehoseConnection) Close() error {
	slog.Warn("closing websocket connection and scheduler")
	metrics.WebSocketConnectionStatus.Set(0)
	if fc.cancel != nil {
		fc.cancel()
	}
	if fc.conn != nil {
		fc.conn.Close()
	}
	fc.isRunning = false
	return nil
}

// SetupScheduler configures the event scheduler with callbacks.
func (fc *FirehoseConnection) SetupScheduler(ctx context.Context) {
	ctx, fc.cancel = context.WithCancel(ctx)
	// create callback
	callback := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			return fc.handler.HandleCommit(ctx, evt)
		},
		Error: func(errf *events.ErrorFrame) error {
			return fmt.Errorf("relay sent error frame: %s: %s", errf.Error, errf.Message)
		},
	}
	// setup scheduler
	fc.scheduler = sequential.NewScheduler("bsky-firehose", callback.EventHandler)
}

// Start begins streaming events.
func (fc *FirehoseConnection) Start(ctx context.Context) error {
	if fc.isRunning {
		return nil
	}

	fc.isRunning = true
	slog.Info("connected - streaming events (ctrl-C to stop)")
	return events.HandleRepoStream(ctx, fc.conn, fc.scheduler, nil)
}
