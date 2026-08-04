package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/vgauthier/bsky-firehose/internal/logger"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
)

// FirehoseConnection manages the WebSocket connection and stream.
type FirehoseConnection struct {
	bskyUrl    string
	conn       *websocket.Conn
	scheduler  *sequential.Scheduler
	cancel     context.CancelFunc
	isRunning  bool
	handler    *BskyMessageHandler
	cursor     int64
	natsUrl    string
	streamName string
	logger     *slog.Logger
}

// NewFirehoseConnection creates a new connection manager.
func NewFirehoseConnection(ctx context.Context, bskyUrl string, handler *BskyMessageHandler, natsUrl string, streamName string, cursor int64) *FirehoseConnection {
	return &FirehoseConnection{
		bskyUrl:    bskyUrl,
		handler:    handler,
		natsUrl:    natsUrl,
		streamName: streamName,
		cursor:     cursor,
		logger:     logger.GetLogger(),
	}
}

// Connect establishes the WebSocket connection.
func (fc *FirehoseConnection) connect(ctx context.Context, uri string) error {

	fc.logger.Info("dialing", "url", uri)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, uri, http.Header{})
	if err != nil {
		metrics.WebSocketConnectionStatus.Set(0)
		return fmt.Errorf("websocket dial failed: %w", err)
	}
	fc.conn = conn
	metrics.WebSocketConnectionStatus.Set(1)
	return nil
}

// Close shuts down the connection.
func (fc *FirehoseConnection) close() error {
	fc.logger.Warn("closing websocket connection and scheduler")
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
func (fc *FirehoseConnection) setupScheduler(ctx context.Context) {
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

func (fc *FirehoseConnection) buildUrl(ctx context.Context) string {
	lastSeq, err := FetchLastMessageSequenceInJetStream(ctx, fc.natsUrl, fc.streamName)
	if err != nil {
		fc.logger.Error("failed to fetch last message sequence", "error", err)
	} else {
		if lastSeq > fc.cursor {
			fc.logger.Info("updating cursor to last sequence", "lastSeq", lastSeq)
			fc.cursor = lastSeq
		}
	}
	if fc.cursor > 0 {
		return fmt.Sprintf("wss://%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", fc.bskyUrl, fc.cursor)
	}
	return fmt.Sprintf("wss://%s/xrpc/com.atproto.sync.subscribeRepos", fc.bskyUrl)
}

// Start begins streaming events.
func (fc *FirehoseConnection) start(ctx context.Context) error {
	if fc.isRunning {
		return nil
	}

	fc.isRunning = true
	fc.logger.Info("connected - streaming events (ctrl-C to stop)")
	return events.HandleRepoStream(ctx, fc.conn, fc.scheduler, nil)
}

// NEW: Run method using existing Connect/SetupScheduler/Start
func (fc *FirehoseConnection) Run(ctx context.Context) error {
	backoff := time.Second
	maxBackoff := 15 * time.Minute
	lastConnection := time.Now()
	backoffResetDurection := 3 * maxBackoff
	// Loop to handle reconnections
	for {
		uri := fc.buildUrl(ctx) // Update URL with current cursor

		// Connect
		if err := fc.connect(ctx, uri); err != nil {
			fc.logger.Error("websocket connect failed", "error", err, "backoff", backoff)
			metrics.WebSocketReconnects.Inc()
			goto retry
		}
		// Setup Scheduler
		fc.setupScheduler(ctx)
		// Start listening
		fc.logger.Info("streaming events")
		if err := fc.start(ctx); err != nil {
			fc.isRunning = false
			fc.logger.Error("stream ended", "error", err)
			fc.close()
			metrics.WebSocketReconnects.Inc()
			goto retry
		}

	retry:
		select {
		// Wait for context cancellation or backoff before retrying
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if time.Since(lastConnection) > backoffResetDurection {
				// reset backoff if the since last reconnect is large enough
				backoff = time.Second
			} else if backoff < maxBackoff {
				// if time since last reconnect small execute backoff
				backoff *= 2
			} else {
				// avoid having too large backoff
				backoff = maxBackoff
			}
			lastConnection = time.Now()
			fc.logger.Info("retrying websocket connection after backoff period", "backoff", backoff)
		}
	}
}
