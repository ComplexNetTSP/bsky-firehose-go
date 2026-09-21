package firehose

import (
	"context"
	"fmt"
	"net/http"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/vgauthier/bsky-firehose/internal/logging"
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
}

// NewFirehoseConnection creates a new connection manager.
func NewFirehoseConnection(ctx context.Context, bskyUrl string, handler *BskyMessageHandler, natsUrl string, streamName string, cursor int64) *FirehoseConnection {
	return &FirehoseConnection{
		bskyUrl:    bskyUrl,
		handler:    handler,
		natsUrl:    natsUrl,
		streamName: streamName,
		cursor:     cursor,
	}
}

// Connect establishes the WebSocket connection.
func (fc *FirehoseConnection) connect(ctx context.Context, uri string) error {
	logger := logging.LoggerFromContext(ctx)
	logger.Info("dialing", "url", uri, "func", "connection.connect")
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(timeoutCtx, uri, http.Header{})
	if err != nil {
		metrics.WebSocketConnectionStatus.Set(0)
		return fmt.Errorf("connection.connect: websocket dial failed: %w", err)
	}
	fc.conn = conn
	metrics.WebSocketConnectionStatus.Set(1)
	return nil
}

// Close shuts down the connection.
func (fc *FirehoseConnection) close(ctx context.Context) error {
	logger := logging.LoggerFromContext(ctx)
	logger.Warn("closing websocket connection and scheduler", "func", "connection.close")
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
	logger := logging.LoggerFromContext(ctx)
	lastSeq, err := FetchLastMessageSequenceInJetStream(ctx, fc.natsUrl, fc.streamName)
	if err != nil {
		logger.Error("failed to fetch last message sequence", "error", err, "func", "connection.buildUrl")
	} else {
		if lastSeq > fc.cursor {
			logger.Info("updating cursor to last sequence", "lastSeq", lastSeq, "func", "connection.buildUrl")
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
	logger := logging.LoggerFromContext(ctx)
	if fc.isRunning {
		return nil
	}

	fc.isRunning = true
	logger.Info("connected - streaming events (ctrl-C to stop)")
	return events.HandleRepoStream(ctx, fc.conn, fc.scheduler, nil)
}

// NEW: Run method using existing Connect/SetupScheduler/Start
func (fc *FirehoseConnection) Run(ctx context.Context) error {
	backoff := time.Second
	maxBackoff := 15 * time.Minute
	lastConnection := time.Now()
	backoffResetDuration := 3 * maxBackoff
	logger := logging.LoggerFromContext(ctx)
	// Loop to handle reconnections
	for {
		uri := fc.buildUrl(ctx) // Update URL with current cursor

		// Connect
		timeoutCtx, timeoutCtxCancel := context.WithTimeout(ctx, 1*time.Second)
		err := fc.connect(timeoutCtx, uri)
		timeoutCtxCancel()
		if err != nil {
			logger.Error("websocket connect failed", "error", err, "backoff", backoff, "func", "connection.Run")
			metrics.WebSocketReconnects.Inc()
			timeoutCtxCancel()
			goto retry
		}

		// Setup Scheduler
		fc.setupScheduler(ctx)
		// Start listening
		logger.Info("start listening streaming events", "func", "connection.Run")
		if err := fc.start(ctx); err != nil {
			fc.isRunning = false
			logger.Error("stream ended", "error", err, "func", "connection.Run")
			fc.close(ctx)
			metrics.WebSocketReconnects.Inc()
			goto retry
		}

	retry:
		select {
		// Wait for context cancellation or backoff before retrying
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			if time.Since(lastConnection) > backoffResetDuration {
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
			logger.Info("retrying websocket connection after backoff period", "backoff", backoff, "func", "connection.Run")
		}
	}
}
