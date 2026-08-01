package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	firehose "github.com/vgauthier/bsky-firehose/internal/firehose"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
	"github.com/vgauthier/bsky-firehose/internal/nats"
)

func BskyFirehoseUrl(relay string, cursor int64) string {
	if cursor == 0 {
		return "wss://" + relay + "/xrpc/com.atproto.sync.subscribeRepos"
	} else {
		return fmt.Sprintf("wss://%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", relay, cursor)
	}
}

func fetchLastMessageSequence(ctx context.Context, natsUrl string, streamName string) (int64, error) {
	retriever, err := nats.NewRetriveLastMessage(natsUrl)
	bskyMessage := firehose.BskyMessage{}
	if err != nil {
		return 0, err
	}
	defer retriever.Close()

	message, err := retriever.GetLastMessage(ctx, streamName)
	if err != nil {
		return 0, err
	}
	json.Unmarshal(message, &bskyMessage)
	return bskyMessage.Seq, nil
}

func main() {
	// fetch flags
	cf := NewCmdFlags()

	// initialize metrics
	metrics.Init()
	metrics.StartServer(cf.MetricsPort)
	slog.Info("Prometheus metrics server started", "port", cf.MetricsPort)

	// create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup cursor
	cursor := cf.Cursor
	// if cursor is 0, try to fetch last message sequence from nats
	if cursor == 0 {
		// try to fetch last message sequence from nats server
		if lastSeq, err := fetchLastMessageSequence(ctx, cf.NatsUrl, cf.StreamName); err == nil {
			cursor = lastSeq
			slog.Info("starting from last message sequence from nats server", "cursor", cursor)
		}
	}

	// setup nats stream
	natsStream := nats.NewStream(cf.NatsUrl, cf.StreamName, cf.MaxStreamMsg)
	err := natsStream.Connect(ctx)
	if err != nil {
		slog.Error("failed to connect to nats server", "error", err)
		log.Fatal(err)
	}
	defer natsStream.Close()

	//setup message handler
	handler := firehose.NewBskyMessageHandler(natsStream)

	// create websocket
	fc := firehose.NewFirehoseConnection(ctx, BskyFirehoseUrl(cf.Relay, cursor), handler)
	if err := fc.Connect(ctx); err != nil {
		slog.Error("websocket connection failed", "error", err)
		log.Fatal(err)
	}
	defer fc.Close()

	// set websocket context
	fc.SetupScheduler(ctx)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("shutting down...")
		cancel()
	}()

	// start listenting
	if err := fc.Start(ctx); err != nil {
		log.Fatalf("stream ended: %v", err)
	}
}
