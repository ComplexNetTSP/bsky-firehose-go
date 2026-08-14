package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	firehose "github.com/vgauthier/bsky-firehose/internal/firehose"
	"github.com/vgauthier/bsky-firehose/internal/logging"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
	"github.com/vgauthier/bsky-firehose/internal/nats"
)

func main() {
	// fetch flags
	cf := NewCmdFlags()

	// create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// logger
	logger := logging.NewLogger(cf.LogLevel)
	ctx = logging.ContextWithLogger(ctx, logger)

	// initialize metrics
	metrics.Init()
	metrics.StartServer(cf.MetricsPort)
	logger.Info("Prometheus metrics server started", "port", cf.MetricsPort, "func", "main")

	// setup nats stream
	natsStream := nats.NewStream(cf.NatsUrl, cf.StreamName, cf.MaxStreamMsg)
	err := natsStream.Connect(ctx)
	if err != nil {
		logger.Error("failed to connect to nats server", "error", err, "func", "main")
		log.Fatal(err)
	}
	defer natsStream.Close()

	//setup message handler
	handler := firehose.NewBskyMessageHandler(natsStream)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("main: shutting down...", "func", "main")
		cancel()
	}()

	fc := firehose.NewFirehoseConnection(ctx, cf.Relay, handler, cf.NatsUrl, cf.StreamName, cf.Cursor)
	if err := fc.Run(ctx); err != nil {
		logger.Error("firehose connection failed", "error", err, "func", "main")
		log.Fatal(err)
	}
}
