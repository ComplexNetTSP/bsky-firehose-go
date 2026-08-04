package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	firehose "github.com/vgauthier/bsky-firehose/internal/firehose"
	"github.com/vgauthier/bsky-firehose/internal/logger"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
	"github.com/vgauthier/bsky-firehose/internal/nats"
)

func main() {
	// fetch the logger
	logger := logger.GetLogger()
	// fetch flags
	cf := NewCmdFlags()

	// initialize metrics
	metrics.Init()
	metrics.StartServer(cf.MetricsPort)
	logger.Info("Prometheus metrics server started", "port", cf.MetricsPort)

	// create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup nats stream
	natsStream := nats.NewStream(cf.NatsUrl, cf.StreamName, cf.MaxStreamMsg)
	err := natsStream.Connect(ctx)
	if err != nil {
		logger.Error("failed to connect to nats server", "error", err)
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
		logger.Info("shutting down...")
		cancel()
	}()

	fc := firehose.NewFirehoseConnection(ctx, cf.Relay, handler, cf.NatsUrl, cf.StreamName, cf.Cursor)
	if err := fc.Run(ctx); err != nil {
		logger.Error("firehose connection failed", "error", err)
		log.Fatal(err)
	}
}
