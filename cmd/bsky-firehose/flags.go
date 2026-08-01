package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

type CmdFlags struct {
	Relay        string
	NatsUrl      string
	Cursor       int64
	StreamName   string
	MaxStreamMsg int64
	MetricsPort  string
}

func NewCmdFlags() *CmdFlags {
	cf := CmdFlags{
		// Set default
		Relay:        "bsky.network",
		Cursor:       0,
		NatsUrl:      "localhost:4222",
		StreamName:   "bskt_test",
		MaxStreamMsg: 100000,
		MetricsPort:  "9090",
	}

	// Environment variable takes precedence over default
	if envRelay := os.Getenv("BSKY_RELAY"); envRelay != "" {
		cf.Relay = envRelay
	}

	if envStream := os.Getenv("BSKY_NATS_STREAM_NAME"); envStream != "" {
		cf.StreamName = envStream
	}

	if envStream := os.Getenv("BSKY_NATS_MAX_STREAM_MSG"); envStream != "" {
		if maxStreamMsg, err := strconv.ParseInt(envStream, 10, 64); err == nil {
			cf.MaxStreamMsg = maxStreamMsg
		}
	}

	if envNats := os.Getenv("BSKY_NATS_SERVER"); envNats != "" {
		cf.NatsUrl = envNats
	}

	if envCursor := os.Getenv("BSKY_CURSOR"); envCursor != "" {
		if cursorVal, err := strconv.ParseInt(envCursor, 10, 64); err == nil {
			cf.Cursor = cursorVal
		}
	}

	if envMetricsPort := os.Getenv("BSKY_METRICS_PORT"); envMetricsPort != "" {
		cf.MetricsPort = envMetricsPort
	}

	// Command-line flag takes precedence over everything
	flag.StringVar(&cf.Relay, "relay", cf.Relay, `The AT Protocol server that provides the firehose feed.
	Default: bsky.network
	Env var: BSKY_RELAY`)
	flag.StringVar(&cf.NatsUrl, "nats", cf.NatsUrl, `The NATS server URL.
	Default: localhost:4222
	Env var: BSKY_NATS`)
	flag.Int64Var(&cf.Cursor, "cursor", cf.Cursor, `The cursor sequence to start from.
	Default: 0 (start from beginning)
	Env var: BSKY_CURSOR`)
	flag.StringVar(&cf.StreamName, "stream", cf.StreamName, `The NATS stream name to publish messages to.
	Default: bskt_test
	Env var: NATS_STREAM_NAME`)
	flag.Int64Var(&cf.MaxStreamMsg, "max-stream-msg", cf.MaxStreamMsg, `The maximum number of messages to keep in the NATS stream.
	Default: 100000
	Env var: NATS_MAX_STREAM_MSG`)
	flag.StringVar(&cf.MetricsPort, "metrics-port", cf.MetricsPort, `The port to expose Prometheus metrics on.
	Default: 9090
	Env var: METRICS_PORT`)

	// Parse the command-line flags
	flag.Parse()

	// Customize flag.Usage to format multi-line descriptions
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}

	return &cf
}
