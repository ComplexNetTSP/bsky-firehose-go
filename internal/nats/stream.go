package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vgauthier/bsky-firehose/internal/logging"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
)

type Stream struct {
	nats_server  string
	jetstream    jetstream.JetStream // JetStream context
	stream       jetstream.Stream
	nc           *nats.Conn // NATS connection
	StreamName   string
	maxStreamMsg int64
}

func NewStream(natsUrl string, streamName string, maxStreamMsg int64) *Stream {
	return &Stream{
		nats_server:  natsUrl,
		StreamName:   streamName,
		maxStreamMsg: maxStreamMsg,
	}
}

func (s *Stream) Connect(ctx context.Context) error {
	var err error
	s.nc, err = nats.Connect(s.nats_server)
	if err != nil {
		metrics.NATSConnectionStatus.Set(0)
		return fmt.Errorf("unable to connect to nats server at url: %s", s.nats_server)
	}

	s.jetstream, err = jetstream.New(s.nc)
	if err != nil {
		metrics.NATSConnectionStatus.Set(0)
		return fmt.Errorf("unable to create nats jetstream: %v", err)
	}
	if err = s.setupStream(ctx); err != nil {
		metrics.NATSConnectionStatus.Set(0)
		return fmt.Errorf("unable to setup stream: %v", err)
	}
	if err = s.setupSubscribers(ctx); err != nil {
		metrics.NATSConnectionStatus.Set(0)
		return fmt.Errorf("unable to setup subscribers: %v", err)
	}
	metrics.NATSConnectionStatus.Set(1)
	return nil
}

func (s *Stream) setupStream(ctx context.Context) error {
	var err error
	jsConfig := jetstream.StreamConfig{
		Name:        s.StreamName,
		Subjects:    []string{fmt.Sprintf("%s.*", s.StreamName)},
		Description: "Jetstream test stream for blusky message",
		MaxMsgs:     s.maxStreamMsg,
	}

	if s.stream, err = s.jetstream.CreateOrUpdateStream(ctx, jsConfig); err != nil {
		return fmt.Errorf("unable to create nats jetstream with error: %v", err)
	}

	return err
}

func (s *Stream) setupSubscribers(ctx context.Context) error {
	configs := []jetstream.ConsumerConfig{
		{
			Durable:       "likes_subscriber",
			AckPolicy:     jetstream.AckExplicitPolicy,
			Description:   "Subscriber for likes events",
			FilterSubject: fmt.Sprintf("%s.likes", s.StreamName),
		},
		{
			Durable:       "reposts_subscriber",
			AckPolicy:     jetstream.AckExplicitPolicy,
			Description:   "Subscriber for reposts events",
			FilterSubject: fmt.Sprintf("%s.reposts", s.StreamName),
		},
		{
			Durable:       "posts_subscriber",
			AckPolicy:     jetstream.AckExplicitPolicy,
			Description:   "Subscriber for posts events",
			FilterSubject: fmt.Sprintf("%s.posts", s.StreamName),
		},
		{
			Durable:       "follows_subscriber",
			AckPolicy:     jetstream.AckExplicitPolicy,
			Description:   "Subscriber for follows events",
			FilterSubject: fmt.Sprintf("%s.follows", s.StreamName),
		},
	}

	for _, jsConfig := range configs {
		if _, err := s.stream.CreateOrUpdateConsumer(ctx, jsConfig); err != nil {
			return fmt.Errorf("unable to create nats jetstream with error: %v", err)
		}
	}
	return nil
}

func (s *Stream) Close(ctx context.Context) {
	logger := logging.LoggerFromContext(ctx)
	logger.Warn("closing nats stream and connection", "func", "stream.Close")
	metrics.NATSConnectionStatus.Set(0)
	s.nc.Close()
}

func (s *Stream) Publish(ctx context.Context, subject string, data []byte) error {
	startTime := time.Now()
	defer func() {
		metrics.NATSPublishTime.Observe(time.Since(startTime).Seconds())
	}()

	if s.nc.Status() != nats.CONNECTED {
		metrics.NATSConnectionStatus.Set(0)
		return fmt.Errorf("nats connection is not connected, status: %v", s.nc.Status())
	}
	if _, err := s.jetstream.Publish(ctx, subject, data); err != nil {
		metrics.PublishErrors.WithLabelValues(subject).Inc()
		return fmt.Errorf("unable to publish message to subject %s: %v", subject, err)
	}
	return nil
}
