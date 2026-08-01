package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type RetriveLastMessage struct {
	nc        *nats.Conn
	jetstream jetstream.JetStream
}

func NewRetriveLastMessage(natsUrl string) (*RetriveLastMessage, error) {
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to nats server at url: %s", natsUrl)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("unable to create jetstream: %v", err)
	}

	return &RetriveLastMessage{
		nc:        nc,
		jetstream: js,
	}, nil
}

func (s *RetriveLastMessage) getLastSequence(ctx context.Context, streamName string) (uint64, error) {
	if s.nc.Status() != nats.CONNECTED {
		return 0, fmt.Errorf("nats connection is not connected, status: %v", s.nc.Status())
	}

	stream, err := s.jetstream.Stream(ctx, streamName)
	if err != nil {
		return 0, fmt.Errorf("unable to get stream: %s", err)
	}

	streamInfo, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to get stream info: %s", err)
	}
	if streamInfo.State.LastSeq == 0 {
		return 0, fmt.Errorf("stream has no messages")
	}
	return streamInfo.State.LastSeq, nil
}

func (s *RetriveLastMessage) getMessageBySequenceWithoutAck(ctx context.Context, streamName string, sequence uint64) ([]byte, error) {
	if s.nc.Status() != nats.CONNECTED {
		return nil, fmt.Errorf("nats connection is not connected, status: %v", s.nc.Status())
	}

	stream, err := s.jetstream.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("unable to get stream: %s", err)
	}
	// Get the message by sequence
	msg, err := stream.GetMsg(ctx, sequence)
	if err != nil {
		return nil, fmt.Errorf("unable to get message: %s", err)
	}
	return msg.Data, nil
}

func (s *RetriveLastMessage) GetLastMessage(ctx context.Context, streamName string) ([]byte, error) {
	lastSeq, err := s.getLastSequence(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("unable to get last sequence: %s", err)
	}

	msgData, err := s.getMessageBySequenceWithoutAck(ctx, streamName, lastSeq)
	if err != nil {
		return nil, fmt.Errorf("unable to get message by sequence: %s", err)
	}

	return msgData, nil
}

func (s *RetriveLastMessage) Close() {
	if s.nc != nil {
		s.nc.Close()
	}
}
