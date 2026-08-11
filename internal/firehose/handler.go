package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/vgauthier/bsky-firehose/internal/message"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
	"github.com/vgauthier/bsky-firehose/internal/nats"
)

type BskyMessageHandler struct {
	stream                     *nats.Stream
	startProcessingMessageTime time.Time
}

func NewBskyMessageHandler(stream *nats.Stream) *BskyMessageHandler {
	return &BskyMessageHandler{
		stream: stream,
	}
}

// opFromCommitOp converts a SyncSubscribeRepos_Commit op to our Op type
func (bmh *BskyMessageHandler) opFromCommitOp(op *comatproto.SyncSubscribeRepos_RepoOp) message.Op {
	var cid string
	if op.Cid != nil && op.Cid.Defined() {
		cid = op.Cid.String()
	}
	return message.Op{
		Action: op.Action,
		Path:   op.Path,
		Cid:    cid,
	}
}

// readRepoFromCar reads and returns a repo from CAR blocks
func (bmh *BskyMessageHandler) readRepoFromCar(ctx context.Context, blocks []byte) (*repo.Repo, error) {
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(blocks))
	if err != nil {
		slog.Error("failed to read CAR blocks", "error", err)
		return nil, err
	}
	return r, nil
}

func (bmh *BskyMessageHandler) HandleCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	// Increment received messages counter
	metrics.MessagesReceived.WithLabelValues("commit").Inc()
	// record the start date when received message message
	bmh.startProcessingMessageTime = time.Now()

	// Update current sequence gauge
	metrics.CurrentSequence.Set(float64(evt.Seq))
	r, err := bmh.readRepoFromCar(ctx, evt.Blocks)
	if err != nil {
		metrics.ProcessingErrors.WithLabelValues("car_read").Inc()
		return err
	}
	//since, prevData := bmh.extractOptionalFields(evt)
	for _, op := range evt.Ops {
		if op.Action == "create" {
			startTime := time.Now()
			_, record, err := r.GetRecord(ctx, op.Path)
			if err != nil {
				metrics.ProcessingErrors.WithLabelValues("record_get").Inc()
				slog.Debug("failed to get record JSON", "path", op.Path, "error", err)
				continue
			}
			op_parsed := bmh.opFromCommitOp(op)

			message, err := message.Parse(evt, op_parsed, record)
			if err != nil {
				metrics.ProcessingErrors.WithLabelValues("record_type_extract").Inc()
				slog.Debug("failed to get record type", "error", err)
				continue
			}

			if err := bmh.sendNatsMessage(ctx, message); err != nil {
				metrics.ProcessingErrors.WithLabelValues("nats_publish").Inc()
				slog.Error("failed to send NATS message", "error", err)
			} else {
				metrics.MessagesProcessed.WithLabelValues(message.MessageType()).Inc()
			}

			// Record processing time
			metrics.MessageProcessingTime.WithLabelValues(message.MessageType()).Observe(time.Since(startTime).Seconds())
		}
	}
	return nil
}

func (bmh *BskyMessageHandler) sendNatsMessage(ctx context.Context, msg message.Message) error {
	subjectMap := map[string]string{
		"app.bsky.feed.like":   fmt.Sprintf("%s.likes", bmh.stream.StreamName),
		"app.bsky.feed.repost": fmt.Sprintf("%s.reposts", bmh.stream.StreamName),
		"app.bsky.feed.post":   fmt.Sprintf("%s.posts", bmh.stream.StreamName),
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		slog.Error("unable to marshal bluesky message", "error", err)
		metrics.ProcessingErrors.WithLabelValues("marshal").Inc()
		return err
	}

	slog.Debug("Message", "msg", jsonData)

	if subject, ok := subjectMap[msg.MessageType()]; ok {
		if err = bmh.stream.Publish(ctx, subject, []byte(jsonData)); err != nil {
			metrics.PublishErrors.WithLabelValues(subject).Inc()
			return err
		}
		metrics.NATSPublishTime.Observe(float64(time.Since(bmh.startProcessingMessageTime).Microseconds()))
		metrics.MessagesPublished.WithLabelValues(subject).Inc()
	}
	return nil
}
