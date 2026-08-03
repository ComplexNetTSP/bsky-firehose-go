package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/vgauthier/bsky-firehose/internal/metrics"
	"github.com/vgauthier/bsky-firehose/internal/nats"
)

type BskyMessageHandler struct {
	stream *nats.Stream
}

func NewBskyMessageHandler(stream *nats.Stream) *BskyMessageHandler {
	return &BskyMessageHandler{
		stream: stream,
	}
}

// opFromCommitOp converts a SyncSubscribeRepos_Commit op to our Op type
func (bmh *BskyMessageHandler) opFromCommitOp(op *comatproto.SyncSubscribeRepos_RepoOp) Op {
	var cid string
	if op.Cid != nil && op.Cid.Defined() {
		cid = op.Cid.String()
	}
	return Op{
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

// getRecordJSON fetches a record from the repo and marshals it to JSON string
func (bmh *BskyMessageHandler) getRecordJSON(ctx context.Context, r *repo.Repo, path string) (json.RawMessage, error) {
	_, rec, err := r.GetRecord(ctx, path)
	if err != nil {
		slog.Debug("failed get record from cbor", "path", path, "error", err)
		return nil, err
	}
	recJSON, err := json.Marshal(rec)
	if err != nil {
		slog.Debug("failed to marshal record to JSON", "path", path, "error", err)
		return nil, err
	}
	return json.RawMessage(recJSON), nil
}

// extractOptionalFields safely extracts Since and PrevData
func (bmh *BskyMessageHandler) extractOptionalFields(evt *comatproto.SyncSubscribeRepos_Commit) (since, prevData string) {
	if evt.Since != nil {
		since = *evt.Since
	}
	if evt.PrevData != nil && evt.PrevData.Defined() {
		prevData = evt.PrevData.String()
	}
	return since, prevData
}

// buildMessage constructs a BskyMessage from processed data
func (bmh *BskyMessageHandler) buildMessage(evt *comatproto.SyncSubscribeRepos_Commit, op Op, record json.RawMessage, since string, prevData string) BskyMessage {
	return BskyMessage{
		Repo:     evt.Repo,
		Rev:      evt.Rev,
		Seq:      evt.Seq,
		Time:     evt.Time,
		PrevData: prevData,
		Commit:   evt.Commit.String(),
		Op:       op,
		Record:   record,
		TooBig:   evt.TooBig,
		Rebase:   evt.Rebase,
		Since:    since,
	}
}

func (bmh *BskyMessageHandler) HandleCommit(ctx context.Context, evt *comatproto.SyncSubscribeRepos_Commit) error {
	// Increment received messages counter
	metrics.MessagesReceived.WithLabelValues("commit").Inc()

	// Update current sequence gauge
	metrics.CurrentSequence.Set(float64(evt.Seq))
	r, err := bmh.readRepoFromCar(ctx, evt.Blocks)
	if err != nil {
		metrics.ProcessingErrors.WithLabelValues("car_read").Inc()
		return err
	}
	since, prevData := bmh.extractOptionalFields(evt)
	for _, op := range evt.Ops {
		if op.Action == "create" {
			startTime := time.Now()
			record, err := bmh.getRecordJSON(ctx, r, op.Path)
			if err != nil {
				metrics.ProcessingErrors.WithLabelValues("record_get").Inc()
				slog.Debug("failed to get record JSON", "path", op.Path, "error", err)
				continue
			}
			op_parsed := bmh.opFromCommitOp(op)
			msg := bmh.buildMessage(evt, op_parsed, record, since, prevData)

			// Get record type for metrics
			recordType, err := bmh.getRecordType(op_parsed)
			if err != nil {
				metrics.ProcessingErrors.WithLabelValues("record_type_extract").Inc()
				slog.Error("failed to get record type", "error", err)
				continue
			}

			if err := bmh.sendNatsMessage(ctx, msg, recordType); err != nil {
				metrics.ProcessingErrors.WithLabelValues("nats_publish").Inc()
				slog.Error("failed to send NATS message", "error", err)
			} else {
				metrics.MessagesProcessed.WithLabelValues(recordType).Inc()
			}

			// Record processing time
			metrics.MessageProcessingTime.WithLabelValues(recordType).Observe(time.Since(startTime).Seconds())
		}
	}
	return nil
}

func (bmh *BskyMessageHandler) getRecordType(op Op) (string, error) {
	var path string
	parts := strings.Split(op.Path, "/")
	if len(parts) == 0 {
		return path, fmt.Errorf("invalid path: %s", op.Path)
	}
	return parts[0], nil
}

func (bmh *BskyMessageHandler) sendNatsMessage(ctx context.Context, msg BskyMessage, recordType string) error {
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

	if err != nil {
		slog.Error("failed to get record type", "error", err)
		metrics.ProcessingErrors.WithLabelValues("record_type_extract").Inc()
		return err
	}
	if subject, ok := subjectMap[recordType]; ok {
		startTime := time.Now()
		if err = bmh.stream.Publish(ctx, subject, []byte(jsonData)); err != nil {
			metrics.PublishErrors.WithLabelValues(subject).Inc()
			return err
		}
		metrics.NATSPublishTime.Observe(time.Since(startTime).Seconds())
		metrics.MessagesPublished.WithLabelValues(subject).Inc()
	}
	return nil
}
