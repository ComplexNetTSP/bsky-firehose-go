package firehose

import (
	"context"
	"encoding/json"

	"github.com/vgauthier/bsky-firehose/internal/nats"
)

type sequenceWrapper struct {
	Seq int64 `json:"seq"`
}

func FetchLastMessageSequenceInJetStream(ctx context.Context, natsUrl string, streamName string) (int64, error) {
	retriever, err := nats.NewRetriveLastMessage(natsUrl)
	var wrapper sequenceWrapper
	if err != nil {
		return 0, err
	}
	defer retriever.Close()

	message, err := retriever.GetLastMessage(ctx, streamName)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(message, &wrapper); err != nil {
		return 0, err
	}
	return wrapper.Seq, nil
}
