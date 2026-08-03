package firehose

import (
	"context"
	"encoding/json"

	"github.com/vgauthier/bsky-firehose/internal/nats"
)

func FetchLastMessageSequenceInJetStream(ctx context.Context, natsUrl string, streamName string) (int64, error) {
	retriever, err := nats.NewRetriveLastMessage(natsUrl)
	bskyMessage := BskyMessage{}
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
