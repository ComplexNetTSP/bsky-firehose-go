package message

import (
	"fmt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	typegen "github.com/whyrusleeping/cbor-gen"
)

type Follow struct {
	Commit    string `json:"commit"`
	Seq       int64  `json:"seq"`
	Time      string `json:"time"`
	CreatedAt string `json:"created_at"`
	Repo      string `json:"repo"`
	Subject   string `json:"follow"`
	Type      string `json:"type"`
}

func (p *Follow) MessageType() string {
	return p.Type
}

func (p *Follow) GetSeq() int64 {
	return p.Seq
}

func NewFollow(evt *comatproto.SyncSubscribeRepos_Commit, op Op, record typegen.CBORMarshaler) (*Follow, error) {
	followRecord, ok := record.(*appbsky.GraphFollow)
	if !ok {
		return nil, fmt.Errorf("record is not a GraphFollow, got %T", record)
	}

	message := &Follow{
		Commit:  evt.Commit.String(),
		Seq:     evt.Seq,
		Time:    evt.Time,
		Type:    "app.bsky.graph.follow",
		Repo:    evt.Repo,
		Subject: followRecord.Subject,
	}
	return message, nil
}
