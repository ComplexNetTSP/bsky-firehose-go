package message

import (
	"encoding/json"
	"fmt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	typegen "github.com/whyrusleeping/cbor-gen"
)

type Like struct {
	Seq        int64           `json:"seq"`
	Time       string          `json:"Time"`
	Repo       string          `json:"repo"`
	Rkey       string          `json:"rkey"`
	SubjectUri string          `json:"subject_uri"`
	SubjectCid string          `json:"subject_cid"`
	CreatedAt  string          `json:"created_at"`
	Via        json.RawMessage `json:"via"`
	Commit     string          `json:"commit"`
	Type       string          `json:"type"`
}

func (l *Like) MessageType() string {
	return l.Type
}

func (l *Like) GetSeq() int64 {
	return l.Seq
}

func (l *Like) Json() ([]byte, error) {
	return json.Marshal(l)
}

func NewLike(evt *comatproto.SyncSubscribeRepos_Commit, op Op, record typegen.CBORMarshaler) (*Like, error) {
	repostRecord, ok := record.(*appbsky.FeedLike)
	if !ok {
		return nil, fmt.Errorf("record is not a FeedLike, got %T", record)
	}

	viaJSON, err := json.Marshal(repostRecord.Via)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal via: %w", err)
	}

	like := &Like{
		Seq:        evt.Seq,
		Time:       evt.Time,
		Repo:       evt.Repo,
		Rkey:       op.Path,
		SubjectUri: repostRecord.Subject.Uri,
		SubjectCid: repostRecord.Subject.Cid,
		CreatedAt:  repostRecord.CreatedAt,
		Via:        viaJSON,
		Commit:     evt.Commit.String(),
		Type:       "app.bsky.feed.like",
	}
	return like, nil
}
