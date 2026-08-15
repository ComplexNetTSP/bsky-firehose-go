package message

import (
	"encoding/json"
	"fmt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	typegen "github.com/whyrusleeping/cbor-gen"
)

type Post struct {
	Commit    string          `json:"commit"`
	Seq       int64           `json:"seq"`
	Time      string          `json:"time"`
	CreatedAt string          `json:"created_at"`
	Repo      string          `json:"repo"`
	Rkey      string          `json:"rkey"`
	Text      string          `json:"text"`
	Langs     []string        `json:"langs"`
	Embed     json.RawMessage `json:"embed"`
	Labels    []string        `json:"labels"`
	Facets    json.RawMessage `json:"facets"`
	Type      string          `json:"type"`
}

func (p *Post) MessageType() string {
	return p.Type
}

func (p *Post) GetSeq() int64 {
	return p.Seq
}

func (p *Post) Json() ([]byte, error) {
	return json.Marshal(p)
}

func NewPost(evt *comatproto.SyncSubscribeRepos_Commit, op Op, record typegen.CBORMarshaler) (*Post, error) {
	postRecord, ok := record.(*appbsky.FeedPost)
	if !ok {
		return nil, fmt.Errorf("record is not a FeedPost, got %T", record)
	}

	embedJSON, err := json.Marshal(postRecord.Embed)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embed: %w", err)
	}

	facetsJSON, err := json.Marshal(postRecord.Facets)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embed: %w", err)
	}

	message := &Post{
		Commit:    evt.Commit.String(),
		Seq:       evt.Seq,
		Time:      evt.Time,
		Type:      "app.bsky.feed.post",
		Repo:      evt.Repo,
		Rkey:      op.Path,
		CreatedAt: postRecord.CreatedAt,
		Text:      postRecord.Text,
		Langs:     postRecord.Langs,
		Embed:     embedJSON,
		Facets:    facetsJSON,
	}
	return message, nil
}
