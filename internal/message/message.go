package message

import (
	"fmt"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	typegen "github.com/whyrusleeping/cbor-gen"
)

type Message interface {
	MessageType() string
	GetSeq() int64
}

type Op struct {
	Action string `json:"action"` // "create", "update", or "delete"
	Cid    string `json:"cid"`
	Path   string `json:"path"`
}

// buildMessage constructs a BskyMessage from processed data
func Parse(evt *comatproto.SyncSubscribeRepos_Commit, op Op, record typegen.CBORMarshaler) (Message, error) {
	recordType, err := getRecordType(op)
	if err != nil {
		return nil, err
	}

	switch recordType {
	case "app.bsky.feed.like":
		return NewLike(evt, op, record)
	case "app.bsky.feed.repost":
		return NewRepost(evt, op, record)
	case "app.bsky.feed.post":
		return NewPost(evt, op, record)
	case "app.bsky.graph.follow":
		return NewFollow(evt, op, record)
	default:
		return nil, fmt.Errorf("unkown record type: %s", recordType)
	}
}

func getRecordType(op Op) (string, error) {
	var path string
	parts := strings.Split(op.Path, "/")
	if len(parts) == 0 {
		return path, fmt.Errorf("invalid path: %s", op.Path)
	}
	return parts[0], nil
}
