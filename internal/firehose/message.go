package firehose

import "encoding/json"

type Op struct {
	Action string `json:"action"` // "create", "update", or "delete"
	Cid    string `json:"cid"`
	Path   string `json:"path"`
}

type BskyMessage struct {
	Repo     string          `json:"repo"` // The DID of the repo (e.g., "did:plc:abc123")
	Rev      string          `json:"rev"`
	Seq      int64           `json:"seq"`
	Time     string          `json:"time"`
	PrevData string          `json:"prevData"`
	Commit   string          `json:"commit"`
	Since    string          `json:"since"`
	Op       Op              `json:"op"`
	Record   json.RawMessage `json:"record"` // The record as JSON string
	TooBig   bool            `json:"tooBig"` // DEPRECATED
	Rebase   bool            `json:"rebase"` // DEPRECATED
}
