package message

import (
	"encoding/json"
	"strings"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/ipfs/go-cid"
	lexutil "github.com/bluesky-social/indigo/lex/util"
)

// validCID is a pre-generated valid CID v1 for testing
const validCID = "bafyreie7q3iidccmpvszul7kudcvvuavuo7u6gzlbobczuk5nqk3b4akba"

func createCommit() lexutil.LexLink {
	c, _ := cid.Parse(validCID)
	return lexutil.LexLink(c)
}

func TestNewRepost_WithVia(t *testing.T) {
	// Setup test data
	viaRef := &comatproto.RepoStrongRef{
		Uri: "at://did:example:user1/app.bsky.feed.post/12345",
		Cid: "bafy123456789",
	}

	subjectRef := &comatproto.RepoStrongRef{
		Uri: "at://did:example:user2/app.bsky.feed.post/67890",
		Cid: "bafy987654321",
	}

	repostRecord := &appbsky.FeedRepost{
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Subject:   subjectRef,
		Via:       viaRef,
	}

	evt := &comatproto.SyncSubscribeRepos_Commit{
		Seq:    12345,
		Time:   "2024-01-01T00:00:00.000Z",
		Repo:   "did:example:user1",
		Commit: createCommit(),
	}

	op := Op{
		Action: "create",
		Path:   "app.bsky.feed.repost/12345",
		Cid:    "bafy123456789",
	}

	// Execute
	repost, err := NewRepost(evt, op, repostRecord)
	if err != nil {
		t.Fatalf("NewRepost failed: %v", err)
	}

	// Verify all fields
	if repost.Seq != 12345 {
		t.Errorf("Expected Seq=12345, got %d", repost.Seq)
	}
	if repost.Repo != "did:example:user1" {
		t.Errorf("Expected Repo=did:example:user1, got %s", repost.Repo)
	}
	if repost.Rkey != "app.bsky.feed.repost/12345" {
		t.Errorf("Expected Rkey=app.bsky.feed.repost/12345, got %s", repost.Rkey)
	}
	if repost.SubjectUri != "at://did:example:user2/app.bsky.feed.post/67890" {
		t.Errorf("Expected SubjectUri=at://did:example:user2/app.bsky.feed.post/67890, got %s", repost.SubjectUri)
	}
	if repost.SubjectCid != "bafy987654321" {
		t.Errorf("Expected SubjectCid=bafy987654321, got %s", repost.SubjectCid)
	}
	if repost.CreatedAt != "2024-01-01T00:00:00.000Z" {
		t.Errorf("Expected CreatedAt=2024-01-01T00:00:00.000Z, got %s", repost.CreatedAt)
	}
	if repost.Type != "app.bsky.feed.repost" {
		t.Errorf("Expected Type=app.bsky.feed.repost, got %s", repost.Type)
	}
	if repost.Commit == "" {
		t.Error("Expected Commit to be non-empty")
	}

	// Verify Via is valid JSON and contains expected data
	if len(repost.Via) == 0 {
		t.Error("Expected Via to be non-empty")
	}

	var viaMap map[string]string
	if err := json.Unmarshal(repost.Via, &viaMap); err != nil {
		t.Errorf("Via field is not valid JSON: %v", err)
	}
	if viaMap["uri"] != "at://did:example:user1/app.bsky.feed.post/12345" {
		t.Errorf("Via.uri mismatch: expected at://did:example:user1/app.bsky.feed.post/12345, got %s", viaMap["uri"])
	}
	if viaMap["cid"] != "bafy123456789" {
		t.Errorf("Via.cid mismatch: expected bafy123456789, got %s", viaMap["cid"])
	}
}

func TestNewRepost_WithoutVia(t *testing.T) {
	// Setup test data with nil Via
	subjectRef := &comatproto.RepoStrongRef{
		Uri: "at://did:example:user2/app.bsky.feed.post/67890",
		Cid: "bafy987654321",
	}

	repostRecord := &appbsky.FeedRepost{
		CreatedAt: "2024-01-01T00:00:00.000Z",
		Subject:   subjectRef,
		Via:       nil, // Explicitly nil
	}

	evt := &comatproto.SyncSubscribeRepos_Commit{
		Seq:    12345,
		Time:   "2024-01-01T00:00:00.000Z",
		Repo:   "did:example:user1",
		Commit: createCommit(),
	}

	op := Op{
		Action: "create",
		Path:   "app.bsky.feed.repost/12345",
		Cid:    "bafy123456789",
	}

	// Execute
	repost, err := NewRepost(evt, op, repostRecord)
	if err != nil {
		t.Fatalf("NewRepost failed: %v", err)
	}

	// Verify all fields except Via
	if repost.Seq != 12345 {
		t.Errorf("Expected Seq=12345, got %d", repost.Seq)
	}
	if repost.SubjectUri != "at://did:example:user2/app.bsky.feed.post/67890" {
		t.Errorf("Expected SubjectUri=at://did:example:user2/app.bsky.feed.post/67890, got %s", repost.SubjectUri)
	}
	if repost.Commit == "" {
		t.Error("Expected Commit to be non-empty")
	}

	// Verify Via is empty (nil bytes)
	if repost.Via != nil {
		t.Errorf("Expected Via to be nil for nil Via field, got %v", repost.Via)
	}
}

func TestNewRepost_JsonOutput(t *testing.T) {
	tests := []struct {
		name   string
		viaRef *comatproto.RepoStrongRef
		hasVia bool
	}{
		{
			name:   "WithVia",
			viaRef: &comatproto.RepoStrongRef{Uri: "at://test/1", Cid: "bafy1"},
			hasVia: true,
		},
		{
			name:   "WithoutVia",
			viaRef: nil,
			hasVia: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subjectRef := &comatproto.RepoStrongRef{
				Uri: "at://did:example:user/app.bsky.feed.post/1",
				Cid: "bafycid1",
			}

			repostRecord := &appbsky.FeedRepost{
				CreatedAt: "2024-01-01T00:00:00.000Z",
				Subject:   subjectRef,
				Via:       tt.viaRef,
			}

			evt := &comatproto.SyncSubscribeRepos_Commit{
				Seq:    1,
				Time:   "2024-01-01T00:00:00.000Z",
				Repo:   "did:test",
				Commit: createCommit(),
			}

			op := Op{
				Action: "create",
				Path:   "app.bsky.feed.repost/1",
				Cid:    "bafy1",
			}

			repost, err := NewRepost(evt, op, repostRecord)
			if err != nil {
				t.Fatalf("NewRepost failed: %v", err)
			}

			jsonBytes, err := repost.Json()
			if err != nil {
				t.Fatalf("Json() failed: %v", err)
			}

			jsonStr := string(jsonBytes)

			// Verify JSON is valid
			var result map[string]interface{}
			if err := json.Unmarshal(jsonBytes, &result); err != nil {
				t.Fatalf("JSON output is not valid: %v", err)
			}

			// Check via field presence
			if tt.hasVia {
				if result["via"] == nil {
					t.Error("Expected via field to be present in JSON")
				}
				// Verify via contains the URI
				if !strings.Contains(jsonStr, "at://test/1") {
					t.Errorf("Expected JSON to contain via URI, got: %s", jsonStr)
				}
			} else {
				// When Via is nil, json.RawMessage marshals to null
				if result["via"] != nil {
					t.Errorf("Expected via field to be null or absent, got: %v", result["via"])
				}
			}
		})
	}
}

func TestNewRepost_InvalidRecord(t *testing.T) {
	evt := &comatproto.SyncSubscribeRepos_Commit{
		Seq:    1,
		Time:   "2024-01-01T00:00:00.000Z",
		Repo:   "did:test",
		Commit: createCommit(),
	}

	op := Op{
		Action: "create",
		Path:   "app.bsky.feed.post/1",
		Cid:    "bafy1",
	}

	// Pass a FeedPost instead of FeedRepost
	wrongRecord := &appbsky.FeedPost{
		Text:      "test post",
		CreatedAt: "2024-01-01T00:00:00.000Z",
	}

	_, err := NewRepost(evt, op, wrongRecord)
	if err == nil {
		t.Error("Expected error when passing wrong record type, got nil")
	}
	if err != nil && err.Error() != "record is not a FeedRepost, got *bsky.FeedPost" {
		t.Errorf("Expected specific error message, got: %s", err.Error())
	}
}

func TestRepost_MessageType(t *testing.T) {
	repost := &Repost{
		Type: "app.bsky.feed.repost",
	}

	if repost.MessageType() != "app.bsky.feed.repost" {
		t.Errorf("Expected MessageType() to return 'app.bsky.feed.repost', got %s", repost.MessageType())
	}
}

func TestRepost_GetSeq(t *testing.T) {
	repost := &Repost{
		Seq: 12345,
	}

	if repost.GetSeq() != 12345 {
		t.Errorf("Expected GetSeq() to return 12345, got %d", repost.GetSeq())
	}
}
