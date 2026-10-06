package broker

import (
	"path/filepath"
	"testing"

	"github.com/MinuuLakshmi17/mini-kafka/internal/protocol"
)

func TestTopicRecoveryAcrossRestart(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")

	// First "incarnation": create a topic and produce.
	b1 := New("127.0.0.1:0", data)
	if err := b1.CreateTopic("events", 2); err != nil {
		t.Fatal(err)
	}
	parts, err := b1.ensureTopic("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parts[1].Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	for _, p := range parts {
		p.Close()
	}

	// Second incarnation on the same data dir: topics must come back.
	b2 := New("127.0.0.1:0", data)
	if err := b2.recoverTopics(); err != nil {
		t.Fatal(err)
	}
	parts2, err := b2.ensureTopic("events")
	if err != nil {
		t.Fatalf("topic lost across restart: %v", err)
	}
	if len(parts2) != 2 {
		t.Fatalf("partitions=%d, want 2", len(parts2))
	}
	if got := parts2[1].NextOffset(); got != 1 {
		t.Fatalf("nextOffset=%d, want 1", got)
	}
	rs, err := parts2[1].ReadFrom(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || string(rs[0].Value) != "hello" {
		t.Fatalf("record lost across restart: %+v", rs)
	}
	for _, p := range parts2 {
		p.Close()
	}
}

func TestListPartitionsReturnsIndices(t *testing.T) {
	b := New("127.0.0.1:0", filepath.Join(t.TempDir(), "data"))
	if err := b.CreateTopic("t", 3); err != nil {
		t.Fatal(err)
	}
	resp := b.dispatch(protocol.Request{Op: "create_topic", Topic: "t", Partitions: 3})
	_ = resp
	lp := b.dispatch(protocol.Request{Op: "list_partitions", Topic: "t"})
	if len(lp.Partitions) != 3 || lp.Partitions[0] != 0 || lp.Partitions[1] != 1 || lp.Partitions[2] != 2 {
		t.Fatalf("list_partitions=%v, want [0 1 2]", lp.Partitions)
	}
}
