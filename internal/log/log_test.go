package log

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReadAndRecovery(t *testing.T) {
	d := t.TempDir()
	p, e := OpenPartition(filepath.Join(d, "p"), 1024, 2, true)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 20; i++ {
		if _, e := p.Append([]byte("msg")); e != nil {
			t.Fatal(e)
		}
	}
	if got := p.NextOffset(); got != 20 {
		t.Fatalf("next=%d", got)
	}
	rs, e := p.ReadFrom(7, 5)
	if e != nil {
		t.Fatal(e)
	}
	if len(rs) != 5 || rs[0].Offset != 7 {
		t.Fatalf("bad read %#v", rs)
	}
	p.Close()
	p, e = OpenPartition(filepath.Join(d, "p"), 1024, 2, true)
	if e != nil {
		t.Fatal(e)
	}
	if p.NextOffset() != 20 {
		t.Fatalf("recovery next=%d", p.NextOffset())
	}
	p.Close()
	_ = os.RemoveAll(d)
}

func TestSegmentRollAndIndexSeek(t *testing.T) {
	d := t.TempDir()
	// Tiny segments force multiple rolls; indexEvery=4.
	p, e := OpenPartition(filepath.Join(d, "p"), 128, 4, false)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 30; i++ {
		if _, e := p.Append([]byte("v")); e != nil {
			t.Fatal(e)
		}
	}
	if len(p.segStarts) < 2 {
		t.Fatalf("expected segment rolling, got %d segments", len(p.segStarts))
	}
	// Read from the middle of a later segment: the sparse index must seek.
	rs, e := p.ReadFrom(17, 5)
	if e != nil {
		t.Fatal(e)
	}
	if len(rs) != 5 || rs[0].Offset != 17 || rs[4].Offset != 21 {
		t.Fatalf("bad indexed read: %+v", rs)
	}
	// Read across a segment boundary.
	rs, e = p.ReadFrom(p.segStarts[1]-1, 4)
	if e != nil {
		t.Fatal(e)
	}
	if len(rs) != 4 || rs[0].Offset != p.segStarts[1]-1 {
		t.Fatalf("bad cross-segment read: %+v", rs)
	}
	p.Close()

	// Reopen: all segments recovered, index rebuilt, reads still correct.
	p, e = OpenPartition(filepath.Join(d, "p"), 128, 4, false)
	if e != nil {
		t.Fatal(e)
	}
	if p.NextOffset() != 30 {
		t.Fatalf("recovery next=%d, want 30", p.NextOffset())
	}
	rs, e = p.ReadFrom(25, 5)
	if e != nil {
		t.Fatal(e)
	}
	if len(rs) != 5 || rs[0].Offset != 25 {
		t.Fatalf("bad post-recovery read: %+v", rs)
	}
	p.Close()
}

func TestTornTailTruncated(t *testing.T) {
	d := t.TempDir()
	dir := filepath.Join(d, "p")
	p, e := OpenPartition(dir, 1024, 2, false)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		if _, e := p.Append([]byte("ok")); e != nil {
			t.Fatal(e)
		}
	}
	p.Close()
	// Simulate a crash mid-write: garbage bytes after the last valid record.
	f, _ := os.OpenFile(filepath.Join(dir, "00000000000000000000.log"), os.O_WRONLY|os.O_APPEND, 0644)
	f.Write([]byte{0xFF, 0xFF, 0x00})
	f.Close()
	p, e = OpenPartition(dir, 1024, 2, false)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if p.NextOffset() != 5 {
		t.Fatalf("torn tail not truncated: next=%d, want 5", p.NextOffset())
	}
}

func TestAppendAfterRecoveryDoesNotOverwrite(t *testing.T) {
	d := t.TempDir()
	dir := filepath.Join(d, "p")
	p, e := OpenPartition(dir, 1024, 2, true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := p.Append([]byte("first")); e != nil {
		t.Fatal(e)
	}
	p.Close()

	p, e = OpenPartition(dir, 1024, 2, true)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	off, e := p.Append([]byte("second"))
	if e != nil {
		t.Fatal(e)
	}
	if off != 1 {
		t.Fatalf("offset=%d, want 1", off)
	}
	rs, e := p.ReadFrom(0, 10)
	if e != nil {
		t.Fatal(e)
	}
	if len(rs) != 2 || string(rs[0].Value) != "first" || string(rs[1].Value) != "second" {
		t.Fatalf("records corrupted after recovery: %+v", rs)
	}
}
