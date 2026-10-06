package group

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestCommit(t *testing.T) {
	c := New(filepath.Join(t.TempDir(), "groups"))
	a, e := c.Join("g", "a", 3)
	if e != nil {
		t.Fatal(e)
	}
	if len(a) != 3 {
		t.Fatal(a)
	}
	if e = c.Commit("g", "t", 0, 9); e != nil {
		t.Fatal(e)
	}
	if c.Offset("g", "t", 0) != 9 {
		t.Fatal("offset")
	}
}

func TestDeterministicAssignment(t *testing.T) {
	for trial := 0; trial < 20; trial++ {
		c := New(t.TempDir())
		c.Join("g", "m1", 5)
		c.Join("g", "m2", 5)
		c.Join("g", "m3", 5)
		// Sorted member IDs -> partition i always goes to ids[i%3].
		// ids = [m1 m2 m3]: m1={0,3}, m2={1,4}, m3={2}.
		want := map[string][]int{"m1": {0, 3}, "m2": {1, 4}, "m3": {2}}
		got := map[string][]int{
			"m1": c.Assignment("g", "m1"),
			"m2": c.Assignment("g", "m2"),
			"m3": c.Assignment("g", "m3"),
		}
		for m, w := range want {
			if fmt.Sprint(got[m]) != fmt.Sprint(w) {
				t.Fatalf("trial %d: assignment %v, want %v", trial, got, want)
			}
		}
		c.Stop()
	}
}

func TestLeaveRebalances(t *testing.T) {
	c := New(t.TempDir())
	defer c.Stop()
	c.Join("g", "m1", 4)
	c.Join("g", "m2", 4)
	c.Leave("g", "m2")
	if got := c.Assignment("g", "m1"); fmt.Sprint(got) != "[0 1 2 3]" {
		t.Fatalf("after leave m1=%v, want all partitions", got)
	}
	if got := c.Assignment("g", "m2"); len(got) != 0 {
		t.Fatalf("departed member still assigned: %v", got)
	}
}

func TestStaleMemberEvicted(t *testing.T) {
	dir := t.TempDir()
	c := New(dir)
	defer c.Stop()
	c.SessionTimeout = 50 * time.Millisecond
	c.Join("g", "old", 2)
	time.Sleep(120 * time.Millisecond) // let "old" go stale, no heartbeat
	got, _ := c.Join("g", "new", 2)
	if fmt.Sprint(got) != "[0 1]" {
		t.Fatalf("stale member not evicted: new=%v", got)
	}
}
