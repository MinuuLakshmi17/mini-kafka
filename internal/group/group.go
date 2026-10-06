package group

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Member is one consumer-group participant.
type Member struct {
	ID       string
	LastSeen time.Time
}

// Coordinator tracks group membership, assigns partitions, and persists
// committed offsets. Members that stop heartbeating are evicted after
// SessionTimeout, which triggers a rebalance.
type Coordinator struct {
	mu              sync.Mutex
	dir             string
	SessionTimeout  time.Duration
	members         map[string]map[string]*Member
	assignments     map[string]map[string][]int
	offsets         map[string]map[string]int64
	groupPartitions map[string]int
	stopCh          chan struct{}
}

func New(dir string) *Coordinator {
	c := &Coordinator{
		dir:             dir,
		SessionTimeout:  30 * time.Second,
		members:         map[string]map[string]*Member{},
		assignments:     map[string]map[string][]int{},
		offsets:         map[string]map[string]int64{},
		groupPartitions: map[string]int{},
		stopCh:          make(chan struct{}),
	}
	go c.reapLoop()
	return c
}

// Stop terminates the background session reaper.
func (c *Coordinator) Stop() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

func (c *Coordinator) reapLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.reapOnce(time.Now())
		case <-c.stopCh:
			return
		}
	}
}

func (c *Coordinator) reapOnce(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for g, members := range c.members {
		changed := false
		for id, m := range members {
			if now.Sub(m.LastSeen) > c.SessionTimeout {
				delete(members, id)
				changed = true
			}
		}
		if changed {
			c.rebalanceLocked(g)
		}
	}
}

// rebalanceLocked recomputes the round-robin assignment deterministically:
// member IDs are sorted so every coordinator computes the same layout.
func (c *Coordinator) rebalanceLocked(group string) {
	members := c.members[group]
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	n := c.groupPartitions[group]
	assign := map[string][]int{}
	for i := 0; i < n && len(ids) > 0; i++ {
		m := ids[i%len(ids)]
		assign[m] = append(assign[m], i)
	}
	c.assignments[group] = assign
}

func (c *Coordinator) load(group string) error {
	if c.offsets[group] != nil {
		return nil
	}
	c.offsets[group] = map[string]int64{}
	b, err := os.ReadFile(filepath.Join(c.dir, group+".json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	tmp := map[string]int64{}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	c.offsets[group] = tmp
	return nil
}

func (c *Coordinator) persist(group string) error {
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c.offsets[group], "", "  ")
	return os.WriteFile(filepath.Join(c.dir, group+".json"), b, 0644)
}

// Join adds (or refreshes) a member and rebalances the group's partitions.
func (c *Coordinator) Join(group, id string, partitions int) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.load(group); err != nil {
		return nil, err
	}
	if c.members[group] == nil {
		c.members[group] = map[string]*Member{}
	}
	c.members[group][id] = &Member{id, time.Now()}
	if partitions > 0 {
		c.groupPartitions[group] = partitions
	}
	// Lazily evict stale members so a join also heals a dead member's absence.
	now := time.Now()
	for mid, m := range c.members[group] {
		if now.Sub(m.LastSeen) > c.SessionTimeout {
			delete(c.members[group], mid)
		}
	}
	c.rebalanceLocked(group)
	return append([]int(nil), c.assignments[group][id]...), nil
}

// Leave removes a member immediately and rebalances.
func (c *Coordinator) Leave(group, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[group] != nil {
		delete(c.members[group], id)
		c.rebalanceLocked(group)
	}
}

// Heartbeat refreshes a member's session. It returns false for unknown members.
func (c *Coordinator) Heartbeat(group, id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[group]
	if m == nil || m[id] == nil {
		return false
	}
	m[id].LastSeen = time.Now()
	return true
}

// Assignment returns the caller's current partitions (nil for non-members).
func (c *Coordinator) Assignment(group, id string) []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.assignments[group][id]...)
}

// Commit persists a consumer offset for a topic partition.
func (c *Coordinator) Commit(group, topic string, partition int64, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.load(group); err != nil {
		return err
	}
	c.offsets[group][fmt.Sprintf("%s:%d", topic, partition)] = offset
	return c.persist(group)
}

// Offset returns the last committed offset, or 0 if none.
func (c *Coordinator) Offset(group, topic string, partition int) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.load(group)
	return c.offsets[group][fmt.Sprintf("%s:%d", topic, partition)]
}

// Status returns all committed offsets for a group.
func (c *Coordinator) Status(group string) map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.load(group)
	out := map[string]int64{}
	for k, v := range c.offsets[group] {
		out[k] = v
	}
	return out
}
