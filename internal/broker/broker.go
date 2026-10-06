package broker

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/MinuuLakshmi17/mini-kafka/internal/group"
	klog "github.com/MinuuLakshmi17/mini-kafka/internal/log"
	"github.com/MinuuLakshmi17/mini-kafka/internal/protocol"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type Broker struct {
	mu         sync.RWMutex
	addr, data string
	ln         net.Listener
	topics     map[string][]*klog.Partition
	groups     *group.Coordinator
}

func New(addr, data string) *Broker {
	return &Broker{addr: addr, data: data, topics: map[string][]*klog.Partition{}, groups: group.New(filepath.Join(data, "groups"))}
}
func (b *Broker) CreateTopic(name string, n int) error {
	if n < 1 {
		return errors.New("partitions must be >= 1")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return fmt.Errorf("topic already exists")
	}
	parts := make([]*klog.Partition, n)
	for i := 0; i < n; i++ {
		p, e := klog.OpenPartition(filepath.Join(b.data, "topics", name, fmt.Sprintf("partition-%d", i)), 64<<20, 16, true)
		if e != nil {
			return e
		}
		parts[i] = p
	}
	b.topics[name] = parts
	return nil
}
func (b *Broker) ensureTopic(name string) ([]*klog.Partition, error) {
	b.mu.RLock()
	p := b.topics[name]
	b.mu.RUnlock()
	if p == nil {
		return nil, fmt.Errorf("topic %q not found", name)
	}
	return p, nil
}
func (b *Broker) ListenAndServe() error {
	if err := os.MkdirAll(b.data, 0755); err != nil {
		return err
	}
	if err := b.recoverTopics(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", b.addr)
	if err != nil {
		return err
	}
	b.ln = ln
	for {
		c, e := ln.Accept()
		if e != nil {
			return e
		}
		go b.handle(c)
	}
}

// recoverTopics reopens every topic found under the data directory so a
// restarted broker serves the partitions that are already on disk.
func (b *Broker) recoverTopics() error {
	topicsDir := filepath.Join(b.data, "topics")
	entries, err := os.ReadDir(topicsDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		pdirs, err := filepath.Glob(filepath.Join(topicsDir, name, "partition-*"))
		if err != nil {
			return err
		}
		byNum := map[int]string{}
		maxN := -1
		for _, pd := range pdirs {
			var n int
			if _, err := fmt.Sscanf(filepath.Base(pd), "partition-%d", &n); err != nil {
				continue
			}
			byNum[n] = pd
			if n > maxN {
				maxN = n
			}
		}
		if maxN < 0 {
			continue
		}
		parts := make([]*klog.Partition, maxN+1)
		for n := 0; n <= maxN; n++ {
			dir, ok := byNum[n]
			if !ok {
				return fmt.Errorf("topic %q: missing partition-%d directory", name, n)
			}
			p, err := klog.OpenPartition(dir, 64<<20, 16, true)
			if err != nil {
				return err
			}
			parts[n] = p
		}
		b.topics[name] = parts
	}
	return nil
}
func (b *Broker) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		var req protocol.Request
		if e := protocol.ReadBuffered(r, &req); e != nil {
			return
		}
		resp := b.dispatch(req)
		if e := protocol.Write(c, resp); e != nil {
			return
		}
	}
}
func (b *Broker) dispatch(req protocol.Request) protocol.Response {
	switch req.Op {
	case "create_topic":
		if e := b.CreateTopic(req.Topic, req.Partitions); e != nil {
			return protocol.Response{Error: e.Error()}
		}
		p, _ := b.ensureTopic(req.Topic)
		idx := make([]int, len(p))
		for i := range p {
			idx[i] = i
		}
		return protocol.Response{OK: true, Partitions: idx}
	case "list_partitions":
		p, e := b.ensureTopic(req.Topic)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		idx := make([]int, len(p))
		for i := range p {
			idx[i] = i
		}
		return protocol.Response{OK: true, Partitions: idx}
	case "produce":
		p, e := b.ensureTopic(req.Topic)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		if req.Partition < 0 || req.Partition >= len(p) {
			return protocol.Response{Error: "invalid partition"}
		}
		off, e := p[req.Partition].Append(req.Payload)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		return protocol.Response{OK: true, Offset: off}
	case "fetch":
		p, e := b.ensureTopic(req.Topic)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		if req.Partition < 0 || req.Partition >= len(p) {
			return protocol.Response{Error: "invalid partition"}
		}
		rs, e := p[req.Partition].ReadFrom(req.Offset, req.Max)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		out := make([]protocol.Record, len(rs))
		for i, r := range rs {
			out[i] = protocol.Record{Offset: r.Offset, UnixNS: r.UnixNS, Value: r.Value}
		}
		return protocol.Response{OK: true, Records: out}
	case "join":
		as, e := b.groups.Join(req.Group, req.MemberID, req.Partitions)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		return protocol.Response{OK: true, MemberID: req.MemberID, Partitions: as}
	case "heartbeat":
		return protocol.Response{OK: b.groups.Heartbeat(req.Group, req.MemberID)}
	case "leave":
		b.groups.Leave(req.Group, req.MemberID)
		return protocol.Response{OK: true}
	case "commit":
		e := b.groups.Commit(req.Group, req.Topic, int64(req.Partition), req.Offset)
		if e != nil {
			return protocol.Response{Error: e.Error()}
		}
		return protocol.Response{OK: true}
	case "offset":
		return protocol.Response{OK: true, Offset: b.groups.Offset(req.Group, req.Topic, req.Partition)}
	case "group_status":
		return protocol.Response{OK: true, Committed: b.groups.Status(req.Group)}
	default:
		return protocol.Response{Error: "unknown operation"}
	}
}
