package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
)

const MaxFrameSize = 16 << 20

type Request struct {
	Op         string `json:"op"`
	Topic      string `json:"topic,omitempty"`
	Partition  int    `json:"partition,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	Payload    []byte `json:"payload,omitempty"`
	Group      string `json:"group,omitempty"`
	MemberID   string `json:"member_id,omitempty"`
	Max        int    `json:"max,omitempty"`
	Partitions int    `json:"partitions,omitempty"`
}

type Response struct {
	OK         bool             `json:"ok"`
	Error      string           `json:"error,omitempty"`
	Offset     int64            `json:"offset,omitempty"`
	Records    []Record         `json:"records,omitempty"`
	Partitions []int            `json:"partitions,omitempty"`
	Group      string           `json:"group,omitempty"`
	MemberID   string           `json:"member_id,omitempty"`
	Assignment map[string][]int `json:"assignment,omitempty"`
	Committed  map[string]int64 `json:"committed,omitempty"`
}

type Record struct {
	Offset int64  `json:"offset"`
	UnixNS int64  `json:"unix_ns"`
	Value  []byte `json:"value"`
}

func Write(conn net.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxFrameSize {
		return fmt.Errorf("frame too large: %d", len(b))
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	if _, err = conn.Write(h[:]); err != nil {
		return err
	}
	_, err = conn.Write(b)
	return err
}

func Read(conn net.Conn, v any) error {
	r := bufio.NewReader(conn)
	return ReadBuffered(r, v)
}

func ReadBuffered(r *bufio.Reader, v any) error {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > MaxFrameSize {
		return errors.New("invalid frame size")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
