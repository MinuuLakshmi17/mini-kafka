package protocol

import (
	"net"
	"testing"
)

func TestFrame(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() { _ = Write(a, Request{Op: "ping", Topic: "x"}) }()
	var r Request
	if e := Read(b, &r); e != nil {
		t.Fatal(e)
	}
	if r.Op != "ping" || r.Topic != "x" {
		t.Fatal(r)
	}
}
