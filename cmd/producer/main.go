package main

import (
	"flag"
	"fmt"
	"github.com/MinuuLakshmi17/mini-kafka/internal/protocol"
	"net"
	"os"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "broker")
	topic := flag.String("topic", "orders", "topic")
	partition := flag.Int("partition", 0, "partition")
	flag.Parse()
	payload := []byte("hello from producer")
	if flag.NArg() > 0 {
		payload = []byte(flag.Arg(0))
	}
	c, e := net.Dial("tcp", *addr)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	defer c.Close()
	protocol.Write(c, protocol.Request{Op: "produce", Topic: *topic, Partition: *partition, Payload: payload})
	var r protocol.Response
	protocol.Read(c, &r)
	if !r.OK {
		fmt.Fprintln(os.Stderr, r.Error)
		os.Exit(1)
	}
	fmt.Println(r.Offset)
}
