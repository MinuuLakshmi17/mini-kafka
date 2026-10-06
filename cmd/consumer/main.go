package main

import (
	"flag"
	"fmt"
	"github.com/MinuuLakshmi17/mini-kafka/internal/protocol"
	"net"
	"os"
	"strconv"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "broker")
	topic := flag.String("topic", "orders", "topic")
	partition := flag.Int("partition", 0, "partition")
	offset := flag.Int64("offset", 0, "offset")
	max := flag.Int("max", 10, "max records")
	flag.Parse()
	c, e := net.Dial("tcp", *addr)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	defer c.Close()
	protocol.Write(c, protocol.Request{Op: "fetch", Topic: *topic, Partition: *partition, Offset: *offset, Max: *max})
	var r protocol.Response
	protocol.Read(c, &r)
	if !r.OK {
		fmt.Fprintln(os.Stderr, r.Error)
		os.Exit(1)
	}
	for _, x := range r.Records {
		fmt.Println(strconv.FormatInt(x.Offset, 10), string(x.Value))
	}
}
