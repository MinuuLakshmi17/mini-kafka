package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/MinuuLakshmi17/mini-kafka/internal/protocol"
	"net"
	"os"
	"strconv"
	"strings"
)

func call(addr string, req protocol.Request) protocol.Response {
	c, e := net.Dial("tcp", addr)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	defer c.Close()
	if e = protocol.Write(c, req); e != nil {
		panic(e)
	}
	var r protocol.Response
	if e = protocol.Read(c, &r); e != nil {
		panic(e)
	}
	if !r.OK && r.Error != "" {
		fmt.Fprintln(os.Stderr, "error:", r.Error)
		os.Exit(1)
	}
	return r
}
func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "broker")
	flag.Parse()
	a := flag.Args()
	if len(a) < 2 {
		fmt.Println("usage: mkctl [--addr host:port] topic create|produce|consume|group ...")
		return
	}
	switch a[0] {
	case "topic":
		if a[1] == "create" && len(a) >= 4 {
			n, _ := strconv.Atoi(a[3])
			r := call(*addr, protocol.Request{Op: "create_topic", Topic: a[2], Partitions: n})
			fmt.Println("created", a[2], "partitions:", len(r.Partitions))
		}
	case "produce":
		if len(a) < 3 {
			return
		}
		p := 0
		if len(a) > 3 {
			p, _ = strconv.Atoi(a[3])
		}
		r := call(*addr, protocol.Request{Op: "produce", Topic: a[1], Partition: p, Payload: []byte(a[2])})
		fmt.Println("offset", r.Offset)
	case "consume":
		if len(a) < 3 {
			return
		}
		p := 0
		off := int64(0)
		max := 10
		if len(a) > 2 {
			p, _ = strconv.Atoi(a[2])
		}
		if len(a) > 3 {
			off, _ = strconv.ParseInt(a[3], 10, 64)
		}
		if len(a) > 4 {
			max, _ = strconv.Atoi(a[4])
		}
		r := call(*addr, protocol.Request{Op: "fetch", Topic: a[1], Partition: p, Offset: off, Max: max})
		for _, x := range r.Records {
			fmt.Printf("%d %s\n", x.Offset, string(x.Value))
		}
	case "group":
		if a[1] == "status" && len(a) > 2 {
			r := call(*addr, protocol.Request{Op: "group_status", Group: a[2]})
			for k, v := range r.Committed {
				fmt.Println(k, v)
			}
		}
		if a[1] == "join" && len(a) > 4 {
			n, _ := strconv.Atoi(a[4])
			r := call(*addr, protocol.Request{Op: "join", Group: a[2], MemberID: a[3], Partitions: n})
			fmt.Println("member", r.MemberID, "partitions:", r.Partitions)
		}
		if a[1] == "leave" && len(a) > 3 {
			call(*addr, protocol.Request{Op: "leave", Group: a[2], MemberID: a[3]})
			fmt.Println("left", a[3])
		}
		if a[1] == "commit" && len(a) > 5 {
			p, _ := strconv.Atoi(a[4])
			off, _ := strconv.ParseInt(a[5], 10, 64)
			call(*addr, protocol.Request{Op: "commit", Group: a[2], Topic: a[3], Partition: p, Offset: off})
			fmt.Println("committed")
		}
	case "help":
		_ = bufio.NewReader(strings.NewReader(""))
	}
}
