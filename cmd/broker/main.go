package main

import (
	"flag"
	"github.com/MinuuLakshmi17/mini-kafka/internal/broker"
	"log"
)

func main() {
	addr := flag.String("addr", ":9092", "listen address")
	data := flag.String("data", "./data", "data directory")
	flag.Parse()
	b := broker.New(*addr, *data)
	log.Printf("mini-kafka broker listening on %s", *addr)
	if err := b.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
