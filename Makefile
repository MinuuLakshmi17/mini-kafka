BINDIR=bin

.PHONY: all build test fmt clean
all: test build

build:
	mkdir -p $(BINDIR)
	go build -o $(BINDIR)/broker ./cmd/broker
	go build -o $(BINDIR)/producer ./cmd/producer
	go build -o $(BINDIR)/consumer ./cmd/consumer
	go build -o $(BINDIR)/mkctl ./cmd/mkctl

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go')

clean:
	rm -rf $(BINDIR) data
