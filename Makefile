GO ?= go
.PHONY: test race vet build run

test:
	$(GO) test ./...
race:
	$(GO) test -race ./...
vet:
	$(GO) vet ./...
build:
	$(GO) build ./cmd/server
run:
	$(GO) run ./cmd/server
