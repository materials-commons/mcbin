.PHONY: bin test all fmt deploy docs server cli setup

all: fmt bin

fmt:
	-go fmt ./...

bin: cli

cli:
	(cd ./cmd/mcdszip; go build)
