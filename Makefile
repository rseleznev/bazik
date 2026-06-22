.DEFAULT_GOAL := build

.PHONY: mod test build
mod:
	go mod download

test: mod
	go test ./... -v -count=5 -race

build: test
	go build -o bazik