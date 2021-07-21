VERSION  := 0.5.2
BIN_NAME := hammer

GOFLAGS := -tags netgo -installsuffix netgo -ldflags '-w -s --extldflags "-static"'
GOFILES := $(shell find . -type f -name '*.go')

build: bin/$(BIN_NAME)

bin/$(BIN_NAME): $(GOFILES)
	go build $(GOFLAGS) -o $@ .

build-cross: clean
	GOOS=linux  GOARCH=amd64 go build $(GOFLAGS) -o dist/$(BIN_NAME)_$(VERSION)_amd64_linux
	GOOS=darwin GOARCH=amd64 go build $(GOFLAGS) -o dist/$(BIN_NAME)_$(VERSION)_amd64_darwin

clean:
	rm -rf bin dist

deps:
	go get -t ./...
	go mod tidy

test:
	go test -race -v ./...
