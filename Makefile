VERSION  := 0.2.0
BIN_NAME := hammer

BUILD_FLAGS := -tags netgo -installsuffix netgo -ldflags '-w -s --extldflags "-static"'
GOFILES := $(shell find . -type f -name '*.go')

build: bin/$(BIN_NAME)

bin/$(BIN_NAME): $(GOFILES)
	go build $(BUILD_FLAGS) -o $@ .

build-cross: clean
	GOOS=linux  GOARCH=amd64 go build $(BUILD_FLAGS) -o dist/$(BIN_NAME)_$(VERSION)_amd64_linux
	GOOS=darwin GOARCH=amd64 go build $(BUILD_FLAGS) -o dist/$(BIN_NAME)_$(VERSION)_amd64_darwin

clean:
	rm -rf bin dist

deps:
	go get -t ./...
	go mod tidy

test:
	go test -race -v ./...
