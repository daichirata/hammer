GOFLAGS := -tags netgo -installsuffix netgo -ldflags '-w -s --extldflags "-static"'
GOVERSION=$(shell go version)
GOOS=$(word 1,$(subst /, ,$(lastword $(GOVERSION))))
GOARCH=$(word 2,$(subst /, ,$(lastword $(GOVERSION))))
BUILD_DIR=build/$(GOOS)-$(GOARCH)

.PHONY: all build clean deps package package-zip package-targz

all: build

build: deps
	mkdir -p $(BUILD_DIR)
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(GOFLAGS) -o $(BUILD_DIR)/hammer

clean:
	rm -rf build package

deps:
	go get -t ./...
	go mod tidy

package:
	$(MAKE) package-targz GOOS=linux GOARCH=amd64
	$(MAKE) package-targz GOOS=linux GOARCH=arm64
	$(MAKE) package-zip GOOS=darwin GOARCH=amd64

package-zip: build
	mkdir -p package
	cd $(BUILD_DIR) && zip ../../package/$(GOOS)_$(GOARCH).zip hammer

package-targz: build
	mkdir -p package
	cd $(BUILD_DIR) && tar zcvf ../../package/$(GOOS)_$(GOARCH).tar.gz hammer

test:
	go test -race -v ./...
