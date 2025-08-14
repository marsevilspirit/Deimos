SHELL := bash
.DELETE_ON_ERROR:
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

# Variables
BINARY_NAME=bin/deimos

.PHONY: help build run test clean lint generate

# Default target
help:
	@echo "Available commands:"
	@echo "  build      - Build the project"
	@echo "  run        - Run deimos cluster"
	@echo "  test       - Run tests"
	@echo "  clean      - Clean build artifacts"
	@echo "  lint       - Run golangci-lint"
	@echo "  generate   - Generate proto files"

# Build the project
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) main.go

# Run deimos cluster
run: build
	@echo "Run Deimos Cluster"
	goreman start

# Run tests
test:
	go test -v ./...

# Clean build artifacts
clean:
	go clean
	rm -f coverage.out coverage.html
	rm -f deimos

# Install golangci-lint and run lint
lint:
	@echo "Installing golangci-lint..."
	@if ! command -v golangci-lint &> /dev/null; then \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.55.2; \
	else \
		echo "golangci-lint already installed"; \
	fi
	@echo "Running golangci-lint..."
	golangci-lint run --timeout=5m

# Build main binary
build-main:
	go build -o deimos ./main.go

# Generate protobuf files
generate:
	@echo "Generate proto files"
	sh scripts/genproto.sh

# Generate protobuf files (alias)
proto: generate
