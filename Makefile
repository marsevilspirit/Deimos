SHELL := bash
.DELETE_ON_ERROR:
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

# Variables
BINARY_NAME=bin/deimos

.PHONY: help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build     Build deimos"
	@echo "  run       Run deimos"
	@echo "  clean     Remove generated wal"
	@echo "  generate  Generate proto files"
	@echo "  help      Show this help message"

.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) main.go

.PHONY: run
run: build
	@echo "Run Deimos Cluster"
	goreman start

.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -rf *data

.PHONY: generate
generate:
	@echo "Generate proto files"
	sh scripts/genproto.sh
