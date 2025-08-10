# Deimos Dockerfile
# Multi-stage build for efficient container image

# Build stage
FROM golang:1.24.5-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make bash

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy only required source to minimize context
COPY Makefile ./
COPY main.go ./
COPY client/ client/
COPY discovery/ discovery/
COPY error/ error/
COPY pkg/ pkg/
COPY proxy/ proxy/
COPY raft/ raft/
COPY server/ server/
COPY snap/ snap/
COPY store/ store/
COPY wait/ wait/
COPY wal/ wal/

# Build the binary
RUN mkdir -p bin && make build

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    && rm -rf /var/cache/apk/*

# Create non-root user
RUN addgroup -g 1000 deimos && \
    adduser -D -s /bin/sh -u 1000 -G deimos deimos

# Create data directory
RUN mkdir -p /var/lib/deimos && \
    chown -R deimos:deimos /var/lib/deimos

# Copy binary from builder stage
COPY --from=builder /app/bin/deimos /usr/local/bin/deimos

# Switch to non-root user
USER deimos

# Set working directory
WORKDIR /var/lib/deimos

# Expose ports
# Client API port
EXPOSE 4001
# Peer communication port  
EXPOSE 7001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:4001/machines || exit 1

# Default command - single node mode
CMD ["deimos", \
     "-name", "node1", \
     "-listen-client-urls", "http://0.0.0.0:4001", \
     "-advertise-client-urls", "http://localhost:4001", \
     "-listen-peer-urls", "http://0.0.0.0:7001", \
     "-advertise-peer-urls", "http://localhost:7001"]
