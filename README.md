# Deimos

<div align="center">
  <pre>
  ██████╗ ███████╗██╗███╗   ███╗ ██████╗ ███████╗
  ██╔══██╗██╔════╝██║████╗ ████║██╔═██╗██╔════╝
  ██║  ██║█████╗  ██║██╔████╔██║██║   ██║███████╗
  ██║  ██║██╔══╝  ██║██║╚██╔╝██║██║   ██║╚════██║
  ██████╔╝███████╗██║██║ ╚═╝ ██║╚██████╔╝███████║
  ╚═════╝ ╚══════╝╚═╝╚═╝     ╚═╝ ╚═════╝ ╚══════╝
  </pre>
</div>

<p align="center">
  <strong>A simple, highly available, distributed key-value store based on the Raft consensus algorithm.</strong>
</p>

---

## Introduction

Deimos is a distributed key-value store written in Go. It uses the [Raft consensus algorithm](https://raft.github.io/) to ensure data consistency and high availability across the cluster. Deimos provides a simple HTTP/JSON API for storing and retrieving data.

## Features

- **Distributed Consensus**: Ensures data consistency and fault tolerance based on the Raft algorithm.
- **HTTP/JSON API**: Provides a simple and easy-to-use RESTful API for easy integration with various clients.
- **Clustering**: Supports multi-node cluster deployment for high availability.
- **Secure Communication**: Supports TLS-based secure communication between clients and peers.
- **Proxy Mode**: Supports read-only and read-write proxying for easy cluster scaling.
- **Persistent Storage**: Ensures data durability through Write-Ahead Logging (WAL) and snapshots.

## Quick Start

### Prerequisites

- [Go](https://golang.org/dl/) (version 1.18 or higher)

### Build from Source

```bash
git clone https://github.com/marsevilspirit/deimos.git
cd deimos
make build
```

### Running a Single Node

```bash
./bin/deimos --name node1 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --advertise-peer-urls http://127.0.0.1:2380 \
  --bootstrap-config "node1=http://127.0.0.1:2380"
```

### Running a 3-Node Cluster

**Node 1:**
```bash
./bin/deimos --name node1 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --advertise-peer-urls http://127.0.0.1:2380 \
  --bootstrap-config "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
```

**Node 2:**
```bash
./bin/deimos --name node2 \
  --listen-client-urls http://127.0.0.1:2479 \
  --advertise-client-urls http://127.0.0.1:2479 \
  --listen-peer-urls http://127.0.0.1:2480 \
  --advertise-peer-urls http://127.0.0.1:2480 \
  --bootstrap-config "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
```

**Node 3:**
```bash
./bin/deimos --name node3 \
  --listen-client-urls http://127.0.0.1:2579 \
  --advertise-client-urls http://127.0.0.1:2579 \
  --listen-peer-urls http://127.0.0.1:2580 \
  --advertise-peer-urls http://127.0.0.1:2580 \
  --bootstrap-config "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
```

## Usage

You can interact with Deimos using `curl` or any other HTTP client.

### Writing Data

```bash
curl -L http://127.0.0.1:2379/keys/mykey -XPUT -d value="this is awesome"
```

### Reading Data

```bash
curl -L http://127.0.0.1:2379/keys/mykey
```

### Deleting Data

```bash
curl -L http://127.0.0.1:2379/keys/mykey -XDELETE
```

## Configuration

Deimos can be configured via command-line flags. You can see all available options by running:

```bash
./bin/deimos --help
```