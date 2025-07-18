# Deimos & Phobos: The Twin Moons of Mars

<div align="center">
  <pre>
  ██████╗ ███████╗██╗███╗   ███╗ ██████╗ ███████╗
  ██╔══██╗██╔════╝██║████╗ ████║██╔═══██╗██╔════╝
  ██║  ██║█████╗  ██║██╔████╔██║██║   ██║███████╗
  ██║  ██║██╔══╝  ██║██║╚██╔╝██║██║   ██║╚════██║
  ██████╔╝███████╗██║██║ ╚═╝ ██║╚██████╔╝███████║
  ╚═════╝ ╚══════╝╚═╝╚═╝     ╚═╝ ╚═════╝ ╚══════╝
  </pre>
</div>

<p align="center">
  <strong>Deimos, the foundation of knowledge, is a distributed and consistent key-value store. Paired with its twin, <a href="https://github.com/marsevilspirit/phobos">Phobos</a>, it forms a complete microservices ecosystem for building resilient and scalable applications.</strong>
</p>

---

## Deimos & Phobos: A Symbiotic Relationship

In the cosmos of microservices, **Deimos** and **Phobos** are two celestial bodies orbiting the same planet: your application. They are designed to work in perfect harmony, each fulfilling a critical role.

*   **Deimos (Dread): The Foundation of Knowledge.** Deimos is the distributed, consistent key-value store that acts as the central nervous system for your services. It provides service discovery, configuration management, and distributed coordination. It handles the "where": where to find other services and the "what": the configuration that governs their behavior.

*   **Phobos (Fear): The Engine of Communication.** Phobos is the RPC framework that governs the interactions between your services. It provides the speed, resilience, and intelligence needed for high-performance communication. It handles the "how": how services talk to each other, how they handle failures, and how they balance load.

Together, they provide a powerful, cohesive, and elegant solution for building and managing complex microservice architectures.

## Features

### Core Key-Value Store (Deimos)

*   **Distributed Consensus**: Ensures data consistency and fault tolerance based on the Raft algorithm.
*   **HTTP/JSON API**: Provides a simple and easy-to-use RESTful API for easy integration with various clients.
*   **Clustering**: Supports multi-node cluster deployment for high availability.
*   **Secure Communication**: Supports TLS-based secure communication between clients and peers.
*   **Proxy Mode**: Supports read-only and read-write proxying for easy cluster scaling.
*   **Persistent Storage**: Ensures data durability through Write-Ahead Logging (WAL) and snapshots.

### Service Governance (with Phobos)

*   **Foundation for Service Discovery:** Deimos serves as the backbone for **Phobos**, providing the service registration and discovery mechanism that Phobos uses to locate and communicate with services.
*   **Centralized Configuration:** Store and manage configuration for all your Phobos services in Deimos, allowing for dynamic configuration updates without service restarts.
*   **Distributed Coordination:** Use Deimos for distributed locking and leader election, enabling complex coordination patterns between your Phobos services.

## Quick Start

This example demonstrates how to run a Deimos cluster.

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
  --listen-client-urls http://127.0.0.1:4001 \
  --advertise-client-urls http://127.0.0.1:4001 \
  --listen-peer-urls http://127.0.0.1:7001 \
  --advertise-peer-urls http://127.0.0.1:7001 \
  --bootstrap-config "node1=http://localhost:7001"
```

### Running a 3-Node Cluster

**Node 1:**
```bash
./bin/deimos --name node1 \
  --listen-client-urls http://127.0.0.1:4001 \
  --advertise-client-urls http://127.0.0.1:4001 \
  --listen-peer-urls http://127.0.0.1:7001 \
  --advertise-peer-urls http://127.0.0.1:7001 \
  --bootstrap-config "node1=http://localhost:7001,node2=http://localhost:7002,node3=http://localhost:7003"
```

**Node 2:**
```bash
./bin/deimos --name node2 \
  --listen-client-urls http://127.0.0.1:4002 \
  --advertise-client-urls http://127.0.0.1:4002 \
  --listen-peer-urls http://127.0.0.1:7002 \
  --advertise-peer-urls http://127.0.0.1:7002 \
  --bootstrap-config "node1=http://localhost:7001,node2=http://localhost:7002,node3=http://localhost:7003"
```

**Node 3:**
```bash
./bin/deimos --name node3 \
  --listen-client-urls http://127.0.0.1:4003 \
  --advertise-client-urls http://127.0.0.1:4003 \
  --listen-peer-urls http://127.0.0.1:7003 \
  --advertise-peer-urls http://127.0.0.1:7003 \
  --bootstrap-config "node1=http://localhost:7001,node2=http://localhost:7002,node3=http://localhost:7003"
```

## Usage

You can interact with Deimos using `curl` or the official [Go client](https://github.com/marsevilspirit/deimos-client).

### Writing Data

```bash
curl -L http://127.0.0.1:4001/keys/mykey -XPUT -d value="this is awesome"
```

### Reading Data

```bash
curl -L http://127.0.0.1:4001/keys/mykey
```

### Deleting Data

```bash
curl -L http://127.0.0.1:4001/keys/mykey -XDELETE
```
