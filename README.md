# go-paxos

[![CI](https://github.com/jtomecek/go-paxos/actions/workflows/ci.yml/badge.svg)](https://github.com/jtomecek/go-paxos/actions/workflows/ci.yml)

A clean, educational, and production-quality implementation of the Multi-Paxos consensus algorithm in Go.

## Overview

Multi-Paxos is a distributed consensus protocol that allows a cluster of nodes to agree on a sequence of values, even in the presence of failures. This library provides:

- **Clean API** - Simple interface for proposing and learning values
- **Pluggable transports** - TCP (default) or custom implementations
- **Pluggable storage** - In-memory, file-based, or custom backends
- **Leader election** - Automatic leader election with lease-based optimization
- **Persistence** - Write-ahead logging with snapshots for crash recovery
- **Well-documented** - Code comments explain the algorithm as you read

## Installation

```bash
go get github.com/jtomecek/go-paxos
```

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/jtomecek/go-paxos/paxos"
)

func main() {
    // Create a 3-node cluster
    node, err := paxos.NewNode(paxos.Config{
        NodeID:  1,
        Address: "localhost:9001",
        Peers:   []string{"localhost:9002", "localhost:9003"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer node.Close()

    // Start the node
    if err := node.Start(); err != nil {
        log.Fatal(err)
    }

    // Propose a value (blocks until consensus is reached)
    instance, err := node.Propose(context.Background(), []byte("hello, paxos"))
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Value committed at instance %d", instance)
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Application                          │
├─────────────────────────────────────────────────────────────┤
│                     Paxos Node API                          │
│              Propose() / Subscribe() / Close()              │
├──────────────────┬──────────────────┬───────────────────────┤
│    Proposer      │    Acceptor      │      Learner          │
│  (Phase 1 & 2)   │   (Voting)       │  (Apply commits)      │
├──────────────────┴──────────────────┴───────────────────────┤
│                    Write-Ahead Log                          │
├─────────────────────────────────────────────────────────────┤
│     Transport (TCP)          │       Storage (File/Memory)  │
└─────────────────────────────────────────────────────────────┘
```

## The Paxos Algorithm

Multi-Paxos extends basic Paxos to agree on a sequence of values. Each value is assigned an **instance number**, and consensus is reached independently for each instance.

### Roles

- **Proposer**: Initiates consensus by proposing values
- **Acceptor**: Votes on proposals (must persist votes before responding)
- **Learner**: Learns committed values and applies them

In this implementation, every node plays all three roles.

### Protocol Phases

**Phase 1: Prepare**
```
Proposer                           Acceptors
    |                                  |
    |-------- Prepare(n) ------------>|  "I want to propose with ballot n"
    |                                  |
    |<------- Promise(n, v?) ---------|  "I promise not to accept < n"
    |                                  |  "Here's the highest value I accepted"
```

**Phase 2: Accept**
```
Proposer                           Acceptors
    |                                  |
    |-------- Accept(n, v) ---------->|  "Please accept value v with ballot n"
    |                                  |
    |<------- Accepted(n, v) ---------|  "I accepted it"
    |                                  |
    |-------- Commit(instance, v) --->|  "Value v is chosen for this instance"
```

### Leader Optimization

To avoid Phase 1 for every proposal, a stable leader can skip directly to Phase 2 using its established ballot number. This dramatically improves throughput.

## Configuration Options

```go
type Config struct {
    // Required
    NodeID  uint64   // Unique identifier for this node
    Address string   // Listen address (e.g., "localhost:9001")
    Peers   []string // Addresses of other nodes

    // Optional (sensible defaults provided)
    Transport        Transport     // Default: TCP
    Storage          Storage       // Default: in-memory
    Logger           Logger        // Default: no logging
    PrepareTimeout   time.Duration // Default: 1s
    AcceptTimeout    time.Duration // Default: 1s
    HeartbeatInterval time.Duration // Default: 100ms
    ElectionTimeout  time.Duration // Default: 1s
}
```

## Examples

See the [examples](./examples) directory:

- **[kv-store](./examples/kv-store)** - Distributed key-value store
- **[counter](./examples/counter)** - Distributed counter

## Testing

```bash
# Run all tests
go test ./...

# Run with race detector
go test -race ./...

# Run specific test with verbose output
go test -v -run TestLeaderElection ./paxos
```

## References

- [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) - Leslie Lamport's accessible explanation
- [Paxos Made Live](https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf) - Google's practical experience
- [Paxos Made Moderately Complex](https://paxos.systems/) - Detailed pseudocode

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) first.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

Jaroslav Tomecek - Based on a 2009 master thesis implementation, reimagined in Go.
