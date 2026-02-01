# The Paxos Algorithm

This document explains how Paxos consensus works and how this library implements it.

## Overview

Paxos is a family of protocols for solving consensus in a network of unreliable processors. It was first described by Leslie Lamport in 1989 and named after a fictional legislative process on the Greek island of Paxos.

The core problem Paxos solves is: **How can a group of computers agree on a single value, even if some of them fail?**

## The Basic Paxos Protocol

### Roles

There are three roles in Paxos:

1. **Proposer**: Proposes values for consensus
2. **Acceptor**: Votes on proposals (must be a majority to agree)
3. **Learner**: Learns the agreed-upon value

In practice, each node typically plays all three roles.

### Key Concepts

- **Ballot Number**: A unique, monotonically increasing identifier for each proposal attempt
- **Instance**: A position in the replicated log (Multi-Paxos only)
- **Quorum**: A majority of acceptors (⌊N/2⌋ + 1 out of N)

### Phase 1: Prepare

```
Proposer                           Acceptors
    |                                  |
    |-------- Prepare(ballot) -------->|
    |                                  |
    |<------- Promise(ballot, ---------|
    |         acceptedBallot?,         |
    |         acceptedValue?)          |
```

1. Proposer chooses a ballot number `n` higher than any it has seen
2. Proposer sends `Prepare(n)` to all acceptors
3. Each acceptor:
   - If `n` > any ballot it has promised: Promise not to accept any ballot < `n`
   - Reply with the highest-numbered proposal it has accepted (if any)

### Phase 2: Accept

```
Proposer                           Acceptors
    |                                  |
    |-------- Accept(ballot, v) ------>|
    |                                  |
    |<------- Accepted(ballot) --------|
```

1. If proposer receives promises from a quorum:
   - If any acceptor reported an accepted value, use the value from the highest ballot
   - Otherwise, use its own proposed value
2. Send `Accept(n, value)` to all acceptors
3. Each acceptor:
   - If it hasn't promised a higher ballot: Accept the proposal
   - Persist the acceptance before responding

### Commit

Once a proposer receives `Accepted` from a quorum, the value is **chosen**. The proposer broadcasts a `Commit` message to all nodes.

## Multi-Paxos: Agreeing on a Sequence

Basic Paxos agrees on a single value. Multi-Paxos extends this to agree on a sequence of values (a log).

Each position in the log is called an **instance**, and each instance runs an independent Paxos consensus.

### Leader Optimization

In Multi-Paxos, a stable leader can skip Phase 1 for subsequent proposals:

1. Leader runs Phase 1 once to establish its ballot
2. For subsequent proposals, it goes directly to Phase 2
3. If preempted, it must run Phase 1 again

This dramatically improves performance under normal operation.

## Safety Properties

Paxos guarantees these safety properties:

1. **Validity**: Only proposed values can be chosen
2. **Agreement**: At most one value can be chosen per instance
3. **Termination**: If a majority of nodes are functioning, a value will eventually be chosen

## This Implementation

### Message Types

| Message | Phase | Description |
|---------|-------|-------------|
| `Prepare` | 1a | "I want to propose with ballot B" |
| `Promise` | 1b | "I promise not to accept ballots < B" |
| `Reject` | 1b | "I've already promised a higher ballot" |
| `Accept` | 2a | "Please accept value V with ballot B" |
| `Accepted` | 2b | "I accepted it" |
| `Nack` | 2b | "I've promised a higher ballot" |
| `Commit` | - | "Value V is chosen" |
| `Heartbeat` | - | "I'm the leader" (lease maintenance) |

### Ballot Number Format

Ballots are 64-bit integers: `(round << 16) | nodeID`

This ensures:
- Ballots from different nodes never collide
- Higher rounds always produce higher ballots
- Node ID is recoverable from the ballot

### Persistence Requirements

For safety, acceptors MUST persist state before responding:
- Promised ballot
- Accepted ballot and value

This implementation does this in `SaveAcceptorState()`.

### Leader Election

This implementation uses heartbeat-based leader election:

1. Leader sends periodic heartbeats
2. If a node doesn't receive heartbeats within `ElectionTimeout`, it starts an election
3. Election is just running Phase 1 for a special "leadership" instance

## Comparison with Raft

| Aspect | Paxos | Raft |
|--------|-------|------|
| Leader election | Implicit (any node can propose) | Explicit (dedicated election) |
| Log replication | Per-instance consensus | Leader-driven append |
| Understandability | Harder | Easier |
| Flexibility | More flexible | More opinionated |

Paxos is more fundamental; Raft is more practical for implementation.

## References

1. [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) - Lamport's accessible explanation
2. [Paxos Made Live](https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf) - Google's Chubby implementation
3. [Paxos Made Moderately Complex](https://paxos.systems/) - Detailed pseudocode and proofs
