# Raft

A complete implementation of the Raft distributed consensus algorithm in Go, covering leader election, log replication, crash recovery with persistent state, log compaction via snapshots, and single-server cluster membership changes.

---

## Overview

Raft is a consensus algorithm designed to be understandable. A cluster of Raft nodes agrees on a totally ordered sequence of commands — the replicated log — even in the presence of server crashes, network partitions, and message loss. As long as a majority of nodes are reachable, the cluster makes progress. Any minority can fail without data loss.

This implementation targets correctness and clarity. Most pieces of the paper are present: randomised election timeouts, term-aware voting, log consistency checks, conflict-accelerated backtracking, durable state, snapshot-based log compaction, and single-node membership changes.

---

## Features

### Leader Election

Nodes start as followers. If a follower hears no heartbeat from a leader within its election timeout, it increments its term, converts to candidate, votes for itself, and requests votes from peers. A candidate that collects votes from a strict majority wins the election and immediately begins sending heartbeats to suppress further elections.

Election timeouts are randomised between 150 ms and 300 ms, which makes it very unlikely that two nodes time out simultaneously and split the vote. An environment variable (`RAFT_FORCE_MORE_REELECTION`) can pin one-third of timeouts to the minimum, used during stress testing to provoke frequent re-elections and verify that the cluster always converges.

### Log Replication

The leader appends new commands to its local log and then replicates them to followers with AppendEntries RPCs. An entry is committed — and delivered to the application — once it has been acknowledged by a strict majority. Followers that fall behind are caught up by the leader, which tracks `nextIndex` and `matchIndex` per peer and retries until success.

**Trigger-based AppendEntries.** Rather than waiting for the next heartbeat tick when a client submits a command, the leader immediately signals a dedicated channel that wakes the replication loop. This keeps commit latency close to one round-trip time under load.

**Conflict-accelerated backtracking.** When a follower rejects an AppendEntries because its log diverges from the leader's, the naive approach decrements `nextIndex` by one and retries. This implementation includes the optimisation from the extended Raft paper: the follower returns the conflicting term and the first index where that term appears in its log, allowing the leader to skip over the entire conflicting term in a single step. Under repeated leader changes or large divergences this turns O(n) round trips into O(number of conflicting terms), which in practice is almost always O(1).

### Persistence

Three fields must survive a crash to guarantee correctness: `currentTerm`, `votedFor`, and the log. All three are persisted to stable storage on every state change that touches them, before the function returns. Log entries are encoded with `encoding/gob` and stored as a single blob alongside the term, vote, log offset, and snapshot metadata.

Two storage backends are provided. `MapStorage` is an in-memory map used by tests — it survives in-process restarts but not process death. `FileStorage` is a durable implementation that writes each key to its own file using a write-then-rename pattern: data is flushed and `fsync`ed to a temporary file in the same directory, then atomically renamed over the target. A crash mid-write therefore never leaves a partially-written or corrupt value behind.

### Log Compaction (Snapshots)

As the log grows, replaying it from the start on restart becomes expensive and transferring it to new or recovering nodes wastes bandwidth. Log compaction solves this: the application periodically serialises its state machine into a snapshot and tells Raft the last log index covered. Raft then discards all log entries up to and including that index, retaining only the snapshot metadata (`lastIncludedIndex`, `lastIncludedTerm`) needed to anchor future AppendEntries calls.

The in-memory log uses a `logOffset` field so that all global log indices remain stable after compaction; arithmetic helpers translate between global indices and the current slice positions transparently throughout the codebase.

When a follower falls so far behind that the leader has already discarded the entries the follower needs, the leader sends an InstallSnapshot RPC instead of AppendEntries. The follower replaces its log and state with the snapshot, advances its commit index, and delivers the snapshot to the application layer via a dedicated channel. The application can then restore its state machine and resume normal operation. Snapshot delivery is non-blocking: a small goroutine forwards the notification so that the RPC handler never stalls waiting for the application.

### Single-Server Membership Changes

The cluster membership — the set of server IDs that participate in elections and quorum counts — can be changed at runtime by adding or removing one server at a time. Each change is serialised as a special `ConfigChangeEntry` in the Raft log and treated like any other command: it is proposed by the leader, replicated to a majority, and committed. Only one membership change may be in flight at a time; a second proposal is rejected until the first commits.

**Adding a node.** The leader initialises replication state for the new peer immediately when the `AddNode` entry is appended, so the new node begins receiving log entries right away and can catch up before the config entry commits. The new peer's votes are not counted in quorum until the entry commits and `maybeApplyConfigAt` updates the live peer list.

**Removing a node.** When a `RemoveNode` entry commits, all nodes remove the target from their peer lists. If the removed node is the current leader, it steps down immediately after the config commits: it calls `becomeFollower`, which starts a new election timer, and the remaining nodes elect a replacement from among themselves.

All membership changes flow through the commit channel alongside regular commands, so the application layer sees the full ordered history and can replay it correctly on restart.

---

## Architecture

Each `Server` owns a `ConsensusModule`. The module drives leader election and log replication entirely from goroutines it spawns itself — one election timer loop, one leader heartbeat loop, and one commit-channel sender. All mutable state is protected by a single mutex; goroutines that need to send RPCs capture the state they need while holding the lock, release the lock, perform the RPC, then re-acquire the lock to process the reply.

An `RPCProxy` sits between the transport and the consensus module. In tests it can be configured to drop all calls after a given count, enabling precise simulation of network partitions and dropped messages without shutting down the server.

---

## Usage

### Starting a cluster

Create one `Server` per node, giving each its own ID, the IDs of its peers, a `Storage` implementation, and a commit channel. Call `Serve()` to bind a TCP listener, then wire up peer connections once all listeners are ready.

```go
storage, _ := NewFileStorage("/var/lib/raft/node-0")
commitChan := make(chan CommitEntry, 64)
ready := make(chan any)

s := NewServer(0, []int{1, 2}, storage, ready, commitChan, logger)
s.Serve()

s.ConnectToPeer(1, peer1Addr)
s.ConnectToPeer(2, peer2Addr)
close(ready) // signals the CM to start its election timer
```

Repeat for each node, using the listener address returned by `GetListenAddr()` as the address passed to `ConnectToPeer` on the other nodes.

### Submitting commands

Send a command to any server. If the server is the current leader it appends the entry and returns immediately with the global log index. If it is not the leader it returns a hint pointing to the last known leader.

```go
result := server.Submit(myCommand)
if !result.IsLeader {
    // retry on result.LeaderHint
}
```

Committed entries arrive on `commitChan` in global index order. `ConfigChangeEntry` values also appear on this channel; applications that need to replay the full log on restart must handle them.

### Taking snapshots

After applying entries up through some index, the application serialises its state and calls `InstallSnapshot` on its local server's consensus module. Raft trims the in-memory log and persists the snapshot metadata. The application controls when to snapshot; a common heuristic is to snapshot when the log exceeds a size threshold.

```go
data := encodeMyStateMachine(stateMachine)
cm.InstallSnapshot(lastAppliedIndex, lastAppliedTerm, data)
```

Snapshots sent by the leader to lagging followers arrive on the channel returned by `SnapshotReady()`. The application must drain this channel and restore its state machine from the delivered data.

### Changing cluster membership

Membership changes must be sent to the current leader. Both operations return `false` immediately if the caller is not the leader or if another config change is already pending.

```go
// Add a new node
leaderServer.AddPeer(newNodeId, newNodeAddr)

// Remove an existing node (including the leader itself)
leaderServer.RemovePeer(targetId)
```

Add the new server to the cluster before calling `AddPeer` so that it can receive log entries during catch-up.

### Storage backends

Swap `MapStorage` for `FileStorage` in production. `FileStorage` is safe for concurrent use and uses atomic rename-on-write so a crash mid-persist never corrupts existing state.

```go
storage, err := NewFileStorage("/var/lib/raft/node-1")
```

---

## Testing

### Running the tests

```bash
go test ./...                          # all tests
go test -run TestElection ./...        # election tests only
go test -run TestSnapshot ./...        # snapshot tests only
go test -run TestMembership ./...      # membership tests only
go test -race ./...                    # with the data-race detector
go test -count=5 ./...                 # repeat each test 5× (stress)
```

### Stress modes

Two environment variables exercise less-common code paths:

```bash
RAFT_FORCE_MORE_REELECTION=1 go test ./...
```

Pins one-third of election timeouts to the minimum (150 ms), producing frequent simultaneous elections and verifying that the cluster always converges to a single leader.

```bash
RAFT_UNRELIABLE_RPC=1 go test ./...
```

Randomly drops (10%) or delays (10%) every RPC, simulating an unreliable network. Tests must still pass; commits must still happen and consistency must be maintained.

---

## Test Coverage

The test suite is organised across three files totalling around 55 test functions.

### Election (`raft_test.go`)

Basic election, leader disconnect and re-election, minority disconnection with no quorum, full disconnect-then-restore, and a stress test that runs repeated disconnect loops across a 5-node cluster. Several regression tests cover specific bugs: missing persist calls during `startElection` and `becomeFollower`, the invariant that `votedFor` is preserved when a follower receives a same-term AppendEntries (so it does not grant two votes in the same term), and that stale vote replies from a previous term are ignored.

### Log Replication (`raft_test.go`)

Commit of a single command, commit of multiple commands, commit across leader disconnections and reconnections, the guarantee that no commit happens without a quorum, correct handling of submissions to followers (they must be rejected), log truncation when a follower has diverging entries from an old leader, commit after a burst of call drops, and crash-then-restart scenarios covering followers, leaders, and full-cluster restarts.

### Snapshots (`snapshot_test.go`)

Basic snapshot installation by the leader's application layer, the InstallSnapshot RPC path triggered by a follower that was crashed before entries were committed (so its `nextIndex` falls behind the snapshot boundary), correct persistence of snapshot state across a process restart, the guarantee that a follower that is already caught up does not receive an unnecessary InstallSnapshot, snapshot delivery after a leader change, and a concurrent-submit-and-compact stress test that verifies log index monotonicity is preserved when compaction races with new submissions.

### Membership Changes (`membership_test.go`)

Growing a 3-node cluster to 4, shrinking a 3-node cluster to 2, removing the current leader (triggering a forced step-down and re-election), a new server catching up on a long historical log before participating in quorum, an add-then-remove cycle that returns to the original topology, log consistency verification across a full add/remove cycle (all global indices must be monotone, no entries lost), adding a server while one existing follower is partitioned, removing a server while another is partitioned, the single-pending-change invariant (a second proposal is rejected while one is in flight), rejection of `AddPeer` and `RemovePeer` on followers, rejection of a duplicate `AddPeer` for an existing member, rejection of `RemovePeer` for a non-existent member, config entry delivery on the commit channel, growing a single-node cluster to two nodes, and persistence of the updated membership across a full cluster restart.

---

## Implementation Notes

**Global log indices.** After a snapshot, the in-memory log slice starts at a non-zero position. A `logOffset` field records the global index of `log[0]`. All external-facing indices (commit index, match index, prev-log index in RPCs) use global coordinates; arithmetic helpers convert to and from local slice positions. This means the rest of the code never needs to know whether a snapshot has been taken.

**Commit channel sender.** Delivering committed entries to the application is done by a dedicated goroutine that wakes on a buffered notification channel. The channel carries no data — only the signal that `commitIndex` has advanced. The sender then locks the module, copies the newly committed entries, unlocks, and sends them one by one. This decouples the critical path (processing AppendEntries replies and advancing `commitIndex`) from the potentially slow application layer.

**Membership changes and quorum.** When an `AddNode` entry commits, the new peer is added to `peerIds` on every node. From that point forward the new node's acknowledgement is required to advance `commitIndex`. Before the entry commits, the leader replicates to the new node (because it initialised `nextIndex` for it when the entry was appended) but does not count its acknowledgement toward the majority. This avoids the double-majority problem of joint consensus while still allowing the new node to catch up before it is counted.

**Leader removal.** When the removed node is the leader, the config change commits via the remaining nodes' acknowledgements. On processing the commit, the leader calls `becomeFollower`, which starts an election timer on the remaining nodes. The removed leader eventually stops receiving heartbeats from a peer, so it will also time out — but since it is no longer in anyone's `peerIds`, its vote requests will be ignored and it will never win.
