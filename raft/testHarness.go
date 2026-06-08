package raft

import (
	"log"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

type Harness struct {
	mu sync.Mutex

	// cluster is a list of all the raft servers participating in a cluster.
	cluster []*Server
	storage []*MapStorage

	// commitChans has a channel per server in cluster with the commit channel for
	// that server.
	commitChans []chan CommitEntry

	// commits at index i holds the sequence of commits made by server i so far.
	// It is populated by goroutines that listen on the corresponding commitChans
	// channel.
	commits [][]CommitEntry

	// snapshots at index i holds all SnapshotEntry values delivered to server i
	// via InstallSnapshotRPC (i.e. snapshots received from the leader, not ones
	// the application itself triggered).  Protected by mu.
	snapshots [][]SnapshotEntry

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	// alive has a bool per server in cluster, specifying whether this server is
	// currently alive (false means it has crashed and wasn't restarted yet).
	// connected implies alive.
	alive []bool

	n      int
	t      *testing.T
	logger *slog.Logger
}

// NewHarness creates a new test Harness, initialized with n servers connected
// to each other.
func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	snapshots := make([][]SnapshotEntry, n)
	ready := make(chan any)
	storage := make([]*MapStorage, n)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create all Servers in this cluster, assign ids and peer ids.
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		storage[i] = NewMapStorage()
		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewServer(i, peerIds, storage[i], ready, commitChans[i], logger)
		ns[i].Serve()
		alive[i] = true
	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	h := &Harness{
		cluster:     ns,
		storage:     storage,
		commitChans: commitChans,
		commits:     commits,
		snapshots:   snapshots,
		connected:   connected,
		alive:       alive,
		n:           n,
		t:           t,
		logger:      logger,
	}
	for i := 0; i < n; i++ {
		go h.collectCommits(i)
		go h.collectSnapshots(i)
	}
	return h
}

// Shutdown shuts down all the servers in the harness and waits for them to
// stop running.
func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			h.cluster[i].Shutdown()
		}
	}
	for i := 0; i < h.n; i++ {
		close(h.commitChans[i])
	}
}

// DisconnectPeer disconnects a server from all other servers in the cluster.
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

// ReconnectPeer connects a server to all other servers in the cluster.
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// CrashPeer "crashes" a server by disconnecting it from all peers and then
// asking it to shut down. We're not going to use the same server instance
// again, but its storage is retained.
func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	h.DisconnectPeer(id)
	h.alive[id] = false
	h.cluster[id].Shutdown()

	// Clear out the commits slice for the crashed server; Raft assumes the client
	// has no persistent state. Once this server comes back online it will replay
	// the whole log to us.
	h.mu.Lock()
	h.commits[id] = h.commits[id][:0]
	h.snapshots[id] = h.snapshots[id][:0]
	h.mu.Unlock()
}

// RestartPeer "restarts" a server by creating a new Server instance and giving
// it the appropriate storage, reconnecting it to peers.
func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan any)

	h.mu.Lock()
	// Initialize the server under the harness lock
	h.cluster[id] = NewServer(id, peerIds, h.storage[id], ready, h.commitChans[id], h.logger)
	h.mu.Unlock()

	h.cluster[id].Serve()
	h.ReconnectPeer(id)
	close(ready)

	h.mu.Lock()
	h.alive[id] = true
	h.mu.Unlock()

	// Spawn a fresh collectSnapshots loop explicitly bound to this specific module instance
	go h.collectSnapshots(id)
	sleepMs(20)
}

// PeerDropCallsAfterN instructs peer `id` to drop calls after the next `n`
// are made.
func (h *Harness) PeerDropCallsAfterN(id int, n int) {
	tlog("peer %d drop calls after %d", id, n)
	h.cluster[id].Proxy().DropCallsAfterN(n)
}

// PeerDontDropCalls instructs peer `id` to stop dropping calls.
func (h *Harness) PeerDontDropCalls(id int) {
	tlog("peer %d don't drop calls")
	h.cluster[id].Proxy().DontDropCalls()
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (h *Harness) CheckSingleLeader() (int, int) {
	for r := 0; r < 8; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].cm.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

// CheckNoLeader checks that no connected server considers itself the leader.
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].cm.Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

// CheckCommitted verifies that all connected servers have cmd committed with
// the same index. It also verifies that all commands *before* cmd in
// the commit sequence match. For this to work properly, all commands submitted
// to Raft should be unique positive ints.
// Returns the number of servers that have this command committed, and its
// log index.
func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	// Build per-server slices containing only integer (application) commands,
	// filtering out ConfigChangeEntry values that also flow through the commit
	// channel.  This keeps CheckCommitted working correctly in membership tests
	// where config entries appear at different positions across servers.
	type intCommit struct {
		cmd   int
		index int
	}
	appCommits := make([][]intCommit, h.n)
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for _, e := range h.commits[i] {
				if cmd, ok := e.Command.(int); ok {
					appCommits[i] = append(appCommits[i], intCommit{cmd, e.Index})
				}
			}
		}
	}

	// All connected servers must have committed the same number of integer
	// entries in the same order up to the point where cmd appears.
	appLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if appLen >= 0 {
				if len(appCommits[i]) != appLen {
					h.t.Fatalf("app-command commit length mismatch: server %d has %d, expected %d",
						i, len(appCommits[i]), appLen)
				}
			} else {
				appLen = len(appCommits[i])
			}
		}
	}

	// Walk the filtered log looking for cmd, checking consistency as we go.
	for c := 0; c < appLen; c++ {
		cmdAtC := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cmdOfN := appCommits[i][c].cmd
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						h.t.Errorf("app-commit mismatch at position %d: server %d has %d, want %d",
							c, i, cmdOfN, cmdAtC)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			// Verify all connected servers agree on the global index.
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					idx := appCommits[i][c].index
					if index >= 0 && idx != index {
						h.t.Errorf("index mismatch for cmd=%d: server %d has index %d, want %d",
							cmd, i, idx, index)
					} else {
						index = idx
					}
					nc++
				}
			}
			return nc, index
		}
	}

	// cmd not found in any connected server's filtered commit log.
	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

// CheckCommittedN verifies that cmd was committed by exactly n connected
// servers.
func (h *Harness) CheckCommittedN(cmd int, n int) {
	h.t.Helper()
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// CheckCommittedAtLeastN verifies that cmd was committed by at least n
// connected servers.  Use this when a newly-joined server may have replayed
// historical entries, making the exact count uncertain.
func (h *Harness) CheckCommittedAtLeastN(cmd int, n int) {
	h.t.Helper()
	nc, _ := h.CheckCommitted(cmd)
	if nc < n {
		h.t.Errorf("CheckCommittedAtLeastN got nc=%d, want >= %d", nc, n)
	}
}

// CheckNotCommitted verifies that no command equal to cmd has been committed
// by any of the active servers yet.
func (h *Harness) CheckNotCommitted(cmd int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for c := 0; c < len(h.commits[i]); c++ {
				gotCmd, ok := h.commits[i][c].Command.(int)
				if !ok {
					continue // skip ConfigChangeEntry and other non-int commands
				}
				if gotCmd == cmd {
					h.t.Errorf("found %d at commits[%d][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

// SubmitToServer submits the command to serverId.
func (h *Harness) SubmitToServer(serverId int, cmd any) int {
	return h.cluster[serverId].Submit(cmd).Index
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

// collectCommits reads channel commitChans[i] and adds all received entries
// to the corresponding commits[i]. It's blocking and should be run in a
// separate goroutine. It returns when commitChans[i] is closed.
func (h *Harness) collectCommits(i int) {
	// Capture the channel under the lock so we never race with AddServerToCluster
	// growing h.commitChans (append may reallocate the backing array, making a
	// concurrent bare read of h.commitChans[i] a data race).
	h.mu.Lock()
	ch := h.commitChans[i]
	h.mu.Unlock()

	for c := range ch {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}

// collectSnapshots drains the SnapshotReady() channel for server i and
// records every snapshot delivered by the Raft layer (i.e. from
// InstallSnapshotRPC). It exits when snapshotDone is closed by Stop().
func (h *Harness) collectSnapshots(i int) {
	// Capture the specific CM reference when the goroutine starts
	h.mu.Lock()
	cm := h.cluster[i].cm
	h.mu.Unlock()

	for {
		select {
		case snap := <-cm.SnapshotReady():
			h.mu.Lock()
			// Only record if this is still the active cluster node component
			if h.cluster[i].cm == cm {
				tlog("collectSnapshots(%d) got snapshot index=%d term=%d", i, snap.Index, snap.Term)
				h.snapshots[i] = append(h.snapshots[i], snap)
			}
			h.mu.Unlock()
		case <-cm.SnapshotDone():
			return
		}
	}
} // CheckSnapshotDelivered waits until server id has received at least one

// snapshot whose Index equals wantIndex, then returns that snapshot.
// It fails the test if no such snapshot arrives within ~5 seconds.
func (h *Harness) CheckSnapshotDelivered(id int, wantIndex int) SnapshotEntry {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		for _, snap := range h.snapshots[id] {
			if snap.Index == wantIndex {
				h.mu.Unlock()
				return snap
			}
		}
		h.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("server %d never received snapshot at index %d", id, wantIndex)
	return SnapshotEntry{}
}

// CheckNoSnapshotDelivered asserts that server id has received no snapshots yet.
func (h *Harness) CheckNoSnapshotDelivered(id int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.snapshots[id]) != 0 {
		h.t.Errorf("server %d has %d snapshot(s), expected none", id, len(h.snapshots[id]))
	}
}

// CheckCommittedIgnoringSnapshot is like CheckCommitted but does NOT require
// that all connected servers have the same length commits slice.  This is
// needed after a snapshot: a restarted server will have had its log replaced
// by the snapshot and will only report the post-snapshot commits, while
// servers that were never restarted show the full history.
//
// Instead of a length equality check it simply looks for cmd in each
// connected server's commits slice and verifies that wherever it appears it
// has the same index.
func (h *Harness) CheckCommittedIgnoringSnapshot(cmd int) (nc int, index int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	index = -1
	nc = 0
	for i := 0; i < h.n; i++ {
		if !h.connected[i] {
			continue
		}
		for _, entry := range h.commits[i] {
			intCmd, ok := entry.Command.(int)
			if !ok {
				continue // skip ConfigChangeEntry and other non-int commands
			}
			if intCmd == cmd {
				if index >= 0 && entry.Index != index {
					h.t.Errorf("server %d committed cmd=%d at index=%d, want index=%d",
						i, cmd, entry.Index, index)
				}
				index = entry.Index
				nc++
				break
			}
		}
	}
	if nc == 0 {
		h.t.Errorf("cmd=%d not found in any connected server's commits", cmd)
	}
	return nc, index
}
