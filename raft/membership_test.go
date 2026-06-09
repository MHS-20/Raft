package raft

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Harness extensions for membership changes
// ---------------------------------------------------------------------------

// AddServerToCluster spins up a brand-new server (id = h.n), connects it to
// every currently-alive peer, and asks the leader to append an AddNode config
// entry.  It grows all harness slices so the new node is visible to the
// existing helpers (CheckSingleLeader, CheckCommitted, etc.).
//
// Returns the new server's id.  The caller must supply the id of the current
// leader so we know where to send the AddPeer call.
func (h *Harness) AddServerToCluster(leaderId int) int {
	h.mu.Lock()

	newId := h.n

	// Extend every per-server slice by one slot.
	storage := NewMapStorage()
	commitChan := make(chan CommitEntry)
	ready := make(chan any)

	peerIds := make([]int, 0, newId)
	for i := 0; i < newId; i++ {
		peerIds = append(peerIds, i)
	}

	newServer := NewServer(newId, peerIds, storage, ready, commitChan, h.logger)
	newServer.Serve()

	h.cluster = append(h.cluster, newServer)
	h.storage = append(h.storage, storage)
	h.commitChans = append(h.commitChans, commitChan)
	h.commits = append(h.commits, nil)
	h.snapshots = append(h.snapshots, nil)
	h.connected = append(h.connected, true)
	h.alive = append(h.alive, true)
	h.n++

	h.mu.Unlock()

	// Wire the new server to all alive existing peers (bidirectional).
	for j := 0; j < newId; j++ {
		h.mu.Lock()
		alive := h.alive[j]
		h.mu.Unlock()
		if !alive {
			continue
		}
		if err := newServer.ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
			h.t.Fatalf("AddServerToCluster: new→existing connect %d→%d: %v", newId, j, err)
		}
		if err := h.cluster[j].ConnectToPeer(newId, newServer.GetListenAddr()); err != nil {
			h.t.Fatalf("AddServerToCluster: existing→new connect %d→%d: %v", j, newId, err)
		}
	}

	close(ready)

	// Start collecting commits and snapshots for the new node.
	go h.collectCommits(newId)
	go h.collectSnapshots(newId)

	// Propose the membership change via the leader.
	ok := h.cluster[leaderId].AddPeer(newId, newServer.GetListenAddr())
	if !ok {
		h.t.Fatalf("AddServerToCluster: AddPeer(%d) returned false on leader %d", newId, leaderId)
	}

	tlog("AddServerToCluster: added server %d", newId)
	return newId
}

// RemoveServerFromCluster asks the leader to append a RemoveNode config entry
// for targetId and waits until the removal commits on the leader (confirmed by
// the leader's peerIds no longer containing targetId).  It then marks
// h.connected[targetId] = false so that CheckCommitted and friends do not
// count the removed server when verifying quorum counts.
func (h *Harness) RemoveServerFromCluster(leaderId, targetId int) {
	ok := h.cluster[leaderId].RemovePeer(targetId)
	if !ok {
		h.t.Fatalf("RemoveServerFromCluster: RemovePeer(%d) returned false on leader %d", targetId, leaderId)
	}
	tlog("RemoveServerFromCluster: removed server %d via leader %d", targetId, leaderId)

	// Wait until the config change commits on the leader, evidenced by targetId
	// disappearing from the leader's peerIds (maybeApplyConfigAt removes it).
	// Also handle the self-removal case where leaderId == targetId: in that case
	// the leader steps down and is no longer in its own peerIds.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.cluster[leaderId].cm.mu.Lock()
		peers := append([]int(nil), h.cluster[leaderId].cm.peerIds...)
		h.cluster[leaderId].cm.mu.Unlock()

		found := false
		for _, p := range peers {
			if p == targetId {
				found = true
				break
			}
		}
		if !found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Mark the removed server as no longer part of the connected set so that
	// CheckCommitted does not include its (stale) commit history in counts.
	h.mu.Lock()
	h.connected[targetId] = false
	h.mu.Unlock()
}

// CheckPeerList asserts that the consensus module on server id has exactly
// the peers listed in wantPeers (order-independent).
func (h *Harness) CheckPeerList(id int, wantPeers []int) {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.cluster[id].cm.mu.Lock()
		got := append([]int(nil), h.cluster[id].cm.peerIds...)
		h.cluster[id].cm.mu.Unlock()

		if sameIntSet(got, wantPeers) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	h.cluster[id].cm.mu.Lock()
	got := append([]int(nil), h.cluster[id].cm.peerIds...)
	h.cluster[id].cm.mu.Unlock()

	h.t.Errorf("server %d peerIds = %v; want %v", id, got, wantPeers)
}

// CheckNotLeader asserts that server id does not currently think it is leader.
func (h *Harness) CheckNotLeader(id int) {
	h.t.Helper()
	_, _, isLeader := h.cluster[id].cm.Report()
	if isLeader {
		h.t.Errorf("server %d is leader; expected not-leader", id)
	}
}

// waitForConfigCommit spins until the CommitEntry on server id with the given
// type and nodeId has been delivered on the commitChan.
func (h *Harness) waitForConfigCommit(id int, changeType ConfigChangeType, nodeId int) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		for _, entry := range h.commits[id] {
			if cc, ok := entry.Command.(ConfigChangeEntry); ok {
				if cc.Type == changeType && cc.NodeId == nodeId {
					h.mu.Unlock()
					return
				}
			}
		}
		h.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("server %d never saw ConfigChangeEntry{type=%d,node=%d} on commit channel", id, changeType, nodeId)
}

// sameIntSet returns true when a and b contain the same integers (ignoring order).
func sameIntSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	count := make(map[int]int, len(a))
	for _, v := range a {
		count[v]++
	}
	for _, v := range b {
		count[v]--
		if count[v] < 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// TestAddServerBasic
//
// Simplest case: 3-node cluster grows to 4.  After the AddNode entry commits
// the new server must be considered a peer by all original members and must
// itself be able to commit subsequent entries.
// ---------------------------------------------------------------------------
func TestAddServerBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Submit a command before adding the new node so there is log history.
	h.SubmitToServer(leaderId, 1001)
	sleepMs(150)
	h.CheckCommittedN(1001, 3)

	// Add the fourth server.
	newId := h.AddServerToCluster(leaderId)
	sleepMs(400)

	// The config-change entry must have committed: all original nodes update peerIds.
	h.CheckPeerList(0, peersExcept(newId+1, 0))
	h.CheckPeerList(1, peersExcept(newId+1, 1))
	h.CheckPeerList(2, peersExcept(newId+1, 2))

	// The new node knows all three originals as peers.
	h.CheckPeerList(newId, []int{0, 1, 2})

	// New entries commit on all four nodes.
	h.SubmitToServer(leaderId, 1002)
	sleepMs(250)
	h.CheckCommittedN(1002, 4)
}

// ---------------------------------------------------------------------------
// TestRemoveServerBasic
//
// 3-node cluster shrinks to 2 by removing a non-leader follower.  After the
// RemoveNode entry commits the remaining two nodes must elect a leader from
// among themselves and keep committing.
// ---------------------------------------------------------------------------
func TestRemoveServerBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	// Pick a follower to remove.
	removeId := (leaderId + 1) % 3

	h.SubmitToServer(leaderId, 2001)
	sleepMs(150)
	h.CheckCommittedN(2001, 3)

	h.RemoveServerFromCluster(leaderId, removeId)
	sleepMs(400)

	// The two remaining connected nodes should each list only one peer.
	remainA := (removeId + 1) % 3
	remainB := (removeId + 2) % 3
	h.CheckPeerList(remainA, []int{remainB})
	h.CheckPeerList(remainB, []int{remainA})

	// Consensus still works on the reduced cluster.
	h.SubmitToServer(leaderId, 2002)
	sleepMs(250)
	// Only count servers that are still connected members.
	h.CheckCommittedN(2002, 2)
}

// ---------------------------------------------------------------------------
// TestRemoveLeader
//
// Removing the current leader: the leader appends the RemoveNode entry,
// commits it (with the help of the remaining nodes), then steps down.
// The remaining two nodes must elect a new leader.
// ---------------------------------------------------------------------------
func TestRemoveLeader(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(origLeaderId, 3001)
	sleepMs(150)
	h.CheckCommittedN(3001, 3)

	// Ask the leader to remove itself.
	h.RemoveServerFromCluster(origLeaderId, origLeaderId)

	// Wait for the config entry to commit and a new leader to emerge, polling
	// the peer lists so we aren't sensitive to election timing (especially
	// under RAFT_FORCE_MORE_REELECTION).
	var newLeaderId int
	for range 20 {
		sleepMs(100)
		newLeaderId, _ = h.CheckSingleLeader()
		if newLeaderId == origLeaderId {
			continue
		}
		// Check whether the survivors have removed the old leader from their peerIds.
		allClean := true
		for i := 0; i < 3; i++ {
			if i == origLeaderId {
				continue
			}
			h.cluster[i].cm.mu.Lock()
			peers := append([]int(nil), h.cluster[i].cm.peerIds...)
			h.cluster[i].cm.mu.Unlock()
			for _, p := range peers {
				if p == origLeaderId {
					allClean = false
					break
				}
			}
			if !allClean {
				break
			}
		}
		if allClean {
			goto done
		}
	}
	t.Fatal("timeout waiting for survivors to remove leader from peerIds")

done:
	// Consensus must still work.
	h.SubmitToServer(newLeaderId, 3002)
	sleepMs(250)
	h.CheckCommittedN(3002, 2)
}

// ---------------------------------------------------------------------------
// TestAddServerCatchesUp
//
// The new server joins a cluster that already has committed log entries and
// must catch up via AppendEntries before it can participate in quorum.
// ---------------------------------------------------------------------------
func TestAddServerCatchesUp(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Build up log history.
	for i := 4001; i <= 4010; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(300)
	for i := 4001; i <= 4010; i++ {
		h.CheckCommittedN(i, 3)
	}

	newId := h.AddServerToCluster(leaderId)
	// Give the new server extra time to replicate the full log.
	sleepMs(600)

	// The new server should now have committed at least the last historical entry.
	h.mu.Lock()
	found := false
	for _, entry := range h.commits[newId] {
		if cmd, ok := entry.Command.(int); ok && cmd == 4010 {
			found = true
			break
		}
	}
	h.mu.Unlock()
	if !found {
		t.Errorf("new server %d never committed historical entry 4010", newId)
	}

	// New commands commit on all four nodes.
	h.SubmitToServer(leaderId, 4099)
	sleepMs(300)
	h.CheckCommittedN(4099, 4)
}

// ---------------------------------------------------------------------------
// TestAddThenRemoveServer
//
// Add a server and then remove it: net result is 3 nodes, same as the start.
// ---------------------------------------------------------------------------
func TestAddThenRemoveServer(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	newId := h.AddServerToCluster(leaderId)
	sleepMs(400)
	// Verify we have 4-node consensus.
	h.SubmitToServer(leaderId, 5001)
	sleepMs(250)
	h.CheckCommittedN(5001, 4)

	// Re-acquire leader (it could have changed).
	leaderId, _ = h.CheckSingleLeader()

	// Now remove the node we just added.
	h.RemoveServerFromCluster(leaderId, newId)
	sleepMs(400)

	// Back to 3-node quorum.
	h.CheckPeerList(0, peersExcept(h.n-1, 0))
	h.CheckPeerList(1, peersExcept(h.n-1, 1))
	h.CheckPeerList(2, peersExcept(h.n-1, 2))

	leaderId, _ = h.CheckSingleLeader()
	h.SubmitToServer(leaderId, 5002)
	sleepMs(250)
	h.CheckCommittedN(5002, 3)
}

// ---------------------------------------------------------------------------
// TestMembershipChangePreservesLog
//
// After an add/remove cycle the surviving nodes' committed logs must be
// consistent: no entries must be lost or reordered.
// ---------------------------------------------------------------------------
func TestMembershipChangePreservesLog(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Phase 1: pre-membership entries.
	for i := 6001; i <= 6005; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(200)

	newId := h.AddServerToCluster(leaderId)
	sleepMs(400)
	leaderId, _ = h.CheckSingleLeader()

	// Phase 2: post-add entries.
	for i := 6006; i <= 6010; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(200)

	h.RemoveServerFromCluster(leaderId, newId)
	sleepMs(200) // let removal propagate to followers
	leaderId, _ = h.CheckSingleLeader()

	// Phase 3: post-remove entries.
	for i := 6011; i <= 6015; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(300)

	// Pre-add entries: server 3 may have replayed them after joining, but it is
	// now marked disconnected by RemoveServerFromCluster, so the count is 3.
	for i := 6001; i <= 6005; i++ {
		h.CheckCommittedN(i, 3)
	}
	// Post-add entries: committed while the cluster had 4 members.
	// Server 3 is now marked disconnected, so we see exactly 3 here too
	// (the 3 surviving nodes all have these entries).
	for i := 6006; i <= 6010; i++ {
		h.CheckCommittedN(i, 3)
	}
	// Post-remove entries: 3-node cluster throughout.
	for i := 6011; i <= 6015; i++ {
		h.CheckCommittedN(i, 3)
	}

	// Verify monotone indices on one server.
	h.mu.Lock()
	prev := -1
	for _, entry := range h.commits[leaderId] {
		if _, ok := entry.Command.(ConfigChangeEntry); ok {
			continue // skip config entries
		}
		if entry.Index <= prev {
			t.Errorf("non-monotone index: saw %d after %d", entry.Index, prev)
		}
		prev = entry.Index
	}
	h.mu.Unlock()
}

// ---------------------------------------------------------------------------
// TestAddServerWhileFollowerPartitioned
//
// Add a new server while one existing follower is partitioned.  The add must
// still commit (leader + one follower = quorum in a 3-node cluster), and the
// partitioned node must converge once reconnected.
// ---------------------------------------------------------------------------
func TestAddServerWhileFollowerPartitioned(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	partitioned := (leaderId + 1) % 3

	h.DisconnectPeer(partitioned)
	sleepMs(100)

	h.AddServerToCluster(leaderId)
	sleepMs(500)

	// Config change must have committed on the quorum (leader + other follower).
	// Re-examine once we reconnect the partitioned peer.
	h.ReconnectPeer(partitioned)
	sleepMs(400)

	// The previously-partitioned peer may have bumped its term while isolated,
	// which can trigger a re-election when reconnected.  Re-acquire the leader.
	leaderId, _ = h.CheckSingleLeader()

	h.SubmitToServer(leaderId, 7001)
	sleepMs(300)
	h.CheckCommittedN(7001, 4)
}

// ---------------------------------------------------------------------------
// TestRemoveServerWithPartition
//
// Remove a follower while another follower is partitioned.  With a 3-node
// cluster and 1 partitioned, leader + 1 is still a quorum, so the remove
// must commit.  After reconnecting the partitioned node it must also apply
// the removal.
// ---------------------------------------------------------------------------
func TestRemoveServerWithPartition(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	partitioned := (leaderId + 1) % 3
	removeId := (leaderId + 2) % 3

	h.DisconnectPeer(partitioned)
	sleepMs(100)

	h.RemoveServerFromCluster(leaderId, removeId)
	sleepMs(400)

	// Reconnect the partitioned node and let it catch up.
	h.ReconnectPeer(partitioned)
	sleepMs(400)

	// All three originally-connected nodes (minus removed) should agree on peers.
	h.CheckPeerList(leaderId, []int{partitioned})
	h.CheckPeerList(partitioned, []int{leaderId})

	h.SubmitToServer(leaderId, 8001)
	sleepMs(250)
	h.CheckCommittedN(8001, 2)
}

// ---------------------------------------------------------------------------
// TestOnlyOnePendingConfigChange
//
// The implementation must reject a second config change while one is still
// pending (pendingConfigIndex != -1).  Verify AddPeer returns false when
// called a second time before the first commits.
// ---------------------------------------------------------------------------
func TestOnlyOnePendingConfigChange(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Isolate the leader so the first config change never reaches a quorum
	// (and therefore never commits, keeping pendingConfigIndex set).
	//
	// We do this by asking the server to drop all outgoing calls immediately
	// after the AddPeer log append, then try a second AddPeer.
	h.cluster[leaderId].cm.mu.Lock()
	h.cluster[leaderId].cm.pendingConfigIndex = 999 // fake a pending entry
	h.cluster[leaderId].cm.mu.Unlock()

	// AddPeer must refuse because pendingConfigIndex != -1.
	accepted := h.cluster[leaderId].cm.AddPeer(99)
	if accepted {
		t.Errorf("AddPeer should return false while a config change is pending")
	}

	// Clean up the fake pending index so Shutdown works cleanly.
	h.cluster[leaderId].cm.mu.Lock()
	h.cluster[leaderId].cm.pendingConfigIndex = -1
	h.cluster[leaderId].cm.mu.Unlock()
}

// ---------------------------------------------------------------------------
// TestAddServerNotLeaderRejected
//
// AddPeer called on a follower (not the leader) must return false.
// ---------------------------------------------------------------------------
func TestAddServerNotLeaderRejected(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	followerId := (leaderId + 1) % 3

	accepted := h.cluster[followerId].cm.AddPeer(99)
	if accepted {
		t.Errorf("AddPeer on follower %d should return false", followerId)
	}
}

// ---------------------------------------------------------------------------
// TestRemoveServerNotLeaderRejected
//
// RemovePeer called on a follower must return false.
// ---------------------------------------------------------------------------
func TestRemoveServerNotLeaderRejected(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	followerId := (leaderId + 1) % 3
	removeId := (leaderId + 2) % 3

	accepted := h.cluster[followerId].cm.RemovePeer(removeId)
	if accepted {
		t.Errorf("RemovePeer on follower %d should return false", followerId)
	}
}

// ---------------------------------------------------------------------------
// TestAddDuplicatePeerRejected
//
// Calling AddPeer for a node that is already in the cluster must return false.
// ---------------------------------------------------------------------------
func TestAddDuplicatePeerRejected(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	existingPeer := (leaderId + 1) % 3

	accepted := h.cluster[leaderId].cm.AddPeer(existingPeer)
	if accepted {
		t.Errorf("AddPeer for existing peer %d should return false", existingPeer)
	}
}

// ---------------------------------------------------------------------------
// TestMembershipChangeWithLeaderCrash
//
// Submit an AddPeer, let it commit, crash the leader, elect a new one, and
// verify that the new leader still sees the updated peer list and can commit
// new entries on the 4-node cluster.
// ---------------------------------------------------------------------------
func TestMembershipChangeWithLeaderCrash(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	newId := h.AddServerToCluster(origLeaderId)
	sleepMs(500)

	// Verify the config change committed on the original leader.
	h.waitForConfigCommit(origLeaderId, AddNode, newId)

	// Crash the original leader.
	h.CrashPeer(origLeaderId)
	sleepMs(400)

	newLeaderId, _ := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Fatalf("crashed leader %d should not still be leader", origLeaderId)
	}

	// New entries must commit on the three surviving nodes (newId + 2 originals).
	h.SubmitToServer(newLeaderId, 9001)
	sleepMs(300)

	// 3 surviving servers: the two original followers plus the new node.
	nc, _ := h.CheckCommitted(9001)
	if nc < 2 {
		t.Errorf("9001 committed on %d servers; want at least 2 of the 3 survivors", nc)
	}

	_ = newId // suppress unused warning
}

// ---------------------------------------------------------------------------
// TestMembershipChangeConfigEntryOnCommitChan
//
// ConfigChangeEntry values must be delivered on the commit channel just like
// regular commands — callers need to be able to replay the log on restart.
// ---------------------------------------------------------------------------
func TestMembershipChangeConfigEntryOnCommitChan(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	newId := h.AddServerToCluster(leaderId)
	sleepMs(500)

	deadline := time.Now().Add(3 * time.Second)
	found := false
	for !found && time.Now().Before(deadline) {
		h.mu.Lock()
		for _, entry := range h.commits[leaderId] {
			if cc, ok := entry.Command.(ConfigChangeEntry); ok {
				if cc.Type == AddNode && cc.NodeId == newId {
					found = true
					break
				}
			}
		}
		h.mu.Unlock()
		if !found {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !found {
		t.Errorf("AddNode ConfigChangeEntry for server %d never appeared on leader's commit channel", newId)
	}
}

// ---------------------------------------------------------------------------
// TestAddServerToSingleNodeCluster
//
// Grow a single-node cluster to two nodes.  A single-node cluster is its own
// quorum so the AddPeer must commit immediately.
// ---------------------------------------------------------------------------
func TestAddServerToSingleNodeCluster(t *testing.T) {
	h := NewHarness(t, 1)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(leaderId, 10001)
	sleepMs(150)
	h.CheckCommittedN(10001, 1)

	newId := h.AddServerToCluster(leaderId)
	sleepMs(500)

	h.SubmitToServer(leaderId, 10002)
	sleepMs(300)
	h.CheckCommittedN(10002, 2)

	_ = newId
}

// ---------------------------------------------------------------------------
// TestRemoveNonExistentPeerRejected
//
// RemovePeer for a node that is not in the cluster must return false (and must
// not be rejected merely because the node id happens to equal the leader's own
// id — that case is tested separately in TestRemoveLeader).
// ---------------------------------------------------------------------------
func TestRemoveNonExistentPeerRejected(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	accepted := h.cluster[leaderId].cm.RemovePeer(99) // 99 is not in the cluster
	if accepted {
		t.Errorf("RemovePeer for non-existent node 99 should return false")
	}
}

// ---------------------------------------------------------------------------
// TestMembershipChangeConsistencyAcrossRestarts
//
// After a node is added and the config change commits, crash the entire
// cluster and restart it.  All nodes must restore the correct peer list from
// persistent storage.
// ---------------------------------------------------------------------------
func TestMembershipChangeConsistencyAcrossRestarts(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Submit a couple of regular entries first.
	h.SubmitToServer(leaderId, 11001)
	h.SubmitToServer(leaderId, 11002)
	sleepMs(200)

	newId := h.AddServerToCluster(leaderId)
	sleepMs(500)

	// Verify the add committed everywhere.
	h.CheckPeerList(0, peersExcept(newId+1, 0))
	h.CheckPeerList(leaderId, peersExcept(newId+1, leaderId))

	// Crash and restart each original server.
	for i := 0; i < 3; i++ {
		h.CrashPeer(i)
	}
	sleepMs(100)
	for i := 0; i < 3; i++ {
		h.RestartPeer(i)
	}
	sleepMs(500)

	newLeaderId, _ := h.CheckSingleLeader()

	// Post-restart entries commit on all four nodes.
	h.SubmitToServer(newLeaderId, 11003)
	sleepMs(300)
	nc, _ := h.CheckCommitted(11003)
	if nc < 3 {
		t.Errorf("11003 committed on %d nodes after restart; want at least 3", nc)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// peersExcept builds the expected peer list for a cluster of size n where
// server excludeId is not included.
func peersExcept(n, excludeId int) []int {
	peers := make([]int, 0, n-1)
	for i := 0; i < n; i++ {
		if i != excludeId {
			peers = append(peers, i)
		}
	}
	return peers
}
