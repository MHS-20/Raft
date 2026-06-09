package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"
)

// encodeSnapshot gob-encodes an integer slice into a byte slice so we have a
// realistic, round-trippable snapshot payload in tests.
func encodeSnapshot(t *testing.T, data []int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		t.Fatalf("encodeSnapshot: %v", err)
	}
	return buf.Bytes()
}

// decodeSnapshot reverses encodeSnapshot.
func decodeSnapshot(t *testing.T, b []byte) []int {
	t.Helper()
	var data []int
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&data); err != nil {
		t.Fatalf("decodeSnapshot: %v", err)
	}
	return data
}

// TestSnapshotBasic verifies that calling InstallSnapshot on the leader (as
// the application layer would after deciding to compact) trims the in-memory
// log and updates the snapshot metadata correctly, without affecting running
// consensus.
func TestSnapshotBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Submit 4 commands and wait for them all to commit.
	for i := 1; i <= 4; i++ {
		h.SubmitToServer(leaderId, i*100)
	}
	sleepMs(250)
	for i := 1; i <= 4; i++ {
		h.CheckCommittedN(i*100, 3)
	}

	// Snapshot at index 3 (the 4th entry, 0-based global index).
	// The application tells the leader "I have compacted everything up to index 3."
	snapData := encodeSnapshot(t, []int{100, 200, 300, 400})
	h.cluster[leaderId].cm.InstallSnapshot(3, h.cluster[leaderId].cm.currentTerm, snapData)

	// The leader's in-memory log should now be empty (all entries compacted).
	h.cluster[leaderId].cm.mu.Lock()
	logLen := h.cluster[leaderId].cm.logLen()
	sli := h.cluster[leaderId].cm.snapshotLastIndex
	slt := h.cluster[leaderId].cm.snapshotLastTerm
	offset := h.cluster[leaderId].cm.logOffset
	h.cluster[leaderId].cm.mu.Unlock()

	if logLen != 0 {
		t.Errorf("after snapshot, leader log len = %d; want 0", logLen)
	}
	if sli != 3 {
		t.Errorf("snapshotLastIndex = %d; want 3", sli)
	}
	if slt <= 0 {
		t.Errorf("snapshotLastTerm = %d; want > 0", slt)
	}
	if offset != 4 {
		t.Errorf("logOffset = %d; want 4", offset)
	}

	// Consensus must still work after compaction: new command commits on all nodes.
	h.SubmitToServer(leaderId, 999)
	sleepMs(150)
	h.CheckCommittedN(999, 3)

	// No connected server should have received a snapshot via InstallSnapshotRPC
	// (no follower was behind the snapshot boundary).
	for i := range 3 {
		h.CheckNoSnapshotDelivered(i)
	}
}

// TestSnapshotRoundTrip verifies the encode/decode cycle for snapshot data
// that travels through the Raft InstallSnapshot RPC path.  We trigger the
// InstallSnapshot path by crashing a follower, compacting the leader's log
// past the follower's last-known index, and then restarting the follower.
// On restart the follower loads an empty-ish state from storage (storage has
// currentTerm and votedFor but the log entries that were below the snapshot
// boundary are gone), so when the leader sends AppendEntries the follower
// will back up nextIndex until it triggers InstallSnapshot.
//
// Concretely: the leader has snapshotLastIndex=4 and the follower's log
// (after restart from storage that also has the snapshot) starts at offset 5.
// The leader's nextIndex for that follower resets to globalLastIndex()+1=7
// on election.  The follower rejects prevLogIndex=6 (it only has index 5,6
// if it had been up, but after compact+restart from storage it has entries
// starting at offset 5).  Conflict backs nextIndex to 5 — still > 4 — so AE
// succeeds.  To guarantee InstallSnapshot we need the leader's snapshot to
// cover ALL entries the follower has after restart.  We achieve this by
// snapshotting at lastIndex BEFORE any post-crash entries and having the
// follower's storage NOT contain any entries after the snapshot point.
//
// Simplest reliable way: crash follower BEFORE any entries, compact, add
// entries, restart.  Storage then has NO log entries and snapshotLastIndex=-1
// (no snapshot in storage either, since the follower crashed before any
// snapshot).  Leader nextIndex resets to globalLastIndex()+1 = N, AppendEntries
// prevLogIndex=N-1 fails (follower has nothing), conflict backs all the way to
// conflictIndex=0, nextIndex=0 ≤ snapshotLastIndex(4) → InstallSnapshot.
func TestSnapshotRoundTrip(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()
	followerId := (leaderId + 2) % 3

	// Crash the follower before ANY entries are submitted.
	// Its storage will have term/votedFor but NO log entries and NO snapshot.
	h.CrashPeer(followerId)

	// Submit 5 entries on the remaining quorum (leader + 1 other follower).
	for i := 1; i <= 5; i++ {
		h.SubmitToServer(leaderId, i*10)
	}
	sleepMs(250)
	h.CheckCommittedN(10, 2)
	h.CheckCommittedN(50, 2)

	// Compact at index 4 (covers all 5 entries, global indices 0-4).
	snapPayload := []int{10, 20, 30, 40, 50}
	snapData := encodeSnapshot(t, snapPayload)
	h.cluster[leaderId].cm.InstallSnapshot(4, leaderTerm, snapData)

	// Submit two more entries while the follower is still down.
	h.SubmitToServer(leaderId, 601)
	h.SubmitToServer(leaderId, 602)
	sleepMs(200)

	// Restart the follower.  Its storage has currentTerm and votedFor but
	// no log entries and no snapshot → snapshotLastIndex=-1, log empty.
	// Leader nextIndex[followerId] starts at globalLastIndex()+1 = 7.
	// First heartbeat: prevLogIndex=6, follower has nothing → conflict at 0.
	// nextIndex drops to 0 ≤ snapshotLastIndex(4) → InstallSnapshot.
	h.RestartPeer(followerId)

	snap := h.CheckSnapshotDelivered(followerId, 4)

	// Verify round-trip integrity: decode and compare.
	got := decodeSnapshot(t, snap.Data)
	if len(got) != len(snapPayload) {
		t.Fatalf("decoded snapshot len=%d; want %d", len(got), len(snapPayload))
	}
	for j, v := range snapPayload {
		if got[j] != v {
			t.Errorf("decoded[%d] = %d; want %d", j, got[j], v)
		}
	}

	// Both post-snapshot commands should eventually be delivered to all 3 nodes.
	sleepMs(300)
	h.CheckCommittedIgnoringSnapshot(601)
	h.CheckCommittedIgnoringSnapshot(602)
}

// TestSnapshotInstallOnLaggingFollower tests the full leader→follower snapshot
// delivery scenario.  A follower is crashed before any entries exist, so it
// has no log in storage.  After the leader compacts and adds more entries,
// restarting the follower forces it to receive InstallSnapshotRPC because its
// nextIndex on the leader will back all the way down to 0, which is ≤
// snapshotLastIndex.
func TestSnapshotInstallOnLaggingFollower(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()
	followerId := (leaderId + 2) % 3

	// Crash the follower before any entries — storage will have no log, no snapshot.
	h.CrashPeer(followerId)

	// Phase 1: submit 5 commands committed by leader + other follower (quorum=2).
	for i := 1; i <= 5; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(250)
	h.CheckCommittedN(1, 2)
	h.CheckCommittedN(5, 2)

	// Phase 2: compact at index 4 (covers all 5 entries).
	snapData := encodeSnapshot(t, []int{1, 2, 3, 4, 5})
	h.cluster[leaderId].cm.InstallSnapshot(4, leaderTerm, snapData)

	// Phase 3: submit 2 more entries.
	h.SubmitToServer(leaderId, 101)
	h.SubmitToServer(leaderId, 102)
	sleepMs(200)
	h.CheckCommittedN(101, 2)
	h.CheckCommittedN(102, 2)

	// Phase 4: restart the follower.  It has no storage entries (it crashed
	// before anything was written), so leader must send InstallSnapshot.
	h.RestartPeer(followerId)

	// The follower must receive the snapshot at index 4 from the leader.
	snap := h.CheckSnapshotDelivered(followerId, 4)

	// Verify payload.
	got := decodeSnapshot(t, snap.Data)
	want := []int{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("decoded snapshot len=%d; want %d", len(got), len(want))
	}
	for j, v := range want {
		if got[j] != v {
			t.Errorf("decoded[%d] = %d; want %d", j, got[j], v)
		}
	}

	// After the snapshot the follower should catch up on 101 and 102.
	sleepMs(300)
	nc1, _ := h.CheckCommittedIgnoringSnapshot(101)
	nc2, _ := h.CheckCommittedIgnoringSnapshot(102)
	if nc1 != 3 {
		t.Errorf("cmd=101 committed on %d servers; want 3", nc1)
	}
	if nc2 != 3 {
		t.Errorf("cmd=102 committed on %d servers; want 3", nc2)
	}
}

// TestSnapshotPersistAcrossRestart verifies that snapshot state is durably
// persisted: after the leader installs a snapshot and then crashes and
// restarts, it still knows about the snapshot and elects successfully.
func TestSnapshotPersistAcrossRestart(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()

	// Submit 3 entries.
	for i := 1; i <= 3; i++ {
		h.SubmitToServer(leaderId, i*1000)
	}
	sleepMs(200)
	for i := 1; i <= 3; i++ {
		h.CheckCommittedN(i*1000, 3)
	}

	// Compact at index 2.
	snapData := encodeSnapshot(t, []int{1000, 2000, 3000})
	h.cluster[leaderId].cm.InstallSnapshot(2, leaderTerm, snapData)

	// Verify snapshot metadata before crash.
	h.cluster[leaderId].cm.mu.Lock()
	sliBeforeCrash := h.cluster[leaderId].cm.snapshotLastIndex
	h.cluster[leaderId].cm.mu.Unlock()
	if sliBeforeCrash != 2 {
		t.Fatalf("pre-crash snapshotLastIndex = %d; want 2", sliBeforeCrash)
	}

	// Crash and restart the leader.
	h.CrashPeer(leaderId)
	h.RestartPeer(leaderId)

	// The restarted node must still know about the snapshot.
	h.cluster[leaderId].cm.mu.Lock()
	sliAfterRestart := h.cluster[leaderId].cm.snapshotLastIndex
	h.cluster[leaderId].cm.mu.Unlock()
	if sliAfterRestart != 2 {
		t.Errorf("post-restart snapshotLastIndex = %d; want 2", sliAfterRestart)
	}

	// Cluster must still be able to elect a leader and commit new entries.
	sleepMs(300)
	newLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(newLeaderId, 9999)
	sleepMs(150)
	h.CheckCommittedIgnoringSnapshot(9999)
}

// TestSnapshotDoesNotAdvanceCommitPastLog verifies the safety property that
// InstallSnapshot on a live leader does not set commitIndex beyond what has
// actually been committed, and that subsequent entries are correctly appended
// relative to the new logOffset.
func TestSnapshotDoesNotAdvanceCommitPastLog(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()

	// Submit and commit 3 entries.
	for i := 1; i <= 3; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(200)
	for i := 1; i <= 3; i++ {
		h.CheckCommittedN(i, 3)
	}

	// Snapshot at index 2 (compact the committed entries).
	h.cluster[leaderId].cm.InstallSnapshot(2, leaderTerm, encodeSnapshot(t, []int{1, 2, 3}))

	// Submit 2 more entries — these must get their global indices correct (3, 4).
	idx1 := h.SubmitToServer(leaderId, 77)
	idx2 := h.SubmitToServer(leaderId, 88)
	sleepMs(200)

	nc1, ci1 := h.CheckCommitted(77)
	nc2, ci2 := h.CheckCommitted(88)

	if nc1 != 3 {
		t.Errorf("cmd=77 committed on %d servers; want 3", nc1)
	}
	if nc2 != 3 {
		t.Errorf("cmd=88 committed on %d servers; want 3", nc2)
	}
	if ci1 != idx1 {
		t.Errorf("cmd=77 commit index = %d; Submit returned %d", ci1, idx1)
	}
	if ci2 != idx2 {
		t.Errorf("cmd=88 commit index = %d; Submit returned %d", ci2, idx2)
	}
	// Indices must be contiguous and follow the snapshot.
	if ci1 != 3 {
		t.Errorf("first post-snapshot entry should be at global index 3, got %d", ci1)
	}
	if ci2 != 4 {
		t.Errorf("second post-snapshot entry should be at global index 4, got %d", ci2)
	}
}

// TestSnapshotDeliveredOnlyOnce verifies that a follower that is already up to
// date does NOT receive an InstallSnapshotRPC — i.e. the leader only sends
// InstallSnapshot when nextIndex has fallen behind the snapshot boundary.
func TestSnapshotDeliveredOnlyOnce(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()

	// Submit and commit 4 entries on all 3 nodes.
	for i := 1; i <= 4; i++ {
		h.SubmitToServer(leaderId, i*10)
	}
	sleepMs(250)
	for i := 1; i <= 4; i++ {
		h.CheckCommittedN(i*10, 3)
	}

	// Compact at index 3 while everyone is connected.
	h.cluster[leaderId].cm.InstallSnapshot(3, leaderTerm, encodeSnapshot(t, []int{10, 20, 30, 40}))

	// Give the leader time to send heartbeats.
	sleepMs(200)

	// No follower should have received a snapshot via RPC because both followers
	// already have nextIndex > snapshotLastIndex(3) — they were fully caught up.
	for i := range 3 {
		if i == leaderId {
			continue
		}
		h.CheckNoSnapshotDelivered(i)
	}

	// New entries still commit cleanly.
	h.SubmitToServer(leaderId, 999)
	sleepMs(150)
	h.CheckCommittedN(999, 3)
}

// TestSnapshotWithLeaderChange verifies that snapshot state is correctly
// carried forward through a leader change.  A new leader elected after the
// original leader takes a snapshot must be able to send that snapshot to
// lagging followers.
func TestSnapshotWithLeaderChange(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()
	// followerId is the node we will crash and later restart — it must receive
	// a snapshot from whatever leader is current after restart.
	followerId := (origLeaderId + 2) % 3

	// Crash the follower before any entries so it has no log in storage.
	h.CrashPeer(followerId)

	// Submit 4 entries on the remaining quorum (origLeader + otherFollower).
	for i := 1; i <= 4; i++ {
		h.SubmitToServer(origLeaderId, i*5)
	}
	sleepMs(250)
	h.CheckCommittedN(5, 2)
	h.CheckCommittedN(20, 2)

	// Compact the original leader's log at index 3.
	h.cluster[origLeaderId].cm.InstallSnapshot(3, origTerm, encodeSnapshot(t, []int{5, 10, 15, 20}))

	// Disconnect the original leader — a new leader must be elected from the
	// remaining two nodes (otherFollowerId is the only connected non-leader).
	// But we need quorum=2, so we keep otherFollowerId connected and restart
	// followerId first so we have 2 nodes after disconnecting origLeader.
	//
	// Restart the crashed follower first, then disconnect the original leader.
	h.RestartPeer(followerId)
	sleepMs(50)

	// Now disconnect the original leader.  New election between followerId and
	// otherFollowerId; both have quorum.
	h.DisconnectPeer(origLeaderId)
	sleepMs(400)

	newLeaderId, _ := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Fatalf("original leader %d should not be leader after disconnect", origLeaderId)
	}

	// The snapshot from the original leader should have been delivered to the
	// restarted follower (either from the orig leader before disconnect, or from
	// the new leader after election since followerId had no log in storage).
	h.CheckSnapshotDelivered(followerId, 3)

	// Submit a new entry via the new leader and verify it commits on all connected nodes.
	h.SubmitToServer(newLeaderId, 777)
	sleepMs(200)
	h.CheckCommittedIgnoringSnapshot(777)
}

// TestSnapshotGobRoundTrip is a pure unit test that verifies the
// encodeSnapshot/decodeSnapshot helpers used throughout snapshot tests produce
// bit-for-bit identical results, guarding against silent gob registration
// issues.
func TestSnapshotGobRoundTrip(t *testing.T) {
	original := []int{1, 2, 3, 100, 200, -1, 0}
	encoded := encodeSnapshot(t, original)
	decoded := decodeSnapshot(t, encoded)

	if len(decoded) != len(original) {
		t.Fatalf("round-trip length mismatch: got %d want %d", len(decoded), len(original))
	}
	for i, v := range original {
		if decoded[i] != v {
			t.Errorf("decoded[%d] = %d; want %d", i, decoded[i], v)
		}
	}
}

// TestSnapshotConcurrentSubmitAndCompact stress-tests the scenario where new
// commands are being submitted at the same time the application layer calls
// InstallSnapshot.  The log must remain consistent throughout.
func TestSnapshotConcurrentSubmitAndCompact(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()

	// Submit and commit a batch.
	for i := 1; i <= 6; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(300)
	for i := 1; i <= 6; i++ {
		h.CheckCommittedN(i, 3)
	}

	// Compact at index 5.
	h.cluster[leaderId].cm.mu.Lock()
	snapTerm := h.cluster[leaderId].cm.currentTerm
	h.cluster[leaderId].cm.mu.Unlock()

	h.cluster[leaderId].cm.InstallSnapshot(5, snapTerm, encodeSnapshot(t, []int{1, 2, 3, 4, 5, 6}))

	// Submit more while the snapshot is "live".
	for i := 7; i <= 10; i++ {
		h.SubmitToServer(leaderId, i)
	}
	sleepMs(300)

	for i := 7; i <= 10; i++ {
		h.CheckCommittedN(i, 3)
	}

	// Verify global indices are monotonically increasing from 6 onward.
	_, idx7 := h.CheckCommitted(7)
	_, idx8 := h.CheckCommitted(8)
	_, idx9 := h.CheckCommitted(9)
	_, idx10 := h.CheckCommitted(10)

	if idx7 >= idx8 || idx8 >= idx9 || idx9 >= idx10 {
		t.Errorf("post-snapshot indices not monotone: %d %d %d %d", idx7, idx8, idx9, idx10)
	}
	if idx7 != 6 {
		t.Errorf("first post-snapshot entry should be at index 6, got %d", idx7)
	}

	// No follower should have received a snapshot via RPC (all were up to date).
	time.Sleep(100 * time.Millisecond)
	for i := range 3 {
		if i == leaderId {
			continue
		}
		h.CheckNoSnapshotDelivered(i)
	}
}
