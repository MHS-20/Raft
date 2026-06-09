package raft

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"
)

func init() {
	gob.Register(ConfigChangeEntry{})
}

// CommitEntry is the data reported by Raft to the commit channel.
type CommitEntry struct {
	Command any
	Index   int
	Term    int
}

// SnapshotEntry is sent on the commit channel when a snapshot is installed.
// The application must restore its state from Data and discard all previously
// applied entries up through Index.
type SnapshotEntry struct {
	Data  []byte
	Index int
	Term  int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type LogEntry struct {
	Command any
	Term    int
}

type ConfigChangeType int

const (
	AddNode ConfigChangeType = iota
	RemoveNode
)

// ConfigChangeEntry is the Command stored in a ConfigChange log entry.
type ConfigChangeEntry struct {
	Type   ConfigChangeType
	NodeId int
}

// ConsensusModule (CM) implements a single node of Raft consensus.
type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int // currentConfig peers (dynamic)
	server  *Server

	// Cluster membership: pendingConfigIndex != -1 while a ConfigChange
	// log entry has been appended but not yet committed.
	pendingConfigIndex int
	storage            Storage
	logger             *slog.Logger

	commitChan           chan<- CommitEntry
	newCommitReadyChan   chan struct{}
	newCommitReadyChanWg sync.WaitGroup
	triggerAEChan        chan struct{}

	// snapshotReadyChan is signalled when a new snapshot should be sent to the
	// application.  A separate goroutine drains it so we never block under mu.
	snapshotReadyChan chan SnapshotEntry

	// snapshotDone is closed by Stop() to signal collectSnapshots to exit.
	snapshotDone chan struct{}

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Snapshot state (persistent)
	//   snapshotData          – the raw application snapshot bytes
	//   snapshotLastIndex     – last global log index included in the snapshot
	//   snapshotLastTerm      – term of snapshotLastIndex
	snapshotData      []byte
	snapshotLastIndex int
	snapshotLastTerm  int

	// logOffset is the global index of log[0].
	// All log accesses use the helpers below to translate between global and
	// local (slice) indices.
	logOffset int

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time
	removedSelf        bool // true when leader removes itself; skip elections

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// ---------------------------------------------------------------------------
// Index arithmetic helpers
// Every place that used to say cm.log[i] now goes through these helpers so
// there is one canonical place to apply / remove the offset.
// ---------------------------------------------------------------------------

// logLen returns the number of entries currently in the trimmed log slice.
func (cm *ConsensusModule) logLen() int { return len(cm.log) }

// globalLastIndex returns the global index of the last log entry, or
// snapshotLastIndex if the log is empty.
func (cm *ConsensusModule) globalLastIndex() int {
	if len(cm.log) == 0 {
		return cm.snapshotLastIndex
	}
	return cm.logOffset + len(cm.log) - 1
}

// localIndex converts a global log index to a slice index.
// The caller must verify the index is within bounds before using it.
func (cm *ConsensusModule) localIndex(globalIdx int) int {
	return globalIdx - cm.logOffset
}

// entryAt returns the log entry at global index globalIdx.
// Panics if the index is out of bounds (programming error).
func (cm *ConsensusModule) entryAt(globalIdx int) LogEntry {
	return cm.log[cm.localIndex(globalIdx)]
}

// lastLogIndexAndTerm returns the global index and term of the last entry.
// If the log is empty but a snapshot exists, returns the snapshot metadata.
// peerContains returns true if the given server id is in our peer list.
func (cm *ConsensusModule) peerContains(id int) bool {
	for _, pid := range cm.peerIds {
		if pid == id {
			return true
		}
	}
	return false
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		last := cm.globalLastIndex()
		return last, cm.entryAt(last).Term
	}
	// Log is empty; the last "known" entry is the snapshot boundary.
	return cm.snapshotLastIndex, cm.snapshotLastTerm
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func NewConsensusModule(
	id int,
	peerIds []int,
	server *Server,
	storage Storage,
	ready <-chan any,
	commitChan chan<- CommitEntry,
	logger *slog.Logger,
) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.logger = logger.With("id", id)
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.snapshotReadyChan = make(chan SnapshotEntry, 16)
	cm.snapshotDone = make(chan struct{})
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.snapshotLastIndex = -1
	cm.snapshotLastTerm = -1
	cm.pendingConfigIndex = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	cm.newCommitReadyChanWg.Add(1)
	go cm.commitChanSender()
	return cm
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// SubmitResult is returned by Submit.
type SubmitResult struct {
	Index      int
	IsLeader   bool
	LeaderHint int
}

// Submit submits a new command to the CM.
func (cm *ConsensusModule) Submit(command any) SubmitResult {
	cm.mu.Lock()
	cm.logger.Debug("Submit received", "state", cm.state, "command", command)
	if cm.state == Leader {
		// The global index for this new entry is one past the current last global index.
		submitIndex := cm.globalLastIndex() + 1
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.logger.Debug("Submit accepted", "submitIndex", submitIndex)
		// Single-node cluster: commit immediately (no peers to replicate to).
		if len(cm.peerIds) == 0 {
			cm.commitIndex = submitIndex
			cm.mu.Unlock()
			cm.newCommitReadyChan <- struct{}{}
			return SubmitResult{Index: submitIndex, IsLeader: true, LeaderHint: cm.id}
		}
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return SubmitResult{Index: submitIndex, IsLeader: true, LeaderHint: cm.id}
	}

	hint := cm.votedFor
	cm.mu.Unlock()
	return SubmitResult{Index: -1, IsLeader: false, LeaderHint: hint}
}

// AddPeer proposes adding nodeId to the cluster. Must be called on the leader.
// Returns false if not leader or a config change is already pending.
func (cm *ConsensusModule) AddPeer(nodeId int) bool {
	cm.mu.Lock()
	if cm.state != Leader || cm.pendingConfigIndex != -1 {
		cm.mu.Unlock()
		return false
	}
	for _, id := range cm.peerIds {
		if id == nodeId {
			cm.mu.Unlock()
			return false // already a member
		}
	}
	entry := ConfigChangeEntry{Type: AddNode, NodeId: nodeId}
	submitIndex := cm.globalLastIndex() + 1
	cm.log = append(cm.log, LogEntry{Command: entry, Term: cm.currentTerm})
	cm.pendingConfigIndex = submitIndex
	// Initialize replication state for the new peer now so AEs are sent.
	cm.nextIndex[nodeId] = cm.globalLastIndex() + 1
	cm.matchIndex[nodeId] = -1
	cm.persistToStorage()
	cm.mu.Unlock()
	cm.triggerAEChan <- struct{}{}
	return true
}

// RemovePeer proposes removing nodeId from the cluster. Must be called on the leader.
func (cm *ConsensusModule) RemovePeer(nodeId int) bool {
	cm.mu.Lock()
	if cm.state != Leader || cm.pendingConfigIndex != -1 {
		cm.mu.Unlock()
		return false
	}
	found := false
	for _, id := range cm.peerIds {
		if id == nodeId {
			found = true
			break
		}
	}
	if !found && nodeId != cm.id {
		cm.mu.Unlock()
		return false // not a member
	}
	entry := ConfigChangeEntry{Type: RemoveNode, NodeId: nodeId}
	submitIndex := cm.globalLastIndex() + 1
	cm.log = append(cm.log, LogEntry{Command: entry, Term: cm.currentTerm})
	cm.pendingConfigIndex = submitIndex
	cm.persistToStorage()
	cm.mu.Unlock()
	cm.triggerAEChan <- struct{}{}
	return true
}

// InstallSnapshot is called by the application layer (or test harness) to
// compact the log.  snapshotData is an opaque blob that fully encodes the
// application state as of lastIndex/lastTerm.
//
// It is safe to call from any goroutine; it acquires cm.mu internally.
func (cm *ConsensusModule) InstallSnapshot(lastIndex, lastTerm int, snapshotData []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Ignore stale snapshots.
	if lastIndex <= cm.snapshotLastIndex {
		return
	}

	cm.logger.Debug("InstallSnapshot (local)", "lastIndex", lastIndex, "lastTerm", lastTerm)

	// Trim the in-memory log: keep entries after lastIndex.
	if lastIndex >= cm.globalLastIndex() {
		// Snapshot covers the whole log.
		cm.log = nil
		cm.logOffset = lastIndex + 1
	} else {
		// Keep the tail.
		localLast := cm.localIndex(lastIndex)
		cm.log = append([]LogEntry(nil), cm.log[localLast+1:]...)
		cm.logOffset = lastIndex + 1
	}

	cm.snapshotData = snapshotData
	cm.snapshotLastIndex = lastIndex
	cm.snapshotLastTerm = lastTerm

	if cm.commitIndex < lastIndex {
		cm.commitIndex = lastIndex
	}
	if cm.lastApplied < lastIndex {
		cm.lastApplied = lastIndex
	}

	cm.persistToStorage()
}

func (cm *ConsensusModule) Stop() {
	cm.logger.Debug("CM.Stop called")
	cm.mu.Lock()
	cm.state = Dead
	cm.mu.Unlock()
	cm.logger.Debug("becomes Dead")
	close(cm.newCommitReadyChan)
	cm.newCommitReadyChanWg.Wait()
	// Signal collectSnapshots goroutines to exit.  We close snapshotDone rather
	// than snapshotReadyChan so that any in-flight goroutine trying to send on
	// snapshotReadyChan doesn't race with a close and panic.
	close(cm.snapshotDone)
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

func (cm *ConsensusModule) restoreFromStorage() {
	decode := func(key string, dst any) {
		data, found := cm.storage.Get(key)
		if !found {
			cm.logger.Error("key not found in storage", "key", key)
			os.Exit(1)
		}
		if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(dst); err != nil {
			cm.logger.Error("failed to decode", "key", key, "err", err)
			os.Exit(1)
		}
	}

	decode("currentTerm", &cm.currentTerm)
	decode("votedFor", &cm.votedFor)
	decode("log", &cm.log)
	decode("logOffset", &cm.logOffset)
	decode("snapshotLastIndex", &cm.snapshotLastIndex)
	decode("snapshotLastTerm", &cm.snapshotLastTerm)

	// snapshotData may be absent on nodes that have never taken a snapshot.
	if data, found := cm.storage.Get("snapshotData"); found {
		cm.snapshotData = data
	}

	// Restore volatile indices from durable state so they survive restarts.
	cm.commitIndex = cm.snapshotLastIndex
	cm.lastApplied = cm.snapshotLastIndex
}

type persistentState struct {
	currentTerm       int
	votedFor          int
	logLen            int
	logOffset         int
	snapshotLastIndex int
	snapshotLastTerm  int
}

func (cm *ConsensusModule) captureState() persistentState {
	return persistentState{
		currentTerm:       cm.currentTerm,
		votedFor:          cm.votedFor,
		logLen:            len(cm.log),
		logOffset:         cm.logOffset,
		snapshotLastIndex: cm.snapshotLastIndex,
		snapshotLastTerm:  cm.snapshotLastTerm,
	}
}

func (cm *ConsensusModule) persistIfChanged(before persistentState) {
	if cm.captureState() != before {
		cm.persistToStorage()
	}
}

func (cm *ConsensusModule) persistToStorage() {
	encode := func(key string, v any) {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(v); err != nil {
			cm.logger.Error("failed to encode", "key", key, "err", err)
			os.Exit(1)
		}
		cm.storage.Set(key, buf.Bytes())
	}

	encode("currentTerm", cm.currentTerm)
	encode("votedFor", cm.votedFor)
	encode("log", cm.log)
	encode("logOffset", cm.logOffset)
	encode("snapshotLastIndex", cm.snapshotLastIndex)
	encode("snapshotLastTerm", cm.snapshotLastTerm)

	if cm.snapshotData != nil {
		cm.storage.Set("snapshotData", cm.snapshotData)
	}
}

// ---------------------------------------------------------------------------
// RequestVote RPC
// ---------------------------------------------------------------------------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.logger.Debug("RequestVote", "args", args, "currentTerm", cm.currentTerm,
		"votedFor", cm.votedFor, "lastLogIndex", lastLogIndex, "lastLogTerm", lastLogTerm)

	// Raft membership safety: reject RequestVote from candidates not in our
	// current configuration BEFORE updating our term.  This prevents a
	// removed (or not-yet-added) server from disrupting the cluster by
	// bumping everyone's term with spurious elections.
	isPeer := args.CandidateId == cm.id
	if !isPeer {
		for _, pid := range cm.peerIds {
			if pid == args.CandidateId {
				isPeer = true
				break
			}
		}
	}
	if !isPeer {
		reply.Term = cm.currentTerm
		reply.VoteGranted = false
		cm.logger.Debug("RequestVote denied: candidate not a peer", "candidateId", args.CandidateId)
		return nil
	}

	before := cm.captureState()

	if args.Term > cm.currentTerm {
		cm.logger.Debug("term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.persistIfChanged(before)
	cm.logger.Debug("RequestVote reply", "reply", reply)
	return nil
}

// ---------------------------------------------------------------------------
// AppendEntries RPC
// ---------------------------------------------------------------------------

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Conflict-index optimisation fields.
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.logger.Debug("AppendEntries", "args", args)

	before := cm.captureState()

	if args.Term > cm.currentTerm {
		cm.logger.Debug("term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		// If this server has been removed from the cluster via a committed
		// config change, ignore all AEs — there is no leader we should
		// follow.
		if cm.removedSelf {
			cm.logger.Debug("AppendEntries denied: server removed itself", "leaderId", args.LeaderId)
			return nil
		}

		// ---- consistency check ------------------------------------------------
		// Three cases for PrevLogIndex relative to our snapshot boundary:
		//
		//  (a) PrevLogIndex == -1  →  no previous entry required (legacy / empty log)
		//  (b) PrevLogIndex == snapshotLastIndex  →  the previous entry is the snapshot
		//      boundary; accept if terms match.
		//  (c) PrevLogIndex is within our live log  →  normal check.
		//  (d) PrevLogIndex < snapshotLastIndex  →  already compacted; treat as
		//      consistent (the leader is behind our snapshot, so we accept and
		//      potentially extend the log).
		// -----------------------------------------------------------------------
		prevOk := false
		switch {
		case args.PrevLogIndex == -1:
			prevOk = true
		case args.PrevLogIndex < cm.logOffset:
			// Covered by our snapshot; the leader's history is a prefix of ours.
			prevOk = true
		case args.PrevLogIndex == cm.snapshotLastIndex:
			prevOk = args.PrevLogTerm == cm.snapshotLastTerm
		default:
			// args.PrevLogIndex is within the live log range.
			globalLast := cm.globalLastIndex()
			if args.PrevLogIndex <= globalLast &&
				cm.entryAt(args.PrevLogIndex).Term == args.PrevLogTerm {
				prevOk = true
			}
		}

		if prevOk {
			reply.Success = true

			// Determine the first global index in our log that the new entries map to.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			// If the insertion point falls inside the already-snapshotted region,
			// skip the corresponding new entries.
			if logInsertIndex < cm.logOffset {
				skip := cm.logOffset - logInsertIndex
				if skip >= len(args.Entries) {
					// All new entries are already covered by the snapshot.
					newEntriesIndex = len(args.Entries)
				} else {
					newEntriesIndex = skip
					logInsertIndex = cm.logOffset
				}
			}

			// Walk forward while existing entries agree with the new ones.
			for logInsertIndex <= cm.globalLastIndex() && newEntriesIndex < len(args.Entries) {
				if cm.entryAt(logInsertIndex).Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.logger.Debug("inserting entries",
					"entries", args.Entries[newEntriesIndex:],
					"fromGlobalIndex", logInsertIndex)
				// Truncate the local slice to match, then append.
				localInsert := cm.localIndex(logInsertIndex)
				cm.log = append(cm.log[:localInsert], args.Entries[newEntriesIndex:]...)
				cm.logger.Debug("log is now", "log", cm.log, "logOffset", cm.logOffset)
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, cm.globalLastIndex())
				cm.logger.Debug("setting commitIndex", "commitIndex", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// Build conflict hint for the leader.
			globalLast := cm.globalLastIndex()
			if args.PrevLogIndex > globalLast {
				reply.ConflictIndex = globalLast + 1
				reply.ConflictTerm = -1
			} else if args.PrevLogIndex < cm.logOffset {
				// Prevent out of bounds panic if leader is somehow behind our logOffset
				reply.ConflictIndex = cm.logOffset
				reply.ConflictTerm = -1
			} else {
				// args.PrevLogIndex is in the live log but term mismatches.
				reply.ConflictTerm = cm.entryAt(args.PrevLogIndex).Term
				i := args.PrevLogIndex - 1
				for i >= cm.logOffset && cm.entryAt(i).Term == reply.ConflictTerm {
					i--
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistIfChanged(before)
	cm.logger.Debug("AppendEntries reply", "reply", *reply)
	return nil
}

// ---------------------------------------------------------------------------
// InstallSnapshot RPC  (leader → follower)
// ---------------------------------------------------------------------------

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot is the RPC handler called on a follower by the leader when
// the follower's nextIndex has fallen behind the leader's snapshot point.
func (cm *ConsensusModule) InstallSnapshotRPC(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.logger.Debug("InstallSnapshotRPC", "args.LastIncludedIndex", args.LastIncludedIndex,
		"args.Term", args.Term)

	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}
	reply.Term = cm.currentTerm

	if args.Term < cm.currentTerm {
		return nil
	}

	// Already have a newer snapshot.
	if args.LastIncludedIndex <= cm.snapshotLastIndex {
		return nil
	}

	// Reset election timer — we heard from the leader.
	cm.electionResetEvent = time.Now()
	if cm.state != Follower {
		cm.becomeFollower(args.Term)
	}

	// Trim or replace the in-memory log.
	globalLast := cm.globalLastIndex()
	if args.LastIncludedIndex >= globalLast {
		cm.log = nil
		cm.logOffset = args.LastIncludedIndex + 1
	} else {
		localLast := cm.localIndex(args.LastIncludedIndex)
		cm.log = append([]LogEntry(nil), cm.log[localLast+1:]...)
		cm.logOffset = args.LastIncludedIndex + 1
	}

	cm.snapshotData = append([]byte(nil), args.Data...)
	cm.snapshotLastIndex = args.LastIncludedIndex
	cm.snapshotLastTerm = args.LastIncludedTerm

	if cm.commitIndex < args.LastIncludedIndex {
		cm.commitIndex = args.LastIncludedIndex
	}
	if cm.lastApplied < args.LastIncludedIndex {
		cm.lastApplied = args.LastIncludedIndex
	}

	cm.persistToStorage()

	// Notify the application via a non-blocking send on snapshotReadyChan.
	// We use select so the goroutine exits cleanly if Stop() is called before
	// there is room in the channel.
	snap := SnapshotEntry{
		Data:  cm.snapshotData,
		Index: cm.snapshotLastIndex,
		Term:  cm.snapshotLastTerm,
	}
	go func() {
		select {
		case cm.snapshotReadyChan <- snap:
		case <-cm.snapshotDone:
		}
	}()

	return nil
}

// SnapshotReady returns the channel on which snapshot notifications arrive.
// The application should drain this channel and restore its state machine.
// The channel is never closed; watch SnapshotDone() to know when to stop.
func (cm *ConsensusModule) SnapshotReady() <-chan SnapshotEntry {
	return cm.snapshotReadyChan
}

// SnapshotDone returns a channel that is closed when the CM is stopped.
// Use it to terminate goroutines that drain SnapshotReady().
func (cm *ConsensusModule) SnapshotDone() <-chan struct{} {
	return cm.snapshotDone
}

// ---------------------------------------------------------------------------
// Election timer / election
// ---------------------------------------------------------------------------

func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.logger.Debug("election timer started", "timeout", timeoutDuration, "term", termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.logger.Debug("election timer bailing out", "state", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			cm.logger.Debug("election timer term changed, bailing out",
				"from", termStarted, "to", cm.currentTerm)
			cm.mu.Unlock()
			return
		}
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	if cm.removedSelf {
		cm.state = Follower
		cm.logger.Debug("skipping election; node removed from cluster")
		return
	}
	cm.state = Candidate
	cm.currentTerm++
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.persistToStorage()
	cm.logger.Debug("becomes Candidate", "currentTerm", savedCurrentTerm)

	votesReceived := 1

	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.logger.Debug("sending RequestVote", "to", peerId, "args", args)
			var reply RequestVoteReply
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.logger.Debug("received RequestVoteReply", "reply", reply)

				if cm.state != Candidate {
					cm.logger.Debug("while waiting for reply, state changed", "state", cm.state)
					return
				}
				if reply.Term > cm.currentTerm {
					cm.logger.Debug("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == cm.currentTerm && reply.VoteGranted {
					votesReceived++
					clusterSize := len(cm.peerIds) + 1
					if votesReceived*2 > clusterSize {
						cm.logger.Debug("wins election", "votes", votesReceived)
						cm.startLeader()
					}
				}
			}
		}()
	}

	// Check for immediate majority (single-node cluster).
	clusterSize := len(cm.peerIds) + 1
	if votesReceived*2 > clusterSize {
		cm.startLeader()
		return
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.logger.Debug("becomes Follower", "term", term)
	cm.state = Follower
	if term > cm.currentTerm {
		cm.currentTerm = term
		cm.votedFor = -1
		cm.persistToStorage()
	}
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

// ---------------------------------------------------------------------------
// Leader logic
// ---------------------------------------------------------------------------

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerId := range cm.peerIds {
		// nextIndex starts just past the leader's last global log index.
		cm.nextIndex[peerId] = cm.globalLastIndex() + 1
		cm.matchIndex[peerId] = -1
	}
	cm.logger.Debug("becomes Leader", "term", cm.currentTerm,
		"nextIndex", cm.nextIndex, "matchIndex", cm.matchIndex)

	go func(heartbeatTimeout time.Duration) {
		cm.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

// leaderSendAEs sends either AppendEntries or InstallSnapshot to every peer,
// depending on whether the peer's nextIndex is still within the live log.
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm
	peers := append([]int(nil), cm.peerIds...)
	// Also include any peers that have replication state but aren't yet in
	// peerIds (i.e. a newly added node catching up before config commits).
	for id := range cm.nextIndex {
		found := false
		for _, p := range peers {
			if p == id {
				found = true
				break
			}
		}
		if !found {
			peers = append(peers, id)
		}
	}

	cm.mu.Unlock()

	for _, peerId := range peers {
		go func() {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]

			// ----------------------------------------------------------------
			// If nextIndex has fallen behind the snapshot point, send the
			// snapshot instead of (potentially missing) log entries.
			// ----------------------------------------------------------------
			if ni <= cm.snapshotLastIndex {
				cm.sendInstallSnapshot(peerId, savedCurrentTerm)
				return // sendInstallSnapshot releases the lock
			}

			// Normal AppendEntries path.
			prevLogIndex := ni - 1
			prevLogTerm := -1
			switch {
			case prevLogIndex == cm.snapshotLastIndex:
				prevLogTerm = cm.snapshotLastTerm
			case prevLogIndex >= cm.logOffset:
				prevLogTerm = cm.entryAt(prevLogIndex).Term
			case prevLogIndex == -1:
				// prevLogTerm stays -1
			}

			// Snapshot of the entries to send so the RPC is consistent.
			globalLast := cm.globalLastIndex()
			var entries []LogEntry
			if ni <= globalLast {
				localNi := cm.localIndex(ni)
				entries = make([]LogEntry, len(cm.log[localNi:]))
				copy(entries, cm.log[localNi:])
			}

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()

			cm.logger.Debug("sending AppendEntries", "to", peerId, "ni", ni, "args", args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				if reply.Term > cm.currentTerm {
					cm.logger.Debug("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					cm.mu.Unlock()
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						// Advance commitIndex if a new majority is reached.
						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i <= cm.globalLastIndex(); i++ {
							if cm.entryAt(i).Term == cm.currentTerm {
								matchCount := 1
								for _, pid := range cm.peerIds {
									if cm.matchIndex[pid] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
									cm.maybeApplyConfigAt(i)
								}
							}
						}
						cm.logger.Debug("AppendEntries reply success",
							"from", peerId,
							"nextIndex", cm.nextIndex,
							"matchIndex", cm.matchIndex,
							"commitIndex", cm.commitIndex)
						if cm.commitIndex != savedCommitIndex {
							cm.logger.Debug("leader sets commitIndex", "commitIndex", cm.commitIndex)
							cm.mu.Unlock()
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						} else {
							cm.mu.Unlock()
						}
					} else {
						// Conflict-index optimisation: back up nextIndex.
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := cm.globalLastIndex(); i >= cm.logOffset; i-- {
								if cm.entryAt(i).Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								cm.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								cm.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
						// Never let nextIndex go below 0.
						if cm.nextIndex[peerId] < 0 {
							cm.nextIndex[peerId] = 0
						}
						cm.logger.Debug("AppendEntries reply !success", "from", peerId,
							"nextIndex", cm.nextIndex[peerId])
						cm.mu.Unlock()
					}
				} else {
					cm.mu.Unlock()
				}
			}
		}()
	}
}

// sendInstallSnapshot sends the current snapshot to peerId.
// MUST be called with cm.mu held; releases the lock before the RPC.
func (cm *ConsensusModule) sendInstallSnapshot(peerId, savedCurrentTerm int) {
	args := InstallSnapshotArgs{
		Term:              savedCurrentTerm,
		LeaderId:          cm.id,
		LastIncludedIndex: cm.snapshotLastIndex,
		LastIncludedTerm:  cm.snapshotLastTerm,
		Data:              append([]byte(nil), cm.snapshotData...),
	}
	cm.mu.Unlock()

	cm.logger.Debug("sending InstallSnapshot", "to", peerId,
		"lastIncludedIndex", args.LastIncludedIndex)
	var reply InstallSnapshotReply
	if err := cm.server.Call(peerId, "ConsensusModule.InstallSnapshotRPC", args, &reply); err == nil {
		cm.mu.Lock()
		defer cm.mu.Unlock()
		if reply.Term > cm.currentTerm {
			cm.becomeFollower(reply.Term)
			return
		}
		if cm.state == Leader && savedCurrentTerm == reply.Term {
			// Advance nextIndex past the snapshot so we can resume AppendEntries.
			cm.nextIndex[peerId] = args.LastIncludedIndex + 1
			cm.matchIndex[peerId] = args.LastIncludedIndex
			cm.logger.Debug("InstallSnapshot reply ok", "from", peerId,
				"nextIndex", cm.nextIndex[peerId])
		}
	}
}

// sendFinalHeartbeat sends an AppendEntries heartbeat to every remaining peer
// with the given LeaderCommit, so they can advance their commitIndex before
// this server relinquishes leadership.  It is called with cm.mu held and
// MUST NOT acquire the lock itself.
func (cm *ConsensusModule) sendFinalHeartbeat(term, leaderCommit int) {
	peers := append([]int(nil), cm.peerIds...)
	for _, pid := range peers {
		pid := pid
		ni := cm.nextIndex[pid]
		lastIdx := cm.globalLastIndex()
		var entries []LogEntry
		prevLogIndex := -1
		prevLogTerm := -1
		if ni <= lastIdx {
			localFrom := cm.localIndex(ni)
			entries = make([]LogEntry, len(cm.log)-localFrom)
			copy(entries, cm.log[localFrom:])
			prevLogIndex = ni - 1
			if prevLogIndex >= cm.logOffset {
				prevLogTerm = cm.entryAt(prevLogIndex).Term
			}
		}
		ents := entries
		go func() {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      ents,
				LeaderCommit: leaderCommit,
			}
			var reply AppendEntriesReply
			cm.server.Call(pid, "ConsensusModule.AppendEntries", args, &reply)
		}()
	}
}

// ---------------------------------------------------------------------------
// Commit sender
// ---------------------------------------------------------------------------

// maybeApplyConfigAt checks whether the entry at globalIdx is a ConfigChange
// and, if so, applies it to peerIds immediately.
// MUST be called with cm.mu held.
func (cm *ConsensusModule) maybeApplyConfigAt(globalIdx int) {
	if globalIdx < cm.logOffset || globalIdx > cm.globalLastIndex() {
		return
	}
	entry := cm.entryAt(globalIdx)
	cc, ok := entry.Command.(ConfigChangeEntry)
	if !ok {
		return
	}
	if globalIdx == cm.pendingConfigIndex {
		cm.pendingConfigIndex = -1
	}
	switch cc.Type {
	case AddNode:
		if cc.NodeId != cm.id {
			for _, id := range cm.peerIds {
				if id == cc.NodeId {
					return
				}
			}
			cm.peerIds = append(cm.peerIds, cc.NodeId)
		}
	case RemoveNode:
		// If the leader is removing another node, send one final
		// heartbeat so the removed node learns about the commitIndex
		// (and applies the config change) before it stops receiving
		// heartbeats and starts a disruptive election.  The heartbeat
		// is sent before peerIds is trimmed so the target is included.
		if cc.NodeId != cm.id && cm.state == Leader {
			cm.sendFinalHeartbeat(cm.currentTerm, cm.commitIndex)
		}
		newPeers := cm.peerIds[:0:0]
		for _, id := range cm.peerIds {
			if id != cc.NodeId {
				newPeers = append(newPeers, id)
			}
		}
		cm.peerIds = newPeers
		delete(cm.nextIndex, cc.NodeId)
		delete(cm.matchIndex, cc.NodeId)
		// Removed leader must step down.
		if cc.NodeId == cm.id {
			cm.removedSelf = true
		}
		if cc.NodeId == cm.id && cm.state == Leader {
			// Send one final heartbeat to every remaining peer so they
			// learn about the commitIndex (and apply the config change)
			// before the old leader disappears / starts an election.
			cm.sendFinalHeartbeat(cm.currentTerm, cm.commitIndex)
			cm.becomeFollower(cm.currentTerm)
		}
	}
}

func (cm *ConsensusModule) commitChanSender() {
	defer cm.newCommitReadyChanWg.Done()

	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			// Only entries that are still in the live log can be applied here.
			// Entries covered by the snapshot were already reported via
			// snapshotReadyChan when the snapshot was installed.
			applyFrom := cm.lastApplied + 1
			if applyFrom < cm.logOffset {
				applyFrom = cm.logOffset
			}
			if applyFrom <= cm.commitIndex {
				localFrom := cm.localIndex(applyFrom)
				localTo := cm.localIndex(cm.commitIndex) + 1
				entries = make([]LogEntry, localTo-localFrom)
				copy(entries, cm.log[localFrom:localTo])
				savedLastApplied = applyFrom - 1
			}
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.logger.Debug("commitChanSender", "entries", entries, "savedLastApplied", savedLastApplied)

		for i, entry := range entries {
			globalIdx := savedLastApplied + i + 1
			if _, ok := entry.Command.(ConfigChangeEntry); ok {
				cm.mu.Lock()
				cm.maybeApplyConfigAt(globalIdx)
				cm.mu.Unlock()
			}
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   globalIdx,
				Term:    entry.Term,
			}
		}
	}
	cm.logger.Debug("commitChanSender done")
}
