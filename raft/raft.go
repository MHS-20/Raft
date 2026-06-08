package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

// CommitEntry is the data reported by Raft to the commit channel
type CommitEntry struct {
	Command any
	Index   int
	Term    int
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

// ConsensusModule (CM) implements a single node of Raft consensus.
type ConsensusModule struct {
	mu sync.Mutex

	// id is the server ID of this CM.
	id      int
	peerIds []int
	server  *Server
	storage Storage

	commitChan           chan<- CommitEntry
	newCommitReadyChan   chan struct{}
	newCommitReadyChanWg sync.WaitGroup
	triggerAEChan        chan struct{}

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewConsensusModule(id int, peerIds []int, server *Server, storage Storage, ready <-chan any, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
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

// SubmitResult is returned by Submit to tell the caller whether the command
// was accepted and, if not, which node is believed to be the current leader.
type SubmitResult struct {
	// Index is the log index assigned to the command, or -1 if not accepted.
	Index int
	// IsLeader is true when the command was accepted by this node.
	IsLeader bool
	// LeaderHint is the ID of the node this server believes is the current
	// leader, or -1 if unknown. Clients can use this to redirect their request.
	LeaderHint int
}

// Submit submits a new command to the CM. It returns a SubmitResult
func (cm *ConsensusModule) Submit(command any) SubmitResult {
	cm.mu.Lock()
	cm.dlog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		submitIndex := len(cm.log)
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return SubmitResult{Index: submitIndex, IsLeader: true, LeaderHint: cm.id}
	}

	// Not the leader — return the best hint we have about who is.
	// votedFor is the last node this server voted for, which is the most
	// recent leader hint we have; if we haven't voted yet, return -1.
	hint := cm.votedFor
	cm.mu.Unlock()
	return SubmitResult{Index: -1, IsLeader: false, LeaderHint: hint}
}

func (cm *ConsensusModule) Stop() {
	cm.dlog("CM.Stop called")
	cm.mu.Lock()
	cm.state = Dead
	cm.mu.Unlock()
	cm.dlog("becomes Dead")
	close(cm.newCommitReadyChan)
	cm.newCommitReadyChanWg.Wait()
}

// restores the persistent state of this CM from storage.
func (cm *ConsensusModule) restoreFromStorage() {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

type persistentState struct {
	currentTerm int
	votedFor    int
	logLen      int
}

func (cm *ConsensusModule) captureState() persistentState {
	return persistentState{cm.currentTerm, cm.votedFor, len(cm.log)}
}

func (cm *ConsensusModule) persistIfChanged(before persistentState) {
	after := cm.captureState()
	if after != before {
		cm.persistToStorage()
	}
}

func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}

// debug log
func (cm *ConsensusModule) dlog(format string, args ...any) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

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
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	before := cm.captureState()

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
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
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

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

	// conflict optimization
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	before := cm.captureState()

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for logInsertIndex < len(cm.log) && newEntriesIndex < len(args.Entries) {
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistIfChanged(before)
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// generates a pseudo-random election timeout duration.
func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.persistToStorage()
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	votesReceived := 1

	// Send RequestVote RPCs to all other servers concurrently.
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

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}

				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == cm.currentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(cm.peerIds)+1 {
							// Won the election!
							cm.dlog("wins election with %d votes", votesReceived)
							cm.startLeader()
							return
						}
					}
				}
			}
		}()
	}

	// Run another election timer, in case this election is not successful.
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	if term > cm.currentTerm {
		cm.currentTerm = term
		cm.votedFor = -1
		cm.persistToStorage()
	}
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader

	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func(heartbeatTimeout time.Duration) {
		// Immediately send AEs to peers.
		cm.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				// Reset timer to fire again after heartbeatTimeout.
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

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func() {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			// Snapshot the slice so the RPC sends consistent data even if the
			// leader appends more entries before the goroutine's Call returns.
			entries := make([]LogEntry, len(cm.log[ni:]))
			copy(entries, cm.log[ni:])

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				if reply.Term > cm.currentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					cm.mu.Unlock()
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							cm.mu.Unlock()
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						} else {
							cm.mu.Unlock()
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
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
						cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
						cm.mu.Unlock()
					}
				} else {
					cm.mu.Unlock()
				}
			}
		}()
	}
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (cm *ConsensusModule) commitChanSender() {
	defer cm.newCommitReadyChanWg.Done()

	for range cm.newCommitReadyChan {
		// Find which entries we have to apply.
		cm.mu.Lock()
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.dlog("send on commitchan i=%v, entry=%v", i, entry)
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    entry.Term,
			}
		}
	}
	cm.dlog("commitChanSender done")
}
