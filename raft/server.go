package raft

import (
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	cm       *ConsensusModule
	storage  Storage
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client

	ready  <-chan any
	quit   chan any
	wg     sync.WaitGroup
	logger *slog.Logger
}

func NewServer(serverId int, peerIds []int, storage Storage, ready <-chan any, commitChan chan<- CommitEntry, logger *slog.Logger) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.storage = storage
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan any)
	s.logger = logger.With("serverId", serverId)
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.storage, s.ready, s.commitChan, s.logger)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = NewProxy(s.cm)
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	s.logger.Info("listening", "addr", s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) Submit(cmd any) SubmitResult {
	return s.cm.Submit(cmd)
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return s.rpcProxy.Call(peer, serviceMethod, args, reply)
	}
}

func (s *Server) IsLeader() bool {
	_, _, isLeader := s.cm.Report()
	return isLeader
}

func (s *Server) Proxy() *RPCProxy {
	return s.rpcProxy
}

type RPCProxy struct {
	mu sync.Mutex
	cm *ConsensusModule

	// numCallsBeforeDrop is used to control dropping RPC calls:
	//   -1: means we're not dropping any calls
	//    0: means we're dropping all calls now
	//   >0: means we'll start dropping calls after this number is made
	numCallsBeforeDrop int
}

func NewProxy(cm *ConsensusModule) *RPCProxy {
	return &RPCProxy{
		cm:                 cm,
		numCallsBeforeDrop: -1,
	}
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.logger.Debug("drop RequestVote")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.logger.Debug("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.logger.Debug("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.logger.Debug("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}

func (rpp *RPCProxy) InstallSnapshotRPC(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		switch dice {
		case 9:
			rpp.cm.logger.Debug("drop InstallSnapshot")
			return fmt.Errorf("RPC failed")
		case 8:
			rpp.cm.logger.Debug("delay InstallSnapshot")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.InstallSnapshotRPC(args, reply)
}

func (rpp *RPCProxy) Call(peer *rpc.Client, method string, args any, reply any) error {
	rpp.mu.Lock()
	if rpp.numCallsBeforeDrop == 0 {
		rpp.mu.Unlock()
		rpp.cm.logger.Debug("drop Call", "method", method, "args", args)
		return fmt.Errorf("RPC failed")
	} else {
		if rpp.numCallsBeforeDrop > 0 {
			rpp.numCallsBeforeDrop--
		}
		rpp.mu.Unlock()
		return peer.Call(method, args, reply)
	}
}

func (rpp *RPCProxy) DropCallsAfterN(n int) {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = n
}

func (rpp *RPCProxy) DontDropCalls() {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = -1
}
