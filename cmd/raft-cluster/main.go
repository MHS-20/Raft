package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/MHS-20/Raft/raft"
)

const dataDir = "/data"

func main() {
	idStr := os.Getenv("NODE_ID")
	if idStr == "" {
		slog.Error("NODE_ID environment variable required")
		os.Exit(1)
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		slog.Error("invalid NODE_ID", "value", idStr, "err", err)
		os.Exit(1)
	}

	clusterSizeStr := os.Getenv("CLUSTER_SIZE")
	if clusterSizeStr == "" {
		slog.Error("CLUSTER_SIZE environment variable required")
		os.Exit(1)
	}
	clusterSize, err := strconv.Atoi(clusterSizeStr)
	if err != nil {
		slog.Error("invalid CLUSTER_SIZE", "value", clusterSizeStr, "err", err)
		os.Exit(1)
	}

	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname, _ = os.Hostname()
	}

	var peerIds []int
	for i := range clusterSize {
		if i != id {
			peerIds = append(peerIds, i)
		}
	}

	storage := raft.NewMapStorage()
	commitChan := make(chan raft.CommitEntry, 100)
	ready := make(chan any)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	s := raft.NewServer(id, peerIds, storage, ready, commitChan, logger)
	s.Serve()

	_, port, err := net.SplitHostPort(s.GetListenAddr().String())
	if err != nil {
		logger.Error("parse listen port", "err", err)
		os.Exit(1)
	}
	advertiseAddr := net.JoinHostPort(hostname, port)
	logger.Info("server listening", "addr", advertiseAddr)

	os.MkdirAll(dataDir, 0755)
	myFile := filepath.Join(dataDir, fmt.Sprintf("node-%d.addr", id))
	os.Remove(myFile)
	os.WriteFile(myFile, []byte(advertiseAddr), 0644)
	defer os.Remove(myFile)

	logger.Info("awaiting peer address files", "peerIds", peerIds)
	for _, pid := range peerIds {
		peerFile := filepath.Join(dataDir, fmt.Sprintf("node-%d.addr", pid))
		var resolved string
		for range 60 {
			data, err := os.ReadFile(peerFile)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			resolved = strings.TrimSpace(string(data))
			break
		}
		if resolved == "" {
			logger.Error("timed out waiting for peer address", "peer", pid)
			os.Exit(1)
		}
		for range 30 {
			tcpAddr, err := net.ResolveTCPAddr("tcp", resolved)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if err := s.ConnectToPeer(pid, tcpAddr); err == nil {
				logger.Info("connected to peer", "peer", pid, "addr", resolved)
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	close(ready)
	logger.Info("cluster ready — election timer started")

	go func() {
		for entry := range commitChan {
			logger.Info("committed",
				"index", entry.Index,
				"term", entry.Term,
				"command", entry.Command,
			)
		}
	}()

	submitTicker := time.NewTicker(time.Duration(2+id*3) * time.Second)
	defer submitTicker.Stop()
	counter := id * 1000
	go func() {
		for range submitTicker.C {
			if !s.IsLeader() {
				continue
			}
			counter++
			result := s.Submit(counter)
			logger.Info("submitted",
				"command", counter,
				"index", result.Index,
			)
		}
	}()

	select {}
}
