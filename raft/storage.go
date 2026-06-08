package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	Set(key string, value []byte)

	Get(key string) ([]byte, bool)

	// HasData returns true iff any Sets were made on this Storage.
	HasData() bool
}

// MapStorage is a simple in-memory implementation of Storage for testing.
type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMapStorage() *MapStorage {
	m := make(map[string][]byte)
	return &MapStorage{
		m: m,
	}
}

func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]
	return v, found
}

func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0
}

// FileStorage is a durable Storage implementation that writes each key to its
// own file under a directory. Writes are atomic: data is first flushed to a
// temp file in the same directory, then renamed over the target, so a crash
// mid-write never leaves a partially-written value behind.
//
// Usage:
//
//	fs, err := NewFileStorage("/var/lib/raft/node-1")
//	if err != nil { log.Fatal(err) }
type FileStorage struct {
	mu  sync.Mutex
	dir string
}

// NewFileStorage creates (or opens) a FileStorage rooted at dir.
// The directory is created with 0700 permissions if it does not exist.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("FileStorage: mkdir %q: %w", dir, err)
	}
	return &FileStorage{dir: dir}, nil
}

func (fs *FileStorage) path(key string) string {
	return filepath.Join(fs.dir, key+".dat")
}

func (fs *FileStorage) tmpPath(key string) string {
	return filepath.Join(fs.dir, key+".tmp")
}

// Set writes value for key atomically. Panics (like MapStorage) on I/O error
// so callers don't have to check — a storage failure is fatal for Raft anyway.
func (fs *FileStorage) Set(key string, value []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	tmp := fs.tmpPath(key)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(fmt.Sprintf("FileStorage.Set: open tmp %q: %v", tmp, err))
	}
	if _, err := f.Write(value); err != nil {
		f.Close()
		panic(fmt.Sprintf("FileStorage.Set: write %q: %v", tmp, err))
	}
	// Flush to OS buffer then sync to disk before rename so the data is
	// durable even if the system crashes immediately after the rename.
	if err := f.Sync(); err != nil {
		f.Close()
		panic(fmt.Sprintf("FileStorage.Set: sync %q: %v", tmp, err))
	}
	f.Close()

	if err := os.Rename(tmp, fs.path(key)); err != nil {
		panic(fmt.Sprintf("FileStorage.Set: rename %q -> %q: %v", tmp, fs.path(key), err))
	}
}

// Get retrieves the value for key. Returns (nil, false) if the key has never
// been Set; panics on unexpected I/O errors.
func (fs *FileStorage) Get(key string) ([]byte, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.path(key))
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("FileStorage.Get: read %q: %v", fs.path(key), err))
	}
	return data, true
}

// HasData returns true if the storage directory contains at least one .dat
// file, meaning at least one Set has been persisted to disk.
func (fs *FileStorage) HasData() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".dat" {
			return true
		}
	}
	return false
}
