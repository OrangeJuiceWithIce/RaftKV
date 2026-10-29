package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Persister struct {
	mu   sync.Mutex
	path string
}

func NewPersister(path string) (*Persister, error) {
	if path == "" {
		return nil, fmt.Errorf("empty persister path")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &Persister{path: path}, nil
}

func (ps *Persister) SaveRaftState(data []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	dir := filepath.Dir(ps.path)

	tmp, err := os.CreateTemp(dir, ".raft-*.tmp")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, ps.path); err != nil {
		return err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()

	if err := dirFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (ps *Persister) ReadRaftState() ([]byte, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := os.ReadFile(ps.path)

	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return data, nil
}
