package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// [新增] 一个文件同时保存 Raft 状态与 Snapshot。
// Snapshot 为 nil 表示没有快照。
type persistentData struct {
	State    []byte
	Snapshot []byte
	Checksum [32]byte
}

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

// [新增] 编码持久化文件。
// Checksum 用于检测文件损坏，不是用于去重。
func encodePersistentData(
	state []byte,
	snapshot []byte,
) ([]byte, error) {
	p := persistentData{
		State:    state,
		Snapshot: snapshot,
	}

	h := sha256.New()
	h.Write(state)
	h.Write(snapshot)
	copy(p.Checksum[:], h.Sum(nil))

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&p); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodePersistentData(data []byte) (
	[]byte, []byte, error,
) {
	var p persistentData

	if err := gob.NewDecoder(
		bytes.NewReader(data),
	).Decode(&p); err != nil {
		return nil, nil, err
	}

	h := sha256.New()
	h.Write(p.State)
	h.Write(p.Snapshot)

	var sum [32]byte
	copy(sum[:], h.Sum(nil))

	if sum != p.Checksum {
		return nil, nil, fmt.Errorf(
			"persistent data checksum mismatch",
		)
	}

	return p.State, p.Snapshot, nil
}

// [新增] 统一的原子保存接口。
// 调用者必须保证 state 和 snapshot 对应同一 Raft 状态。
func (ps *Persister) SaveStateAndSnapshot(
	state []byte,
	snapshot []byte,
) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := encodePersistentData(state, snapshot)
	if err != nil {
		return err
	}

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

	// 原子替换旧文件。
	if err := os.Rename(tmpName, ps.path); err != nil {
		return err
	}

	// 确保目录项的修改持久化。
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()

	return dirFile.Sync()
}

// [修改] 仅更新 Raft State，保留原快照。
// 注意：如果此前已有旧格式持久化文件，必须先迁移。
func (ps *Persister) SaveRaftState(data []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	_, snapshot, err := ps.readLocked()
	if err != nil {
		return err
	}

	encoded, err := encodePersistentData(data, snapshot)
	if err != nil {
		return err
	}

	return ps.writeLocked(encoded)
}

// [新增] 内部写入函数，调用者已持有 ps.mu。
func (ps *Persister) writeLocked(data []byte) error {
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

	return dirFile.Sync()
}

// [新增] 读取同一持久化版本的 Raft State 与 Snapshot。
func (ps *Persister) ReadStateAndSnapshot() (
	[]byte, []byte, error,
) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.readLocked()
}

func (ps *Persister) readLocked() (
	[]byte, []byte, error,
) {
	data, err := os.ReadFile(ps.path)

	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	return decodePersistentData(data)
}

// [修改] 保留旧 API。
func (ps *Persister) ReadRaftState() ([]byte, error) {
	state, _, err := ps.ReadStateAndSnapshot()
	return state, err
}

// [新增] 单独读取快照。
func (ps *Persister) ReadSnapshot() ([]byte, error) {
	_, snapshot, err := ps.ReadStateAndSnapshot()
	return snapshot, err
}

// [新增] 供 KV 层判断何时需要触发快照。
// 返回 Raft State 的序列化大小，不包含 Snapshot。
func (ps *Persister) RaftStateSize() int {
	state, err := ps.ReadRaftState()
	if err != nil {
		panic(err)
	}
	return len(state)
}
