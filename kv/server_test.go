package kvraft

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/OrangeJuiceWithIce/raftkv/raft"
)

// 创建仅用于测试状态机的 KVServer。
// 不启动 Raft，不启动 gRPC。
func newTestKV() *KVServer {
	return &KVServer{
		data:           make(map[string]string),
		lastOperations: make(map[int64]LastOperation),
		notifyChans:    make(map[int]chan ApplyResult),
		maxRaftState:   -1,
	}
}

// 测试 Get、Put、Append 和写请求去重。
func TestKVStateMachine(t *testing.T) {
	kv := newTestKV()

	// 1. Put("x", "A")
	reply := kv.applyCommandLocked(Command{
		Op:        OpPut,
		Key:       "x",
		Value:     "A",
		ClientID:  1001,
		CommandID: 1,
	})

	if reply.Err != OK {
		t.Fatalf("Put failed: %+v", reply)
	}

	// 2. Append("x", "B")
	reply = kv.applyCommandLocked(Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "B",
		ClientID:  1001,
		CommandID: 2,
	})

	if reply.Err != OK {
		t.Fatalf("Append failed: %+v", reply)
	}

	// 3. 模拟客户端重试同一个 Append。
	reply = kv.applyCommandLocked(Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "B",
		ClientID:  1001,
		CommandID: 2,
	})

	if reply.Err != OK {
		t.Fatalf("duplicate request failed: %+v", reply)
	}

	// 重复 Append 不能执行两次。
	if got := kv.data["x"]; got != "AB" {
		t.Fatalf(
			"duplicate Append executed: got %q, want AB",
			got,
		)
	}

	// 4. Get("x")
	reply = kv.applyCommandLocked(Command{
		Op:        OpGet,
		Key:       "x",
		ClientID:  2001,
		CommandID: 1,
	})

	if reply.Err != OK || reply.Value != "AB" {
		t.Fatalf("Get returned unexpected result: %+v", reply)
	}

	// 5. 获取不存在的 Key。
	reply = kv.applyCommandLocked(Command{
		Op:        OpGet,
		Key:       "missing",
		ClientID:  2001,
		CommandID: 2,
	})

	if reply.Err != ErrNoKey {
		t.Fatalf("expected ErrNoKey, got %+v", reply)
	}

	// 6. 两个不同的客户端允许使用相同 CommandID。
	reply = kv.applyCommandLocked(Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "C",
		ClientID:  3001,
		CommandID: 1,
	})

	if reply.Err != OK || kv.data["x"] != "ABC" {
		t.Fatalf("different client dedup error: %+v", reply)
	}
}

// 验证 applyLog 按日志索引执行并通知 Handler。
func TestKVApplyLogAndNotify(t *testing.T) {
	kv := newTestKV()

	cmd := Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "A",
		ClientID:  1001,
		CommandID: 1,
	}

	// 模拟 Handler 为 Index 1 注册通知。
	ch := make(chan ApplyResult, 1)
	kv.notifyChans[1] = ch

	// 模拟 Raft 已经提交 Index 1。
	kv.applyLog(raft.ApplyMsg{
		CommandValid: true,
		CommandIndex: 1,
		Command:      cmd,
	})

	// 检查状态机。
	if kv.lastApplied != 1 {
		t.Fatalf("lastApplied=%d, want 1", kv.lastApplied)
	}

	if kv.data["x"] != "A" {
		t.Fatalf("data[x]=%q, want A", kv.data["x"])
	}

	// 检查 Handler 是否收到通知。
	select {
	case result := <-ch:
		if result.Command != cmd {
			t.Fatalf("unexpected command: %+v", result.Command)
		}
		if result.Reply.Err != OK {
			t.Fatalf("unexpected reply: %+v", result.Reply)
		}

	default:
		t.Fatal("KV applier did not notify Handler")
	}

	// 同一个请求再次出现在 Index 2。
	kv.applyLog(raft.ApplyMsg{
		CommandValid: true,
		CommandIndex: 2,
		Command:      cmd,
	})

	// 日志应用索引必须前进。
	if kv.lastApplied != 2 {
		t.Fatalf("lastApplied=%d, want 2", kv.lastApplied)
	}

	// 但是重复写请求不能修改两次数据。
	if kv.data["x"] != "A" {
		t.Fatalf("duplicate command executed: %q", kv.data["x"])
	}
}

// 测试 Snapshot 能否完整保存并恢复 KV 状态。
func TestKVSnapshotRoundTrip(t *testing.T) {
	kv := newTestKV()

	kv.applyCommandLocked(Command{
		Op:        OpPut,
		Key:       "x",
		Value:     "A",
		ClientID:  1001,
		CommandID: 1,
	})

	kv.applyCommandLocked(Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "B",
		ClientID:  1001,
		CommandID: 2,
	})

	// 模拟状态机已经执行到 Index 10。
	kv.lastApplied = 10

	// 1. 编码快照。
	snapshot, err := kv.encodeSnapshotLocked()
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}

	// 2. 解码快照。
	state, err := decodeSnapshot(snapshot)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}

	// 3. 创建新 KVServer，模拟重启恢复。
	recovered := newTestKV()
	recovered.restoreSnapshotLocked(state)

	if recovered.data["x"] != "AB" {
		t.Fatalf("recovered data=%q, want AB", recovered.data["x"])
	}

	if recovered.lastApplied != 10 {
		t.Fatalf(
			"recovered lastApplied=%d, want 10",
			recovered.lastApplied,
		)
	}

	last, ok := recovered.lastOperations[1001]
	if !ok || last.CommandID != 2 {
		t.Fatalf("dedup state not recovered: %+v", last)
	}

	// 4. 重新执行快照之前已经完成的 Append。
	recovered.applyCommandLocked(Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "B",
		ClientID:  1001,
		CommandID: 2,
	})

	// 快照恢复后依然必须能去重。
	if recovered.data["x"] != "AB" {
		t.Fatalf(
			"duplicate executed after recovery: %q",
			recovered.data["x"],
		)
	}

	// 5. 新命令仍然能够正常执行。
	recovered.applyCommandLocked(Command{
		Op:        OpAppend,
		Key:       "x",
		Value:     "C",
		ClientID:  1001,
		CommandID: 3,
	})

	if recovered.data["x"] != "ABC" {
		t.Fatalf("new command failed: %q", recovered.data["x"])
	}
}

// 测试 Persister 原子保存两份数据，
// 以及普通 Raft State 更新不会覆盖已有 Snapshot。
func TestKVSnapshotPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.gob")

	ps, err := raft.NewPersister(path)
	if err != nil {
		t.Fatal(err)
	}

	kv := newTestKV()
	kv.data["x"] = "AB"
	kv.lastApplied = 10
	kv.lastOperations[1001] = LastOperation{
		CommandID: 2,
		Reply:     CommandReply{Err: OK},
	}

	snapshot, err := kv.encodeSnapshotLocked()
	if err != nil {
		t.Fatal(err)
	}

	// 这里用测试字节模拟已经编码好的 Raft State。
	raftState1 := []byte("raft-state-v1")

	// 1. 同时保存 Raft State 和 KV Snapshot。
	if err := ps.SaveStateAndSnapshot(
		raftState1,
		snapshot,
	); err != nil {
		t.Fatal(err)
	}

	// 2. 模拟 Raft 后续发生任期或日志变化，
	// 仅更新 Raft State。
	raftState2 := []byte("raft-state-v2")

	if err := ps.SaveRaftState(raftState2); err != nil {
		t.Fatal(err)
	}

	// 3. 重新从磁盘读取。
	gotState, gotSnapshot, err := ps.ReadStateAndSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Raft State 应该是最新版本。
	if !bytes.Equal(gotState, raftState2) {
		t.Fatal("Raft state was not updated")
	}

	// Snapshot 应该仍然完整保留。
	if !bytes.Equal(gotSnapshot, snapshot) {
		t.Fatal("snapshot lost after SaveRaftState")
	}

	// 4. 验证磁盘上的 Snapshot 能真正恢复业务数据。
	decoded, err := decodeSnapshot(gotSnapshot)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Data["x"] != "AB" ||
		decoded.LastApplied != 10 ||
		decoded.LastOperations[1001].CommandID != 2 {

		t.Fatalf("invalid persisted KV state: %+v", decoded)
	}
}
