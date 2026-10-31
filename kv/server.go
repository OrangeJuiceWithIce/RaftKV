package kvraft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrangeJuiceWithIce/raftkv/raft"
)

const (
	commandWaitTimeout = 1200 * time.Millisecond
)

type KVServer struct {
	mu sync.Mutex

	me int

	rf *raft.Raft

	applyCh chan raft.ApplyMsg

	persister    *raft.Persister
	maxRaftState int

	data map[string]string

	lastOperations map[int64]LastOperation

	lastApplied int

	notifyChans map[int]chan ApplyResult

	dead   atomic.Bool
	stopCh chan struct{}
}

func NewKVServer(
	me int,
	peers []*raft.PeerClient,
	ps *raft.Persister,
	maxRaftState int,
) (*KVServer, error) {

	if ps == nil {
		return nil, fmt.Errorf("nil Raft persister")
	}

	if maxRaftState != -1 && maxRaftState <= 0 {
		return nil, fmt.Errorf(
			"invalid maxRaftState: %d",
			maxRaftState,
		)
	}

	kv := &KVServer{
		me:           me,
		persister:    ps,
		maxRaftState: maxRaftState,

		applyCh: make(chan raft.ApplyMsg, 128),

		data:           make(map[string]string),
		lastOperations: make(map[int64]LastOperation),

		lastApplied: 0,

		notifyChans: make(map[int]chan ApplyResult),

		stopCh: make(chan struct{}),
	}

	snapshot, err := ps.ReadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	if len(snapshot) > 0 {
		state, err := decodeSnapshot(snapshot)
		if err != nil {
			return nil, err
		}

		kv.restoreSnapshotLocked(state)
	}

	rf, err := raft.NewRaft(
		me,
		peers,
		kv.applyCh,
		ps,
	)

	if err != nil {
		return nil, err
	}

	kv.rf = rf

	logs := rf.GetLogs()

	if len(logs) == 0 {
		rf.Kill()
		return nil, fmt.Errorf("empty Raft logs")
	}

	// Raft 的日志压缩边界必须和 KV Snapshot 恢复的索引一致。
	if logs[0].Index != kv.lastApplied {
		rf.Kill()
		return nil, fmt.Errorf(
			"snapshot boundary mismatch: "+
				"raft=%d, kv=%d",
			logs[0].Index,
			kv.lastApplied,
		)
	}

	go kv.applier()

	return kv, nil
}

func (kv *KVServer) Raft() *raft.Raft {
	return kv.rf
}

func (kv *KVServer) Kill() {
	if !kv.dead.CompareAndSwap(false, true) {
		return
	}

	close(kv.stopCh)

	if kv.rf != nil {
		kv.rf.Kill()
	}
}

func (kv *KVServer) Execute(
	ctx context.Context,
	cmd Command,
) CommandReply {
	if cmd.Op != OpGet &&
		cmd.Op != OpPut &&
		cmd.Op != OpAppend {
		return CommandReply{Err: ErrInvalidOp}
	}

	if ctx.Err() != nil {
		return CommandReply{Err: ErrTimeout}
	}

	if kv.dead.Load() {
		return CommandReply{Err: ErrStopped}
	}

	kv.mu.Lock()

	if kv.dead.Load() {
		kv.mu.Unlock()
		return CommandReply{Err: ErrStopped}
	}

	index, term, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		kv.mu.Unlock()
		return CommandReply{Err: ErrWrongLeader}
	}

	ch := make(chan ApplyResult, 1)

	kv.notifyChans[index] = ch

	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if kv.notifyChans[index] == ch {
			delete(kv.notifyChans, index)
		}

		kv.mu.Unlock()
	}()

	timer := time.NewTimer(commandWaitTimeout)
	defer timer.Stop()

	select {
	case result := <-ch:
		if result.Command != cmd {
			return CommandReply{Err: ErrWrongLeader}
		}

		currentTerm, leader := kv.rf.GetState()

		if !leader || currentTerm != term {
			return CommandReply{Err: ErrWrongLeader}
		}

		return result.Reply

	case <-timer.C:
		return CommandReply{Err: ErrTimeout}

	case <-ctx.Done():

		return CommandReply{Err: ErrTimeout}

	case <-kv.stopCh:
		return CommandReply{Err: ErrStopped}
	}
}

func (kv *KVServer) applier() {
	for {
		select {

		case <-kv.stopCh:
			return

		case msg, ok := <-kv.applyCh:
			if !ok {
				return
			}

			if msg.CommandValid {
				kv.applyLog(msg)
			}

			if msg.SnapshotValid {
				kv.applySnapshot(msg)
			}
		}
	}
}

func (kv *KVServer) applyLog(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	index := msg.CommandIndex

	// 1. 忽略已经应用过的日志。
	if index <= kv.lastApplied {
		return
	}

	// 2. 保证状态机按照日志索引顺序执行。
	if index != kv.lastApplied+1 {
		panic(fmt.Sprintf(
			"KVServer %d: unexpected log index %d, want %d",
			kv.me,
			index,
			kv.lastApplied+1,
		))
	}

	// 3. 从 Raft 日志中提取 KV 命令。
	cmd, ok := msg.Command.(Command)
	if !ok {
		panic(fmt.Sprintf(
			"KVServer %d: invalid command type %T",
			kv.me,
			msg.Command,
		))
	}

	// 4. 执行状态机，包含写请求去重。
	reply := kv.applyCommandLocked(cmd)

	// 5. 更新 KV 已应用的日志索引。
	kv.lastApplied = index

	// 6. 通知等待该日志结果的 RPC Handler。
	if ch, ok := kv.notifyChans[index]; ok {
		result := ApplyResult{
			Command: cmd,
			Reply:   reply,
		}

		// 每个日志索引仅发送一次结果。
		// notifyChans[index] 是容量为 1 的缓冲 channel。
		select {
		case ch <- result:
		default:
		}
	}

	if kv.maxRaftState > 0 {

		// Raft State 达到限制的 90% 时触发。
		threshold := kv.maxRaftState -
			kv.maxRaftState/10

		if kv.persister.RaftStateSize() >= threshold {

			// 8. KV 层负责编码状态机数据。
			// 包括 data、lastOperations、lastApplied。
			snapshot, err := kv.encodeSnapshotLocked()
			if err != nil {
				panic(fmt.Errorf(
					"KVServer %d: encode snapshot: %w",
					kv.me,
					err,
				))
			}

			// 9. Raft 层负责日志压缩和持久化。
			kv.rf.Snapshot(index, snapshot)
		}
	}
}

func (kv *KVServer) applyCommandLocked(
	cmd Command,
) CommandReply {

	if cmd.Op == OpPut || cmd.Op == OpAppend {
		if last, ok := kv.lastOperations[cmd.ClientID]; ok {
			if cmd.CommandID <= last.CommandID {
				return last.Reply
			}
		}
	}

	reply := CommandReply{Err: OK}

	switch cmd.Op {

	case OpGet:
		value, ok := kv.data[cmd.Key]

		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
		}

	case OpPut:
		kv.data[cmd.Key] = cmd.Value

	case OpAppend:
		kv.data[cmd.Key] += cmd.Value

	default:
		reply.Err = ErrInvalidOp
	}

	if cmd.Op == OpPut || cmd.Op == OpAppend {
		kv.lastOperations[cmd.ClientID] = LastOperation{
			CommandID: cmd.CommandID,
			Reply:     reply,
		}
	}

	return reply
}
