package kvraft

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/OrangeJuiceWithIce/raftkv/raft"
)

type snapshotState struct {
	Data           map[string]string
	LastOperations map[int64]LastOperation
	LastApplied    int
}

func (kv *KVServer) encodeSnapshotLocked() ([]byte, error) {
	state := snapshotState{
		Data:           kv.data,
		LastOperations: kv.lastOperations,
		LastApplied:    kv.lastApplied,
	}

	var buf bytes.Buffer

	encoder := gob.NewEncoder(&buf)

	if err := encoder.Encode(&state); err != nil {
		return nil, fmt.Errorf("encode KV snapshot: %w", err)
	}

	return buf.Bytes(), nil
}

func decodeSnapshot(data []byte) (snapshotState, error) {
	var state snapshotState

	if len(data) == 0 {
		return state, fmt.Errorf("empty KV snapshot")
	}

	decoder := gob.NewDecoder(bytes.NewReader(data))

	if err := decoder.Decode(&state); err != nil {
		return state, fmt.Errorf("decode KV snapshot: %w", err)
	}

	if state.LastApplied <= 0 {
		return state, fmt.Errorf(
			"invalid snapshot index: %d",
			state.LastApplied,
		)
	}

	if state.Data == nil {
		state.Data = make(map[string]string)
	}

	if state.LastOperations == nil {
		state.LastOperations = make(map[int64]LastOperation)
	}

	return state, nil
}

func (kv *KVServer) restoreSnapshotLocked(
	state snapshotState,
) {
	kv.data = state.Data
	kv.lastOperations = state.LastOperations
	kv.lastApplied = state.LastApplied
}

// applySnapshot 处理 Raft 发送的 SnapshotValid 消息。
func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {

	// 1. 先解码，确认快照数据合法。
	state, err := decodeSnapshot(msg.Snapshot)
	if err != nil {
		panic(fmt.Errorf(
			"KVServer %d: invalid snapshot: %w",
			kv.me,
			err,
		))
	}

	// Snapshot 内部记录的应用位置必须与
	// Raft ApplyMsg 中的索引一致。
	if state.LastApplied != msg.SnapshotIndex {
		panic(fmt.Errorf(
			"KVServer %d: snapshot index mismatch: "+
				"state=%d, raft=%d",
			kv.me,
			state.LastApplied,
			msg.SnapshotIndex,
		))
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 2. 旧快照不允许覆盖新状态。
	if msg.SnapshotIndex <= kv.lastApplied {
		return
	}

	// 3. 让 Raft 判断能否安装该快照。
	//
	// 如果快照已过期，Raft 会拒绝。
	// 如果安装成功，Raft 会更新日志压缩边界、
	// commitIndex、lastApplied，并持久化状态。
	if !kv.rf.CondInstallSnapshot(
		msg.SnapshotTerm,
		msg.SnapshotIndex,
		msg.Snapshot,
	) {
		return
	}

	// 4. Raft 已经接受快照，
	// 现在恢复 KV 状态机。
	kv.restoreSnapshotLocked(state)

	// 5. 清理已经被快照覆盖的等待请求。
	//
	// 不能假设这些等待请求执行成功，
	// 应通知它们重试。
	for index, ch := range kv.notifyChans {

		if index <= kv.lastApplied {

			// 零值 Command 不可能与正常业务命令匹配。
			// Handler 收到后会返回 ErrWrongLeader。
			result := ApplyResult{
				Reply: CommandReply{
					Err: ErrWrongLeader,
				},
			}

			select {
			case ch <- result:
			default:
			}

			delete(kv.notifyChans, index)
		}
	}
}
