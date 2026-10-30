package raft

import (
	"context"
	"time"
)

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

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	first := rf.firstIndexLocked()

	// 已经压缩过，或者超出已提交范围
	if index <= first || index > rf.commitIndex {
		return
	}

	offset, ok := rf.indexToOffsetLocked(index)
	if !ok {
		return
	}

	// 保留 Snapshot 边界日志的 Index 和 Term
	// 将其转化为新的哨兵日志
	newLogs := make([]LogEntry, 1, len(rf.logs)-offset)

	newLogs[0] = LogEntry{
		Index: index,
		Term:  rf.logs[offset].Term,
	}

	// 保留尚未被快照覆盖的后续日志
	newLogs = append(newLogs, rf.logs[offset+1:]...)

	rf.logs = newLogs

	// 状态机已应用到 index，保证本地应用边界不落后于快照
	rf.lastApplied = max(rf.lastApplied, index)

	// 保存快照数据
	rf.snapshot = append([]byte(nil), snapshot...)

	// 原子持久化 Raft 状态和 Snapshot
	rf.persistLocked()
}

func (rf *Raft) SendInstallSnapshot(
	id int,
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) bool {
	p := rf.peers[id]
	if p == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	result, err := p.InstallSnapshot(ctx, args)
	if err != nil {
		return false
	}

	*reply = *result
	return true
}

func (rf *Raft) HandleInstallSnapshot(
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		reply.Term = rf.currentTerm
	}()

	if rf.killed() || args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()

	// 已经提交到该位置，不需要安装旧快照
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot: append(
			[]byte(nil),
			args.Data...,
		),
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	// 异步通知状态机，不阻塞 RPC Handler
	go func() {
		select {
		case rf.applyCh <- msg:
		case <-rf.stopCh:
		}
	}()
}

func (rf *Raft) CondInstallSnapshot(
	lastIncludedTerm int,
	lastIncludedIndex int,
	snapshot []byte,
) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 快照已经过期
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	// 创建新的哨兵日志
	newLogs := []LogEntry{
		{
			Index: lastIncludedIndex,
			Term:  lastIncludedTerm,
		},
	}

	// 判断能否保留快照边界之后的日志
	offset, ok := rf.indexToOffsetLocked(lastIncludedIndex)

	if ok && rf.logs[offset].Term == lastIncludedTerm {
		newLogs = append(
			newLogs,
			rf.logs[offset+1:]...,
		)
	}

	// 更新日志
	rf.logs = newLogs

	// 更新提交和应用进度
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	// 保存状态机快照
	rf.snapshot = append([]byte(nil), snapshot...)

	// 原子持久化 Raft 状态和 Snapshot
	rf.persistLocked()

	return true
}
