package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

const (
	heartbeatInterval     = 100 * time.Millisecond
	electionTimeoutMin    = 500 * time.Millisecond
	electionTimeoutJitter = 400 * time.Millisecond
	rpcTimeout            = 300 * time.Millisecond
)

type Raft struct {
	mu sync.Mutex

	me    int
	peers []*PeerClient
	dead  int32

	logs        []LogEntry
	currentTerm int
	votedFor    int

	persister *Persister

	commitIndex, lastApplied int

	nextIndex, matchIndex []int

	state                           Role
	electionDeadline, lastHeartbeat time.Time

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	replicatedConds []*sync.Cond
	stopCh          chan struct{}

	snapshot []byte
}

func NewRaft(me int, peers []*PeerClient, applyCh chan ApplyMsg, persister *Persister) (*Raft, error) {
	n := len(peers)

	if n == 0 {
		return nil, fmt.Errorf("peers is empty")
	}

	if me < 0 || me >= n {
		return nil, fmt.Errorf("invalid me=%d", me)
	}

	if applyCh == nil {
		return nil, fmt.Errorf("nil applyCh")
	}

	if persister == nil {
		return nil, fmt.Errorf("nil persister")
	}

	rf := &Raft{
		me:              me,
		peers:           peers,
		votedFor:        -1,
		logs:            []LogEntry{{Index: 0, Term: 0}},
		persister:       persister,
		state:           Follower,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
		replicatedConds: make([]*sync.Cond, n),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	if err := rf.readPersist(); err != nil {
		return nil, fmt.Errorf("read raft state: %w", err)
	}

	rf.resetElectionTimerLocked()

	for peer := range peers {
		if peer == rf.me {
			continue
		}

		rf.replicatedConds[peer] = sync.NewCond(&rf.mu)
		go rf.replicator(peer)
	}

	go rf.ticker()
	go rf.applier()

	return rf, nil
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader && !rf.killed()
}

func (rf *Raft) Status() (int, Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state
}

func (rf *Raft) GetLogs() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return append([]LogEntry(nil), rf.logs...)
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		next := rf.lastApplied + 1
		offset, _ := rf.indexToOffsetLocked(next)

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[offset].Command,
			CommandIndex: next,
		}

		rf.mu.Unlock()

		select {
		case rf.applyCh <- msg:
		case <-rf.stopCh:
			return
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, next)
		rf.mu.Unlock()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	term := rf.currentTerm
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}

	lastIndex := rf.lastIndexLocked()
	index := lastIndex + 1
	rf.logs = append(rf.logs, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.persistLocked()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.updateCommitIndexLocked()
	rf.signalReplicatorsLocked()
	rf.mu.Unlock()

	return index, term, true
}

func (rf *Raft) Kill() {
	if atomic.CompareAndSwapInt32(&rf.dead, 0, 1) {
		close(rf.stopCh)
		rf.mu.Lock()

		rf.applyCond.Broadcast()

		for peer, cond := range rf.replicatedConds {
			if peer != rf.me && cond != nil {
				cond.Broadcast()
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
