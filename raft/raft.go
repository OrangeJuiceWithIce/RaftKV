package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

type LogEntry struct {
	Term    int
	Command interface{}
}
type RequestVoteArgs struct{ Term, CandidateId, LastLogIndex, LastLogTerm int }
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Entries                                   []LogEntry
	LeaderCommit                              int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	commitIndex, lastApplied int

	nextIndex, matchIndex []int

	state                           Role
	electionDeadline, lastHeartbeat time.Time

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	stopCh chan struct{}
}

func NewRaft(me int, peers []*PeerClient, applyCh chan ApplyMsg) (*Raft, error) {
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

	rf := &Raft{
		me:         me,
		peers:      peers,
		votedFor:   -1,
		logs:       []LogEntry{{Term: 0}},
		state:      Follower,
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		applyCh:    applyCh,
		stopCh:     make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimerLocked()

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

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionDeadline = time.Now().Add(electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutJitter))))
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		return
	}

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}

	rf.state = Follower
}

func (rf *Raft) lastLogInfoLocked() (int, int) {
	i := len(rf.logs) - 1
	return i, rf.logs[i].Term
}

func (rf *Raft) isLogUpToDateLocked(index, term int) bool {
	i, t := rf.lastLogInfoLocked()
	return term > t || (term == t && index >= i)
}

// handler
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if rf.killed() || args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	}
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if rf.killed() || args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionTimerLocked()

	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) {
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	start := args.PrevLogIndex + 1
	i := 0
	for i < len(args.Entries) && start+i < len(rf.logs) && rf.logs[start+i].Term == args.Entries[i].Term {
		i++
	}

	if i < len(args.Entries) {
		rf.logs = append(rf.logs[:start+i], args.Entries[i:]...)
	}

	reply.Success = true
	matched := args.PrevLogIndex + len(args.Entries)
	commit := min(args.LeaderCommit, matched)
	if commit > rf.commitIndex {
		rf.commitIndex = commit
		rf.applyCond.Signal()
	}
}

// sender
func (rf *Raft) SendRequestVote(id int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	p := rf.peers[id]
	if p == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	result, err := p.RequestVote(ctx, args)
	if err != nil {
		return false
	}
	*reply = *result
	return true
}

func (rf *Raft) SendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	p := rf.peers[id]
	if p == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	result, err := p.AppendEntries(ctx, args)
	if err != nil {
		return false
	}
	*reply = *result
	return true
}

func (rf *Raft) startElectionLocked() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me

	rf.resetElectionTimerLocked()

	term := rf.currentTerm
	index, lastTerm := rf.lastLogInfoLocked()

	votes := 1
	majority := len(rf.peers)/2 + 1

	if votes >= majority {
		rf.becomeLeaderLocked()
		return
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  lastTerm,
	}

	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply

			if !rf.SendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority {
					rf.becomeLeaderLocked()
				}
			}
		}(id)
	}
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	last, _ := rf.lastLogInfoLocked()
	for id := range rf.peers {
		rf.nextIndex[id] = last + 1
		rf.matchIndex[id] = 0
	}
	rf.nextIndex[rf.me] = last + 1
	rf.matchIndex[rf.me] = last
	rf.lastHeartbeat = time.Now()
	go rf.broadcastAppendEntries()
}

func (rf *Raft) broadcastAppendEntries() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		go rf.replicateToPeer(id)
	}
}

func (rf *Raft) replicateToPeer(server int) {
	rf.mu.Lock()

	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	next := rf.nextIndex[server]
	prev := next - 1

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.logs[prev].Term,
		Entries:      append([]LogEntry(nil), rf.logs[next:]...),
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.SendAppendEntries(server, &args, &reply) {
		return
	}

	rf.mu.Lock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		rf.mu.Unlock()
		return
	}

	if rf.killed() || rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		matched := args.PrevLogIndex + len(args.Entries)

		if matched > rf.matchIndex[server] {
			rf.matchIndex[server] = matched
		}

		if matched+1 > rf.nextIndex[server] {
			rf.nextIndex[server] = matched + 1
		}
		rf.updateCommitIndexLocked()

		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex <= rf.matchIndex[server] || rf.nextIndex[server] != args.PrevLogIndex+1 {
		rf.mu.Unlock()
		return
	}

	next = rf.nextIndex[server] - 1
	if next < 1 {
		next = 1
	}

	rf.nextIndex[server] = next
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitIndexLocked() {
	if rf.state != Leader {
		return
	}

	majority := len(rf.peers)/2 + 1

	for n := len(rf.logs) - 1; n > rf.commitIndex; n-- {
		if rf.logs[n].Term != rf.currentTerm {
			continue
		}

		count := 0
		for _, matched := range rf.matchIndex {
			if matched >= n {
				count++
			}
		}

		if count >= majority {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

func (rf *Raft) ticker() {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-rf.stopCh:
			return
		case <-t.C:
		}

		now := time.Now()
		rf.mu.Lock()

		if rf.state == Leader {
			if now.Sub(rf.lastHeartbeat) >= heartbeatInterval {
				rf.lastHeartbeat = now
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
				continue
			}
		} else if !now.Before(rf.electionDeadline) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
	}
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
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[next].Command,
			CommandIndex: next,
		}

		rf.mu.Unlock()

		select {
		case rf.applyCh <- msg:
		case <-rf.stopCh:
			return
		}

		rf.mu.Lock()
		rf.lastApplied = next
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

	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})

	index := len(rf.logs) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.updateCommitIndexLocked()
	rf.mu.Unlock()

	go rf.broadcastAppendEntries()
	return index, term, true
}

func (rf *Raft) Kill() {
	if atomic.CompareAndSwapInt32(&rf.dead, 0, 1) {
		close(rf.stopCh)
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
