package raft

import (
	"context"
	"math/rand"
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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
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
		rf.persistLocked()

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

	first := rf.firstIndexLocked()
	last := rf.lastIndexLocked()

	if args.PrevLogIndex < first {
		reply.ConflictIndex = first + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1
		reply.ConflictTerm = -1
		return
	}

	term, _ := rf.termAtLocked(args.PrevLogIndex)
	offset, _ := rf.indexToOffsetLocked(args.PrevLogIndex)

	if term != args.PrevLogTerm {
		for offset > 0 && term == rf.logs[offset-1].Term {
			offset--
		}

		reply.ConflictTerm = term
		reply.ConflictIndex = rf.logs[offset].Index
		return
	}

	start := offset + 1
	i := 0
	for i < len(args.Entries) && start+i < len(rf.logs) && rf.logs[start+i].Term == args.Entries[i].Term {
		i++
	}

	if i < len(args.Entries) {
		rf.logs = append(rf.logs[:start+i], args.Entries[i:]...)
		rf.persistLocked()
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

	rf.persistLocked()

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

	rf.signalReplicatorsLocked()
	rf.lastHeartbeat = time.Now()
	go rf.broadcastHeartbeat()
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		return
	}

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1

		rf.persistLocked()
	}

	rf.state = Follower
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionDeadline = time.Now().Add(electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutJitter))))
}
