package raft

import "time"

func (rf *Raft) signalReplicatorsLocked() {
	for peer, cond := range rf.replicatedConds {
		if peer == rf.me {
			continue
		}

		cond.Signal()
	}
}

func (rf *Raft) replicator(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		for !rf.killed() && !rf.needReplicating(peer) {
			rf.replicatedConds[peer].Wait()
		}

		if rf.killed() {
			return
		}

		rf.replicateOneRoundLocked(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	last, _ := rf.lastLogInfoLocked()

	return rf.state == Leader &&
		rf.matchIndex[peer] < last
}

func (rf *Raft) replicateOneRoundLocked(server int) {
	term := rf.currentTerm
	first := rf.firstIndexLocked()
	next := rf.nextIndex[server]

	if next <= first {
		args := InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: first,
			LastIncludedTerm:  rf.logs[0].Term,
			Data:              append([]byte(nil), rf.snapshot...),
		}

		// 发送 RPC 前释放锁
		rf.mu.Unlock()

		var reply InstallSnapshotReply
		ok := rf.SendInstallSnapshot(server, &args, &reply)

		if !ok {
			select {
			case <-time.After(heartbeatInterval):
			case <-rf.stopCh:
			}
		}

		rf.mu.Lock()

		if !ok || rf.killed() {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.state != Leader || rf.currentTerm != term {
			return
		}

		// 只调整下一次尝试的复制位置
		rf.nextIndex[server] = max(
			rf.nextIndex[server],
			args.LastIncludedIndex+1,
		)

		return
	}

	prev := next - 1

	prevTerm, _ := rf.termAtLocked(prev)
	nextOffset := next - first

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      append([]LogEntry(nil), rf.logs[nextOffset:]...),
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply

	ok := rf.SendAppendEntries(server, &args, &reply)
	if !ok {
		select {
		case <-time.After(heartbeatInterval):
		case <-rf.stopCh:
		}
	}

	rf.mu.Lock()

	if !ok || rf.killed() {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	if rf.killed() || rf.state != Leader || rf.currentTerm != term {
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

		return
	}

	// if args.PrevLogIndex <= rf.matchIndex[server] || rf.nextIndex[server] != args.PrevLogIndex+1 {
	// 	return
	// }

	next = reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		for i := len(rf.logs) - 1; i >= 0; i-- {
			if rf.logs[i].Term == reply.ConflictTerm {
				next = rf.logs[i].Index + 1
				break
			}
		}
	}

	rf.nextIndex[server] = max(
		rf.matchIndex[server]+1,
		next,
	)
}

func (rf *Raft) updateCommitIndexLocked() {
	if rf.state != Leader {
		return
	}

	majority := len(rf.peers)/2 + 1
	last, _ := rf.lastLogInfoLocked()

	for n := last; n > rf.commitIndex; n-- {
		term, _ := rf.termAtLocked(n)
		if term != rf.currentTerm {
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
