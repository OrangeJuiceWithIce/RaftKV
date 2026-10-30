package raft

import "time"

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

				rf.broadcastHeartbeat()
				continue
			}
		} else if !now.Before(rf.electionDeadline) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) broadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.sendHeartbeat(peer)
	}
}

func (rf *Raft) sendHeartbeat(peer int) {
	rf.mu.Lock()

	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	lastIndex, lastTerm := rf.lastLogInfoLocked()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.SendAppendEntries(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
}
