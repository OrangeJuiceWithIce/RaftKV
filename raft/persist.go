package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type raftPersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry
}

func (rf *Raft) persistLocked() {
	state := raftPersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Logs:        rf.logs,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&state); err != nil {
		panic(fmt.Errorf("raft: encode state: %w", err))
	}

	// 修改：同时保存 Raft State 和 Snapshot
	if err := rf.persister.SaveStateAndSnapshot(
		buf.Bytes(),
		rf.snapshot,
	); err != nil {
		panic(fmt.Errorf("raft: persist state and snapshot: %w", err))
	}
}

func (rf *Raft) readPersist() error {
	data, snapshot, err := rf.persister.ReadStateAndSnapshot()
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var state raftPersistentState
	if err := gob.NewDecoder(
		bytes.NewReader(data),
	).Decode(&state); err != nil {
		return fmt.Errorf("decode raft state: %w", err)
	}

	if len(state.Logs) == 0 {
		return fmt.Errorf("invalid raft state: empty logs")
	}

	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.logs = state.Logs
	rf.snapshot = append([]byte(nil), snapshot...)

	first := rf.firstIndexLocked()
	rf.commitIndex = first
	rf.lastApplied = first

	return nil
}
