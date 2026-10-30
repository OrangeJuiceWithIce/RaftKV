package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (rf *Raft) firstLogInfoLocked() (int, int) {
	first := rf.logs[0]
	return first.Index, first.Term
}

func (rf *Raft) lastLogInfoLocked() (int, int) {
	last := rf.logs[len(rf.logs)-1]
	return last.Index, last.Term
}

func (rf *Raft) isLogUpToDateLocked(lastIndex, lastTerm int) bool {
	myIndex, myTerm := rf.lastLogInfoLocked()

	if lastTerm != myTerm {
		return lastTerm > myTerm
	}

	return lastIndex >= myIndex
}

func (rf *Raft) firstIndexLocked() int {
	first := rf.logs[0]
	return first.Index
}

func (rf *Raft) lastIndexLocked() int {
	last := rf.logs[len(rf.logs)-1]
	return last.Index
}

func (rf *Raft) indexToOffsetLocked(index int) (int, bool) {
	first, _ := rf.firstLogInfoLocked()
	last, _ := rf.lastLogInfoLocked()
	if index < first || index > last {
		return 0, false
	}
	return index - first, true
}

func (rf *Raft) termAtLocked(index int) (int, bool) {
	offset, ok := rf.indexToOffsetLocked(index)
	if !ok {
		return 0, false
	}
	return rf.logs[offset].Term, true
}
