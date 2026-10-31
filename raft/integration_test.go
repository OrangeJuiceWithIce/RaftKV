package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/OrangeJuiceWithIce/raftkv/raft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testOp struct{ Kind, Key, Value string }

func init() { gob.Register(testOp{}) }

// blockEntries 只阻止携带 Entries 的 AppendEntries，允许空心跳重置选举超时。
type testRPCServer struct {
	*GRPCServer
	blockEntries atomic.Bool
}

func (s *testRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.blockEntries.Load() && len(req.Entries) > 0 {
		return nil, status.Error(codes.Unavailable, "simulated log replication failure")
	}
	return s.GRPCServer.AppendEntries(ctx, req)
}

// 伪 KV 状态机：单 goroutine 消费 ApplyMsg，普通日志要求严格连续。
// 快照消息由该 goroutine 调用 CondInstallSnapshot，安装后忽略迟到的旧日志。
type applyRecorder struct {
	mu                sync.Mutex
	rf                *Raft
	entries           []ApplyMsg
	lastApplied       int
	installedSnapshot int
	err               error
}

func (r *applyRecorder) attachRaft(rf *Raft) {
	r.mu.Lock()
	r.rf = rf
	r.mu.Unlock()
}

func (r *applyRecorder) record(msg ApplyMsg) {
	if msg.SnapshotValid {
		r.mu.Lock()
		if r.err != nil || msg.SnapshotIndex <= r.lastApplied {
			r.mu.Unlock()
			return
		}
		rf := r.rf
		r.mu.Unlock()
		if rf == nil {
			r.mu.Lock()
			r.err = fmt.Errorf("snapshot recorder has no Raft instance")
			r.mu.Unlock()
			return
		}
		// 不持 recorder 锁调用 Raft，避免锁序反转。
		if !rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
			return
		}
		r.mu.Lock()
		r.lastApplied = msg.SnapshotIndex
		r.installedSnapshot = msg.SnapshotIndex
		r.mu.Unlock()
		return
	}
	if !msg.CommandValid {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil || msg.CommandIndex <= r.lastApplied {
		return
	}
	expected := r.lastApplied + 1
	if msg.CommandIndex != expected {
		r.err = fmt.Errorf("apply order broken: expected index %d, got %d", expected, msg.CommandIndex)
		return
	}
	r.entries = append(r.entries, msg)
	r.lastApplied = msg.CommandIndex
}

func (r *applyRecorder) snapshot(index int) (ApplyMsg, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return ApplyMsg{}, false, r.err
	}
	for _, msg := range r.entries {
		if msg.CommandIndex == index {
			return msg, true, nil
		}
	}
	return ApplyMsg{}, false, nil
}

func (r *applyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

func (r *applyRecorder) installedSnapshotIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.installedSnapshot
}

type testCluster struct {
	t       *testing.T
	rf      []*Raft
	rpc     []*testRPCServer
	servers []*grpc.Server
	clients [][]*PeerClient
	record  []*applyRecorder
	paths   []string
	stop    chan struct{}
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{
		t:  t,
		rf: make([]*Raft, n), rpc: make([]*testRPCServer, n),
		servers: make([]*grpc.Server, n), clients: make([][]*PeerClient, n),
		record: make([]*applyRecorder, n), paths: make([]string, n),
		stop: make(chan struct{}),
	}
	listeners := make([]net.Listener, n)
	addresses := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addresses[i] = ln.Addr().String()
	}
	t.Cleanup(func() {
		for _, rf := range c.rf {
			if rf != nil {
				rf.Kill()
			}
		}
		close(c.stop)
		for _, s := range c.servers {
			if s != nil {
				s.Stop()
			}
		}
		for _, peers := range c.clients {
			for _, p := range peers {
				if p != nil {
					_ = p.Close()
				}
			}
		}
		for _, ln := range listeners {
			if ln != nil {
				_ = ln.Close()
			}
		}
	})

	stateDir := t.TempDir()
	for i := range c.paths {
		c.paths[i] = filepath.Join(stateDir, fmt.Sprintf("node-%d", i), "state.gob")
	}
	for i := 0; i < n; i++ {
		c.clients[i] = make([]*PeerClient, n)
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			p, err := NewPeerClient(addresses[j])
			if err != nil {
				t.Fatal(err)
			}
			c.clients[i][j] = p
		}
		ch := make(chan ApplyMsg, 128)
		c.record[i] = &applyRecorder{}
		rec := c.record[i]
		go func() {
			for {
				select {
				case msg := <-ch:
					rec.record(msg)
				case <-c.stop:
					return
				}
			}
		}()
		ps, err := NewPersister(c.paths[i])
		if err != nil {
			t.Fatal(err)
		}
		rf, err := NewRaft(i, c.clients[i], ch, ps)
		if err != nil {
			t.Fatal(err)
		}
		c.rf[i] = rf
		c.record[i].attachRaft(rf)
		c.rpc[i] = &testRPCServer{GRPCServer: NewGRPCServer(rf)}
		c.servers[i] = grpc.NewServer()
		pb.RegisterRaftServiceServer(c.servers[i], c.rpc[i])
	}
	for i := 0; i < n; i++ {
		server, ln := c.servers[i], listeners[i]
		go func() { _ = server.Serve(ln) }()
	}
	return c
}

func (c *testCluster) leader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		who, count := -1, 0
		for i, rf := range c.rf {
			_, ok := rf.GetState()
			if ok {
				who = i
				count++
			}
		}
		if count == 1 {
			return who
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("timeout waiting for exactly one leader")
	return -1
}

func (c *testCluster) waitApplied(node, index int, command any, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, found, err := c.record[node].snapshot(index)
		if err != nil {
			c.t.Fatalf("node %d: %v", node, err)
		}
		if found {
			if !reflect.DeepEqual(msg.Command, command) {
				c.t.Fatalf("node %d index %d: got (%T)%#v, want (%T)%#v", node, index, msg.Command, msg.Command, command, command)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %d: timeout waiting for index %d (recorded %d)", node, index, c.record[node].count())
}

func (c *testCluster) start(command any) (int, int) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader := c.leader(4 * time.Second)
		index, _, ok := c.rf[leader].Start(command)
		if ok {
			return leader, index
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("could not start command")
	return -1, -1
}

var _ pb.RaftServiceServer = (*testRPCServer)(nil)

// ====================== 原有测试 1~11 ======================

func TestRaftLeaderElection(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader(5 * time.Second)
	for i := 0; i < 5; i++ {
		time.Sleep(120 * time.Millisecond)
		if got := c.leader(2 * time.Second); got != leader {
			t.Fatalf("leader changed unexpectedly: %d -> %d", leader, got)
		}
	}
}

func TestRaftBasicAgreement(t *testing.T) {
	c := newTestCluster(t, 3)
	_, index := c.start("first-command")
	if index != 1 {
		t.Fatalf("first index=%d, want 1", index)
	}
	for i := range c.rf {
		c.waitApplied(i, index, "first-command", 5*time.Second)
	}
}

func TestRaftGobCommand(t *testing.T) {
	c := newTestCluster(t, 3)
	cmd := testOp{Kind: "Put", Key: "name", Value: "Alice"}
	_, index := c.start(cmd)
	for i := range c.rf {
		c.waitApplied(i, index, cmd, 5*time.Second)
	}
}

func TestRaftFollowerCatchUp(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader(5 * time.Second)
	slow, other := (leader+1)%3, (leader+2)%3
	c.rpc[slow].blockEntries.Store(true)
	index1, _, ok := c.rf[leader].Start("one")
	if !ok {
		t.Fatal("leader rejected first Start")
	}
	index2, _, ok := c.rf[leader].Start("two")
	if !ok {
		t.Fatal("leader rejected second Start")
	}
	if index2 != index1+1 {
		t.Fatalf("non-consecutive indexes: %d, %d", index1, index2)
	}
	for _, id := range []int{leader, other} {
		c.waitApplied(id, index1, "one", 5*time.Second)
		c.waitApplied(id, index2, "two", 5*time.Second)
	}
	if count := c.record[slow].count(); count != 0 {
		t.Fatalf("slow follower applied %d entries before recovery", count)
	}
	c.rpc[slow].blockEntries.Store(false)
	c.waitApplied(slow, index1, "one", 5*time.Second)
	c.waitApplied(slow, index2, "two", 5*time.Second)
}

func TestRaftLeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)
	oldLeader := c.leader(5 * time.Second)
	c.rf[oldLeader].Kill()
	c.servers[oldLeader].Stop()
	deadline := time.Now().Add(6 * time.Second)
	newLeader := -1
	for time.Now().Before(deadline) {
		for i, rf := range c.rf {
			if i == oldLeader {
				continue
			}
			_, ok := rf.GetState()
			if ok {
				newLeader = i
				break
			}
		}
		if newLeader >= 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader < 0 {
		t.Fatal("remaining nodes did not elect a leader")
	}
	index, _, ok := c.rf[newLeader].Start("after-failover")
	if !ok {
		t.Fatal("new leader rejected Start")
	}
	for i := range c.rf {
		if i != oldLeader {
			c.waitApplied(i, index, "after-failover", 5*time.Second)
		}
	}
}

func TestRaftConcurrentStart(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader(5 * time.Second)
	const commands = 10
	var mu sync.Mutex
	var wg sync.WaitGroup
	expected := make(map[int]string, commands)
	for i := 0; i < commands; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := fmt.Sprintf("command-%d", i)
			index, _, ok := c.rf[leader].Start(cmd)
			if !ok {
				t.Errorf("Start rejected command %q", cmd)
				return
			}
			mu.Lock()
			expected[index] = cmd
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if len(expected) != commands {
		t.Fatalf("accepted %d commands, want %d", len(expected), commands)
	}
	for node := range c.rf {
		for index := 1; index <= commands; index++ {
			c.waitApplied(node, index, expected[index], 5*time.Second)
		}
	}
}

func newDiskNodeForIntegration(t *testing.T, path string) *Raft {
	t.Helper()
	ps, err := NewPersister(path)
	if err != nil {
		t.Fatal(err)
	}
	rf, err := NewRaft(0, []*PeerClient{nil}, make(chan ApplyMsg, 128), ps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rf.Kill)
	return rf
}

func TestRaftDiskVoteSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	rf := newDiskNodeForIntegration(t, path)
	var first RequestVoteReply
	rf.HandleRequestVote(&RequestVoteArgs{Term: 12, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0}, &first)
	if !first.VoteGranted || first.Term != 12 {
		t.Fatalf("initial vote: %+v", first)
	}
	rf.Kill()
	restarted := newDiskNodeForIntegration(t, path)
	restarted.mu.Lock()
	term, voted, role := restarted.currentTerm, restarted.votedFor, restarted.state
	restarted.mu.Unlock()
	if term != 12 || voted != 1 || role != Follower {
		t.Fatalf("recovered term=%d vote=%d role=%v; want term=12 vote=1 follower", term, voted, role)
	}
	var again RequestVoteReply
	restarted.HandleRequestVote(&RequestVoteArgs{Term: 12, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}, &again)
	if again.VoteGranted {
		t.Fatalf("double voting after restart: %+v", again)
	}
}

func TestRaftDiskHigherTermOnRejectedAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	rf := newDiskNodeForIntegration(t, path)
	var reply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{Term: 17, LeaderId: 1, PrevLogIndex: 99, PrevLogTerm: 17}, &reply)
	if reply.Success || reply.Term != 17 {
		t.Fatalf("expected rejection with updated term: %+v", reply)
	}
	rf.Kill()
	restarted := newDiskNodeForIntegration(t, path)
	term, _ := restarted.GetState()
	if term != 17 {
		t.Fatalf("term lost: got %d want 17", term)
	}
}

func TestRaftDiskConflictOverwriteSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	rf := newDiskNodeForIntegration(t, path)
	var reply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 3, Command: "old-1"},
			{Index: 2, Term: 3, Command: "old-2"},
		},
	}, &reply)
	if !reply.Success {
		t.Fatalf("initial append rejected: %+v", reply)
	}
	rf.Kill()
	rf2 := newDiskNodeForIntegration(t, path)
	if got := rf2.GetLogs(); len(got) != 3 || got[2].Command != "old-2" {
		t.Fatalf("initial logs not recovered: %#v", got)
	}
	reply = AppendEntriesReply{}
	rf2.HandleAppendEntries(&AppendEntriesArgs{
		Term: 4, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 4, Command: "new-1"},
			{Index: 2, Term: 4, Command: "new-2"},
		},
	}, &reply)
	if !reply.Success {
		t.Fatalf("conflict overwrite rejected: %+v", reply)
	}
	rf2.Kill()
	rf3 := newDiskNodeForIntegration(t, path)
	want := []LogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 4, Command: "new-1"},
		{Index: 2, Term: 4, Command: "new-2"},
	}
	if got := rf3.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("logs after restart: got %#v, want %#v", got, want)
	}
}

func TestRaftDiskAgreementDurable(t *testing.T) {
	c := newTestCluster(t, 3)
	cmd := testOp{Kind: "Put", Key: "persist", Value: "value"}
	leader, index := c.start(cmd)
	ps, err := NewPersister(c.paths[leader])
	if err != nil {
		t.Fatal(err)
	}
	probe := &Raft{persister: ps, votedFor: -1, logs: []LogEntry{{Index: 0, Term: 0}}}
	if err := probe.readPersist(); err != nil {
		t.Fatal(err)
	}
	if index >= len(probe.logs) || !reflect.DeepEqual(probe.logs[index].Command, cmd) {
		t.Fatalf("leader Start did not persist command at index %d: %#v", index, probe.logs)
	}
	for i := range c.rf {
		c.waitApplied(i, index, cmd, 5*time.Second)
	}
	for i, path := range c.paths {
		ps, err := NewPersister(path)
		if err != nil {
			t.Fatal(err)
		}
		probe := &Raft{persister: ps, votedFor: -1, logs: []LogEntry{{Index: 0, Term: 0}}}
		if err := probe.readPersist(); err != nil {
			t.Fatal(err)
		}
		if index >= len(probe.logs) || !reflect.DeepEqual(probe.logs[index].Command, cmd) {
			t.Fatalf("node %d disk missing command at index %d: %#v", i, index, probe.logs)
		}
	}
}

func TestRaftDiskCorruptStateRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("invalid gob bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	ps, err := NewPersister(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRaft(0, []*PeerClient{nil}, make(chan ApplyMsg, 16), ps); err == nil {
		t.Fatal("corrupted Raft state should produce an error")
	}
}

// ====================== Snapshot 测试 12~18 ======================

func newSnapshotTestNode(t *testing.T, path string) (*Raft, chan ApplyMsg) {
	t.Helper()
	ps, err := NewPersister(path)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan ApplyMsg, 128)
	rf, err := NewRaft(0, []*PeerClient{nil}, ch, ps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rf.Kill)
	return rf, ch
}

func snapshotWaitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func snapshotRaftProgress(rf *Raft) (first, last, committed, applied int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.firstIndexLocked(), rf.lastIndexLocked(), rf.commitIndex, rf.lastApplied
}

func snapshotRecv(t *testing.T, ch <-chan ApplyMsg) ApplyMsg {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ApplyMsg")
		return ApplyMsg{}
	}
}

func TestRaftSnapshotLocalPersistAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	rf, ch := newSnapshotTestNode(t, path)
	entries := make([]LogEntry, 6)
	for i := range entries {
		entries[i] = LogEntry{Index: i + 1, Term: 5, Command: fmt.Sprintf("cmd-%d", i+1)}
	}
	var reply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 5, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: entries, LeaderCommit: 6,
	}, &reply)
	if !reply.Success {
		t.Fatalf("initial append failed: %+v", reply)
	}
	for index := 1; index <= 6; index++ {
		msg := snapshotRecv(t, ch)
		if !msg.CommandValid || msg.CommandIndex != index {
			t.Fatalf("apply[%d]: %+v", index, msg)
		}
	}
	data := []byte("state-after-index-4")
	rf.Snapshot(4, data)
	want := []LogEntry{
		{Index: 4, Term: 5},
		{Index: 5, Term: 5, Command: "cmd-5"},
		{Index: 6, Term: 5, Command: "cmd-6"},
	}
	if got := rf.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("logs after Snapshot: got %#v, want %#v", got, want)
	}
	ps, err := NewPersister(path)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := ps.ReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(persisted, data) {
		t.Fatalf("snapshot not persisted: got %q, want %q", persisted, data)
	}

	rf.Kill()
	restarted, _ := newSnapshotTestNode(t, path)
	if got := restarted.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restarted logs: got %#v, want %#v", got, want)
	}
	first, last, commit, applied := snapshotRaftProgress(restarted)
	if first != 4 || last != 6 || commit != 4 || applied != 4 {
		t.Fatalf("restart progress: first=%d last=%d commit=%d applied=%d", first, last, commit, applied)
	}
	restarted.mu.Lock()
	restored := append([]byte(nil), restarted.snapshot...)
	term := restarted.currentTerm
	restarted.mu.Unlock()
	if !bytes.Equal(restored, data) {
		t.Fatalf("restarted Snapshot: got %q", restored)
	}
	var nextReply AppendEntriesReply
	restarted.HandleAppendEntries(&AppendEntriesArgs{
		Term: term + 1, LeaderId: 1, PrevLogIndex: 6, PrevLogTerm: 5,
		Entries: []LogEntry{{Index: 7, Term: term + 1, Command: "cmd-7"}}, LeaderCommit: 4,
	}, &nextReply)
	if !nextReply.Success {
		t.Fatalf("append after restart failed: %+v", nextReply)
	}
	_, last, _, _ = snapshotRaftProgress(restarted)
	if last != 7 {
		t.Fatalf("global last index after restart = %d, want 7", last)
	}
}

func TestRaftSnapshotCondInstallSuffix(t *testing.T) {
	for _, tc := range []struct {
		name               string
		snapTerm, wantLast int
	}{
		{"matching-boundary-retains-suffix", 2, 5},
		{"conflicting-boundary-discards-suffix", 4, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rf, _ := newSnapshotTestNode(t, filepath.Join(t.TempDir(), "state.gob"))
			var reply AppendEntriesReply
			rf.HandleAppendEntries(&AppendEntriesArgs{
				Term: 5, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
				Entries: []LogEntry{
					{Index: 1, Term: 1, Command: "a"},
					{Index: 2, Term: 1, Command: "b"},
					{Index: 3, Term: 2, Command: "c"},
					{Index: 4, Term: 3, Command: "d"},
					{Index: 5, Term: 3, Command: "e"},
				}, LeaderCommit: 1,
			}, &reply)
			if !reply.Success {
				t.Fatalf("append failed: %+v", reply)
			}
			data := []byte("cond-snapshot")
			if !rf.CondInstallSnapshot(tc.snapTerm, 3, data) {
				t.Fatal("CondInstallSnapshot unexpectedly rejected fresh snapshot")
			}
			logs := rf.GetLogs()
			if logs[0].Index != 3 || logs[0].Term != tc.snapTerm || logs[len(logs)-1].Index != tc.wantLast {
				t.Fatalf("logs after CondInstallSnapshot: %#v", logs)
			}
			if tc.wantLast == 5 && (len(logs) != 3 || logs[1].Command != "d" || logs[2].Command != "e") {
				t.Fatalf("matching suffix not preserved: %#v", logs)
			}
			if tc.wantLast == 3 && len(logs) != 1 {
				t.Fatalf("conflicting suffix not discarded: %#v", logs)
			}
			first, _, commit, applied := snapshotRaftProgress(rf)
			if first != 3 || commit != 3 || applied != 3 {
				t.Fatalf("progress after CondInstall: first=%d commit=%d applied=%d", first, commit, applied)
			}
			diskSnapshot, err := rf.persister.ReadSnapshot()
			if err != nil || !bytes.Equal(diskSnapshot, data) {
				t.Fatalf("snapshot not persisted: data=%q err=%v", diskSnapshot, err)
			}
		})
	}
}

func TestRaftSnapshotHandleDeferredInstall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.gob")
	rf, ch := newSnapshotTestNode(t, path)
	data := []byte("remote-snapshot")
	var reply InstallSnapshotReply
	rf.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 8, LeaderId: 1, LastIncludedIndex: 10, LastIncludedTerm: 7, Data: data,
	}, &reply)
	if reply.Term != 8 {
		t.Fatalf("reply.Term=%d, want 8", reply.Term)
	}
	msg := snapshotRecv(t, ch)
	if !msg.SnapshotValid || msg.SnapshotIndex != 10 || msg.SnapshotTerm != 7 || !bytes.Equal(msg.Snapshot, data) {
		t.Fatalf("unexpected snapshot ApplyMsg: %+v", msg)
	}
	first, _, _, _ := snapshotRaftProgress(rf)
	if first != 0 {
		t.Fatalf("handler installed snapshot too early: first=%d", first)
	}
	if !rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		t.Fatal("CondInstallSnapshot rejected fresh snapshot")
	}
	first, last, commit, applied := snapshotRaftProgress(rf)
	if first != 10 || last != 10 || commit != 10 || applied != 10 {
		t.Fatalf("installed state: first=%d last=%d commit=%d applied=%d", first, last, commit, applied)
	}
	rf.Kill()
	restarted, _ := newSnapshotTestNode(t, path)
	first, _, commit, applied = snapshotRaftProgress(restarted)
	if first != 10 || commit != 10 || applied != 10 || !bytes.Equal(restarted.snapshot, data) {
		t.Fatalf("installed snapshot lost on restart: first=%d commit=%d applied=%d snapshot=%q", first, commit, applied, restarted.snapshot)
	}
}

func TestRaftSnapshotBecomesStaleBeforeCondInstall(t *testing.T) {
	rf, ch := newSnapshotTestNode(t, filepath.Join(t.TempDir(), "state.gob"))
	var reply InstallSnapshotReply
	rf.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 6, LeaderId: 1, LastIncludedIndex: 3, LastIncludedTerm: 6, Data: []byte("snapshot-3"),
	}, &reply)
	msg := snapshotRecv(t, ch)
	if !msg.SnapshotValid || msg.SnapshotIndex != 3 {
		t.Fatalf("expected snapshot(3) ApplyMsg, got %+v", msg)
	}
	var appendReply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 6, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 6, Command: "a"},
			{Index: 2, Term: 6, Command: "b"},
			{Index: 3, Term: 6, Command: "c"},
			{Index: 4, Term: 6, Command: "d"},
			{Index: 5, Term: 6, Command: "e"},
		}, LeaderCommit: 5,
	}, &appendReply)
	if !appendReply.Success {
		t.Fatalf("append while snapshot in flight failed: %+v", appendReply)
	}
	if rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		t.Fatal("accepted snapshot(3) after commitIndex advanced to 5")
	}
	first, last, commit, _ := snapshotRaftProgress(rf)
	if first != 0 || last != 5 || commit != 5 {
		t.Fatalf("stale CondInstall damaged logs: first=%d last=%d commit=%d", first, last, commit)
	}
}

func TestRaftSnapshotFollowerCatchUpViaGRPC(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader(5 * time.Second)
	slow, other := (leader+1)%3, (leader+2)%3
	c.rpc[slow].blockEntries.Store(true)
	for i := 1; i <= 7; i++ {
		index, _, ok := c.rf[leader].Start(fmt.Sprintf("cmd-%d", i))
		if !ok || index != i {
			t.Fatalf("Start cmd-%d returned index=%d, ok=%t", i, index, ok)
		}
	}
	for i := 1; i <= 7; i++ {
		for _, id := range []int{leader, other} {
			c.waitApplied(id, i, fmt.Sprintf("cmd-%d", i), 5*time.Second)
		}
	}
	if got := c.record[slow].count(); got != 0 {
		t.Fatalf("blocked follower applied %d commands", got)
	}
	data := []byte("mock-kv-state-through-index-4")
	c.rf[leader].Snapshot(4, data)
	first, _, _, _ := snapshotRaftProgress(c.rf[leader])
	if first != 4 {
		t.Fatalf("leader failed to compact prefix: first=%d", first)
	}

	// 关键：解除 Entries 阻断，Leader 才能从冲突回复中发现 Follower 缺日志。
	// 之前那版测试一直阻断 Entries，会让 nextIndex 无法回退，因此根本不会触发 Snapshot。
	c.rpc[slow].blockEntries.Store(false)
	snapshotWaitUntil(t, 5*time.Second, "slow follower installing snapshot(4)", func() bool {
		f, _, _, _ := snapshotRaftProgress(c.rf[slow])
		return f == 4 && c.record[slow].installedSnapshotIndex() >= 4
	})
	persisted, err := c.rf[slow].persister.ReadSnapshot()
	if err != nil || !bytes.Equal(persisted, data) {
		t.Fatalf("slow follower snapshot: got %q, err=%v", persisted, err)
	}
	for i := 5; i <= 7; i++ {
		c.waitApplied(slow, i, fmt.Sprintf("cmd-%d", i), 5*time.Second)
	}
	first, last, _, _ := snapshotRaftProgress(c.rf[slow])
	if first != 4 || last != 7 {
		t.Fatalf("catch-up logs: first=%d last=%d, want 4..7", first, last)
	}
}

func TestRaftSnapshotRejectStaleRPC(t *testing.T) {
	rf, ch := newSnapshotTestNode(t, filepath.Join(t.TempDir(), "state.gob"))
	var appendReply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 4, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 4, Command: "one"},
			{Index: 2, Term: 4, Command: "two"},
			{Index: 3, Term: 4, Command: "three"},
		}, LeaderCommit: 3,
	}, &appendReply)
	if !appendReply.Success {
		t.Fatalf("initial append failed: %+v", appendReply)
	}
	for i := 1; i <= 3; i++ {
		msg := snapshotRecv(t, ch)
		if !msg.CommandValid || msg.CommandIndex != i {
			t.Fatalf("unexpected ApplyMsg[%d]: %+v", i, msg)
		}
	}
	var reply InstallSnapshotReply
	rf.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 4, LeaderId: 1, LastIncludedIndex: 2, LastIncludedTerm: 4, Data: []byte("obsolete"),
	}, &reply)
	if reply.Term != 4 {
		t.Fatalf("reply term=%d, want 4", reply.Term)
	}
	select {
	case msg := <-ch:
		t.Fatalf("stale Snapshot RPC produced ApplyMsg: %+v", msg)
	case <-time.After(80 * time.Millisecond):
	}
	first, last, commit, _ := snapshotRaftProgress(rf)
	if first != 0 || last != 3 || commit != 3 {
		t.Fatalf("stale RPC changed state: first=%d last=%d commit=%d", first, last, commit)
	}
}

func TestRaftSnapshotLocalBoundaryChecks(t *testing.T) {
	rf, ch := newSnapshotTestNode(t, filepath.Join(t.TempDir(), "state.gob"))
	var reply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 3, Command: "a"},
			{Index: 2, Term: 3, Command: "b"},
			{Index: 3, Term: 3, Command: "c"},
		}, LeaderCommit: 2,
	}, &reply)
	if !reply.Success {
		t.Fatalf("append failed: %+v", reply)
	}
	for i := 1; i <= 2; i++ {
		msg := snapshotRecv(t, ch)
		if !msg.CommandValid || msg.CommandIndex != i {
			t.Fatalf("unexpected ApplyMsg[%d]: %+v", i, msg)
		}
	}
	rf.Snapshot(3, []byte("not-committed"))
	first, _, _, _ := snapshotRaftProgress(rf)
	if first != 0 {
		t.Fatal("Snapshot compacted an uncommitted log")
	}
	rf.Snapshot(2, []byte("correct-snapshot"))
	first, _, _, _ = snapshotRaftProgress(rf)
	if first != 2 {
		t.Fatalf("Snapshot(2) did not compact: first=%d", first)
	}
	rf.Snapshot(1, []byte("old-snapshot"))
	rf.Snapshot(2, []byte("duplicate-snapshot"))
	persisted, err := rf.persister.ReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if string(persisted) != "correct-snapshot" {
		t.Fatalf("old/duplicate snapshot replaced current snapshot: %q", persisted)
	}
}
func TestSnapshotCrashBeforeInstall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.gob")
	rf, ch := newSnapshotTestNode(t, path)
	data := []byte("snapshot-at-10")

	var reply InstallSnapshotReply
	rf.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 8, LeaderId: 1, LastIncludedIndex: 10,
		LastIncludedTerm: 7, Data: data,
	}, &reply)
	if reply.Term != 8 {
		t.Fatalf("reply term=%d, want 8", reply.Term)
	}
	msg := snapshotRecv(t, ch)
	if !msg.SnapshotValid || msg.SnapshotIndex != 10 {
		t.Fatalf("wrong ApplyMsg: %+v", msg)
	}

	// 故意不调用 CondInstallSnapshot。
	rf.Kill()

	restarted, ch2 := newSnapshotTestNode(t, path)
	first, last, committed, applied := snapshotRaftProgress(restarted)
	if first != 0 || last != 0 || committed != 0 || applied != 0 {
		t.Fatalf("uninstalled snapshot survived restart: first=%d last=%d commit=%d applied=%d",
			first, last, committed, applied)
	}
	persisted, err := restarted.persister.ReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 0 {
		t.Fatalf("uninstalled snapshot persisted: %q", persisted)
	}

	// 模拟 Leader 重试：收到新 RPC，KV 真正安装。
	reply = InstallSnapshotReply{}
	restarted.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 8, LeaderId: 1, LastIncludedIndex: 10,
		LastIncludedTerm: 7, Data: data,
	}, &reply)
	msg = snapshotRecv(t, ch2)
	if !restarted.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		t.Fatal("retried snapshot should be installable")
	}
	restarted.Kill()

	recovered, _ := newSnapshotTestNode(t, path)
	first, last, committed, applied = snapshotRaftProgress(recovered)
	if first != 10 || last != 10 || committed != 10 || applied != 10 {
		t.Fatalf("installed snapshot not recovered: first=%d last=%d commit=%d applied=%d",
			first, last, committed, applied)
	}
	if !bytes.Equal(recovered.snapshot, data) {
		t.Fatalf("recovered data=%q, want %q", recovered.snapshot, data)
	}
}

// 测试：两个合法 Snapshot RPC 都已经到达，
// 但 KV 故意先安装新快照，再处理旧快照。
func TestSnapshotOutOfOrder(t *testing.T) {
	rf, ch := newSnapshotTestNode(t, filepath.Join(t.TempDir(), "state.gob"))
	for _, v := range []struct {
		index int
		term  int
		data  string
	}{
		{index: 8, term: 4, data: "old-snapshot"},
		{index: 12, term: 6, data: "new-snapshot"},
	} {
		var reply InstallSnapshotReply
		rf.HandleInstallSnapshot(&InstallSnapshotArgs{
			Term: 9, LeaderId: 1,
			LastIncludedIndex: v.index, LastIncludedTerm: v.term,
			Data: []byte(v.data),
		}, &reply)
		if reply.Term != 9 {
			t.Fatalf("unexpected RPC reply term: %+v", reply)
		}
	}

	// goroutine 发送顺序不确定：先收齐，再人为选择处理顺序。
	messages := make(map[int]ApplyMsg)
	for i := 0; i < 2; i++ {
		msg := snapshotRecv(t, ch)
		if !msg.SnapshotValid {
			t.Fatalf("expected snapshot ApplyMsg, got %+v", msg)
		}
		messages[msg.SnapshotIndex] = msg
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 distinct snapshots, got %d", len(messages))
	}

	newer, older := messages[12], messages[8]
	if !rf.CondInstallSnapshot(newer.SnapshotTerm, newer.SnapshotIndex, newer.Snapshot) {
		t.Fatal("newer snapshot rejected")
	}
	if rf.CondInstallSnapshot(older.SnapshotTerm, older.SnapshotIndex, older.Snapshot) {
		t.Fatal("older snapshot overwrote newer snapshot")
	}
	first, last, commit, applied := snapshotRaftProgress(rf)
	if first != 12 || last != 12 || commit != 12 || applied != 12 {
		t.Fatalf("wrong progress: first=%d last=%d commit=%d applied=%d",
			first, last, commit, applied)
	}
	persisted, err := rf.persister.ReadSnapshot()
	if err != nil || !bytes.Equal(persisted, []byte("new-snapshot")) {
		t.Fatalf("stored snapshot=%q, err=%v", persisted, err)
	}
}

// 测试模拟 KV 的安全防线：先安装 Snapshot(10)，
// 再收到旧的 Log(9)、Log(10)。
//
// 这是确定性测试 applyRecorder.record 的过滤逻辑，
// 不声称能够强制 Raft 内部 applier 产生这样的调度顺序。
func TestSnapshotRecorderDropsDelayedCommands(t *testing.T) {
	rf, _ := newSnapshotTestNode(t, filepath.Join(t.TempDir(), "state.gob"))
	r := &applyRecorder{}
	r.attachRaft(rf)

	r.record(ApplyMsg{
		SnapshotValid: true, SnapshotIndex: 10, SnapshotTerm: 5,
		Snapshot: []byte("state-at-10"),
	})
	if r.err != nil || r.installedSnapshotIndex() != 10 {
		t.Fatalf("recorder failed to install Snapshot(10): err=%v installed=%d",
			r.err, r.installedSnapshotIndex())
	}

	// 即使发生旧日志迟到，也不能再次执行。
	r.record(ApplyMsg{CommandValid: true, CommandIndex: 9, Command: "stale-9"})
	r.record(ApplyMsg{CommandValid: true, CommandIndex: 10, Command: "stale-10"})

	// 后续日志必须按 SnapshotIndex+1 继续应用。
	r.record(ApplyMsg{CommandValid: true, CommandIndex: 11, Command: "fresh-11"})
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.count() != 1 {
		t.Fatalf("recorded %d commands; want only fresh index 11", r.count())
	}
	msg, ok, err := r.snapshot(11)
	if err != nil || !ok || msg.Command != "fresh-11" {
		t.Fatalf("unexpected Log(11): msg=%+v ok=%v err=%v", msg, ok, err)
	}
}

// 慢消费者：没有 goroutine 读取 applyCh 时，Handler 仍应快速返回；
// Kill 后阻塞在 applyCh 的发送 goroutine 应能通过 stopCh 退出。
//
// 本测试不要求未 Kill 前 goroutine 数量有界：当前实现本身没有限流。
func TestSnapshotSlowConsumerKill(t *testing.T) {
	ps, err := NewPersister(filepath.Join(t.TempDir(), "state.gob"))
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan ApplyMsg) // 故意不启动任何消费者
	rf, err := NewRaft(0, []*PeerClient{nil}, ch, ps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rf.Kill)

	const requests = 40
	baseline := runtime.NumGoroutine()
	done := make(chan error, 1)
	go func() {
		for i := 0; i < requests; i++ {
			var reply InstallSnapshotReply
			rf.HandleInstallSnapshot(&InstallSnapshotArgs{
				Term: 20, LeaderId: 1, LastIncludedIndex: 100 + i,
				LastIncludedTerm: 19, Data: []byte("snapshot"),
			}, &reply)
			if reply.Term != 20 {
				done <- fmt.Errorf("request %d returned term %d", i, reply.Term)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		rf.Kill()
		t.Fatal("InstallSnapshot Handler blocked on slow KV consumer")
	}

	stalled := runtime.NumGoroutine()
	t.Logf("goroutines while consumer is blocked: baseline=%d now=%d", baseline, stalled)
	rf.Kill()

	// 容许少量运行时波动，但不能留下几十个长期阻塞的发送 goroutine。
	snapshotWaitUntil(t, 3*time.Second, "snapshot sender goroutines to exit after Kill", func() bool {
		return runtime.NumGoroutine() <= baseline+12
	})
}

// 根据现有 testCluster 的架构创建独立的第二代 gRPC 集群，
// 但所有 Raft 节点都从指定的原磁盘路径恢复。
//
// recorder 充当 KV，启动前先恢复 SnapshotIndex，
// 避免把日志 index+1 误判为乱序。
func newSnapshotClusterFromPaths(t *testing.T, paths []string) *testCluster {
	t.Helper()
	n := len(paths)
	if n == 0 {
		t.Fatal("empty restart paths")
	}
	c := &testCluster{
		t:       t,
		rf:      make([]*Raft, n),
		rpc:     make([]*testRPCServer, n),
		servers: make([]*grpc.Server, n),
		clients: make([][]*PeerClient, n),
		record:  make([]*applyRecorder, n),
		paths:   append([]string(nil), paths...),
		stop:    make(chan struct{}),
	}
	listeners := make([]net.Listener, n)
	addresses := make([]string, n)
	for i := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addresses[i] = ln.Addr().String()
	}
	t.Cleanup(func() {
		for _, rf := range c.rf {
			if rf != nil {
				rf.Kill()
			}
		}
		close(c.stop)
		for _, server := range c.servers {
			if server != nil {
				server.Stop()
			}
		}
		for _, peers := range c.clients {
			for _, peer := range peers {
				if peer != nil {
					_ = peer.Close()
				}
			}
		}
		for _, ln := range listeners {
			if ln != nil {
				_ = ln.Close()
			}
		}
	})

	for i := 0; i < n; i++ {
		c.clients[i] = make([]*PeerClient, n)
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			peer, err := NewPeerClient(addresses[j])
			if err != nil {
				t.Fatal(err)
			}
			c.clients[i][j] = peer
		}
		ch := make(chan ApplyMsg, 128)
		ps, err := NewPersister(c.paths[i])
		if err != nil {
			t.Fatal(err)
		}
		rf, err := NewRaft(i, c.clients[i], ch, ps)
		if err != nil {
			t.Fatal(err)
		}
		c.rf[i] = rf
		first, _, _, _ := snapshotRaftProgress(rf)
		rec := &applyRecorder{lastApplied: first}
		if first > 0 {
			rec.installedSnapshot = first
		}
		rec.attachRaft(rf)
		c.record[i] = rec
		go func(rec *applyRecorder, msgs <-chan ApplyMsg) {
			for {
				select {
				case msg := <-msgs:
					rec.record(msg)
				case <-c.stop:
					return
				}
			}
		}(rec, ch)

		c.rpc[i] = &testRPCServer{GRPCServer: NewGRPCServer(rf)}
		c.servers[i] = grpc.NewServer()
		pb.RegisterRaftServiceServer(c.servers[i], c.rpc[i])
	}
	for i := range listeners {
		server, ln := c.servers[i], listeners[i]
		go func() { _ = server.Serve(ln) }()
	}
	return c
}

// 三节点全部压缩、全部停止、使用原磁盘路径整体重启，
// 然后继续提交新日志。
func TestSnapshotRestartAllNodes(t *testing.T) {
	c := newTestCluster(t, 3)
	for i := 1; i <= 5; i++ {
		_, index := c.start(fmt.Sprintf("before-%d", i))
		if index != i {
			t.Fatalf("before restart: index=%d, want %d", index, i)
		}
		for node := range c.rf {
			c.waitApplied(node, index, fmt.Sprintf("before-%d", i), 5*time.Second)
		}
	}
	data := []byte("all-nodes-state-through-3")
	for _, rf := range c.rf {
		rf.Snapshot(3, data)
	}
	for node, rf := range c.rf {
		first, _, _, _ := snapshotRaftProgress(rf)
		if first != 3 {
			t.Fatalf("node %d did not snapshot to 3: first=%d", node, first)
		}
	}

	// 模拟整个集群同时关闭。
	// 原 testCluster 在 t.Cleanup 中还会重复 Kill/Stop，
	// 这两个操作可重复调用；这里不主动 close(c.stop)，避免重复关闭。
	for _, rf := range c.rf {
		rf.Kill()
	}
	for _, server := range c.servers {
		server.Stop()
	}

	restarted := newSnapshotClusterFromPaths(t, c.paths)
	for i, rf := range restarted.rf {
		first, _, commit, applied := snapshotRaftProgress(rf)
		if first != 3 || commit < 3 || applied < 3 {
			t.Fatalf("node %d failed to recover snapshot boundary: first=%d commit=%d applied=%d",
				i, first, commit, applied)
		}
		if !bytes.Equal(rf.snapshot, data) {
			t.Fatalf("node %d failed to restore snapshot bytes", i)
		}
	}

	_, index := restarted.start("after-restart")
	if index != 6 {
		t.Fatalf("new command index=%d, want 6", index)
	}
	for node := range restarted.rf {
		for i := 4; i <= 5; i++ {
			restarted.waitApplied(node, i, fmt.Sprintf("before-%d", i), 8*time.Second)
		}
		restarted.waitApplied(node, index, "after-restart", 8*time.Second)
	}
}
