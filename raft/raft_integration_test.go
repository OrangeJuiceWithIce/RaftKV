package raft

import (
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/OrangeJuiceWithIce/raftkv/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------- 测试命令 ----------

type testOp struct {
	Kind  string
	Key   string
	Value string
}

func init() {
	gob.Register(testOp{})
}

// ---------- 测试用 gRPC Server ----------

// blockEntries 模拟 Follower 暂时无法接收日志复制。
// 阻止带有日志 Entries 的 AppendEntries，
// 但允许合法的空心跳通过，以避免测试中发生不必要的重新选举。
type testRPCServer struct {
	*GRPCServer
	blockEntries atomic.Bool
}

func (s *testRPCServer) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	if s.blockEntries.Load() && len(req.Entries) > 0 {
		return nil, status.Error(
			codes.Unavailable,
			"simulated log replication failure",
		)
	}

	return s.GRPCServer.AppendEntries(ctx, req)
}

// ---------- ApplyMsg 记录器 ----------

// 每个节点只有一个 goroutine 消费 applyCh。
// 其他测试只读取记录器，不直接消费 applyCh。
type applyRecorder struct {
	mu      sync.Mutex
	entries []ApplyMsg
	err     error
}

func (r *applyRecorder) record(msg ApplyMsg) {
	if !msg.CommandValid {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return
	}

	expected := len(r.entries) + 1

	if msg.CommandIndex != expected {
		r.err = fmt.Errorf(
			"apply order broken: expected index %d, got %d",
			expected,
			msg.CommandIndex,
		)
		return
	}

	r.entries = append(r.entries, msg)
}

func (r *applyRecorder) snapshot(
	index int,
) (ApplyMsg, bool, error) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return ApplyMsg{}, false, r.err
	}

	if index < 1 || index > len(r.entries) {
		return ApplyMsg{}, false, nil
	}

	return r.entries[index-1], true, nil
}

func (r *applyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// ---------- 测试集群 ----------

type testCluster struct {
	t *testing.T

	rf      []*Raft
	rpc     []*testRPCServer
	servers []*grpc.Server
	clients [][]*PeerClient

	record []*applyRecorder
	paths  []string
	stop   chan struct{}
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	c := &testCluster{
		t:       t,
		rf:      make([]*Raft, n),
		rpc:     make([]*testRPCServer, n),
		servers: make([]*grpc.Server, n),
		clients: make([][]*PeerClient, n),
		record:  make([]*applyRecorder, n),
		paths:   make([]string, n),
		stop:    make(chan struct{}),
	}

	// 先给每个节点分配监听地址。
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

	// 测试结束时释放资源。
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

	// 为每个节点分配独立的磁盘状态路径，避免测试之间相互影响。
	stateDir := t.TempDir()
	for i := 0; i < n; i++ {
		c.paths[i] = filepath.Join(stateDir, fmt.Sprintf("node-%d", i), "state.gob")
	}

	// 创建每个节点的 PeerClient、Raft 和 gRPC Server。
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

		applyCh := make(chan ApplyMsg, 128)
		c.record[i] = &applyRecorder{}

		recorder := c.record[i]

		go func() {
			for {
				select {
				case msg := <-applyCh:
					recorder.record(msg)

				case <-c.stop:
					return
				}
			}
		}()

		ps, err := NewPersister(c.paths[i])
		if err != nil {
			t.Fatal(err)
		}
		rf, err := NewRaft(i, c.clients[i], applyCh, ps)
		if err != nil {
			t.Fatal(err)
		}

		c.rf[i] = rf

		c.rpc[i] = &testRPCServer{
			GRPCServer: NewGRPCServer(rf),
		}

		c.servers[i] = grpc.NewServer()

		pb.RegisterRaftServiceServer(
			c.servers[i],
			c.rpc[i],
		)
	}

	// 启动所有 gRPC Server。
	for i := 0; i < n; i++ {
		server := c.servers[i]
		ln := listeners[i]

		go func() {
			_ = server.Serve(ln)
		}()
	}

	return c
}

// ---------- 测试辅助函数 ----------

// 等待集群中恰好出现一个 Leader。
func (c *testCluster) leader(
	timeout time.Duration,
) int {
	c.t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		who := -1
		count := 0

		for i, rf := range c.rf {
			_, isLeader := rf.GetState()

			if isLeader {
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

// 等待某个节点应用指定索引的日志。
func (c *testCluster) waitApplied(
	node int,
	index int,
	command any,
	timeout time.Duration,
) {
	c.t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		msg, found, err := c.record[node].snapshot(index)

		if err != nil {
			c.t.Fatalf(
				"node %d: %v",
				node,
				err,
			)
		}

		if found {
			if !reflect.DeepEqual(msg.Command, command) {
				c.t.Fatalf(
					"node %d index %d: got (%T)%#v, want (%T)%#v",
					node,
					index,
					msg.Command,
					msg.Command,
					command,
					command,
				)
			}

			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	c.t.Fatalf(
		"node %d: timeout waiting for index %d (recorded %d)",
		node,
		index,
		c.record[node].count(),
	)
}

// 找到 Leader 并提交一条命令。
// 返回 Leader ID 和日志索引。
func (c *testCluster) start(
	command any,
) (int, int) {
	c.t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		leader := c.leader(4 * time.Second)

		// Start 返回：(index, term, isLeader)
		index, _, ok := c.rf[leader].Start(command)

		if ok {
			return leader, index
		}

		time.Sleep(20 * time.Millisecond)
	}

	c.t.Fatal("could not start command")
	return -1, -1
}

// ---------- Test 1: Leader Election ----------

func TestRaftLeaderElection(t *testing.T) {
	c := newTestCluster(t, 3)

	leader := c.leader(5 * time.Second)
	t.Logf("leader=%d", leader)

	// 观察多个心跳周期，验证 Leader 稳定。
	for i := 0; i < 5; i++ {
		time.Sleep(120 * time.Millisecond)

		got := c.leader(2 * time.Second)

		if got != leader {
			t.Fatalf(
				"leader changed unexpectedly: %d -> %d",
				leader,
				got,
			)
		}
	}
}

// ---------- Test 2: Basic Agreement ----------

func TestRaftBasicAgreement(t *testing.T) {
	c := newTestCluster(t, 3)

	command := "first-command"

	_, index := c.start(command)

	if index != 1 {
		t.Fatalf(
			"first index=%d, want 1",
			index,
		)
	}

	for i := range c.rf {
		c.waitApplied(
			i,
			index,
			command,
			5*time.Second,
		)
	}
}

// ---------- Test 3: Gob Serialization ----------

func TestRaftGobCommand(t *testing.T) {
	c := newTestCluster(t, 3)

	command := testOp{
		Kind:  "Put",
		Key:   "name",
		Value: "Alice",
	}

	_, index := c.start(command)

	for i := range c.rf {
		c.waitApplied(
			i,
			index,
			command,
			5*time.Second,
		)
	}
}

// ---------- Test 4: Follower Catch-up ----------

func TestRaftFollowerCatchUp(t *testing.T) {
	c := newTestCluster(t, 3)

	leader := c.leader(5 * time.Second)

	slow := (leader + 1) % 3
	other := (leader + 2) % 3

	// 暂时阻止 slow 接收带日志的 AppendEntries。
	c.rpc[slow].blockEntries.Store(true)

	// 注意：Start 返回 index、term、isLeader。
	index1, _, ok := c.rf[leader].Start("one")
	if !ok {
		t.Fatal("leader rejected first Start")
	}

	index2, _, ok := c.rf[leader].Start("two")
	if !ok {
		t.Fatal("leader rejected second Start")
	}

	t.Logf(
		"leader=%d slow=%d other=%d index1=%d index2=%d",
		leader,
		slow,
		other,
		index1,
		index2,
	)

	if index2 != index1+1 {
		t.Fatalf(
			"non-consecutive indexes: %d, %d",
			index1,
			index2,
		)
	}

	// Leader 和 other 构成多数派，应该能提交两条日志。
	for _, id := range []int{leader, other} {
		c.waitApplied(
			id,
			index1,
			"one",
			5*time.Second,
		)

		c.waitApplied(
			id,
			index2,
			"two",
			5*time.Second,
		)
	}

	// slow 此时还不能应用这些日志。
	if count := c.record[slow].count(); count != 0 {
		t.Fatalf(
			"slow follower applied %d entries before recovery",
			count,
		)
	}

	// 恢复日志复制。
	c.rpc[slow].blockEntries.Store(false)

	// 验证 slow 按日志索引顺序完成追赶。
	c.waitApplied(
		slow,
		index1,
		"one",
		5*time.Second,
	)

	c.waitApplied(
		slow,
		index2,
		"two",
		5*time.Second,
	)
}

// ---------- Test 5: Leader Failover ----------

func TestRaftLeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)

	oldLeader := c.leader(5 * time.Second)

	// 模拟旧 Leader 宕机。
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

	t.Logf(
		"old leader=%d, new leader=%d",
		oldLeader,
		newLeader,
	)

	index, term, ok := c.rf[newLeader].Start("after-failover")

	if !ok {
		t.Fatal("new leader rejected Start")
	}

	t.Logf("term=%d, index=%d", term, index)

	for i := range c.rf {
		if i == oldLeader {
			continue
		}

		c.waitApplied(
			i,
			index,
			"after-failover",
			5*time.Second,
		)
	}
}

// ---------- Test 6: Concurrent Start ----------

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

			command := fmt.Sprintf("command-%d", i)

			index, _, ok := c.rf[leader].Start(command)

			if !ok {
				t.Errorf(
					"Start rejected command %q",
					command,
				)
				return
			}

			mu.Lock()
			expected[index] = command
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if len(expected) != commands {
		t.Fatalf(
			"accepted %d commands, want %d",
			len(expected),
			commands,
		)
	}

	// 检查每个节点的应用内容与 Leader 分配的索引一致。
	for node := range c.rf {
		for index := 1; index <= commands; index++ {
			c.waitApplied(
				node,
				index,
				expected[index],
				5*time.Second,
			)
		}
	}
}

// 编译时检查测试 RPC Server 实现了对应接口。
var _ pb.RaftServiceServer = (*testRPCServer)(nil)

// ---------- Test 7-11: Disk persistence / restart ----------

// 从同一个磁盘路径重建 Raft；使用单节点避免修改已有的 gRPC 集群生命周期。
// 测试直接调用 RPC Handler，因此不依赖真实的网络连接。
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
	rf.HandleRequestVote(&RequestVoteArgs{
		Term: 12, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0,
	}, &first)
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
	restarted.HandleRequestVote(&RequestVoteArgs{
		Term: 12, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	}, &again)
	if again.VoteGranted {
		t.Fatalf("double voting in the same term after restart: %+v", again)
	}
}

func TestRaftDiskHigherTermOnRejectedAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	rf := newDiskNodeForIntegration(t, path)
	var reply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 17, LeaderId: 1, PrevLogIndex: 99, PrevLogTerm: 17,
	}, &reply)
	if reply.Success || reply.Term != 17 {
		t.Fatalf("expected rejection with updated term: %+v", reply)
	}
	rf.Kill()

	restarted := newDiskNodeForIntegration(t, path)
	term, _ := restarted.GetState()
	if term != 17 {
		t.Fatalf("term lost after rejected append: got %d want 17", term)
	}
}

func TestRaftDiskConflictOverwriteSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node0", "state.gob")
	rf := newDiskNodeForIntegration(t, path)

	var reply AppendEntriesReply
	rf.HandleAppendEntries(&AppendEntriesArgs{
		Term: 3, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 3, Command: "old-1"},
			{Term: 3, Command: "old-2"},
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
			{Term: 4, Command: "new-1"},
			{Term: 4, Command: "new-2"},
		},
	}, &reply)
	if !reply.Success {
		t.Fatalf("conflict overwrite rejected: %+v", reply)
	}
	rf2.Kill()

	rf3 := newDiskNodeForIntegration(t, path)
	want := []LogEntry{
		{Term: 0},
		{Term: 4, Command: "new-1"},
		{Term: 4, Command: "new-2"},
	}
	if got := rf3.GetLogs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("logs after restart: got %#v, want %#v", got, want)
	}
}

// 在现有三节点 gRPC 集群上验证：Start 返回时已持久化，
// Follower 追赶完成后，磁盘上也存在相同的命令。
func TestRaftDiskAgreementDurable(t *testing.T) {
	c := newTestCluster(t, 3)
	command := testOp{Kind: "Put", Key: "persist", Value: "value"}
	leader, index := c.start(command)

	// Start 返回后，无需停止节点即可从磁盘读取其状态。
	ps, err := NewPersister(c.paths[leader])
	if err != nil {
		t.Fatal(err)
	}
	probe := &Raft{persister: ps, votedFor: -1, logs: []LogEntry{{Term: 0}}}
	if err := probe.readPersist(); err != nil {
		t.Fatal(err)
	}
	if index >= len(probe.logs) || !reflect.DeepEqual(probe.logs[index].Command, command) {
		t.Fatalf("leader Start did not persist command at index %d: %#v", index, probe.logs)
	}

	for i := range c.rf {
		c.waitApplied(i, index, command, 5*time.Second)
	}
	for i, path := range c.paths {
		ps, err := NewPersister(path)
		if err != nil {
			t.Fatal(err)
		}
		probe := &Raft{persister: ps, votedFor: -1, logs: []LogEntry{{Term: 0}}}
		if err := probe.readPersist(); err != nil {
			t.Fatal(err)
		}
		if index >= len(probe.logs) || !reflect.DeepEqual(probe.logs[index].Command, command) {
			t.Fatalf("node %d disk missing committed command at index %d: %#v", i, index, probe.logs)
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
