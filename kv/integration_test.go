package kvraft

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kvpb "github.com/OrangeJuiceWithIce/raftkv/kv/pb"
	"github.com/OrangeJuiceWithIce/raftkv/raft"
	raftpb "github.com/OrangeJuiceWithIce/raftkv/raft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// 一个 integrationNode 就是一个真正的 Raft + KV + gRPC 节点。
// stopNode / startNode 会复用同一个 Persister 文件，模拟崩溃与重启。
type integrationNode struct {
	kv        *KVServer
	ps        *raft.Persister
	peers     []*raft.PeerClient
	server    *grpc.Server
	listener  net.Listener
	serveDone chan struct{}
}

type integrationCluster struct {
	t            *testing.T
	addresses    []string
	paths        []string
	nodes        []*integrationNode
	conns        []*grpc.ClientConn
	clients      []kvpb.KVServiceClient
	maxRaftState int

	// 仅测试使用：在接收端阻断带 Entries 的 AppendEntries
	// 和 InstallSnapshot，但仍允许空心跳，保持 Follower 不选举。
	blockReplication []atomic.Bool
	snapshotRPCs     []atomic.Int64
	leaderHint       int
}

func newIntegrationCluster(t *testing.T, n, maxRaftState int) *integrationCluster {
	t.Helper()
	c := &integrationCluster{
		t:                t,
		addresses:        make([]string, n),
		paths:            make([]string, n),
		nodes:            make([]*integrationNode, n),
		conns:            make([]*grpc.ClientConn, n),
		clients:          make([]kvpb.KVServiceClient, n),
		maxRaftState:     maxRaftState,
		blockReplication: make([]atomic.Bool, n),
		snapshotRPCs:     make([]atomic.Int64, n),
	}
	t.Cleanup(c.close)

	// 先分配所有端口，否则 PeerClient 不知道其他节点的地址。
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate listener for node %d: %v", i, err)
		}
		listeners[i] = ln
		c.addresses[i] = ln.Addr().String()
		c.paths[i] = filepath.Join(t.TempDir(), fmt.Sprintf("node-%d.gob", i))
	}

	for i := 0; i < n; i++ {
		c.startNode(i, listeners[i])
	}
	for i, address := range c.addresses {
		conn, err := grpc.NewClient(address,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("create KV client %d: %v", i, err)
		}
		c.conns[i] = conn
		c.clients[i] = kvpb.NewKVServiceClient(conn)
	}
	return c
}

func (c *integrationCluster) startNode(id int, ln net.Listener) {
	c.t.Helper()
	if c.nodes[id] != nil {
		c.t.Fatalf("node %d is already running", id)
	}
	if ln == nil {
		var err error
		ln, err = net.Listen("tcp", c.addresses[id])
		if err != nil {
			c.t.Fatalf("restart listener %d: %v", id, err)
		}
	}

	peers := make([]*raft.PeerClient, len(c.addresses))
	for j, addr := range c.addresses {
		if j == id {
			continue
		}
		p, err := raft.NewPeerClient(addr)
		if err != nil {
			for _, prior := range peers {
				if prior != nil {
					_ = prior.Close()
				}
			}
			_ = ln.Close()
			c.t.Fatalf("new Raft PeerClient %d->%d: %v", id, j, err)
		}
		peers[j] = p
	}

	ps, err := raft.NewPersister(c.paths[id])
	if err != nil {
		c.t.Fatalf("new Persister %d: %v", id, err)
	}
	kv, err := NewKVServer(id, peers, ps, c.maxRaftState)
	if err != nil {
		c.t.Fatalf("new KVServer %d: %v", id, err)
	}

	// 不修改生产 Raft 代码：直接在测试 gRPC 接收端注入故障。
	interceptor := func(ctx context.Context, req any,
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if c.blockReplication[id].Load() {
			switch r := req.(type) {
			case *raftpb.AppendEntriesRequest:
				if len(r.Entries) > 0 {
					return nil, status.Error(codes.Unavailable, "replication blocked by test")
				}
			case *raftpb.InstallSnapshotRequest:
				return nil, status.Error(codes.Unavailable, "snapshot blocked by test")
			}
		}
		if _, ok := req.(*raftpb.InstallSnapshotRequest); ok {
			c.snapshotRPCs[id].Add(1)
		}
		return handler(ctx, req)
	}

	gs := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	raftpb.RegisterRaftServiceServer(gs, raft.NewGRPCServer(kv.rf))
	kvpb.RegisterKVServiceServer(gs, NewKVGRPCServer(kv))

	node := &integrationNode{
		kv: kv, ps: ps, peers: peers, server: gs, listener: ln,
		serveDone: make(chan struct{}),
	}
	c.nodes[id] = node
	go func() {
		defer close(node.serveDone)
		_ = gs.Serve(ln)
	}()
}

func (c *integrationCluster) stopNode(id int) {
	node := c.nodes[id]
	if node == nil {
		return
	}
	// Stop 关闭正在服务的连接，模拟进程退出而非网络隔离。
	node.server.Stop()
	_ = node.listener.Close()
	<-node.serveDone
	node.kv.Kill()
	for _, p := range node.peers {
		if p != nil {
			_ = p.Close()
		}
	}
	c.nodes[id] = nil
}

func (c *integrationCluster) close() {
	for i := range c.nodes {
		c.stopNode(i)
	}
	for _, conn := range c.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func (c *integrationCluster) leader(exclude int) int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range c.nodes {
			if id == exclude || node == nil {
				continue
			}
			if _, ok := node.kv.rf.GetState(); ok {
				return id
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("no Raft leader elected (excluding %d)", exclude)
	return -1
}

func (c *integrationCluster) waitFor(description string, timeout time.Duration, f func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("timeout waiting for %s", description)
}

func (c *integrationCluster) valueAt(id int, key string) (string, int) {
	node := c.nodes[id]
	if node == nil {
		return "", -1
	}
	node.kv.mu.Lock()
	defer node.kv.mu.Unlock()
	return node.kv.data[key], node.kv.lastApplied
}

func (c *integrationCluster) snapshotIndex(id int) int {
	node := c.nodes[id]
	if node == nil {
		return 0
	}
	snapshot, err := node.ps.ReadSnapshot()
	if err != nil {
		c.t.Fatalf("read snapshot node %d: %v", id, err)
	}
	if len(snapshot) == 0 {
		return 0
	}
	state, err := decodeSnapshot(snapshot)
	if err != nil {
		c.t.Fatalf("decode snapshot node %d: %v", id, err)
	}
	return state.LastApplied
}

// 使用真实 KV gRPC 调用并限制整个请求的重试时间。
// 同一个逻辑命令的 ClientID/CommandID 在所有重试中保持不变。
func (c *integrationCluster) command(req *kvpb.CommandRequest) *kvpb.CommandResponse {
	c.t.Helper()
	deadline := time.Now().Add(18 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		for k := range c.clients {
			id := (c.leaderHint + k) % len(c.clients)
			if c.nodes[id] == nil { // 已明确停止的节点无需等待连接超时。
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1600*time.Millisecond)
			resp, err := c.clients[id].Command(ctx, req)
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			if resp == nil {
				lastErr = fmt.Errorf("nil KV response from node %d", id)
				continue
			}
			switch Err(resp.Err) {
			case OK, ErrNoKey:
				c.leaderHint = id
				return resp
			case ErrWrongLeader, ErrTimeout, ErrStopped:
				lastErr = fmt.Errorf("node %d: %s", id, resp.Err)
			default:
				c.t.Fatalf("unexpected KV response node %d: %+v", id, resp)
			}
		}
		time.Sleep(60 * time.Millisecond)
	}
	c.t.Fatalf("KV request never succeeded: op=%s key=%q client=%d command=%d: %v",
		req.Op, req.Key, req.ClientId, req.CommandId, lastErr)
	return nil
}

func (c *integrationCluster) write(op OpType, key, value string, clientID, commandID int64) {
	c.t.Helper()
	resp := c.command(&kvpb.CommandRequest{
		Op: string(op), Key: key, Value: value,
		ClientId: clientID, CommandId: commandID,
	})
	if Err(resp.Err) != OK {
		c.t.Fatalf("write returned %s", resp.Err)
	}
}

func (c *integrationCluster) read(key string) string {
	c.t.Helper()
	resp := c.command(&kvpb.CommandRequest{
		Op: string(OpGet), Key: key,
		ClientId: 900000, CommandId: time.Now().UnixNano(),
	})
	if Err(resp.Err) != OK {
		c.t.Fatalf("read key %q returned %s", key, resp.Err)
	}
	return resp.Value
}

func TestRaftKVBasic(t *testing.T) {
	c := newIntegrationCluster(t, 3, -1)
	c.leader(-1)

	// 通过实际 Clerk 而不是直接访问 KV 内部状态完成读写。
	ck, err := NewClerk(c.addresses)
	if err != nil {
		t.Fatal(err)
	}
	defer ck.Close()
	done := make(chan string, 1)
	go func() {
		ck.Put("x", "A")
		ck.Append("x", "B")
		done <- ck.Get("x")
	}()
	select {
	case got := <-done:
		if got != "AB" {
			t.Fatalf("Clerk.Get x=%q, want AB", got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Clerk basic Put/Append/Get timed out")
	}

	c.waitFor("all replicas apply x=AB", 10*time.Second, func() bool {
		for i := range c.nodes {
			value, _ := c.valueAt(i, "x")
			if value != "AB" {
				return false
			}
		}
		return true
	})

	resp := c.command(&kvpb.CommandRequest{
		Op: string(OpGet), Key: "does-not-exist", ClientId: 700, CommandId: 1,
	})
	if Err(resp.Err) != ErrNoKey || resp.Value != "" {
		t.Fatalf("missing key response=%+v", resp)
	}
}

func TestRaftKVLeaderFailover(t *testing.T) {
	c := newIntegrationCluster(t, 3, -1)
	oldLeader := c.leader(-1)
	c.write(OpPut, "x", "A", 1001, 1)

	c.stopNode(oldLeader)
	newLeader := c.leader(oldLeader)
	t.Logf("leader failover: %d -> %d", oldLeader, newLeader)

	c.write(OpAppend, "x", "B", 1001, 2)
	if got := c.read("x"); got != "AB" {
		t.Fatalf("read after failover: got %q, want AB", got)
	}

	// 旧节点从自己的持久化状态重新启动并追赶新日志。
	c.startNode(oldLeader, nil)
	c.waitFor("restarted node catch up", 12*time.Second, func() bool {
		value, _ := c.valueAt(oldLeader, "x")
		return value == "AB"
	})
}

func TestRaftKVDuplicateAfterFailover(t *testing.T) {
	c := newIntegrationCluster(t, 3, -1)
	oldLeader := c.leader(-1)

	// 同一个 (ClientID,CommandID) 可以在 Leader 变更后重试。
	c.write(OpAppend, "x", "A", 2026, 7)
	c.stopNode(oldLeader)
	c.leader(oldLeader)
	c.write(OpAppend, "x", "A", 2026, 7) // 故意重复

	if got := c.read("x"); got != "A" {
		t.Fatalf("duplicate Append executed across leaders: got %q", got)
	}
}

func TestRaftKVSnapshotCatchup(t *testing.T) {
	c := newIntegrationCluster(t, 3, 2048)
	leader := c.leader(-1)
	lagging := (leader + 1) % 3

	// 只阻断落后节点的非空日志复制和 Snapshot，保留心跳。
	c.blockReplication[lagging].Store(true)
	const count = 30
	payload := strings.Repeat("z", 160)
	for i := int64(1); i <= count; i++ {
		c.write(OpAppend, "x", payload, 3001, i)
	}
	expected := strings.Repeat(payload, count)

	// 多数派能正常提交，并且至少一个在线节点已经生成快照。
	c.waitFor("leader snapshot", 12*time.Second, func() bool {
		for i := range c.nodes {
			if i != lagging && c.snapshotIndex(i) > 0 {
				return true
			}
		}
		return false
	})
	_, laggedIndex := c.valueAt(lagging, "x")
	_, leaderIndex := c.valueAt(leader, "x")
	if laggedIndex >= leaderIndex {
		t.Fatalf("follower did not lag: follower=%d, leader=%d", laggedIndex, leaderIndex)
	}

	// 解除网络阻断。由于之前的日志前缀已经压缩，
	// 落后节点应该通过 InstallSnapshot 安装快照。
	c.blockReplication[lagging].Store(false)
	c.waitFor("InstallSnapshot delivered to lagging follower", 15*time.Second, func() bool {
		return c.snapshotRPCs[lagging].Load() > 0
	})
	c.waitFor("lagging follower restore and catch up", 15*time.Second, func() bool {
		value, _ := c.valueAt(lagging, "x")
		return value == expected
	})
	if got := c.read("x"); got != expected {
		t.Fatalf("read after snapshot catch-up: length=%d, want=%d", len(got), len(expected))
	}
}

func TestRaftKVSnapshotRestart(t *testing.T) {
	c := newIntegrationCluster(t, 3, 2048)
	c.leader(-1)

	payload := strings.Repeat("R", 160)
	const count = 30
	for i := int64(1); i <= count; i++ {
		c.write(OpAppend, "persist", payload, 4001, i)
	}
	expected := strings.Repeat(payload, count)

	// 至少确认所有副本在重启前都有自己的有效 Snapshot。
	c.waitFor("all nodes persist snapshots", 12*time.Second, func() bool {
		for i := range c.nodes {
			if c.snapshotIndex(i) == 0 {
				return false
			}
		}
		return true
	})
	for i := range c.nodes {
		c.stopNode(i)
	}
	for i := range c.nodes {
		c.startNode(i, nil)
	}
	c.leader(-1)

	if got := c.read("persist"); got != expected {
		t.Fatalf("data lost after full restart: length=%d, want=%d", len(got), len(expected))
	}

	// 使用快照前已执行的请求号再次 Append，必须被去重。
	c.write(OpAppend, "persist", payload, 4001, 1)
	if got := c.read("persist"); got != expected {
		t.Fatalf("dedup lost after restart: length=%d, want=%d", len(got), len(expected))
	}

	// 重启后新的写请求仍然可以提交。
	c.write(OpAppend, "persist", "!", 4001, count+1)
	if got := c.read("persist"); got != expected+"!" {
		t.Fatalf("write after restart failed: got length %d", len(got))
	}
}

func TestRaftKVConcurrentClients(t *testing.T) {
	c := newIntegrationCluster(t, 3, -1)
	c.leader(-1)

	const clients = 10
	results := make(chan error, clients)
	for i := 0; i < clients; i++ {
		go func(id int) {
			ck, err := NewClerk(c.addresses)
			if err != nil {
				results <- err
				return
			}
			defer ck.Close()
			token := fmt.Sprintf("<%02d>", id)
			reply := ck.Command(OpAppend, "parallel", token)
			if reply.Err != OK {
				results <- fmt.Errorf("client %d: %s", id, reply.Err)
				return
			}
			results <- nil
		}(i)
	}

	deadline := time.After(20 * time.Second)
	for i := 0; i < clients; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("concurrent clerks timed out")
		}
	}

	got := c.read("parallel")
	// append 顺序由 Raft 决定，不假设具体次序；每个客户端 token 必须恰好出现一次。
	for i := 0; i < clients; i++ {
		token := fmt.Sprintf("<%02d>", i)
		if n := strings.Count(got, token); n != 1 {
			t.Fatalf("token %s appears %d times in %q", token, n, got)
		}
	}
}

func TestRaftKVNoQuorumCannotAcknowledge(t *testing.T) {
	c := newIntegrationCluster(t, 3, -1)
	leader := c.leader(-1)

	// 切断另外两个节点，旧 Leader 只剩自己，不能提交新的写请求。
	for id := range c.nodes {
		if id != leader {
			c.stopNode(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	resp, err := c.clients[leader].Command(ctx, &kvpb.CommandRequest{
		Op: string(OpPut), Key: "no-quorum", Value: "X",
		ClientId: 5001, CommandId: 1,
	})
	if err == nil && resp != nil && Err(resp.Err) == OK {
		t.Fatal("write acknowledged successfully without majority")
	}
	// 这里不能断言这个超时写请求将来一定不会执行；
	// 恢复多数派后它仍可能被 Raft 提交，因此客户端重试必须沿用原请求 ID。
}
