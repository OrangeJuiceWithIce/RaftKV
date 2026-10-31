package kvraft

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	kvpb "github.com/OrangeJuiceWithIce/raftkv/kv/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	clientRPCTimeout = 1500 * time.Millisecond
	clientRetryDelay = 80 * time.Millisecond
)

type KVPeer struct {
	conn   *grpc.ClientConn
	server kvpb.KVServiceClient
}

type Clerk struct {
	mu sync.Mutex

	peers []KVPeer

	leader int

	clientID int64

	commandID int64
}

func nrand() (int64, error) {
	max := big.NewInt(int64(1) << 62)

	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return 0, err
	}

	return n.Int64(), nil
}

func NewClerk(addresses []string) (*Clerk, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("empty KV server addresses")
	}

	id, err := nrand()
	if err != nil {
		return nil, err
	}

	ck := &Clerk{
		peers:     make([]KVPeer, len(addresses)),
		leader:    0,
		clientID:  id,
		commandID: 1,
	}

	for i, address := range addresses {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			ck.Close()
			return nil, err
		}

		ck.peers[i].conn = conn
		ck.peers[i].server = kvpb.NewKVServiceClient(conn)
	}

	return ck, nil
}

func (ck *Clerk) Close() {
	for _, peer := range ck.peers {
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
	}
}

func (ck *Clerk) Command(
	op OpType,
	key string,
	value string,
) CommandReply {
	if op != OpGet && op != OpPut && op != OpAppend {
		return CommandReply{Err: ErrInvalidOp}
	}

	ck.mu.Lock()
	defer ck.mu.Unlock()

	req := &kvpb.CommandRequest{
		Op:        string(op),
		Key:       key,
		Value:     value,
		ClientId:  ck.clientID,
		CommandId: ck.commandID,
	}

	server := ck.leader

	for {

		ctx, cancel := context.WithTimeout(
			context.Background(),
			clientRPCTimeout,
		)

		resp, err := ck.peers[server].server.Command(
			ctx,
			req,
		)

		cancel()

		if err == nil && resp != nil {
			result := CommandReply{
				Err:   Err(resp.Err),
				Value: resp.Value,
			}

			switch result.Err {

			case OK:
				// 请求已获得明确的成功响应。
				ck.leader = server
				ck.commandID++
				return result

			case ErrNoKey:
				if op == OpGet {
					ck.leader = server
					ck.commandID++
					return result
				}

				panic("unexpected ErrNoKey for write request")

			case ErrWrongLeader, ErrTimeout, ErrStopped:
				// 结果不确定，尝试其他节点。

			case ErrInvalidOp:
				// 合法请求不应收到这个结果。
				panic("KV Server rejected a valid operation")

			default:
				panic(fmt.Sprintf(
					"unknown KV response: %s",
					result.Err,
				))
			}
		}

		server = (server + 1) % len(ck.peers)

		time.Sleep(clientRetryDelay)
	}
}

// Get 读取 key 对应的值。
func (ck *Clerk) Get(key string) string {
	reply := ck.Command(OpGet, key, "")

	return reply.Value
}

// Put 覆盖 key 对应的值。
func (ck *Clerk) Put(key, value string) {
	ck.Command(OpPut, key, value)
}

// Append 向 key 当前值后追加内容。
func (ck *Clerk) Append(key, value string) {
	ck.Command(OpAppend, key, value)
}

func (ck *Clerk) PutAppend(
	key string,
	value string,
	op string,
) {
	ck.Command(OpType(op), key, value)
}
