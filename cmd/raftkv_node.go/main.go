package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	kvraft "github.com/OrangeJuiceWithIce/raftkv/kv"
	kvpb "github.com/OrangeJuiceWithIce/raftkv/kv/pb"
	"github.com/OrangeJuiceWithIce/raftkv/raft"
	raftpb "github.com/OrangeJuiceWithIce/raftkv/raft/pb"

	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// 1. 解析启动参数。
	id := flag.Int(
		"id",
		-1,
		"Raft node ID, starting from 0",
	)

	nodes := flag.String(
		"nodes",
		"127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003",
		"comma-separated RaftKV node addresses",
	)

	dataDir := flag.String(
		"data-dir",
		"./data",
		"persistent data directory",
	)

	maxRaftState := flag.Int(
		"max-raft-state",
		4096,
		"maximum Raft state size, -1 disables snapshots",
	)

	flag.Parse()

	// 2. 解析集群成员地址。
	addresses := strings.Split(*nodes, ",")

	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
		if addresses[i] == "" {
			return fmt.Errorf("empty node address at %d", i)
		}
	}

	if *id < 0 || *id >= len(addresses) {
		return fmt.Errorf(
			"invalid node ID %d for %d nodes",
			*id,
			len(addresses),
		)
	}

	// 3. 为当前节点创建 TCP Listener。
	//
	// 每个进程只监听属于自己的地址。
	listener, err := net.Listen(
		"tcp",
		addresses[*id],
	)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	// 4. 创建 Raft PeerClient。
	//
	// peers[i] 对应 Raft 节点 i。
	// 自己的位置留 nil，不需要连接自己。
	peers := make(
		[]*raft.PeerClient,
		len(addresses),
	)

	defer func() {
		for _, peer := range peers {
			if peer != nil {
				_ = peer.Close()
			}
		}
	}()

	for i, address := range addresses {
		if i == *id {
			continue
		}

		peer, err := raft.NewPeerClient(address)
		if err != nil {
			return fmt.Errorf(
				"create peer %d: %w",
				i,
				err,
			)
		}

		peers[i] = peer
	}

	// 5. 为当前节点创建独立的持久化文件。
	//
	// 即使三个节点使用相同 data-dir，
	// 也会写入不同的 node-ID 子目录。
	statePath := filepath.Join(
		*dataDir,
		fmt.Sprintf("node-%d", *id),
		"state.gob",
	)

	persister, err := raft.NewPersister(statePath)
	if err != nil {
		return fmt.Errorf(
			"create persister: %w",
			err,
		)
	}

	// 6. 创建 KVServer。
	//
	// NewKVServer 内部：
	// - 恢复 KV Snapshot
	// - 创建 Raft
	// - 恢复 Raft 持久化状态
	// - 启动 KV applier
	kv, err := kvraft.NewKVServer(
		*id,
		peers,
		persister,
		*maxRaftState,
	)
	if err != nil {
		return fmt.Errorf(
			"create KVServer: %w",
			err,
		)
	}
	defer kv.Kill()

	// 7. 创建一个 gRPC 网络服务器。
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	// 8. 注册 RaftService。
	//
	// 注意：使用 KVServer 内部的同一个 Raft。
	raftpb.RegisterRaftServiceServer(
		grpcServer,
		raft.NewGRPCServer(kv.Raft()),
	)

	// 9. 注册 KVService。
	kvpb.RegisterKVServiceServer(
		grpcServer,
		kvraft.NewKVGRPCServer(kv),
	)

	log.Printf(
		"RaftKV node %d listening at %s, state=%s",
		*id,
		addresses[*id],
		statePath,
	)

	// 10. 启动 gRPC 服务。
	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	// 11. 等待终止信号，执行资源清理。
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	select {
	case <-ctx.Done():
		log.Printf("RaftKV node %d shutting down", *id)
		return nil

	case err := <-serveErr:
		if err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("gRPC serve: %w", err)
		}
		return nil
	}
}
