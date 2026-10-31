package raft

import (
	"context"

	pb "github.com/OrangeJuiceWithIce/raftkv/raft/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	conn   *grpc.ClientConn
	client pb.RaftServiceClient
}

// 创建一个指向其他 Raft 节点的客户端。
func NewPeerClient(addr string) (*PeerClient, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, err
	}

	return &PeerClient{
		conn:   conn,
		client: pb.NewRaftServiceClient(conn),
	}, nil
}

// 关闭底层 ClientConn。
func (p *PeerClient) Close() error {
	return p.conn.Close()
}

// 向其他 Raft 节点请求投票。
func (p *PeerClient) RequestVote(
	ctx context.Context,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {

	req := &pb.RequestVoteRequest{
		Term:         int64(args.Term),
		CandidateId:  int64(args.CandidateId),
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	}

	resp, err := p.client.RequestVote(ctx, req)
	if err != nil {
		return nil, err
	}

	return &RequestVoteReply{
		Term:        int(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

// 向其他 Raft 节点发送 AppendEntries。
func (p *PeerClient) AppendEntries(
	ctx context.Context,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {

	entries := make(
		[]*pb.LogEntry,
		0,
		len(args.Entries),
	)

	for _, e := range args.Entries {
		command, err := encodeCommand(e.Command)
		if err != nil {
			return nil, err
		}

		entries = append(entries, &pb.LogEntry{
			Index:   int64(e.Index),
			Term:    int64(e.Term),
			Command: command,
		})
	}

	req := &pb.AppendEntriesRequest{
		Term:         int64(args.Term),
		LeaderId:     int64(args.LeaderId),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int64(args.LeaderCommit),
	}

	resp, err := p.client.AppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}

	return &AppendEntriesReply{
		Term:          int(resp.Term),
		Success:       resp.Success,
		ConflictIndex: int(resp.ConflictIndex),
		ConflictTerm:  int(resp.ConflictTerm),
	}, nil
}

func (p *PeerClient) InstallSnapshot(
	ctx context.Context,
	args *InstallSnapshotArgs,
) (*InstallSnapshotReply, error) {

	req := &pb.InstallSnapshotRequest{
		Term:              int64(args.Term),
		LeaderId:          int64(args.LeaderId),
		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),
		Data:              args.Data,
	}

	resp, err := p.client.InstallSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	return &InstallSnapshotReply{
		Term: int(resp.Term),
	}, nil
}
