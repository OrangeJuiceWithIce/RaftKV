package raft

import (
	"context"

	pb "github.com/OrangeJuiceWithIce/raftkv/pb"
)

// gRPC 服务端实现。
type GRPCServer struct {
	pb.UnimplementedRaftServiceServer

	rf *Raft
}

func NewGRPCServer(rf *Raft) *GRPCServer {
	return &GRPCServer{rf: rf}
}

// 接收 gRPC RequestVote 请求。
func (s *GRPCServer) RequestVote(
	_ context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {

	// Protobuf -> Raft 内部类型
	args := RequestVoteArgs{
		Term:         int(req.Term),
		CandidateId:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	var reply RequestVoteReply

	s.rf.HandleRequestVote(&args, &reply)

	// Raft 内部类型 -> Protobuf
	return &pb.RequestVoteResponse{
		Term:        int64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

// 接收 gRPC AppendEntries 请求。
func (s *GRPCServer) AppendEntries(
	_ context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	entries := make(
		[]LogEntry,
		0,
		len(req.Entries),
	)

	for _, e := range req.Entries {
		command, err := decodeCommand(e.Command)
		if err != nil {
			return nil, err
		}

		entries = append(entries, LogEntry{
			Term:    int(e.Term),
			Command: command,
		})
	}

	args := AppendEntriesArgs{
		Term:         int(req.Term),
		LeaderId:     int(req.LeaderId),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
	}

	var reply AppendEntriesReply

	s.rf.HandleAppendEntries(&args, &reply)

	return &pb.AppendEntriesResponse{
		Term:    int64(reply.Term),
		Success: reply.Success,
	}, nil
}
