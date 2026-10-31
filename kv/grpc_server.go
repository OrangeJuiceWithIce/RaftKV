package kvraft

import (
	"context"

	kvpb "github.com/OrangeJuiceWithIce/raftkv/kv/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVGRPCServer struct {
	kvpb.UnimplementedKVServiceServer

	kv *KVServer
}

var _ kvpb.KVServiceServer = (*KVGRPCServer)(nil)

func NewKVGRPCServer(kv *KVServer) *KVGRPCServer {
	if kv == nil {
		panic("nil KVServer")
	}

	return &KVGRPCServer{
		kv: kv,
	}
}

func (s *KVGRPCServer) Command(
	ctx context.Context,
	req *kvpb.CommandRequest,
) (*kvpb.CommandResponse, error) {

	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"nil CommandRequest",
		)
	}

	cmd := Command{
		Op:        OpType(req.Op),
		Key:       req.Key,
		Value:     req.Value,
		ClientID:  req.ClientId,
		CommandID: req.CommandId,
	}

	reply := s.kv.Execute(ctx, cmd)

	resp := &kvpb.CommandResponse{
		Err:   string(reply.Err),
		Value: reply.Value,
	}

	return resp, nil
}
