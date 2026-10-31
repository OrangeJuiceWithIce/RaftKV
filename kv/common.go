package kvraft

import "encoding/gob"

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrInvalidOp   Err = "ErrInvalidOp"
	ErrStopped     Err = "ErrStopped"
)

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

// 客户端发送到 KV Server 的请求
type CommandRequest struct {
	Op        OpType
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
}

// KV Server 返回给客户端的结果
type CommandReply struct {
	Err   Err
	Value string
}

// 真正传递给 Raft.Start() 的命令
type Command struct {
	Op        OpType
	Key       string
	Value     string
	ClientID  int64
	CommandID int64
}

// 记录一个客户端最后执行成功的写操作
// 后续用于 RPC 重试去重
type LastOperation struct {
	CommandID int64
	Reply     CommandReply
}

type ApplyResult struct {
	Command Command
	Reply   CommandReply
}

// Raft 的 LogEntry.Command 是 interface{}，
// 使用 gob 编码时需要注册具体类型。
func init() {
	gob.Register(Command{})
}
