package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	kvraft "github.com/OrangeJuiceWithIce/raftkv/kv"
)

func main() {
	nodes := flag.String(
		"nodes",
		"127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003",
		"comma-separated KV node addresses",
	)

	op := flag.String(
		"op",
		"get",
		"operation: get, put, append",
	)

	key := flag.String(
		"key",
		"",
		"key",
	)

	value := flag.String(
		"value",
		"",
		"value for put or append",
	)

	flag.Parse()

	addresses := strings.Split(*nodes, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}

	// 创建 Clerk。
	ck, err := kvraft.NewClerk(addresses)
	if err != nil {
		log.Fatal(err)
	}
	defer ck.Close()

	var commandOp kvraft.OpType

	switch strings.ToLower(*op) {
	case "get":
		commandOp = kvraft.OpGet

	case "put":
		commandOp = kvraft.OpPut

	case "append":
		commandOp = kvraft.OpAppend

	default:
		log.Fatalf("unknown operation: %s", *op)
	}

	// 调用已有的统一 Command RPC。
	reply := ck.Command(commandOp, *key, *value)

	switch reply.Err {
	case kvraft.OK:
		if commandOp == kvraft.OpGet {
			fmt.Fprintln(os.Stdout, reply.Value)
		} else {
			fmt.Println("OK")
		}

	case kvraft.ErrNoKey:
		fmt.Println("key not found")

	default:
		log.Fatalf("KV command failed: %s", reply.Err)
	}
}
