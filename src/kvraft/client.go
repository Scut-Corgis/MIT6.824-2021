package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd //注意与Server不对应，位置随机
	// You will have to modify this struct.
	clientId       int64
	requestId      int
	recentLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.recentLeaderId = len(servers) - 1
	DPrintf("Client %d Start!", ck.clientId)
	time.Sleep(time.Millisecond * 550)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId

	for {
		args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: requestId}
		reply := GetReply{}

		DPrintf("[ClientSend GET]From ClientId %d, RequesetId %d, To Server %d, key : %v", ck.clientId, ck.requestId, server, key)
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			DPrintf("[ClientSend GET SUCCESS]From ClientId %d, RequesetId %d, key : %v, get value :%v", ck.clientId, ck.requestId, key, reply.Value)
			ck.recentLeaderId = server
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, operation string) {
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId

	for {
		args := PutAppendArgs{Key: key, Value: value, Operation: operation, ClientId: ck.clientId, RequestId: requestId}
		reply := PutAppendReply{}
		DPrintf("[ClientSend PUTAPPEND]From ClientId %d, RequesetId %d, To Server %d, key : %v, value : %s, Opreation : %v", ck.clientId, ck.requestId, server, key, value, operation)
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = server
			DPrintf("[ClientSend PUTAPPEND SUCCESS]From ClientId %d, RequesetId %d, key : %v, value : %v, Opreation : %v", ck.clientId, ck.requestId, key, value, operation)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "append")
}
