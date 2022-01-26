package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const CONSENSUS_TIMEOUT = 500 // 客户端等待Raft共识最长的时间

type Op struct {
	Operation string //"get" "put" "append"
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg //Raft层和Service层交换 logs和snapshot的通道
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big——注意单位为字节数，即当前Raft需持久化变量占用内存

	// Your definitions here.
	kvDB          map[string]string //KV数据库
	waitApplyCh   map[int]chan Op   //log index -> chan，用于唤醒客户端RPC调用(即数据库操作)，当Raft提交给Service层了，便将此Op应用到状态机并反馈给客户端
	lastRequestId map[int64]int     //客户端id -> requestId

	//最后快照位置, 对应raft log index
	lastSSPointRaftLogIndex int
}

// 打印数据库中现存所有值
func (kv *KVServer) DprintfKVDB() {
	if !Debug {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	INFO("{%d}***DBInfo ----", kv.me)
	for key, value := range kv.kvDB {
		INFO("Key : %v, Value : %v", key, value)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	//判断自己是否为Leader
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//转发给Raft
	op := Op{Operation: "get", Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}
	DPrintf("{%d}***GET StartToRaft From Client %d (Request %d) To Server %d, Key : %v", kv.me, args.ClientId, args.RequestId, kv.me, op.Key)
	raftIndex, _, _ := kv.rf.Start(op)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	//等待Chan或超时
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		DPrintf("{%d}***GET TIMEOUT!!! From Client %d (Request %d) To Server %d, Key : %v, raftIndex %d", kv.me, args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

		_, ifLeader := kv.rf.GetState()
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader {
			// 一个极其阴险的bug
			// 执行到这里是极少数情况， 即Client给Server发了Get()RPC, 但是Server执行完后要回复给Client时网络出现故障，导致RPC直接返回失败，因此Client会转而去寻找其他Leader，在
			// 一个循环后重新回到当前这个Leader继续提交同样的已经commit的请求，但是又出现了网络拥塞，出现了超时，因此就会出现ifRequestDuplicate为True的情况，所以这种情况发送完全可以再执行一遍
			// Get(),当然PutAppend同理，直接返回ok表示已经执行过了
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		DPrintf("{%d}***WaitChanGetRaftApplyMessage<-- , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			//不相等，说明发生了网络分区，此server的log被后来的leader冲刷掉了，所以返回WrongLeader要求客户端访问新的leader
			reply.Err = ErrWrongLeader
		}

	}
	//释放无用chan的内存占用
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		//DPrintf("[PUTAPPEND SendToWrongLeader]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
		return
	}

	op := Op{Operation: args.Operation, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}

	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("{%d}***PUTAPPEND StartToRaft From Client %d (Request %d) To Server %d, Key: %v, raftIndex %d", kv.me, args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	// create waitForCh
	kv.mu.Lock()
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		DPrintf("{%d}***TIMEOUT PUTAPPEND !!!! , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		DPrintf("{%d}***WaitChanGetRaftApplyMessage<--, get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			// 极少数情况会进入这，原因参考Get()的注释
			reply.Err = OK
		} else {
			//不相等，说明发生了网络分区，此server的log被后来的leader冲刷掉了，所以返回WrongLeader要求客户端访问新的leader
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	DPrintf("{%d}***Server begin！！！", me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShotToInstall(snapshot)
	}

	go kv.ReadRaftApplyCommandLoop()
	return kv
}
