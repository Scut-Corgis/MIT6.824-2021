package kvraft

import (
	"6.824/raft"
)

// long time running GoRoutine, Service层检测Raft提交信息
// eg. 经过长时间的bug排查，发现killed()方法可能会导致内存泄露(重复释放内存或指针指向已经释放的内存)，猜测是mit的测试程序Kill方法
// 有问题，所以在我的实现中直接杀不死service层只能杀死raft层，避开有问题的bug代码
// 最终还是会2%的几率少数测试出现内存泄露，不知道为什么。
func (kv *KVServer) ReadRaftApplyCommandLoop() {
	// for !kv.killed() {
	// 	message := <-kv.applyCh
	// 	if message.CommandValid {
	// 		kv.GetCommandFromRaft(message)
	// 	} else if message.SnapshotValid {
	// 		kv.GetSnapShotFromRaft(message)
	// 	}

	// }
	for message := range kv.applyCh {
		// listen to every command applied by its raft ,delivery to relative RPC Handler
		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}

	}
}

// 收到Raft层的提交命令信息，进行处理
func (kv *KVServer) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)
	DPrintf("{%d}***RaftApplyCommand, Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}
	//DEBUG用
	if kv.ifRequestDuplicate(op.ClientId, op.RequestId) && op.Operation != "get" {
		INFO("{%d}***出现重复命令 LogIndex:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
	}

	// 重复命令不执行
	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
		// execute command
		if op.Operation == "put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == "append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}
	//检查是否需要生成快照
	if kv.maxraftstate != -1 {
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChan(op, message.CommandIndex)
}

// 命令已Raft replicate处理完毕，发送处理完成信息给上层
func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	if exist {
		DPrintf("{%d}***KVServerExeGET---- ClientId :%d ,RequestID :%d ,Key : %v, value :%v", kv.me, op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("{%d}***KVServerExeGET---- ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", kv.me, op.ClientId, op.RequestId, op.Key)
	}
	kv.DprintfKVDB()
	return value, exist
}

func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {

	kv.mu.Lock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("{%d}***KVServerExePUT---- ClientId :%d ,RequestID :%d ,Key : %v, value : %v", kv.me, op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

func (kv *KVServer) ExecuteAppendOpOnKVDB(op Op) {
	//if op.IfDuplicate {
	//	return
	//}

	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("{%d}***KVServerExeAPPEND----- ClientId :%d ,RequestID :%d ,Key : %v, value : %v", kv.me, op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

// 判断是否是重复请求
func (kv *KVServer) ifRequestDuplicate(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := kv.lastRequestId[newClientId]
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}
