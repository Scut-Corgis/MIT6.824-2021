package shardctrler

import (
	"6.824/raft"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//

func (sc *ShardCtrler) ReadRaftApplyCommandLoop() {
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
		}
	}
}

func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= sc.lastSSPointRaftLogIndex {
		return
	}
	//DPrintf("[RaftApplyCommand]Server %d , Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v",sc.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation)
	if !sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == JoinOp {
			sc.ExecJoinOnController(op)
		}
		if op.Operation == LeaveOp {
			sc.ExecLeaveOnController(op)
		}
		if op.Operation == MoveOp {
			sc.ExecMoveOnController(op)
		}
	}

	if sc.maxraftstate != -1 {
		sc.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	sc.SendMessageToWaitChan(op, message.CommandIndex)
}

func (sc *ShardCtrler) SendMessageToWaitChan(op Op, raftIndex int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitApplyCh[raftIndex]
	if exist {
		ch <- op
	}
	return exist
}

// 打印对应配置的信息，用于debug
func ShowConfig(config Config, op string) {
	DPrintf("=========== Config For op %v", op)
	DPrintf("[ConfigNum]%d", config.Num)
	for index, value := range config.Shards {
		DPrintf("[shards]Shard %d --> gid %d", index, value)
	}
	for gid, servers := range config.Groups {
		DPrintf("[Groups]Gid %d --> servers %v", gid, servers)
	}
}

func (sc *ShardCtrler) ExecQueryOnController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.Num_Query == -1 || op.Num_Query >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[op.Num_Query]
	}

}

func (sc *ShardCtrler) ExecJoinOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, JOIN, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, *sc.MakeJoinConfig(op.Servers_Join))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ExecLeaveOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, LEAVE, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, *sc.MakeLeaveConfig(op.Gids_Leave))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ExecMoveOnController(op Op) {
	sc.mu.Lock()
	//DPrintf("[Exec]Server %d, MOVE, ClientId %d, RequestId %d",sc.me, op.ClientId,op.RequestId)
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, *sc.MakeMoveConfig(op.Shard_Move, op.Gid_Move))
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ifRequestDuplicate(newClientId int64, newRequestId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// return true if message is duplicate
	lastRequestId, ifClientInRecord := sc.lastRequestId[newClientId]
	if !ifClientInRecord {
		// kv.lastRequestId[newClientId] = newRequestId
		return false
	}
	return newRequestId <= lastRequestId
}
