package shardkv

import "time"

func (kv *ShardKV) PullNewConfigLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		_, ifLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !ifLeader {
			time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
			continue
		}
		// 此处并不是请求最新config， 而是比自己大一号的config
		newestConfig := kv.mck.Query(lastConfigNum + 1)
		// 若确实自己的config落后了，则将此消息通过raft共识出去
		if newestConfig.Num == lastConfigNum+1 {
			// Got a new Config
			op := Op{Operation: NEWCONFIGOp, Config_NEWCONFIG: newestConfig}
			kv.mu.Lock()
			if _, ifLeader := kv.rf.GetState(); ifLeader {
				kv.rf.Start(op)
				//raftIndex, _, _ := kv.rf.Start(op)
				//DPrintf("[NewConfigPull]Gid %d, Server %d, RaftIndex %d, ConfigNum %d,lastConfigNum %d",kv.gid,kv.me,raftIndex,newestConfig.Num,lastConfigNum)
			}
			kv.mu.Unlock()
		}

		time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
	}
}

// 会间歇检测migrate bool数组，如果有true，说明已经转移成了新config，则需要数据迁移
func (kv *ShardKV) SendShardToOtherGroupLoop() {
	for !kv.killed() {
		kv.mu.Lock()
		_, ifLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !ifLeader {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		noMigrateing := true
		kv.mu.Lock()
		for shard := 0; shard < NShards; shard++ {
			if kv.migratingShard[shard] {
				noMigrateing = false
			}
		}
		kv.mu.Unlock()
		if noMigrateing {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		ifNeedSend, sendData := kv.ifHaveSendData()
		if !ifNeedSend {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}
		//DPrintf("[MakeSendData]%v",sendData)
		kv.sendShardComponent(sendData)
		time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) ifHaveSendData() (bool, map[int][]ShardComponent) {
	sendData := kv.MakeSendShardComponent()
	if len(sendData) == 0 {
		return false, make(map[int][]ShardComponent)
	}
	//DPrintf("[OriganalMakeData]%v",sendData)
	return true, sendData
}

// 返回值： 分片现在的group(应该发送至的集群) 对应的 分片数据结构数组(因为可能会有多个分片分给了对应的group)
func (kv *ShardKV) MakeSendShardComponent() map[int][]ShardComponent {
	// kv.config already be update
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)
	for shard := 0; shard < NShards; shard++ {
		nowOwner := kv.config.Shards[shard]
		// 数据标记为迁移 并且拥有者自己不是自己这个group了
		if kv.migratingShard[shard] && kv.gid != nowOwner {
			tempComponent := ShardComponent{ShardIndex: shard, KVDBOfShard: make(map[string]string), ClientRequestId: make(map[int64]int)}
			CloneSecondComponentIntoFirstExceptShardIndex(&tempComponent, kv.kvDB[shard])
			sendData[nowOwner] = append(sendData[nowOwner], tempComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for aimGid, ShardComponents := range sendData {
		kv.mu.Lock()
		args := &MigrateShardArgs{ConfigNum: kv.config.Num, MigrateData: make([]ShardComponent, 0)}
		groupServers := kv.config.Groups[aimGid]
		kv.mu.Unlock()
		for _, components := range ShardComponents {
			tempComponent := ShardComponent{ShardIndex: components.ShardIndex, KVDBOfShard: make(map[string]string), ClientRequestId: make(map[int64]int)}
			CloneSecondComponentIntoFirstExceptShardIndex(&tempComponent, components)
			args.MigrateData = append(args.MigrateData, tempComponent)
		}

		go kv.callMigrateRPC(groupServers, args)
	}
}

// 发送方push不属于自己的分片出去，成功返回后raft持久化(共识),最终修改迁移数组
func (kv *ShardKV) callMigrateRPC(groupServers []string, args *MigrateShardArgs) {
	for _, groupMember := range groupServers {
		callEnd := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := callEnd.Call("ShardKV.MigrateShard", args, &migrateReply)
		kv.mu.Lock()
		myConfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && migrateReply.Err == OK {
			// Send to Raft : I'have Send my Components out
			if myConfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData) {
				return
			} else {
				kv.rf.Start(Op{Operation: MIGRATESHARDOp, MigrateData_MIGRATE: args.MigrateData, ConfigNum_MIGRATE: args.ConfigNum})
				return
			}
		}
	}
}

// 分片接受方收到的RPC调用
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()
	if args.ConfigNum > myConfigNum {
		reply.Err = ErrConfigNum
		reply.ConfigNum = myConfigNum
		return
	}

	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}

	if kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}

	//DPrintf("[GET SendToWrongLeader]From Client %d (Request %d) To Server %d",args.ClientId,args.RequestId, kv.me)
	op := Op{Operation: MIGRATESHARDOp, MigrateData_MIGRATE: args.MigrateData, ConfigNum_MIGRATE: args.ConfigNum}

	raftIndex, _, _ := kv.rf.Start(op)

	// create waitForCh
	kv.mu.Lock()
	//DPrintf("[MiGRATE StartToRaft]From GId %d, Server %d ,ConfigNum %d", kv.gid,kv.me, args.ConfigNum)
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()
	// timeout
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		//DPrintf("[MiGATE TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)
		kv.mu.Lock()
		_, ifLeaderr := kv.rf.GetState()
		tempConfig := kv.config.Num
		kv.mu.Unlock()

		if args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) && ifLeaderr {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-chForRaftIndex:
		//DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		kv.mu.Lock()
		tempConfig := kv.config.Num
		kv.mu.Unlock()
		if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum && args.ConfigNum <= tempConfig && kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigNum = tempConfig
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}
