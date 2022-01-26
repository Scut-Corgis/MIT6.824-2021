package kvraft

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
)

func (kv *KVServer) IfNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		// Send SnapShot Command
		snapshot := kv.MakeSnapShot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

// Handler the SnapShot from kv.rf.applyCh
func (kv *KVServer) GetSnapShotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapShotToInstall(message.Snapshot)
		DPrintf("{%d}***上层收到了raft快照lastSSPointRaftLogIndex: %d", kv.me, message.SnapshotIndex)
		kv.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}

// SnapShot include KVDB, lastrequestId map
// Pass it to raft when server decide to start a snapshot
func (kv *KVServer) MakeSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

// 外层有锁，无需加锁
func (kv *KVServer) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // 无数据直接返回
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb map[string]string
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!", kv.me)
	} else {
		kv.kvDB = persist_kvdb
		kv.lastRequestId = persist_lastRequestId
	}
}
