package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

//leader给follower发送快照的RPC调用
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	reply.Term = args.Term
	if rf.currentState != ROLE_FOLLOWER {
		rf.currentState = ROLE_FOLLOWER
		rf.votedFor = args.LeaderId
		rf.persist()
	}
	if rf.lastSSPointIndex >= args.LastIncludedIndex {
		INFO("[%d]---SnapShot bigger than leader [%d]", rf.me, args.LeaderId)
		rf.mu.Unlock()
		return
	}
	index := args.LastIncludedIndex
	tempLogs := make([]LogEntry, 0)
	for i := index + 1; i <= rf.lastLogIndex(); i++ {
		tempLogs = append(tempLogs, rf.getLogWithIndex(i))
	}
	rf.lastSSPointTerm = args.LastIncludedTerm
	rf.lastSSPointIndex = args.LastIncludedIndex

	//交换
	rf.logs = tempLogs
	INFO("[%d]---Current logSize %d all logSize %d", rf.me, len(rf.logs), rf.lastLogIndex())
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persistStateAndSnapshot(args.Data)
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastSSPointTerm,
		SnapshotIndex: rf.lastSSPointIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	DPrintf("[%d]---InstallSnapshot from leader [%d], index %d", rf.me, args.LeaderId, rf.lastSSPointIndex)
}

//这个函数的定义比较复杂，见课程网站lab4的解释，我的实现是它一直返回true就行
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//上层Service层创建了一个快照，Raft层可以截断logs了
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//断言不会超过commitIndex,因为我的实现持久化了commitIndex
	if index > rf.commitIndex || index > rf.lastApplied {
		DPrintFatal("In Snapshot : index > rf.commitIndex || index > rf.lastApplied")
	}
	if rf.lastSSPointIndex >= index {
		return
	}
	tempLogs := make([]LogEntry, 0)
	for i := index + 1; i <= rf.lastLogIndex(); i++ {
		tempLogs = append(tempLogs, rf.getLogWithIndex(i))
	}
	rf.lastSSPointTerm = rf.getLogTermWithIndex(index)
	rf.lastSSPointIndex = index

	rf.logs = tempLogs
	rf.persistStateAndSnapshot(snapshot)
	INFO("[%d]---Current logSize %d all logSize %d", rf.me, len(rf.logs), rf.lastLogIndex())
	DPrintf("[%d]---Change Snapshot until index %d", rf.me, rf.lastSSPointIndex)
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	DPrintf("[%d]---Leader in term %d send snapshot to follower [%d], index %d", rf.me, rf.currentTerm, server, rf.lastSSPointIndex)
	ssArgs := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastSSPointIndex,
		rf.lastSSPointTerm,
		rf.persister.ReadSnapshot(),
	}
	ssReply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendSnapShot(server, &ssArgs, &ssReply)

	if ok {
		rf.mu.Lock()
		if rf.currentState != ROLE_LEADER || rf.currentTerm != ssArgs.Term {
			rf.mu.Unlock()
			return
		}
		if ssReply.Term > rf.currentTerm {
			rf.currentTerm = ssReply.Term
			rf.currentState = ROLE_FOLLOWER
			rf.votedFor = -1
			rf.persist()
			rf.resetTimer()
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[server] = Max(rf.matchIndex[server], ssArgs.LastIncludedIndex)
		rf.nextIndex[server] = Max(rf.matchIndex[server]+1, ssArgs.LastIncludedIndex+1)
		INFO("[%d]---Received true from [%d], Now matchIndex = %d, nextIndex = %d---<leaderSendSnapShot> ", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
		rf.mu.Unlock()
		return
	}

}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
