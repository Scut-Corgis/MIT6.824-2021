package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                  int
	Success               bool
	FollowerCommitedIndex int //follower的commitedIndex
}

// RPC调用，用于心跳或者日志追加
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//大于时，直接转follower没问题，等于时，说明当下任期已经选出了一个leader，直接跟随它
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = ROLE_FOLLOWER
		rf.votedFor = args.LeaderId
		rf.persist()
	}
	//日志不匹配，请求leader调低index
	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogIndex < rf.lastSSPointIndex || (args.PrevLogTerm != 0 && args.PrevLogTerm != rf.getLogTermWithIndex(args.PrevLogIndex)) {
		//我这个优化过头了，不过测试过没问题，直接从commitIndex发给自己，一次搞定！
		reply.FollowerCommitedIndex = rf.commitIndex
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.resetTimer()
		return
	}
	//检查日志有没有冲突的，有的话，删掉自己的，然后全部append；
	//没有冲突分两种情况，比我的长，加上去，没我的长，忽略它，因为可能是老RPC包
	for i, j := (args.PrevLogIndex - rf.lastSSPointIndex), 0; i < len(rf.logs) && j < len(args.Entries); i, j = i+1, j+1 {
		//冲突的情况
		if rf.logs[i].Term != args.Entries[j].Term {
			rf.logs = rf.logs[:rf.getGlobalToNowIndex(args.PrevLogIndex)+1]
			rf.logs = append(rf.logs, args.Entries...)

			toPrintLogs := make([]int, 0)
			for _, theTerm := range rf.logs {
				toPrintLogs = append(toPrintLogs, theTerm.Term)
			}
			DPrintf("[%d]---Success Receive logs from [%d], lastSSPointIndex %d, current logSize %d, current logs %v", rf.me, args.LeaderId, rf.lastSSPointIndex, rf.lastLogIndex(), toPrintLogs)
			rf.persist()
			if len(args.Entries)+args.PrevLogIndex != rf.lastLogIndex() {
				DPrintFatal("长度不匹配，断言出错！")
			}
		}
	}
	//没冲突并且比我长的情况
	if len(args.Entries)+args.PrevLogIndex > rf.lastLogIndex() {
		rf.logs = rf.logs[:rf.getGlobalToNowIndex(args.PrevLogIndex)+1]
		rf.logs = append(rf.logs, args.Entries...)

		toPrintLogs := make([]int, 0)
		for _, theTerm := range rf.logs {
			toPrintLogs = append(toPrintLogs, theTerm.Term)
		}
		DPrintf("[%d]---Success Receive logs from [%d], lastSSPointIndex %d, current logSize %d, current logs %v", rf.me, args.LeaderId, rf.lastSSPointIndex, rf.lastLogIndex(), toPrintLogs)
		rf.persist()
	}

	//修改commitIndex
	if args.LeaderCommit > rf.commitIndex {
		prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		nowCommitIndex := rf.commitIndex
		if prevCommitIndex != nowCommitIndex {
			DPrintf("[%d]---Follower commitIndex change to %d", rf.me, rf.commitIndex)
			rf.applyCond.Broadcast()
		}
	}
	//走到这一步一定返回true
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetTimer()
}

//心跳定时器，long time running协程
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		<-rf.heartbeatTimer.C
		if rf.killed() {
			break
		}
		go rf.boardCastEntries()
		rf.resetHeartbeatTimer(heartbeatInterval)
	}
}

//心跳一次，如果是leader则广播心跳包(可能是发心跳数据包，也有可能发快照包，取决于nextIndex[server]是不是已经在快照当中了)
func (rf *Raft) boardCastEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != ROLE_LEADER {
		return
	}
	DPrintf("[%d]---Boardcast AppendEntries in %d term ", rf.me, rf.currentTerm)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		//检查是发快照还是直接发日志
		prevLogIndexTemp := rf.nextIndex[server] - 1
		if prevLogIndexTemp < rf.lastSSPointIndex {
			go rf.leaderSendSnapShot(server)
			continue
		}
		//复制一份用来发送
		theLogLengthToSend := rf.lastLogIndex() - rf.nextIndex[server] + 1
		toServerEntries := make([]LogEntry, theLogLengthToSend)
		copy(toServerEntries, rf.logs[rf.getGlobalToNowIndex(rf.nextIndex[server]):])

		//处理边界问题
		var preLogTerm int
		if rf.nextIndex[server] == 1 {
			preLogTerm = 0
		} else {
			preLogTerm = rf.getLogTermWithIndex(rf.nextIndex[server] - 1)
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  preLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      toServerEntries,
		}
		go func(server int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			startTime1 := time.Now()
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				startTime2 := time.Now()
				subTime := startTime2.Sub(startTime1)
				INFO("[%d]---Send AppendEntries to [%d] in term %d failed, start rpc time %v ; Duration time %v", rf.me, server, args.Term, startTime1, subTime)
			}
			if ok {
				rf.handleAppendResults(server, &args, &reply)
			}
		}(server, args)
	}
}

//发送心跳RPC接口
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//处理日志追加回复信息
func (rf *Raft) handleAppendResults(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//对方比我任期大了，立即转为follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.resetTimer()
		return
	}
	//收到了过期的RPC回复
	if args.Term != rf.currentTerm || reply.Term != rf.currentTerm {
		return
	}
	//收到了正确的回答,修改对应matchIndex和nextIndex
	if reply.Success {
		prev := rf.matchIndex[server]
		rf.matchIndex[server] = Max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		now := rf.matchIndex[server]
		rf.nextIndex[server] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.matchIndex[server])
		INFO("[%d]---Received true from [%d], Now matchIndex = %d, nextIndex = %d---<handleAppendResults> ", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
		if prev != now {
			//判断commit是否可以向前推，排序，取中位数和现在的commitIndex对比
			sortMatchIndex := make([]int, 0)
			sortMatchIndex = append(sortMatchIndex, rf.lastLogIndex())
			for i, value := range rf.matchIndex {
				if i == rf.me {
					continue
				}
				sortMatchIndex = append(sortMatchIndex, value)
			}
			sort.Ints(sortMatchIndex)
			newCommitIndex := sortMatchIndex[len(rf.peers)/2]
			//注意：只能提交自己的任期的日志，解决了论文Fig 8的问题
			if newCommitIndex > rf.commitIndex && rf.getLogTermWithIndex(newCommitIndex) == rf.currentTerm {
				rf.commitIndex = newCommitIndex
				DPrintf("[%d]---Leader commitIndex change to %d", rf.me, rf.commitIndex)
				rf.applyCond.Broadcast()
			}
		}
	} else {
		//直接从follower的commitIndex发
		rf.nextIndex[server] = reply.FollowerCommitedIndex + 1
	}
}

func (rf *Raft) resetHeartbeatTimer(duration time.Duration) {
	rf.heartbeatTimerLock.Lock()
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(duration)
	rf.heartbeatTimerLock.Unlock()
}
