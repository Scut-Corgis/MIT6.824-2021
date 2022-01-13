package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//RPC调用，请求为调用者投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("[%d]---Handle RequestVote, CandidatesId[%d] Term%d CurrentTerm%d LastLogIndex%d LastLogTerm%d votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("[%d]---Return RequestVote, CandidatesId[%d] VoteGranted %v ", rf.me, args.CandidateId, reply.VoteGranted)
	}()
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentState = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm < rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex < rf.lastLogIndex()) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetTimer()
	}
}

//选举定时器，long running time goroutine
func (rf *Raft) candidateElectionTicker() {
	for !rf.killed() {
		<-rf.timer.C
		if rf.killed() {
			break
		}
		go rf.timeoutElection()

		rf.resetTimer()
	}
}

//选举定时器处理主体函数
func (rf *Raft) timeoutElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != ROLE_LEADER {
		rf.currentState = ROLE_CANDIDATES
		rf.currentTerm++
		rf.persist()
		DPrintf("[%d]---In %d term try to elect", rf.me, rf.currentTerm)
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}
		rf.votedFor = rf.me
		rf.voteCounts = 1
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int, currentTerm int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					rf.handleVoteResult(currentTerm, reply)
				}
			}(server, rf.currentTerm, args)
		}
	}
}

//选举RPC调用接口
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//处理选举回复reply内容
func (rf *Raft) handleVoteResult(currentTerm int, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//任期变了，因为此函数的协程处理晚了，导致已经到了新任期却收到了旧任期的回包，则忽略
	if currentTerm != rf.currentTerm {
		return
	}
	//这个是包收到的太慢，可能因为网络问题，收到了旧任期时的RPC回复包
	if reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("[%d]---In %d term has trasnformed to follwer", rf.me, rf.currentTerm)
		rf.currentState = ROLE_FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.resetTimer()
		return
	}

	if rf.currentState == ROLE_CANDIDATES && reply.VoteGranted {
		rf.voteCounts++
		if rf.voteCounts >= len(rf.peers)/2+1 {
			rf.currentState = ROLE_LEADER
			DPrintf("[%d]---Is the %d term leader now", rf.me, rf.currentTerm)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.resetHeartbeatTimer(RIGHTNOW)
				rf.nextIndex[i] = rf.lastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			rf.resetTimer()
		}
		return
	}
}
func (rf *Raft) resetTimer() {
	rf.timerLock.Lock()
	//timer的使用手法，好像没有别的正确的方法了，不能修改下面的任何代码
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
	duration := time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart
	rf.timer.Reset(duration)
	rf.timerLock.Unlock()
}
