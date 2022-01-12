package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	ROLE_LEADER                           = "Leader"
	ROLE_FOLLOWER                         = "Follower"
	ROLE_CANDIDATES                       = "Candidates"
	electionTimeoutStart    time.Duration = 400 * time.Millisecond
	electionTimeoutInterval time.Duration = 150 * time.Millisecond
	heartbeatInterval       time.Duration = 100 * time.Millisecond
	RIGHTNOW                time.Duration = 5 * time.Millisecond //RIGHTNOW是我的一种实现技巧，通过给heartbeat timer赋予一个极小的时间，就可以实现马上发心跳包了
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int //当前任期
	votedFor    int //记录在当前任期投票给谁了
	logs        []LogEntry

	//volatile state on all servers
	commitIndex int   //最大已提交索引
	lastApplied int   //当前应用到状态机的索引
	nextIndex   []int //每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex  []int //每个follower的log同步进度（初始为0）

	//my define value
	voteCounts   int
	currentState string

	//选举定时器
	timer     *time.Timer
	timerLock sync.Mutex

	//心跳定时器
	heartbeatTimer     *time.Timer
	heartbeatTimerLock sync.Mutex

	applyCh chan ApplyMsg //应用层的提交队列
}
type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.currentState == ROLE_LEADER
	return term, isleader
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                   int
	Success                bool
	ConflictEntryTerm      int
	ConflictTermFirstIndex int
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

//心跳一次，如果是leader则广播心跳包
func (rf *Raft) boardCastEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != ROLE_LEADER {
		return
	}
	DPrintf("[%d] leader server boardcast entries in %d term ", rf.me, rf.currentTerm)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		var reply AppendEntriesReply
		go func(server int, args AppendEntriesArgs, currentTerm int) {
			startTime1 := time.Now()
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				startTime2 := time.Now()
				subTime := startTime2.Sub(startTime1)
				DPrintf("[%d] send AppendEntries to [%d] in term %d failed, start rpc time %v ; Duration time %v", rf.me, server, currentTerm, startTime1, subTime)
			}
			if ok {
				rf.handleAppendResults()
			}
		}(server, args, rf.currentTerm)
	}
}

//发送RPC接口
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//处理日志追加回复信息
func (rf *Raft) handleAppendResults() {
	return
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
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = ROLE_FOLLOWER
		rf.votedFor = args.LeaderId
	}
	rf.resetTimer()

}

//RPC调用，请求为调用者投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term%d CurrentTerm%d LastLogIndex%d LastLogTerm%d votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted %v ", rf.me, args.CandidateId, reply.VoteGranted)
	}()
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentState = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm < rf.lastLogTerm() || args.LastLogIndex < rf.lastLogIndex() {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimer()
	}
}

//选举定时器
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
		DPrintf("[%d] in %d term try to elect", rf.me, rf.currentTerm)
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
		DPrintf("[%d] in %d term trasnform to follwer", rf.me, rf.currentTerm)
		rf.currentState = ROLE_FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if rf.currentState == ROLE_CANDIDATES && reply.VoteGranted {
		rf.voteCounts++
		if rf.voteCounts >= len(rf.peers)/2+1 {
			rf.currentState = ROLE_LEADER
			DPrintf("[%d] is the %d term leader now", rf.me, rf.currentTerm)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.resetHeartbeatTimer(RIGHTNOW)
				// rf.nextIndex[i] = rf.lastLogIndex() + 1
				// rf.matchIndex[i] = 0
			}
			rf.resetTimer()
		}
		return
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != ROLE_LEADER {
		return -1, -1, false
	}
	LogEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, LogEntry)
	index = len(rf.logs)
	term = rf.currentTerm
	DPrintf("[%d] RaftNode Add Command, logIndex %d currentTerm %d", rf.me, index, term)
	return index, term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

////////////////////////////////////////////////
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
///////////////////////////////////////////////////

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) lastLogIndex() int {
	return len(rf.logs)
}
func (rf *Raft) lastLogTerm() int {
	if len(rf.logs) == 0 {
		return 0
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = ROLE_FOLLOWER
	rf.votedFor = -1
	rf.commitIndex = 0

	rf.applyCh = applyCh

	rand.Seed(int64(rf.me))
	rf.timerLock.Lock()
	rf.timer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart)
	rf.timerLock.Unlock()

	rf.heartbeatTimerLock.Lock()
	rf.heartbeatTimer = time.NewTimer(time.Duration(heartbeatInterval))
	rf.heartbeatTimerLock.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.candidateElectionTicker()
	go rf.heartbeatTicker()
	return rf
}
