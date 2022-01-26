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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	ROLE_LEADER                           = "Leader"
	ROLE_FOLLOWER                         = "Follower"
	ROLE_CANDIDATES                       = "Candidates"
	electionTimeoutStart    time.Duration = 400 * time.Millisecond
	electionTimeoutInterval time.Duration = 150 * time.Millisecond //选举随机时间范围
	heartbeatInterval       time.Duration = 100 * time.Millisecond //心跳间隔
	RIGHTNOW                time.Duration = 5 * time.Millisecond   //RIGHTNOW是我的一种实现技巧，通过给heartbeat timer赋予一个极小的时间，就可以实现马上发心跳包了
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//持久化变量
	currentTerm      int //当前任期
	votedFor         int //记录在当前任期投票给谁了
	logs             []LogEntry
	lastSSPointIndex int //快照中最后一个日志的索引
	lastSSPointTerm  int //快照中最后一个日志对应的任期

	commitIndex int //最大已提交索引(论文中可以不持久化，我持久化进行了优化)

	//易失变量，crash后丢失
	lastApplied int   //当前应用到状态机的索引
	nextIndex   []int //每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex  []int //每个follower的log同步进度（初始为0）

	//自定义的必需变量
	voteCounts   int    //投票计数值
	currentState string //当前的状态

	//选举定时器
	timer     *time.Timer
	timerLock sync.Mutex

	//心跳定时器
	heartbeatTimer     *time.Timer
	heartbeatTimerLock sync.Mutex

	applyCh   chan ApplyMsg //应用层的提交队列
	applyCond *sync.Cond    //用来唤醒给应用层提交的协程，在commit更新时唤醒

}
type LogEntry struct {
	Term    int
	Command interface{}
}

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
// Service层转发命令给Raft层处理
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
	rf.persist()
	index = rf.lastLogIndex()
	term = rf.currentTerm
	DPrintf("[%d]---Add Command, logIndex %d currentTerm %d", rf.me, index, term)
	//打印全部logs
	toPrintLogs := make([]int, 0)
	for _, theTerm := range rf.logs {
		toPrintLogs = append(toPrintLogs, theTerm.Term)
	}
	INFO("[%d]---<Start> Leader Current logs %v", rf.me, toPrintLogs)
	//立即发一个心跳包
	rf.resetHeartbeatTimer(RIGHTNOW)
	return index, term, isLeader
}

// long time running协程，一但可以commit就给客户端commit，一般等待在条件变量applyCond上
func (rf *Raft) applyGoRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		msg := ApplyMsg{CommandValid: true, Command: rf.getLogWithIndex(rf.lastApplied).Command, CommandIndex: rf.lastApplied, SnapshotValid: false}
		rf.mu.Unlock()
		INFO("[%d]---Have committed index %d of logs to UpperFloor", rf.me, rf.lastApplied)
		rf.applyCh <- msg
	}
	close(rf.applyCh)
}

//持久化常规数据(无需加锁，外部有锁)
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//自己加的，持久化commitIndex可以显著减少网络流量
	e.Encode(rf.commitIndex)
	//add in lab2D
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)

	e.Encode(rf.logs)

	data := w.Bytes()
	INFO("[%d]---Persisted      currentTerm %d voteFor %d the size of logs %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.logs))
	rf.persister.SaveRaftState(data)
}

//从存储中读常规数据(每次恢复重启时调用)
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("[%d]---Server Begin!", rf.me)
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var commitIndex int
	var lastSSPointIndex int
	var lastSSPointTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSSPointIndex) != nil ||
		d.Decode(&lastSSPointTerm) != nil ||
		d.Decode(&logs) != nil {
		DPrintFatal("[%d]---read persist failed!", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		rf.lastSSPointIndex = lastSSPointIndex
		rf.lastSSPointTerm = lastSSPointTerm
		rf.logs = logs
	}
	DPrintf("[%d]---Server Restart!", rf.me)
}

//常规数据和快照一起持久化,lab 2d
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	//add in lab2D
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)
	e.Encode(rf.logs)

	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

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
// 初始化
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = ROLE_FOLLOWER
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	//全初始化为0
	rf.logs = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastSSPointIndex = 0
	rf.lastSSPointTerm = 0

	rand.Seed(int64(rf.me))
	rf.timerLock.Lock()
	rf.timer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart)
	rf.timerLock.Unlock()

	rf.heartbeatTimerLock.Lock()
	rf.heartbeatTimer = time.NewTimer(time.Duration(heartbeatInterval))
	rf.heartbeatTimerLock.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastSSPointIndex > 0 {
		rf.lastApplied = rf.lastSSPointIndex
	}
	go rf.candidateElectionTicker()
	go rf.heartbeatTicker()
	go rf.applyGoRoutine()

	return rf
}
