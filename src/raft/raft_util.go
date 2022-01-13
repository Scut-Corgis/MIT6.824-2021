package raft

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) + rf.lastSSPointIndex
}
func (rf *Raft) lastLogTerm() int {
	return rf.getLogTermWithIndex(rf.lastLogIndex())

}

//用全局索引获取LogEntry
func (rf *Raft) getLogWithIndex(globalIndex int) LogEntry {
	return rf.logs[globalIndex-rf.lastSSPointIndex-1]
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	if globalIndex-rf.lastSSPointIndex == 0 {
		return rf.lastSSPointTerm
	} else {
		return rf.getLogWithIndex(globalIndex).Term
	}
}

//将全局Index转换为当前[]logs里面对应的索引
func (rf *Raft) getGlobalToNowIndex(globalIndex int) int {
	if globalIndex == rf.lastSSPointIndex {
		return -1
	}
	if globalIndex < rf.lastSSPointIndex {
		DPrintFatal("getGlobalToNowIndex越界转换错误")
		return 0
	} else {
		return globalIndex - rf.lastSSPointIndex - 1
	}
}
