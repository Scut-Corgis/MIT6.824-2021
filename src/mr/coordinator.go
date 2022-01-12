package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mlog *log.Logger
var mlogFile *os.File

func mlogInit() {
	workerId := os.Getpid()
	logName := "coordinator" + strconv.Itoa(workerId) + ".log"
	mlogFile, _ = os.Create(logName)
	mlog = log.New(mlogFile, "", log.Lmicroseconds|log.Lshortfile)
}

type Coordinator struct {
	files       []string
	mapTasks    []int
	reduceTasks []int
	mapCount    int
	mapFinish   bool
	allFinish   bool
	mutex       sync.Mutex
}

const (
	taskIdle = iota
	taskRunning
	taskDown
)

func (m *Coordinator) backgroundTimer(taskType string, index int) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	<-timer.C
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch taskType {
	case "map":
		if m.mapTasks[index] == taskRunning {
			m.mapTasks[index] = taskIdle
			mlog.Printf("%s task %d timeout!\n", taskType, index)
			mlog.Println("now map task", m.mapTasks)
		}
	case "reduce":
		if m.reduceTasks[index] == taskRunning {
			m.reduceTasks[index] = taskIdle
			mlog.Printf("%s task %d timeout!\n", taskType, index)
			mlog.Println("now reduce task", m.reduceTasks)
		}
	}
}
func (m *Coordinator) Work(args *RequstArgs, reply *RequstReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.allFinish {
		reply.AllFinish = true
		return nil
	}
	for i, taskState := range m.mapTasks {
		if taskState != taskIdle {
			continue
		}
		reply.File = m.files[i]
		reply.TaskId = i
		reply.TaskType = "map"
		reply.AllFinish = false
		reply.NMap = len(m.mapTasks)
		reply.NReduce = len(m.reduceTasks)
		m.mapTasks[i] = taskRunning
		go m.backgroundTimer("map", i)
		mlog.Println("map task", i, "has sent to worker ", args.WorkerId)
		mlog.Println("now map task", m.mapTasks)
		return nil
	}
	if !m.mapFinish {
		reply.AllFinish = false
		return nil
	}
	for i, taskState := range m.reduceTasks {
		if taskState != taskIdle {
			continue
		}
		reply.TaskId = i
		reply.TaskType = "reduce"
		reply.AllFinish = false
		reply.NMap = len(m.mapTasks)
		reply.NReduce = len(m.reduceTasks)
		m.reduceTasks[i] = taskRunning
		go m.backgroundTimer("reduce", i)
		mlog.Println("reduce task", i, "has sent to worker", args.WorkerId)
		mlog.Println("now reduce task", m.reduceTasks)
		return nil
	}
	if m.allFinish {
		reply.AllFinish = true
	} else {
		reply.AllFinish = false
	}
	return nil
}
func (m *Coordinator) Commit(args *CommitArgs, reply *CommitReply) error {
	mlog.Println("worker", args.WorkerId, "commit", args.TaskType, "task", args.TaskId)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch args.TaskType {
	case "map":
		m.mapTasks[args.TaskId] = taskDown
		m.mapCount++
		if m.mapCount == len(m.files) {
			m.mapFinish = true
		}
	case "reduce":
		m.reduceTasks[args.TaskId] = taskDown
	}
	for _, state := range m.reduceTasks {
		if state != taskDown {
			return nil
		}
	}
	m.allFinish = true
	mlog.Println("all tasks finish")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		mlog.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	mlog.Println("the Coordinator begin server")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Coordinator) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.allFinish {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		files:       files,
		mapTasks:    make([]int, len(files)),
		reduceTasks: make([]int, nReduce),
		mapCount:    0,
		mapFinish:   false,
		allFinish:   false,
	}
	mlogInit()
	// Your code here.

	m.server()
	return &m
}
