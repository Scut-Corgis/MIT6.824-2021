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

func (c *Coordinator) backgroundTimer(taskType string, index int) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	<-timer.C
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch taskType {
	case "map":
		if c.mapTasks[index] == taskRunning {
			c.mapTasks[index] = taskIdle
			mlog.Printf("%s task %d timeout!\n", taskType, index)
			mlog.Println("now map task", c.mapTasks)
		}
	case "reduce":
		if c.reduceTasks[index] == taskRunning {
			c.reduceTasks[index] = taskIdle
			mlog.Printf("%s task %d timeout!\n", taskType, index)
			mlog.Println("now reduce task", c.reduceTasks)
		}
	}
}
func (c *Coordinator) Work(args *RequstArgs, reply *RequstReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.allFinish {
		reply.AllFinish = true
		return nil
	}
	for i, taskState := range c.mapTasks {
		if taskState != taskIdle {
			continue
		}
		reply.File = c.files[i]
		reply.TaskId = i
		reply.TaskType = "map"
		reply.AllFinish = false
		reply.NMap = len(c.mapTasks)
		reply.NReduce = len(c.reduceTasks)
		c.mapTasks[i] = taskRunning
		go c.backgroundTimer("map", i)
		mlog.Println("map task", i, "has sent to worker ", args.WorkerId)
		mlog.Println("now map task", c.mapTasks)
		return nil
	}
	if !c.mapFinish {
		reply.AllFinish = false
		return nil
	}
	for i, taskState := range c.reduceTasks {
		if taskState != taskIdle {
			continue
		}
		reply.TaskId = i
		reply.TaskType = "reduce"
		reply.AllFinish = false
		reply.NMap = len(c.mapTasks)
		reply.NReduce = len(c.reduceTasks)
		c.reduceTasks[i] = taskRunning
		go c.backgroundTimer("reduce", i)
		mlog.Println("reduce task", i, "has sent to worker", args.WorkerId)
		mlog.Println("now reduce task", c.reduceTasks)
		return nil
	}
	if c.allFinish {
		reply.AllFinish = true
	} else {
		reply.AllFinish = false
	}
	return nil
}
func (c *Coordinator) Commit(args *CommitArgs, reply *CommitReply) error {
	mlog.Println("worker", args.WorkerId, "commit", args.TaskType, "task", args.TaskId)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch args.TaskType {
	case "map":
		c.mapTasks[args.TaskId] = taskDown
		c.mapCount++
		if c.mapCount == len(c.files) {
			c.mapFinish = true
		}
	case "reduce":
		c.reduceTasks[args.TaskId] = taskDown
	}
	for _, state := range c.reduceTasks {
		if state != taskDown {
			return nil
		}
	}
	c.allFinish = true
	mlog.Println("all tasks finish")
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
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
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.allFinish {
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
	c := Coordinator{
		files:       files,
		mapTasks:    make([]int, len(files)),
		reduceTasks: make([]int, nReduce),
		mapCount:    0,
		mapFinish:   false,
		allFinish:   false,
	}
	mlogInit()
	// Your code here.

	c.server()
	return &c
}
