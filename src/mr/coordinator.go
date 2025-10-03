package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	wgmap    sync.WaitGroup
	wgreduce sync.WaitGroup

	mu sync.RWMutex

	mapdone    int
	reducedone int

	files []string

	taskmu           sync.RWMutex
	maptaskstatus    map[string]int
	reducetaskstatus map[string]int

	workers       map[int]int //0 空闲 1 工作中 2异常
	nextworkerid  int
	nextmaptastid int
	nreduce       int
}

func (c *Coordinator) GetNextWorkerID(args *GetNextWorkerIDArgs, reply *GetNextWorkerIDReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ID = c.nextworkerid
	c.nextworkerid = c.nextworkerid + 1
	return nil
}

func (c *Coordinator) GetnReduce(args *GetnReduceArgs, reply *GetnReduceReply) error {
	reply.Nreduce = c.nreduce
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.taskmu.Lock()
	defer c.taskmu.Unlock()

	if c.reducedone == DONE {
		reply.Success = false
		return nil
	}

	switch c.mapdone {
	case START: //分配map任务
		for k, v := range c.maptaskstatus {
			if v == START {
				reply.Tasktype = TASKTYPEMAP
				reply.Filename = k
				reply.Success = true
				go c.recallTask(TASKTYPEMAP, k)
				return nil
			}
		}
	case DONE: //分配reduce任务
		for k, v := range c.reducetaskstatus {
			if v == START {
				reply.Tasktype = TASKTYPEREDUCE
				reply.Filename = k
				reply.Success = true
				go c.recallTask(TASKTYPEREDUCE, k)
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) GetTaskStatus(args *GetTaskStatusArgs, reply *GetTaskStatusReply) error {
	c.taskmu.RLock()
	defer c.taskmu.RUnlock()
	switch args.TaskType {
	case TASKTYPEMAP:
		if s, ok := c.maptaskstatus[args.Filename]; ok {
			reply.Status = s
			reply.Success = true
		}
	case TASKTYPEREDUCE:
		if s, ok := c.reducetaskstatus[args.Filename]; ok {
			reply.Status = s
			reply.Success = true
		}
	}
	return nil
}

func (c *Coordinator) recallTask(tasktype int, filename string) {
	timer := time.NewTimer(10 * time.Second)
	<-timer.C

	c.taskmu.Lock()
	defer c.taskmu.Unlock()

	switch tasktype {
	case TASKTYPEMAP:
		if status, ok := c.maptaskstatus[filename]; ok {
			if status == DONE {
				return
			}
		}
		c.maptaskstatus[filename] = START
		fmt.Println("map任务超时,filename:", filename)
	case TASKTYPEREDUCE:
		if status, ok := c.reducetaskstatus[filename]; ok {
			if status == DONE {
				return
			}
		}

		c.reducetaskstatus[filename] = START
		fmt.Println("reduce任务超时,filename:", filename)

	}

}

// 更改任务状态
func (c *Coordinator) ChangeTaskStatus(args *ChangeTaskArgs, reply *ChangeTaskReply) error {
	c.taskmu.Lock()
	defer c.taskmu.Unlock()

	switch args.Tasktype {
	case TASKTYPEMAP:
		if st, ok := c.maptaskstatus[args.Filename]; ok {
			if st != DONE && args.Taskstatus == DONE {
				c.maptaskstatus[args.Filename] = args.Taskstatus
				c.wgmap.Done()
			}
			reply.Success = true
		}
	case TASKTYPEREDUCE:
		if st, ok := c.reducetaskstatus[args.Filename]; ok {
			if st != DONE && args.Taskstatus == DONE {
				c.reducetaskstatus[args.Filename] = args.Taskstatus
				c.wgreduce.Done()
			}
			reply.Success = true
		}
	}
	return nil
}

// 获取master状态
func (c *Coordinator) GetMasterStatus(args *GetMasterStatusargs, reply *GetMasterStatusReply) error {
	reply.Status = c.reducedone
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterToCoordinator(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := args.ID
	nodestatus := args.Status
	c.workers[id] = nodestatus
	reply.Success = true
	return nil
}

func (c *Coordinator) DeleteSelf(args *DeleteSelfargs, reply *DeleteSelfreply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, args.ID)
	reply.Success = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.reducedone == DONE {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func (c *Coordinator) initTask() {
	c.initmapTask()
	c.initreduceTask()
}

func (c *Coordinator) initmapTask() {
	//初始化map任务表
	files := c.files
	for _, file := range files {
		c.maptaskstatus[file] = START
		c.wgmap.Add(1)
	}
	go func() {
		c.wgmap.Wait()
		c.mu.Lock()
		c.mapdone = DONE
		fmt.Println("allmap任务完成")
		c.mu.Unlock()
	}()
}

func (c *Coordinator) initreduceTask() {
	//初始化reduce任务表
	for i := 0; i < c.nreduce; i++ {
		s := strconv.Itoa(i)
		c.reducetaskstatus[s] = START
		c.wgreduce.Add(1)
	}
	go func() {
		c.wgreduce.Wait()
		c.mu.Lock()
		c.reducedone = DONE
		fmt.Println("reduce任务完成")
		c.mu.Unlock()
	}()
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:               sync.RWMutex{},
		wgmap:            sync.WaitGroup{},
		wgreduce:         sync.WaitGroup{},
		workers:          make(map[int]int),
		nreduce:          nReduce,
		maptaskstatus:    make(map[string]int),
		reducetaskstatus: make(map[string]int),
		files:            files,
	}

	// Your code here.
	//初始化所有任务
	c.initTask()
	c.server()
	return &c
}
