package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 上号时上报信息
type RegisterArgs struct {
	ID     int
	Status int
}

type RegisterReply struct {
	Success bool
}

//获得下一个id号

type GetNextWorkerIDArgs struct{}

type GetNextWorkerIDReply struct {
	ID int
}

// 获取nreduce数量
type GetnReduceArgs struct{}
type GetnReduceReply struct {
	Nreduce int
}

// 向master申请派发任务
type GetTaskArgs struct{}
type GetTaskReply struct {
	Tasktype int
	Filename string
	Success  bool
}

// 更新任务状态
type ChangeTaskArgs struct {
	Tasktype   int
	Filename   string
	Taskstatus int
}
type ChangeTaskReply struct {
	Success bool
}

// 获取master状态
type GetMasterStatusargs struct{}
type GetMasterStatusReply struct {
	Status  int  //所有任务都结束了吗
	Success bool //获取成功了吗
}

// 获取任务状态
type GetTaskStatusArgs struct {
	TaskType int
	Filename string
}

// 从master节点集中删除
type DeleteSelfargs struct {
	ID int
}
type DeleteSelfreply struct {
	Success bool
}

type GetTaskStatusReply struct {
	Status  int
	Success bool
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
