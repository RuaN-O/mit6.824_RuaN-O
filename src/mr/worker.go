package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	mu                 sync.RWMutex
	id                 int
	status             int
	mapf               func(string, string) []KeyValue
	reducef            func(string, []string) string
	nreduce            int
	gettaskfailedcount int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//初始化worker

	w := newWorker(mapf, reducef)
	//上号时，上报自己的状态给master
	w.register()
	//go w.HeartBeat(5 * time.Second)

	//持续向master申请任务，直到所有任务都完成
	for {
		//time.Sleep(1 * time.Second)
		status, ok := getmasterstatus()
		if !ok || status == DONE {
			return
		}
		tasktype, filename, ok := getTask()
		if !ok || len(filename) == 0 {
			w.gettaskfailedcount++
			if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		if tasktype == TASKTYPEMAP {
			args := ChangeTaskArgs{
				Tasktype:   tasktype,
				Filename:   filename,
				Taskstatus: DOING,
			}
			reply := ChangeTaskReply{}

			w.changeTask(&args, &reply)
			if !reply.Success {
				continue
			}

			w.mu.Lock()
			w.status = WORKING
			w.mu.Unlock()

			ok := w.updatestatus(w.status)
			if !ok {
				w.gettaskfailedcount++
				if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}

			err := w.domaptask(filename)

			w.mu.Lock()
			w.status = IDLE
			w.mu.Unlock()

			ok = w.updatestatus(w.status)
			if !ok {
				w.gettaskfailedcount++
				if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			if err != nil {
				fmt.Printf("MAP TASK ERR:%v\n", err)
				continue
			}

			args = ChangeTaskArgs{
				Tasktype:   tasktype,
				Filename:   filename,
				Taskstatus: DONE,
			}
			reply = ChangeTaskReply{}
			w.changeTask(&args, &reply)
			if !reply.Success {
				continue
			}
		} else if tasktype == TASKTYPEREDUCE {
			number, err := strconv.Atoi(filename)
			if err != nil {
				fmt.Println("REDUCE任务分配错误,filename: ", filename)
				continue
			}
			args := ChangeTaskArgs{
				Tasktype:   tasktype,
				Filename:   filename,
				Taskstatus: DOING,
			}
			reply := ChangeTaskReply{}

			w.changeTask(&args, &reply)
			if !reply.Success {

				continue
			}
			w.status = WORKING
			ok = w.updatestatus(w.status)
			if !ok {
				w.gettaskfailedcount++
				if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			err = w.doreducetask(number)
			w.status = IDLE
			ok = w.updatestatus(w.status)
			if !ok {
				w.gettaskfailedcount++
				if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			if err != nil {
				fmt.Printf("REDUCE TASK ERR:%v\n", err)
				continue
			}

			args = ChangeTaskArgs{
				Tasktype:   tasktype,
				Filename:   filename,
				Taskstatus: DONE,
			}
			reply = ChangeTaskReply{}

			w.changeTask(&args, &reply)
			if !reply.Success {
				continue
			}

		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (w *worker) changeTask(args *ChangeTaskArgs, reply *ChangeTaskReply) {
	ok := call("Coordinator.ChangeTaskStatus", &args, &reply)
	if ok && reply.Success {
		fmt.Printf("ChangeTaskStatus Success !\n")
	} else {
		fmt.Printf("ChangeTaskStatus failed! args: %v\n", args)
	}
}

func (w *worker) deleteself(args *DeleteSelfargs, reply *DeleteSelfreply) {
	ok := call("Coordinator.DeleteSelf", &args, &reply)
	if ok && reply.Success {
		fmt.Printf("DeleteSelf Success !\n")
	} else {
		fmt.Printf("DeleteSelf failed! args: %v\n", args)
	}
}

func (w *worker) HeartBeat(tick time.Duration) {

	// ticker := time.NewTicker(tick)
	// defer ticker.Stop()
	// for {
	// 	<-ticker.C
	// 	w.mu.RLock()
	// 	status := w.status
	// 	w.mu.RUnlock()
	// 	ok := w.updatestatus(status)
	// 	if !ok {
	// 		w.gettaskfailedcount++
	// 		if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
	// 			return
	// 		}
	// 		time.Sleep(20 * time.Millisecond)
	// 	}
	// }

	for {
		w.mu.RLock()
		status := w.status
		w.mu.RUnlock()
		ok := w.updatestatus(status)
		if !ok {
			w.gettaskfailedcount++
			if w.gettaskfailedcount == MAXGETTASKFAILEDCOUNT {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(tick)
	}

}

func (w *worker) updatestatus(status int) bool {
	args := &RegisterArgs{
		ID:     w.id,
		Status: status,
	}
	reply := &RegisterReply{}

	ok := call("Coordinator.RegisterToCoordinator", &args, &reply)
	if ok {
		fmt.Printf("Update to Coordinator Success worker.id: %v\n", w.id)
	} else {
		fmt.Printf("Update to Coordinator failed! args: %v\n", args)
	}
	return reply.Success
}

func getnReduce() int {
	args := GetnReduceArgs{}
	reply := GetnReduceReply{}
	ok := call("Coordinator.GetnReduce", &args, &reply)
	if ok {
		fmt.Printf("getnReduce Success nReduce: %v\n", reply.Nreduce)
	} else {
		fmt.Printf("getnReduce failed! args: %v\n", args)
	}
	return reply.Nreduce
}

func (w *worker) register() bool {
	args := &RegisterArgs{
		ID:     w.id,
		Status: 0,
	}
	reply := &RegisterReply{
		Success: false,
	}

	ok := call("Coordinator.RegisterToCoordinator", &args, &reply)
	if ok {
		fmt.Printf("Register to Coordinator Success worker.id: %v\n", w.id)
	} else {
		fmt.Printf("Register to Coordinator failed! args: %v\n", args)
	}
	return reply.Success

}

func newWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *worker {
	id := getworkerid()
	nreduce := getnReduce()

	worker := &worker{
		mu:      sync.RWMutex{},
		id:      id,
		status:  0,
		mapf:    mapf,
		reducef: reducef,
		nreduce: nreduce,
	}

	return worker

}

func getmasterstatus() (int, bool) {
	args := GetMasterStatusargs{}
	reply := GetMasterStatusReply{}
	ok := call("Coordinator.GetMasterStatus", &args, &reply)
	if ok {
		reply.Success = true
		fmt.Printf("GetMasterStatusSuccess status: %v\n", reply.Status)
	} else {
		fmt.Printf("GetMasterStatus failed! args: %v\n", args)
	}
	return reply.Status, reply.Success
}

func getworkerid() int {
	args := GetNextWorkerIDArgs{}
	reply := GetNextWorkerIDReply{}
	ok := call("Coordinator.GetNextWorkerID", &args, &reply)
	if ok {
		fmt.Printf("Get WorkerID Success worker.id: %v\n", reply.ID)
	} else {
		fmt.Printf("Get WorkerID failed! args: %v\n", args)
	}
	return reply.ID
}

func getTask() (int, string, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok && reply.Success {
		fmt.Printf("GetTask Success获取任务成功 tasktype: %v,taskfilename: %v\n", reply.Tasktype, reply.Filename)
	} else {
		fmt.Printf("GetTask failed! args: %v,reply: %v\n", args, reply)
	}
	return reply.Tasktype, reply.Filename, reply.Success

}

func getTaskStatus(tasktype int, filename string) (status int, success bool) {
	args := GetTaskStatusArgs{
		TaskType: tasktype,
		Filename: filename,
	}
	reply := GetTaskStatusReply{}

	ok := call("Coordinator.GetTaskStatus", &args, &reply)
	if ok && reply.Success {
		fmt.Printf("GetTaskStatus Success Filename: %v,Status: %v\n", filename, reply.Status)
	} else {
		fmt.Printf("GetTaskStatus failed! args: %v,reply: %v\n", args, reply)
	}
	return reply.Status, reply.Success

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *worker) domaptask(filename string) (err error) {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	//写文件
	//先写内存，再写文件
	chosencache := make(map[int][]KeyValue)
	for i := range intermediate {
		nchosen := ihash(intermediate[i].Key) % w.nreduce
		if _, ok := chosencache[nchosen]; !ok {
			chosencache[nchosen] = []KeyValue{}
		}
		chosencache[nchosen] = append(chosencache[nchosen], intermediate[i])
	}

	status, ok := getmasterstatus()
	if !ok || status == DONE {
		return nil
	}
	status, ok = getTaskStatus(TASKTYPEMAP, filename)
	if !ok || status == DONE || status == WRITTING {
		return nil
	}

	args := ChangeTaskArgs{
		Tasktype:   TASKTYPEMAP,
		Filename:   filename,
		Taskstatus: WRITTING,
	}
	reply := ChangeTaskReply{}

	w.changeTask(&args, &reply)
	if !reply.Success {
		return
	}

	for k, v := range chosencache {
		//oname := "mr" + strconv.Itoa(ihash(filename)) + strconv.Itoa(k)
		oname := fmt.Sprintf("mr-%d-%d", ihash(filename), k)
		ofile, _ := os.Create(oname)
		for i := range v {
			fmt.Fprintf(ofile, "%v %v\n", v[i].Key, v[i].Value)
		}
		ofile.Close()
	}

	return nil
}

func (w *worker) doreducetask(number int) (err error) {
	files, err := filepath.Glob("mr-*")
	if err != nil {
		return
	}

	intermediate := []KeyValue{}
	for _, filename := range files {
		if isFileForThisReduceTask(filename, number) {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			intermediatestring := string(content)
			intermediates := strings.Split(intermediatestring, "\n")

			for i := range intermediates {
				ss := strings.Split(intermediates[i], " ")
				if len(ss) != 2 {
					continue
				}
				key, value := ss[0], ss[1]

				keyvalue := KeyValue{
					Key:   key,
					Value: value,
				}
				intermediate = append(intermediate, keyvalue)
			}
		}
	}

	status, ok := getmasterstatus()
	if !ok || status == DONE {
		return nil
	}
	outputid := number
	soutputid := strconv.Itoa(outputid)
	oname := "mr-out-" + soutputid
	status, ok = getTaskStatus(TASKTYPEREDUCE, soutputid)
	if !ok || status == DONE || status == WRITTING {
		return nil
	}

	args := ChangeTaskArgs{
		Tasktype:   TASKTYPEREDUCE,
		Filename:   soutputid,
		Taskstatus: WRITTING,
	}
	reply := ChangeTaskReply{}

	w.changeTask(&args, &reply)
	if !reply.Success {
		return
	}

	ofile, _ := os.Create(oname)
	//每个中间文件局部有序，需要整体再排一次序
	sort.Sort(ByKey(intermediate))
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return nil
}

func isFileForThisReduceTask(filename string, reduceNumber int) bool {
	// 文件名格式应该是 "mr-X-Y"
	parts := strings.Split(filename, "-")
	if len(parts) < 3 {
		return false
	}

	fileReduceNum, err := strconv.Atoi(parts[2])
	if err != nil {
		return false
	}

	return fileReduceNum == reduceNumber
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
