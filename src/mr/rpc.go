package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//获取任务请求结构
type TaskRequest struct {
	X int
}

type TaskResponse struct {
	// FileName string //Map任务下存在
	// NumMapWorkers int
	// NumReduceWorkers int
	XTask Task //未知task
	NumMapTask int //存入文件时要获取X数量
	NumReduceTask int
	Id int 
	//用state表示状态替换chan
	State int32 //用来保证任务状态是否真正完成
	CurNumMapTask int //当前map任务数
	CurNumReduceTask int // 当前Reduce任务数
	// MapTaskFinish chan bool //告知worker已做完map
	// ReduceTaskFinish chan bool
}

// type TaskFinRequest struct {
// 	X int
// }

// type TaskFinResponse struct {
// 	// FileName string //Map任务下存在
// 	// NumMapWorkers int
// 	// NumReduceWorkers int
// 	XTask Task //未知task
// 	NumMapTask int //存入文件时要获取X数量
// 	NumReduceTask int
// 	Id int 
// 	MapTaskFinish chan bool //告知worker已做完map
// 	ReduceTaskFinish chan bool
// }


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
