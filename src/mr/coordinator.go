package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
// import "fmt"
/*
coordinator任务总结：
1、需要响应worker的请求，向其分配 Task，并记录信息
2、对于超时的 Task，重新分配一遍
初步想法：

*/

type Coordinator struct {
	// Your definitions here.
	// task Task 随便测试task
	State int //任务状态 0 初始 1 Map 2 Reduce
	// NumMapWorkers int
	// NumReduceWorkers int
	//必须是一个线程安全的队列，因为多个worker会从中取任务，所以用channel去做
	MapTask chan Task 
	ReduceTask chan Task
	//采用所有Map执行结束后转Reduce（hadoop），保证数据一致性而不是流处理方式
	NumMapTask int
	NumReduceTask int
	MapTaskFinish chan bool
	ReduceTaskFinish chan bool


}
//第一个小任务，向RPC发送任务，所以定义Task类型
type Task struct {
	FileName string
	// State int //0 开始 1 运行 2 结束
	// Id int //每个worker拿到一个任务id
	//出于严谨，区分两个处理阶段的id
	IdMap int
	IdReduce int

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1 //RPC请求-响应逻辑
	return nil
}

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	// reply.Name = c.task.name
	if len(c.MapTaskFinish) !=c.NumMapTask {
		// fmt.Printf(">>>>>> map task,%d %d %d\n",len(c.MapTaskFinish),c.NumMapTask,len(c.MapTask))
		maptask,ok := <-c.MapTask
		if ok {
			reply.XTask = maptask
			// reply.NumMapWorkers = c.NumMapWorkers
		}
	} else { // 如果get不成功说明已经get完了
		// fmt.Printf(">>>>>> reduce task\n")
		reducetask, ok := <-c.ReduceTask //去Reduce里get
		if ok {
			reply.XTask = reducetask
		}
	}
	reply.NumMapTask = c.NumMapTask 
	reply.NumReduceTask = c.NumReduceTask
	// reply.MapTaskFinish = c.MapTaskFinish
	// reply.ReduceTaskFinish = c.ReduceTaskFinish

	return nil
}
//task结束调用RPC发true
func (c *Coordinator) TaskFin(args *ExampleArgs, reply *ExampleReply) error {
	if len(c.MapTaskFinish) != c.NumMapTask { //map任务结束
		c.MapTaskFinish <- true
	} else { //Reduce task结束
		c.ReduceTaskFinish <- true
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() { //c是coordinator的引用
	rpc.Register(c) //RPC服务
	rpc.HandleHTTP() //设置RPC系统使用HTTP协议来接收RPC请求。
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname) //socket监听
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) //启动一个新的goroutine来处理通过监听到的连接传入的HTTP请求。使得协调者能够异步处理RPC请求。
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool { //用于确定所有任务（map/reduce）是否完成
	ret := false

	if len(c.ReduceTaskFinish) == c.NumReduceTask {
		ret = true
	}
	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// c := Coordinator{task: Task {name:"ccc"} }
	c := Coordinator{
		//因为默认是无缓存channel，管道中不消费就没法读,所以考虑提前给初始长度
		State: 0,
		// NumMapWorkers: 3,
		// NumReduceWorkers: nReduce,
		NumMapTask: len(files),
		NumReduceTask: nReduce,
		MapTask: make(chan Task,len(files)),
		ReduceTask: make(chan Task,nReduce),
		MapTaskFinish : make(chan bool,len(files)),
		ReduceTaskFinish: make(chan bool,nReduce)}

	//	需要初始化coordinator来分配任务等
	// Your code here.
	for id, file := range files {
		c.MapTask <- Task{FileName: file,IdMap: id}
	}
	for i := 0;i < nReduce;i++{
		c.ReduceTask <- Task{IdReduce: i}

	}
	c.server() //启动coordinator的RPC服务器接受工作节点调用
	return &c
}
