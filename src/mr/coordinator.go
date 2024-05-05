package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"
import "sync/atomic"
import "sync"


/*
coordinator任务总结：
1、需要响应worker的请求，向其分配 Task，并记录信息
2、对于超时的 Task，重新分配一遍
初步想法：

*/

type Coordinator struct {
	// Your definitions here.
	// task Task 随便测试task
	State int32 //任务状态 0 Map 1 Reduce 2 finish
	// NumMapWorkers int
	// NumReduceWorkers int
	//必须是一个线程安全的队列，因为多个worker会从中取任务，所以用channel去做
	MapTask chan Task 
	ReduceTask chan Task
	//采用所有Map执行结束后转Reduce（hadoop），保证数据一致性而不是流处理方式
	NumMapTask int
	NumReduceTask int
	// MapTaskFinish chan bool
	// ReduceTaskFinish chan bool
	//需要判断taskfinish里的完成情况（channel不可遍历）
	MapTaskTime sync.Map //并发安全映射
	ReduceTaskTime sync.Map 
	files []string

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

type TimeStamp struct {
	Time int64 //时间
	Fin bool // 标记 完成就是true
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

//被CallGetTask调用
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	// reply.Name = c.task.name
	state :=atomic.LoadInt32(&c.State) //用原子计数来完成，原先bool的channel size比int32大很多
	if state == 0 {
		if len(c.MapTask) != 0 {
			// fmt.Printf(">>>>>> map task,%d %d %d\n",len(c.MapTaskFinish),c.NumMapTask,len(c.MapTask))
			maptask,ok := <-c.MapTask
			if ok {
				reply.XTask = maptask
				// reply.NumMapWorkers = c.NumMapWorkers
			}
			reply.CurNumMapTask = len(c.MapTask)	
		} else {
			reply.CurNumMapTask = -1
		}
		reply.CurNumReduceTask = len(c.ReduceTask)
	}else if state == 1 {
		if len(c.ReduceTask) != 0 { // 如果map get不成功说明已经get完了,继续判断Reduce任务
			// fmt.Printf(">>>>>> reduce task\n")
			reducetask, ok := <-c.ReduceTask //去Reduce里get
			if ok {
				reply.XTask = reducetask
			}
			reply.CurNumReduceTask = len(c.ReduceTask)
		} else {
			reply.CurNumMapTask = -1
		}
		reply.CurNumReduceTask = -1
	}

	//更新响应数据
	reply.NumMapTask = c.NumMapTask 
	reply.NumReduceTask = c.NumReduceTask
	reply.State = state //替换下面两个状态chan
	// reply.MapTaskFinish = c.MapTaskFinish
	// reply.ReduceTaskFinish = c.ReduceTaskFinish
	return nil
}

//task结束调用RPC发true，可以被多线程访问
func (c *Coordinator) TaskFin(args *Task, reply *ExampleReply) error {
	time_now := time.Now().Unix()
	if lenTaskFin(&c.MapTaskTime) != c.NumMapTask { //map任务未完成
		start_time, _ := c.MapTaskTime.Load(args.IdMap) //指定id的时间
		if time_now - start_time.(TimeStamp).Time > 10 { //超过10s返回结果丢弃
			return nil
		}
		c.MapTaskTime.Store(args.IdMap, TimeStamp{time_now, true}) //标记
		if lenTaskFin(&c.MapTaskTime) == c.NumMapTask { //map task全部完成
			atomic.StoreInt32(&c.State , 1) //原子操作，因为可能会被多线程读写
			for i :=0; i < c.NumReduceTask;i++ { //现在才分配Reduce任务(map结束)
				c.ReduceTask <-Task{IdReduce : i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if lenTaskFin(&c.ReduceTaskTime) != c.NumReduceTask { // Reduce
		start_time, _ := c.ReduceTaskTime.Load(args.IdMap) //指定id的时间
		if time_now - start_time.(TimeStamp).Time > 10 { //超过10s返回结果丢弃
			return nil
		}
		c.ReduceTaskTime.Store(args.IdReduce, TimeStamp{time_now, true}) //标记
		if lenTaskFin(&c.ReduceTaskTime) == c.NumReduceTask { //reduce task全部完成
			atomic.StoreInt32(&c.State , 2) //原子操作，因为可能会被多线程读写
		}
	}

	// if len(c.MapTaskFinish) != c.NumMapTask { //map任务结束
	// 	c.MapTaskFinish <- true
	// 	if len(c.MapTaskFinish) == c.NumMapTask {
	// 		c.State = 1 //所有map任务finish
	// 	}
	// } else if len(c.ReduceTaskFinish) != c.NumReduceTask { //判断是否Reduce task结束
	// 	c.ReduceTaskFinish <- true
	// 	if len(c.ReduceTaskFinish) != c.NumReduceTask {
	// 		c.State = 2 //所有Reduce任务finish
	// 	}
	// }	
	//解决冲突问题：保证Finish指令发送完才能执行get task操作
	// time.Sleep(time.Second)
	return nil
}

//判断有无超时任务，有则重发 timetick可以多线程共享，但是里面的东西不可（确保安全）
func (c *Coordinator) TimeTick() {
	state :=atomic.LoadInt32(&c.State)
	time_now :=time.Now().Unix()
	
	if state == 0 {
		for i := 0; i < c.NumMapTask ; i++ {
			tmp,_ := c.MapTaskTime.Load(i)
			//未完成且超时
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time >10 {
				fmt.Println("map time out")
				c.MapTask <- Task{FileName: c.files[i], IdMap: i}
				c.MapTaskTime.Store(i, TimeStamp{time_now, false}) //刷新时间
			}		
		}
	} else if state ==1 {
		for i := 0; i < c.NumReduceTask ; i++ {
			tmp,_ := c.ReduceTaskTime.Load(i)
			//未完成且超时
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time >10 {
				fmt.Println("reduce time out")
				c.ReduceTask <- Task{IdReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false}) //刷新时间
			}		
		}
	}
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
	// Your code here.
	if lenTaskFin(&c.ReduceTaskTime) == c.NumReduceTask {
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
		files: files,
		// MapTaskFinish : make(chan bool,len(files)),
		// ReduceTaskFinish: make(chan bool,nReduce),
	}

	//	需要初始化coordinator来分配任务等
	// Your code here.

	//先分配完map任务然后再在fin调用中分配Reduce任务
	time_now := time.Now().Unix()
	for id, file :=range files{
		c.MapTask <- Task{FileName: file, IdMap:id}
		c.MapTaskTime.Store(id, TimeStamp{time_now, false})
	}

	// for id, file := range files {
	// 	c.MapTask <- Task{FileName: file,IdMap: id}
	// }
	// for i := 0;i < nReduce;i++{
	// 	c.ReduceTask <- Task{IdReduce: i}
	// }

	c.server() //启动coordinator的RPC服务器接受工作节点调用
	return &c
}

//统计map中有多少任务已经完成
func lenTaskFin(m *sync.Map) int { // 要保证map的线程安全所以用线程map
	var i int //已完成任务数
	m.Range(func(k,v interface{}) bool { // v为Timestamp类型
		if v.(TimeStamp).Fin {
			i++
		}
		return true
	})
	return i
}

func lenSyncMap(m *sync.Map) int{
	var i int
	m.Range(func(k,v interface{})bool{
		i++
		return true
	})
	return i
}
