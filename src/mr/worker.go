package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"

/*
worker任务总结：
1、向coordinator请求任务
2、执行map/reduce 任务 或 处于空闲状态
3、向coordinator报告任务是否执行完毕

*/

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int { //确定给定键后应该被分配到哪个Reduce任务上
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) //7fffff确保哈希值是非负数
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//应将中间map输出存放到当前目录下的文件中方便作为Reduce任务的输入来读取
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := TaskRequest{}
		reply := TaskResponse{} //期待回复
		CallGetTask(&args,&reply)
		if reply.FileName != "" {
			file, err :=os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open mapTask %v",reply.FileName)
			}
			content,err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatal("cannot read %v",reply.FileName)				
			}
			file.Close()
			kva := mapf(reply.FileName,string(content))
			
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallGetTask(args *TaskRequest,reply *TaskResponse) {

	// declare an argument structure.
	// args := TaskRequest{}
	// // // fill in the argument(s).
	// // args.X = 99 //请求参数
	// // declare a reply structure.
	// reply := TaskResponse{} //期待回复

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply) //与coordinator交互
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.FileName %s\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99 //请求参数

	// declare a reply structure.
	reply := ExampleReply{} //期待回复

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply) //与coordinator交互
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool { //UNIX socket建立连接发送RPC请求
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock() //获取socket名称
	c, err := rpc.DialHTTP("unix", sockname) //建立RPC连接，c连接实例
	if err != nil {
		log.Fatal("dialing:", err) //打印log并终止
	}
	defer c.Close() //延迟关闭

	err = c.Call(rpcname, args, reply) //发送RPC请求
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
