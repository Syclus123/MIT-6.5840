package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		filename := reply.XTask.FileName
		CallGetTask(&args,&reply)
		
		//map部分使用ihash（key）函数为给定的key选择reduce任务。
		if filename != "" { //map任务
			id := strconv.Itoa(reply.XTask.IdMap) //任务id
			file, err :=os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open mapTask %v",filename)
			}
			content,err := ioutil.ReadAll(file) //读取
			if err != nil {
				log.Fatal("cannot read %v",filename)				
			}
			file.Close()
			kva := mapf(filename,string(content)) //生成键值对
			num_reduce := reply.NumReduceTask
			
			//每个buket存储每个Reduce任务需要处理的中间键值对
			bucket :=make([][]KeyValue, num_reduce) //一次性读完，防止一次写一个频繁文件IO操作
			
			//分桶逻辑为哈希
			for _,kv :=range kva {
				num := ihash(kv.Key) % num_reduce
				bucket[num] = append(bucket[num],kv)
			}
			
			//遍历所有桶，将桶内容写入临时mapout文件
			for i:= 0;i < num_reduce; i++ {
				tmp_file,error :=ioutil.TempFile("","mr-map-*") //创建临时文件
				if error != nil{
					log.Fatalf("cannot open tmp_file")
				}
				enc := json.NewEncoder(tmp_file) 
				err := enc.Encode(bucket[i]) //json编码
				if err !=nil {
					log.Fatalf("encode bucket error")
				}
				tmp_file.Close()
				out_file := "mr-" + id + "-" + strconv.Itoa(i)
				os.Rename(tmp_file.Name(),out_file) //临时文件重命名为最终文件
				//原子重命名技巧参考MapReduce论文
			}
			//worker完成任务后需要返回信号 出bug
			// reply.MapTaskFinish <- true 
			CallTaskFin() //发true不需要参数
			// fmt.Printf(">>>>>> send map task finish\n")
		} else { //Reduce任务
			// CallGetReduceTask(&args,&reply)
			fmt.Printf(">>>>>> Reduce task!\n")
			id := strconv.Itoa(reply.XTask.IdReduce) //任务id
			num_map := reply.NumMapTask
			//原来的逻辑：if len(reply.MapTaskFinish) == num_map { 但是已经接收到Reduce操作已经代表Map做完了
			//所有map任务做完后开始Reduce任务
			intermediate := []KeyValue{}
			for i :=0; i < num_map; i++ {
				map_filename := "mr-" + strconv.Itoa(i) + "-" + id
				inputFile, err := os.OpenFile(map_filename, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %v", map_filename)
				}
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}
			//排序写入reduceUutput文件（according to mrsequential）
			sort.Sort(ByKey(intermediate))
			out_file := "mr-out-" + id
			tmp_file, err := ioutil.TempFile("", "mr-reduce-*")
			if err != nil {
				log.Fatalf("cannot open tmp_file")
			}
			//去重操作
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
				output := reducef(intermediate[i].Key, values)
			
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmp_file, "%v %v\n", intermediate[i].Key, output)
			
				i = j
			}
			tmp_file.Close()
			os.Rename(tmp_file.Name(),out_file) //临时文件重命名为最终文件
			// reply.ReduceTaskFinish <- true
			CallTaskFin() //调用RPC
			//所有任务完成
			if len(reply.ReduceTaskFinish) == reply.NumReduceTask{
				break
			}
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
	ok := call("Coordinator.GetTask", &args, &reply) //与coordinator交互
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.FileName %s\n", reply.XTask.FileName)
		fmt.Printf("call get task ok!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

//每次task结束直接调用远程RPC即可
func CallTaskFin() {
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	// declare an argument structure.
	args := ExampleArgs{}
	reply := ExampleReply{} //期待回复
	//上面是无用参数
	ok := call("Coordinator.TaskFin",&args, &reply) //与coordinator交互
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.FileName %s\n", reply.XTask.FileName)
		fmt.Printf("call task finish ok!\n")
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
