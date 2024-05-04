package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.5840/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	//映射阶段应将中间键分成多个桶，供nReduce减缩任务使用，其中nReduce是减缩任务的数量(参数)。每个映射器应创建nReduce中间文件，供还原任务使用。
	m := mr.MakeCoordinator(os.Args[1:], 10) //由mrcoordinator传参
	//希望coordinator实现done()，MapReduce作业完成时返回true
	for m.Done() == false { //等待协调者完成
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
